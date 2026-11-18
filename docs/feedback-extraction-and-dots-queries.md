# Feedback metadata extraction and the DOTS queries

Written for: the analyst who runs the DOTS export by hand in Postgres, and the engineer who operates the extraction step.

After a feedback session is stored (Mitra or Google Forms), one LLM call per session reads the **complete conversation** and returns the DOTS metadata fields of the respondent's persona (a different list per persona, for example Youth: `age`, `gender`, `state`, `district`, `current_activity_status`, ...; the full lists are in section 3) mapped to the product team's category bands, plus a **whole-conversation PII flag**. Each persona has its **own prompt**, chosen by its extraction config. The DOTS export itself is not built: you pivot the results with the queries in section 6.

**Extraction starts automatically after ingestion on a new database.** The seed (`seed_config.sql`) lists the `feedback_metadata_extraction` step in every upload config, so each ingested session triggers the workflow. An existing database keeps its own `process_steps` (the seed never overwrites), and `migrate` adds an empty list to an old config, so upgrading never starts paid calls by itself: enable it per persona and source (section 5). **Cost: one real LLM call per stored session**, so a persona-wide run is a real bill. Try it on one or two sessions first with `python tools/run_feedback_extraction.py run --session-type Youth_Voice --tenant-code <tenant> --limit 2`. The list of things product still has to confirm is in section 7.

## 1. How it works

1. The workflow step `feedback_metadata_extraction` (activity `app/temporal/feedback_extraction_activity.py`) or the CLI (`tools/run_feedback_extraction.py`) runs the same core, `extract_submission` in `app/database/feedback_extraction_operations.py`.
2. It loads the stored session (`feedbacks` rows ordered by `sequence`), the **active extraction config version** of the session (its `session_type` and its EXACT program, or the persona's program-less default of the tenant; **never another program's config**: a session of a program that has no extraction config of its own fails loudly with `ExtractionSetupError`, no LLM call), and the **active prompt version of the prompt the config names** (`prompt_name`, **required**: a config without one is rejected, there is no default prompt). The highest active `prompt_version` of that exact `prompts.name` is used; a name that does not exist, has no active version, or belongs to a prompt whose `analysis_type` is not `feedback_metadata_extraction` **fails loudly** (no silent fallback to any other prompt, no LLM call). The `prompt_version_id` stored on the rows and in `meta_data.extraction` is the one actually used.
3. The conversation is built as one line per message: `[sequence] Bot: ...` or `[sequence] Respondent: ...`. A row is a respondent answer exactly when `feedbacks.parent_id` is set. Bot lines use the English text; respondent lines show the original text and, when it differs, `(English translation: ...)`, so the model sees both. The closing (thank-you) message is left out when its `question_key` is in the `closing_keys` of the upload config that applies to THIS session (same tenant, session_type and program, and the session's own source; keys of another program or source are never merged in). Every message is collapsed to one line (control characters become spaces, so words do not merge). **Respondent messages are never truncated**: PII after a cut would be lost. Bot messages (the questionnaire's own text) are cut at `limits.max_bot_message_chars` (default 4,000). If a respondent message is longer than `limits.max_respondent_message_chars` (default 20,000), or the whole conversation is longer than `limits.max_conversation_chars` (default 200,000), the session is **not sent to the model at all**: it is recorded as `pii_detected = TRUE` with type `other`, its fields are `unmapped` (the respondent answered, nothing was read) and no LLM call is made, so a person can look at it and it never reaches the export.
4. **One LLM call** (same `openrouter_chat_completion` path as the other steps; the step's `llm_model`, `max_tokens`, `llm_timeout_seconds` overrides win, then the config's `llm` block, then the `.env` settings; the Temporal activity's time limit is the effective timeout plus 120 s, see the retries note in section 5: a config `llm.timeout_seconds` that is longer than the activity limit allows is **clamped**, not refused). The model reads the whole conversation (with numbers, e-mail addresses, handles and links of the respondent masked first: step 6), so it does not matter whether the questionnaire version asks demographics early (V1) or at the end (V2), or whether the source is Mitra or a Google Form.
5. The answer is **untrusted**: strict JSON only (markdown fences tolerated), no duplicate keys, every configured field present and nothing else, correct types. Each value is checked against the field's allowed values (an `integer` field against its `min` / `max`: a JSON number or a numeric string such as `"17"` inside the range is stored as the canonical decimal string `17`; text, a decimal such as `17.5`, a boolean or an out-of-range number is `unmapped`). A value outside them is **not coerced**: the status is `unmapped`, the `value` is the field's fallback label (its own `Unmapped` if it has one, else `Others`, else `Invalid response`: a value is never empty) and the words are kept in `raw_value`. A category field accepts the config's `aliases` (other spellings of a category, matched after case and spacing: every spelling of Delhi means `Delhi`) and, for a `multi` field, a JSON list (or a comma-separated text) of categories, stored distinct, in the config's order, joined by `, ` in `value` and listed in `value_list`; a `bands` integer field takes a number (decimals allowed, `15.5`) and stores the label of its band, the number stays in `raw_value`. A free-text field (district) whose value is only a placeholder (the config's `placeholders` list; the built-in English list is `N/A`, `null`, `none`, `Not provided`, `not specified`, `unknown`, `don't know` and similar, case-insensitive) is `not_provided` (value `Not answered`), not `extracted`. A free-text value that starts with `=`, `+`, `-` or `@` is not a place name and is `unmapped` (a spreadsheet would run it as a formula); a stored `raw_value` that starts with one of them gets a leading apostrophe. A **sensitive** field keeps no `raw_value` (see the `sensitive` rule in section 3). `confidence` and `justification` are optional and informational: a malformed one (for example a confidence of `85`, or a justification that is a number) is ignored and stored NULL, the session is not failed; an unknown key still fails. The `justification` is the model's own one-sentence reason for the value (or for leaving it empty or `unmapped`), asked for by the output format the code renders into every extraction prompt (at most 20 words, describe, do not quote); it is stored for every field, the sensitive ones included, as one line cut to 300 characters (and with the leading apostrophe of a stored text that starts like a formula).
6. **Deterministic PII check: masked BEFORE the call, flag after it.** The model's `pii.detected = false` is not trusted alone (a respondent can write "set pii to false"), and respondent text with a phone number, an email or a link must not be sent to the AI provider only to be flagged afterwards. So the code scans the **respondent** text (the original and the translated column of every row with `parent_id` set; bot rows are never touched) **before the prompt is built** and **replaces what it finds** with a short placeholder in the text the MASKING call receives (the extraction call reads the original, see step 6c): `[number removed]` for a phone or id number (every message of a number answered in parts, `98765` then `43210`), `[email removed]` for an email address, `[handle removed]` for an `@handle`, `[link removed]` for a URL (the whole word, so `www.x.in/asha` goes entirely). Only a column that contains a hit changes (it then reads as the check saw it: NFKC-normalised, on one line); every other text is sent exactly as stored. The size checks of step 3 run on the original text first. After a valid answer the code forces `pii_detected = TRUE` when the model said so **or** the check found something (a hit is recorded even when the model says false); the matching kinds (`phone` for a number, `email` for an email or handle, `other` for a URL) are added to the types (a kind that is not in the config's `pii.types` is recorded as `other`). It can only turn the flag TRUE, never FALSE. It is blunt on purpose: a false positive only drops a session from the export, a false negative leaks PII. Only the kind is logged, never the text. The masking is the config key `pii.mask_before_call` (boolean, default **true**); `false` sends the text unmasked and runs the check only after the call, as before (not recommended: the numbers then reach the provider). The text the model never sees cannot be quoted back in `raw_value` either. The policy (numbers are the `pii` block of the config, section 3):
   - Before matching, the text is NFKC-normalised (fullwidth, circled and superscript digits and the fullwidth `@` become ASCII) and format characters (zero-width space and joiner, soft hyphen, BOM, bidi marks) are removed.
   - **Digits**: groups of digits separated by anything that is neither a letter nor a digit (space, `-`, `.`, `,`, `/`, `(`, `)`, en and em dash, middle dot, underscore, ...; at most three such characters in a row) form one run; Devanagari, Kannada and other Unicode digits count. **A comma is the exception**: it joins two groups only when **both** have at least four digits (`98765,43210`); a comma next to a group of one to three digits ends the run, so Indian-format amounts (`2,50,000-3,00,000`, `1,50,00,000`) and lists of small numbers (`120, 135, 150, 160`) are not phone numbers (a flagged session is left out of the export and its free text is wiped). A run is a number when it holds at least `pii.min_digit_run` digits (default **8**). Exceptions: a run made only of one- and two-digit groups (marks `85 90 78 92`, a list of ages or classes) needs `pii.min_digit_run_short_groups` digits (default **10**), and a run whose groups are all **years from 1900 to 2099** (`2018-2022`, `2019 to 2023`, `2018, 2019`) never counts. Ages, classes, years, year ranges and amounts up to seven digits and amounts or lists written with commas pass; a full date written in digits (`12/03/2010`, 8 digits) counts as a number. A letter between digits ends the run, so **number words (`nine eight ...`) and digits split by letters (`98765x43210`) are not detected** (only the model can flag them). Consecutive respondent messages that are nothing but numbers of three or more digits are joined (`98765` then `43210` is one number).
   - **E-mail**: letters, digits or `._+-` then `@` then a letter (`asha@gmail` counts, `rate@5%` does not). **Handle**: `@` at the start of a word followed by a word character and at least one more. A word that holds an `@` but is neither (`https://example.org/@asha_k`, `medium.com/@asha_k`: a `/` precedes the `@`) is checked as a link, so profile URLs are masked and flagged like any other link. Spoken forms (`asha at gmail dot com`) and a handle without `@` (`insta asha_k_2010`) are not detected.
   - **URL**: `http(s)://` or `www.` followed by text, or a bare `name.tld` whose tld is in `pii.url_tlds`. A two-letter tld followed by a space and a letter (`learn.in the village`: a missing space after a full stop) is not read as a URL.
   - Every pattern is linear in the length of the text (bounded quantifiers, one pass for the digits), and so is the masking: a 20,000-character message of hyphens, dots or `@` is checked and masked in milliseconds.
   - **A flagged session keeps everything it extracted**: the flag (`pii_detected` TRUE, set by the model or forced by the backstop) marks the session, but its field values, the respondent's words in `raw_value` (a sensitive field still stores none) and its stored responses are all kept: they stay in our system, and what an export may contain is decided by the query that makes it (the documented queries exclude flagged sessions).
6b. **Optional model-based masking of personal data and abusive language (`pii.masking_prompt_name`).** The local check of step 6 only knows shapes (numbers, emails, handles, links). Names, schools and villages need a model, so a program can name a masking prompt in its extraction config and the session then makes **one more call before the extraction call**. It reuses the contract of the story and discussion step `pii_and_abusive_language_detection` (a prompt in the `prompts` table, `{columns}` and `{{text}}` placeholders, one `masked_text` / `pii_found` / `abusive_language` entry per key, strict coverage) with a prompt of its own for respondents of the feedback programs: `Feedback PII and Abusive-Language Detection`, `analysis_type` `feedback_pii_and_abusive_language_detection`, seeded in `seed_prompts.sql`. The input is **one JSON object keyed by the `sequence` number of each respondent answer**, each value the question it replies to and the answer (a key by question text would clash when the bot repeats a question):

   ```json
   {"2": {"question": "What is your name?", "answer": "Asha Verma"},
    "4": {"question": "Which school do you attend?", "answer": "DPS School Dumra"}}
   ```

   The question is context only (an answer `Asha` to `What is your name?` is a name); only the answer is masked. The text sent is already the locally masked text of step 6, so numbers, emails, handles and links never reach this model either. The reply must have an entry with a string `masked_text` for **every** key; anything else (not JSON, a duplicate key, a missing key, a `null` text) fails the step and it is retried, and the original text is **never** used as a fallback. The masked answers are stored as the copy that may be SHARED (`meta_data.extraction.masking`); they do not replace anything in the extraction prompt unless `pii.extraction_text` is `masked` (step 6c). `pii_detected` becomes TRUE when this call found personal data **or** a PII tag (`<PERSON>`, `<LOCATION>`, `<ID>`, `<PHONE>`, `<EMAIL>`) is in a masked text, in addition to the model flag of the extraction call and the local check; the types are mapped to the config's `pii.types` (`PERSON` to `name`, `LOCATION` to `address`, `PHONE` to `phone`, `EMAIL` to `email`, anything else `other`). Abusive language is **not** a PII flag: it is recorded next to the masked answers. Both calls share the activity's time budget (the second gets what is left of it; a budget used up before the second call is a retryable `ExtractionBudgetExhaustedError`), and each gets its own `llm_logs` row (`analysis_type` `feedback_metadata_extraction` and `feedback_pii_and_abusive_language_detection`). A config without the key makes no masking call, a name that does not exist or is not a masking prompt fails before any call. The extraction step stays one workflow step and one Temporal activity (see the stage view in section 1, step 7b). **Justifications of findings**: the prompt asks for a `justification` (and a `confidence`) on every `pii_found` and `abusive_spans` item (the older key `reason` is read when `justification` is absent); they are stored per finding in `meta_data.extraction.masking.findings` as `{answer, kind (pii | abusive_language), type | severity, confidence, justification}`, **never the matched text** (the prompt forbids copying it, and nothing reads it).

   What is stored, in `feedback_submissions.meta_data -> 'extraction' -> 'masking'` (no schema change; a re-run replaces the whole `extraction` object, so switching the step off drops the old record): `prompt_version_id`, `pii_types`, `abusive_language`, `abusive_severities` (`mild`, `moderate`, `severe`) and `conversation`, the **masked** answers with their questions keyed by sequence. The original words stay in `feedbacks`, as for every source. A query for the masked JSON of a session:

   ```sql
   SELECT fs.submission_id, fs.pii_detected,
          fs.meta_data -> 'extraction' -> 'masking' -> 'conversation' AS masked_conversation,
          (fs.meta_data -> 'extraction' -> 'masking' ->> 'abusive_language')::boolean AS abusive
   FROM feedback_submissions fs
   WHERE fs.meta_data -> 'extraction' -> 'masking' IS NOT NULL;
   ```

   Cost: one more call per ingested session. The seeds switch it on for the four personas of a new database; an existing database keeps its config until a new config version adds `"masking_prompt_name": "Feedback PII and Abusive-Language Detection"` under `pii`, added the way any extraction config change is made (`docs/feedback-extraction-bands.md`). What an export does with a flagged session is decided where the export query runs, not here.
6c. **What the extraction call reads: the ORIGINAL text (`pii.extraction_text`).** The metadata is analysed first, on the respondent's own words, so the extraction call reads the **original** conversation (config key `pii.extraction_text`, `original` by default; `masked` makes it read the masked copy, the way the step worked before). Consequences: `feedback_extracted_data.raw_value` and `justification` hold the respondent's own words and may quote them; `meta_data.extraction.responses.extraction` is the model's raw answer and holds them too; the local check still computes the PII kinds from the original text, and the model's own `pii.detected` now judges the real text. **Treat `raw_value`, `justification`, `meta_data.extraction.responses` and the `feedbacks` table as INTERNAL ONLY.** What may be shared with DOTS or a dashboard is `value` / `value_list` and the masked copy (`meta_data.extraction.masking.conversation`, with its findings): the masking call and its placeholders are exactly as before and the masked copy never holds the original words (its findings drop a justification that repeats a word the masking took out). The model can still be told nothing it should not see only by switching `pii.extraction_text` to `masked`: with `original`, numbers, emails, handles and links of the respondent reach the AI provider in the extraction call (the masking call still reads the locally masked text).

7. One transaction writes the field rows to `feedback_extracted_data` (upsert: a re-run overwrites, a field that is no longer configured is deleted), sets `feedback_submissions.pii_detected`, merges an `extraction` object into `feedback_submissions.meta_data` (`config_version_id`, `prompt_version_id`, `pii_types`, `run_at`, and the `masking` object of step 6b when that step ran) and writes the `llm_logs` row (two rows when step 6b ran). `run_at` is the time the input was **loaded**, not the time of the write.
7a. **The complete answers of the calls.** The extraction object also holds `responses` (`{"masking": <text>, "extraction": <text>}`, the answers exactly as the model returned them), **for every session, flagged ones included** (the data stays in our system; the export query decides what leaves it). A re-run replaces the whole `extraction` object, so only the latest run's answers are kept, and a re-run that flags the session removes the older ones. The answer of a call that **could not be used** (not JSON, a missing key) is kept, cut to 4,000 characters (`response_truncated: true` when cut), in the `meta_data` of that call's **failed** `llm_logs` row, because nothing else is written for a failed session; a call that failed without an answer (timeout, refused) keeps none, and a good call's answer is not copied into `llm_logs`. Read them with `meta_data -> 'extraction' -> 'responses'` and `llm_logs.meta_data ->> 'response'`; never export them (they are free text).
7b. **One activity, two visible stages.** Temporal shows ONE activity (`feedback_metadata_extraction_activity`) for the step, with the two LLM calls inside it. The stage-by-stage view is a list `[{"name": "pii_masking" | "metadata_extraction", "status": "running" | "success" | "failed" | "skipped", "seconds", "prompt_tokens", "completion_tokens", "reason"}]` (no text) that is: (a) the activity's **heartbeat details**, updated after every change, visible in the Temporal UI on the pending activity while it runs and between retries (Temporal throttles heartbeats, so a very short stage can be missed live); (b) `stages` in the activity's **result** (the completed event in the workflow history); (c) the step's `stages` in `submissions.process_status`; (d) for a non-retryable failure, the failed stage in the error text (`... (stage: metadata_extraction)`) and the list in the error's details; (e) one INFO log line per stage change. `masking` is listed only when the config names a masking prompt; a session without a respondent answer makes no call and both stages are `skipped` with the reason. A `failed` `llm_logs` row is written only when a call was really made (a missing prompt or config, a template error or an oversized session costs nothing and writes no row).
8. **A session that grows during the LLM call is never overwritten with a stale flag.** The transaction first locks the `feedback_submissions` row, re-reads the messages and compares a content fingerprint (row count and a hash over sequence, parent link, question key, message and translation) with the one taken at load. If it differs **nothing is written** (the flag stays NULL, as the ingest left it) and the step fails with `StaleExtractionError`, which the workflow retries (3 attempts) on the new content; the wasted call is logged in `llm_logs` as `failed`.
9. A conversation with **no respondent answer** makes no LLM call: every field is `not_provided` and `pii_detected` is FALSE.

### Statuses and values (`feedback_extracted_data.status`, `value`, `value_list`)

**`value` is never empty.** It is a category or band label of the field, or one of the labels the engine writes itself (sentence case, one spelling for every program and field: `Not answered`, `N/A`, `Prefer not to say`, `Invalid response`, `Others`, `Unmapped`, `Unclear`).

| status | meaning | `value` | `raw_value` |
|---|---|---|---|
| `extracted` | the answer maps to an allowed value (or, for `district`, a name that is not a placeholder), also the answers `Prefer not to say`, `Others`, `Outside India`, `Unsure / don't know` | the category / band label, spelled as in the config; several joined by `, ` for a `multi` field | the respondent's own words as the model copied them from the ORIGINAL text (internal only; NULL for a sensitive field); for a banded number, the stated number |
| `unmapped` | the respondent answered but it needs a person's look: the model chose `Unmapped`, `Invalid response` or `Unclear`, or its value is outside the list; also every field of a session skipped for an oversized respondent message (value `Not answered`, nothing was extracted, the justification says why) | that label, or the field's fallback (`Unmapped` if the field has it, else `Others`, else `Invalid response`) | the respondent's words, or the rejected value when the model gave no words |
| `not_provided` | the question applies but there is no answer (skipped, a placeholder such as "N/A", or the session ended first) | `Not answered` | NULL |
| `not_applicable` | the question does not exist in the session's questionnaire version (field `versions`, decided by the code, the model is not asked), or an earlier answer makes it moot (field `applies_when`: for example `disability_type` unless `disability` is Yes) | `N/A` | NULL |

`value_list` (TEXT[]) always lists the individual labels of `value` (one for a single-value field, several for a `multi` field such as `current_activity_status`, `disability_type`, `child_gender`, `child_grade`, `teaching_level`): count categories with it (`unnest(value_list)`, or `'Studying' = ANY(value_list)`), export `value`. In a multi field the labels that stand alone (`Prefer not to say`, `Invalid response`, `Unclear`, `Unmapped`, `Unsure / don't know`, `Outside India`) give way to a real category; `Others` combines. The same combination is always the same text.

A category is matched ignoring case and repeated spaces and stored with the config's own spelling (`tamil nadu` becomes `Tamil Nadu`); other spellings are repaired only when the config lists them as `aliases` (`Tamilnadu`, `Orissa`, `New Delhi`, `NCT of Delhi`, `Delhi/NCR` ...). `confidence` and `justification` are stored only if the model returned them.

### The PII flag

`pii_detected` is `NULL` until the session was checked, then TRUE or FALSE, the same idea as `pii_masked` on story and discussion submissions. TRUE means the **respondent's words** contain something that could identify a person or an institution (name, phone, email, address, school name, id number, other); the prompt says to flag when in doubt, and the deterministic pre-check of step 6 adds numbers, emails, handles and URLs (and masks them in the text the model receives). State, district, gender, age, class, kind of school and income bands are not PII. The types found are in `feedback_submissions.meta_data -> 'extraction' -> 'pii_types'`. A response where the model says `detected: false` but lists types is rejected. Types the model names that are not in the config's `pii.types` list are recorded as `other`.

### Safety rules in the code

- The conversation is data, not instructions: the prompt says so, the conversation sits between delimiter lines carrying an id derived from its own text (a respondent cannot know it), every message is a single line (no forged `[n] Bot:` lines) and the template variables are filled in one pass (respondent text cannot inject a variable).
- Conversation text is never logged by this code and never in an error message. `llm_logs` stores tokens, status and the provider's usage only, not the prompt (the existing convention); the one exception is the cut answer of a call that gave no usable one (step 7a), kept so a bad answer can be read without paying for the call again. **Provider errors are sanitised**: a failed LLM call is re-raised as `LlmCallError` carrying only the exception class and, when it can be read from the message prefix, the HTTP status (`LLM call failed (RuntimeError, HTTP 429)`); the provider's error body, which can echo the prompt, reaches neither `llm_logs.error_message` nor the Temporal failure history. For any other unexpected error only the class name is stored. `openrouter_chat_completion` (`app/services/llm.py`, shared by every step) logs **only the model, the prompt size and, on an HTTP error, the status code**: neither the prompt (not even at DEBUG) nor the provider's error body is logged or put in the exception message. The opt-in setting `LLM_LOG_PAYLOADS=true` (default false, `.env.example`) restores the full prompt at DEBUG and the error body at ERROR for debugging; never switch it on where real respondents' data flows (the exception message stays clean even then).
- The CLI prints aggregates only.

## 2. Tables

- `feedback_extracted_data`: one row per session per field. Columns: `submission_id`, `tenant_code`, `field_name`, `value`, `raw_value`, `status`, `confidence`, `justification`, `config_version_id`, `prompt_version_id`, `extracted_at`. Unique on `(submission_id, tenant_code, field_name)`; deleted with the submission; index on `(field_name, value)`. It deliberately has **no `session_type` or `questionnaire_version` column**: copies would go stale when `tools/questionnaire_versions.py set` or `restamp` relabels sessions. Read both from `feedback_submissions` (every query below does).
- `feedback_submissions.pii_detected BOOLEAN` (new) and `feedback_submissions.meta_data -> 'extraction'`.
- `config` row per persona with `config_type = 'feedback_extraction'`, `config_source = 'any'` (it applies to both sources), `session_type`, `program_id`; versions in `config_version`.

## 3. The extraction config (`config_version.config`)

```json
{
  "schema_version": 1,
  "persona": "Youth",
  "session_type": "Youth_Voice",
  "prompt_name": "Feedback Metadata Extraction - Youth",
  "source_note": "free text",
  "fields": [
    {"name": "age", "kind": "integer", "description": "...", "min": 0, "max": 120, "categories": ["Prefer not to say", "Unclear", "Invalid response"],
     "bands": [{"label": "< 12", "min": null, "max": 12}, {"label": "≥ 12 and < 16", "min": 12, "max": 16}, {"label": "≥ 16", "min": 16, "max": null}]},
    {"name": "state", "kind": "category", "description": "...", "categories": ["Andhra Pradesh", "..."],
     "guidance": {"Delhi": "Delhi, New Delhi, NCT of Delhi"}, "aliases": {"Delhi": ["New Delhi", "NCT of Delhi", "Delhi/NCR"]}},
    {"name": "district", "kind": "free_text", "description": "...", "categories": ["Prefer not to say", "Unmapped", "Invalid response"]},
    {"name": "current_activity_status", "kind": "category", "description": "...", "categories": ["Studying", "Working", "..."], "multi": true},
    {"name": "monthly_household_income", "kind": "category", "description": "...", "categories": ["< ₹25,000", "..."], "sensitive": true, "versions": ["V2"]},
    {"name": "disability", "kind": "category", "description": "...", "categories": ["Yes", "No", "..."], "sensitive": true},
    {"name": "disability_type", "kind": "category", "description": "...", "categories": ["Physical disability", "..."], "multi": true,
     "sensitive": true, "applies_when": {"field": "disability", "in": ["Yes"]}}
  ],
  "pii": {"types": ["name", "phone", {"name": "id_number", "description": "a national id, a roll number"}, "other"],
          "min_digit_run": 8, "min_digit_run_short_groups": 10, "url_tlds": ["com", "in", "org"]},
  "limits": {"max_respondent_message_chars": 20000, "max_bot_message_chars": 4000, "max_conversation_chars": 200000,
             "max_value_chars": 120, "max_raw_chars": 500},
  "placeholders": ["n/a", "none", "not provided", "<the same words in the program's own language>"],
  "prompt_variables": {"programme_context": "free text for the program's prompt"},
  "llm": {"model": null, "max_tokens": null, "timeout_seconds": null}
}
```

- `prompt_name` (**required**): the exact `prompts.name` of this program's prompt: a non-empty string without surrounding spaces. A config without it is rejected when it is loaded (there is no default prompt, so a program can never be answered with another program's wording). It must resolve (see section 1, step 2): there is no fallback. `Feedback Metadata Extraction` in `seed_prompts.sql` is a program-neutral TEMPLATE: copy it, give the copy its own name and wording, and point the config at the copy.
- `prompt_variables` (optional): `{name: text}` of extra `{{name}}` variables for the program's prompt (for example the programme, the languages, what is not PII). Names match `[a-z][a-z0-9_]*`, may not be one of the engine's own variables, at most 20 entries of at most 4,000 characters. They are filled in the same single pass as the built-in ones, so a value that contains `{{conversation}}` stays literal text.
- `pii.types` (optional): a list of lower-case names or of `{"name", "description"}` objects (mixed is fine), unique, including `other`. The description is what the model is told the type means; a plain name takes the neutral built-in description when it is one of the default seven, else it is rendered `as named`. `pii.min_digit_run` (4-30, default 8), `pii.min_digit_run_short_groups` (4-30, default 10, never below `min_digit_run`) and `pii.url_tlds` (lower-case letters-only tlds; an empty list leaves only `http(s)://` and `www.` URLs) set the deterministic backstop of section 1, step 6. `pii.extraction_text` (optional, `original` (default) or `masked`): what the extraction call reads, see section 1, step 6c. `pii.masking_prompt_name` (optional, string or null): the exact `prompts.name` of the model-based masking prompt of section 1, step 6b (`analysis_type` `feedback_pii_and_abusive_language_detection`); absent or null makes no masking call. `pii.mask_before_call` (boolean, default `true`) decides whether what that check finds is replaced by a placeholder before the text is sent to the model; `false` checks only after the call.
- `limits` (optional, every key optional, bounds are checked): `max_respondent_message_chars` (default 20,000), `max_bot_message_chars` (4,000), `max_conversation_chars` (200,000: a longer conversation is treated like an oversized message, no call), `max_value_chars` (120, a longer free-text value is `unmapped`), `max_raw_chars` (500). The defaults are the values the engine always used, so an older config keeps its behaviour.
- `placeholders` (optional): the words that mean "no answer" for a free-text or integer field, and for a category field when the model copies one of them into `value` instead of null (`skip`, `idk`, `none`, `NA` become `Not answered`, never `Others`; a category of the field itself such as `Not sure` or `Don't know` wins first), compared case-insensitively ignoring spaces and surrounding punctuation. It **replaces** the built-in English list, so a program whose respondents answer in another language lists its own words next to the English ones.
- `kind`: `category` (needs `categories`, each unique ignoring case; optional `guidance` per category, shown to the model), `free_text` (the value is taken as given; a value longer than `limits.max_value_chars` (120) or starting with `=`, `+`, `-`, `@` is treated as `unmapped`) or `integer` (needs integer `min` and `max`, `min <= max`, booleans rejected; the model may return a JSON number or a numeric string; inside the range it is stored as the canonical decimal string, for example `"17"`; outside it, not a whole number (`17.5`, `17 years`) or a boolean it is `unmapped` with the raw kept; `null` is `not_provided`). `min` / `max` are not allowed on the other kinds, `categories` / `guidance` only on `category`.
- `categories` / `guidance` / `aliases` / `multi` / `bands` / `versions` / `applies_when` (all optional; the data teams' finalised lists are in `docs/feedback-extraction-bands.md`): `categories` are the allowed values in the **normalised spelling** (sentence case) and must **not** contain `Not answered` or `N/A` (the engine writes those); on a `free_text` field they are the *special answers* the model may give instead of a name (`Prefer not to say`, `Unmapped` ...), on an `integer` field with `bands` they are its special answers (`Prefer not to say`, `Unclear`, `Invalid response`); `aliases` is `{category: [other spellings]}` matched by the code (case and spacing ignored; an alias may not mean two categories); `multi: true` (category fields only) lets the answer be several categories (no comma in any label); `bands` is a list of `{label, min, max}` for an `integer` field, contiguous, the first open below (`min` null) and the last open above (`max` null), `min` inclusive and `max` exclusive, and the number is stored as the band's label (decimals allowed); `versions: ["V2"]` makes the field exist in those questionnaire versions only (any other version: `N/A`, the model is not asked); `applies_when: {"field": <an earlier field>, "in": [<its values>]}` makes the field `N/A` unless the earlier field holds one of them (a multi field counts when any of its labels does).
- `provisional: true` marks bands product has not confirmed; it has no effect on the processing.
- `sensitive: true` (optional, any kind, default false): adds the rule line `(sensitive: fill only when the respondent states it explicitly; never infer)` under the field in the prompt text the model sees, and the code **does not store the respondent's words** of that field: `raw_value` is NULL (the band or value and the status are stored as usual; an `unmapped` sensitive answer keeps its status but not its words). `store_raw: true` on the field keeps `raw_value` (an explicit product decision); `store_raw: false` switches it off for any other field. Validation is unchanged. Note that the **`value` of a sensitive `free_text` field is the extracted answer itself** and is stored; keep such columns out of an export until product confirms (section 6 marks them). The field is still sent to the model: `sensitive` limits what is stored, not what the provider sees.
- A free-text value that is a placeholder (see `placeholders`) is `not_provided`; longer than `limits.max_value_chars` or starting with `=`, `+`, `-` or `@` is `unmapped`. The same placeholders as an integer answer are `not_provided`.
- The config is validated when it is read (unknown keys, duplicate names, bad kinds or bounds are rejected, never defaulted) and when the seed runs. `llm` values are optional; a workflow step override wins over them. The block applies to **both** calls of the activity (masking and extraction), and `max_tokens` is only a ceiling (tokens are paid when generated). **The four seeded configs set `llm.max_tokens: 10000`**: the masking call writes every answer out again, and a long Kannada or Hindi session needs more than the 2048-token `.env` default (31 of 455 real General Public sessions were cut off at 2048 and failed with `the masking answer is not valid JSON`; the output size follows the input size at roughly 1.1 output tokens per input token minus 1,300, so the largest session seen needs about 4,300, so 10,000 leaves a wide margin). To change it on a live database write a new `config_version`, or in development update the `llm` of the v1 row (see the recipe below).
- `copy_to_submission` (optional, top level): `{"state": "state", "district": "district"}` copies the extracted state and district of a session into `submissions.state` / `submissions.district` (the columns the platform's own reports use), in the **same transaction** as the extraction rows. Keys are the submissions columns (only `state` and `district`), values are single-value `category` or `free_text` fields of the same config; a district needs a state mapping. All four seeded personas set it. What is written: a value only when its field is `extracted` and is a real place; **`Prefer not to say`, `Unmapped`, `Invalid response`, `Outside India`, `Others`, `Not answered`, `N/A` and `Unsure / don't know` leave the column NULL**; a district only when the state is valid too (a district next to an unusable state is NULL); a state derived from a named district counts as a state. Every extraction overwrites both columns (a later answer that is not valid clears them, so they never disagree with `feedback_extracted_data`). A session flagged `pii_detected = TRUE` keeps its place (the place is not PII; the flag only governs what an export may contain). A reset (persona change, or a questionnaire relabel that makes the session go through extraction again) sets both columns to NULL until the new extraction writes them; a relabel that only renames the label keeps them. A config without the key never touches the columns. **Check before relying on it:** the state values are the 36 official names of the finalised list; confirm they equal the names the platform uses (`select state, count(*) from submissions where state is not null group by 1 order by 1;` on a database with stories), and note that `district` has no reference list, so spelling variants of one district can appear.
- Show what is live: `python tools/run_feedback_extraction.py show-config [--session-type Youth_Voice]` prints, per persona, its `prompt_name` (`(none)` for a config that names none, which cannot run), each field with its kind and `sensitive` / `provisional` flags, and the full config JSON.

### The columns of each persona (field names are the DOTS column names)

| persona (`session_type`) | fields (S = `sensitive`) |
|---|---|
| Youth (`Youth_Voice`) | `gender`, `age`, `state`, `district`, `current_activity_status`, `education_level`, `course_details`, `institution_type`, `monthly_household_income` (S, V2 only), `annual_household_income` (S, V1 only), `disability` (S), `disability_type` (S) |
| General Public (`General_Public_Voice`) | `gender`, `age`, `state`, `district`, `occupation` |
| Parent (`Parent_Voice`) | `state`, `district`, `child_gender`, `disability` (S), `disability_type` (S), `respondent_relationship`, `school_type`, `child_grade`, `parent_education`, `parent_education_level` |
| Educator (`Educator_Voice`) | `state`, `district`, `gender`, `institution_type`, `teaching_level` |

Parent uses `child_gender` (the CHILD's gender), not `gender`. The allowed values (bands), the `provisional` flags and the open product questions of each field are in `docs/feedback-extraction-bands.md`; the live truth is the config row (`show-config`). Two things that hold for every persona: `state` is the 28 states and 8 union territories (official names, spelling variants in the guidance; a place that is not an Indian state or union territory is `unmapped`), and `district` is free text normalised by the model with **no reference validation** (there is no district reference list yet; open question H2 in `docs/katha-stories-vs-dots-data-review.md`).

## 4. Changing a band (or a description) = a new config version

Seeds never overwrite. To change a persona's bands, add a new config version with the full edited JSON and make it the active one. Rollback is re-activating an older version.

```sql
-- 1. find the config: SELECT id FROM config WHERE config_type = 'feedback_extraction' AND session_type = 'Youth_Voice';
BEGIN;
UPDATE config_version SET is_active = FALSE, updated_at = now()
WHERE config_id = '<config id>' AND is_active;
INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT config_id, max(version) + 1, TRUE, '<edited JSON from show-config>'::jsonb, 'your name', 'what changed and why'
FROM config_version WHERE config_id = '<config id>' GROUP BY config_id;
COMMIT;
```

**The seeded config is data in `seed_config.sql`, inserted with `ON CONFLICT DO NOTHING`.** The fields, bands and prompt name of each persona are the JSON of the block `-- CONFIG feedback_extraction any <session_type>` of `seed_config.sql` (repo root); there is no Python copy. A change to that file (new fields, a new prompt name, corrected descriptions) reaches a **new** database only: a database that already holds the config keeps its version, because version 1 is only inserted while the config has no version at all. **To bring an existing database to the content of the file, write a new `config_version` with the recipe above**, using the JSON of the block (keep the active version's `llm` block if one was set): it deactivates the active version (history is kept, so re-activating the old version is the rollback) and writes the next one. There is no `upgrade-config` command any more; `show-config` prints what the database holds now. **It does not touch the prompts**: the persona prompts are inserted by `seed_prompts.sql` at startup, and a config only works once the prompt it names exists (otherwise a run fails loudly). Re-run the sessions afterwards (`run` without `--only-unprocessed`); their rows record the `config_version_id` and `prompt_version_id` they were produced with. Step by step, per persona: `docs/feedback-extraction-bands.md`, "How an existing database picks this up".

For a one-off change to a single persona by hand the manual recipe above still works: take the JSON from `show-config`, edit it and insert it as the next `config_version`.

### Changing a persona's prompt (a new `prompt_version` row, no deploy)

**The extraction prompts are protected the same way**: `seed_prompts.sql` inserts each prompt's version 1 with `ON CONFLICT DO NOTHING`, so a manual edit survives a restart. The prompt is the PII logic: **to change a persona's prompt, insert a new `prompt_version` of that persona's prompt (the next version number, `is_active = TRUE`) and deactivate the previous one**, do not edit it in place. The engine reads the highest active version of the exact `prompts.name` on the next run; no deploy and no config change are needed. Take the name from `show-config` (`prompt_name`):

```sql
BEGIN;
UPDATE prompt_version SET is_active = FALSE
WHERE prompt_id = (SELECT id FROM prompts WHERE name = 'Feedback Metadata Extraction - Youth') AND is_active;
INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note)
SELECT p.id, (SELECT max(version) + 1 FROM prompt_version WHERE prompt_id = p.id),
       '<system prompt>', '<user prompt>', TRUE, 'what changed and why'
FROM prompts p WHERE p.name = 'Feedback Metadata Extraction - Youth';
COMMIT;
```

A prompt is free-form text that may use these template variables plus the config's own `prompt_variables` (anything else fails the render, nothing is substituted twice): `{{persona}}`, `{{fields}}` (the persona's field list rendered from its config, with allowed values, integer ranges and the sensitive rule line), `{{field_names}}`, `{{pii_types}}`, `{{output_format}}` (the JSON shape the answer must have), `{{conversation_id}}` and `{{conversation}}`. Keep the security rules and the PII check in it: the answer is still validated strictly whatever the prompt says. To point a persona at a **different prompt** create the `prompts` row (`analysis_type = 'feedback_metadata_extraction'`, the exact `name`) with an active version first, then add a config version whose `prompt_name` is that name; a persona config without `prompt_name` uses `Feedback Metadata Extraction`.

Take the JSON from `show-config`, edit it, and keep it valid: a `guidance` entry for a category you removed is rejected (the config is validated when it is read), so remove it too. Every extracted row records the `config_version_id` it used, so you can tell which rows predate a change.

## 5. Running it

- **In the workflow**: a new database is seeded with the step in every upload config. On an existing database whose `process_steps` is `[]` (an old config, or one you emptied), extraction does not run until you enable it per persona and per source. For Mitra Youth:

```
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key process_steps --json '[{"name":"feedback_metadata_extraction"}]'
```

  and the same with `--source google_forms` for the Google Forms sessions of that persona (optional step keys `llm_model`, `max_tokens`, `llm_timeout_seconds`). **Every stored session of that persona then costs one real LLM call**, including each re-processing of a session that grew.
- **Trying it**: use the CLI below on one or two sessions (`--limit 2`) before enabling the workflow step; do not run it persona-wide to "see what happens".
- **By hand, without Temporal** (needs `OPENROUTER_API_KEY`; sessions go to an external provider):

```
python tools/run_feedback_extraction.py run --session-type Youth_Voice --tenant-code <tenant> --limit 2                 # a trial: 2 real LLM calls
python tools/run_feedback_extraction.py run --session-type Youth_Voice --tenant-code <tenant> --dry-run --limit 20     # LLM called, nothing written
python tools/run_feedback_extraction.py run --session-type Youth_Voice --tenant-code <tenant> --only-unprocessed        # sessions with pii_detected NULL
python tools/run_feedback_extraction.py run --session-type Youth_Voice --tenant-code <tenant> --submission-id 12345     # one session
python tools/run_feedback_extraction.py run --session-type Parent_Voice --tenant-code <tenant> --program-id <program uuid>  # one program (a session_type can exist in two programs)
python tools/run_feedback_extraction.py run --session-type Parent_Voice --tenant-code <tenant> --concurrency 8 --model <model>
python tools/run_feedback_extraction.py show-config
```

  The output is aggregates only: counts per field and status, the most frequent canonical values of the category fields (`--top`; for a free-text field such as `district` only the number of distinct values, never the values; for an `integer` field such as `age` the number of distinct values and the smallest and largest value, never the values themselves), PII counts and types, token usage, error types.
- **Retries**: the workflow retries a failed extraction step up to 3 times (the activity timeout follows the LLM timeout: `llm_timeout_seconds` of the step, else the global one, plus 120 s, never below 5 minutes and never above 60 minutes; a config `llm.timeout_seconds` is limited to 3,480 s. The workflow cannot read the database, so it sizes the activity from the step's `llm_timeout_seconds` or the global setting only: a longer config timeout is **clamped** to the activity limit minus 120 s (config 200 s with no step override: limit 300 s, so the call runs with 180 s) and a WARNING names the two numbers; to give a longer call more time, set `llm_timeout_seconds` on the workflow step. The time the step already used before the call (connection, status update, loading the session and the prompt) is subtracted as well, so the call always ends before the 120 s margin that the write needs. Only an activity limit with nothing left after the 120 s margin fails the step before any call, and that is non-retryable; if a slow database stall used up the budget and less than 30 s remain for the call, no call is made and the step is **retried** with a fresh budget (nothing was billed or written)). Failures that cannot get better are **non-retryable** (no repeated paid calls): a missing or malformed extraction config, a missing prompt or an unknown prompt variable, a session that is not stored, and a provider answer with a 4xx status other than 408/409/425/429. Retried: provider 5xx and 429, timeouts, a session that grew during the call, and a bad model answer **once**. A `failed` `llm_logs` row exists only for real calls. A bulk re-run (the CLI or a sweep of failed submissions) calls the model again for every session it selects, so fix the configuration first.
- **Re-run after a config or prompt change**: run without `--only-unprocessed`; every selected session is re-read (one LLM call each) and its rows are overwritten.
- **A session that grew** goes back to `pii_detected = NULL` when the new messages arrive (also when only a parent link changed) and is re-processed by the existing pending logic. If it grows while a call is running, the write is refused and retried (section 1, step 8). A flag that survives from before a manual change to `feedbacks` is caught by the staleness check in section 6.7.

## 6. Postgres queries for the DOTS export

All queries read `feedback_extracted_data` (long form) and pivot it to **one row per submission**. Each persona has its own query (6.1 to 6.4) with its own columns. They **exclude `pii_detected = TRUE`** and, in the export-ready form, also the sessions that were not checked yet (`pii_detected IS NULL`): only `pii_detected = FALSE` rows are exported. `value` is the label of the category or band (never empty: `Not answered`, `N/A`, `Unmapped` ... included; for `age` the band label, the stated number is `raw_value`; a multi-value field shows its labels joined by `, `). The **`<field>_status` columns** tell them apart: `extracted` (the value is set), `not_provided` (the respondent never answered, or answered only a placeholder such as "N/A"), `unmapped` (answered, but the words fit no allowed value: for example a non-Indian place or an unclear state, since `state` has no `Others` band, or an age outside the allowed range; see 6.9 for the words), and NULL status = no extraction row for that field at all (the persona has no such field, or the config changed). `session_type` and `questionnaire_version` are read from `feedback_submissions`, so they follow a relabel. **How `questionnaire_version` is set:** for a Mitra bot session (adapter `conversation_rows`) it is the rank of its bot id inside its persona (tenant, program, `session_type`, source; `meta_data ->> 'bot_id'`, ranked by number): the lowest bot id is `V1`, the next `V2`, and so on, so the label depends on which bots exist, not on dates. When a bot id lower than the stored ones arrives the others shift (bot 103 is `V1` until bot 89 appears, then `V2`); the shifted sessions are relabelled in the ingest transaction and go back through extraction (their extracted rows are deleted, `pii_detected` is NULL and the status is `pending`), so for a moment they show up in 6.5 and not in the export-ready rows. A Google Forms response (adapter `wide_form`) carries the version of its form from the config.

### 6.1 Export-ready rows: Youth (PII-checked and clean)

`age` is the label of its band (`≥ 16 and < 22`, ...). `monthly_household_income`, `annual_household_income`, `disability` and `disability_type` are **sensitive**: they are in the commented-out block, **do not include them in a DOTS export until product confirms** (see section 7). They are still extracted and stored.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.age,
       e.age_status,
       e.gender,
       e.gender_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.current_activity_status,
       e.current_activity_status_status,
       e.education_level,
       e.education_level_status,
       e.course_details,
       e.course_details_status,
       e.institution_type,
       e.institution_type_status
       -- SENSITIVE columns: do NOT export until product confirms. To include them, remove the leading '-- ' of the
       -- lines below here and in the LATERAL list further down (the comma is part of each commented line).
       -- , e.monthly_household_income
       -- , e.monthly_household_income_status
       -- , e.annual_household_income
       -- , e.annual_household_income_status
       -- , e.disability
       -- , e.disability_status
       -- , e.disability_type
       -- , e.disability_type_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'age') AS age,
           max(d.status) FILTER (WHERE d.field_name = 'age') AS age_status,
           max(d.value)  FILTER (WHERE d.field_name = 'gender') AS gender,
           max(d.status) FILTER (WHERE d.field_name = 'gender') AS gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'current_activity_status') AS current_activity_status,
           max(d.status) FILTER (WHERE d.field_name = 'current_activity_status') AS current_activity_status_status,
           max(d.value)  FILTER (WHERE d.field_name = 'education_level') AS education_level,
           max(d.status) FILTER (WHERE d.field_name = 'education_level') AS education_level_status,
           max(d.value)  FILTER (WHERE d.field_name = 'course_details') AS course_details,
           max(d.status) FILTER (WHERE d.field_name = 'course_details') AS course_details_status,
           max(d.value)  FILTER (WHERE d.field_name = 'institution_type') AS institution_type,
           max(d.status) FILTER (WHERE d.field_name = 'institution_type') AS institution_type_status
           -- SENSITIVE (see above):
           -- , max(d.value)  FILTER (WHERE d.field_name = 'monthly_household_income') AS monthly_household_income
           -- , max(d.status) FILTER (WHERE d.field_name = 'monthly_household_income') AS monthly_household_income_status
           -- , max(d.value)  FILTER (WHERE d.field_name = 'annual_household_income') AS annual_household_income
           -- , max(d.status) FILTER (WHERE d.field_name = 'annual_household_income') AS annual_household_income_status
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability') AS disability
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability') AS disability_status
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability_type') AS disability_type
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability_type') AS disability_type_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'Youth_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.2 Parent

`child_gender` is the CHILD's gender; `school_type` and `child_grade` are the child's. `disability` and `disability_type` are **sensitive** and commented out: **do not include them in a DOTS export until product confirms**.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.child_gender,
       e.child_gender_status,
       e.respondent_relationship,
       e.respondent_relationship_status,
       e.school_type,
       e.school_type_status,
       e.child_grade,
       e.child_grade_status,
       e.parent_education,
       e.parent_education_status,
       e.parent_education_level,
       e.parent_education_level_status
       -- SENSITIVE columns: do NOT export until product confirms. To include them, remove the leading '-- ' of the
       -- lines below here and in the LATERAL list further down (the comma is part of each commented line).
       -- , e.disability
       -- , e.disability_status
       -- , e.disability_type
       -- , e.disability_type_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'child_gender') AS child_gender,
           max(d.status) FILTER (WHERE d.field_name = 'child_gender') AS child_gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'respondent_relationship') AS respondent_relationship,
           max(d.status) FILTER (WHERE d.field_name = 'respondent_relationship') AS respondent_relationship_status,
           max(d.value)  FILTER (WHERE d.field_name = 'school_type') AS school_type,
           max(d.status) FILTER (WHERE d.field_name = 'school_type') AS school_type_status,
           max(d.value)  FILTER (WHERE d.field_name = 'child_grade') AS child_grade,
           max(d.status) FILTER (WHERE d.field_name = 'child_grade') AS child_grade_status,
           max(d.value)  FILTER (WHERE d.field_name = 'parent_education') AS parent_education,
           max(d.status) FILTER (WHERE d.field_name = 'parent_education') AS parent_education_status,
           max(d.value)  FILTER (WHERE d.field_name = 'parent_education_level') AS parent_education_level,
           max(d.status) FILTER (WHERE d.field_name = 'parent_education_level') AS parent_education_level_status
           -- SENSITIVE (see above):
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability') AS disability
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability') AS disability_status
           -- , max(d.value)  FILTER (WHERE d.field_name = 'disability_type') AS disability_type
           -- , max(d.status) FILTER (WHERE d.field_name = 'disability_type') AS disability_type_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'Parent_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.3 General Public

No sensitive columns. `age` is the label of its band.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.age,
       e.age_status,
       e.gender,
       e.gender_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.occupation,
       e.occupation_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'age') AS age,
           max(d.status) FILTER (WHERE d.field_name = 'age') AS age_status,
           max(d.value)  FILTER (WHERE d.field_name = 'gender') AS gender,
           max(d.status) FILTER (WHERE d.field_name = 'gender') AS gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'occupation') AS occupation,
           max(d.status) FILTER (WHERE d.field_name = 'occupation') AS occupation_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'General_Public_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.3.1 General Public: the DOTS export (the columns of the DOTS sheet)

One row per session with the DOTS columns `session_id, created_at, status, source, age, gender, state, district, occupation, question_answer`. The metadata columns are the `value` of `feedback_extracted_data` (`age` is the band label). `question_answer` is a JSON object, in conversation order, of every question and the respondent's **masked** answer, read from the model-masked copy `meta_data.extraction.masking.conversation`, **never** from `feedbacks` or `raw_value` (those hold the original words and stay internal). Rules built into the query: only sessions checked and clean (`pii_detected = FALSE`, and the masking record's own `pii_types` is empty: two independent guards, a session with no masking record is left out) whose masked copy exists; a session flagged for PII is left out **entirely** (no row, not even its metadata); the flag cannot be stale, because new or edited messages reset it to NULL until the session is checked again; the opening and thank-you messages are left out (as in the DOTS sample); a question asked twice is keyed `question (2)`; sessions where nothing but the greeting was answered are dropped; respondents below 12 are excluded (`age` band `< 12`); `created_at` is shown in IST; `source` is `Mitra` / `Google Form`. A non-English answer reads `<original> (English translation: <english>)`, as the extraction saw it. Add the commented `completion_status = 'COMPLETED'` line for finished conversations only. Measured on the 455-session General Public test upload: 433 clean, minus 19 greeting-only, minus 6 under 12 = 408 rows; no e-mail, phone-length number, handle, pin code or link in any answer. Three clean sessions have abusive language, masked as `<INSULT>`, `<PROFANITY>`, `<THREAT>` (abuse is not PII); the commented `abusive_language` line leaves them out.

```sql
SELECT s.session_id,
       to_char(s.submission_date AT TIME ZONE 'Asia/Kolkata', 'YYYY-MM-DD HH24:MI:SS') AS created_at,
       fs.completion_status                                                             AS status,
       CASE fs.source WHEN 'mitra' THEN 'Mitra' WHEN 'google_forms' THEN 'Google Form' ELSE fs.source END AS source,
       e.age, e.gender, e.state, e.district, e.occupation,
       qa.question_answer
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
JOIN LATERAL (
    SELECT max(d.value) FILTER (WHERE d.field_name = 'age')        AS age,
           max(d.value) FILTER (WHERE d.field_name = 'gender')     AS gender,
           max(d.value) FILTER (WHERE d.field_name = 'state')      AS state,
           max(d.value) FILTER (WHERE d.field_name = 'district')   AS district,
           max(d.value) FILTER (WHERE d.field_name = 'occupation') AS occupation
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
JOIN LATERAL (
    -- every question with the respondent's MASKED answer, in conversation order (the opening and thank-you messages left out)
    SELECT json_object_agg(t.question_key, t.answer ORDER BY t.seq)::text AS question_answer
    FROM (
        SELECT u.seq, u.answer,
               CASE WHEN row_number() OVER (PARTITION BY u.question ORDER BY u.seq) = 1 THEN u.question
                    ELSE u.question || ' (' || row_number() OVER (PARTITION BY u.question ORDER BY u.seq) || ')'
               END AS question_key
        FROM (
            SELECT c.key::int AS seq,
                   c.value ->> 'answer' AS answer,
                   -- the age question of the first message carries the greeting in front of it: keep the question only
                   regexp_replace(c.value ->> 'question', '^Hello!.*?(Can you please tell me your age\?|How old are you\?)$', '\1') AS question
            FROM jsonb_each(fs.meta_data #> '{extraction,masking,conversation}') AS c
            JOIN feedbacks f ON f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code AND f.sequence = c.key::int
            WHERE f.question_key NOT IN ('OPENING', 'THANK_YOU')
        ) u
    ) t
) qa ON TRUE
WHERE fs.tenant_code = 'mitra'
  AND fs.session_type = 'General_Public_Voice'
  AND fs.pii_detected = FALSE                                                    -- checked and clean
  AND jsonb_typeof(fs.meta_data #> '{extraction,masking,conversation}') = 'object'  -- the masked copy exists
  AND jsonb_array_length(fs.meta_data #> '{extraction,masking,pii_types}') = 0     -- second guard: the masking record itself found no PII (a missing record excludes the row)
  -- AND fs.abusive_language IS NOT TRUE                                           -- optional: also leave out sessions with abusive language (masked as <INSULT>, <PROFANITY> ...)
  AND qa.question_answer IS NOT NULL                                             -- at least one question answered (not only the greeting)
  AND e.age <> '< 12'                                                            -- not eligible: below 12
  -- AND fs.completion_status = 'COMPLETED'                                      -- optional: finished conversations only
ORDER BY s.submission_date, s.submission_id
```

### 6.4 Educator

No sensitive columns.

```sql
SELECT s.submission_id, s.submission_date, fs.session_type, fs.source, fs.questionnaire_version,
       fs.completion_status,
       e.state,
       e.state_status,
       e.district,
       e.district_status,
       e.gender,
       e.gender_status,
       e.institution_type,
       e.institution_type_status,
       e.teaching_level,
       e.teaching_level_status
FROM feedback_submissions fs
JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
LEFT JOIN LATERAL (
    SELECT max(d.value)  FILTER (WHERE d.field_name = 'state') AS state,
           max(d.status) FILTER (WHERE d.field_name = 'state') AS state_status,
           max(d.value)  FILTER (WHERE d.field_name = 'district') AS district,
           max(d.status) FILTER (WHERE d.field_name = 'district') AS district_status,
           max(d.value)  FILTER (WHERE d.field_name = 'gender') AS gender,
           max(d.status) FILTER (WHERE d.field_name = 'gender') AS gender_status,
           max(d.value)  FILTER (WHERE d.field_name = 'institution_type') AS institution_type,
           max(d.status) FILTER (WHERE d.field_name = 'institution_type') AS institution_type_status,
           max(d.value)  FILTER (WHERE d.field_name = 'teaching_level') AS teaching_level,
           max(d.status) FILTER (WHERE d.field_name = 'teaching_level') AS teaching_level_status
    FROM feedback_extracted_data d
    WHERE d.submission_id = fs.submission_id AND d.tenant_code = fs.tenant_code
) e ON TRUE
WHERE fs.tenant_code = 'mitra' AND fs.session_type = 'Educator_Voice' AND fs.pii_detected = FALSE
ORDER BY s.submission_date, s.submission_id;
```

### 6.5 The sessions that were not checked yet (variant: `pii_detected IS NULL`)

These are not in the export-ready rows. Run the extraction for them, or list them:

```sql
SELECT fs.session_type, fs.questionnaire_version, count(*) AS unchecked_sessions
FROM feedback_submissions fs
WHERE fs.tenant_code = 'mitra' AND fs.pii_detected IS NULL
GROUP BY fs.session_type, fs.questionnaire_version
ORDER BY fs.session_type, fs.questionnaire_version;
```

To list the rows themselves (one persona), use 6.1 with `fs.pii_detected IS NULL` in place of `fs.pii_detected = FALSE`; its field columns will be empty.

### 6.6 How many sessions are excluded, per persona

```sql
SELECT fs.session_type,
       count(*) FILTER (WHERE fs.pii_detected = FALSE) AS exported,
       count(*) FILTER (WHERE fs.pii_detected = TRUE)  AS excluded_pii,
       count(*) FILTER (WHERE fs.pii_detected IS NULL) AS not_checked
FROM feedback_submissions fs
WHERE fs.tenant_code = 'mitra'
GROUP BY fs.session_type
ORDER BY fs.session_type;
```

### 6.7 Stale flags: sessions that grew after their extraction

A session that gained messages after its extraction is re-processed later; until then its `pii_detected` describes the older text. Exclude these as well in a strict export:

```sql
SELECT fs.submission_id, fs.session_type, fs.pii_detected,
       (fs.meta_data -> 'extraction' ->> 'run_at')::timestamptz AS extracted_at,
       (SELECT max(f.updated_at) FROM feedbacks f
         WHERE f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code) AS last_message_change
FROM feedback_submissions fs
WHERE fs.tenant_code = 'mitra' AND fs.pii_detected IS NOT NULL
  AND (fs.meta_data -> 'extraction' ->> 'run_at')::timestamptz <
      (SELECT max(f.updated_at) FROM feedbacks f
        WHERE f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code)
ORDER BY fs.session_type, fs.submission_id;
```

### 6.8 Review: statuses per persona and field

```sql
SELECT fs.session_type, d.field_name, d.status, count(*) AS sessions
FROM feedback_extracted_data d
JOIN feedback_submissions fs ON fs.submission_id = d.submission_id AND fs.tenant_code = d.tenant_code
WHERE d.tenant_code = 'mitra'
GROUP BY fs.session_type, d.field_name, d.status
ORDER BY fs.session_type, d.field_name, d.status;
```

### 6.9 Review: the unmapped answers of one field (raw words are respondent text: analysts only, not for sharing; none for a sensitive field or a PII-flagged session)

```sql
SELECT d.submission_id, fs.questionnaire_version, d.field_name, d.raw_value
FROM feedback_extracted_data d
JOIN feedback_submissions fs ON fs.submission_id = d.submission_id AND fs.tenant_code = d.tenant_code
WHERE d.tenant_code = 'mitra' AND fs.session_type = 'Youth_Voice' AND d.field_name = 'state'
  AND d.status = 'unmapped' AND fs.pii_detected = FALSE
ORDER BY d.raw_value;
```

## 7. Caveats and what product still has to confirm (`docs/katha-stories-vs-dots-data-review.md`, `docs/feedback-extraction-bands.md`)

Confirmation items found in review. Until product answers, the affected fields are marked `provisional` in the config (see `show-config` or the bands document for which) and a change is a new config version plus a re-run:

- **Sensitive columns are not for the DOTS export yet.** `monthly_household_income` and `annual_household_income` (Youth), `disability` and `disability_type` (Youth and Parent) are flagged `sensitive` in the config: they are extracted only when the respondent states them explicitly, the respondent's own words are not stored for them (`raw_value` is NULL; the labels are stored), and they are in the commented-out block of the queries in 6.1 and 6.2. **Do not include them in a DOTS export until product confirms** that they may be exported.
- **State**: `Unmapped` (a direction, a region, a city, or a district name used in several states with no state given) and `Outside India` are categories of `state` and `district`; `not_provided` (value `Not answered`) and `unmapped` stay apart in the `status` column. A state is derived from a named district only when the district belongs to exactly one state (the justification says so).
- **Gender**: an explicit `do not want to say` is `Prefer not to say`; a skip is `Not answered`; `something else` is `Others`; `Unsure / don't know` is its own value.
- **School and institution type**: the Youth bot asks everybody, including people who work (about their past school); per the data team's comments an answer about a college is `Invalid response` (the question is about schools), also for Youth and Educator, and an aided school is `Others` for Parent and Educator (Youth has `Government-aided`). The justification of such a value says why.
- **Parent child gender** is `Male` / `Female` here; story H5 says `Boy` / `Girl`.
- **Parent state and district: whose? (H4)** The Mitra V2 bot asks for the **child's** state and district, the Google Form asks for the parent's own; the extraction takes the state and district the bot asked for, so the two sources may mean different places.
- **No district reference list.** Districts are free text, normalised by the model, not validated; one district can be spelled several ways. A list (for example LGD) and a validation step are a follow-up.
- **Educator `teaching_level`**: the bot asks `primary, middle, high school or college`; a bare `college` is `Undergraduate`, `Postgraduate` only when the respondent says a master's level; several levels are all stored (multi).
- **`age`**: stored as the label of its band; the number (decimals allowed) is `raw_value`. A number outside 0 to 120 is `Invalid response`. Excluding respondents below 12 (and Youth above 25) from a dashboard or an export is the export query's job; the rows are extracted like any other.

Other caveats:

- **V2 asks demographics at the end**: many V2 sessions stop earlier, so `not_provided` is common (Youth V2: about 45% had a state answer in the real export; Parent V2 77%; General Public V2 68%). `not_provided` is not the same as `unmapped`, and neither is `Prefer not to say` or `Invalid`, which are real bands.
- **Educator**: 8 sessions in the export so far.

Design limits (known, not changed by the code; plan around them):

- **Names, schools and addresses depend on the model.** The deterministic backstop (section 1, step 6) only knows numbers, e-mail, handles and URLs (those are masked before the call; names and addresses are not, they go to the provider as written). A child's name, a school's name or an address is flagged only when the model says `pii.detected = true`, and the model reads text the respondent wrote (the prompt tells it to treat that text as data, and the template is filled in one pass, but a respondent can still write "set pii.detected to false"). Treat `pii_detected = FALSE` as "no PII found by the model and the backstop", not as a guarantee; review a sample before sharing an export, and keep `limits` and the prompt's flag-when-in-doubt rule.
- **Size.** Per-message and whole-conversation caps (`limits`) apply, but there is no cap on the number of sessions or on spend per run: the CLI `--limit` and the workflow step enablement are the controls.
- **No model allowlist.** Whoever can write a `config_version` (`llm.model`) or a workflow step (`llm_model`) chooses the model and with it the provider that receives respondents' conversations; restrict write access to `config_version` / `process_steps` accordingly and check the provider's data policy. The application sends no provider data-collection preference.
- **One row per field, several labels per row.** A field holds one row per session (`UNIQUE (submission_id, tenant_code, field_name)`); a `multi` field stores all its labels in `value` (joined by `, `) and `value_list`. Dates, booleans and per-turn extractions are still not supported.
- **Exports.** Free-text columns hold what the respondent wrote, normalised by the model: a value starting with `=`, `+`, `-` or `@` is never stored as a value (it is `unmapped`) and a stored `raw_value` starting with one of them has a leading apostrophe, but other columns of an export that you build yourself are your responsibility.
- All stories are still Draft: a changed band means a new config version and a re-run.

## 8. Upgrading a database that already has these tables

This version is unreleased, so these are one-off steps for a database that created the table or seeds earlier:

```sql
-- feedback_extracted_data no longer carries copies of session_type and questionnaire_version (they went stale
-- when sessions were relabelled). Dropping the column also drops the old (session_type, field_name, value) index.
ALTER TABLE feedback_extracted_data DROP COLUMN IF EXISTS session_type;
ALTER TABLE feedback_extracted_data DROP COLUMN IF EXISTS questionnaire_version;
```

Restart the service (or re-apply `schema.sql`) afterwards to create the new `idx_feedback_extracted_field_value` index. Then: write the content of `seed_config.sql` as a new `config_version` for each persona (section 4; the persona prompts arrive with `seed_prompts.sql` at startup), and, if an extraction prompt was changed by hand, keep it (the seed never overwrites it). Use the queries of section 6 as printed here: the older ones read the dropped columns.


## Which config applies, and how fast a change is seen

One rule finds the config of a session or an upload, for the upload config and for the extraction config alike: for a (tenant, source, session_type) the config of the session's **own program** wins; when the program has none, the tenant's **program-less default** config (`program_id` NULL) for that source and persona is used; a config of **another program** is never used. `load_extraction_config` and `load_closing_keys` (this document), the upload (`load_feedback_target`), the Kafka consumer's schema lookup and the process steps (`get_active_feedback_config`, `get_feedback_process_steps` in `app/database/feedback_operations.py`) all apply it, so a program that only has the shared default config uploads, ingests and extracts like one with its own. When more than one config is left at the winning level (a program with several session types and none named, or several program-less defaults and no own config) the upload is refused with 409 and the lookups raise `AmbiguousFeedbackConfigError`. The questionnaire version windows follow the rule: a default config relabels the sessions of the programs that have no config of their own for that source and persona, never the sessions of a program that has one.

**Operator note.** The Kafka consumer caches a persona's settings (ingestion schema, adapter, process steps) for up to **60 seconds** after a config change; uploads read the active config on every request and see the new value at once. Events consumed in the minute after a change can still be validated against the previous values.

## Notes from the PR review (behaviour added after review)

- **Retry with no steps:** the retry endpoints accept a feedback submission whose `process_steps` is the valid empty list and finish it as a no-op success; unsupported submission types without steps are still rejected.
- **Stale-write check:** besides the message fingerprint, the extraction write also compares the session's `session_type`, `source` and program under the write locks, so a redelivery that only changes the persona cannot be overwritten by the old persona's fields.
- **Upload check:** an upload is refused when the config JSON names a `session_type` that differs from the config row (or names none).
- **`detect`:** proposes a version only inside the latest recorded window; changes seen in older windows are reported separately under `historical_changes`; wording variants are not boundary evidence unless `--include-wording` is given.
