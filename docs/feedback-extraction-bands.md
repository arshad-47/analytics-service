# Feedback extraction: fields, bands and prompts per persona

Written for: product (who confirms the bands) and the engineer who operates the extraction step.

Each persona has its own extraction config (the DOTS metadata columns of its export) and its own prompt, both stored as data in the database. **Where the bands live:** the fields, bands and `prompt_name` of each persona are in the block `-- CONFIG feedback_extraction any <session_type>` of `seed_config.sql` (repo root, next to `schema.sql` and `seed_prompts.sql`), which gives a NEW database its version 1; in a running database the live values are the `config_version` rows (`python tools/run_feedback_extraction.py show-config`). There is no Python copy of them. The engine, the output contract and the pivot queries are in `docs/feedback-extraction-and-dots-queries.md`. Source of the bands: the Katha stories (all still Draft) as read in `docs/katha-stories-vs-dots-data-review.md` (findings B1-B5, H1-H8, M1-M7) and `docs/katha-sheet-review-comments.md` (C1-C15, F1-F4); the open questions are in `docs/katha-story-questions-for-product.md`. **Everything below except `state` and `district` is provisional**: product has not frozen the stories, so a band list can change by a new config version without a deploy (last sections).

Reading the tables: **kind** is `category` (one allowed value), `free_text` or `integer` (a whole number, stored as its decimal string). **S** = sensitive: the prompt tells the model to fill it only when the respondent states it explicitly and never to infer it. **Prov.** = provisional. A field the respondent did not answer, or the bot never asked, is `not_provided` (value and raw NULL); a stated answer that fits no band is `unmapped` (value NULL, the words in `raw_value`).

Configs: `prompt_name` in each config names the persona's prompt exactly:

| Persona (`session_type`) | `prompt_name` (prompts.name, analysis_type `feedback_metadata_extraction`) |
|---|---|
| Youth (`Youth_Voice`) | `Feedback Metadata Extraction - Youth` |
| General Public (`General_Public_Voice`) | `Feedback Metadata Extraction - General Public` |
| Parent (`Parent_Voice`) | `Feedback Metadata Extraction - Parent` |
| Educator (`Educator_Voice`) | `Feedback Metadata Extraction - Educator` |

`prompt_name` is required in every extraction config. The program-neutral prompt `Feedback Metadata Extraction` stays seeded only as a TEMPLATE to copy for a new program; no config falls back to it.

## Shared across personas

- **`state`** (category, not provisional): the 28 states and 8 union territories (official names, spelling variants in the guidance). A place that is not an Indian state or union territory is `unmapped`. Open: no "Others" / "Not provided" band, which authoritative list (H2, C10), whose state for Parent (H4).
- **`district`** (free text, not provisional): the commonly used official spelling; no reference list (H2).
- **`gender`** (Youth, General Public, Educator; `child_gender` for Parent): `Male`, `Female`, `Others`, `Prefer not to say`, `Invalid`; provisional. Open: M6 / C7 (does "skip" and "not sure" map to Prefer not to say, "Something Else" to Others); for Parent, H5 / F3 (Male/Female as seeded, or Boy/Girl as the bot says).
- **`age`** (Youth, General Public; integer, 5 to 100, provisional): the stated number. The bounds only reject numbers that cannot be an age. The Youth maximum is not 25 on purpose: 25% of the Youth bot's first answers are over 25 and whether those are valid Youth responses is open (H1, C2), so a tight maximum would turn them into `unmapped` before product has decided. Banding is done later in SQL (the age bands overlap in the stories). Open: H1 (final bands, what is Invalid), C3 (General Public 60 is in two bands).

## Youth (`Youth_Voice`, DOTS: 11 columns)

| Field | Kind | Bands / range | S | Prov. | Source | Open product questions |
|---|---|---|---|---|---|---|
| `age` | integer | 5 to 100 | | yes | 2122, 2119 | H1, C2: bands without overlap, valid range |
| `gender` | category | Male; Female; Others; Prefer not to say; Invalid | | yes | 2122 | M6, C7 |
| `state` | category | 36 states and UTs | | no | 2122, 2119 | H2, C10 |
| `district` | free text | | | no | 2122 | H2 |
| `current_activity_status` | category | Studying; Working; Self-employed; Looking for work/Unemployed; Other; Invalid | | yes | 2122 (M7, C5) | the sheet's K9 lists four bands, M9 six; a student who also works; exam preparation (the seed says Studying); "doing something else" (Other) |
| `education_level` | free text | the class as a number, or the college year in a few words | | yes | 2119 (H7, F1) | no bands exist: categories (for example 1-12, College) or raw? Empty when not studying |
| `course_details` | free text | the course or stream in a few words | | yes | 2119 (H7, F1) | a standard list or the respondent's words? |
| `institution_type` | category | Government; Government-aided; Private; Others; Invalid | | yes | 2122 (H8, C4) | "Not applicable" for a working respondent; international and open schools; colleges |
| `monthly_household_income` | category | Less than ₹25,000; ₹25,000 - ₹66,000; ₹67,000 - ₹83,000; Above ₹83,000; Don't know | S | yes | 2122 (B4, C1); "Don't know" from C4 | edges without gap (the seed closes 66,001-66,999 and puts 83,000 in the third band in the guidance, labels unchanged); is "Don't know" a band; V1 and Google Form ask per YEAR |
| `disability` | category | Yes; No; Prefer not to say; Invalid | S | yes | 2122 (C8) | "Not specified" and "Invalid" one band or two; the older V1 bot never asked |
| `disability_type` | free text | the kind, only when `disability` is Yes | S | yes | 2119 (F2) | free text or a standard list (RPwD Act categories)? |

Income rule (config description and prompt): the band is always MONTHLY. If the bot's question or the respondent speaks of a year (annual, yearly, a lakh a year) the model divides by 12 first, then picks the band; the period the respondent states wins over the period the bot asked; a range spanning two bands or an unclear period is `unmapped`. Open (C1): is dividing annual answers by 12 acceptable, or a separate annual column?

## General Public (`General_Public_Voice`, DOTS: 5 columns)

| Field | Kind | Bands / range | S | Prov. | Source | Open product questions |
|---|---|---|---|---|---|---|
| `age` | integer | 5 to 100 | | yes | 2123, 2120 | C3: 60 is in two bands; smallest valid age |
| `gender` | category | as above | | yes | 2123 | M6, C7 |
| `state` | category | 36 states and UTs | | no | 2123 | H2, C10 |
| `district` | free text | | | no | 2123 | H2 |
| `occupation` | category | Education; Health; Government and public services; IT and services; Manufacturing; Agriculture; Retail and trade; NGO or social sector; Others; Invalid | | yes | 2123 (C6, M6) | the list is a SECTOR list: overlaps (a government school teacher: the seed says the sector wins), where homemaker, student, retired, unemployed, self-employed and daily-wage workers go (the seed: Others). The labels are capitalised; DOTS may want the raw answer (B3) |

## Parent (`Parent_Voice`, DOTS: 10 columns; `child_*`, `disability*`, `school_type` are about the CHILD, `parent_*` about the respondent)

| Field | Kind | Bands / range | S | Prov. | Source | Open product questions |
|---|---|---|---|---|---|---|
| `state` | category | 36 states and UTs | | no | 2124 | H4: the child's or the parent's state |
| `district` | free text | | | no | 2124 | H4 |
| `child_gender` | category | Male; Female; Others; Prefer not to say; Invalid | | yes | 2121 (H5, F3) | no story categories: Boy/Girl or Male/Female |
| `disability` | category | Yes; No; Prefer not to say; Invalid | S | yes | 2124 (C9) | whose disability (the seed: the child's); "not asked" for V1 sessions |
| `disability_type` | free text | the kind, only when `disability` is Yes | S | yes | 2121 (F2) | free text or a standard list |
| `respondent_relationship` | category | Mother; Father; Guardian | | yes | 2121 (C11) | grandparent, aunt, uncle, older sibling (the seed: Guardian); "both parents" is `unmapped`; Others / Prefer not to say |
| `school_type` | category | Government; Private; Others; Invalid | | yes | 2124 (H8, C4) | Government-aided for Parent; one name (the story says Child's Institution Type) |
| `child_grade` | category | Primary (1-5); Middle (6-8); Secondary (9-10); Higher Secondary (11-12); College/University; Other | | yes | 2124 (C12) as quoted in the sheet review | the V2 bot no longer asks it (M5): V1 and Google Form only; Pre-primary (nursery, LKG, UKG) fits no band; several children |
| `parent_education` | category | Yes; No | | yes | 2121 (H6, F4) | what the yes/no holds when not answered |
| `parent_education_level` | category | Did Not Go to School; Primary; Middle; Secondary; Higher Secondary; Undergraduate; Postgraduate; Others; Invalid | | yes | 2124 (C13), ladder inferred from story 2116's copy of the text | H6: `Did Not Go to School` or N/A when `parent_education` is No; PhD above Postgraduate; diploma / ITI; "studied up to class 7" (the seed: the last class attended, Middle) |

## Educator (`Educator_Voice`, DOTS: 5 columns)

| Field | Kind | Bands / range | S | Prov. | Source | Open product questions |
|---|---|---|---|---|---|---|
| `state` | category | 36 states and UTs | | no | 2116 | H2, C10 |
| `district` | free text | | | no | 2116 | H2 |
| `gender` | category | as above | | yes | 2116 | M6, C7 |
| `institution_type` | category | Government; Private; Others; Invalid | | yes | 2116 (H8, C4) | Government-aided; "Other: teacher trainer" shows both (bug 5759) |
| `teaching_level` | category | Primary; Middle; Secondary; Higher Secondary; Undergraduate; Postgraduate; Others; Invalid | | yes | 2116 (B1, C14) | **blocking**: the bot asks "primary, middle, high school or college". "High school" and "college" without a named class or degree cannot be split, so the seed leaves them `unmapped` with the words in `raw_value` (re-bandable later). Change the bands to the bot's four options, change the question, or add a "Not specified" band; several levels at once; the metric's name |

Sensitive fields: `monthly_household_income`, `disability` and `disability_type` for Youth, `disability` and `disability_type` for Parent. They are extracted and stored but must stay out of the DOTS export until product confirms (see the commented-out columns of the pivot queries). Gender and age are not marked sensitive: the bot asks them explicitly and the prompt already forbids inferring a gender from a name.

## Changing a band or a field (or a prompt) without a deploy

Seeds never overwrite: `seed_config.sql` inserts a config's version 1 only while that config has no version at all, and only `ON CONFLICT DO NOTHING`. A change to a running database is a new version row; every extracted row records the `config_version_id` and `prompt_version_id` it used, and rollback is re-activating the older version. Two places, two purposes:

- **a running database (any environment, no deploy):** the SQL recipe below writes a new `config_version`;
- **what a fresh database starts with:** edit the JSON of the persona's block in `seed_config.sql` (a diff of the file reads like a diff of the bands) and ship it with the next release. It does nothing to a database that is already seeded, so also apply the recipe there (next section).

**A new config version** (take the current JSON from `python tools/run_feedback_extraction.py show-config --session-type Youth_Voice`, edit it, keep it valid: a `guidance` key for a band you removed is rejected; the `llm` block of the active version is an operator setting, keep it in the new JSON):

```sql
BEGIN;
SELECT id FROM config WHERE config_type = 'feedback_extraction' AND session_type = 'Youth_Voice' FOR UPDATE;  -- lock first
UPDATE config_version SET is_active = FALSE, updated_at = now()
WHERE config_id = '<config id>' AND is_active;
INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT config_id, max(version) + 1, TRUE, '<edited JSON>'::jsonb, 'your name', 'what changed and why'
FROM config_version WHERE config_id = '<config id>' GROUP BY config_id;
COMMIT;
```

**A new prompt version** of one persona's prompt (the name must be exactly the config's `prompt_name`; keep the safety and PII parts and the template variables `persona`, `fields`, `field_names`, `pii_types`, `output_format`, `conversation_id`, `conversation`):

```sql
BEGIN;
UPDATE prompt_version SET is_active = FALSE
WHERE is_active AND prompt_id = (SELECT id FROM prompts WHERE name = 'Feedback Metadata Extraction - Youth');
INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active, change_note)
SELECT id, (SELECT coalesce(max(version), 0) + 1 FROM prompt_version WHERE prompt_id = prompts.id),
       '<system prompt>', '<user prompt>', TRUE, 'what changed and why'
FROM prompts WHERE name = 'Feedback Metadata Extraction - Youth';
COMMIT;
```

**To point a persona at a different prompt**, add a new prompt row (`INSERT INTO prompts (name, analysis_type) VALUES ('<name>', 'feedback_metadata_extraction')` plus its version 1) and put the exact name in a new config version's `prompt_name`. A `prompt_name` that does not resolve fails loudly; there is no silent fallback to any other prompt.

After a band or prompt change, re-run the affected sessions (one real LLM call each): `python tools/run_feedback_extraction.py run --session-type Youth_Voice --tenant-code <tenant>` without `--only-unprocessed`. Try one or two first with `--limit 2`.

## How an existing database picks this up

A new database gets the persona configs and prompts at the first start (`seed_config.sql`, `seed_prompts.sql`). **An existing database keeps what it has**: the seed never overwrites, so a change made to `seed_config.sql` reaches it only through a new config version. (The old `run_feedback_extraction.py upgrade-config` command and the Python seed definitions it compared with are gone.) To bring one persona of an existing database to the content of the file:

1. copy the JSON between the dollar quotes of the block `-- CONFIG feedback_extraction any <session_type>` of `seed_config.sql` (or edit it first), and keep the active version's `llm` block in it if one was set;
2. run the recipe of the previous section with that JSON: it deactivates the active version (kept as history) and inserts the next version in one transaction. Dry run: `SELECT config = '<JSON>'::jsonb AS already_equal FROM config_version WHERE config_id = '<config id>' AND is_active;` tells whether anything would change;
3. make sure the prompt the config names (`prompt_name`) exists: the persona prompts arrive with the normal start (`seed_prompts.sql`, `ON CONFLICT DO NOTHING`), so apply a config only on a database whose application version includes them;
4. re-run the sessions you want re-extracted (`run` without `--only-unprocessed`; one real LLM call each).

The same applies to the two upload configs (`-- CONFIG feedback_upload mitra|google_forms <session_type>`): their settings can also be changed with `python tools/feedback_config.py set ...` (one key at a time, a new version each).
