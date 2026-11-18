# Feedback extraction: fields, bands and prompts per persona

Written for: product (who confirms the bands) and the engineer who operates the extraction step.

Each persona has its own extraction config (the DOTS metadata columns of its export) and its own prompt, both stored as data in the database. **Where the bands live:** the fields, category lists and `prompt_name` of each persona are in the block `-- CONFIG feedback_extraction any <session_type>` of `seed_config.sql` (repo root); the live truth is the `config_version` row. The category lists are the data team's finalised lists (the "DATA team finalised categories/bands" column of the 1MV dashboard requirements sheet), written in one normalised spelling.

**What `feedback_extracted_data.value` holds.** A value is never empty. It is one of the field's categories, the label of its band (age and income are bands), or one of the labels the engine writes itself:

| `status` | `value` | When |
|---|---|---|
| `extracted` | a category / band label | the respondent answered and it maps (also `Prefer not to say`, `Others`, `Outside India` ...) |
| `unmapped` | the category (`Unmapped`, `Invalid response`, `Unclear`) the model chose, or the field's fallback (`Unmapped` if the field has it, else `Others`, else `Invalid response`) | answered but needs a person's look; the respondent's words are in `raw_value` |
| `not_provided` | `Not answered` | the question applies but there is no answer (skipped, or the session ended first) |
| `not_applicable` | `N/A` | the question does not exist in the session's questionnaire version (`versions`), or an earlier answer makes it moot (`applies_when`) |

`value_list` always lists the individual labels (one for a single-value field); a **multi** field (`multi`) can hold several, written in the config's order joined by `, ` in `value` (for example `Studying, Working`) and listed in `value_list` (count categories with `value_list`). Labels use sentence case (`Prefer not to say`, `Invalid response`, `Others`, `Not sure`), proper nouns keep their capitals, money and age bands use symbols (`< 12`, `≥ 12 and < 16`, `≥ ₹83,000`). The engine's `Not answered` and `N/A` are never listed in a config's categories.

Other config keys of a field: `aliases` (other spellings of a category, mapped by the code after the model answered: every spelling of Delhi, Orissa, Pondicherry ...), `bands` (a number stored as the label of the band that holds it; the number stays in `raw_value`), `versions` (the questionnaire versions that ask it; the others get `N/A` without asking the model), `applies_when` (`{"field": <an earlier field>, "in": [values]}`; any other answer there makes it `N/A`), `sensitive` (filled only when the respondent states it explicitly, `raw_value` not stored).

Configs: `prompt_name` in each config names the persona's prompt exactly:

| Persona (`session_type`) | `prompt_name` (prompts.name, analysis_type `feedback_metadata_extraction`) |
|---|---|
| Youth (`Youth_Voice`) | `Feedback Metadata Extraction - Youth` |
| General Public (`General_Public_Voice`) | `Feedback Metadata Extraction - General Public` |
| Parent (`Parent_Voice`) | `Feedback Metadata Extraction - Parent` |
| Educator (`Educator_Voice`) | `Feedback Metadata Extraction - Educator` |

`prompt_name` is required in every extraction config. The program-neutral prompt `Feedback Metadata Extraction` stays seeded only as a TEMPLATE to copy for a new program; no config falls back to it.

## Shared across personas

- **`state`**: the 28 states and 8 union territories (`Delhi` is the one name; New Delhi, NCT, NCT of Delhi, Delhi NCR, Delhi/NCR and NCR are aliases), plus `Prefer not to say`, `Unmapped` (a direction, a region, a city, or a district name used in several states with no state given), `Invalid response`, `Outside India`. A region such as `Delhi/NCR` is decided with the district (Uttar Pradesh or Haryana district: that state; no district or a Delhi district: Delhi). When the respondent names no state but names a district that belongs to exactly one state, the state is derived from the district and the justification says so.
- **`district`**: free text (the commonly used official spelling) or one of the special answers `Prefer not to say`, `Unmapped`, `Invalid response`, `Outside India`. No district reference list yet (a list such as LGD would let the code validate it and resolve names that exist in several states).
- **`gender`** (Youth, General Public, Educator; `child_gender` for Parent, multi): `Male`, `Female`, `Others`, `Prefer not to say`, `Invalid response`, `Unsure / don't know`.
- **`age`** (Youth, General Public): a number (decimals such as 15.5 are valid) stored as a band label; the stated number is `raw_value`. Special answers `Prefer not to say`, `Unclear`, `Invalid response`; a number outside 0 to 120 is `Invalid response`. Rows of respondents below 12 (and, for Youth, above 25) are recorded like any other; excluding them from a dashboard or a DOTS export is the job of the export query.
- **Flagged sessions** (`pii_detected` TRUE) keep everything that was extracted, the respondent's words in `raw_value` included; the flag marks them and the export query decides what to leave out.

## Youth (`Youth_Voice`, DOTS: 12 columns)

| Field | Kind | Categories / bands | Flags |
|---|---|---|---|
| `gender` | category | Male; Female; Others; Prefer not to say; Invalid response; Unsure / don't know |  |
| `age` | integer | < 12; ≥ 12 and < 16; ≥ 16 and < 22; ≥ 22 and < 26; ≥ 26 and < 30; ≥ 30 and < 35; ≥ 35 and < 40; ≥ 40 and < 45; ≥ 45 and < 50; ≥ 50 and < 55; ≥ 55 and < 60; ≥ 60 | special: Prefer not to say; Unclear; Invalid response |  |
| `state` | category | the 36 states and union territories | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `district` | free_text | free text | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `current_activity_status` | category | Studying; Working; Self-employed; Looking for work/unemployed; Not currently studying; Others; Prefer not to say; Invalid response | multi |
| `education_level` | category | Grade 6 to 10; Grade 11 to 12; Skill programs; Pursuing undergraduate and postgraduate; Others; Prefer not to say; Invalid response | N/A unless current_activity_status is Studying |
| `course_details` | category | Science; Commerce; Electrical or electronics; Mechanical or manufacturing; Automobile or transport; Civil or construction; Computer or IT; Health; Beauty; Hospitality; Arts (BA or MA); Science (B.Sc or M.Sc); Commerce (B.Com or M.Com); Technology (B.Tech or M.Tech); Business administration (BBA or MBA); Computer applications (BCA or MCA); Medicine (MBBS or MD); Law (LLB or LLM); Others; Prefer not to say; Invalid response | N/A unless education_level is Grade 11 to 12 or Skill programs or Pursuing undergraduate and postgraduate |
| `institution_type` | category | Government; Private; Government-aided; Others; Not sure; Prefer not to say; Invalid response |  |
| `monthly_household_income` | category | Zero income / no income; < ₹25,000; ≥ ₹25,000 and < ₹66,000; ≥ ₹66,000 and < ₹83,000; ≥ ₹83,000; Don't know; Prefer not to say; Invalid response; Unclear | sensitive; only V2 |
| `annual_household_income` | category | Zero income / no income; < ₹3 lakh; ≥ ₹3 lakh and < ₹8 lakh; ≥ ₹8 lakh and < ₹10 lakh; ≥ ₹10 lakh; Don't know; Prefer not to say; Invalid response; Unclear | sensitive; only V1 |
| `disability` | category | Yes; No; Prefer not to say; Invalid response | sensitive |
| `disability_type` | category | Physical disability; Intellectual disability; Others; Prefer not to say; Invalid response | sensitive; multi; N/A unless disability is Yes |

## General Public (`General_Public_Voice`, DOTS: 5 columns)

| Field | Kind | Categories / bands | Flags |
|---|---|---|---|
| `gender` | category | Male; Female; Others; Prefer not to say; Invalid response; Unsure / don't know |  |
| `age` | integer | < 12; ≥ 12 and < 26; ≥ 26 and < 36; ≥ 36 and < 46; ≥ 46 and < 60; ≥ 60 | special: Prefer not to say; Unclear; Invalid response |  |
| `state` | category | the 36 states and union territories | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `district` | free_text | free text | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `occupation` | category | Education; Health; Government and public services; IT and services; Manufacturing; Agriculture; Retail and trade; NGO or social sector; Others; Not currently working; Homemaker; Student; Prefer not to say; Unclear; Invalid response |  |

## Parent (`Parent_Voice`, DOTS: 10 columns; `child_*`, `disability*`, `school_type` are about the CHILD, `parent_*` about the respondent)

| Field | Kind | Categories / bands | Flags |
|---|---|---|---|
| `state` | category | the 36 states and union territories | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `district` | free_text | free text | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `child_gender` | category | Male; Female; Others; Prefer not to say; Invalid response; Unsure / don't know | multi |
| `disability` | category | Yes; No; Prefer not to say; Invalid response | sensitive |
| `disability_type` | category | Physical disability; Intellectual disability; Others; Prefer not to say; Invalid response | sensitive; multi; N/A unless disability is Yes |
| `respondent_relationship` | category | Mother; Father; Guardian; Other relative; Unmapped; Prefer not to say; Invalid response |  |
| `school_type` | category | Government; Private; Others; Not sure; Prefer not to say; Invalid response |  |
| `child_grade` | category | Pre-primary; Primary (classes 1-5); Middle (classes 6-8); High school (classes 9-12); Undergraduate/postgraduate; Skill programs; Others; Prefer not to say; Invalid response; Unsure / don't know; Out of school / not currently enrolled | multi; only V1 |
| `parent_education` | category | Yes; No; Prefer not to say; Invalid response |  |
| `parent_education_level` | category | Pre-primary; Primary (classes 1-5); Middle (classes 6-8); High school (classes 9-12); Undergraduate/postgraduate; Skill programs; Others; Prefer not to say; Invalid response; Unsure / don't know; Did not go to school |  |

## Educator (`Educator_Voice`, DOTS: 5 columns)

| Field | Kind | Categories / bands | Flags |
|---|---|---|---|
| `state` | category | the 36 states and union territories | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `district` | free_text | free text | special: Prefer not to say; Unmapped; Invalid response; Outside India |  |
| `gender` | category | Male; Female; Others; Prefer not to say; Invalid response; Unsure / don't know |  |
| `institution_type` | category | Government; Private; Others; Not sure; Prefer not to say; Invalid response |  |
| `teaching_level` | category | Pre-primary; Primary (classes 1-5); Middle (classes 6-8); High school (classes 9-12); Undergraduate; Postgraduate; Others; Prefer not to say; Invalid response | multi |

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
