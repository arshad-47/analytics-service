# Analytics Service

An orchestration and ingestion service built with FastAPI, Kafka, and Temporal. It manages dynamic data ingestion pipelines, rule-based content moderation, local NLP vector similarity matching, and fallback LLM classification.

---

## Setup & Running Guide

### Prerequisites

Ensure the following dependencies are installed and running locally:
1. **Python**: Version 3.9+
2. **PostgreSQL**: Running and initialized with the database schema
3. **Apache Kafka & Zookeeper**: Running on `localhost:9092`
4. **Temporal Server**: Running on `localhost:7233` (e.g. via `temporal server start-dev` or Docker)

### Installation

1. Clone the repository and navigate to the project root.
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your environment variables:
   Copy `.env.example` to `.env` and fill in the required configurations (such as database credentials and your OpenRouter API key):
   ```bash
   cp .env.example .env
   ```

### Temporal Setup via Docker

A [docker-compose.yaml](file:///Users/user/Documents/AI/analytics-arch/analytics_service/docker-compose.yaml) file is provided to start the local Temporal server and its UI dashboard.

To start Temporal:
```bash
docker-compose up -d
```
This launches:
* **Temporal Server** on port `7233`
* **Temporal Web UI** on port `8233`

The configuration automatically points to your local host's PostgreSQL instance (`host.docker.internal`). Ensure PostgreSQL is running and has the `temporal` and `temporal_visibility` databases created prior to starting the containers.

#### Configuration ([docker-compose.yaml](file:///Users/user/Documents/AI/analytics-arch/analytics_service/docker-compose.yaml)):
```yaml
version: '3.8'

services:
  temporal:
    image: temporalio/auto-setup:1.24
    container_name: temporal
    ports:
      - "7233:7233"
    environment:
      - DB=postgres12
      - POSTGRES_SEEDS=host.docker.internal
      - POSTGRES_USER=postgres
      - POSTGRES_PWD=postgres
      - DB_PORT=5432
      - TEMPORAL_DB=temporal
      - TEMPORAL_VISIBILITY_DB=temporal_visibility
    extra_hosts:
      - "host.docker.internal:host-gateway"

  temporal-ui:
    image: temporalio/ui:2.34.0
    container_name: temporal-ui
    ports:
      - "8233:8080"
    environment:
      - TEMPORAL_ADDRESS=temporal:7233
      - TEMPORAL_UI_PORT=8080
    depends_on:
      - temporal
```

### Running the Application

The **`main.py`** entry point supports starting different components using the `--mode` flag:

```bash
# Default – launches web server, Kafka consumer, and Temporal worker concurrently
python main.py

# Launch specific services separately
python main.py --mode web        # Starts only the FastAPI web server
python main.py --mode consumer   # Starts only the Kafka consumer
python main.py --mode worker     # Starts only the Temporal worker
```

---

## Running the tests

Install the dependencies (`requirements.txt` or `requirements-prod.txt`, plus `pytest`, `httpx` and `json5`, which `requirements.txt` lists), then from the repository root:

```bash
pytest tests -q --ignore=tests/unit_testing.py --ignore=tests/test_hf_thematic.py -p no:cacheprovider
```

* **No setup is needed.** `tests/conftest.py` sets every setting the suite reads (`AUTH_TOKEN`, a `DATABASE_URL` that nothing listens on, the processing mode, the LLM and storage defaults, ...) before the app is imported, with `os.environ.setdefault`, so a fresh clone, CI and a developer machine behave alike and no test depends on your `.env`. A unit test can never reach a real database or the LLM.
* **Real-Postgres tests are opt-in.** About one test in seven (every `tests/test_*_pg.py` file and the database parts of a few others) skip unless `FEEDBACK_TEST_DATABASE_URL` is set. It names a Postgres **server**, not a database to test in: any database on that server, with a role that has `CREATEDB`, for example `postgresql://postgres:postgres@localhost:5432/postgres`. For every test module that has such tests, `tests/pg_support.py` (through the module-scoped fixture in `tests/conftest.py`) creates a fresh, empty database `scratch_pg_<module>_<random>` on that server, points the module at it and drops it when the module is done. The destructive statements of the tests (`DELETE FROM config`, `DROP SCHEMA public`) can therefore only reach a database that the run itself created, and the modules are independent of each other and of their order. The database named in the URL is only used to connect and is never modified. If the role cannot create databases, or the server cannot be reached, the run stops at once with a message (the URL is never printed). A run that was killed can leave a `scratch_pg_*` database behind: drop it by hand.
* **CI** (`.github/workflows/tests.yml`) runs the same command on every pull request and on pushes to `main` and `release-*`, against a `postgres:16` service container, and fails when any test was skipped (so a green run really exercised the Postgres tests).
* **What not to run:** `tests/unit_testing.py` is the older harness; it is left out above because a run of it rewrites the tracked `tests/TEST_CASES.csv` (the `conftest.py` hook syncs each test's result into the sheet). Run it only on purpose, as `pytest tests/unit_testing.py` (a run that names this file keeps your own `DATABASE_URL`: three of its tests, `test_storage_015`, `test_upload_003` and `test_upload_013`, reach that database through the upload route), and restore the sheet with `git checkout -- tests/TEST_CASES.csv` if you did not mean to change it. `tests/test_hf_thematic.py` needs the Hugging Face models and is not part of the CI run. Never point `FEEDBACK_TEST_DATABASE_URL` at a production server.

---

## Health Endpoint

Verify that all systems are running by calling the health check API:
```bash
curl http://localhost:8000/health
```
Example response:
```json
{
  "status": "healthy",
  "consumer_running": true,
  "worker_running": true
}
```

---

## Orchestration Modes

The service supports two processing modes configured via the `PROCESSING_MODE` environment variable in your `.env` file:

### 1. Real-Time Processing (`PROCESSING_MODE=real-time`)
*   **Ingestion Flow**: When a new Kafka event is received, the consumer automatically attempts to trigger a Temporal workflow (`ConfigDrivenProcessingWorkflow`) immediately.
*   **Temporal Connection Self-Healing**: 
    *   On startup, the Kafka consumer initializes the Temporal client connection.
    *   If the Temporal server is offline or unreachable during consumer startup or during execution, the consumer **does not crash**.
    *   Instead, the consumer gracefully logs the connection error and leaves the submission in the PostgreSQL database with a `'pending'` status.
    *   When new Kafka events arrive, the consumer automatically checks if the Temporal client is disconnected and attempts to reconnect/heal the connection. Once the Temporal server is back online, real-time workflow triggering resumes automatically.

### 2. Batch Processing (`PROCESSING_MODE=batch`)
*   **Ingestion Flow**: When a new Kafka event is received, the consumer simply stores the record in PostgreSQL with a `'pending'` status. The consumer **completely bypasses all Temporal calls** during ingestion. Even if the Temporal backend is entirely down, Kafka event ingestion works flawlessly.
*   **Automatic Daily Schedule Registration**: 
    *   When the Temporal worker process starts, it inspects `PROCESSING_MODE`. If set to `batch`, the worker automatically registers a daily batch Schedule in the Temporal Server named `daily-batch-processing`.
    *   The run time is configured via the `BATCH_SCHEDULE_CRON` environment variable (default: `0 20 * * *` - 8:00 PM UTC).
    *   If the schedule is already registered, the worker logs this and skips registration gracefully.
*   **Batch Execution**: 
    *   At the scheduled time, the Temporal Server triggers the `BatchProcessingWorkflow`.
    *   This workflow calls the `fetch_pending_submissions_activity` to retrieve all submissions in PostgreSQL with a `'pending'` status.
    *   It then spawns child workflows (`ConfigDrivenProcessingWorkflow`) to process all retrieved submissions in parallel.
    *   If the Temporal Server backend is offline at the scheduled run time, its *catch-up* policy will trigger the missed batch immediately upon starting up again.


---

## Feedback uploads: sources, adapters, and preparing the export (1 Million Voices is the first program)

`feedback` is a **generic** report type. Nothing that is specific to a program, data source or persona is in code:
it all comes from the **upload config** (the `config` / `config_version` rows; `seed_config.sql` seeds the 1 Million
Voices ones and is data, not a template of limits). A new program (another tenant, another persona, other column
names) is onboarded by adding config rows, with no code change.

* **Source** = a free label of a data feed (`mitra`, `google_forms`, `ptm_app`, ...): 1 to 40 characters of
  `[a-z0-9_]` (`any` is reserved). There is no list in code: a source is usable when an ACTIVE `feedback_upload` config
  exists for (tenant, program, persona, source); otherwise the upload is rejected with
  `No upload config for source '<source>' is set up for <persona>`.
* **Adapter** = how the CSV is shaped, named by the config key `adapter` (REQUIRED; an unknown or missing value is
  rejected on upload and by the consumer). The code never looks at the source name to choose:
  * `conversation_rows`: one row per message (a bot conversation export). Sessions grow, so a re-upload adds the new rows.
    Optional `column_map` (role to CSV column name; roles not listed keep the role name as header; required roles:
    `session_type`, `session_status`, `session`, `submission_id`, `language`, `sequence`, `message`, `created_at`, `stage`,
    `sender_id`; optional: `bot_id`, `bot_name`, `translated_message`, `conversation_row_id`, `receiver_id`, `updated_at`)
    and optional `completion_map` (the source's own status words to `COMPLETED` / `IN_PROGRESS`; the seed lists both
    explicitly). A source with other headers or other status words only changes these two keys.
  * `wide_form`: one row per respondent, one column per question (a form export). A response is final: it has a natural
    key (`sourceRef`), a generated UUID as its id, is always `COMPLETED`, and is never updated once stored.
  Keys that belong to the other adapter are rejected (`validate_adapter_config`, `app/services/feedback_adapter_config.py`).
  Keys that no code read (`id_strategy`, `layout`, `ignore_extra_columns`, `columns`, the `kind` of a conversation question)
  were removed from the seed; old configs that still carry them keep working (they are ignored).
* `tenant_code` and `source` are **required** form fields of a feedback upload (422 when missing; there is no default
  tenant or source). story and discussion uploads keep their default tenant `mitra`.
* Identifiers are validated at the boundary: tenant codes and persona keys are `[A-Za-z0-9._-]{1,64}`, submission and session
  ids `[A-Za-z0-9._:-]{1,128}` (no spaces, no CR/LF or other control characters), a stored sequence is 1 to 2147483647,
  and a `created_at` outside the range a UTC timestamp can hold is an unparsable date of that session. Problem messages
  (400 body, DLQ, logs, `csv_uploads.meta_data`) carry counts, data row numbers and column names, never a cell value.
* `POST /v1/upload/` checks the bearer token and a hard request ceiling (`MAX_UPLOAD_REQUEST_BYTES`, 512 MiB, 413 above it,
  also for a body without Content-Length) before the multipart body is read; the persona's own cap is
  `max_csv_upload_bytes` (below).

The rest of this section describes the 1 Million Voices program, the first user of these settings.

`POST /v1/upload/` with `report_type=feedback` takes **one persona per upload** (the persona is the file's
`session_type` column: `Youth_Voice`, `General_Public_Voice`, `Parent_Voice`, `Educator_Voice`; the `leader_category`
form field must be the same persona) and rejects a body above the persona config's `max_csv_upload_bytes` (100 MB
when seeded; see step 4). The Mitra
export holds all personas in one file, so it is prepared before upload. These are operator tools in `tools/`; the
service never calls them.

**1. Split the export, one file per persona (and under the size cap).** Run this on the workstation that holds the
export: `split_mitra_export.py` is not part of the Docker image. The other tools of this section
(`feedback_config.py`, `questionnaire_versions.py`, `run_feedback_extraction.py`) are in the image and run inside the
container, for example `docker exec <container> python tools/feedback_config.py show`.
```bash
python tools/feedback_config.py show     # the max_csv_upload_bytes of each persona: 100000000 as seeded
python tools/split_mitra_export.py all_4_persona_data.csv --out-dir out/ --max-mb 80
# out/Youth_Voice.csv, out/General_Public_Voice.csv, out/Educator_Voice.csv, out/Parent_Voice_part1.csv, ... and split_summary.json
```
`--max-mb` (1 MB = 1,000,000 bytes) must be **at or below the smallest `max_csv_upload_bytes / 1_000_000` of the
personas in the export**: a part above its persona's cap is refused by the upload with 413. The example fits the seeded
100,000,000 bytes; after `feedback_config.py set ... --key max_csv_upload_bytes --value 50000000` (step 4) split with
`--max-mb 50` or less (or run the tool once per persona with its own limit). The tool does not read the database.
A session is never split across files, so a persona that is too big for one upload becomes several parts. Uploading
the parts is safe: the delta check publishes only sessions that are new or grew. Uploading the SAME file again (same
file name and size, same program, persona and report type, unless that earlier upload failed) is rejected with 400
`FILE ALREADY EXISTS`; a re-export under another name or size is accepted and only its new or grown sessions are published.

**2. Upload each file with its persona.**
```bash
curl -X POST localhost:8000/v1/upload/ -H "Authorization: Bearer $TOKEN" \
  -F report_type=feedback -F program_name="1M Voice" -F leader_category=Youth \
  -F tenant_code=mitra -F source=mitra -F file=@out/Youth_Voice.csv
```

**3. Record questionnaire versions as data, not code.** Every session carries a `questionnaire_version` (V1, V2 ...)
derived from when it started, using windows stored in the database (the persona's Mitra upload config). A new
database only has the baseline `V1`. When a bot's questions change, record the moment the change went live; no code
change and no deploy are needed, for V2, V3, V4 alike. You do not have to know the date: `detect` reads the
questions the stored sessions were asked (the scripted question of each step, not the LLM probes), and when a new
question set has appeared it proposes the next key, the instant it started and the `set` command to run. It is
read-only; a person confirms by running `set`:
```bash
python tools/questionnaire_versions.py detect --session-type Youth_Voice
python tools/questionnaire_versions.py list
python tools/questionnaire_versions.py set --session-type Youth_Voice --key V3 \
    --effective-from 2026-12-01T10:30:00+05:30 --dry-run     # shows what would change, writes nothing
python tools/questionnaire_versions.py set --session-type Youth_Voice --key V3 \
    --effective-from 2026-12-01T10:30:00+05:30 --note "new confidence question"
```
`set` writes a new config version (the old one stays as history and can be re-activated to roll back) and relabels
the sessions already stored. Sessions uploaded before a version is recorded are relabelled when it is.
`detect` learns what each version asks from its first 100 stored sessions (`--baseline-sessions`), so on a very
small deployment lower that number; it finds new question sets but does not audit dates you recorded by hand.

**4. Per-bot settings live in the config tables, not in `.env`.** Every feedback bot can have a different input
structure, so the settings that differ per bot are keys of the persona's active `feedback_upload` config (Mitra and
Google Forms each have one): `adapter` (see above), `max_csv_upload_bytes` (size cap of one upload, 413 above it, checked before the file is
stored; seeded 104857600), `ingestion_schema` (required / optional fields of the Kafka event, same structure as
`STORY_KAFKA_SCHEMA`; an event that does not satisfy it goes to the DLQ), `process_steps` (steps run after ingestion,
same format as `PROCESS_CONFIG_STORY`; the seed lists `feedback_metadata_extraction` for every upload config, so on a new database extraction (one paid LLM call per session)
starts right after ingestion; a config with an empty list marks the submission success at ingestion; an existing database keeps its own list, change it with the `set --key process_steps` line below) and the optional `expect_stages` (list of stage names; an upload then
reports, per stage that is neither expected nor a closing key, how many sessions carry it, in the upload response
`stage_report` and in `csv_uploads.meta_data`; report-only, it never skips a session). A config that lacks one of the
required keys (`adapter`, `max_csv_upload_bytes`, `ingestion_schema`, `process_steps`), or has one of the wrong type, is rejected with a clear message (400 on upload, DLQ in the
consumer). The Kafka consumer caches the schema per persona for 60 seconds. The seed (`seed_config.sql`, run at every start)
never overwrites, so an existing
database keeps the config version it was seeded with: upgrade it once (next section), then change values without SQL
(each change is a new config version, the old one stays as history):
```bash
python tools/feedback_config.py show [--session-type Youth_Voice]
python tools/feedback_config.py migrate --dry-run            # lists what would be added; then run without --dry-run
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key max_csv_upload_bytes --value 50000000
# turn metadata extraction ON for one persona of an existing database (new databases are seeded with it; one paid LLM call per submission); drop --dry-run to apply
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key process_steps \
    --json '[{"name": "feedback_metadata_extraction"}]' --dry-run
python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key expect_stages --unset
```

`show` and `set` also validate the adapter-related keys of the whole config (`adapter`, `column_map`, `completion_map`
and the keys that belong to the other adapter): a problem shows under `adapter_config`, and `set --key adapter` refuses a
switch the config would not survive. The startup health check logs the same problems as one ERROR per config.

**Which config applies (one rule everywhere).** For a (tenant, source, session_type) the config of the session's **own
program** wins; when that program has none, the tenant's **program-less default** config (`program_id` NULL) for the same
source and persona is used; a config of **another program** is never used. The upload endpoints, the Kafka consumer's
schema lookup, the process steps and the metadata extraction all follow it, so a program that only has the shared default
can be uploaded to, ingested and extracted the same way. If more than one config is left at the winning level (a program
with several session types and none named, or a tenant with the defaults of several personas and no own config) the upload
is refused with 409 and an operator must disambiguate. The questionnaire versions follow it too: the windows of a default
config relabel the sessions of the programs that have no config of their own for that source and persona, and never the
sessions of a program that has one. The tools that edit a config (`set`, `migrate`, the version `set`) stay exact: they act
on the one config row they are told, never on the fallback.

**Operating notes.** A persona's settings are read in two places that do not see a change at the same moment. The upload
endpoints read the active config on every request, so a change made with `tools/feedback_config.py set` applies to the next
upload at once. The Kafka consumer caches each persona's settings (the ingestion schema, the adapter and the process steps,
as loaded and validated by its config cache) for up to 60 seconds, so events consumed in the minute after a change can
still be validated against the previous values (a config that was just fixed can still be reported invalid for that
minute); after that the new ones apply without a restart. Tightening an `ingestion_schema` therefore does not take
effect at once.

### Upgrading an existing database (created before `session_type`, questionnaire versions and adapters)

A database from release 1.3 has one `config` row per Mitra bot, no persona key (`session_type`) on configs or stored
sessions, and configs without `adapter`, `questionnaire_versions`, `opening_row`, `column_map`, `completion_map` or the
per-form `questionnaire_version`. **Startup never fails on it**: `schema.sql` adds the nullable columns, drops the
`config_source` CHECK (a source is a free label), and creates the unique index of `config`
(`uq_config_tenant_source_type_program_session_type`) only when no two rows collide; otherwise it keeps the old index and
logs a WARNING naming table `config` and the tool below. What startup cannot do is invent the persona key of a config
or of a stored session (inference is not generic), so until the one-off step below is run the health check logs ONE
ERROR, `Legacy feedback data: ...`, that contains the exact dry-run command, uploads for those personas are rejected
and their pending sessions fail.

The one-off step is `tools/feedback_config.py migrate-legacy`: tenant-scoped (`--tenant-code`, one tenant per run),
idempotent (a second run changes nothing), `--dry-run` computes everything and writes nothing, and it prints counts and
ids only. **You** supply which persona each program is: `--map KEY=SESSION_TYPE`, where KEY is a program id, a program
name that is unique in the tenant, or `leader:<leader category name>`. A key that matches nothing (or an ambiguous name)
is an error; a program you leave out is reported as `unmapped` and left untouched. Nothing is guessed.
```bash
# 1. read the health ERROR in the service log (it lists the program ids), then check the counts; nothing is written
python tools/feedback_config.py migrate-legacy --tenant-code mitra --dry-run \
    --map leader:Youth=Youth_Voice --map leader:Parent=Parent_Voice \
    --map "leader:General Public=General_Public_Voice" --map leader:Educator=Educator_Voice
# 2. the same command without --dry-run
# 3. several config rows of one source and program (the old one-row-per-bot design, for example a V2 bot): they cannot
#    both stay. The run lists them under "duplicates" and "unique_index": "blocked". Keep one and retire the others
#    (deletes that config row and its versions); the V2 window is then recorded as data, step 3 above
python tools/feedback_config.py migrate-legacy --tenant-code mitra --map ... --retire-config <config id of the row to drop>
```
For every mapped config it writes ONE new config version (the config row is locked first; the old version is deactivated
and kept as history) that adds only the keys that are missing, with these defaults: `session_type`; `adapter` derived from
the legacy source (`mitra` -> `conversation_rows`, `google_forms` -> `wide_form`; any other source is reported, never
invented); for `conversation_rows` `questionnaire_versions` `[{"key": "V1", "effective_from": null}]`, `opening_row`
`{"mode": "auto"}` (reads files with and without the bot's opening row), `column_map` (every role reads the column of the
same name) and `completion_map` (`COMPLETED` / `IN_PROGRESS` map to themselves); for `wide_form` each form's
`questionnaire_version` `"V1"`; and `max_csv_upload_bytes`, `ingestion_schema`, `process_steps` as `migrate` does. An existing key
is never overwritten, and a config whose result would not validate is not written (reported under `invalid`). It then fills
`feedback_submissions.session_type` (from the only config of the session's tenant, source and program) and
`questionnaire_version` (`conversation_rows`: from the config's windows and the session's first message; `wide_form`:
the version all forms share) where they are NULL; a value already set is never changed. Finally it creates the unique
index once the data allows it. If you later record a V2 window, run `tools/questionnaire_versions.py set` as usual: it
relabels the sessions. `migrate` (only the three per-bot defaults and the adapter) stays for databases that already
have `session_type`.

