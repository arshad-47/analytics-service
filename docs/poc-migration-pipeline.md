# Migration Pipeline — How It Works

This document explains how submissions (Stories and Chaupal Discussions) are moved from the **ShikshaLokam (SL) Production Database** into the **Analytics Platform**, and how the script keeps things in sync over time.

---

## 1. What This Does

The script (`migrate-scripts/migrate.py`) runs periodically and does one job: find submissions in the SL DB that have **not yet been sent to Analytics**, and push only those.

It never re-sends what already exists. It never touches the Analytics app code.

---

## 2. Step-by-Step Flow

```
SL Production DB  (accessed via Metabase API Key)
        │
        ▼
Step 1 — Ask Analytics DB: "which submission IDs do you already have?"
        │
        ▼
Step 2 — Ask SL DB: "give me all valid submission IDs"  (candidate_ids.sql)
        │
        ▼
Step 3 — Compute the gap: SL IDs  minus  Analytics IDs  =  New IDs to send
        │
        ▼
Step 4 — Fetch full records from SL DB only for those new IDs  (submission.sql)
        │
        ▼
Step 5 — Enrich each record (geo detection, organization lookup, program mapping)
        │
        ▼
Step 6 — Build a CSV file and POST it to Analytics Upload API  (/v1/upload/)
        │
        ▼
Analytics handles the rest: saves to cloud, publishes to Kafka, writes to DB,
triggers processing workflows
```

---

## 3. Key Design Choices

### Only send what is new
The script compares two sets of IDs — what SL DB has vs what Analytics already has — and sends only the difference. This avoids duplicates and keeps runs fast.

Analytics identifies a submission by both `submission_id` and `tenant_code`, so the gap query filters by tenant to make sure we don't skip records that belong to a different tenant with the same ID.

### Use the Upload API, not Kafka directly
Instead of writing Kafka messages ourselves (which would need changes inside the app), the script packages records as a standard CSV and posts it to the existing Upload API endpoint. The platform then handles everything downstream — validation, deduplication, Kafka, DB writes — the same way a normal upload would.

This means **zero changes to the app code**.

### Filter session types upfront
Both SQL files only return sessions with valid, configured session types. Test sessions, orphan sessions, and anything not in the config are ignored before any data is fetched.

### Safe file naming
Every CSV sent to the Upload API has a unique name:
```
migration_{report_type}_{run_id}_{YYYYMMDDHHMMSS}.csv
```
The `run_id` is a UUID generated at the start of each run, so files never clash even if two runs happen at the same time.

### Safe against in-flight uploads
Because `/v1/upload/` processes files in the background, a run that starts right after a previous upload might find that the earlier submissions haven't finished writing to the database yet. To prevent uploading the same records again, the script checks the `csv_uploads` table first. If an earlier migration upload is still pending or in progress, the script logs an informative message and exits cleanly, waiting for background processing to finish.

### Metabase API Key for SL DB access
The script uses a static API key (`METABASE_API_KEY`) to query SL DB through Metabase. No session login, no expired tokens.

---

## 4. Config Files

| File | What it does |
|---|---|
| `migrate.py` | The main script |
| `candidate_ids.sql` | Fetches valid submission IDs from SL DB |
| `submission.sql` | Fetches full record details for a given set of IDs |
| `programs_config.json` | Maps session types to program names and leader categories |
| `organizations.json` | Maps states, districts, and keywords to organization names |
| `mapping.json` | Additional field mappings used during enrichment |

### programs_config.json
Defines which session types belong to which program:
- `shiksha_chaupal` → Chaupal Discussion session types (`shikshalokam_chaupal`, `guest-discussion`)
- `shiksha_chaupal_mi` → Story session types (`guest-mi-story`, `guided-validation`, `mi-story`)

Each program entry also sets the default `program_name` and `leader_category` used in the CSV.

### organizations.json
Maps geographic data to organization names. Instead of hardcoding organization names inside the script, all mappings live here so they can be updated without touching any code.

Structure:
- `state_district_map` — district-level org lookup per state
- `keyword_rules` — if the location text contains a keyword, use this org
- `default_organization` — fallback if nothing matches

---

## 5. Environment Variables (`.env`)

```bash
# Metabase — used to query SL Production DB
METABASE_URL=qa.mitra.dashboard.shikshalokam.org
METABASE_API_KEY=your_metabase_api_key
METABASE_DATABASE_ID=3

# Analytics — where data is sent
ANALYTICS_API_BASE_URL=http://localhost:8000
AUTH_TOKEN=your-bearer-token
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/local-analytics

# Config file paths
PROGRAMS_CONFIG_PATH=migrate-scripts/programs_config.json
ORGANIZATIONS_CONFIG_PATH=migrate-scripts/organizations.json
```

> **Note:** `DATABASE_URL` and `PROGRAMS_CONFIG_PATH` are required. The script will raise an error and stop if either is missing or points to a file that does not exist.

---

## 6. Running the Script

### Preview only (no data is sent)
```bash
python migrate-scripts/migrate.py --dry-run
```
Shows how many new submissions would be sent, with a sample of the rows.

### Full sync run
```bash
python migrate-scripts/migrate.py
```

### Automated Run via Temporal Scheduled Workflow
The migration can also be scheduled automatically inside Temporal:
1. In `.env`:
   ```bash
   ENABLE_MIGRATION_CRON=true
   MIGRATION_CRON_SCHEDULE="30 6 * * *"  # Runs everyday at 12 noon
   ```
2. When the Temporal Worker starts (`python main.py --mode worker`), it registers the schedule `scheduled-sl-analytics-migration`.
3. In the Temporal Web UI (`http://<temporal-server>:8080/schedules`), you can monitor executions, view run logs, pause/resume the schedule, or trigger an immediate run anytime.


---

## 7. What Happens to Enrichment Data

Before building the CSV, each record goes through an enrichment step:

- **Geo detection** (`GeoMapping`) — reads location, district, state fields from the survey response and resolves them to a clean state/district.
- **Organization lookup** (`OrganizationResolver`) — uses the resolved state and district to look up the organization name from `organizations.json`. Falls back to keyword matching on the location text, then to the configured default.
- **Program mapping** — uses `programs_config.json` to assign the right program name and leader category based on the session type.

The enriched data is then passed into `build_story_csv_row` or `build_discussion_csv_row` depending on the submission type, which produces the final row written to the CSV.
