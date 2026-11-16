# Analytics Service: Step-Level Retry Architecture & Operations Guide

This document is the dedicated, comprehensive technical reference for the **Step-Level Retry Mechanism** in the Analytics Service. It covers the end-to-end architecture, step-skipping mechanics, API endpoints, automated cron sweeps, keyset pagination, and operational troubleshooting.

---

## 1. Executive Summary & Core Concept

When an AI inference pipeline fails midway (e.g. at Step 4 due to an LLM timeout, rate limit, or transient network outage), standard retries often re-run the entire pipeline from Step 1. In an LLM-heavy pipeline, this wastes hundreds of dollars in API tokens and computation time.

The **Step-Level Retry** pattern solves this:
1. When a submission fails, PostgreSQL records which steps succeeded, which step failed, and the error details inside `submissions.process_status`.
2. When retried (via API or nightly cron), the workflow reads the recorded history (`existing_steps`).
3. Any step that previously recorded `"status": "success"` is **skipped**—its prior result is preserved and reused.
4. Execution resumes directly at the failed step and proceeds through all remaining downstream steps.
5. If the retry succeeds, the submission is updated to `status = 'success'`.

---

## 2. Architecture & Data Flow

```
                                  RETRY TRIGGERS
       ┌────────────────────────────────┬───────────────────────────────┐
       ▼                                ▼                               ▼
[Manual Single Retry]         [Manual Bulk Retry]           [Automated Nightly Cron]
POST /v1/retry-step           POST /v1/retry-step/all       Temporal Schedule (2:00 AM)
(One submission)              (Tenant or All Tenants)       (Circuit-breaker: max 5 attempts)
       │                                │                               │
       │                                └───────────────┬───────────────┘
       │                                                │
       │                                                ▼
       │                                  ┌───────────────────────────┐
       │                                  │   Keyset Cursor Query     │
       │                                  │ (updated_at, submission)  │
       │                                  │ WHERE status IN            │
       │                                  │ ('failed','processing')   │
       │                                  └─────────────┬─────────────┘
       │                                                │
       │                                    Chunked in batches of 50
       │                                    workflow.continue_as_new()
       │                                                │
       └────────────────────────┬───────────────────────┘
                                │
                                ▼
               ┌─────────────────────────────────┐
               │ ConfigDrivenProcessingWorkflow  │
               │ ID: step-retry-{id}-{tenant}    │ (Deterministic & deduplicated)
               └────────────────┬────────────────┘
                                │
             ┌──────────────────┴──────────────────┐
             ▼                                     ▼
     [Existing Step = "success"]           [Failed or Unrun Step]
     * Do NOT call LLM/CV                  * Execute Temporal Activity
     * Preserve previous DB data           * 3x Exponential Backoff
     * Skip to next step                   * Catch and log errors
             │                                     │
             └──────────────────┬──────────────────┘
                                │
                                ▼
                  ┌───────────────────────────┐
                  │ Update PostgreSQL Status  │
                  │ - If all pass: 'success'  │
                  │ - If fails:    'failed'   │
                  │   retry_attempt += 1      │
                  │   failed_at_step recorded │
                  └───────────────────────────┘
```

---

## 3. Database Schema & State Tracking

All retry state is tracked inside the `submissions` table using PostgreSQL's native `JSONB` capabilities:

```sql
submissions (
    id SERIAL PRIMARY KEY,
    submission_id VARCHAR UNIQUE,
    session_id VARCHAR UNIQUE,
    tenant_code VARCHAR,
    submission_type VARCHAR,          -- 'story', 'discussion', etc.
    status VARCHAR,                   -- 'pending', 'processing', 'success', 'failed'
    process_status JSONB,             -- Step execution history & retry counters
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
```

### 3.1. Structure of `process_status` (JSONB)
When a submission fails, `process_status` records the exact audit trail:

```json
{
  "status": "failed",
  "failed_at_step": "story_rating",
  "retry_attempt": 1,
  "error_message": "LLM timeout after 600s",
  "timestamp": "2026-11-03T06:35:39.300182+00:00",
  "steps": [
    {
      "name": "pii_and_abusive_language_detection",
      "status": "success",
      "completed_timestamp": "2026-11-03T06:25:30.350971+00:00"
    },
    {
      "name": "thematic_classification",
      "status": "success",
      "completed_timestamp": "2026-11-03T06:27:12.112450+00:00"
    },
    {
      "name": "story_rating",
      "status": "failed",
      "error": "LLM timeout after 600s",
      "completed_timestamp": "2026-11-03T06:35:39.300182+00:00"
    },
    {
      "name": "environment_detection",
      "status": "skipped",
      "completed_timestamp": null
    }
  ]
}
```

### 3.2. Supporting Partial Index
To enable lightning-fast queries during bulk retries without scanning millions of successful rows, the database uses a **partial composite index**:

```sql
CREATE INDEX IF NOT EXISTS idx_submissions_retryable 
ON submissions (updated_at, submission_id) 
WHERE status IN ('failed', 'processing');
```
* **Partial (`WHERE status IN ('failed', 'processing')`)**: Indexes only failed and processing rows (typically 1–2% of the table), keeping index size in memory minimal.
* **Composite `(updated_at, submission_id)`**: Enables keyset pagination where timestamps might be identical across batch inserts.

**Upgrade Note (Existing Databases):** Because deployed databases already have the older `idx_submissions_failed` index, run the following non-blocking statements manually during deployment:
```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_submissions_retryable ON submissions (updated_at, submission_id) WHERE status IN ('failed', 'processing');
DROP INDEX CONCURRENTLY IF EXISTS idx_submissions_failed;
```

---

## 4. The Step-Skipping Execution Engine

The core workflow engine lives in [`app/temporal/workflows.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/temporal/workflows.py) under `ConfigDrivenProcessingWorkflow`.

### 4.1. How Step-Skipping Works
When the workflow starts:
1. It parses `existing_steps` into a dictionary: `prior_steps = {s["name"]: s for s in existing_steps}`.
2. It loops through the pipeline's configured steps:
   ```python
   for step in process_steps:
       step_name = step["name"]
       prior = prior_steps.get(step_name)

       # If previously succeeded, PRESERVE AND SKIP:
       if prior and prior.get("status") == "success":
           steps_execution.append(prior)
           continue

       # If failed or pending, EXECUTE:
       res = await workflow.execute_activity(activity_func, ...)
       steps_execution.append({"name": step_name, "status": "success", ...})
   ```
3. Only the failed step and remaining steps run.

### 4.2. Failure Handling & Circuit Breaker
If an activity fails after all its internal retries:
1. The exception is caught.
2. The remaining unexecuted steps are marked as `"skipped"`.
3. The first failed step name is identified and stored in `"failed_at_step"`.
4. The `retry_attempt` counter is incremented:
   ```python
   prior_process_status = payload.get("process_status") or {}
   retry_attempt = (prior_process_status.get("retry_attempt") or 0) + 1
   ```
5. `update_status_activity` writes `status = 'failed'` and the updated `process_status` to PostgreSQL.

---

## 5. Keyset Cursor Pagination (`BulkStepRetryWorkflow`)

When retrying in bulk (via API or cron), traditional `OFFSET` pagination causes slow table scans and can skip records if rows are updated during the sweep. 

Instead, the service uses **Keyset Cursor Pagination**:

```sql
SELECT submission_id, session_id, tenant_code, submission_type, status, process_status, updated_at
FROM submissions
WHERE (
    status = 'failed'
    OR (status = 'processing' AND updated_at < now() - make_interval(mins => $7::int))
  )
  AND ($1::text IS NULL OR tenant_code = $1)
  AND updated_at < $3
  AND ($4::timestamptz IS NULL OR (updated_at, submission_id) > ($4, $5))
  AND (process_status->>'retry_attempt' IS NULL OR (process_status->>'retry_attempt')::int < $6)
ORDER BY updated_at ASC, submission_id ASC
LIMIT $2;
```

* **Cutoff (`updated_at < $3`)**: Set to the start timestamp of the sweep. New failures occurring during the sweep are ignored until the next sweep.
* **Keyset Bookmark `(updated_at, submission_id) > ($cursor_time, $cursor_id)`**: The query instantly jumps to the next row using the index.
* **`continue_as_new`**: After processing a batch of submissions, `BulkStepRetryWorkflow` calls `workflow.continue_as_new(...)` passing the cursor forward, ensuring Temporal event history remains bounded.

---

## 6. The 3 Retry Trigger Modes & API Endpoints

### 6.1. Mode 1: Manual Single Retry
Triggers retry for one failed submission.

* **Method**: `POST /v1/retry-step`
* **Route File**: [`app/api/routes/uploads.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/api/routes/uploads.py)
* **Headers**: `Authorization: Bearer <AUTH_TOKEN>`

#### Request Body:
```json
{
  "submission_id": "3281",
  "session_id": "odhwxrizppbt0bi4xguwo90oepn4fr0e",
  "tenant_code": "mitra"
}
```

#### Validations & Response:
* **404 Not Found**: If no record matches `(submission_id, tenant_code)`.
* **400 Bad Request**: If `status` is `'pending'` or `'success'` ("Only failed or stuck processing submissions can be retried").
* **400 Bad Request**: If no pipeline steps are configured for the `submission_type`.
* **409 Conflict**: If `status == 'processing'` and any workflow (`realtime-*`, `batch-child-*`, or `step-retry-*`) is currently actively `RUNNING` in Temporal.
* **409 Conflict**: If a retry workflow is already currently running (`WorkflowAlreadyStartedError`).
* **200 OK (Reconciled)**: If `status == 'processing'` and Temporal reports the workflow is `COMPLETED` (e.g., due to an earlier post-start write delay); PostgreSQL is reconciled to `status = 'success'` without triggering a duplicate workflow.
* **200 OK (Started)**:
  ```json
  {
    "workflow_id": "step-retry-3281-mitra",
    "submission_id": "3281",
    "session_id": "odhwxrizppbt0bi4xguwo90oepn4fr0e",
    "message": "Retry workflow started."
  }
  ```
  *Note on step resumption:* Failed rows resume from the first failed step (skipping previously successful steps). Stuck processing rows whose worker crashed before checkpointing progress will rerun from step 1.


---

### 6.2. Mode 2: Manual Bulk Retry
Triggers an asynchronous sweep across failed submissions.

* **Method**: `POST /v1/retry-step/all`
* **Headers**: `Authorization: Bearer <AUTH_TOKEN>`

#### Request Body (Tenant-Specific):
```json
{
  "tenant_code": "mitra"
}
```

#### Request Body (All Tenants):
```json
{}
```

#### Response (200 OK):
```json
{
  "workflow_id": "bulk-retry-sweep-20261103-120000",
  "tenant_code": "mitra",
  "message": "Bulk retry sweep started in background for failed submissions before 2026-11-03T12:00:00Z."
}
```
*Dispatches individual workflows with stable IDs (`step-retry-{id}-{tenant}`). Returns immediately without blocking the HTTP request.*

---

### 6.3. Mode 3: Automated Nightly Cron
Runs scheduled automated recovery during off-peak hours.

* **Workflow**: `TriggerBulkRetryWorkflow`
* **Schedule ID**: `daily-failed-submissions-retry`
* **Configuration**:
  - `ENABLE_RETRY_CRON`: `true` / `false` (default: `false`)
  - `RETRY_CRON_SCHEDULE`: Cron expression (default: `"0 2 * * *"`, every night at 2:00 AM)
* **Worker Schedule Update**: In [`app/temporal/worker.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/temporal/worker.py), restarting the worker updates the cron spec while strictly **preserving operator paused/unpaused state**:
  ```python
  def _apply_retry(inp):
      sched = inp.description.schedule
      sched.action = retry_schedule.action
      sched.spec = retry_schedule.spec
      return ScheduleUpdate(schedule=sched)
  await handle.update(_apply_retry)
  ```
* **Resilience & Timeouts**: `TriggerBulkRetryWorkflow` executes `trigger_bulk_retry_activity` with:
  - `start_to_close_timeout`: `2 hours` (ensures large backlogs can be processed without timing out)
  - `heartbeat_timeout`: `5 minutes` (heartbeats emitted after each page; allows cancellation if worker fails)
  - `maximum_attempts`: `1` (prevents overlapping concurrent cron sweeps)

---

## 7. The `max_retry_attempts = 5` Circuit Breaker

### Why It Exists:
If a submission failed because of **corrupted input data** (e.g. an unreadable file, missing mandatory columns, empty text), retrying it automatically every night would run forever, burning LLM tokens and API quotas.

### How It Works:
1. Every time a retry workflow fails, `retry_attempt` increments by 1 in `process_status`.
2. The bulk and cron fetch query contains:
   ```sql
   AND (process_status->>'retry_attempt' IS NULL OR (process_status->>'retry_attempt')::int < 5)
   ```
3. Once `retry_attempt` reaches `5`, automated sweeps stop picking up the record.
4. It is marked as dead-lettered, awaiting human inspection. (An operator can still trigger a single manual retry via `/v1/retry-step` once the underlying data issue is resolved).

---

## 8. Frequently Asked Questions (FAQ)

### Q1: Can we retry a submission after Temporal workflow history retention expires (e.g. 7 or 30 days)?
**Yes.** Temporal retention only purges internal execution logs in Temporal's cluster. Our retry system reads previous state and successful steps directly from **PostgreSQL** (`submissions.process_status`), which is stored permanently. The retry starts a clean workflow run that picks up from PostgreSQL.

### Q2: Why start a new workflow with ID `step-retry-{id}-{tenant}` instead of using Temporal Workflow Reset?
Temporal Workflow Reset (`temporal workflow reset`) rewinds event history, but it **fails completely if the workflow is older than the namespace retention window** (e.g. older than 7 days). Starting a deterministic resumption workflow (`step-retry-...`) works indefinitely, costs zero memory when idle, and provides a clean audit trail.

### Q3: What happens if two retries are triggered for the same submission at the same time?
The workflow ID is deterministic: `step-retry-{submission_id}-{tenant_code}`. Temporal enforces that only one execution per workflow ID can run concurrently. If a second attempt arrives, Temporal throws `WorkflowAlreadyStartedError`, and the API returns **`409 Conflict`**.

### Q4: If Step 2 fails, does Step 3 run?
**No.** Execution stops immediately at Step 2. Downstream steps are marked `"status": "skipped"` and saved to PostgreSQL.

---

## 9. File & Module Reference

| File | Responsibilities |
| :--- | :--- |
| [`app/temporal/workflows.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/temporal/workflows.py) | `ConfigDrivenProcessingWorkflow` (step skipping & retry attempt incrementing), `BulkStepRetryWorkflow` (keyset pagination & `continue_as_new`). |
| [`app/temporal/liveness.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/temporal/liveness.py) | `check_submission_temporal_state`: inspects candidate prefixes (`realtime`, `batch-child`, `step-retry`) for active execution or prior completion. |
| [`app/temporal/activities.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/temporal/activities.py) | `fetch_retryable_submissions_activity` (keyset cursor fetching, passing `process_status`), `update_status_activity`. |
| [`app/temporal/worker.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/temporal/worker.py) | Registers workflows/activities, handles `daily-failed-submissions-retry` cron schedule with state preservation. |
| [`app/api/routes/uploads.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/api/routes/uploads.py) | Endpoints: `POST /v1/retry-step` (single) and `POST /v1/retry-step/all` (bulk sweep). |
| [`app/database/operations.py`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/app/database/operations.py) | `fetch_retryable_submissions` (SQL with keyset cursor, grace period, and `retry_attempt < 5` filter), `claim_submission_for_retry`, `reconcile_completed_submission`. |
| [`schema.sql`](file:///home/rohan/workspace/shishalokam/newflows/analytics-service-1/schema.sql) | Table schema and partial index `idx_submissions_retryable`. |
