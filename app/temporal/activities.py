import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.feedback_operations import get_process_steps, mark_submission_config_failed
from app.database.operations import insert_llm_log, get_submission_type_and_payload
from app.services.feedback_constants import FEEDBACK_REPORT_TYPE

logger = logging.getLogger("analytics_service.temporal.activities")


def map_column_to_db_col(col: str, sub_type: str) -> str:
    col_lower = col.lower().strip()
    if col_lower == "challenges" and "story" in sub_type:
        return "challenge"
    if col_lower == "actionsteps":
        return "action_steps"
    return col

@activity.defn
async def update_status_activity(params: Dict[str, Any]) -> None:
    """
    Temporal activity to update the overall processing status of a submission in PostgreSQL.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    status = params["status"]
    process_status = params.get("process_status")

    async with db.pool.acquire() as conn:
        from app.database.operations import update_submission_status
        await update_submission_status(conn, submission_id, tenant_code, status, process_status)


@activity.defn
async def fetch_pending_submissions_activity(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Retrieves up to `limit` submissions currently in a 'pending' state (oldest first)
    and attaches their config-driven process steps. Bounded by `limit` (default
    settings.BATCH_SIZE) so a large pending queue is never loaded into memory in one
    go — BatchProcessingWorkflow calls this repeatedly in chunks instead.

    A feedback submission whose persona config cannot give its steps (no active config, or process_steps
    missing / invalid) must not stop the sweep, or one misconfigured persona would starve the story / discussion
    rows behind it: it is logged with its identifiers, marked 'failed' with the reason in its process_status
    (so it leaves the pending queue and the bulk retry picks it up once the config is fixed) and left out of the
    returned list. A database error is not caught: the activity fails and Temporal retries it. The persona
    lookup is memoized for the call.
    """
    requested_limit = (params or {}).get("limit")
    limit = settings.BATCH_SIZE if requested_limit is None else requested_limit
    persona_memo: Dict[Any, Any] = {}

    async with db.pool.acquire() as conn:
        results = []
        # BatchProcessingWorkflow treats a page shorter than `limit` as "the queue is drained", so a feedback row
        # that is marked failed here must not shorten the page (or empty it while later rows are still pending):
        # the page is refilled from the rows behind it. A returned row stays pending until its child workflow
        # starts, so the refill asks for `limit` plus the rows already handled and skips those by identity.
        handled = set()
        while True:
            asked = limit + len(handled)
            rows = await conn.fetch(
                "SELECT submission_id, tenant_code, submission_type FROM submissions "
                "WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1",
                asked
            )
            fresh = [row for row in rows if (row["tenant_code"], row["submission_id"]) not in handled]
            for row in fresh:
                if len(results) >= limit:
                    break
                sub_id = row["submission_id"]
                tenant = row["tenant_code"]
                sub_type = row["submission_type"]
                handled.add((tenant, sub_id))
                # Process steps: story/discussion from settings, feedback from its persona's upload config (config tables).
                try:
                    process_steps = await get_process_steps(conn, sub_type, sub_id, tenant, persona_memo)
                except ValueError as exc:
                    if FEEDBACK_REPORT_TYPE not in str(sub_type).lower():
                        raise
                    logger.error(
                        "Cannot resolve the process steps of submission '%s' (tenant '%s', type '%s'); marking it failed: %s",
                        sub_id, tenant, sub_type, exc,
                    )
                    await mark_submission_config_failed(conn, sub_id, tenant, str(exc))
                    continue
                results.append({
                    "submission_id": sub_id,
                    "tenant_code": tenant,
                    "submission_type": sub_type,
                    "process_steps": process_steps
                })
            if len(results) >= limit or len(rows) < asked or len(fresh) == 0:
                break
        return results


@activity.defn
async def fetch_failed_submissions_activity(params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Retrieves failed submissions for bulk retry.
    """
    params = params or {}
    tenant_code = params.get("tenant_code")  # None = all tenants
    limit = params.get("limit", settings.BATCH_SIZE)
    failed_before = params.get("failed_before")
    cursor_updated_at = params.get("cursor_updated_at")
    cursor_submission_id = params.get("cursor_submission_id")
    max_retry_attempts = params.get("max_retry_attempts", 5)

    persona_memo: Dict[Any, Any] = {}
    async with db.pool.acquire() as conn:
        from app.database.operations import fetch_retryable_failed_submissions

        # BulkStepRetryWorkflow stops at the first page shorter than `limit` and moves its cursor to the last
        # RETURNED row, so a feedback row left out here (its persona config cannot give steps) must not shorten the
        # page, nor may a page of such rows come back empty while later failures remain: the page is refilled from
        # the rows behind the cursor. The skipped rows stay failed and are looked at again by the next sweep.
        results = []
        while True:
            remaining = limit - len(results)
            rows = await fetch_retryable_failed_submissions(
                conn,
                tenant_code,
                remaining,
                failed_before=failed_before,
                cursor_updated_at=cursor_updated_at,
                cursor_submission_id=cursor_submission_id,
                max_retry_attempts=max_retry_attempts,
            )
            for row in rows:
                process_status = row.get("process_status")
                if isinstance(process_status, str):
                    try:
                        process_status = json.loads(process_status)
                    except Exception:
                        process_status = {}

                existing_steps = (process_status or {}).get("steps", [])
                sub_type = row["submission_type"]
                try:
                    process_steps = await get_process_steps(conn, sub_type, row["submission_id"], row["tenant_code"], persona_memo)
                except ValueError as exc:
                    if FEEDBACK_REPORT_TYPE not in str(sub_type).lower():
                        raise
                    # A feedback persona whose config cannot give steps: leave the submission failed (it is
                    # retried after the config is fixed) and keep sweeping the others.
                    logger.error(
                        "Cannot resolve the process steps of submission '%s' (tenant '%s', type '%s'): %s",
                        row["submission_id"], row["tenant_code"], sub_type, exc,
                    )
                    continue

                results.append({
                    "submission_id": row["submission_id"],
                    "session_id": row.get("session_id"),
                    "tenant_code": row["tenant_code"],
                    "submission_type": sub_type,
                    "process_steps": process_steps,
                    "existing_steps": existing_steps,
                    "process_status": process_status,
                    "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
                })
            if len(rows) < remaining or len(results) >= limit or len(rows) == 0:
                break
            # the whole scanned page was not dispatchable (or only part of it): continue after the last scanned row
            cursor_updated_at = rows[-1]["updated_at"]
            cursor_submission_id = rows[-1]["submission_id"]
        return results




