import asyncio
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
from app.services.retry import dispatch_all_retry_workflows

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
async def trigger_bulk_retry_activity(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Called by the Temporal cron schedule (TriggerBulkRetryWorkflow) to run the same
    bulk retry sweep as POST /v1/retry-step/all — liveness check, reconciliation,
    claim, and client.start_workflow — all inside this activity (no HTTP call needed).
    """
    params = params or {}
    tenant_code = params.get("tenant_code")
    result = await dispatch_all_retry_workflows(tenant_code, on_page=activity.heartbeat)
    logger.info("Cron bulk retry sweep complete: sweep_id=%s", result.get("sweep_id"))
    return result





