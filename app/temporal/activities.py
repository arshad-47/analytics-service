import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import insert_llm_log, get_submission_type_and_payload
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
    """
    limit = (params or {}).get("limit") or settings.BATCH_SIZE

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT submission_id, tenant_code, submission_type FROM submissions "
            "WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1",
            limit
        )
        results = []
        for row in rows:
            sub_id = row["submission_id"]
            tenant = row["tenant_code"]
            sub_type = row["submission_type"]
            # Load process steps dynamically from settings based on type
            process_steps = settings.get_process_config(sub_type)
            results.append({
                "submission_id": sub_id,
                "tenant_code": tenant,
                "submission_type": sub_type,
                "process_steps": process_steps
            })
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






