import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import insert_llm_log, get_submission_type_and_payload

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

    async with db.pool.acquire() as conn:
        from app.database.operations import fetch_retryable_failed_submissions
        rows = await fetch_retryable_failed_submissions(
            conn,
            tenant_code,
            limit,
            failed_before=failed_before,
            cursor_updated_at=cursor_updated_at,
            cursor_submission_id=cursor_submission_id,
            max_retry_attempts=max_retry_attempts,
        )

        results = []
        for row in rows:
            process_status = row.get("process_status")
            if isinstance(process_status, str):
                try:
                    process_status = json.loads(process_status)
                except Exception:
                    process_status = {}

            existing_steps = (process_status or {}).get("steps", [])
            sub_type = row["submission_type"]
            process_steps = settings.get_process_config(sub_type)

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
        return results




