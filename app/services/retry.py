"""
Shared retry orchestration service used by both:
  - app/api/routes/uploads.py  (POST /v1/retry-step and POST /v1/retry-step/all)
  - app/temporal/activities.py (trigger_bulk_retry_activity → Temporal cron schedule)
"""
import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from temporalio.exceptions import WorkflowAlreadyStartedError

from app.config import settings
from app.database.db import db
from app.database.operations import (
    claim_submission_for_retry,
    fetch_retryable_submissions,
    reconcile_completed_submission,
    rollback_submission_claim,
)
from app.temporal.client import get_temporal_client
from app.temporal.liveness import (
    TemporalLivenessStatus,
    check_submission_temporal_state,
)

logger = logging.getLogger("analytics_service.services.retry")


@dataclass
class RetryOutcome:
    """Result of attempting to retry or reconcile a single submission."""

    status: str  # "started" | "reconciled" | "already_running" | "already_started" | "no_steps"
    workflow_id: str
    active_wf: Optional[str] = None


def _parse_process_status(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return dict(raw) if isinstance(raw, dict) else {}


async def retry_single_submission(
    client: Any,
    row: Dict[str, Any],
    *,
    liveness_sem: Optional[asyncio.Semaphore] = None,
) -> RetryOutcome:
    """
    Shared single-submission retry orchestration:
      1. Validate configured process steps for submission_type.
      2. If status == 'processing', check Temporal liveness (skip if RUNNING).
      3. Atomically claim retry_attempt in PostgreSQL.
      4. Start ConfigDrivenProcessingWorkflow with deterministic ID 'step-retry-{id}-{tenant}'.
    """
    from app.temporal.workflows import ConfigDrivenProcessingWorkflow

    sub_id = row["submission_id"]
    tenant_code = row["tenant_code"]
    sub_type = row["submission_type"]
    workflow_id = f"step-retry-{sub_id}-{tenant_code}"

    process_steps = settings.get_process_config(sub_type)
    if not process_steps:
        return RetryOutcome(status="no_steps", workflow_id=workflow_id)

    process_status = _parse_process_status(row.get("process_status"))
    existing_steps = process_status.get("steps", [])

    if row["status"] == "processing":
        if liveness_sem is not None:
            async with liveness_sem:
                liveness, active_wf = await check_submission_temporal_state(client, sub_id, tenant_code)
        else:
            liveness, active_wf = await check_submission_temporal_state(client, sub_id, tenant_code)

        if liveness == TemporalLivenessStatus.RUNNING:
            return RetryOutcome(status="already_running", workflow_id=workflow_id, active_wf=active_wf)
        if liveness == TemporalLivenessStatus.COMPLETED:
            async with db.pool.acquire() as conn:
                await reconcile_completed_submission(conn, sub_id, tenant_code)
            return RetryOutcome(
                status="reconciled",
                workflow_id=active_wf or workflow_id,
                active_wf=active_wf,
            )

    async with db.pool.acquire() as conn:
        claim_info = await claim_submission_for_retry(conn, sub_id, tenant_code)
        if not claim_info:
            return RetryOutcome(status="claim_rejected", workflow_id=workflow_id)
        process_status["retry_attempt"] = claim_info["retry_attempt"]

    payload = {
        "submission_id": sub_id,
        "session_id": row.get("session_id"),
        "tenant_code": tenant_code,
        "submission_type": sub_type,
        "process_steps": process_steps,
        "existing_steps": existing_steps,
        "process_status": process_status,
    }

    try:
        await client.start_workflow(
            ConfigDrivenProcessingWorkflow.run,
            payload,
            id=workflow_id,
            task_queue=settings.TEMPORAL_QUEUE,
        )
        return RetryOutcome(status="started", workflow_id=workflow_id)
    except WorkflowAlreadyStartedError:
        async with db.pool.acquire() as conn:
            await rollback_submission_claim(conn, sub_id, tenant_code)
        return RetryOutcome(status="already_started", workflow_id=workflow_id)


async def dispatch_all_retry_workflows(
    tenant_code: Optional[str],
    cutoff: Optional[str] = None,
    sweep_id: Optional[str] = None,
    on_page: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """
    Pages through ALL retryable submissions (failed, or stuck processing older than
    STUCK_PROCESSING_MIN_AGE_MINUTES) and orchestrates each via retry_single_submission.
    """
    if cutoff is None:
        cutoff = datetime.now(timezone.utc).isoformat()
    if sweep_id is None:
        tenant_label = tenant_code if tenant_code is not None else "all"
        sweep_id = f"bulk-retry-sweep-{tenant_label}-{uuid.uuid4().hex[:8]}"

    client = await get_temporal_client()
    total_success = 0
    total_reconciled = 0
    total_skipped = 0
    total_failed = 0
    page = 0
    cursor_updated_at = None
    cursor_submission_id = None
    sem = asyncio.Semaphore(10)

    while True:
        async with db.pool.acquire() as conn:
            rows = await fetch_retryable_submissions(
                conn,
                tenant_code,
                settings.BATCH_SIZE,
                failed_before=cutoff,
                cursor_updated_at=cursor_updated_at,
                cursor_submission_id=cursor_submission_id,
                stuck_processing_min_age_minutes=settings.STUCK_PROCESSING_MIN_AGE_MINUTES,
            )

        if not rows:
            break

        cursor_updated_at = rows[-1]["updated_at"]
        cursor_submission_id = rows[-1]["submission_id"]
        page += 1

        async def _process_row(row: Dict[str, Any]) -> str:
            sub_id = row["submission_id"]
            try:
                outcome = await retry_single_submission(client, row, liveness_sem=sem)
            except Exception as e:
                logger.warning(f"[{sweep_id}] Failed retry orchestration for '{sub_id}': {e}")
                return "failed"

            if outcome.status == "already_running":
                logger.info(f"[{sweep_id}] Skipping submission '{sub_id}' — actively running in Temporal ({outcome.active_wf})")
                return "skipped"
            if outcome.status == "reconciled":
                logger.info(f"[{sweep_id}] Reconciling submission '{sub_id}' — already COMPLETED in Temporal ({outcome.active_wf})")
                return "reconciled"
            if outcome.status in ("no_steps", "claim_rejected", "already_started"):
                return "skipped"
            return "started"

        results = await asyncio.gather(*[_process_row(r) for r in rows], return_exceptions=True)
        page_success = sum(1 for r in results if r == "started")
        page_reconciled = sum(1 for r in results if r == "reconciled")
        page_skipped = sum(1 for r in results if r == "skipped")
        page_failed = sum(1 for r in results if r == "failed" or isinstance(r, Exception))

        total_success += page_success
        total_reconciled += page_reconciled
        total_skipped += page_skipped
        total_failed += page_failed

        logger.info(
            f"[{sweep_id}] Page {page}: processed {len(rows)} "
            f"({page_success} started, {page_reconciled} reconciled, {page_skipped} skipped, {page_failed} failed) — "
            f"running total {total_success} started."
        )

        if on_page:
            try:
                on_page({
                    "page": page,
                    "started": total_success,
                    "reconciled": total_reconciled,
                    "skipped": total_skipped,
                    "failed": total_failed,
                })
            except Exception as e:
                logger.debug(f"[{sweep_id}] on_page callback failed: {e}")

        if len(rows) < settings.BATCH_SIZE:
            break

    logger.info(
        f"[{sweep_id}] Bulk retry sweep complete for tenant_code={tenant_code or 'all'}: "
        f"{total_success} started, {total_reconciled} reconciled, {total_skipped} skipped, {total_failed} failed across {page} page(s)."
    )

    return {
        "sweep_id": sweep_id,
        "started": total_success,
        "reconciled": total_reconciled,
        "skipped": total_skipped,
        "failed": total_failed,
        "pages": page,
    }

