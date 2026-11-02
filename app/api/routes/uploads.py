import asyncio
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, Form, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app.api.deps import verify_auth_token
from app.api.validators.uploads import (
    validate_report_type,
    validate_extension,
    validate_file_bytes,
)
from app.api.models.uploads import UploadResponse
from app.api.models.submissions import (
    StepRetryRequest,
    StepRetryResponse,
    BulkRetryRequest,
    BulkRetryResponse,
)
from app.api.services import uploads as upload_service
from app.config import settings
from app.database.db import db
from app.database.operations import (
    fetch_failed_submission_for_retry,
    fetch_retryable_failed_submissions,
)
from app.temporal.client import get_temporal_client
from app.temporal.workflows import ConfigDrivenProcessingWorkflow
from temporalio.exceptions import WorkflowAlreadyStartedError

logger = logging.getLogger("analytics_service.api.routes.uploads")

uploads_router = APIRouter(prefix="/v1")


# ==========================================
# CSV Pipeline Endpoints
# ==========================================

@uploads_router.post("/upload/", response_model=UploadResponse, tags=["CSV Pipeline"])
async def upload_report(
    background_tasks: BackgroundTasks,
    report_type: str = Form(...),
    program_name: str = Form(...),
    leader_category: str = Form(...),
    tenant_code: str = Form(default="mitra"),
    file: UploadFile = File(...),
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Upload a CSV report file.

    The file is stored in GCS and a tracking record is created with
    status='pending'. Processing (column validation, Kafka publishing)
    runs in the background.
    """
    # Pure request-shape checks
    report_type = validate_report_type(report_type)
    validate_extension(file.filename)

    file_bytes = await file.read(settings.MAX_CSV_UPLOAD_BYTES + 1)
    validate_file_bytes(file_bytes)

    # Delegate to domain service
    return await upload_service.handle_upload(
        report_type=report_type,
        program_name=program_name,
        leader_category=leader_category,
        tenant_code=tenant_code,
        file_name=file.filename,
        file_bytes=file_bytes,
        background_tasks=background_tasks,
    )


@uploads_router.post("/process/csv/{record_id}", tags=["CSV Pipeline"])
async def push_record(
    record_id: int,
    background_tasks: BackgroundTasks,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Manually trigger processing for a specific csv_upload record in pending status.
    """
    return await upload_service.handle_push(record_id, background_tasks)


# ==========================================
# Step Retry Endpoints
# ==========================================

@uploads_router.post("/retry-step", response_model=StepRetryResponse, tags=["Step Retry"])
async def retry_step(
    body: StepRetryRequest,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Retry a single failed submission. Automatically resumes from the failed
    step using the persisted process status in the database.
    """
    async with db.pool.acquire() as conn:
        row = await fetch_failed_submission_for_retry(
            conn, body.submission_id, body.session_id, body.tenant_code
        )

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Submission not found with submission_id='{body.submission_id}', session_id='{body.session_id}', and tenant_code='{body.tenant_code}'."
            )

        # Status check
        if row["status"] != "failed":
            raise HTTPException(
                status_code=400,
                detail=f"Submission status is '{row['status']}'. Only failed submissions can be retried."
            )

        # Parse process_status
        process_status = row.get("process_status") or {}
        if isinstance(process_status, str):
            try:
                process_status = json.loads(process_status)
            except Exception:
                process_status = {}

        sub_type = row["submission_type"]
        process_steps = settings.get_process_config(sub_type)
        existing_steps = (process_status or {}).get("steps", [])

        submission_id = row["submission_id"]
        # Stable workflow ID prevents concurrent duplicate retries for the same submission
        workflow_id = f"step-retry-{submission_id}-{body.tenant_code}"

        payload = {
            "submission_id": submission_id,
            "session_id": row["session_id"],
            "tenant_code": body.tenant_code,
            "submission_type": sub_type,
            "process_steps": process_steps,
            "existing_steps": existing_steps,
        }

        # 5. Dispatch to Temporal using shared singleton client
        client = await get_temporal_client()
        try:
            await client.start_workflow(
                ConfigDrivenProcessingWorkflow.run,
                payload,
                id=workflow_id,
                task_queue=settings.TEMPORAL_QUEUE,
            )
        except WorkflowAlreadyStartedError:
            raise HTTPException(
                status_code=409,
                detail=f"A retry workflow for submission '{submission_id}' is already running.",
            )

        return StepRetryResponse(
            workflow_id=workflow_id,
            submission_id=submission_id,
            session_id=row["session_id"],
            message="Retry workflow started.",
        )


async def _dispatch_all_retry_workflows(
    tenant_code: Optional[str],
    cutoff: str,
    sweep_id: str,
) -> None:
    """
    Background worker: pages through ALL retryable failed submissions (100 at a time)
    and fires an individual ConfigDrivenProcessingWorkflow for each one.
    Only submissions that failed BEFORE this sweep started are picked up,
    so failures that occur during the sweep are not re-fetched mid-run.
    No parent workflow is created in Temporal — only individual step-retry-* entries appear.
    """
    client = await get_temporal_client()
    total_success = 0
    total_failed = 0
    page = 0
    cursor_updated_at = None
    cursor_submission_id = None

    while True:
        async with db.pool.acquire() as conn:
            rows = await fetch_retryable_failed_submissions(
                conn,
                tenant_code,
                settings.BATCH_SIZE,
                failed_before=cutoff,
                cursor_updated_at=cursor_updated_at,
                cursor_submission_id=cursor_submission_id,
            )

        if not rows:
            break

        # Advance keyset cursor to the last row of the current page
        cursor_updated_at = rows[-1]["updated_at"]
        cursor_submission_id = rows[-1]["submission_id"]

        page += 1
        tasks = []
        for row in rows:
            process_status = row.get("process_status") or {}
            if isinstance(process_status, str):
                try:
                    process_status = json.loads(process_status)
                except Exception:
                    process_status = {}

            sub_type = row["submission_type"]
            tasks.append(
                client.start_workflow(
                    ConfigDrivenProcessingWorkflow.run,
                    {
                        "submission_id": row["submission_id"],
                        "session_id": row.get("session_id"),
                        "tenant_code": row["tenant_code"],
                        "submission_type": sub_type,
                        "process_steps": settings.get_process_config(sub_type),
                        "existing_steps": (process_status or {}).get("steps", []),
                    },
                    # Stable ID: prevents duplicate retries if called twice
                    id=f"step-retry-{row['submission_id']}-{row['tenant_code']}",
                    task_queue=settings.TEMPORAL_QUEUE,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        page_success = sum(1 for r in results if not isinstance(r, Exception))
        page_failed = len(results) - page_success
        for r in results:
            if isinstance(r, Exception) and not isinstance(r, WorkflowAlreadyStartedError):
                logger.warning(f"[{sweep_id}] Failed to dispatch retry workflow: {r}")

        total_success += page_success
        total_failed += page_failed
        logger.info(
            f"[{sweep_id}] Page {page}: dispatched {len(rows)} "
            f"({page_success} ok, {page_failed} skipped/failed) — running total {total_success}."
        )

        if len(rows) < settings.BATCH_SIZE:
            # Last page — no more rows to fetch
            break

    logger.info(
        f"[{sweep_id}] Bulk retry sweep complete for tenant_code={tenant_code or 'all'}: "
        f"{total_success} started, {total_failed} skipped/failed across {page} page(s)."
    )


@uploads_router.post("/retry-step/all", response_model=BulkRetryResponse, tags=["Step Retry"])
async def retry_all_failed(
    body: BulkRetryRequest,
    background_tasks: BackgroundTasks,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Enqueue a background sweep that pages through ALL retryable failed submissions
    (BATCH_SIZE at a time) and fires an individual ConfigDrivenProcessingWorkflow
    for each one. Only individual step-retry-* workflows appear in Temporal UI.
    Returns immediately with a sweep_id you can use to correlate logs.
    """
    cutoff = datetime.now(timezone.utc).isoformat()
    sweep_id = f"bulk-retry-sweep-{body.tenant_code or 'all'}-{uuid.uuid4().hex[:8]}"

    background_tasks.add_task(
        _dispatch_all_retry_workflows,
        body.tenant_code,
        cutoff,
        sweep_id,
    )

    logger.info(
        f"Bulk retry sweep '{sweep_id}' enqueued for tenant_code={body.tenant_code or 'all'} "
        f"(cutoff={cutoff}, batch_size={settings.BATCH_SIZE})."
    )

    return BulkRetryResponse(
        workflow_id=sweep_id,
        message=(
            f"Bulk retry sweep started (tenant_code={body.tenant_code or 'all'}). "
            f"Individual step-retry-* workflows will appear in Temporal as they are dispatched. "
            f"Track progress in logs with sweep_id='{sweep_id}'."
        ),
    )
