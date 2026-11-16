import logging
import uuid
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, Form, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app.api.deps import verify_auth_token
from app.api.validators.uploads import (
    validate_report_type,
    validate_extension,
    validate_file_bytes,
    validate_source,
    max_upload_bytes,
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
from app.services.feedback_constants import FEEDBACK_REPORT_TYPE
from app.database.db import db
from app.database.operations import fetch_retryable_submission_for_retry
from app.services.retry import (
    dispatch_all_retry_workflows,
    retry_single_submission,
)
from app.temporal.client import get_temporal_client

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
    source: str = Form(default="mitra"),
    file: UploadFile = File(...),
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Upload a CSV report file.

    The file is stored in GCS and a tracking record is created with
    status='pending'. Processing (column validation, Kafka publishing)
    runs in the background.

    report_type='feedback' has its own, higher size cap and an optional `source` field:
    'mitra' (default; the raw bot conversation CSV, one file per persona) or 'google_forms'
    (the original wide Google Forms CSV, reshaped by the service; only responses not already
    stored are processed). story/discussion ignore `source`.
    """
    # Pure request-shape checks
    report_type = validate_report_type(report_type)
    validate_extension(file.filename)
    if report_type == FEEDBACK_REPORT_TYPE:
        source = validate_source(source)

    file_bytes = await file.read(max_upload_bytes(report_type) + 1)
    validate_file_bytes(file_bytes, report_type)

    # Delegate to domain service
    return await upload_service.handle_upload(
        report_type=report_type,
        program_name=program_name,
        leader_category=leader_category,
        tenant_code=tenant_code,
        source=source,
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
    Retry a single failed or stuck submission. Automatically resumes from the failed
    step using the persisted process status in the database.
    """
    async with db.pool.acquire() as conn:
        row = await fetch_retryable_submission_for_retry(
            conn, body.submission_id, body.session_id, body.tenant_code
        )

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Submission not found with submission_id='{body.submission_id}', session_id='{body.session_id}', and tenant_code='{body.tenant_code}'."
            )

        # Status check: Allow 'failed' and stuck 'processing' submissions to be retried
        if row["status"] not in ("failed", "processing"):
            raise HTTPException(
                status_code=400,
                detail=f"Submission status is '{row['status']}'. Only failed or stuck processing submissions can be retried."
            )

        submission_id = row["submission_id"]
        session_id = row["session_id"]
        sub_type = row["submission_type"]

    # DB connection released — delegate liveness, reconciliation, claim & dispatch to shared service
    client = await get_temporal_client()
    outcome = await retry_single_submission(client, row)

    if outcome.status == "no_steps":
        raise HTTPException(
            status_code=400,
            detail=f"No process steps configured for submission type '{sub_type}'. Cannot retry."
        )
    if outcome.status == "already_running":
        raise HTTPException(
            status_code=409,
            detail=f"Submission '{submission_id}' is currently actively executing in Temporal (workflow: '{outcome.active_wf}')."
        )
    if outcome.status == "reconciled":
        return StepRetryResponse(
            workflow_id=outcome.workflow_id,
            submission_id=submission_id,
            session_id=session_id,
            message=f"Submission '{submission_id}' was already completed in Temporal; status reconciled to 'success'.",
        )
    if outcome.status == "claim_rejected":
        raise HTTPException(
            status_code=409,
            detail=f"Submission '{submission_id}' cannot be claimed for retry (exceeded max retry attempts or invalid status).",
        )
    if outcome.status == "already_started":
        raise HTTPException(
            status_code=409,
            detail=f"A retry workflow for submission '{submission_id}' is already running.",
        )

    return StepRetryResponse(
        workflow_id=outcome.workflow_id,
        submission_id=submission_id,
        session_id=session_id,
        message="Retry workflow started.",
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

    Any holder of AUTH_TOKEN can trigger this endpoint including across all tenants
    (tenant_code=None). This is intentional — the endpoint is internal-only, protected
    by the shared AUTH_TOKEN.
    """
    cutoff = datetime.now(timezone.utc).isoformat()
    tenant_label = body.tenant_code if body.tenant_code is not None else "all"
    sweep_id = f"bulk-retry-sweep-{tenant_label}-{uuid.uuid4().hex[:8]}"

    background_tasks.add_task(
        dispatch_all_retry_workflows,
        body.tenant_code,
        cutoff,
        sweep_id,
    )

    logger.info(
        f"Bulk retry sweep '{sweep_id}' enqueued for tenant_code={tenant_label} "
        f"(cutoff={cutoff}, batch_size={settings.BATCH_SIZE})."
    )

    return BulkRetryResponse(
        workflow_id=sweep_id,
        message=(
            f"Bulk retry sweep started (tenant_code={tenant_label}). "
            f"Individual step-retry-* workflows will appear in Temporal as they are dispatched. "
            f"Track progress in logs with sweep_id='{sweep_id}'."
        ),
    )
