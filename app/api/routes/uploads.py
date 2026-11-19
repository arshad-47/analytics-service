import logging
import uuid
from datetime import datetime, timezone
from typing import Optional
import secrets
from fastapi import APIRouter, Depends, Form, UploadFile, File, BackgroundTasks, HTTPException, Request
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials

from app.api.deps import security as bearer_security, verify_auth_token
from app.api.validators.uploads import (
    validate_report_type,
    validate_extension,
    validate_file_bytes,
    validate_source,
    validate_feedback_tenant_code,
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
from app.services.feedback_constants import FEEDBACK_REPORT_TYPE, MAX_UPLOAD_REQUEST_BYTES
from app.database.db import db
from app.database.operations import fetch_retryable_submission_for_retry
from app.services.retry import (
    dispatch_all_retry_workflows,
    retry_single_submission,
)
from app.temporal.client import get_temporal_client

logger = logging.getLogger("analytics_service.api.routes.uploads")

uploads_router = APIRouter(prefix="/v1")


class UploadPreflightRoute(APIRoute):
    """
    The route class of POST /v1/upload/: checks the request BEFORE the multipart body is read.

    FastAPI parses the whole multipart form (spooling every file to a temporary file) before it runs the route's
    dependencies, so on a plain route an unauthenticated 32 MB POST is fully received before the bearer token or the
    size cap can reject it. Here, ahead of the body:
      * a missing or wrong bearer token is refused (same token check as verify_auth_token);
      * a Content-Length above MAX_UPLOAD_REQUEST_BYTES is refused with 413, and a body that grows past it while it is
        being received (no Content-Length, chunked) is cut off with 413.
    The persona's own cap (max_csv_upload_bytes of its config) is still applied by the route after its config is
    resolved; this ceiling only bounds what is ever buffered for any caller.
    """

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request):
            credentials = await bearer_security(request)   # 403/401 as the dependency would, but before the body
            if credentials is None or not secrets.compare_digest(credentials.credentials, settings.AUTH_TOKEN):
                raise HTTPException(status_code=401, detail="Unauthorized: Invalid token")
            limit = MAX_UPLOAD_REQUEST_BYTES
            declared = request.headers.get("content-length")
            if declared is not None and declared.isdigit() and int(declared) > limit:
                raise HTTPException(status_code=413, detail="Uploaded file is too large")
            received = 0
            inner_receive = request.receive

            async def counting_receive():
                nonlocal received
                message = await inner_receive()
                if message["type"] == "http.request":
                    received += len(message.get("body", b""))
                    if received > limit:
                        raise HTTPException(status_code=413, detail="Uploaded file is too large")
                return message

            return await original(Request(request.scope, counting_receive, request._send))

        return handler

# story / discussion uploads keep their historical default tenant; feedback has NO default (tenant_code and source are
# required form fields of a feedback upload, so a forgotten field can never silently select another program's tenant
# or source).
STORY_DISCUSSION_DEFAULT_TENANT_CODE = "mitra"


def _missing_form_field(name: str) -> HTTPException:
    """The 422 a required form field would give (same body shape as FastAPI's own validation error)."""
    return HTTPException(
        status_code=422,
        detail=[{"type": "missing", "loc": ["body", name], "msg": f"Field required when report_type is feedback"}],
    )


# ==========================================
# CSV Pipeline Endpoints
# ==========================================

async def upload_report(
    background_tasks: BackgroundTasks,
    report_type: str = Form(...),
    program_name: str = Form(...),
    leader_category: str = Form(...),
    tenant_code: Optional[str] = Form(default=None),
    source: Optional[str] = Form(default=None),
    file: UploadFile = File(...),
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Upload a CSV report file.

    The file is stored in GCS and a tracking record is created with
    status='pending'. Processing (column validation, Kafka publishing)
    runs in the background.

    report_type='feedback' takes its size cap from the persona's upload config (`max_csv_upload_bytes`) and
    REQUIRES `tenant_code` and `source` (no defaults). `source` is a free label ([a-z0-9_], at most 40 characters)
    that must have an ACTIVE feedback_upload config for the program + persona of the tenant; the config's `adapter`
    decides how the file is read ('conversation_rows': one row per message, one file per persona; 'wide_form': the
    original wide form export, reshaped by the service, only responses not already stored are processed).
    story/discussion default `tenant_code` to 'mitra' and ignore `source`.
    """
    # Pure request-shape checks
    report_type = validate_report_type(report_type)
    validate_extension(file.filename)
    feedback_target = None
    if report_type == FEEDBACK_REPORT_TYPE:
        if tenant_code is None or len(tenant_code.strip()) == 0:
            raise _missing_form_field("tenant_code")
        if source is None or len(source.strip()) == 0:
            raise _missing_form_field("source")
        tenant_code = validate_feedback_tenant_code(tenant_code)
        source = validate_source(source)
        # The cap is part of the persona's config: resolve it first (once; handle_upload reuses it), so the
        # size check happens before the file is stored and the read stays bounded.
        feedback_target = await upload_service.load_feedback_target(program_name, leader_category, tenant_code, source)
        cap = feedback_target["max_csv_upload_bytes"]
    else:
        if tenant_code is None or len(tenant_code) == 0:
            tenant_code = STORY_DISCUSSION_DEFAULT_TENANT_CODE
        cap = max_upload_bytes(report_type)

    file_bytes = await file.read(cap + 1)
    validate_file_bytes(file_bytes, report_type, cap)

    # Delegate to domain service
    return await upload_service.handle_upload(
        report_type=report_type,
        program_name=program_name,
        leader_category=leader_category,
        tenant_code=tenant_code,
        source=source if report_type == FEEDBACK_REPORT_TYPE else None,
        file_name=file.filename,
        file_bytes=file_bytes,
        background_tasks=background_tasks,
        feedback_target=feedback_target,
    )


uploads_router.add_api_route(
    "/upload/", upload_report, methods=["POST"], response_model=UploadResponse, response_model_exclude_none=True,
    tags=["CSV Pipeline"], route_class_override=UploadPreflightRoute,
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
    if outcome.status == "config_error":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot resolve the process steps of submission '{submission_id}': {outcome.error}",
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
