import asyncio
import contextlib
import json
import uuid
import logging
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
from app.database.feedback_operations import get_process_steps
from app.database.operations import (
    fetch_failed_submission_for_retry,
    fetch_retryable_failed_submissions,
    is_feedback_type,
)
from app.temporal.client import get_temporal_client
from app.temporal.workflows import ConfigDrivenProcessingWorkflow
from temporalio.exceptions import WorkflowAlreadyStartedError

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
        try:
            # feedback: the steps of its persona's upload config (config tables); story/discussion: settings
            process_steps = await get_process_steps(conn, sub_type, row["submission_id"], body.tenant_code)
        except ValueError as exc:
            # A server-side configuration problem (the persona's upload config), not a malformed request: 409.
            raise HTTPException(
                status_code=409,
                detail=f"Cannot resolve the process steps of submission '{body.submission_id}': {exc}",
            )
        existing_steps = (process_status or {}).get("steps", [])

        submission_id = row["submission_id"]
        session_id = row["session_id"]

    # DB connection released — now build payload and dispatch to Temporal

    # A feedback persona may legitimately have NO steps (process_steps: []: ingestion alone is the whole job); the
    # empty-step workflow then marks the submission 'success', which is how a failed one recovers. Any other type
    # without steps is an unsupported type and keeps the rejection.
    if len(process_steps) == 0 and not is_feedback_type(sub_type):
        raise HTTPException(
            status_code=400,
            detail=f"No process steps configured for submission type '{sub_type}'. Cannot retry."
        )

    # Stable workflow ID prevents concurrent duplicate retries for the same submission
    workflow_id = f"step-retry-{submission_id}-{body.tenant_code}"

    payload = {
        "submission_id": submission_id,
        "session_id": session_id,
        "tenant_code": body.tenant_code,
        "submission_type": sub_type,
        "process_steps": process_steps,
        "existing_steps": existing_steps,
        "process_status": process_status,
    }

    # Dispatch to Temporal using shared singleton client
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
        session_id=session_id,
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
    persona_memo: dict = {}  # persona lookup of feedback rows, remembered for the whole sweep

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
        steps_conn = None  # one connection per page, acquired only when a feedback row needs the config tables
        async with contextlib.AsyncExitStack() as page_resources:
            for row in rows:
                process_status = row.get("process_status") or {}
                if isinstance(process_status, str):
                    try:
                        process_status = json.loads(process_status)
                    except Exception:
                        process_status = {}

                sub_type = row["submission_type"]
                try:
                    if is_feedback_type(sub_type) and steps_conn is None:
                        steps_conn = await page_resources.enter_async_context(db.pool.acquire())
                    process_steps = await get_process_steps(
                        steps_conn, sub_type, row["submission_id"], row["tenant_code"], persona_memo
                    )
                except ValueError as exc:
                    logger.warning(f"[{sweep_id}] Skipping submission '{row['submission_id']}' — cannot resolve its process steps: {exc}")
                    continue

                if len(process_steps) == 0 and not is_feedback_type(sub_type):
                    logger.warning(f"[{sweep_id}] Skipping submission '{row['submission_id']}' — no steps configured for type '{sub_type}'")
                    continue

                tasks.append(
                    client.start_workflow(
                        ConfigDrivenProcessingWorkflow.run,
                        {
                            "submission_id": row["submission_id"],
                            "session_id": row.get("session_id"),
                            "tenant_code": row["tenant_code"],
                            "submission_type": sub_type,
                            "process_steps": process_steps,
                            "existing_steps": (process_status or {}).get("steps", []),
                            "process_status": process_status,
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

    Any holder of AUTH_TOKEN can trigger this endpoint including across all tenants
    (tenant_code=None). This is intentional — the endpoint is internal-only, protected
    by the shared AUTH_TOKEN.
    """
    cutoff = datetime.now(timezone.utc).isoformat()
    tenant_label = body.tenant_code if body.tenant_code is not None else "all"
    sweep_id = f"bulk-retry-sweep-{tenant_label}-{uuid.uuid4().hex[:8]}"

    background_tasks.add_task(
        _dispatch_all_retry_workflows,
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
