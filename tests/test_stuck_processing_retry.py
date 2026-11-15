import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta
from fastapi import HTTPException
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from app.temporal.liveness import (
    check_submission_temporal_state,
    TemporalLivenessStatus,
)
from app.api.models.submissions import StepRetryRequest
from app.api.routes.uploads import retry_step
from app.services.retry import dispatch_all_retry_workflows
from app.database.db import db


def _mock_db_pool():
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return mock_pool, mock_conn


# =========================================================================
# 1. Tests for check_submission_temporal_state
# =========================================================================

def test_liveness_detects_running_workflow():
    """If realtime-, batch-child-, or step-retry- is RUNNING, return RUNNING."""
    async def _run():
        client = MagicMock()

        def get_handle(wf_id: str):
            handle = MagicMock()
            if wf_id.startswith("realtime-"):
                handle.describe = AsyncMock(side_effect=RPCError("not found", RPCStatusCode.NOT_FOUND, b""))
            elif wf_id.startswith("batch-child-"):
                desc = MagicMock(status=WorkflowExecutionStatus.RUNNING)
                handle.describe = AsyncMock(return_value=desc)
            else:
                handle.describe = AsyncMock(side_effect=RPCError("not found", RPCStatusCode.NOT_FOUND, b""))
            return handle

        client.get_workflow_handle.side_effect = get_handle

        status, active_id = await check_submission_temporal_state(client, "sub123", "mitra")
        assert status == TemporalLivenessStatus.RUNNING
        assert active_id == "batch-child-sub123-mitra"

    asyncio.run(_run())


def test_liveness_detects_completed_workflow():
    """If a workflow is COMPLETED and none is RUNNING, return COMPLETED."""
    async def _run():
        client = MagicMock()

        def get_handle(wf_id: str):
            handle = MagicMock()
            if wf_id.startswith("realtime-"):
                desc = MagicMock(status=WorkflowExecutionStatus.COMPLETED)
                handle.describe = AsyncMock(return_value=desc)
            else:
                handle.describe = AsyncMock(side_effect=RPCError("not found", RPCStatusCode.NOT_FOUND, b""))
            return handle

        client.get_workflow_handle.side_effect = get_handle

        status, completed_id = await check_submission_temporal_state(client, "sub123", "mitra")
        assert status == TemporalLivenessStatus.COMPLETED
        assert completed_id == "realtime-sub123-mitra"

    asyncio.run(_run())


def test_liveness_returns_retryable_when_failed_or_terminated():
    """If workflows are FAILED, TERMINATED, or NOT_FOUND, return RETRYABLE."""
    async def _run():
        client = MagicMock()

        def get_handle(wf_id: str):
            handle = MagicMock()
            if wf_id.startswith("realtime-"):
                desc = MagicMock(status=WorkflowExecutionStatus.FAILED)
                handle.describe = AsyncMock(return_value=desc)
            elif wf_id.startswith("step-retry-"):
                desc = MagicMock(status=WorkflowExecutionStatus.TERMINATED)
                handle.describe = AsyncMock(return_value=desc)
            else:
                handle.describe = AsyncMock(side_effect=RPCError("not found", RPCStatusCode.NOT_FOUND, b""))
            return handle

        client.get_workflow_handle.side_effect = get_handle

        status, active_id = await check_submission_temporal_state(client, "sub123", "mitra")
        assert status == TemporalLivenessStatus.RETRYABLE
        assert active_id is None

    asyncio.run(_run())


def test_liveness_fails_closed_on_unexpected_rpc_error():
    """Any RPC error other than NOT_FOUND is re-raised to prevent accidental retries."""
    async def _run():
        client = MagicMock()
        handle = MagicMock()
        handle.describe = AsyncMock(side_effect=RPCError("service unavailable", RPCStatusCode.UNAVAILABLE, b""))
        client.get_workflow_handle.return_value = handle

        with pytest.raises(RPCError) as exc_info:
            await check_submission_temporal_state(client, "sub123", "mitra")
        assert exc_info.value.status == RPCStatusCode.UNAVAILABLE

    asyncio.run(_run())


# =========================================================================
# 2. Tests for retry_step Endpoint
# =========================================================================

def test_retry_step_rejects_pending_or_success():
    """Submissions in 'pending' or 'success' must return HTTP 400."""
    async def _run():
        fake_row = {
            "submission_id": "100",
            "session_id": "sess-100",
            "tenant_code": "mitra",
            "submission_type": "story",
            "status": "pending",
            "process_status": {},
        }

        req = StepRetryRequest(submission_id="100", session_id="sess-100", tenant_code="mitra")
        mock_pool, _ = _mock_db_pool()
        db.pool = mock_pool
        try:
            with patch("app.api.routes.uploads.fetch_retryable_submission_for_retry", new=AsyncMock(return_value=fake_row)):
                with pytest.raises(HTTPException) as exc_info:
                    await retry_step(body=req, _token=MagicMock())
                assert exc_info.value.status_code == 400
                assert "Only failed or stuck processing" in exc_info.value.detail
        finally:
            db.pool = None

    asyncio.run(_run())


def test_retry_step_returns_409_if_workflow_is_running():
    """When status is 'processing' and a workflow is running, return HTTP 409 Conflict."""
    async def _run():
        fake_row = {
            "submission_id": "100",
            "session_id": "sess-100",
            "tenant_code": "mitra",
            "submission_type": "story",
            "status": "processing",
            "process_status": {},
        }

        req = StepRetryRequest(submission_id="100", session_id="sess-100", tenant_code="mitra")
        mock_pool, _ = _mock_db_pool()
        db.pool = mock_pool
        try:
            with patch("app.api.routes.uploads.fetch_retryable_submission_for_retry", new=AsyncMock(return_value=fake_row)):
                with patch("app.api.routes.uploads.get_temporal_client", new=AsyncMock()):
                    with patch(
                        "app.services.retry.check_submission_temporal_state",
                        new=AsyncMock(return_value=(TemporalLivenessStatus.RUNNING, "realtime-100-mitra"))
                    ):
                        with pytest.raises(HTTPException) as exc_info:
                            await retry_step(body=req, _token=MagicMock())
                        assert exc_info.value.status_code == 409
                        assert "actively executing in Temporal" in exc_info.value.detail
        finally:
            db.pool = None

    asyncio.run(_run())


def test_retry_step_reconciles_if_temporal_is_completed():
    """When status is 'processing' and Temporal is COMPLETED, reconcile DB and return 200 without retrying."""
    async def _run():
        fake_row = {
            "submission_id": "100",
            "session_id": "sess-100",
            "tenant_code": "mitra",
            "submission_type": "story",
            "status": "processing",
            "process_status": {},
        }

        req = StepRetryRequest(submission_id="100", session_id="sess-100", tenant_code="mitra")
        mock_reconcile = AsyncMock()
        mock_pool, _ = _mock_db_pool()
        db.pool = mock_pool
        try:
            with patch("app.api.routes.uploads.fetch_retryable_submission_for_retry", new=AsyncMock(return_value=fake_row)):
                with patch("app.api.routes.uploads.get_temporal_client", new=AsyncMock()):
                    with patch(
                        "app.services.retry.check_submission_temporal_state",
                        new=AsyncMock(return_value=(TemporalLivenessStatus.COMPLETED, "realtime-100-mitra"))
                    ):
                        with patch("app.services.retry.reconcile_completed_submission", new=mock_reconcile):
                            res = await retry_step(body=req, _token=MagicMock())
                            assert res.submission_id == "100"
                            assert "status reconciled to 'success'" in res.message
                            mock_reconcile.assert_awaited_once()
        finally:
            db.pool = None

    asyncio.run(_run())


def test_retry_step_claims_and_starts_workflow_for_dead_processing_row():
    """A stuck processing row with RETRYABLE liveness is claimed and started."""
    async def _run():
        fake_row = {
            "submission_id": "100",
            "session_id": "sess-100",
            "tenant_code": "mitra",
            "submission_type": "story",
            "status": "processing",
            "process_status": {},
        }

        req = StepRetryRequest(submission_id="100", session_id="sess-100", tenant_code="mitra")
        mock_claim = AsyncMock(return_value={"retry_attempt": 1, "process_status": {"retry_attempt": 1}})
        mock_temporal_client = MagicMock()
        mock_temporal_client.start_workflow = AsyncMock()
        mock_pool, _ = _mock_db_pool()
        db.pool = mock_pool
        try:
            with patch("app.api.routes.uploads.fetch_retryable_submission_for_retry", new=AsyncMock(return_value=fake_row)):
                with patch("app.api.routes.uploads.get_temporal_client", new=AsyncMock(return_value=mock_temporal_client)):
                    with patch(
                        "app.services.retry.check_submission_temporal_state",
                        new=AsyncMock(return_value=(TemporalLivenessStatus.RETRYABLE, None))
                    ):
                        with patch("app.services.retry.claim_submission_for_retry", new=mock_claim):
                            res = await retry_step(body=req, _token=MagicMock())
                            assert res.workflow_id == "step-retry-100-mitra"
                            assert res.message == "Retry workflow started."
                            mock_claim.assert_awaited_once()
                            mock_temporal_client.start_workflow.assert_awaited_once()
        finally:
            db.pool = None

    asyncio.run(_run())


# =========================================================================
# 3. Tests for Bulk Sweep (_dispatch_all_retry_workflows)
# =========================================================================

def test_bulk_sweep_skips_running_and_dispatches_retryable():
    """Bulk sweep skips running rows, reconciles completed rows, and starts retries for retryable ones."""
    async def _run():
        rows = [
            # Row 1: Stuck processing, but actually RUNNING in Temporal
            {
                "submission_id": "101",
                "session_id": "s101",
                "tenant_code": "mitra",
                "submission_type": "story",
                "status": "processing",
                "process_status": {},
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=20),
            },
            # Row 2: Stuck processing, COMPLETED in Temporal
            {
                "submission_id": "102",
                "session_id": "s102",
                "tenant_code": "mitra",
                "submission_type": "story",
                "status": "processing",
                "process_status": {},
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=20),
            },
            # Row 3: Failed row, ready for retry
            {
                "submission_id": "103",
                "session_id": "s103",
                "tenant_code": "mitra",
                "submission_type": "story",
                "status": "failed",
                "process_status": {},
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=5),
            },
        ]

        mock_client = MagicMock()
        mock_client.start_workflow = AsyncMock()

        async def mock_liveness(client, sub_id, t_code):
            if sub_id == "101":
                return TemporalLivenessStatus.RUNNING, "realtime-101-mitra"
            elif sub_id == "102":
                return TemporalLivenessStatus.COMPLETED, "realtime-102-mitra"
            return TemporalLivenessStatus.RETRYABLE, None

        mock_claim = AsyncMock(return_value={"retry_attempt": 1, "process_status": {"retry_attempt": 1}})
        mock_reconcile = AsyncMock()
        mock_pool, _ = _mock_db_pool()
        db.pool = mock_pool

        try:
            with patch("app.services.retry.get_temporal_client", new=AsyncMock(return_value=mock_client)):
                with patch("app.services.retry.fetch_retryable_submissions", new=AsyncMock(side_effect=[rows, []])):
                    with patch("app.services.retry.check_submission_temporal_state", new=mock_liveness):
                        with patch("app.services.retry.claim_submission_for_retry", new=mock_claim):
                            with patch("app.services.retry.reconcile_completed_submission", new=mock_reconcile):
                                await dispatch_all_retry_workflows(
                                    tenant_code="mitra",
                                    cutoff=datetime.now(timezone.utc).isoformat(),
                                    sweep_id="test-sweep"
                                )

                                # 101 was skipped (RUNNING)
                                # 102 was reconciled
                                assert mock_reconcile.await_count == 1
                                recon_args = mock_reconcile.await_args[0]
                                assert recon_args[1] == "102"
                                assert recon_args[2] == "mitra"

                                # Only 103 was dispatched to start_workflow
                                assert mock_client.start_workflow.await_count == 1
                                call_args = mock_client.start_workflow.call_args[1]
                                assert call_args["id"] == "step-retry-103-mitra"
        finally:
            db.pool = None

    asyncio.run(_run())


# =========================================================================
# 4. Tests for Database Operations (Grace Period, Retry Cap, Claim)
# =========================================================================

def test_fetch_retryable_submissions_passes_grace_period():
    """fetch_retryable_submissions supplies stuck_processing_min_age_minutes to the query."""
    async def _run():
        from app.database.operations import fetch_retryable_submissions
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        await fetch_retryable_submissions(
            conn=mock_conn,
            tenant_code="mitra",
            limit=50,
            failed_before="2026-11-05T12:00:00Z",
            max_retry_attempts=3,
            stuck_processing_min_age_minutes=15,
        )

        mock_conn.fetch.assert_awaited_once()
        args = mock_conn.fetch.call_args[0]
        # Query parameters: tenant_code ($1), limit ($2), cutoff ($3), cursor_updated_at ($4), cursor_id ($5), max_retry_attempts ($6), stuck_mins ($7)
        assert args[1] == "mitra"
        assert args[2] == 50
        assert args[6] == 3
        assert args[7] == 15

    asyncio.run(_run())


def test_claim_submission_for_retry():
    """claim_submission_for_retry executes atomic update and parses process_status."""
    async def _run():
        from app.database.operations import claim_submission_for_retry
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "retry_attempt": 2,
            "process_status": '{"status": "failed", "retry_attempt": 2}'
        })

        res = await claim_submission_for_retry(mock_conn, "sub-claim", "mitra")
        assert res["retry_attempt"] == 2
        assert res["process_status"]["retry_attempt"] == 2
        mock_conn.fetchrow.assert_awaited_once()

    asyncio.run(_run())


def test_reconcile_completed_submission():
    """reconcile_completed_submission updates status to success."""
    async def _run():
        from app.database.operations import reconcile_completed_submission
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()

        await reconcile_completed_submission(mock_conn, "sub-recon", "mitra")
        mock_conn.execute.assert_awaited_once()
        query = mock_conn.execute.call_args[0][0]
        assert "status = 'success'" in query

    asyncio.run(_run())


def test_trigger_bulk_retry_activity_calls_dispatch():
    """trigger_bulk_retry_activity calls dispatch_all_retry_workflows with activity.heartbeat."""
    async def _run():
        from app.temporal.activities import trigger_bulk_retry_activity
        from temporalio import activity

        fake_result = {"sweep_id": "bulk-retry-sweep-all-1234", "started": 1}
        with patch(
            "app.temporal.activities.dispatch_all_retry_workflows",
            new_callable=AsyncMock,
            return_value=fake_result,
        ) as mock_dispatch:
            result = await trigger_bulk_retry_activity({"tenant_code": None})

            mock_dispatch.assert_awaited_once_with(None, on_page=activity.heartbeat)
            assert result["sweep_id"] == "bulk-retry-sweep-all-1234"

    asyncio.run(_run())


def test_trigger_bulk_retry_workflow_configuration():
    """TriggerBulkRetryWorkflow must use 2h start_to_close, 5m heartbeat, and maximum_attempts=1."""
    async def _run():
        from app.temporal.workflows import TriggerBulkRetryWorkflow
        from datetime import timedelta
        from temporalio.common import RetryPolicy

        wf = TriggerBulkRetryWorkflow()
        with patch("temporalio.workflow.execute_activity", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = {"sweep_id": "bulk-123"}
            await wf.run({"tenant_code": "mitra"})

            mock_exec.assert_awaited_once()
            _, kwargs = mock_exec.call_args
            assert kwargs["start_to_close_timeout"] == timedelta(hours=2)
            assert kwargs["heartbeat_timeout"] == timedelta(minutes=5)
            assert isinstance(kwargs["retry_policy"], RetryPolicy)
            assert kwargs["retry_policy"].maximum_attempts == 1

    asyncio.run(_run())


def test_dispatch_all_retry_workflows_invokes_on_page_heartbeat():
    """dispatch_all_retry_workflows invokes on_page callback after each page."""
    async def _run():
        from app.services.retry import dispatch_all_retry_workflows

        rows_page_1 = [
            {
                "submission_id": "1",
                "session_id": "s1",
                "tenant_code": "t1",
                "submission_type": "story",
                "status": "failed",
                "process_status": {},
                "updated_at": datetime.now(timezone.utc),
            }
        ]
        mock_client = MagicMock()
        mock_client.start_workflow = AsyncMock()
        mock_pool, _ = _mock_db_pool()
        db.pool = mock_pool
        on_page_mock = MagicMock()

        try:
            with patch("app.services.retry.get_temporal_client", new=AsyncMock(return_value=mock_client)), \
                 patch("app.services.retry.fetch_retryable_submissions", new=AsyncMock(side_effect=[rows_page_1, []])), \
                 patch("app.services.retry.claim_submission_for_retry", new=AsyncMock(return_value={"retry_attempt": 1})), \
                 patch("app.services.retry.settings") as mock_settings:
                mock_settings.BATCH_SIZE = 1  # forces loop to fetch next page and trigger break
                mock_settings.TEMPORAL_QUEUE = "queue"
                mock_settings.STUCK_PROCESSING_MIN_AGE_MINUTES = 15
                mock_settings.get_process_config.return_value = ["step1"]

                res = await dispatch_all_retry_workflows(
                    tenant_code="t1",
                    on_page=on_page_mock,
                )
                assert res["started"] == 1
                assert on_page_mock.call_count == 1
                cb_arg = on_page_mock.call_args[0][0]
                assert cb_arg["page"] == 1
                assert cb_arg["started"] == 1
        finally:
            db.pool = None

    asyncio.run(_run())




