"""
Wiring tests of the feedback_metadata_extraction step with fakes (no Temporal, no database, no network): the
workflow branch, the activity's parameter handling, the LLM settings precedence, the worker registration.
"""
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import install_fake_workflow_context

import app.database.feedback_extraction_operations as ops
import app.temporal.feedback_extraction_activity as activity_module
import app.temporal.workflows as workflows_module
from app.services import feedback_extraction as fx
from app.services.feedback_extraction_constants import STEP_NAME
from seed_support import extraction_config_v1

ROOT = Path(__file__).resolve().parent.parent


def _payload(steps):
    return {"submission_id": "s1", "tenant_code": "mitra", "process_steps": steps}


def test_workflow_dispatches_the_step_to_the_extraction_activity_with_overrides(monkeypatch):
    async def run():
        exec_mock, _, _ = install_fake_workflow_context(
            monkeypatch,
            activity_results={workflows_module.feedback_metadata_extraction_activity: {"status": "success"}})
        step = {"name": STEP_NAME, "llm_model": "some/model", "max_tokens": 700, "llm_timeout_seconds": 30}
        result = await workflows_module.ConfigDrivenProcessingWorkflow().run(_payload([step]))
        calls = [c for c in exec_mock.call_args_list if c.args[0] is workflows_module.feedback_metadata_extraction_activity]
        assert len(calls) == 1
        assert calls[0].args[1] == {
            "submission_id": "s1", "tenant_code": "mitra", "analysis_type": STEP_NAME,
            "llm_model": "some/model", "max_tokens": 700, "llm_timeout_seconds": 30,
        }
        assert result["status"] == "success"
        assert result["steps"][0]["name"] == STEP_NAME and result["steps"][0]["status"] == "success"
        # the final update_status_activity marks the submission success only after the step
        last = exec_mock.call_args_list[-1].args[1]
        assert last["status"] == "success" and last["process_status"]["steps"][0]["status"] == "success"
    asyncio.run(run())


def test_workflow_step_without_overrides_passes_none_so_the_activity_falls_back(monkeypatch):
    async def run():
        exec_mock, _, _ = install_fake_workflow_context(
            monkeypatch,
            activity_results={workflows_module.feedback_metadata_extraction_activity: {"status": "success"}})
        await workflows_module.ConfigDrivenProcessingWorkflow().run(_payload([{"name": STEP_NAME}]))
        call = next(c for c in exec_mock.call_args_list if c.args[0] is workflows_module.feedback_metadata_extraction_activity)
        assert call.args[1]["llm_model"] is None and call.args[1]["max_tokens"] is None
    asyncio.run(run())


def test_a_failing_extraction_marks_the_step_and_the_submission_failed_and_reraises(monkeypatch):
    async def run():
        exec_mock, _, _ = install_fake_workflow_context(
            monkeypatch,
            activity_results={workflows_module.feedback_metadata_extraction_activity: RuntimeError("boom")})
        with pytest.raises(RuntimeError):
            await workflows_module.ConfigDrivenProcessingWorkflow().run(_payload([{"name": STEP_NAME}]))
        last = exec_mock.call_args_list[-1].args[1]
        assert last["status"] == "failed"
        assert last["process_status"]["failed_at_step"] == STEP_NAME
    asyncio.run(run())


def test_start_worker_registers_the_extraction_activity_and_the_workflow(monkeypatch):
    """start_worker() with a fake Temporal client and a fake Worker: what it hands to Worker(...) is what runs."""
    import app.temporal.worker as worker_module

    captured = {}

    class FakeWorker:
        def __init__(self, client, **kwargs):
            captured["client"] = client
            captured.update(kwargs)

        async def run(self):
            captured["ran"] = True

    client = MagicMock()
    client.get_schedule_handle.return_value.delete = AsyncMock()
    client.create_schedule = AsyncMock()
    monkeypatch.setattr(worker_module.Client, "connect", AsyncMock(return_value=client))
    monkeypatch.setattr(worker_module, "Worker", FakeWorker)
    monkeypatch.setattr(worker_module.db, "connect", AsyncMock())
    monkeypatch.setattr(worker_module.db, "disconnect", AsyncMock())
    monkeypatch.setattr(worker_module.torch, "set_num_threads", MagicMock())

    asyncio.run(worker_module.start_worker())

    assert captured["ran"] is True and captured["client"] is client
    assert activity_module.feedback_metadata_extraction_activity in captured["activities"]
    assert len(captured["activities"]) == len(set(captured["activities"]))          # nothing registered twice
    assert workflows_module.ConfigDrivenProcessingWorkflow in captured["workflows"]


def test_the_activity_runs_inside_temporals_activity_environment_and_deterministic_failures_are_not_retried(monkeypatch):
    """
    temporalio.testing.ActivityEnvironment runs the REAL activity definition (decorator, activity.info(), the
    ApplicationError it raises) in-process: no server, no download.
    """
    import dataclasses
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment

    monkeypatch.setattr(activity_module, "db", SimpleNamespace(pool="POOL"))
    params = {"submission_id": "s1", "tenant_code": "mitra", "analysis_type": STEP_NAME,
              "llm_model": None, "max_tokens": None, "llm_timeout_seconds": None}

    async def ok(pool, submission_id, tenant_code, **kwargs):
        assert kwargs["activity_budget_seconds"] is None or kwargs["activity_budget_seconds"] > 0
        result = fx.empty_result(fx.parse_extraction_config(extraction_config_v1("Youth")))
        return SimpleNamespace(run=SimpleNamespace(result=result, llm_called=False), stages=[])

    monkeypatch.setattr(activity_module, "extract_submission", ok)
    out = asyncio.run(ActivityEnvironment().run(activity_module.feedback_metadata_extraction_activity, params))
    assert out["status"] == "success" and out["llm_called"] is False

    async def no_config(*args, **kwargs):
        raise ops.ExtractionSetupError("No extraction config for session_type 'Youth_Voice'")

    monkeypatch.setattr(activity_module, "extract_submission", no_config)
    with pytest.raises(ApplicationError) as caught:
        asyncio.run(ActivityEnvironment().run(activity_module.feedback_metadata_extraction_activity, params))
    assert caught.value.non_retryable is True and caught.value.type == "ExtractionSetupError"

    async def bad_answer(*args, **kwargs):
        raise fx.ExtractionOutputError("model answer is not valid JSON")

    monkeypatch.setattr(activity_module, "extract_submission", bad_answer)
    env = ActivityEnvironment()
    with pytest.raises(fx.ExtractionOutputError):                                   # first attempt: the policy retries
        asyncio.run(env.run(activity_module.feedback_metadata_extraction_activity, params))
    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, attempt=2)
    with pytest.raises(ApplicationError) as second:                                 # second attempt: give up
        asyncio.run(env.run(activity_module.feedback_metadata_extraction_activity, params))
    assert second.value.non_retryable is True


def test_activity_passes_ids_and_overrides_and_marks_processing(monkeypatch):
    seen = {}

    async def fake_extract_submission(pool, submission_id, tenant_code, **kwargs):
        seen.update(pool=pool, submission_id=submission_id, tenant_code=tenant_code, **kwargs)
        result = fx.empty_result(fx.parse_extraction_config(extraction_config_v1("Youth")))
        return SimpleNamespace(run=SimpleNamespace(result=result, llm_called=False), stages=[])

    monkeypatch.setattr(activity_module, "extract_submission", fake_extract_submission)
    monkeypatch.setattr(activity_module, "db", SimpleNamespace(pool="POOL"))

    async def run():
        return await activity_module.feedback_metadata_extraction_activity({
            "submission_id": "s1", "tenant_code": "mitra", "analysis_type": STEP_NAME,
            "llm_model": "m", "max_tokens": None, "llm_timeout_seconds": 20})
    out = asyncio.run(run())
    assert seen["pool"] == "POOL" and seen["submission_id"] == "s1" and seen["tenant_code"] == "mitra"
    assert seen["write"] is True and seen["mark_processing"] is True
    assert seen["llm_overrides"] == {"llm_model": "m", "max_tokens": None, "llm_timeout_seconds": 20}
    assert out == {"status": "success", "llm_called": False, "pii_detected": False,
                   "fields": dict.fromkeys(fx.parse_extraction_config(extraction_config_v1("Youth")).field_names(),
                                           "not_provided"),
                   "stages": []}


def test_activity_rejects_a_foreign_analysis_type_and_reraises_core_errors(monkeypatch):
    async def failing(*args, **kwargs):
        raise fx.ExtractionOutputError("bad")
    monkeypatch.setattr(activity_module, "extract_submission", failing)
    monkeypatch.setattr(activity_module, "db", SimpleNamespace(pool="POOL"))
    with pytest.raises(ValueError, match="cannot run analysis_type"):
        asyncio.run(activity_module.feedback_metadata_extraction_activity(
            {"submission_id": "s", "tenant_code": "mitra", "analysis_type": "thematic_classification"}))
    with pytest.raises(fx.ExtractionOutputError):
        asyncio.run(activity_module.feedback_metadata_extraction_activity({"submission_id": "s", "tenant_code": "mitra"}))


def test_llm_settings_precedence_is_step_override_then_config_then_env(monkeypatch):
    monkeypatch.setattr(ops.settings, "OPENROUTER_MODEL", "env/model")
    monkeypatch.setattr(ops.settings, "LLM_MAX_TOKENS", 2048)
    monkeypatch.setattr(ops.settings, "LLM_TIMEOUT_SECONDS", 60)

    def config(**llm):
        raw = extraction_config_v1("Youth")
        raw["llm"] = llm
        return fx.parse_extraction_config(raw)

    assert ops.resolve_llm_settings(config(), {}) == {"model": "env/model", "max_tokens": 2048, "timeout": 60}
    assert ops.resolve_llm_settings(config(model="cfg/model", max_tokens=900), {}) == {
        "model": "cfg/model", "max_tokens": 900, "timeout": 60}
    overrides = {"llm_model": "step/model", "max_tokens": None, "llm_timeout_seconds": 15}
    assert ops.resolve_llm_settings(config(model="cfg/model", max_tokens=900), overrides) == {
        "model": "step/model", "max_tokens": 900, "timeout": 15}


@pytest.mark.parametrize("overrides", [{"max_tokens": 0}, {"llm_timeout_seconds": -1}, {"llm_model": ""},
                                       {"max_tokens": True}])
def test_invalid_llm_overrides_are_rejected_not_defaulted(overrides):
    config = fx.parse_extraction_config(extraction_config_v1("Youth"))
    with pytest.raises(ValueError):
        ops.resolve_llm_settings(config, overrides)
