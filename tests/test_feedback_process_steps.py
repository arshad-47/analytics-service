"""
Unit tests (in-memory fakes) of where the feedback process steps come from: the persona's active upload config
(app/database/feedback_operations.get_feedback_process_steps), the story/discussion dispatch (get_process_steps),
and the call sites that used settings.get_process_config for feedback (the Temporal activities). The real-Postgres
lookup is in tests/test_feedback_config_pg.py.
"""
import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

import app.api.routes.uploads as uploads_routes
import app.temporal.activities as activities
from app.api.models.submissions import StepRetryRequest
from app.config import settings
from app.database import feedback_operations as ops
from seed_support import mitra_config_v1
from conftest import FakeConn, FakePool, install_fake_db

PROGRAM = "dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a"
EXTRACTION = [{"name": "feedback_metadata_extraction"}]
SUB_ROW = {"session_type": "Youth_Voice", "source": "mitra", "program_id": PROGRAM}


def _conn(sub=SUB_ROW, config_row="default"):
    conn = FakeConn()
    if config_row == "default":
        # the seed default is no steps (extraction is opt-in); these tests need a config that has one
        config_row = {"version": 3, "config": {**mitra_config_v1("Youth"), "process_steps": EXTRACTION}}
    conn.fetchrow = AsyncMock(side_effect=[sub, config_row])
    return conn


def _steps(conn):
    return asyncio.run(ops.get_feedback_process_steps(conn, "184884", "mitra"))


def test_steps_are_the_active_config_versions_process_steps():
    conn = _conn()
    assert _steps(conn) == [{"name": "feedback_metadata_extraction"}]
    first, second = conn.fetchrow.await_args_list
    # the submission is looked up by (submission_id, tenant) ...
    assert first.args[1:] == ("184884", "mitra")
    # ... and its persona (session_type + source, same tenant) picks the config; only the ACTIVE version is read
    assert "v.is_active" in second.args[0] and "c.config_type" in second.args[0]
    assert second.args[1:5] == ("feedback_upload", "mitra", "Youth_Voice", "mitra")
    assert second.args[5] == PROGRAM


def test_the_source_of_the_submission_selects_the_config_source():
    conn = _conn(sub={**SUB_ROW, "source": "google_forms"})
    _steps(conn)
    assert conn.fetchrow.await_args_list[1].args[2] == "google_forms"


def test_config_returned_as_json_text_is_accepted():
    conn = _conn(config_row={"version": 1, "config": json.dumps({**mitra_config_v1("Youth"), "process_steps": EXTRACTION})})
    assert _steps(conn) == [{"name": "feedback_metadata_extraction"}]


def test_empty_steps_list_is_valid():
    config = {**mitra_config_v1("Youth"), "process_steps": []}
    assert _steps(_conn(config_row={"version": 1, "config": config})) == []


def test_story_style_steps_are_returned_as_configured():
    steps = [{"name": "feedback_metadata_extraction", "llm_model": "m", "max_tokens": 100, "llm_timeout_seconds": 30}]
    config = {**mitra_config_v1("Youth"), "process_steps": steps}
    assert _steps(_conn(config_row={"version": 1, "config": config})) == steps


def test_unknown_submission_is_an_error_not_an_empty_list():
    with pytest.raises(ValueError, match="No stored feedback submission '184884'"):
        _steps(_conn(sub=None))


def test_persona_without_an_active_config_is_an_error():
    with pytest.raises(ValueError, match="No active mitra feedback_upload config for session_type 'Youth_Voice'"):
        _steps(_conn(config_row=None))


def test_config_without_process_steps_is_an_error_with_the_migrate_hint():
    config = mitra_config_v1("Youth")
    del config["process_steps"]
    with pytest.raises(ValueError, match="missing 'process_steps'.*tools/feedback_config.py migrate"):
        _steps(_conn(config_row={"version": 1, "config": config}))


@pytest.mark.parametrize("bad, needle", [
    (None, "must be a list"),
    ("feedback_metadata_extraction", "must be a list"),
    ([{"columns": ["x"]}], "non-empty string 'name'"),
    ([{"name": ""}], "non-empty string 'name'"),
    (["feedback_metadata_extraction"], "must be an object"),
])
def test_invalid_process_steps_is_an_error(bad, needle):
    config = {**mitra_config_v1("Youth"), "process_steps": bad}
    with pytest.raises(ValueError, match=needle):
        _steps(_conn(config_row={"version": 1, "config": config}))


# dispatcher -----------------------------------------------------------------------------------

def test_get_process_steps_dispatches_by_type(monkeypatch):
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(settings, "PROCESS_CONFIG_DISCUSSION", '[{"name": "discussion_step"}]')
    conn = _conn()

    async def run():
        assert await ops.get_process_steps(conn, "story", "1", "mitra") == [{"name": "story_step"}]
        assert await ops.get_process_steps(conn, "discussion", "1", "mitra") == [{"name": "discussion_step"}]
        assert await ops.get_process_steps(conn, "survey", "1", "mitra") == []
        conn.fetchrow.assert_not_awaited()  # story / discussion / unknown never touch the config tables
        assert await ops.get_process_steps(conn, " Feedback ", "1", "mitra") == [{"name": "feedback_metadata_extraction"}]
    asyncio.run(run())


# call sites -----------------------------------------------------------------------------------

# db.pool is per event loop: the fake pool is installed inside the running loop.

def test_pending_batch_activity_uses_the_persona_steps_for_feedback_and_settings_for_story(monkeypatch):
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    steps = AsyncMock(return_value=[{"name": "from_config"}])
    monkeypatch.setattr(ops, "get_feedback_process_steps", steps)

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=[
            {"submission_id": "f1", "tenant_code": "mitra", "submission_type": "feedback"},
            {"submission_id": "s1", "tenant_code": "mitra", "submission_type": "story"},
        ])
        rows = await activities.fetch_pending_submissions_activity({"limit": 10})
        assert [(r["submission_id"], r["process_steps"]) for r in rows] == [
            ("f1", [{"name": "from_config"}]), ("s1", [{"name": "story_step"}])]
        steps.assert_awaited_once_with(conn, "f1", "mitra", {})
    asyncio.run(run())


def test_failed_retry_activity_skips_a_feedback_submission_without_usable_steps(monkeypatch):
    import app.database.operations as operations
    rows = [
        {"submission_id": "f1", "tenant_code": "mitra", "submission_type": "feedback", "process_status": None,
         "session_id": "x", "updated_at": None},
        {"submission_id": "f2", "tenant_code": "mitra", "submission_type": "feedback", "process_status": None,
         "session_id": "y", "updated_at": None},
    ]
    monkeypatch.setattr(operations, "fetch_retryable_failed_submissions", AsyncMock(return_value=rows))
    monkeypatch.setattr(ops, "get_feedback_process_steps",
                        AsyncMock(side_effect=[ValueError("no config"), [{"name": "feedback_metadata_extraction"}]]))

    async def run():
        install_fake_db(monkeypatch, activities)
        out = await activities.fetch_failed_submissions_activity({})
        assert [r["submission_id"] for r in out] == ["f2"]
        assert out[0]["process_steps"] == [{"name": "feedback_metadata_extraction"}]
    asyncio.run(run())


def test_pending_sweep_limit_zero_selects_nothing_and_a_missing_limit_uses_the_batch_size(monkeypatch):
    """`limit` 0 is a value ("fetch none", the PR changed `or` into an explicit None check on purpose), not 'not given'."""
    monkeypatch.setattr(settings, "BATCH_SIZE", 7)

    async def run(params):
        conn = install_fake_db(monkeypatch, activities)
        rows = await activities.fetch_pending_submissions_activity(params)
        return rows, conn.fetch.await_args.args[1]

    assert asyncio.run(run({"limit": 0})) == ([], 0)
    assert asyncio.run(run({"limit": 3}))[1] == 3
    assert asyncio.run(run({}))[1] == 7
    assert asyncio.run(run(None))[1] == 7


# sweeps and retries: one bad persona config does not starve the others -------------------------------

def _pending_rows():
    return [
        {"submission_id": "story-old", "tenant_code": "mitra", "submission_type": "story"},
        {"submission_id": "fb-bad", "tenant_code": "mitra", "submission_type": "feedback"},
        {"submission_id": "fb-good", "tenant_code": "mitra", "submission_type": "feedback"},
    ]


def test_pending_sweep_skips_marks_failed_and_logs_a_feedback_row_whose_config_is_bad(monkeypatch, caplog):
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(ops, "get_feedback_process_steps", AsyncMock(
        side_effect=[ValueError("Feedback config of session_type 'Youth_Voice' (mitra) is missing 'process_steps'"),
                     [{"name": "from_config"}]]))
    status = AsyncMock()
    monkeypatch.setattr(ops, "update_submission_status", status)

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=_pending_rows())
        with caplog.at_level(logging.ERROR):
            rows = await activities.fetch_pending_submissions_activity({"limit": 10})
        # the older story row and the healthy feedback row are returned; the bad one is left out
        assert [r["submission_id"] for r in rows] == ["story-old", "fb-good"]
        # ... and it is marked failed with the reason (identifiers + config problem, no content)
        status.assert_awaited_once()
        args = status.await_args.args
        assert args[1:4] == ("fb-bad", "mitra", "failed")
        assert args[4]["status"] == "failed" and "missing 'process_steps'" in args[4]["error_message"]
        assert "retry_attempt" not in args[4]  # the bulk retry picks it up once the config is fixed
        assert "fb-bad" in caplog.text and "mitra" in caplog.text
    asyncio.run(run())


def test_pending_sweep_does_not_swallow_a_database_error(monkeypatch):
    monkeypatch.setattr(ops, "get_feedback_process_steps", AsyncMock(side_effect=RuntimeError("connection lost")))

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=_pending_rows())
        with pytest.raises(RuntimeError, match="connection lost"):
            await activities.fetch_pending_submissions_activity({"limit": 10})
    asyncio.run(run())


def test_pending_sweep_without_limit_uses_the_batch_size_and_a_story_value_error_still_raises(monkeypatch):
    monkeypatch.setattr(settings, "BATCH_SIZE", 7)
    monkeypatch.setattr(ops, "get_process_steps", AsyncMock(side_effect=ValueError("story problem")))
    monkeypatch.setattr(activities, "get_process_steps", ops.get_process_steps)

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(return_value=[{"submission_id": "s", "tenant_code": "mitra", "submission_type": "story"}])
        with pytest.raises(ValueError, match="story problem"):  # story behaviour unchanged
            await activities.fetch_pending_submissions_activity(None)
        assert conn.fetch.await_args.args[1] == 7
    asyncio.run(run())


# the persona lookup is memoized per call --------------------------------------------------------

def _persona_conn(config):
    """fetchrow answers the submission lookup, then (at most once per persona) the config lookup."""
    conn = FakeConn()
    calls = {"submission": 0, "config": 0}

    async def fetchrow(sql, *args):
        if "FROM feedback_submissions" in sql:
            calls["submission"] += 1
            return {"session_type": "Youth_Voice", "source": "mitra", "program_id": PROGRAM}
        calls["config"] += 1
        return None if config is None else {"version": 1, "config": config}

    conn.fetchrow = fetchrow
    conn.calls = calls
    return conn


def test_the_persona_config_is_read_once_per_sweep_and_not_shared_between_calls():
    conn = _persona_conn({**mitra_config_v1("Youth"), "process_steps": [{"name": "feedback_metadata_extraction"}]})

    async def run():
        memo = {}
        for sid in ("1", "2", "3"):
            assert await ops.get_feedback_process_steps(conn, sid, "mitra", memo) == [{"name": "feedback_metadata_extraction"}]
        assert conn.calls == {"submission": 3, "config": 1}
        # a result is a copy: changing it does not change what the next row gets
        first = await ops.get_feedback_process_steps(conn, "4", "mitra", memo)
        first.append({"name": "x"})
        assert await ops.get_feedback_process_steps(conn, "5", "mitra", memo) == [{"name": "feedback_metadata_extraction"}]
        # no memo: nothing is remembered
        await ops.get_feedback_process_steps(conn, "6", "mitra")
        await ops.get_feedback_process_steps(conn, "7", "mitra")
        assert conn.calls["config"] == 3
    asyncio.run(run())


def test_a_persona_config_error_is_memoized_too_and_names_each_submission():
    conn = _persona_conn(None)

    async def run():
        memo = {}
        for sid in ("1", "2"):
            with pytest.raises(ValueError, match=f"No active mitra feedback_upload config.*submission '{sid}'"):
                await ops.get_feedback_process_steps(conn, sid, "mitra", memo)
        assert conn.calls["config"] == 1
    asyncio.run(run())


class _CountingPool(FakePool):
    def __init__(self, conn):
        super().__init__(conn)
        self.acquired = 0

    def acquire(self):
        self.acquired += 1
        return self.conn


def test_bulk_retry_sweep_only_takes_a_connection_for_feedback_rows_and_reads_each_persona_once(monkeypatch):
    rows = [
        {"submission_id": "s1", "session_id": "x", "tenant_code": "mitra", "submission_type": "story",
         "process_status": None, "updated_at": 1},
        {"submission_id": "f1", "session_id": "x", "tenant_code": "mitra", "submission_type": "feedback",
         "process_status": None, "updated_at": 2},
        {"submission_id": "f2", "session_id": "y", "tenant_code": "mitra", "submission_type": "feedback",
         "process_status": None, "updated_at": 3},
    ]
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(settings, "BATCH_SIZE", 100)
    monkeypatch.setattr(uploads_routes, "fetch_retryable_failed_submissions", AsyncMock(side_effect=[rows]))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(uploads_routes, "get_temporal_client", AsyncMock(return_value=client))
    conn = _persona_conn({**mitra_config_v1("Youth"), "process_steps": [{"name": "feedback_metadata_extraction"}]})
    pool = _CountingPool(conn)

    async def run():
        monkeypatch.setattr(uploads_routes.db, "pool", pool, raising=False)  # db.pool is per event loop
        await uploads_routes._dispatch_all_retry_workflows("mitra", "2026-01-01T00:00:00+00:00", "sweep")
    asyncio.run(run())

    assert pool.acquired == 2          # one for the page of rows, one for the config lookups of the feedback rows
    assert conn.calls["config"] == 1   # f1 and f2 share a persona
    assert client.start_workflow.await_count == 3


def test_bulk_retry_sweep_with_only_story_rows_takes_one_connection(monkeypatch):
    rows = [{"submission_id": "s1", "session_id": "x", "tenant_code": "mitra", "submission_type": "story",
             "process_status": None, "updated_at": 1}]
    monkeypatch.setattr(settings, "PROCESS_CONFIG_STORY", '[{"name": "story_step"}]')
    monkeypatch.setattr(settings, "BATCH_SIZE", 100)
    monkeypatch.setattr(uploads_routes, "fetch_retryable_failed_submissions", AsyncMock(side_effect=[rows]))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(uploads_routes, "get_temporal_client", AsyncMock(return_value=client))
    pool = _CountingPool(FakeConn())

    async def run():
        monkeypatch.setattr(uploads_routes.db, "pool", pool, raising=False)
        await uploads_routes._dispatch_all_retry_workflows("mitra", "2026-01-01T00:00:00+00:00", "sweep")
    asyncio.run(run())
    assert pool.acquired == 1 and client.start_workflow.await_count == 1


# retry-step: a server-side config problem is a 409 -------------------------------------------------

def test_retry_step_returns_409_when_the_persona_config_cannot_give_steps(monkeypatch):
    row = {"submission_id": "f1", "session_id": "x", "status": "failed", "submission_type": "feedback",
           "process_status": None}
    monkeypatch.setattr(uploads_routes, "fetch_failed_submission_for_retry", AsyncMock(return_value=row))
    monkeypatch.setattr(uploads_routes, "get_process_steps", AsyncMock(side_effect=ValueError("no config")))
    body = StepRetryRequest(submission_id="f1", session_id="x", tenant_code="mitra")

    async def run():
        install_fake_db(monkeypatch, uploads_routes)
        await uploads_routes.retry_step(body, None)
    with pytest.raises(HTTPException) as info:
        asyncio.run(run())
    assert info.value.status_code == 409
    assert "Cannot resolve the process steps of submission 'f1': no config" in info.value.detail
