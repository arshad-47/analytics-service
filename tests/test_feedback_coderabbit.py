"""
Tests of the CodeRabbit follow-up on PR #36 (pure, in-memory fakes; the Postgres ones are in
tests/test_feedback_coderabbit_pg.py). Each section names the comment (CRn) it proves.
"""
import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

import app.api.routes.uploads as uploads_routes
import app.services.retry as retry_service
import app.temporal.activities as activities
from app.api.models.submissions import StepRetryRequest
from app.config import settings
from conftest import FakeConn, FakePool, install_fake_db


# --------------------------------------------------------------------------------------------------------------------
# CR1: a feedback submission whose process_steps is the valid empty list is retried (the workflow marks it success);
# an unsupported type with no steps keeps the rejection.
# --------------------------------------------------------------------------------------------------------------------

def _failed_row(sub_type):
    return {"submission_id": "f1", "session_id": "x", "tenant_code": "mitra", "status": "failed",
            "submission_type": sub_type, "process_status": None}


def _retry_one(monkeypatch, sub_type):
    """retry_step through the shared retry service (app/services/retry.py, which the route delegates to)."""
    monkeypatch.setattr(uploads_routes, "fetch_retryable_submission_for_retry", AsyncMock(return_value=_failed_row(sub_type)))
    monkeypatch.setattr(retry_service, "get_process_steps", AsyncMock(return_value=[]))
    monkeypatch.setattr(retry_service, "claim_submission_for_retry", AsyncMock(return_value={"retry_attempt": 1}))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(uploads_routes, "get_temporal_client", AsyncMock(return_value=client))
    body = StepRetryRequest(submission_id="f1", session_id="x", tenant_code="mitra")

    async def run():
        install_fake_db(monkeypatch, uploads_routes)
        return await uploads_routes.retry_step(body, None)
    return client, run


def test_cr1_retry_step_dispatches_a_feedback_submission_with_an_empty_step_list(monkeypatch):
    client, run = _retry_one(monkeypatch, "feedback")
    response = asyncio.run(run())
    assert response.submission_id == "f1"
    client.start_workflow.assert_awaited_once()
    assert client.start_workflow.await_args.args[1]["process_steps"] == []


def test_cr1_retry_step_still_rejects_an_unsupported_type_without_steps(monkeypatch):
    client, run = _retry_one(monkeypatch, "unknown_kind")
    with pytest.raises(HTTPException) as info:
        asyncio.run(run())
    assert info.value.status_code == 400
    client.start_workflow.assert_not_awaited()


def test_cr1_a_feedback_retry_reads_the_persona_config_never_settings(monkeypatch):
    """Regression for the upstream merge: the shared retry service called settings.get_process_config for every
    type, which raises for feedback (its steps live in the persona's upload config), so no feedback retry worked."""
    steps = [{"name": "feedback_metadata_extraction"}]
    client, run = _retry_one(monkeypatch, "feedback")
    persona = AsyncMock(return_value=steps)
    monkeypatch.setattr(retry_service, "get_process_steps", persona)
    guard = MagicMock()
    guard.get_process_config.side_effect = AssertionError("settings must not be read for feedback")
    guard.TEMPORAL_QUEUE = "queue"
    monkeypatch.setattr(retry_service, "settings", guard)
    asyncio.run(run())
    persona.assert_awaited_once()
    assert persona.await_args.args[1:4] == ("feedback", "f1", "mitra")
    assert client.start_workflow.await_args.args[1]["process_steps"] == steps


def test_cr1_bulk_retry_sweep_dispatches_feedback_rows_with_an_empty_step_list_and_skips_other_empty_types(monkeypatch):
    rows = [
        {"submission_id": "f1", "session_id": "x", "tenant_code": "mitra", "submission_type": "feedback",
         "status": "failed", "process_status": None, "updated_at": 1},
        {"submission_id": "u1", "session_id": "y", "tenant_code": "mitra", "submission_type": "unknown_kind",
         "status": "failed", "process_status": None, "updated_at": 2},
    ]
    monkeypatch.setattr(settings, "BATCH_SIZE", 100)
    monkeypatch.setattr(retry_service, "fetch_retryable_submissions", AsyncMock(side_effect=[rows]))
    monkeypatch.setattr(retry_service, "get_process_steps", AsyncMock(return_value=[]))
    monkeypatch.setattr(retry_service, "claim_submission_for_retry", AsyncMock(return_value={"retry_attempt": 1}))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(retry_service, "get_temporal_client", AsyncMock(return_value=client))

    async def run():
        install_fake_db(monkeypatch, retry_service)
        await retry_service.dispatch_all_retry_workflows("mitra", "2026-01-01T00:00:00+00:00", "sweep")
    asyncio.run(run())
    started = [call.args[1]["submission_id"] for call in client.start_workflow.await_args_list]
    assert started == ["f1"]


# --------------------------------------------------------------------------------------------------------------------
# CR8: filtering a feedback row whose config is invalid must not make a full page look short (the workflows stop at
# the first short page) and an all-invalid page must not end the sweep while later rows remain.
# --------------------------------------------------------------------------------------------------------------------

def _failed_db_row(n, sub_type="feedback"):
    return {"submission_id": f"s{n:04d}", "session_id": f"x{n}", "tenant_code": "mitra", "submission_type": sub_type,
            "process_status": None, "updated_at": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=n)}


class _Source:
    """A keyset-paginated source of failed rows: honours `limit` and the cursor like the SQL does."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def __call__(self, conn, tenant_code, limit, failed_before=None, cursor_updated_at=None,
                       cursor_submission_id=None, max_retry_attempts=5, stuck_processing_min_age_minutes=None):
        self.calls.append((limit, cursor_submission_id))
        assert cursor_updated_at is None or isinstance(cursor_updated_at, dt.datetime)  # what asyncpg accepts
        rest = [r for r in self.rows
                if cursor_submission_id is None or (r["updated_at"], r["submission_id"]) > (cursor_updated_at, cursor_submission_id)]
        return rest[:limit]


def _sweep(monkeypatch, source, bad_ids, limit):
    """Runs the bulk retry sweep (dispatch_all_retry_workflows) against `source`; a row in `bad_ids` has a persona
    config that cannot give steps. Returns the ids of the dispatched workflows."""
    monkeypatch.setattr(settings, "BATCH_SIZE", limit)
    monkeypatch.setattr(retry_service, "fetch_retryable_submissions", source)

    async def steps(conn, sub_type, submission_id, tenant, memo=None):
        if submission_id in bad_ids:
            raise ValueError("config invalid")
        return [{"name": "feedback_metadata_extraction"}]
    monkeypatch.setattr(retry_service, "get_process_steps", steps)
    monkeypatch.setattr(retry_service, "claim_submission_for_retry", AsyncMock(return_value={"retry_attempt": 1}))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(retry_service, "get_temporal_client", AsyncMock(return_value=client))

    async def run():
        install_fake_db(monkeypatch, retry_service)
        await retry_service.dispatch_all_retry_workflows("mitra", "2026-01-01T00:00:00+00:00", "sweep")
    asyncio.run(run())
    return [call.args[1]["submission_id"] for call in client.start_workflow.await_args_list]


def _failed_rows(n, sub_type="feedback"):
    return [{**_failed_db_row(i, sub_type), "status": "failed"} for i in range(n)]


def test_cr8_a_page_with_one_invalid_config_does_not_stop_the_sweep_early(monkeypatch):
    rows = _failed_rows(25)
    started = _sweep(monkeypatch, _Source(rows), {"s0003"}, limit=10)
    assert sorted(started) == sorted(r["submission_id"] for r in rows if r["submission_id"] != "s0003")


def test_cr8_an_entirely_invalid_page_does_not_hide_the_later_rows(monkeypatch):
    rows = _failed_rows(25)
    bad = {f"s{n:04d}" for n in range(10)}
    started = _sweep(monkeypatch, _Source(rows), bad, limit=10)
    assert sorted(started) == sorted(r["submission_id"] for r in rows if r["submission_id"] not in bad)


def test_cr8_a_page_is_never_longer_than_the_limit_and_the_cursor_follows_the_scanned_rows(monkeypatch):
    rows = _failed_rows(40)
    source = _Source(rows)
    _sweep(monkeypatch, source, {"s0003"}, limit=10)
    assert [limit for limit, _ in source.calls] == [10, 10, 10, 10, 10]   # four full pages, then the empty one
    assert [cursor for _, cursor in source.calls] == [None, "s0009", "s0019", "s0029", "s0039"]


def test_cr8_the_pending_sweep_is_refilled_after_invalid_rows_are_marked_failed(monkeypatch):
    """Invalid feedback rows are marked failed (they leave the pending queue), the page is filled from the next ones."""
    import app.database.feedback_operations as ops
    queue = [{"submission_id": f"p{n:03d}", "tenant_code": "mitra", "submission_type": "feedback"} for n in range(12)]
    bad = {"p001", "p002", "p003"}
    marked = []

    async def mark(conn, sub_id, tenant, reason):
        marked.append(sub_id)
        queue[:] = [r for r in queue if r["submission_id"] != sub_id]

    async def steps(conn, sub_type, sub_id, tenant, memo=None):
        if sub_id in bad:
            raise ValueError("no config")
        return [{"name": "feedback_metadata_extraction"}]

    monkeypatch.setattr(activities, "get_process_steps", steps)
    monkeypatch.setattr(activities, "mark_submission_config_failed", mark)

    async def fetch(sql, limit):
        return list(queue[:limit])

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(side_effect=fetch)
        return await activities.fetch_pending_submissions_activity({"limit": 5})
    out = asyncio.run(run())
    assert marked == ["p001", "p002", "p003"]
    assert [r["submission_id"] for r in out] == ["p000", "p004", "p005", "p006", "p007"]


def test_cr8_an_iso_string_cursor_reaches_the_query_as_a_datetime():
    """The keyset cursor may be handed over as an ISO string (the retired BulkStepRetryWorkflow did; any JSON
    hand-off does); asyncpg refuses a str for timestamptz. Kept after the upstream rename to
    fetch_retryable_submissions."""
    import app.database.operations as operations
    conn = FakeConn()
    asyncio.run(operations.fetch_retryable_submissions(
        conn, "mitra", 5, failed_before="2026-01-01T00:00:00+00:00",
        cursor_updated_at="2026-01-01T00:00:05+00:00", cursor_submission_id="s5"))
    args = conn.fetch.await_args.args
    assert isinstance(args[3], dt.datetime) and isinstance(args[4], dt.datetime)


def test_cr8_a_trailing_z_is_read_as_utc_on_python_310():
    """datetime.fromisoformat only accepts 'Z' from Python 3.11; the service image runs Python 3.10."""
    import app.database.operations as operations
    conn = FakeConn()
    asyncio.run(operations.fetch_retryable_submissions(
        conn, "mitra", 5, failed_before="2026-11-05T12:00:00Z", cursor_updated_at="2026-11-05T11:00:00Z",
        cursor_submission_id="s5"))
    args = conn.fetch.await_args.args
    assert args[3] == dt.datetime(2026, 11, 5, 12, tzinfo=dt.timezone.utc)
    assert args[4] == dt.datetime(2026, 11, 5, 11, tzinfo=dt.timezone.utc)


# --------------------------------------------------------------------------------------------------------------------
# CR2: a Google Forms config without session_type or a form's questionnaire_version is refused at upload (ingestion
# would reject every response of the persona); migrate reports them (never invents them).
# --------------------------------------------------------------------------------------------------------------------

def _gforms_df(config):
    import pandas as pd
    form = next(iter(config["forms"].values()))
    row = {config["timestamp_column"]: "01/02/2026 10:00:00"}
    for q in form["questions"]:
        row[q["headers"][0]] = "x"
    return pd.DataFrame([row])


@pytest.mark.parametrize("breakage", ["no_session_type", "blank_session_type", "no_form_version", "blank_form_version",
                                      "non_string_form_version", "form_not_an_object"])
def test_cr2_an_incomplete_google_forms_config_is_refused_before_a_file_is_accepted(breakage):
    import copy
    from app.api.services import feedback_gforms
    from seed_support import gforms_config_v1
    config = copy.deepcopy(gforms_config_v1("Youth"))
    first = next(iter(config["forms"]))
    if breakage == "no_session_type":
        del config["session_type"]
    elif breakage == "blank_session_type":
        config["session_type"] = "  "
    elif breakage == "no_form_version":
        del config["forms"][first]["questionnaire_version"]
    elif breakage == "blank_form_version":
        config["forms"][first]["questionnaire_version"] = ""
    elif breakage == "non_string_form_version":
        config["forms"][first]["questionnaire_version"] = 2
    else:
        config["forms"][first] = "not an object"
    with pytest.raises(ValueError) as info:
        feedback_gforms.check_gforms_file(_gforms_df(gforms_config_v1("Youth")), config)
    text = str(info.value)
    assert ("session_type" in text) or ("questionnaire_version" in text) or ("form" in text)


def test_cr2_the_seeded_google_forms_configs_are_complete():
    from app.api.services import feedback_gforms
    from seed_support import GFORMS_PERSONAS, gforms_config_v1
    for persona in GFORMS_PERSONAS:
        config = gforms_config_v1(persona)
        feedback_gforms._check_config(config)   # does not raise


def test_cr2_the_problem_text_names_the_form_and_never_a_value():
    from app.services.feedback_adapter_config import wide_form_identity_problem
    assert wide_form_identity_problem({"session_type": "Youth_Voice", "forms": {"f1": {"questionnaire_version": "V1"}}}) is None
    assert "session_type" in wide_form_identity_problem({"forms": {"f1": {"questionnaire_version": "V1"}}})
    problem = wide_form_identity_problem({"session_type": "Youth_Voice", "forms": {"f1": {}, "f2": {"questionnaire_version": "V1"}}})
    assert "'f1'" in problem and "questionnaire_version" in problem and "'f2'" not in problem


# --------------------------------------------------------------------------------------------------------------------
# CR4: the failure log after a stale save must not ask the pool for a second connection while the first is held.
# --------------------------------------------------------------------------------------------------------------------

class _OneConnectionPool:
    """A pool of ONE connection: a second acquire() while the first is held can never be served (it times out)."""

    def __init__(self):
        self.conn = FakeConn()
        self._free = None
        self.acquired = 0

    def acquire(self):
        pool = self

        class _Acquire:
            async def __aenter__(self_inner):
                if pool._free is None:
                    pool._free = asyncio.Semaphore(1)
                await asyncio.wait_for(pool._free.acquire(), timeout=0.5)   # TimeoutError == the deadlock
                pool.acquired += 1
                return pool.conn

            async def __aexit__(self_inner, *exc):
                pool._free.release()
                return False
        return _Acquire()


def _stale_extraction_world(monkeypatch, save):
    from types import SimpleNamespace
    import app.database.feedback_extraction_operations as ops
    from app.services.feedback_extraction import ExtractionRun

    config = SimpleNamespace(llm={}, fields=[])
    inputs = {"session_type": "Youth_Voice", "questionnaire_version": "V1", "identity": ("Youth_Voice", "mitra", None),
              "rows": [], "fingerprint": "f", "loaded_at": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
              "config_row": {"id": "cv", "config": config}, "closing_keys": []}
    monkeypatch.setattr(ops, "load_submission_inputs", AsyncMock(return_value=inputs))
    monkeypatch.setattr(ops, "load_prompt_for_config", AsyncMock(return_value={
        "id": "pv", "name": "p", "system_prompt": "s", "user_prompt": "u"}))
    monkeypatch.setattr(ops, "extract", lambda *a, **k: ExtractionRun(
        result=SimpleNamespace(), usage={"prompt_tokens": 3, "completion_tokens": 2}, llm_called=True, conversation_turns=1))
    monkeypatch.setattr(ops, "save_extraction", save)
    logged = AsyncMock()
    monkeypatch.setattr(ops, "insert_llm_log", logged)
    return ops, logged


def test_cr4_a_stale_result_is_logged_without_a_second_connection_from_a_one_connection_pool(monkeypatch):
    import app.database.feedback_extraction_operations as ops_module

    async def stale_save(conn, **kwargs):
        raise ops_module.StaleExtractionError("changed")
    ops, logged = _stale_extraction_world(monkeypatch, stale_save)
    pool = _OneConnectionPool()

    async def run():
        with pytest.raises(ops.StaleExtractionError):
            await ops.extract_submission(pool, "s1", "mitra", llm_function=lambda *a, **k: ("{}", {}))
    asyncio.run(run())
    logged.assert_awaited_once()
    assert logged.await_args.args[8] == "failed"   # the call is logged as failed (it cost tokens)


def test_cr4_the_log_of_a_stale_result_is_still_best_effort(monkeypatch):
    """A failing log write must not hide the stale error."""
    import app.database.feedback_extraction_operations as ops_module

    async def stale_save(conn, **kwargs):
        raise ops_module.StaleExtractionError("changed")
    ops, logged = _stale_extraction_world(monkeypatch, stale_save)
    logged.side_effect = RuntimeError("log table gone")

    async def run():
        with pytest.raises(ops.StaleExtractionError):
            await ops.extract_submission(_OneConnectionPool(), "s1", "mitra", llm_function=lambda *a, **k: ("{}", {}))
    asyncio.run(run())


# --------------------------------------------------------------------------------------------------------------------
# CR3 + CR17 (pure part): the identity is part of what is compared; the lock order is submissions, feedback_submissions.
# --------------------------------------------------------------------------------------------------------------------

def test_cr3_the_identity_of_a_session_is_session_type_source_and_program_as_text():
    import uuid
    from app.database.feedback_extraction_operations import persona_identity
    program = uuid.uuid4()
    assert persona_identity("Youth_Voice", "mitra", program) == ("Youth_Voice", "mitra", str(program))
    assert persona_identity("Youth_Voice", "mitra", None) == ("Youth_Voice", "mitra", None)
    assert persona_identity("Youth_Voice", "mitra", program) != persona_identity("General_Public_Voice", "mitra", program)
    assert persona_identity("Youth_Voice", "mitra", program) != persona_identity("Youth_Voice", "google_forms", program)
    assert persona_identity("Youth_Voice", "mitra", program) != persona_identity("Youth_Voice", "mitra", uuid.uuid4())


def test_cr17_save_extraction_locks_submissions_before_feedback_submissions():
    import app.database.feedback_extraction_operations as ops
    from app.services.feedback_extraction import ExtractionResult

    statements = []

    class Conn(FakeConn):
        def __init__(self):
            super().__init__()
            self.fetchrow = AsyncMock(side_effect=self._fetchrow)
            self.fetch = AsyncMock(return_value=[])
            self.execute = AsyncMock()

        async def _fetchrow(self, sql, *args):
            statements.append(" ".join(sql.split()))
            if "FROM submissions" in sql:
                return {"program_id": None}
            return {"session_type": "Youth_Voice", "source": "mitra"}

    result = ExtractionResult(fields=[], pii_detected=False, pii_types=[])
    asyncio.run(ops.save_extraction(
        Conn(), submission_id="s1", tenant_code="mitra", input_fingerprint=ops.rows_fingerprint([]),
        input_identity=("Youth_Voice", "mitra", None), result=result, config_version_id="c", prompt_version_id="p",
        run_at=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)))
    assert "FROM submissions" in statements[0] and "FOR NO KEY UPDATE" in statements[0]
    assert "FROM feedback_submissions" in statements[1] and statements[1].endswith("FOR UPDATE")


# --------------------------------------------------------------------------------------------------------------------
# CR10 + CR18: tools/run_feedback_extraction.py
# --------------------------------------------------------------------------------------------------------------------

def _load_extraction_cli():
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parent.parent / "tools" / "run_feedback_extraction.py"
    spec = importlib.util.spec_from_file_location("run_feedback_extraction_cr", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _outcome(kinds, values, usage=None, llm_called=True):
    from types import SimpleNamespace
    from app.services.feedback_extraction import ExtractionResult, FieldResult
    fields = tuple(FieldResult(name, value, value, "extracted" if value is not None else "not_provided", None)
                   for name, value in values.items())
    run = SimpleNamespace(llm_called=llm_called, usage=usage or {}, skip_reason=None,
                          result=ExtractionResult(fields=fields, pii_detected=False, pii_types=()))
    return SimpleNamespace(run=run, field_kinds=kinds)


def _aggregate(cli, outcomes, top=5):
    from collections import Counter
    agg = cli._new_aggregates("Youth_Voice", True)
    values = {}
    for outcome in outcomes:
        cli._add_outcome(agg, outcome, top, values)
    return cli._finish(agg, top, values)


def test_cr10_a_field_that_is_a_category_in_one_config_and_free_text_in_another_prints_no_values():
    cli = _load_extraction_cli()
    respondent_words = "Hosahalli village near the old tank"
    for order in ("category_first", "free_text_first"):
        a = _outcome({"district": "category"}, {"district": "Mysuru"})
        b = _outcome({"district": "free_text"}, {"district": respondent_words})
        out = _aggregate(cli, [a, b] if order == "category_first" else [b, a])
        entry = out["fields"]["district"]
        assert "top_values" not in entry, order
        assert entry["distinct_values"] == 2
        assert respondent_words not in str(out), order


def test_cr10_a_field_with_one_category_kind_in_every_config_still_prints_its_top_values():
    cli = _load_extraction_cli()
    out = _aggregate(cli, [_outcome({"g": "category"}, {"g": "Female"}), _outcome({"g": "category"}, {"g": "Female"}),
                           _outcome({"g": "category"}, {"g": "Male"})])
    assert out["fields"]["g"]["top_values"] == {"Female": 2, "Male": 1}


def test_cr10_integer_in_one_config_and_free_text_in_another_prints_no_min_max_or_values():
    cli = _load_extraction_cli()
    out = _aggregate(cli, [_outcome({"age": "integer"}, {"age": "12"}), _outcome({"age": "free_text"}, {"age": "twelve"})])
    entry = out["fields"]["age"]
    assert "min_value" not in entry and "top_values" not in entry and "twelve" not in str(out)


def test_cr10_an_unknown_kind_prints_no_values():
    cli = _load_extraction_cli()
    out = _aggregate(cli, [_outcome({}, {"x": "secret words"})])
    assert "top_values" not in out["fields"]["x"] and "secret words" not in str(out)


def _run_cli(cli, monkeypatch, outcomes_or_errors):
    """Runs cli._run over len(items) sessions, extract_submission answering each item in order."""
    items = list(outcomes_or_errors)

    async def fake_select(conn, *args, **kwargs):
        return [f"s{i}" for i in range(len(items))]

    async def fake_extract(pool, submission_id, tenant_code, **kwargs):
        item = items[int(submission_id[1:])]
        if isinstance(item, BaseException):
            raise item
        return item
    monkeypatch.setattr(cli, "_select_submissions", fake_select)
    monkeypatch.setattr(cli, "extract_submission", fake_extract)
    args = cli.build_parser().parse_args(["run", "--session-type", "Youth_Voice", "--tenant-code", "mitra", "--dry-run",
                                          "--concurrency", "1"])

    async def go():
        install_fake_db(monkeypatch, cli)
        return await cli._run(args)
    return asyncio.run(go())


def test_cr18_a_malformed_answer_still_counts_as_a_call_with_its_tokens(monkeypatch):
    from app.services.feedback_extraction import ExtractionOutputError
    cli = _load_extraction_cli()
    bad = ExtractionOutputError("not json")
    bad.usage = {"prompt_tokens": 50, "completion_tokens": 5}
    good = _outcome({"g": "category"}, {"g": "F"}, usage={"prompt_tokens": 100, "completion_tokens": 20})
    out = _run_cli(cli, monkeypatch, [good, bad])
    assert out["submissions"]["llm_calls"] == 2
    assert out["tokens"] == {"prompt": 150, "completion": 25}
    assert out["submissions"]["failed"] == 1 and out["errors"] == {"ExtractionOutputError": 1}


def test_cr18_a_failed_call_counts_as_a_call_and_a_setup_failure_does_not(monkeypatch):
    from app.database.feedback_extraction_operations import ExtractionSetupError, LlmCallError, StaleExtractionError
    cli = _load_extraction_cli()
    stale = StaleExtractionError("changed")
    stale.usage = {"prompt_tokens": 7, "completion_tokens": 3}
    out = _run_cli(cli, monkeypatch, [LlmCallError("LLM call failed (TimeoutError)"), ExtractionSetupError("no config"),
                                      ValueError("not a stored session"), stale])
    assert out["submissions"]["failed"] == 4
    assert out["submissions"]["llm_calls"] == 2            # the failed call and the stale one; not the setup errors
    assert out["tokens"] == {"prompt": 7, "completion": 3}


def test_cr18_extract_submission_attaches_the_usage_to_a_stale_error(monkeypatch):
    import app.database.feedback_extraction_operations as ops_module

    async def stale_save(conn, **kwargs):
        raise ops_module.StaleExtractionError("changed")
    ops, logged = _stale_extraction_world(monkeypatch, stale_save)

    async def run():
        with pytest.raises(ops.StaleExtractionError) as info:
            await ops.extract_submission(_OneConnectionPool(), "s1", "mitra", llm_function=lambda *a, **k: ("{}", {}))
        return info.value
    assert asyncio.run(run()).usage == {"prompt_tokens": 3, "completion_tokens": 2}


# --------------------------------------------------------------------------------------------------------------------
# CR11 (already fixed by fixer S, B14: kept as a guard), CR12 (same generated name) and CR13 (bounded open files):
# tools/split_mitra_export.py
# --------------------------------------------------------------------------------------------------------------------

def _split_tool():
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    if str(root / "tools") not in sys.path:
        sys.path.insert(0, str(root / "tools"))
    import split_mitra_export
    return split_mitra_export


def _write_export(path, rows):
    import csv
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["session_type", "session", "message"])
        w.writerows(rows)


def test_cr11_a_session_type_that_names_a_path_is_refused_and_nothing_is_written(tmp_path):
    sme = _split_tool()
    for bad in ("../outside", "/abs/path", "a/b", "..\\x"):
        src = tmp_path / "in.csv"
        _write_export(src, [[bad, "s1", "hello"]])
        out = tmp_path / "out"
        with pytest.raises(sme.SplitError):
            sme.split_export(src, out, 10_000, force=True)
        assert not out.exists() or list(out.iterdir()) == []
        assert not (tmp_path / "outside.csv").exists()


def test_cr12_two_outputs_with_the_same_generated_name_are_refused_before_any_file_is_opened(tmp_path):
    sme = _split_tool()
    src = tmp_path / "in.csv"
    # 'Youth_Voice' needs two files (Youth_Voice_part1.csv, _part2.csv); the type 'Youth_Voice_part1' needs one
    rows = [["Youth_Voice", f"a{n}", "x" * 60] for n in range(3)] + [["Youth_Voice_part1", "b0", "y"]]
    _write_export(src, rows)
    out = tmp_path / "out"
    with pytest.raises(sme.SplitError, match="same file name"):
        sme.split_export(src, out, 120)
    assert not out.exists() or list(out.iterdir()) == []      # nothing was created, not even a temporary file


def test_cr12_distinct_names_are_still_written(tmp_path):
    sme = _split_tool()
    src = tmp_path / "in.csv"
    _write_export(src, [["Youth_Voice", f"a{n}", "x" * 60] for n in range(3)] + [["Parent_Voice", "b0", "y"]])
    summary = sme.split_export(src, tmp_path / "out", 120)
    assert sorted(f["name"] for f in summary["files"]) == [
        "Parent_Voice.csv", "Youth_Voice_part1.csv", "Youth_Voice_part2.csv", "Youth_Voice_part3.csv"]


def test_cr13_the_open_output_files_are_bounded_and_the_result_is_the_same_as_unbounded(tmp_path, monkeypatch):
    import os
    sme = _split_tool()
    src = tmp_path / "in.csv"
    # 40 session types with 3 sessions each, rows of the types interleaved so every writer is evicted and reopened
    rows = []
    for round_no in range(3):
        for t in range(40):
            rows.append([f"T{t:02d}", f"s{t:02d}_{round_no}", f"message {t} {round_no}"])
            rows.append([f"T{t:02d}", f"s{t:02d}_{round_no}", f"reply {t} {round_no}"])
    _write_export(src, rows)

    unbounded = tmp_path / "unbounded"
    sme.split_export(src, unbounded, 10**9, max_open_files=1000)

    live = {"now": 0, "peak": 0}
    real_fdopen = os.fdopen

    class Tracked:
        def __init__(self, handle):
            self._handle = handle
            live["now"] += 1
            live["peak"] = max(live["peak"], live["now"])

        def close(self):
            if not self._handle.closed:
                live["now"] -= 1
            self._handle.close()

        def __getattr__(self, name):
            return getattr(self._handle, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.close()

    monkeypatch.setattr(sme.os, "fdopen", lambda *a, **k: Tracked(real_fdopen(*a, **k)))
    bounded = tmp_path / "bounded"
    sme.split_export(src, bounded, 10**9, max_open_files=4)
    assert live["peak"] <= 4 and live["now"] == 0

    names = sorted(p.name for p in unbounded.iterdir() if p.suffix == ".csv")
    assert names == sorted(p.name for p in bounded.iterdir() if p.suffix == ".csv") and len(names) == 40
    for name in names:
        assert (unbounded / name).read_bytes() == (bounded / name).read_bytes()      # same rows, same order
        text = (bounded / name).read_text(encoding="utf-8")
        assert text.count("session_type,session,message") == 1                       # the header is written once
    assert not [p for p in bounded.iterdir() if p.name.endswith(".tmp")]


def test_cr13_a_failure_while_routing_removes_every_temporary_file(tmp_path, monkeypatch):
    sme = _split_tool()
    src = tmp_path / "in.csv"
    _write_export(src, [[f"T{t:02d}", f"s{t}", "m"] for t in range(10)])
    calls = {"n": 0}
    real = sme._WriterCache.writer

    def failing(self, key, path):
        calls["n"] += 1
        if calls["n"] == 5:
            raise OSError("disk full")
        return real(self, key, path)
    monkeypatch.setattr(sme._WriterCache, "writer", failing)
    out = tmp_path / "out"
    with pytest.raises(OSError):
        sme.split_export(src, out, 10**9, max_open_files=2)
    assert list(out.iterdir()) == []


def test_cr13_the_bound_must_be_positive(tmp_path):
    sme = _split_tool()
    src = tmp_path / "in.csv"
    _write_export(src, [["T", "s", "m"]])
    with pytest.raises(sme.SplitError):
        sme.split_export(src, tmp_path / "out", 1000, max_open_files=0)


# --------------------------------------------------------------------------------------------------------------------
# CR15: the stage report never NAMES an identifying number, whether or not it is shaped like a stage key.
# --------------------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["9876543210", "98765_43210", "ab9876543210", "ID_987654321"])
def test_cr15_an_identifying_number_in_the_stage_column_is_counted_never_named(value):
    import json
    from app.api.services.feedback_uploads import report_unexpected_stages
    from seed_support import mitra_config_v1
    from test_feedback_uploads import make_df, new_rows
    config = {**mitra_config_v1("Youth"), "expect_stages": ["STATE_IDENTIFICATION"]}
    rows = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q"), ("R", value, "a", "a"),
                     ("B", "MYSTERY_STAGE", "q2", "q2")], submission_id="100", session="a" * 32)
    report = report_unexpected_stages(make_df(rows), config)
    assert value not in json.dumps(report)
    assert report["unexpected_stages"] == {"MYSTERY_STAGE": 1}
    assert report["other_stage_values"] == 1
    assert report["sessions_with_unexpected_stages"] == 1


def test_cr15_ordinary_stage_keys_with_a_few_digits_are_still_named():
    from app.api.services.feedback_uploads import report_unexpected_stages
    from seed_support import mitra_config_v1
    from test_feedback_uploads import make_df, new_rows
    config = {**mitra_config_v1("Youth"), "expect_stages": ["X"]}
    rows = new_rows([("O", "", "h", "h"), ("B", "Q12_ANSWER_2026", "q", "q"), ("B", "STAGE_7", "q", "q")],
                    submission_id="1", session="a" * 32)
    report = report_unexpected_stages(make_df(rows), config)
    assert set(report["unexpected_stages"]) == {"Q12_ANSWER_2026", "STAGE_7"} and "other_stage_values" not in report


# --------------------------------------------------------------------------------------------------------------------
# CR16: a config whose JSON session_type disagrees with its row is refused at upload (the file check reads the row,
# the builder reads the JSON: every session would be skipped to the DLQ).
# --------------------------------------------------------------------------------------------------------------------

def _target_world(monkeypatch, row_session_type, config):
    import app.api.services.uploads as uploads_service
    from app.database import feedback_operations as feedback_ops

    async def resolved(*a, **k):
        return {"program": {"id": "p", "name": "P"}, "leader": {"id": "l", "name": "Teacher"}, "tenant_code": "mitra"}

    async def active(program_id, source, **k):
        return {"config_id": "c", "config_version_id": "v", "version": 1, "session_type": row_session_type,
                "config": config}
    monkeypatch.setattr(feedback_ops, "get_program_and_leader", resolved)
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config", active)
    return uploads_service


def test_cr16_a_config_json_session_type_that_differs_from_the_row_is_refused(monkeypatch):
    from seed_support import mitra_config_v1
    from app.api.services.uploads import InvalidFeedbackFile
    config = mitra_config_v1("Youth")
    uploads_service = _target_world(monkeypatch, "Parent_Voice", config)    # row says Parent, JSON says Youth
    with pytest.raises(InvalidFeedbackFile) as info:
        asyncio.run(uploads_service.load_feedback_target("P", "Teacher", "mitra", "mitra"))
    text = info.value.errors[0]
    assert "session_type" in text and "Youth_Voice" not in text and "Parent_Voice" not in text   # no value echoed


def test_cr16_a_config_without_a_json_session_type_is_refused_too(monkeypatch):
    import copy
    from seed_support import mitra_config_v1
    from app.api.services.uploads import InvalidFeedbackFile
    config = copy.deepcopy(mitra_config_v1("Youth"))
    del config["session_type"]
    uploads_service = _target_world(monkeypatch, "Youth_Voice", config)
    with pytest.raises(InvalidFeedbackFile) as info:
        asyncio.run(uploads_service.load_feedback_target("P", "Teacher", "mitra", "mitra"))
    assert "session_type" in info.value.errors[0]


def test_cr16_a_consistent_config_is_accepted(monkeypatch):
    from seed_support import mitra_config_v1
    config = mitra_config_v1("Youth")
    uploads_service = _target_world(monkeypatch, "Youth_Voice", config)
    target = asyncio.run(uploads_service.load_feedback_target("P", "Teacher", "mitra", "mitra"))
    assert target["adapter"] == "conversation_rows"


# --------------------------------------------------------------------------------------------------------------------
# CR21 (already fixed by fixer S, C11: guard), CR22 (latest window only), CR23 (wording variants): version detection.
# --------------------------------------------------------------------------------------------------------------------

_T0 = dt.datetime(2026, 10, 1, 6, 0, tzinfo=dt.timezone.utc)
_OLD = ["What is your age?", "Which state do you live in?", "What do you like about school?", "Any wish for the future?"]
_V2 = ["What is your age?", "How do you feel about school these days?", "Which job do you want?", "Any wish for the future?"]


def _sessions(n, questions, first, stamped, step=dt.timedelta(minutes=10)):
    from app.services.feedback_version_detect import SessionQuestions
    return [SessionQuestions(f"s{first + i:05d}", _T0 + step * (first + i), stamped, tuple(questions)) for i in range(n)]


def test_cr21_sessions_that_share_one_instant_are_never_split_by_the_proposed_cut():
    """80 old-question and 80 new-question sessions at ONE instant, ids sorting old first: no proposal inside it."""
    from app.services.feedback_version_detect import SessionQuestions, detect
    before = _sessions(60, _OLD, 0, "V1")
    instant = _T0 + dt.timedelta(days=1)
    tied = ([SessionQuestions(f"a{n:03d}", instant, "V1", tuple(_OLD)) for n in range(80)]
            + [SessionQuestions(f"b{n:03d}", instant, "V1", tuple(_V2)) for n in range(80)])
    result = detect(before + tied, ["V1"])
    # the cut cannot fall inside the tied instant, and the whole tied block is only 50% new-question sessions, which
    # is below MIN_SHARE_AFTER: nothing is proposed (a cut inside the tie would have reported share_after 1.0)
    assert result["proposal"] is None, result["reason"]


def test_cr22_a_change_that_lies_inside_an_older_window_is_not_proposed_as_a_new_appended_version():
    """120 baseline V1, 180 V1 sessions with an unknown question, then 60 V2 sessions: V3 would start before V2."""
    from app.services.feedback_version_detect import detect
    odd = _OLD[:3] + ["Tell us about your village."]
    sessions = _sessions(120, _OLD, 0, "V1") + _sessions(180, odd, 120, "V1") + _sessions(60, _V2, 300, "V2")
    result = detect(sessions, ["V1", "V2"], baseline_sessions=40)
    assert result["proposal"] is None
    assert "latest" in result["reason"] or "older" in result["reason"]
    # the historical change is still reported, separately from the proposal
    assert any(q["text"] == "Tell us about your village." for q in result["historical_changes"])
    # and the proposal rule still works inside the latest window
    v3 = _V2[:3] + ["Which festival do you enjoy?"]
    later = detect(sessions + _sessions(80, v3, 360, "V2"), ["V1", "V2"], baseline_sessions=40)
    assert later["proposal"]["key"] == "V3"
    assert later["proposal"]["effective_from"] == (_T0 + dt.timedelta(minutes=10) * 360).isoformat()


def test_cr22_a_single_recorded_window_behaves_as_before():
    from app.services.feedback_version_detect import detect
    sessions = _sessions(120, _OLD, 0, "V1") + _sessions(80, _V2, 120, "V1")
    result = detect(sessions, ["V1"])
    assert result["proposal"]["key"] == "V2" and result["historical_changes"] == []


def test_cr23_wording_variants_are_not_boundary_evidence_unless_asked():
    """120 baseline, 80 sessions with ONLY a close wording variant, then 40 with that variant AND a new question."""
    from app.services.feedback_version_detect import detect
    # a close wording variant of the 3rd question ("about school" -> "about the school")
    variant = [_OLD[0], _OLD[1], "What do you like about the school?", _OLD[3]]
    new = [_OLD[0], _OLD[1], "What do you like about the school?", "Which job do you want?"]
    sessions = _sessions(120, _OLD, 0, "V1") + _sessions(80, variant, 120, "V1") + _sessions(40, new, 200, "V1")
    default = detect(sessions, ["V1"])
    assert default["proposal"] is not None
    assert default["proposal"]["effective_from"] == (_T0 + dt.timedelta(minutes=10) * 200).isoformat()   # not 120
    assert default["proposal"]["sessions_from_start"] == 40
    # the variant stays in the diagnostics
    assert any("similar_to" in q for q in default["new_questions"])
    assert len(default["wording_variants"]) >= 1
    # with include_wording the variant counts as evidence again (and the boundary moves to where it first appears)
    wording = detect(sessions, ["V1"], include_wording=True)
    assert wording["proposal"]["effective_from"] == (_T0 + dt.timedelta(minutes=10) * 120).isoformat()


# --------------------------------------------------------------------------------------------------------------------
# CR24: the README split example fits the persona upload caps and states the rule.
# --------------------------------------------------------------------------------------------------------------------

def test_cr24_the_readme_split_examples_fit_the_seeded_caps_and_state_the_rule():
    import re
    from pathlib import Path
    from seed_support import seed_config_blocks
    readme = (Path(__file__).resolve().parent.parent / "README.md").read_text(encoding="utf-8")
    caps = [b["config"]["max_csv_upload_bytes"] for b in seed_config_blocks() if b["config_type"] == "feedback_upload"]
    smallest_mb = min(caps) / 1_000_000
    shown = [float(m) for m in re.findall(r"split_mitra_export\.py[^\n]*--max-mb (\d+(?:\.\d+)?)", readme)]
    assert len(shown) > 0 and all(mb <= smallest_mb for mb in shown), (shown, smallest_mb)
    assert "max_csv_upload_bytes / 1_000_000" in readme and "at or below" in readme


# --------------------------------------------------------------------------------------------------------------------
# CR25: the check used by the CLI cleanup test can tell an omitted disconnect from a successful one.
# --------------------------------------------------------------------------------------------------------------------

def test_cr25_the_loop_keyed_registry_shows_a_pool_that_was_never_disconnected():
    from app.database.db import db
    before = dict(db._pools)
    sentinel = object()

    async def forgets_to_disconnect():
        db.pool = sentinel

    async def disconnects():
        db.pool = sentinel
        db.pool = None

    asyncio.run(disconnects())
    assert db.pool is None and db._pools == before          # clean exit: nothing registered
    asyncio.run(forgets_to_disconnect())
    try:
        assert db.pool is None                              # outside the loop the property cannot see the leak ...
        assert db._pools != before and sentinel in db._pools.values()   # ... the registry does
    finally:
        for loop in [k for k, v in db._pools.items() if v is sentinel]:
            db._pools.pop(loop, None)
    assert db._pools == before
