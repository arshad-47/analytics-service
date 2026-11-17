"""
CodeRabbit round 2 (fixer P2), platform / retry / uploads part, in-memory tests (the real-Postgres ones are in
tests/test_feedback_coderabbit_r2_platform_pg.py). Synthetic data only.

  inline #6 / nitpick activities.py:68  the pending-queue refill stays bounded by the page size
  inline #10                            the stage report holds counts only, never a value of the file
  inline #11 / #27                      the 409 of /retry-step through the real retry path (no patched retry service)
  inline #20                            resolved steps are keyed by (tenant_code, submission_id)
  review body feedback_uploads.py:429   a sequence of thousands of leading zeros is a value, not a crash
  review body ingestion_validation.py:170  the Kafka path takes canonical identifiers only (a padded one is refused)
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

import app.api.routes.uploads as uploads_routes
import app.services.retry as retry_service
import app.temporal.activities as activities
from app.api.models.submissions import StepRetryRequest
from app.api.services.feedback_uploads import build_feedback_events, report_unexpected_stages
from app.config import settings
from app.services.ingestion_validation import validate_ingestion_schema
from conftest import FakeConn, install_fake_db
from seed_support import mitra_config_v1
from test_feedback_uploads import LEADER, PROGRAM, PUBLISHED, config_with_v2, make_df, new_rows


# --------------------------------------------------------------------------------------------------------------------
# inline #6 + nitpick: refill queries are bounded by the requested page size
# --------------------------------------------------------------------------------------------------------------------

def test_a_backlog_of_invalid_configs_never_makes_the_refill_ask_for_more_than_the_page_size(monkeypatch):
    queue = [{"submission_id": f"bad{n:04d}", "tenant_code": "mitra", "submission_type": "feedback"} for n in range(250)]
    queue += [{"submission_id": f"ok{n:03d}", "tenant_code": "mitra", "submission_type": "feedback"} for n in range(25)]
    asked = []

    async def fetch(sql, limit):
        asked.append(limit)
        return list(queue[:limit])

    async def mark(conn, sub_id, tenant, reason):
        queue[:] = [r for r in queue if r["submission_id"] != sub_id]   # a failed row leaves the pending queue
        return True

    async def steps(conn, sub_type, sub_id, tenant, memo=None):
        if sub_id.startswith("bad"):
            raise ValueError("no config")
        return [{"name": "feedback_metadata_extraction"}]

    monkeypatch.setattr(activities, "get_process_steps", steps)
    monkeypatch.setattr(activities, "mark_pending_submission_config_failed", mark)

    async def run():
        conn = install_fake_db(monkeypatch, activities)
        conn.fetch = AsyncMock(side_effect=fetch)
        return await activities.fetch_pending_submissions_activity({"limit": 10})
    out = asyncio.run(run())
    assert [r["submission_id"] for r in out] == [f"ok{n:03d}" for n in range(10)]   # the page is full, not short
    assert len(asked) == 26 and set(asked) == {10}   # 25 pages of rejected rows, then the good one: never 20, 30, ...


# --------------------------------------------------------------------------------------------------------------------
# inline #20: two tenants, the same submission_id in one all-tenant page
# --------------------------------------------------------------------------------------------------------------------

def test_the_same_submission_id_in_two_tenants_gets_each_tenants_own_steps(monkeypatch):
    def row(tenant, status="failed"):
        return {"submission_id": "s1", "session_id": f"sess-{tenant}", "tenant_code": tenant, "submission_type": "feedback",
                "status": status, "process_status": None, "updated_at": 1}

    extraction = [{"name": "feedback_metadata_extraction"}]

    async def steps(conn, sub_type, sub_id, tenant, memo=None):
        return extraction if tenant == "tenant_a" else []   # tenant b's persona has no steps

    monkeypatch.setattr(settings, "BATCH_SIZE", 100)
    monkeypatch.setattr(retry_service, "fetch_retryable_submissions",
                        AsyncMock(side_effect=[[row("tenant_a"), row("tenant_b")]]))
    monkeypatch.setattr(retry_service, "claim_submission_for_retry", AsyncMock(return_value={"retry_attempt": 1}))
    monkeypatch.setattr(retry_service, "get_process_steps", steps)
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(retry_service, "get_temporal_client", AsyncMock(return_value=client))

    async def run():
        install_fake_db(monkeypatch, retry_service)
        return await retry_service.dispatch_all_retry_workflows(None, "2026-01-01T00:00:00+00:00", "sweep")
    result = asyncio.run(run())
    started = {c.args[1]["tenant_code"]: c.args[1]["process_steps"] for c in client.start_workflow.await_args_list}
    assert started == {"tenant_a": extraction, "tenant_b": []}   # the later row must not overwrite the earlier one
    assert result["started"] == 2


def test_a_config_error_of_one_tenant_does_not_skip_the_other_tenants_row_with_the_same_id(monkeypatch):
    def row(tenant):
        return {"submission_id": "s1", "session_id": f"sess-{tenant}", "tenant_code": tenant, "submission_type": "feedback",
                "status": "failed", "process_status": None, "updated_at": 1}

    async def steps(conn, sub_type, sub_id, tenant, memo=None):
        if tenant == "tenant_b":
            raise ValueError("no config")
        return [{"name": "feedback_metadata_extraction"}]

    monkeypatch.setattr(settings, "BATCH_SIZE", 100)
    monkeypatch.setattr(retry_service, "fetch_retryable_submissions",
                        AsyncMock(side_effect=[[row("tenant_a"), row("tenant_b")]]))
    monkeypatch.setattr(retry_service, "claim_submission_for_retry", AsyncMock(return_value={"retry_attempt": 1}))
    monkeypatch.setattr(retry_service, "get_process_steps", steps)
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(retry_service, "get_temporal_client", AsyncMock(return_value=client))

    async def run():
        install_fake_db(monkeypatch, retry_service)
        return await retry_service.dispatch_all_retry_workflows(None, "2026-01-01T00:00:00+00:00", "sweep")
    result = asyncio.run(run())
    assert [c.args[1]["tenant_code"] for c in client.start_workflow.await_args_list] == ["tenant_a"]
    assert result["started"] == 1 and result["skipped"] == 1


# --------------------------------------------------------------------------------------------------------------------
# inline #11 / #27: /retry-step -> 409 through the REAL retry path (only the database is faked)
# --------------------------------------------------------------------------------------------------------------------

def _route_row(status="failed"):
    return {"submission_id": "f1", "session_id": "x", "tenant_code": "mitra", "status": status,
            "submission_type": "feedback", "process_status": None}


def test_retry_step_409_comes_from_the_real_persona_config_lookup(monkeypatch):
    """No patch of retry_service / get_process_steps: the route finds the row, the real retry service asks the real
    get_feedback_process_steps, whose (fake) database holds a stored session but NO active config for its persona."""
    monkeypatch.setattr(uploads_routes, "fetch_retryable_submission_for_retry", AsyncMock(return_value=_route_row()))
    monkeypatch.setattr(uploads_routes, "get_temporal_client", AsyncMock(return_value=MagicMock()))
    conn = FakeConn()
    conn.fetchrow = AsyncMock(side_effect=[
        {"session_type": "Youth_Voice", "source": "mitra", "program_id": "dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a"},
        None,   # no active upload config for the persona
    ])
    claim = AsyncMock()
    monkeypatch.setattr(retry_service, "claim_submission_for_retry", claim)
    body = StepRetryRequest(submission_id="f1", session_id="x", tenant_code="mitra")

    async def run():
        install_fake_db(monkeypatch, uploads_routes, conn)
        install_fake_db(monkeypatch, retry_service, conn)
        await uploads_routes.retry_step(body, None)
    with pytest.raises(HTTPException) as info:
        asyncio.run(run())
    assert info.value.status_code == 409 and "Cannot resolve the process steps of submission 'f1'" in info.value.detail
    claim.assert_not_awaited()   # nothing was claimed or started: the row stays failed


def test_retry_step_reads_a_feedback_submissions_steps_from_the_persona_config_not_settings(monkeypatch):
    monkeypatch.setattr(uploads_routes, "fetch_retryable_submission_for_retry", AsyncMock(return_value=_route_row()))
    client = MagicMock()
    client.start_workflow = AsyncMock()
    monkeypatch.setattr(uploads_routes, "get_temporal_client", AsyncMock(return_value=client))
    monkeypatch.setattr(retry_service, "claim_submission_for_retry", AsyncMock(return_value={"retry_attempt": 1}))

    guard = MagicMock()
    guard.get_process_config.side_effect = AssertionError("settings must not be read for a feedback submission")
    guard.TEMPORAL_QUEUE = "queue"
    monkeypatch.setattr(retry_service, "settings", guard)
    conn = FakeConn()
    conn.fetchrow = AsyncMock(side_effect=[
        {"session_type": "Youth_Voice", "source": "mitra", "program_id": "dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a"},
        {"version": 1, "config": {**mitra_config_v1("Youth"), "process_steps": [{"name": "feedback_metadata_extraction"}]}},
    ])
    body = StepRetryRequest(submission_id="f1", session_id="x", tenant_code="mitra")

    async def run():
        install_fake_db(monkeypatch, uploads_routes, conn)
        install_fake_db(monkeypatch, retry_service, conn)
        return await uploads_routes.retry_step(body, None)
    response = asyncio.run(run())
    assert response.message == "Retry workflow started."
    assert client.start_workflow.await_args.args[1]["process_steps"] == [{"name": "feedback_metadata_extraction"}]


# --------------------------------------------------------------------------------------------------------------------
# inline #10: the stage report never carries a value of the file
# --------------------------------------------------------------------------------------------------------------------

def test_a_personal_name_in_the_stage_column_is_not_in_the_report():
    config = {**mitra_config_v1("Youth"), "expect_stages": ["STATE_IDENTIFICATION"]}
    rows = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q"),
                     ("R", "John_Smith", "a", "a")], submission_id="100", session="a" * 32)
    report = report_unexpected_stages(make_df(rows), config)
    assert "John" not in json.dumps(report) and "Smith" not in json.dumps(report)
    assert report == {"sessions": 1, "sessions_with_unexpected_stages": 1, "unexpected_stage_count": 1}


# --------------------------------------------------------------------------------------------------------------------
# review body feedback_uploads.py:429-430: a huge digit string is a value, never an uncaught ValueError
# --------------------------------------------------------------------------------------------------------------------

def _sequence_session(sequence_cell, sub="100", sess="a" * 32):
    rows = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q"), ("R", "STATE_IDENTIFICATION", "a", "a")],
                    submission_id=sub, session=sess)
    rows[2]["sequence"] = sequence_cell
    return rows


def _build(df):
    return build_feedback_events(df, config_with_v2("Parent"), program_info=PROGRAM, leader_info=LEADER,
                                 tenant_code="mitra", source="mitra", published_at=PUBLISHED)


def test_thousands_of_leading_zeros_do_not_crash_the_upload_builder():
    """On Python 3.10.7+ int() refuses a string of more than 4300 digits, leading zeros included: the reproducer is
    5000 zeros and a 1. The value is 1 (a duplicate of the opening row here), so the session is skipped with a
    problem, and the other session of the file is still built."""
    good = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q"),
                     ("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="101", session="b" * 32)
    df = make_df(_sequence_session("0" * 5000 + "1") + good)
    events, skipped = _build(df)
    assert [e["submissionId"] for e in events] == ["101"]
    assert [s["submissionId"] for s in skipped] == ["100"]
    assert all("0000" not in problem for problem in skipped[0]["problems"])   # the cell value is never echoed


def test_a_zero_padded_sequence_is_read_as_its_value():
    rows = _sequence_session("0" * 5000 + "3")   # 3: the respondent row after the question at sequence 2
    events, skipped = _build(make_df(rows))
    assert skipped == [] and [m["sequence"] for m in events[0]["data"]["messages"]] == [1, 2, 3]


@pytest.mark.parametrize("cell, fragment", [
    ("0" * 5000, "must be 1 or greater"),           # zero, however many digits
    ("1" + "0" * 5000, "is above"),                 # far beyond the stored column
    ("0" * 5000 + "99999999999", "is above"),       # 11 significant digits after the zeros
])
def test_huge_digit_strings_become_row_problems(cell, fragment):
    events, skipped = _build(make_df(_sequence_session(cell)))
    assert events == [] and len(skipped) == 1
    assert any(fragment in problem for problem in skipped[0]["problems"])


# --------------------------------------------------------------------------------------------------------------------
# review body ingestion_validation.py:170: the Kafka path takes canonical identifiers
# --------------------------------------------------------------------------------------------------------------------

def test_the_kafka_path_refuses_a_padded_session_type_the_producer_never_sends():
    """Decision: the Kafka create path takes canonical values only (our own upload service is the only producer and
    sends the config's session_type as stored). A padded value is refused at the boundary, with the same
    message as any other unsupported identifier, and never echoed. The direct writer still trims defensively
    (tests/test_feedback_consumer.py::test_feedback_write_trims_session_type_and_version), which no Kafka event reaches."""
    from test_feedback_ingestion_validation import SCHEMA, _load
    event = _load("create", "create_feedback.json")
    assert validate_ingestion_schema(event, "feedback", "create", feedback_schema=SCHEMA) == []
    for padded in ("  Parent_Voice ", "Parent_Voice ", " Parent_Voice"):
        event["data"]["sessionType"] = padded
        (problem,) = validate_ingestion_schema(event, "feedback", "create", feedback_schema=SCHEMA)
        assert "'data.sessionType' must be 1 to 64 characters" in problem and "Parent_Voice" not in problem


# --------------------------------------------------------------------------------------------------------------------
# inline #9 (already fixed by CR2): a Google Forms config without session_type / a form's questionnaire_version
# --------------------------------------------------------------------------------------------------------------------

def test_an_incomplete_google_forms_config_is_refused_at_upload_not_at_storage():
    from app.api.services import feedback_gforms
    from seed_support import GFORMS_PERSONAS, gforms_config_v1
    config = gforms_config_v1(GFORMS_PERSONAS[0])
    feedback_gforms._check_config(config)   # the seeded one is complete
    without_type = {k: v for k, v in config.items() if k != "session_type"}
    with pytest.raises(ValueError, match="session_type"):
        feedback_gforms._check_config(without_type)
    form_key = next(iter(config["forms"]))
    without_version = {**config, "forms": {**config["forms"], form_key: {
        k: v for k, v in config["forms"][form_key].items() if k != "questionnaire_version"}}}
    with pytest.raises(ValueError, match="questionnaire_version"):
        feedback_gforms._check_config(without_version)
