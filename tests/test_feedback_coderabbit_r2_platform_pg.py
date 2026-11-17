"""
CodeRabbit round 2 (fixer P2), real-Postgres tests (opt-in like tests/test_feedback_versions_pg.py:
FEEDBACK_TEST_DATABASE_URL names a Postgres server; tests/pg_support.py creates a scratch database for this file alone).
Synthetic data only.

  inline #17  an event with a programId and no leaderCategoryId is still checked against the program's tenant
  inline #18  a redelivery that moves a session to another persona (session_type, source or program) resets the PII flag
              and drops the old persona's extracted fields in the same transaction
  review body activities.py:92  the config-failure write only applies to a row that is still 'pending'
"""
import copy
import json
import uuid

import asyncpg
import pytest

import app.temporal.activities as activities
from app.database.operations import (
    TenantOwnershipError,
    insert_or_update_submission,
    mark_pending_submission_config_failed,
)
import factories
import pg_support
from test_feedback_versions_pg import _run

DSN = pg_support.SERVER_URL   # the fixture of tests/conftest.py swaps in this module's own scratch database

pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

_uid = factories.uid


async def _cleanup(conn, *tenants):
    for tenant in tenants:
        await conn.execute("DELETE FROM submissions WHERE tenant_code = $1", tenant)
        await conn.execute("DELETE FROM programs WHERE tenant_code = $1", tenant)
        await conn.execute("DELETE FROM leader_category WHERE tenant_code = $1", tenant)
        await conn.execute("DELETE FROM tenant WHERE code = $1", tenant)


def _without_leader(event):
    event = copy.deepcopy(event)
    del event["tags"]["leaderCategoryId"]
    return event


# --------------------------------------------------------------------------------------------------------------------
# inline #17
# --------------------------------------------------------------------------------------------------------------------

def test_an_event_without_a_leader_cannot_store_another_tenants_program():
    async def body(conn):
        await _cleanup(conn, "other", "abc")
        try:
            other = await factories.add_program(conn, "other", "PTM-other", "Parent")
            hostile = _without_leader(factories.conversation_event(
                {**other, "tenant": "abc"}, "Teacher_Voice", "ptm_app"))
            assert "leaderCategoryId" not in hostile["tags"] and hostile["tags"]["programId"] == other["program_id"]
            with pytest.raises(TenantOwnershipError, match="programId"):
                await insert_or_update_submission(conn, hostile)
            # nothing was stored for 'abc', and the program of 'other' is untouched
            assert await conn.fetchval("SELECT count(*) FROM submissions WHERE tenant_code = 'abc'") == 0
            assert await conn.fetchval("SELECT count(*) FROM feedbacks WHERE tenant_code = 'abc'") == 0
            row = await conn.fetchrow("SELECT name, tenant_code FROM programs WHERE id = $1::uuid", other["program_id"])
            assert (row["name"], row["tenant_code"]) == ("PTM-other", "other")
        finally:
            await _cleanup(conn, "other", "abc")
    _run(body)


def test_an_event_without_a_leader_may_still_name_a_program_of_its_own_tenant():
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            own = await factories.add_program(conn, "abc", "PTM-abc", "Parent")
            event = _without_leader(factories.conversation_event(own, "Teacher_Voice", "ptm_app"))
            result = await insert_or_update_submission(conn, event)
            assert result["changed"] is True
            stored = await conn.fetchval(
                "SELECT program_id::text FROM submissions WHERE submission_id = $1 AND tenant_code = 'abc'",
                event["submissionId"])
            assert stored == own["program_id"]
        finally:
            await _cleanup(conn, "abc")
    _run(body)


def test_an_event_without_a_leader_for_a_program_that_does_not_exist_keeps_the_old_behaviour():
    """The ownership check has nothing to compare: it must not raise TenantOwnershipError (the write then fails the way
    it always did, on the foreign key of submissions.program_id)."""
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program = {"tenant": "abc", "program_id": _uid(), "leader_id": _uid(), "program_name": "P", "leader_name": "L"}
            event = _without_leader(factories.conversation_event(program, "Teacher_Voice", "ptm_app"))
            with pytest.raises(asyncpg.exceptions.ForeignKeyViolationError):
                await insert_or_update_submission(conn, event)
        finally:
            await _cleanup(conn, "abc")
    _run(body)


# --------------------------------------------------------------------------------------------------------------------
# inline #18
# --------------------------------------------------------------------------------------------------------------------

async def _store_and_extract(conn, program, session_type="Teacher_Voice", source="ptm_app"):
    """A stored session that has been through an extraction: PII flag set, extracted fields and the extraction record."""
    event = factories.conversation_event(program, session_type, source)
    await insert_or_update_submission(conn, event)
    sid = event["submissionId"]
    await conn.execute(
        "INSERT INTO feedback_extracted_data (submission_id, tenant_code, field_name, value, status) "
        "VALUES ($1, $2, 'age', '18-24', 'extracted'), ($1, $2, 'gender', NULL, 'not_provided')",
        sid, program["tenant"])
    await conn.execute(
        "UPDATE feedback_submissions SET pii_detected = FALSE, "
        "meta_data = COALESCE(meta_data, '{}'::jsonb) || '{\"bot_id\": \"7\", \"extraction\": {\"pii_types\": []}}'::jsonb "
        "WHERE submission_id = $1 AND tenant_code = $2", sid, program["tenant"])
    await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1 AND tenant_code = $2",
                       sid, program["tenant"])
    return event


async def _state(conn, event):
    sid, tenant = event["submissionId"], event["tenantCode"]
    row = await conn.fetchrow(
        "SELECT fs.session_type, fs.source, fs.pii_detected, fs.meta_data::text AS meta, s.status, s.program_id::text AS program_id "
        "FROM feedback_submissions fs JOIN submissions s USING (submission_id, tenant_code) "
        "WHERE fs.submission_id = $1 AND fs.tenant_code = $2", sid, tenant)
    fields = await conn.fetchval(
        "SELECT count(*) FROM feedback_extracted_data WHERE submission_id = $1 AND tenant_code = $2", sid, tenant)
    return dict(row), fields


@pytest.mark.parametrize("change", ["session_type", "source", "program"])
def test_a_redelivery_that_changes_only_the_persona_invalidates_the_old_extraction(change):
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program = await factories.add_program(conn, "abc", "PTM-abc", "Parent")
            second_program = await factories.add_program(conn, "abc", "PTM-abc-2", "Parent2")
            event = await _store_and_extract(conn, program)
            before, fields_before = await _state(conn, event)
            assert before["pii_detected"] is False and fields_before == 2 and "extraction" in before["meta"]

            moved = copy.deepcopy(event)           # the SAME messages: only the persona selector changes
            if change == "session_type":
                moved["data"]["sessionType"] = "Parent_Voice"
            elif change == "source":
                moved["data"]["source"] = moved["tags"]["source"] = "other_app"
            else:
                moved["tags"].update(programId=second_program["program_id"], programName="PTM-abc-2",
                                     leaderCategoryId=second_program["leader_id"], leaderCategoryName="Parent2")
            result = await insert_or_update_submission(conn, moved)
            assert result["changed"] is True and result["messages_inserted"] == 0 and result["messages_updated"] == 0

            after, fields_after = await _state(conn, event)
            assert after["pii_detected"] is None                      # "not checked" again
            assert fields_after == 0                                  # the old persona's fields are gone
            meta = json.loads(after["meta"])
            assert "extraction" not in meta and meta["bot_id"] == "7"  # only the extraction record goes
            assert after["status"] == "pending"                       # it runs again for the new persona
        finally:
            await _cleanup(conn, "abc")
    _run(body)


def test_a_redelivery_of_the_same_persona_keeps_the_extraction():
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program = await factories.add_program(conn, "abc", "PTM-abc", "Parent")
            event = await _store_and_extract(conn, program)
            result = await insert_or_update_submission(conn, copy.deepcopy(event))
            assert result["changed"] is False
            state, fields = await _state(conn, event)
            assert state["pii_detected"] is False and fields == 2 and "extraction" in state["meta"]
            assert state["status"] == "success"
        finally:
            await _cleanup(conn, "abc")
    _run(body)


def test_a_first_delivery_is_not_a_persona_change():
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program = await factories.add_program(conn, "abc", "PTM-abc", "Parent")
            event = factories.conversation_event(program, "Teacher_Voice", "ptm_app")
            await insert_or_update_submission(conn, event)
            state, fields = await _state(conn, event)
            assert state["pii_detected"] is None and fields == 0 and state["status"] == "pending"
        finally:
            await _cleanup(conn, "abc")
    _run(body)


# --------------------------------------------------------------------------------------------------------------------
# review body activities.py:92: the failure write only applies to a row that is still pending
# --------------------------------------------------------------------------------------------------------------------

async def _stored_pending(conn, program):
    event = factories.conversation_event(program, "Teacher_Voice", "ptm_app")
    await insert_or_update_submission(conn, event)
    return event["submissionId"]


def test_the_failure_write_marks_a_pending_row_and_leaves_any_other_state_alone():
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program = await factories.add_program(conn, "abc", "PTM-abc", "Parent")
            pending, running = await _stored_pending(conn, program), await _stored_pending(conn, program)
            await conn.execute("UPDATE submissions SET status = 'processing' WHERE submission_id = $1", running)
            assert await mark_pending_submission_config_failed(conn, pending, "abc", "no config") is True
            assert await mark_pending_submission_config_failed(conn, running, "abc", "no config") is False
            rows = {r["submission_id"]: r for r in await conn.fetch(
                "SELECT submission_id, status, process_status::text AS ps FROM submissions WHERE tenant_code = 'abc'")}
            assert rows[pending]["status"] == "failed"
            status = json.loads(rows[pending]["ps"])
            assert "no config" in status["error_message"] and "retry_attempt" not in status
            assert rows[running]["status"] == "processing" and rows[running]["ps"] is None
        finally:
            await _cleanup(conn, "abc")
    _run(body)


def test_a_row_that_moves_to_processing_during_the_config_lookup_is_not_overwritten_by_the_failure(monkeypatch):
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program = await factories.add_program(conn, "abc", "PTM-abc", "Parent")
            sid = await _stored_pending(conn, program)

            async def steps(pool_conn, sub_type, sub_id, tenant, memo=None):
                # the workflow (or the consumer) takes the row while the activity awaits the config lookup
                await conn.execute("UPDATE submissions SET status = 'processing' WHERE submission_id = $1", sub_id)
                raise ValueError("no config for the persona")

            monkeypatch.setattr(activities, "get_process_steps", steps)
            returned = await activities.fetch_pending_submissions_activity({"limit": 5})
            assert returned == []
            row = await conn.fetchrow(
                "SELECT status, process_status FROM submissions WHERE submission_id = $1 AND tenant_code = 'abc'", sid)
            assert row["status"] == "processing" and row["process_status"] is None   # not replaced by 'failed'
        finally:
            await _cleanup(conn, "abc")
    _run(body)


def test_a_pending_row_with_a_bad_config_is_still_marked_failed_by_the_sweep(monkeypatch):
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program = await factories.add_program(conn, "abc", "PTM-abc", "Parent")
            sid = await _stored_pending(conn, program)

            async def steps(pool_conn, sub_type, sub_id, tenant, memo=None):
                raise ValueError("no config for the persona")

            monkeypatch.setattr(activities, "get_process_steps", steps)
            assert await activities.fetch_pending_submissions_activity({"limit": 5}) == []
            assert await conn.fetchval(
                "SELECT status FROM submissions WHERE submission_id = $1 AND tenant_code = 'abc'", sid) == "failed"
        finally:
            await _cleanup(conn, "abc")
    _run(body)
