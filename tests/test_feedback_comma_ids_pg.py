"""
Real-Postgres test: a session whose submission id and session id contain commas ('710001,710002') is stored, found by
the delta lookup, left untouched by an identical re-send, grown by a longer one, and deleted. Opt-in like
tests/test_feedback_versions_pg.py (FEEDBACK_TEST_DATABASE_URL names a Postgres server; tests/pg_support.py creates a
scratch database for this file alone). Synthetic data only.
"""
import pytest

from app.database.feedback_operations import get_mitra_session_state
from app.database.operations import delete_submission, insert_or_update_submission
import factories
import pg_support
from test_feedback_versions_pg import _run

DSN = pg_support.SERVER_URL   # the fixture of tests/conftest.py swaps in this module's own scratch database

pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

SID = "710001,710002"


def test_comma_ids_are_stored_looked_up_resent_grown_and_deleted():
    async def body(conn):
        program = await factories.add_program(conn, "abc", "PTM", "Teacher")
        event = factories.conversation_event(program, "Teacher_Voice", "ptm_app", "V1", sid=SID)
        event["sessionId"] = "s,1,2"
        try:
            first = await insert_or_update_submission(conn, event)
            assert first["submission_id"] == SID and first["changed"] is True
            assert await conn.fetchval("SELECT count(*) FROM submissions WHERE submission_id = $1", SID) == 1
            assert await conn.fetchval(
                "SELECT session_id FROM submissions WHERE submission_id = $1 AND tenant_code = 'abc'", SID) == "s,1,2"

            state = await get_mitra_session_state("abc", [SID, "not-stored"])
            assert list(state) == [SID] and state[SID]["session_id"] == "s,1,2"

            assert (await insert_or_update_submission(conn, event))["changed"] is False   # identical re-send

            grown = factories.conversation_event(
                program, "Teacher_Voice", "ptm_app", "V1", sid=SID,
                messages=factories.conversation_messages("How old are you?", ("41", "and a second answer"), "Q1"))
            grown["sessionId"] = "s,1,2"
            assert (await insert_or_update_submission(conn, grown))["changed"] is True
            assert (await get_mitra_session_state("abc", [SID]))[SID]["message_count"] > state[SID]["message_count"]

            assert await delete_submission(conn, SID, "abc") is True
            assert await conn.fetchval("SELECT count(*) FROM submissions WHERE submission_id = $1", SID) == 0
            assert await conn.fetchval("SELECT count(*) FROM feedbacks WHERE submission_id = $1", SID) == 0
        finally:
            for table in ("submissions",):
                await conn.execute(f"DELETE FROM {table} WHERE tenant_code = 'abc'")
            await conn.execute("DELETE FROM programs WHERE tenant_code = 'abc'")
            await conn.execute("DELETE FROM leader_category WHERE tenant_code = 'abc'")
            await conn.execute("DELETE FROM tenant WHERE code = 'abc'")
    _run(body)
