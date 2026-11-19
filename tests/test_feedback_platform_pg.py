"""
Real-Postgres tests of the generic feedback platform (opt-in like tests/test_feedback_versions_pg.py:
FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards)). Synthetic data only.

  * two tenants: an event of tenant A can neither rename tenant B nor rename or re-parent B's leader category / program
    (upsert_metadata), and an existing tenant keeps its name;
  * a source is a free label and the identity rules follow the event's adapter: a second program (tenant 'abc',
    source 'ptm_app') is stored with no code or schema change, a wide_form event of any source is immutable and
    deduplicated by its natural key, and a conversation_rows event may not carry a natural key.
"""
import uuid

import pytest

from app.database.operations import TenantOwnershipError, insert_or_update_submission
import factories
import pg_support
from test_feedback_versions_pg import _run

DSN = pg_support.SERVER_URL   # the fixture of tests/conftest.py swaps in this module's own scratch database

pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")


_uid = factories.uid


def _event(tenant, program_id, leader_id, *, leader_name="Teacher", program_name="PTM", source="ptm_app",
           adapter="conversation_rows", sid=None, source_ref=None):
    program = {"tenant": tenant, "program_id": program_id, "leader_id": leader_id, "program_name": program_name,
               "leader_name": leader_name}
    return factories.conversation_event(
        program, "Teacher_Voice", source, "V1", adapter=adapter, sid=sid, source_ref=source_ref,
        messages=factories.conversation_messages("Who is attending?", ("the parents",), "ATTENDEE"))


async def _cleanup(conn, *tenants):
    for tenant in tenants:
        await conn.execute("DELETE FROM submissions WHERE tenant_code = $1", tenant)
        await conn.execute("DELETE FROM programs WHERE tenant_code = $1", tenant)
        await conn.execute("DELETE FROM leader_category WHERE tenant_code = $1", tenant)
        await conn.execute("DELETE FROM tenant WHERE code = $1", tenant)


async def _two_tenants(conn):
    """tenant 'other' ('Other Foundation') with a leader category + program of its own; returns their ids."""
    await _cleanup(conn, "other", "abc")
    await conn.execute("INSERT INTO tenant (name, code) VALUES ('Other Foundation', 'other')")
    other = await factories.add_program(conn, "other", "PTM-other", "Parent")
    return other["leader_id"], other["program_id"]


async def _rows(conn, leader, program):
    leader_row = await conn.fetchrow("SELECT name, tenant_code FROM leader_category WHERE id = $1::uuid", leader)
    program_row = await conn.fetchrow(
        "SELECT name, tenant_code, leaders_id::text AS leaders_id FROM programs WHERE id = $1::uuid", program)
    return dict(leader_row), dict(program_row)


def test_an_event_of_tenant_a_cannot_rename_or_take_over_the_rows_of_tenant_b():
    async def body(conn):
        leader_b, program_b = await _two_tenants(conn)
        before = await _rows(conn, leader_b, program_b)
        try:
            # tenant 'abc' (new) names tenant 'other''s leader category id with a new name
            hostile = _event("abc", _uid(), leader_b, leader_name="RENAMED-BY-ABC")
            with pytest.raises(TenantOwnershipError, match="leaderCategoryId"):
                await insert_or_update_submission(conn, hostile)
            assert await _rows(conn, leader_b, program_b) == before

            # tenant 'abc' with its own leader but tenant 'other''s PROGRAM id
            own_leader = _uid()
            hostile = _event("abc", program_b, own_leader, program_name="RENAMED-PROGRAM")
            with pytest.raises(TenantOwnershipError, match="programId"):
                await insert_or_update_submission(conn, hostile)
            assert await _rows(conn, leader_b, program_b) == before
            # the failed event wrote nothing at all (one transaction): no leader category for 'abc', no submission
            assert await conn.fetchval("SELECT count(*) FROM leader_category WHERE tenant_code = 'abc'") == 0
            assert await conn.fetchval("SELECT count(*) FROM submissions WHERE tenant_code = 'abc'") == 0
        finally:
            await _cleanup(conn, "other", "abc")
    _run(body)


def test_an_existing_tenant_is_never_renamed_by_its_own_events():
    async def body(conn):
        leader_b, program_b = await _two_tenants(conn)
        try:
            await insert_or_update_submission(conn, _event("other", program_b, leader_b, leader_name="Parent",
                                                           program_name="PTM-other", source="ptm_app"))
            names = {r["code"]: r["name"] for r in await conn.fetch("SELECT code, name FROM tenant")}
            assert names["other"] == "Other Foundation"                     # it was "Other" before the fix
            # a brand-new tenant is created with its derived name
            await insert_or_update_submission(conn, _event("abc", _uid(), _uid()))
            assert {r["code"]: r["name"] for r in await conn.fetch("SELECT code, name FROM tenant")}["abc"] == "Abc"
        finally:
            await _cleanup(conn, "other", "abc")
    _run(body)


def test_the_owning_tenant_can_still_update_its_own_leader_category_and_program():
    async def body(conn):
        leader_b, program_b = await _two_tenants(conn)
        try:
            await insert_or_update_submission(conn, _event("other", program_b, leader_b, leader_name="Parents",
                                                           program_name="PTM-other-2"))
            leader_row, program_row = await _rows(conn, leader_b, program_b)
            assert leader_row == {"name": "Parents", "tenant_code": "other"}
            assert program_row["name"] == "PTM-other-2" and program_row["tenant_code"] == "other"
        finally:
            await _cleanup(conn, "other", "abc")
    _run(body)


def test_a_second_program_is_stored_with_a_free_source_label_and_no_config_row():
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            event = _event("abc", _uid(), _uid(), source="ptm_app", adapter="conversation_rows")
            result = await insert_or_update_submission(conn, event)
            assert result["changed"] is True and result["messages_inserted"] == 2
            row = await conn.fetchrow(
                "SELECT source, session_type, source_ref FROM feedback_submissions WHERE tenant_code = 'abc'")
            assert (row["source"], row["session_type"], row["source_ref"]) == ("ptm_app", "Teacher_Voice", None)
            # a malformed label or adapter is refused by the writer too
            for field, value in (("source", "Not A Label"), ("adapter", "excel")):
                bad = _event("abc", event["tags"]["programId"], event["tags"]["leaderCategoryId"])
                bad["data"][field] = value
                with pytest.raises(ValueError, match=f"data.{field}"):
                    await insert_or_update_submission(conn, bad)
        finally:
            await _cleanup(conn, "abc")
    _run(body)


def test_wide_form_identity_rules_follow_the_adapter_not_the_source_name():
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            program, leader = _uid(), _uid()
            ref = "2026-10-02T10:09:20:" + uuid.uuid4().hex + uuid.uuid4().hex
            first = _event("abc", program, leader, source="ptm_form", adapter="wide_form", sid=_uid(), source_ref=ref)
            stored = await insert_or_update_submission(conn, first)
            assert stored["changed"] is True
            # the same natural key under another submission id: a no-op returning the stored submission
            again = _event("abc", program, leader, source="ptm_form", adapter="wide_form", sid=_uid(), source_ref=ref)
            result = await insert_or_update_submission(conn, again)
            assert result["changed"] is False and result["submission_id"] == first["submissionId"]
            assert await conn.fetchval(
                "SELECT count(*) FROM feedback_submissions WHERE tenant_code = 'abc' AND source_ref = $1", ref) == 1
            # a wide_form event without a natural key, and a conversation_rows event WITH one (under the label
            # 'google_forms'!), are both refused
            no_ref = _event("abc", program, leader, source="ptm_form", adapter="wide_form", sid=_uid())
            with pytest.raises(ValueError, match="sourceRef"):
                await insert_or_update_submission(conn, no_ref)
            rows_with_ref = _event("abc", program, leader, source="google_forms", adapter="conversation_rows",
                                   source_ref=ref)
            with pytest.raises(ValueError, match="sourceRef"):
                await insert_or_update_submission(conn, rows_with_ref)
        finally:
            await _cleanup(conn, "abc")
    _run(body)


def test_unsafe_identifiers_are_refused_by_the_writer():
    async def body(conn):
        await _cleanup(conn, "abc")
        try:
            for field, value in (("submissionId", "1\nFORGED"), ("sessionId", "s\r\nFORGED"), ("tenantCode", "abc\n")):
                event = _event("abc", _uid(), _uid())
                event[field] = value
                with pytest.raises(ValueError, match="unsafe") as raised:
                    await insert_or_update_submission(conn, event)
                assert "FORGED" not in str(raised.value)
            event = _event("abc", _uid(), _uid())
            event["data"]["messages"][1]["sequence"] = 2147483648
            with pytest.raises(ValueError, match="sequence must be an integer from 1 to 2147483647"):
                await insert_or_update_submission(conn, event)
        finally:
            await _cleanup(conn, "abc")
    _run(body)
