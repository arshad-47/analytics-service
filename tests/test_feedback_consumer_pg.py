"""
Real-Postgres tests for the feedback branch of insert_or_update_submission / delete_submission.

Skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database
for this file and drops it afterwards). The file applies schema.sql itself
(the way app/database/db.py does it, idempotent), seeds tenant 'mitra' plus a leader category / program, uses
random submission ids and cleans up the rows it creates. Synthetic data only.
"""
import asyncio
import json
import uuid

import asyncpg
import pytest
from seed_support import FEEDBACK_PROGRAM_NAME
import pg_support

from app.database import operations
from app.database.operations import (
    FeedbackUpdateNotSupportedError,
    delete_submission,
    insert_or_update_submission,
)

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(DSN is None, reason="FEEDBACK_TEST_DATABASE_URL not set")

LEADER_ID = "0a0a0a0a-0000-4000-8000-00000000d001"
PROGRAM_ID = "0a0a0a0a-0000-4000-8000-00000000d002"


_SCHEMA_APPLIED = False


async def _ensure_schema(conn):
    global _SCHEMA_APPLIED
    if not _SCHEMA_APPLIED:
        await conn.execute(pg_support.schema_sql())
        _SCHEMA_APPLIED = True


def _run(body):
    """Runs `body(conn, created)` on a fresh connection with tenant/leader/program seeded; cleans up afterwards."""
    async def main():
        conn = await asyncpg.connect(DSN)
        created = []
        try:
            await _ensure_schema(conn)
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute(
                "INSERT INTO leader_category (id, name, tenant_code) VALUES ($1, 'Youth', 'mitra') ON CONFLICT (id) DO NOTHING",
                LEADER_ID)
            await conn.execute(
                "INSERT INTO programs (id, leaders_id, name, tenant_code) VALUES ($1, $2, '1M Voice', 'mitra') "
                "ON CONFLICT (id) DO NOTHING", PROGRAM_ID, LEADER_ID)
            return await body(conn, created)
        finally:
            for sid in created:
                await conn.execute("DELETE FROM submissions WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
            await conn.close()
    return asyncio.run(main())


def _msg(sequence, message, parent=None, key=None, follow_up=False, translated=None):
    return {
        "sequence": sequence, "questionKey": key, "isFollowUp": follow_up, "message": message,
        "translatedMessage": translated, "messageAt": f"2026-10-15T08:{sequence:02d}:00.000Z",
        "parentSequence": parent,
    }


# Q1, A (parent 1), follow-up Q3, two-message answer (4, 5 -> parent 3), closing 6.
SIX = [
    _msg(1, "How old are you?", key="AGE_IDENTIFICATION", translated="Q"),
    _msg(2, "17", parent=1),
    _msg(3, "What would you change?", key="Q_CHANGE", follow_up=True),
    _msg(4, "More buses", parent=3),
    _msg(5, "and a library", parent=3),
    _msg(6, "Thank you!", key="THANK_YOU"),
]


def _event(submission_id, session_id, messages, completion="COMPLETED", data=None, **tags):
    base_tags = {
        "programId": PROGRAM_ID, "programName": FEEDBACK_PROGRAM_NAME,
        "leaderCategoryId": LEADER_ID, "leaderCategoryName": "Youth",
        "state": "Karnataka", "district": None, "source": "mitra",
    }
    base_tags.update(tags)
    return {
        "submissionId": submission_id, "submissionType": "feedback", "sessionId": session_id,
        "tenantCode": "mitra", "eventType": "create", "eventPublishedAt": "2026-10-15T09:00:00.000Z",
        "tags": base_tags,
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": "Youth_Voice",
                 "questionnaireVersion": "V2", "botId": "101", "botName": "Mitra Youth Test Bot",
                 "language": "en", "completionStatus": completion,
                 "submissionDate": "2026-10-15T08:01:00.000Z", "messages": messages, **(data or {})},
    }


def _ids():
    sid = f"t{uuid.uuid4().hex[:12]}"
    return sid, f"sess-{uuid.uuid4().hex}"


async def _snapshot(conn, sid):
    sub = await conn.fetchrow(
        "SELECT id, status, state, district, submission_type, program_id, leader_id, session_id, updated_at "
        "FROM submissions WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
    fs = await conn.fetchrow(
        "SELECT id, source, session_type, questionnaire_version, meta_data::text AS meta_data, language, "
        "completion_status, source_ref, updated_at FROM feedback_submissions "
        "WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
    fbs = await conn.fetch(
        "SELECT id, sequence, parent_id, message, updated_at, created_at FROM feedbacks "
        "WHERE submission_id = $1 AND tenant_code = 'mitra' ORDER BY sequence", sid)
    return dict(sub) if sub else None, dict(fs) if fs else None, [dict(r) for r in fbs]


def test_create_session_resolves_parents():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX))
        assert res["changed"] is True and res["status"] == "pending"
        assert res["messages_inserted"] == 6 and res["parents_set"] == 3
        sub, fs, fbs = await _snapshot(conn, sid)
        assert sub["submission_type"] == "feedback" and sub["state"] == "Karnataka" and sub["district"] is None
        assert str(sub["program_id"]) == PROGRAM_ID and str(sub["leader_id"]) == LEADER_ID
        assert sub["session_id"] == sess and sub["status"] == "pending"
        assert fs["source"] == "mitra" and fs["completion_status"] == "COMPLETED" and fs["source_ref"] is None
        assert fs["session_type"] == "Youth_Voice" and fs["questionnaire_version"] == "V2"
        assert json.loads(fs["meta_data"]) == {"bot_id": "101", "bot_name": "Mitra Youth Test Bot"}
        by_seq = {r["sequence"]: r for r in fbs}
        assert sorted(by_seq) == [1, 2, 3, 4, 5, 6]
        assert by_seq[1]["parent_id"] is None and by_seq[3]["parent_id"] is None and by_seq[6]["parent_id"] is None
        assert by_seq[2]["parent_id"] == by_seq[1]["id"]
        assert by_seq[4]["parent_id"] == by_seq[3]["id"] and by_seq[5]["parent_id"] == by_seq[3]["id"]
        row = await conn.fetchrow(
            "SELECT is_follow_up, question_key, translated_message, language, message_at, program_id, session_id "
            "FROM feedbacks WHERE submission_id = $1 AND sequence = 3", sid)
        assert row["is_follow_up"] is True and row["question_key"] == "Q_CHANGE"
        assert row["language"] == "en" and row["message_at"] is not None
        assert str(row["program_id"]) == PROGRAM_ID and row["session_id"] == sess
    _run(body)


def test_identical_event_changes_nothing():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _snapshot(conn, sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX))
        after = await _snapshot(conn, sid)
        assert res["changed"] is False and res["status"] == "success"
        assert res["messages_inserted"] == 0 and res["messages_updated"] == 0 and res["parents_set"] == 0
        assert after == before  # ids, parent ids and every updated_at unchanged
    _run(body)


def test_grown_event_adds_only_new_rows_and_completes():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        first = SIX[:5]  # no closing row yet
        await insert_or_update_submission(conn, _event(sid, sess, first, completion="IN_PROGRESS"))
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _snapshot(conn, sid)
        assert before[1]["completion_status"] == "IN_PROGRESS" and len(before[2]) == 5

        # re-upload of the whole grown session: old rows 1..5 re-sent unchanged plus 6 and 7 (answer to 6)
        grown = SIX + [_msg(7, "Bye", parent=6)]
        res = await insert_or_update_submission(conn, _event(sid, sess, grown, completion="COMPLETED"))
        after = await _snapshot(conn, sid)
        assert res["changed"] is True and res["messages_inserted"] == 2 and res["messages_updated"] == 0
        assert res["status"] == "pending" and after[0]["status"] == "pending"  # sent back to processing
        assert after[1]["completion_status"] == "COMPLETED" and after[1]["id"] == before[1]["id"]
        assert after[1]["updated_at"] >= before[1]["updated_at"]
        assert [r["sequence"] for r in after[2]] == [1, 2, 3, 4, 5, 6, 7]
        for old, new in zip(before[2], after[2][:5]):
            assert new == old  # same id, parent, text and updated_at
        assert after[2][6]["parent_id"] == after[2][5]["id"]

        # delta-only event: only the new tail, parent resolved from the stored row
        tail = [_msg(8, "Really bye", parent=6)]
        res = await insert_or_update_submission(conn, _event(sid, sess, tail, completion="COMPLETED"))
        assert res["messages_inserted"] == 1 and res["parents_set"] == 1
        final = await _snapshot(conn, sid)
        assert final[2][7]["parent_id"] == after[2][5]["id"]
        assert final[2][:7] == after[2]
    _run(body)


def test_changed_text_updates_in_place_keeping_id():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        before = await _snapshot(conn, sid)
        edited = [dict(m) for m in SIX]
        edited[3]["message"] = "Many more buses"
        res = await insert_or_update_submission(conn, _event(sid, sess, edited))
        after = await _snapshot(conn, sid)
        assert res["messages_updated"] == 1 and res["messages_inserted"] == 0
        assert after[2][3]["id"] == before[2][3]["id"] and after[2][3]["message"] == "Many more buses"
        assert after[2][3]["updated_at"] >= before[2][3]["updated_at"]
        for i in (0, 1, 2, 4, 5):
            assert after[2][i] == before[2][i]
    _run(body)


def test_stale_in_progress_does_not_downgrade_completed():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX, completion="COMPLETED"))
        before = await _snapshot(conn, sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX[:4], completion="IN_PROGRESS"))
        after = await _snapshot(conn, sid)
        assert after[1]["completion_status"] == "COMPLETED"
        assert res["changed"] is False
        assert after == before
    _run(body)


def test_delete_cascades():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        assert await delete_submission(conn, sid, "mitra") is True
        for table in ("submissions", "feedback_submissions", "feedbacks"):
            n = await conn.fetchval(f"SELECT count(*) FROM {table} WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
            assert n == 0, table
    _run(body)


def test_update_event_is_rejected_and_writes_nothing():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        event = _event(sid, sess, SIX)
        event["eventType"] = "update"
        with pytest.raises(FeedbackUpdateNotSupportedError) as exc:
            await insert_or_update_submission(conn, event)
        assert sid in str(exc.value) and "mitra" in str(exc.value)
        assert await _snapshot(conn, sid) == (None, None, [])
    _run(body)


def test_unresolvable_parent_rolls_back_whole_event():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        bad = [_msg(1, "Q", key="K"), _msg(3, "A", parent=2)]  # sequence 2 never exists
        with pytest.raises(ValueError, match="parentSequence"):
            await insert_or_update_submission(conn, _event(sid, sess, bad))
        assert await _snapshot(conn, sid) == (None, None, [])
    _run(body)


def test_session_id_reuse_by_another_submission_is_rejected():
    async def body(conn, created):
        sid, sess = _ids()
        sid2, _ = _ids()
        created.extend([sid, sid2])
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        with pytest.raises(ValueError, match="session"):
            await insert_or_update_submission(conn, _event(sid2, sess, SIX))
        # same submission id arriving with a different session token is also rejected
        with pytest.raises(ValueError, match="already belongs to session"):
            await insert_or_update_submission(conn, _event(sid, f"sess-{uuid.uuid4().hex}", SIX))
        assert (await _snapshot(conn, sid2)) == (None, None, [])
    _run(body)


# ---------------------------------------------------------------- Google Forms (Phase 2, slice 2c)

GF_MESSAGES = [
    _msg(1, "How old are you?", key="AGE_IDENTIFICATION"),
    _msg(2, "17", parent=1, key="AGE_IDENTIFICATION"),
    _msg(3, "What would you change?", key="SCHOOL_EXPERIENCE"),
    _msg(4, "More buses", parent=3, key="SCHOOL_EXPERIENCE"),
    _msg(5, "What next?", key="FUTURE_GOAL"),
    _msg(6, "Teacher", parent=5, key="FUTURE_GOAL"),
]


def _gf_ref():
    return f"2026-10-02T10:09:20:{uuid.uuid4().hex}{uuid.uuid4().hex}"


def _gf_event(source_ref, submission_id=None, messages=None):
    sid = submission_id or str(uuid.uuid4())
    event = _event(sid, sid, messages or GF_MESSAGES, source="google_forms")
    event["data"].update(source="google_forms", adapter="wide_form", sourceRef=source_ref, questionnaireVersion="V1")
    del event["data"]["botId"], event["data"]["botName"]  # Google Forms has no Mitra bot
    return event


async def _gf_count(conn, source_ref):
    return await conn.fetchval(
        "SELECT count(*) FROM feedback_submissions WHERE tenant_code = 'mitra' AND source_ref = $1", source_ref)


def test_gforms_new_response_stored_with_source_ref():
    async def body(conn, created):
        ref = _gf_ref()
        event = _gf_event(ref)
        sid = event["submissionId"]
        created.append(sid)
        res = await insert_or_update_submission(conn, event)
        assert res["changed"] is True and res["status"] == "pending" and res["submission_id"] == sid
        assert res["messages_inserted"] == 6 and res["parents_set"] == 3
        sub, fs, fbs = await _snapshot(conn, sid)
        assert sub["session_id"] == sid and sub["submission_type"] == "feedback"
        assert fs["source"] == "google_forms" and fs["source_ref"] == ref
        assert fs["completion_status"] == "COMPLETED" and fs["language"] == "en"
        assert fs["session_type"] == "Youth_Voice" and fs["questionnaire_version"] == "V1"
        assert fs["meta_data"] is None  # no Mitra bot on a Google Forms response
        by_seq = {r["sequence"]: r for r in fbs}
        assert sorted(by_seq) == [1, 2, 3, 4, 5, 6]
        assert [by_seq[s]["parent_id"] for s in (1, 3, 5)] == [None, None, None]
        assert by_seq[2]["parent_id"] == by_seq[1]["id"]
        assert by_seq[4]["parent_id"] == by_seq[3]["id"] and by_seq[6]["parent_id"] == by_seq[5]["id"]
    _run(body)


def test_gforms_same_event_again_is_unchanged():
    async def body(conn, created):
        ref = _gf_ref()
        event = _gf_event(ref)
        sid = event["submissionId"]
        created.append(sid)
        await insert_or_update_submission(conn, event)
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _snapshot(conn, sid)
        res = await insert_or_update_submission(conn, event)
        assert res["changed"] is False and res["status"] == "success" and res["submission_id"] == sid
        assert res["messages_inserted"] == 0 and res["parents_set"] == 0
        assert await _snapshot(conn, sid) == before
    _run(body)


def test_gforms_different_uuid_same_source_ref_writes_nothing():
    async def body(conn, created):
        ref = _gf_ref()
        first = _gf_event(ref)
        created.append(first["submissionId"])
        await insert_or_update_submission(conn, first)
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", first["submissionId"])
        before = await _snapshot(conn, first["submissionId"])

        second = _gf_event(ref)  # fresh uuid, same natural key (a re-upload), even with a different body
        second["data"]["messages"] = GF_MESSAGES[:2]
        second_id = second["submissionId"]
        created.append(second_id)
        res = await insert_or_update_submission(conn, second)
        assert res["changed"] is False and res["submission_id"] == first["submissionId"]
        assert res["status"] == "success" and res["messages_inserted"] == 0
        assert await _snapshot(conn, second_id) == (None, None, [])
        assert await _snapshot(conn, first["submissionId"]) == before
        assert await _gf_count(conn, ref) == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM submissions WHERE submission_id = ANY($1::text[]) AND tenant_code = 'mitra'",
            [first["submissionId"], second_id]) == 1
    _run(body)


def test_gforms_two_different_source_refs_make_two_submissions():
    async def body(conn, created):
        a, b = _gf_event(_gf_ref()), _gf_event(_gf_ref())
        created.extend([a["submissionId"], b["submissionId"]])
        ra = await insert_or_update_submission(conn, a)
        rb = await insert_or_update_submission(conn, b)
        assert ra["changed"] is True and rb["changed"] is True and ra["id"] != rb["id"]
        assert await conn.fetchval(
            "SELECT count(*) FROM submissions WHERE submission_id = ANY($1::text[]) AND tenant_code = 'mitra'",
            [a["submissionId"], b["submissionId"]]) == 2
        assert await conn.fetchval(
            "SELECT count(*) FROM feedbacks WHERE submission_id = ANY($1::text[]) AND tenant_code = 'mitra'",
            [a["submissionId"], b["submissionId"]]) == 12
    _run(body)


def test_gforms_response_already_stored_by_another_connection_is_a_clean_noop():
    async def body(conn, created):
        ref = _gf_ref()
        stored_id = str(uuid.uuid4())
        created.append(stored_id)
        other = await asyncpg.connect(DSN)
        try:
            await other.execute(
                "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type, status) "
                "VALUES ($1, $1, 'mitra', 'feedback', 'success')", stored_id)
            await other.execute(
                "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, "
                "questionnaire_version, completion_status, source_ref) "
                "VALUES ($1, 'mitra', 'google_forms', 'Youth_Voice', 'V1', 'COMPLETED', $2)", stored_id, ref)
        finally:
            await other.close()
        event = _gf_event(ref)
        created.append(event["submissionId"])
        res = await insert_or_update_submission(conn, event)
        assert res["changed"] is False and res["submission_id"] == stored_id and res["status"] == "success"
        assert await _snapshot(conn, event["submissionId"]) == (None, None, [])
        assert await _gf_count(conn, ref) == 1
    _run(body)


def test_gforms_race_after_precheck_hits_unique_violation_and_is_a_clean_noop(monkeypatch):
    """The pre-check misses (as if the other writer committed just after it); the real unique constraint fires."""
    async def body(conn, created):
        ref = _gf_ref()
        stored_id = str(uuid.uuid4())
        created.append(stored_id)
        await conn.execute(
            "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type, status) "
            "VALUES ($1, $1, 'mitra', 'feedback', 'success')", stored_id)
        await conn.execute(
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, "
            "questionnaire_version, completion_status, source_ref) "
            "VALUES ($1, 'mitra', 'google_forms', 'Youth_Voice', 'V1', 'COMPLETED', $2)", stored_id, ref)
        real_lookup = operations._find_stored_gforms_response
        calls = []

        async def lookup(c, tenant, source_ref):
            calls.append(source_ref)
            return None if len(calls) == 1 else await real_lookup(c, tenant, source_ref)

        monkeypatch.setattr(operations, "_find_stored_gforms_response", lookup)
        event = _gf_event(ref)
        created.append(event["submissionId"])
        res = await insert_or_update_submission(conn, event)
        assert len(calls) == 2  # the violation path re-read the stored response
        assert res["changed"] is False and res["submission_id"] == stored_id
        # the losing event rolled back completely: no submission, payload or message rows
        assert await _snapshot(conn, event["submissionId"]) == (None, None, [])
        assert await _gf_count(conn, ref) == 1
    _run(body)


def test_gforms_true_concurrent_writers_store_the_response_once():
    """Writer 1 holds an uncommitted insert; writer 2 passes its pre-check, blocks on the unique key, then no-ops."""
    async def main():
        ref = _gf_ref()
        writer1_id = str(uuid.uuid4())
        event2 = _gf_event(ref)
        conn1 = await asyncpg.connect(DSN)
        conn2 = await asyncpg.connect(DSN)
        try:
            await _ensure_schema(conn1)
            await conn1.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn1.execute(
                "INSERT INTO leader_category (id, name, tenant_code) VALUES ($1, 'Youth', 'mitra') ON CONFLICT (id) DO NOTHING",
                LEADER_ID)
            await conn1.execute(
                "INSERT INTO programs (id, leaders_id, name, tenant_code) VALUES ($1, $2, '1M Voice', 'mitra') "
                "ON CONFLICT (id) DO NOTHING", PROGRAM_ID, LEADER_ID)
            tx = conn1.transaction()
            await tx.start()
            await conn1.execute(
                "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type, status) "
                "VALUES ($1, $1, 'mitra', 'feedback', 'success')", writer1_id)
            await conn1.execute(
                "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, "
                "questionnaire_version, completion_status, source_ref) "
                "VALUES ($1, 'mitra', 'google_forms', 'Youth_Voice', 'V1', 'COMPLETED', $2)", writer1_id, ref)
            task = asyncio.ensure_future(insert_or_update_submission(conn2, event2))
            await asyncio.sleep(1.0)
            assert not task.done()  # blocked on the uncommitted unique key
            await tx.commit()
            res = await asyncio.wait_for(task, timeout=10)
            assert res["changed"] is False and res["submission_id"] == writer1_id
            assert await _gf_count(conn1, ref) == 1
            assert await conn1.fetchval(
                "SELECT count(*) FROM submissions WHERE submission_id = $1 AND tenant_code = 'mitra'",
                event2["submissionId"]) == 0
        finally:
            for sid in (writer1_id, event2["submissionId"]):
                await conn1.execute("DELETE FROM submissions WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
            await conn1.close()
            await conn2.close()
    asyncio.run(main())


def test_gforms_new_source_ref_with_an_existing_submission_id_is_rejected_unchanged():
    async def body(conn, created):
        first = _gf_event(_gf_ref())
        created.append(first["submissionId"])
        await insert_or_update_submission(conn, first)
        before = await _snapshot(conn, first["submissionId"])
        clash = _gf_event(_gf_ref(), submission_id=first["submissionId"], messages=GF_MESSAGES[:2])
        with pytest.raises(ValueError, match="never updates an existing submission"):
            await insert_or_update_submission(conn, clash)
        assert await _snapshot(conn, first["submissionId"]) == before
    _run(body)


def test_gforms_event_never_modifies_a_stored_mitra_submission():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        before = await _snapshot(conn, sid)
        clash = _gf_event(_gf_ref(), submission_id=sid)
        clash["sessionId"] = sid
        with pytest.raises(ValueError):
            await insert_or_update_submission(conn, clash)
        assert await _snapshot(conn, sid) == before
    _run(body)


def test_gforms_identity_is_checked_at_the_boundary():
    async def body(conn, created):
        event = _gf_event(_gf_ref())
        created.append(event["submissionId"])
        no_ref = _gf_event(_gf_ref())
        del no_ref["data"]["sourceRef"]
        with pytest.raises(ValueError, match="sourceRef"):
            await insert_or_update_submission(conn, no_ref)
        mitra_with_ref = _event(*_ids(), SIX)
        mitra_with_ref["data"]["sourceRef"] = _gf_ref()
        with pytest.raises(ValueError, match="sourceRef"):
            await insert_or_update_submission(conn, mitra_with_ref)
        assert await _snapshot(conn, no_ref["submissionId"]) == (None, None, [])
        assert await _snapshot(conn, mitra_with_ref["submissionId"]) == (None, None, [])
    _run(body)


def test_mitra_events_still_store_null_source_ref():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        sid2, sess2 = _ids()
        created.append(sid2)
        await insert_or_update_submission(conn, _event(sid2, sess2, SIX))  # two mitra rows: NULLs never collide
        for s in (sid, sid2):
            assert (await _snapshot(conn, s))[1]["source_ref"] is None
    _run(body)


# ------------------------------------------------ session type / questionnaire version / bot meta

async def _fs(conn, sid):
    row = await conn.fetchrow(
        "SELECT session_type, questionnaire_version, meta_data::text AS meta, completion_status, updated_at "
        "FROM feedback_submissions WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)
    return dict(row) if row else None


def test_create_writes_session_type_version_and_bot_meta():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(
            conn, _event(sid, sess, SIX, data={"sessionType": "Parent_Voice", "questionnaireVersion": "V1"}))
        fs = await _fs(conn, sid)
        assert fs["session_type"] == "Parent_Voice" and fs["questionnaire_version"] == "V1"
        assert json.loads(fs["meta"]) == {"bot_id": "101", "bot_name": "Mitra Youth Test Bot"}
    _run(body)


def test_create_with_blank_bot_fields_omits_them_from_meta():
    async def body(conn, created):
        sid, sess = _ids()
        sid2, sess2 = _ids()
        sid3, sess3 = _ids()
        created.extend([sid, sid2, sid3])
        await insert_or_update_submission(conn, _event(sid, sess, SIX, data={"botId": "205", "botName": "  "}))
        await insert_or_update_submission(conn, _event(sid2, sess2, SIX, data={"botId": None, "botName": ""}))
        event3 = _event(sid3, sess3, SIX)
        del event3["data"]["botId"], event3["data"]["botName"]
        await insert_or_update_submission(conn, event3)
        assert json.loads((await _fs(conn, sid))["meta"]) == {"bot_id": "205"}
        assert (await _fs(conn, sid2))["meta"] is None
        assert (await _fs(conn, sid3))["meta"] is None
    _run(body)


def test_redelivery_with_changed_version_refreshes_it_and_resends_to_processing():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX, data={"questionnaireVersion": "V1"}))
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _fs(conn, sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX, data={"questionnaireVersion": "V2"}))
        after = await _fs(conn, sid)
        assert res["changed"] is True and res["messages_inserted"] == 0
        assert before["questionnaire_version"] == "V1" and after["questionnaire_version"] == "V2"
        assert after["session_type"] == before["session_type"] and after["meta"] == before["meta"]
        assert after["updated_at"] >= before["updated_at"]
    _run(body)


def test_redelivery_with_changed_session_type_refreshes_it():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        res = await insert_or_update_submission(
            conn, _event(sid, sess, SIX, data={"sessionType": "General_Public_Voice"}))
        assert res["changed"] is True
        assert (await _fs(conn, sid))["session_type"] == "General_Public_Voice"
    _run(body)


def test_redelivery_with_new_bot_merges_meta_data():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX, data={"botId": "101", "botName": None}))
        assert json.loads((await _fs(conn, sid))["meta"]) == {"bot_id": "101"}
        # a second delivery adds the bot name and replaces the id; keys it does not carry are kept
        res = await insert_or_update_submission(
            conn, _event(sid, sess, SIX, data={"botId": "110", "botName": "Educator Test Bot"}))
        assert res["changed"] is True
        assert json.loads((await _fs(conn, sid))["meta"]) == {"bot_id": "110", "bot_name": "Educator Test Bot"}
        # a later delivery without bot info leaves the stored meta_data alone and changes nothing
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _snapshot(conn, sid)
        later = _event(sid, sess, SIX, data={"botId": None, "botName": ""})
        res = await insert_or_update_submission(conn, later)
        assert res["changed"] is False
        assert await _snapshot(conn, sid) == before
        assert json.loads((await _fs(conn, sid))["meta"]) == {"bot_id": "110", "bot_name": "Educator Test Bot"}
    _run(body)


def test_identical_redelivery_leaves_session_type_version_and_meta_untouched():
    async def body(conn, created):
        sid, sess = _ids()
        created.append(sid)
        await insert_or_update_submission(conn, _event(sid, sess, SIX))
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)
        before = await _snapshot(conn, sid)
        fs_before = await _fs(conn, sid)
        res = await insert_or_update_submission(conn, _event(sid, sess, SIX))
        assert res["changed"] is False and res["status"] == "success"
        assert await _snapshot(conn, sid) == before
        assert await _fs(conn, sid) == fs_before  # including updated_at
    _run(body)


def test_gforms_event_stores_session_type_and_form_version():
    async def body(conn, created):
        event = _gf_event(_gf_ref())
        event["data"].update(sessionType="Parent_Voice", questionnaireVersion="V1")
        created.append(event["submissionId"])
        await insert_or_update_submission(conn, event)
        fs = await _fs(conn, event["submissionId"])
        assert fs["session_type"] == "Parent_Voice" and fs["questionnaire_version"] == "V1"
        assert fs["meta"] is None
    _run(body)


def test_gforms_redelivery_never_changes_stored_session_type_or_version():
    async def body(conn, created):
        ref = _gf_ref()
        event = _gf_event(ref)
        created.append(event["submissionId"])
        await insert_or_update_submission(conn, event)
        again = _gf_event(ref)
        again["data"].update(sessionType="Parent_Voice", questionnaireVersion="V9")
        res = await insert_or_update_submission(conn, again)
        assert res["changed"] is False
        fs = await _fs(conn, event["submissionId"])
        assert fs["session_type"] == "Youth_Voice" and fs["questionnaire_version"] == "V1"
    _run(body)


def test_event_without_session_type_or_version_is_rejected_and_writes_nothing():
    async def body(conn, created):
        for field in ("sessionType", "questionnaireVersion"):
            for bad in (None, "", "  "):
                sid, sess = _ids()
                created.append(sid)
                with pytest.raises(ValueError, match=f"data.{field} must be a non-empty string"):
                    await insert_or_update_submission(conn, _event(sid, sess, SIX, data={field: bad}))
                assert await _snapshot(conn, sid) == (None, None, [])
        sid, sess = _ids()
        created.append(sid)
        event = _event(sid, sess, SIX)
        del event["data"]["sessionType"]
        with pytest.raises(ValueError, match="data.sessionType"):
            await insert_or_update_submission(conn, event)
        assert await _snapshot(conn, sid) == (None, None, [])
    _run(body)


def test_null_session_type_or_version_cannot_be_inserted_directly():
    async def body(conn, created):
        for column_list, values in (
            ("session_type, questionnaire_version", "NULL, 'V1'"),
            ("session_type, questionnaire_version", "'Youth_Voice', NULL"),
            ("questionnaire_version", "'V1'"),   # session_type omitted
            ("session_type", "'Youth_Voice'"),   # questionnaire_version omitted
        ):
            sid, sess = _ids()
            created.append(sid)
            await conn.execute(
                "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type) "
                "VALUES ($1, $2, 'mitra', 'feedback')", sess, sid)
            with pytest.raises(asyncpg.exceptions.NotNullViolationError):
                await conn.execute(
                    f"INSERT INTO feedback_submissions (submission_id, tenant_code, source, completion_status, "
                    f"{column_list}) VALUES ($1, 'mitra', 'mitra', 'COMPLETED', {values})", sid)
        # a complete row is accepted
        sid, sess = _ids()
        created.append(sid)
        await conn.execute(
            "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type) "
            "VALUES ($1, $2, 'mitra', 'feedback')", sess, sid)
        await conn.execute(
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, "
            "questionnaire_version, completion_status) VALUES ($1, 'mitra', 'mitra', 'Youth_Voice', 'V1', 'COMPLETED')",
            sid)
        assert (await _fs(conn, sid))["session_type"] == "Youth_Voice"
    _run(body)
