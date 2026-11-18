"""
Real-Postgres test: a stored session whose PII check is TRUE/FALSE goes back to NULL (not checked) when the session
grows or a message changes, and keeps its flag when the same content arrives again. Opt-in like
tests/test_feedback_versions_pg.py (FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards)).
Synthetic data only.
"""
import json
import uuid
from datetime import datetime, timezone

import pytest

from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS
import pg_support
from test_feedback_versions_pg import _run

DSN = pg_support.SERVER_URL   # the fixture of tests/conftest.py swaps in this module's own scratch database

pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH = PERSONAS["Youth"]
STARTED = datetime(2026, 11, 1, 6, 0, tzinfo=timezone.utc)


def _msg(sequence, text, parent=None):
    return {"sequence": sequence, "questionKey": "AGE", "isFollowUp": False, "message": text,
            "translatedMessage": None, "messageAt": None, "parentSequence": parent}


def _event(sid, sess, messages, bot_id=YOUTH["bot_id"]):
    event = {
        "submissionId": sid, "sessionId": sess, "submissionType": "feedback", "tenantCode": "mitra",
        "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": YOUTH["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": YOUTH["leader_category_id"], "leaderCategoryName": "Youth",
                 "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": YOUTH["session_type"], "questionnaireVersion": "V1",
                 "language": "en", "completionStatus": "IN_PROGRESS", "submissionDate": STARTED.isoformat(),
                 "messages": messages},
    }
    if bot_id is not None:
        event["data"]["botId"] = bot_id
    return event


async def _flag(conn, sid):
    return await conn.fetchval("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", sid)


def test_pii_flag_is_reset_only_when_the_conversation_changes():
    async def body(conn):
        sid, sess = f"p{uuid.uuid4().hex[:14]}", f"sess-{uuid.uuid4().hex}"
        first = [_msg(1, "How old are you?"), _msg(2, "17", 1)]
        await insert_or_update_submission(conn, _event(sid, sess, first))
        assert await _flag(conn, sid) is None                                   # never checked

        await conn.execute("UPDATE feedback_submissions SET pii_detected = TRUE WHERE submission_id = $1", sid)
        await insert_or_update_submission(conn, _event(sid, sess, first))        # the same content again
        assert await _flag(conn, sid) is True                                   # untouched

        grown = first + [_msg(3, "Which state do you live in?"), _msg(4, "Kerala", 3)]
        await insert_or_update_submission(conn, _event(sid, sess, grown))        # the session grew
        assert await _flag(conn, sid) is None                                   # back to "not checked"

        await conn.execute("UPDATE feedback_submissions SET pii_detected = FALSE WHERE submission_id = $1", sid)
        edited = [first[0], _msg(2, "18", 1)] + grown[2:]
        await insert_or_update_submission(conn, _event(sid, sess, edited))       # an answer was corrected
        assert await _flag(conn, sid) is None
    _run(body)


def test_a_parent_link_only_change_resets_the_flag_too():
    """A bot row that becomes a respondent row (only its parent link changes, no message text) is new content."""
    async def body(conn):
        sid, sess = f"p{uuid.uuid4().hex[:14]}", f"sess-{uuid.uuid4().hex}"
        as_bot_rows = [_msg(1, "How old are you?"), _msg(2, "17", None)]           # row 2 is stored as a bot row
        await insert_or_update_submission(conn, _event(sid, sess, as_bot_rows))
        await conn.execute("UPDATE feedback_submissions SET pii_detected = FALSE WHERE submission_id = $1", sid)
        same = await insert_or_update_submission(conn, _event(sid, sess, as_bot_rows))
        assert same["parents_set"] == 0 and await _flag(conn, sid) is False        # nothing changed: flag kept

        linked = [_msg(1, "How old are you?"), _msg(2, "17", 1)]                   # same text, now an answer
        result = await insert_or_update_submission(conn, _event(sid, sess, linked))
        assert result["messages_inserted"] == 0 and result["messages_updated"] == 0 and result["parents_set"] == 1
        assert await _flag(conn, sid) is None                                      # reset by the parent-link change
        assert await conn.fetchval(
            "SELECT count(*) FROM feedbacks WHERE submission_id = $1 AND parent_id IS NOT NULL", sid) == 1
    _run(body)


def test_the_bot_id_is_required_and_stored():
    # CHANGED (was: stored only when the event has one): a conversation_rows event without a bot id is rejected now,
    # because its questionnaire version is the rank of its bot id
    async def body(conn):
        sid, sess = f"p{uuid.uuid4().hex[:14]}", f"sess-{uuid.uuid4().hex}"
        msgs = [_msg(1, "How old are you?"), _msg(2, "17", 1)]
        with pytest.raises(ValueError, match="data.botId is invalid"):
            await insert_or_update_submission(conn, _event(sid, sess, msgs, bot_id=None))
        assert await conn.fetchval("SELECT count(*) FROM submissions WHERE submission_id = $1", sid) == 0
        await insert_or_update_submission(conn, _event(sid, sess, msgs))
        meta = await conn.fetchval("SELECT meta_data FROM feedback_submissions WHERE submission_id = $1", sid)
        assert (json.loads(meta) if isinstance(meta, str) else meta)["bot_id"] == YOUTH["bot_id"]
    _run(body)
