"""
Real-Postgres test of detect_questionnaire_versions (app/database/feedback_operations.py): sessions are stored through
the real ingest path, then the questions they were asked are compared. Opt-in like tests/test_feedback_versions_pg.py:
FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards). Synthetic data only.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app.database.feedback_operations import detect_questionnaire_versions, set_questionnaire_version
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS
import factories
import pg_support
from test_feedback_versions_pg import _run

DSN = pg_support.SERVER_URL   # the fixture of tests/conftest.py swaps in this module's own scratch database

pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH = PERSONAS["Youth"]
T0 = datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc)
V1_QS = [("AGE", "How old are you?"), ("STATE", "Which state do you live in?"), ("LIKE", "What do you like about school?")]
V2_QS = [("AGE", "How old are you?"), ("FEEL", "How do you feel about school these days?"), ("JOB", "Which job do you want?")]


def _messages(questions, probe=True):
    messages, seq = [], 1
    for key, text in questions:
        messages.append({"sequence": seq, "questionKey": key, "isFollowUp": False, "message": text,
                         "translatedMessage": None, "messageAt": None, "parentSequence": None})
        question_seq, seq = seq, seq + 1
        messages.append({"sequence": seq, "questionKey": key, "isFollowUp": False, "message": "an answer",
                         "translatedMessage": None, "messageAt": None, "parentSequence": question_seq})
        seq += 1
        if probe:
            messages.append({"sequence": seq, "questionKey": key, "isFollowUp": True, "message": f"probe {uuid.uuid4().hex}",
                             "translatedMessage": None, "messageAt": None, "parentSequence": None})
            seq += 1
    messages.append({"sequence": seq, "questionKey": "THANK_YOU", "isFollowUp": False, "message": "Thank you!",
                     "translatedMessage": None, "messageAt": None, "parentSequence": None})
    return messages


async def _add(conn, questions, minutes, stamped="V1"):
    sid = f"d{uuid.uuid4().hex[:14]}"
    started = T0 + timedelta(minutes=minutes)
    event = ({
        "submissionId": sid, "sessionId": f"sess-{uuid.uuid4().hex}", "submissionType": "feedback",
        "tenantCode": "mitra", "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": YOUTH["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": YOUTH["leader_category_id"], "leaderCategoryName": "Youth",
                 "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": YOUTH["session_type"], "questionnaireVersion": stamped,
                 "language": "en", "completionStatus": "COMPLETED", "submissionDate": started.isoformat(),
                 "messages": _messages(questions), "botId": YOUTH["bot_id"]},
    })
    await insert_or_update_submission(conn, event)
    # the sessions are stamped from windows here (detect reads the labels and the bot ids as evidence): a legacy
    # session without a bot id, since a bot session's label is the rank of its bot id
    await factories.as_window_stamped_session(conn, sid, stamped)
    return started


def test_detect_proposes_the_new_version_and_the_proposed_set_command_records_it():
    async def body(conn):
        for i in range(60):
            await _add(conn, V1_QS, i)
        change = None
        for i in range(60, 100):
            started = await _add(conn, V2_QS, i)
            change = change or started

        found = await detect_questionnaire_versions("Youth_Voice", min_sessions=10, baseline_sessions=40)
        youth = found["Youth_Voice"]
        assert set(found) == {"Youth_Voice"}
        proposal = youth["proposal"]
        assert proposal["key"] == "V2" and proposal["effective_from"] == change.isoformat()
        # the sessions are legacy ones without a bot id here (see _add): no bot-id evidence on either side
        assert proposal["bot_ids_before"] == []
        assert proposal["bot_id_changed_at_start"] is False
        texts = {q["text"] for q in youth["new_questions"]}
        assert texts == {"How do you feel about school these days?", "Which job do you want?"}
        assert "Which state do you live in?" in youth["dropped_questions"] or "What do you like about school?" in youth["dropped_questions"]
        # probes and the closing message are not questions of the version
        assert all("probe" not in q["text"] and q["text"] != "Thank you!" for q in youth["new_questions"])
        assert f"--key V2 --effective-from {change.isoformat()}" in youth["next_step"] and "--dry-run" in youth["next_step"]

        # read-only: nothing was written
        labels = await conn.fetch("SELECT DISTINCT questionnaire_version FROM feedback_submissions")
        assert [r["questionnaire_version"] for r in labels] == ["V1"]
        assert await conn.fetchval(
            "SELECT count(*) FROM config_version v JOIN config c ON c.id = v.config_id WHERE c.session_type = 'Youth_Voice' AND c.config_source = 'mitra'") == 1

        # the person records it with the proposed values; detect then has nothing left to propose
        done = await set_questionnaire_version("Youth_Voice", proposal["key"], proposal["effective_from"])
        assert done["restamp"]["versions"] == {"V1": 60, "V2": 40}
        again = (await detect_questionnaire_versions("Youth_Voice", min_sessions=10, baseline_sessions=40))["Youth_Voice"]
        assert again["proposal"] is None and again["next_step"] is None
        assert again["versions"]["V1"]["sessions"] == 60 and again["versions"]["V2"]["sessions"] == 40
    _run(body)


def test_detect_with_no_sessions_or_one_version_of_questions_proposes_nothing():
    async def body(conn):
        empty = (await detect_questionnaire_versions("Youth_Voice"))["Youth_Voice"]
        assert empty["sessions"] == 0 and empty["proposal"] is None
        for i in range(30):
            await _add(conn, V1_QS, i)
        same = (await detect_questionnaire_versions("Youth_Voice", min_sessions=5))["Youth_Voice"]
        assert same["sessions"] == 30 and same["proposal"] is None and same["new_questions"] == []
        everyone = await detect_questionnaire_versions()
        assert set(everyone) == {"Educator_Voice", "General_Public_Voice", "Parent_Voice", "Youth_Voice"}
        assert await detect_questionnaire_versions("Nobody_Voice") == {}
    _run(body)


def test_questions_of_another_tenant_that_points_at_a_mitra_program_never_make_a_proposal():
    """
    Sixty mitra sessions ask one set of questions; forty sessions of tenant 'other' carry the mitra program id and a
    DIFFERENT set (written with plain SQL, the ingest path refuses them). Counting them would propose a new version.
    """
    async def body(conn):
        for i in range(60):
            await _add(conn, V1_QS, i)
        for i in range(40):
            await pg_support.insert_foreign_session(
                conn, tenant_code="other", submission_id=f"foreign-{i}", program_id=YOUTH["program_id"],
                started_at=T0 + timedelta(minutes=60 + i), version="V1", session_type="Youth_Voice",
                questions=[text for _, text in V2_QS])
        try:
            youth = (await detect_questionnaire_versions("Youth_Voice", min_sessions=10, baseline_sessions=40))["Youth_Voice"]
            assert youth["sessions"] == 60
            assert youth["proposal"] is None and youth["new_questions"] == []
        finally:
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'other'")
    _run(body)
