"""
Real-Postgres tests of the process steps of a feedback submission (the unit side: tests/test_feedback_process_steps.py):
a pending feedback submission whose persona config is missing / invalid must not stop the batch sweep (the older
story row is still returned, the bad row is marked failed with the reason), the same for the retry sweep, and the
startup check against real config rows.

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch
database of its own for this file and drops it afterwards). Synthetic data only.
"""
import asyncio
import json
from datetime import datetime
import uuid

import asyncpg
import pytest
import pg_support

from app.database.db import db
from app.database.feedback_operations import (
    get_active_feedback_config,
    get_feedback_process_steps,
    get_process_steps,
    list_feedback_config,
    migrate_feedback_config,
    set_feedback_config_value,
    set_questionnaire_version,
)
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, load_seed_config, seed_config
from app.services.feedback_constants import (
    DEFAULT_INGESTION_SCHEMA,
    DEFAULT_MAX_CSV_UPLOAD_BYTES,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
)
from seed_support import PERSONAS, SEED_UPLOAD_CONFIG_COUNT

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH = PERSONAS["Youth"]
STEPS = load_seed_config("feedback_upload", "mitra", "Youth_Voice")["process_steps"]  # what the seed ships (the extraction step)
SEEDED_CONFIGS = SEED_UPLOAD_CONFIG_COUNT   # the upload configs of the seed (one per persona and source)


def _run(body):
    """Runs `body(conn)` on a scratch database: schema applied, tenant 'mitra', clean seed, db.pool pointing at it."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await seed_config(conn)
            pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
            db.pool = pool
            return await body(conn)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.close()
    return asyncio.run(main())


def _event(source="mitra", session_type=None):
    session_type = session_type or YOUTH["session_type"]
    sid = f"c{uuid.uuid4().hex[:14]}"
    ref = {"sourceRef": f"2026-10-02T10:09:20|{uuid.uuid4().hex}"} if source == "google_forms" else {}
    if source == "google_forms":
        sid = str(uuid.uuid4())
    return {
        "submissionId": sid, "sessionId": sid if source == "google_forms" else f"sess-{uuid.uuid4().hex}",
        "submissionType": "feedback", "tenantCode": "mitra", "eventType": "create",
        "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": YOUTH["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": YOUTH["leader_category_id"], "leaderCategoryName": "Youth",
                 "state": None, "district": None, "source": source},
        "data": {"source": source, "adapter": "wide_form" if source == "google_forms" else "conversation_rows",
                 "sessionType": session_type, "questionnaireVersion": "V1", "language": "en",
                 "completionStatus": "COMPLETED", "submissionDate": "2026-11-10T08:00:00.000Z", **ref,
                 "messages": [
                     {"sequence": 1, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "Age?",
                      "translatedMessage": None, "messageAt": None, "parentSequence": None},
                     {"sequence": 2, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "17",
                      "translatedMessage": None, "messageAt": None, "parentSequence": 1}]},
    }


async def _store(conn, source="mitra"):
    event = _event(source)
    await insert_or_update_submission(conn, event)
    return event["submissionId"]


async def _versions(conn, session_type, source="mitra"):
    rows = await conn.fetch(
        """SELECT v.version, v.is_active, v.config, v.created_by, v.change_note FROM config c
           JOIN config_version v ON v.config_id = c.id
           WHERE c.session_type = $1 AND c.config_source = $2 ORDER BY v.version""", session_type, source)
    return [{**dict(r), "config": json.loads(r["config"])} for r in rows]


async def _strip_keys(conn, *keys):
    """Make every active config look like an older database's: the keys that moved out of .env are absent."""
    for key in keys:
        await conn.execute("UPDATE config_version SET config = config - $1::text", key)



from app.database.feedback_config_health import check_feedback_configs
import app.temporal.activities as activities
import app.services.retry as retry_service
from unittest.mock import AsyncMock, MagicMock


async def _insert_story(conn, submission_id, created_at):
    await conn.execute(
        "INSERT INTO submissions (submission_id, tenant_code, submission_type, status, created_at) "
        "VALUES ($1, 'mitra', 'story', 'pending', $2)", submission_id, datetime.fromisoformat(created_at))


async def _remove_key_from_active_config(conn, session_type, source, key):
    await conn.execute(
        """UPDATE config_version SET config = config - $3::text
           WHERE is_active AND config_id IN (SELECT id FROM config WHERE session_type = $1 AND config_source = $2)""",
        session_type, source, key)


def test_batch_sweep_returns_the_older_story_row_when_a_feedback_persona_config_lacks_process_steps():
    async def body(conn):
        await _insert_story(conn, "story-old", "2026-01-01T00:00:00+00:00")
        bad = await _store(conn)                      # Youth Mitra, pending, stored through the real ingest path
        await _remove_key_from_active_config(conn, "Youth_Voice", "mitra", "process_steps")
        rows = await activities.fetch_pending_submissions_activity({"limit": 10})
        assert [r["submission_id"] for r in rows] == ["story-old"]
        row = await conn.fetchrow("SELECT status, process_status FROM submissions WHERE submission_id = $1", bad)
        process_status = json.loads(row["process_status"])
        assert row["status"] == "failed" and process_status["status"] == "failed"
        assert "process_steps" in process_status["error_message"] and "retry_attempt" not in process_status
        # it left the pending queue, so the next sweep does not see it again
        assert [r["submission_id"] for r in await activities.fetch_pending_submissions_activity({"limit": 10})] == ["story-old"]
    _run(body)


def test_batch_sweep_still_returns_feedback_rows_of_healthy_personas_and_isolates_the_bad_one():
    async def body(conn):
        good = await _store(conn)                                                   # Youth, healthy
        await conn.execute("UPDATE submissions SET created_at = created_at - interval '1 hour' WHERE submission_id = $1", good)
        parent_event = _event(session_type="Parent_Voice")
        parent_event["tags"].update(programId=PERSONAS["Parent"]["program_id"],
                                    leaderCategoryId=PERSONAS["Parent"]["leader_category_id"])
        await insert_or_update_submission(conn, parent_event)                      # Parent, broken below
        await _remove_key_from_active_config(conn, "Parent_Voice", "mitra", "process_steps")
        rows = await activities.fetch_pending_submissions_activity({"limit": 10})
        assert [(r["submission_id"], r["process_steps"]) for r in rows] == [(good, STEPS)]
        statuses = {r["submission_id"]: r["status"] for r in await conn.fetch("SELECT submission_id, status FROM submissions")}
        assert statuses[good] == "pending" and statuses[parent_event["submissionId"]] == "failed"
    _run(body)


def test_failed_sweep_skips_a_feedback_row_with_a_bad_config_and_keeps_the_story_row():
    async def body(conn):
        await _insert_story(conn, "story-failed", "2026-01-01T00:00:00+00:00")
        bad = await _store(conn)
        await conn.execute("UPDATE submissions SET status = 'failed', updated_at = now() - interval '1 day'")
        await _remove_key_from_active_config(conn, "Youth_Voice", "mitra", "process_steps")
        client = MagicMock()
        client.start_workflow = AsyncMock()
        original = retry_service.get_temporal_client
        retry_service.get_temporal_client = AsyncMock(return_value=client)
        try:
            result = await retry_service.dispatch_all_retry_workflows(None)
        finally:
            retry_service.get_temporal_client = original
        started = [c.args[1]["submission_id"] for c in client.start_workflow.await_args_list]
        assert started == ["story-failed"]
        assert bad not in started
        assert result["started"] == 1 and result["skipped"] == 1
        # the bad row stays failed (looked at again by the next sweep, once the config is fixed)
        assert await conn.fetchval("SELECT status FROM submissions WHERE submission_id = $1", bad) == "failed"
    _run(body)


def test_startup_check_reports_the_broken_config_rows_by_session_type_and_source_only():
    async def body(conn):
        assert await check_feedback_configs(conn) == []                      # a fresh seed is healthy
        await _strip_keys(conn, *REQUIRED_FEEDBACK_CONFIG_KEYS)               # an older database
        invalid = await check_feedback_configs(conn)
        assert len(invalid) == SEEDED_CONFIGS
        assert all(set(i["problems"]) == set(REQUIRED_FEEDBACK_CONFIG_KEYS) for i in invalid)
        assert all(set(i) == {"session_type", "source", "tenant_code", "problems"} for i in invalid)
        assert await migrate_feedback_config()                                # the documented fix
        assert await check_feedback_configs(conn) == []
    _run(body)
