"""
Real-Postgres tests of the CodeRabbit follow-up on PR #36 (see tests/test_feedback_coderabbit.py for the pure
ones). Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates an empty
scratch database for this file alone and drops it afterwards). SYNTHETIC data only, a fake LLM, no network.
"""
import asyncio
import json
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest
import pg_support

from app.database.db import SEED_PROMPTS_FILE, db
from app.database.feedback_extraction_operations import (
    StaleExtractionError,
    extract_submission,
    load_submission_inputs,
    persona_identity,
    save_extraction,
)
from app.database.operations import insert_or_update_submission
from app.services import feedback_extraction as fx
from seed_support import PERSONAS, extraction_config_id, extraction_config_v1, seed_config

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(DSN is None or DSN == "", reason=pg_support.SKIP_REASON)


def _four_field_raw(persona="Youth"):
    raw = extraction_config_v1(persona)
    by_name = {f["name"]: f for f in raw["fields"]}
    gender = by_name["gender"] if "gender" in by_name else by_name["child_gender"]
    raw["fields"] = [by_name["state"], by_name["district"], dict(gender, name="gender")]
    for school_name in ("institution_type", "school_type"):
        if school_name in by_name:
            raw["fields"].append(dict(by_name[school_name], name="school_type"))
    raw["prompt_name"] = "Feedback Metadata Extraction"
    return raw


def _run(body):
    """Runs `body(conn, pool)` on the scratch database: schema, tenant 'mitra', prompts, seeds, db.pool set."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await conn.execute("DELETE FROM prompt_version WHERE prompt_id IN "
                               "(SELECT id FROM prompts WHERE analysis_type = 'feedback_metadata_extraction')")
            await conn.execute(SEED_PROMPTS_FILE.read_text(encoding="utf-8"))
            await seed_config(conn)
            for persona_name, persona in PERSONAS.items():
                await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                                   extraction_config_id(persona["session_type"]),
                                   json.dumps(_four_field_raw(persona_name)))
            pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
            db.pool = pool
            return await body(conn, pool)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.close()
    return asyncio.run(main())


def _messages():
    def row(seq, key, text, parent):
        return {"sequence": seq, "questionKey": key, "isFollowUp": False, "message": text,
                "translatedMessage": None, "messageAt": None, "parentSequence": parent}
    return [row(1, "STATE_IDENTIFICATION", "Which state do you stay in?", None), row(2, "STATE_IDENTIFICATION", "karnatka", 1),
            row(3, "GENDER_IDENTIFICATION", "Are you a boy or a girl?", None), row(4, "GENDER_IDENTIFICATION", "girl", 3)]


def _event(persona, sid, session_id, version="V2"):
    p = PERSONAS[persona]
    return {
        "submissionId": sid, "sessionId": session_id, "submissionType": "feedback", "tenantCode": "mitra",
        "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": p["program_id"], "programName": p["program_name"], "leaderCategoryId": p["leader_category_id"],
                 "leaderCategoryName": persona, "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": p["session_type"],
                 "questionnaireVersion": version, "language": "en", "completionStatus": "COMPLETED",
                 "submissionDate": "2026-11-10T00:00:00+00:00", "botId": p["bot_id"], "messages": _messages()},
    }


async def _add(conn, persona="Youth"):
    sid = f"x{uuid.uuid4().hex[:14]}"
    session_id = f"sess-{uuid.uuid4().hex}"
    await insert_or_update_submission(conn, _event(persona, sid, session_id))
    return sid, session_id


def _answer(school_type=True):
    entry = lambda value, raw: {"value": value, "raw": raw}  # noqa: E731
    fields = {"state": entry("Karnataka", "karnatka"), "district": entry(None, None), "gender": entry("Female", "girl")}
    if school_type:   # the General Public persona has no such field
        fields["school_type"] = entry(None, None)
    return json.dumps({"fields": fields, "pii": {"detected": False, "types": []}})


# --------------------------------------------------------------------------------------------------------------------
# CR3: a redelivery that changes ONLY the persona-selection inputs while the model runs must not let the first
# persona's fields overwrite the session (the message fingerprint is unchanged, so it cannot catch this).
# --------------------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("moved_to", ["General Public", "Parent"])
def test_cr3_a_session_moved_to_another_persona_during_the_llm_call_is_not_overwritten(moved_to):
    async def body(conn, pool):
        sid, session_id = await _add(conn, "Youth")
        loop = asyncio.get_running_loop()
        redelivered = {"done": False}

        def llm(prompt, model=None, max_tokens=None, timeout=None):
            # the redelivery lands while the model is "running" (this function is on a worker thread)
            async def redeliver():
                async with pool.acquire() as other:
                    await insert_or_update_submission(other, _event(moved_to, sid, session_id))
            asyncio.run_coroutine_threadsafe(redeliver(), loop).result(timeout=20)
            redelivered["done"] = True
            return _answer(), {"prompt_tokens": 10, "completion_tokens": 5}

        with pytest.raises(StaleExtractionError, match="another persona"):
            await extract_submission(pool, sid, "mitra", llm_function=llm)
        assert redelivered["done"]
        # nothing of the Youth run was written ...
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE submission_id = $1", sid) == 0
        row = await conn.fetchrow(
            "SELECT session_type, meta_data ? 'extraction' AS has_extraction, pii_detected FROM feedback_submissions "
            "WHERE submission_id = $1", sid)
        assert row["session_type"] == PERSONAS[moved_to]["session_type"]
        assert row["has_extraction"] is False
        # ... and the call that cost tokens is logged as failed
        assert await conn.fetchval(
            "SELECT count(*) FROM llm_logs WHERE submission_id = $1 AND status = 'failed'", sid) == 1
        # run again: it now runs for the NEW persona's config (no stale)
        outcome = await extract_submission(pool, sid, "mitra", llm_function=lambda *a, **k: (_answer(school_type=moved_to != "General Public"), {}), write=False)
        assert outcome.session_type == PERSONAS[moved_to]["session_type"]
    _run(body)


def test_cr3_an_unchanged_session_still_saves(monkeypatch):
    async def body(conn, pool):
        sid, _ = await _add(conn, "Youth")
        outcome = await extract_submission(pool, sid, "mitra", llm_function=lambda *a, **k: (_answer(), {}))
        assert outcome.written is True
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE submission_id = $1", sid) > 0
    _run(body)


def test_cr3_save_extraction_compares_the_identity_under_the_lock():
    """Direct: identity loaded for one persona, the row now belongs to another, fingerprint equal -> stale, no write."""
    async def body(conn, pool):
        sid, session_id = await _add(conn, "Youth")
        async with pool.acquire() as c:
            inputs = await load_submission_inputs(c, sid, "mitra")
        await insert_or_update_submission(conn, _event("General Public", sid, session_id))
        result = fx.ExtractionResult(fields=[], pii_detected=False, pii_types=[])
        async with pool.acquire() as c:
            with pytest.raises(StaleExtractionError):
                await save_extraction(
                    c, submission_id=sid, tenant_code="mitra", input_fingerprint=inputs["fingerprint"],
                    input_identity=inputs["identity"], result=result, config_version_id=uuid.uuid4(),
                    prompt_version_id=uuid.uuid4(), run_at=datetime.now(timezone.utc))
            # without the identity the old check (fingerprint only) lets it through: the gap CodeRabbit named
            await save_extraction(
                c, submission_id=sid, tenant_code="mitra", input_fingerprint=inputs["fingerprint"],
                result=result, config_version_id=uuid.uuid4(), prompt_version_id=uuid.uuid4(),
                run_at=datetime.now(timezone.utc))
        assert inputs["identity"] == persona_identity(
            PERSONAS["Youth"]["session_type"], "mitra", PERSONAS["Youth"]["program_id"])
    _run(body)


# --------------------------------------------------------------------------------------------------------------------
# CR17: the extraction write takes the submissions row lock first, like the ingest does, so it waits for an ingest
# transaction in flight on the same session (and does not hold the feedback_submissions row while it waits).
# --------------------------------------------------------------------------------------------------------------------

def test_cr17_the_extraction_write_waits_on_the_submissions_row_before_it_locks_feedback_submissions():
    async def body(conn, pool):
        sid, _ = await _add(conn, "Youth")
        async with pool.acquire() as c:
            inputs = await load_submission_inputs(c, sid, "mitra")
        result = fx.ExtractionResult(fields=[], pii_detected=False, pii_types=[])

        ingest = await asyncpg.connect(DSN)           # plays the ingest: it holds the submissions row first
        probe = await asyncpg.connect(DSN)
        tx = ingest.transaction()
        await tx.start()
        try:
            await ingest.execute(
                "UPDATE submissions SET updated_at = now() WHERE submission_id = $1 AND tenant_code = 'mitra'", sid)

            async def write():
                async with pool.acquire() as c:
                    await save_extraction(
                        c, submission_id=sid, tenant_code="mitra", input_fingerprint=inputs["fingerprint"],
                        input_identity=inputs["identity"], result=result, config_version_id=uuid.uuid4(),
                        prompt_version_id=uuid.uuid4(), run_at=datetime.now(timezone.utc))
            task = asyncio.ensure_future(write())
            await asyncio.sleep(0.8)
            assert task.done() is False                # blocked behind the ingest transaction ...
            # ... and it does NOT hold the feedback_submissions row meanwhile (a competing locker gets it at once)
            got = await probe.fetchval(
                "SELECT 1 FROM feedback_submissions WHERE submission_id = $1 AND tenant_code = 'mitra' "
                "FOR UPDATE NOWAIT", sid)
            assert got == 1
        finally:
            await tx.commit()
            await ingest.close()
            await probe.close()
        await asyncio.wait_for(task, timeout=20)       # ingest committed: the write completes
        assert await conn.fetchval(
            "SELECT meta_data ? 'extraction' FROM feedback_submissions WHERE submission_id = $1", sid) is True
    _run(body)


# --------------------------------------------------------------------------------------------------------------------
# CR6: restamp reads the windows of a config only AFTER it holds the config row lock (and keeps it to the commit).
# --------------------------------------------------------------------------------------------------------------------

import app.database.feedback_operations as fo
from seed_support import FEEDBACK_PROGRAM_NAME, mitra_config_v1

YOUTH = PERSONAS["Youth"]
YOUTH_V2_TEXT = "2026-11-03T18:23:03+05:30"


def _versions_run(body):
    """Schema, tenant, clean seeds, db.pool; Youth gets its V2 window (restamp off). `body(conn, pool)`."""
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
            res = await fo.set_questionnaire_version("Youth_Voice", "V2", YOUTH_V2_TEXT, created_by="test", restamp=False)
            assert res["changed"] is True
            return await body(conn, pool)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.close()
    return asyncio.run(main())


async def _add_youth(conn, started_at, stored_version):
    sid = f"r{uuid.uuid4().hex[:14]}"
    event = {
        "submissionId": sid, "sessionId": f"sess-{uuid.uuid4().hex}", "submissionType": "feedback",
        "tenantCode": "mitra", "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": YOUTH["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": YOUTH["leader_category_id"], "leaderCategoryName": "Youth",
                 "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": YOUTH["session_type"],
                 "questionnaireVersion": stored_version, "language": "en", "completionStatus": "COMPLETED",
                 "submissionDate": started_at.isoformat(), "botId": YOUTH["bot_id"], "messages": _messages()[:2]},
    }
    await insert_or_update_submission(conn, event)
    return sid


def test_cr6_a_restamp_that_started_before_a_new_window_committed_does_not_write_the_old_labels_over_it(monkeypatch):
    async def body(conn, pool):
        # a session of 5 Nov, stored as V2 (consistent with the windows V1 | V2 from 3 Nov)
        sid = await _add_youth(conn, datetime(2026, 11, 5, 9, 0, tzinfo=timezone.utc), "V2")
        real = fo._restamp_sessions
        entered, release, first = asyncio.Event(), asyncio.Event(), {"pending": True}

        async def paused(c, cfg, versions):
            if first["pending"]:
                first["pending"] = False
                entered.set()
                await release.wait()     # the restamp has its windows and has not touched a session yet
            return await real(c, cfg, versions)
        monkeypatch.setattr(fo, "_restamp_sessions", paused)

        restamp = asyncio.ensure_future(fo.restamp_questionnaire_versions("Youth_Voice"))
        await asyncio.wait_for(entered.wait(), timeout=20)
        # an operator now records V3 from 4 Nov (this relabels the session to V3 in its own transaction)
        record = asyncio.ensure_future(fo.set_questionnaire_version(
            "Youth_Voice", "V3", "2026-11-04T00:00:00+00:00", created_by="test"))
        await asyncio.sleep(0.8)    # with the lock held by the restamp, record waits here; without it, it is done
        release.set()
        await asyncio.wait_for(asyncio.gather(restamp, record), timeout=30)
        label = await conn.fetchval("SELECT questionnaire_version FROM feedback_submissions WHERE submission_id = $1", sid)
        assert label == "V3"
    _versions_run(body)


def test_cr6_restamp_still_relabels_from_the_active_windows():
    async def body(conn, pool):
        wrong = await _add_youth(conn, datetime(2026, 11, 5, 9, 0, tzinfo=timezone.utc), "V1")
        ok = await _add_youth(conn, datetime(2026, 11, 1, 9, 0, tzinfo=timezone.utc), "V1")
        res = await fo.restamp_questionnaire_versions("Youth_Voice")
        assert res["changed"] == 1
        labels = {r["submission_id"]: r["questionnaire_version"]
                  for r in await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")}
        assert labels == {wrong: "V2", ok: "V1"}
    _versions_run(body)


# --------------------------------------------------------------------------------------------------------------------
# CR2: `migrate` reports (never invents) a Google Forms config without session_type / a form's questionnaire_version.
# --------------------------------------------------------------------------------------------------------------------

def test_cr2_migrate_reports_a_google_forms_config_that_lacks_a_form_version_and_writes_nothing_for_it():
    async def body(conn, pool):
        config_id = PERSONAS["Youth"]["gforms_config_id"]
        raw = await conn.fetchval("SELECT config FROM config_version WHERE config_id = $1::uuid AND is_active", config_id)
        config = json.loads(raw) if isinstance(raw, str) else raw
        for form in config["forms"].values():
            form.pop("questionnaire_version", None)
        await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid AND is_active",
                           config_id, json.dumps(config))
        result = await fo.migrate_feedback_config(dry_run=True)
        reported = [i for i in result["invalid"] if i["key"] == "wide_form_identity"]
        assert len(reported) == 1
        assert reported[0]["session_type"] == "Youth_Voice" and reported[0]["source"] == "google_forms"
        assert "questionnaire_version" in reported[0]["problem"]
        # a real run writes no version for it (there is nothing to derive) and says the same again
        real = await fo.migrate_feedback_config()
        assert [i["key"] for i in real["invalid"]] == ["wide_form_identity"]
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid", config_id) == 1
    _versions_run(body)


def test_cr2_migrate_reports_nothing_for_the_complete_seeded_configs():
    async def body(conn, pool):
        result = await fo.migrate_feedback_config(dry_run=True)
        assert [i for i in result["invalid"] if i["key"] == "wide_form_identity"] == []
    _versions_run(body)
