"""
Real-Postgres tests of "a relabelled bot session is reprocessed only when that changes something"
(apply_bot_version_ranks in app/database/feedback_bot_versions.py, the ingest in app/database/operations.py and the
repair in app/database/feedback_operations.py). Opt-in like the other *_pg files: FEEDBACK_TEST_DATABASE_URL names a
Postgres server and tests/pg_support.py gives this file its own scratch database. No LLM, no Temporal, synthetic data.

When a lower bot id arrives, the sessions of the higher ones are relabelled (V1 -> V2). The persona's ACTIVE extraction
config says whether that matters: when the old and the new label ask the same fields (General Public: no
version-dependent field) only the label changes and the finished extraction, the PII flag and the status stay; when a
field is asked in one version only (Youth: annual_household_income V1, monthly_household_income V2) the stored rows no
longer describe the session, so it goes back to 'pending' and is listed for dispatch (reset_submission_ids).
"""
import asyncio
import copy
import json

import asyncpg
import pytest

import factories
import pg_support
from app.database.db import SEED_PROMPTS_FILE, db
from app.database.feedback_operations import restamp_questionnaire_versions
from app.database.operations import insert_or_update_submission
from seed_support import PERSONAS, extraction_config_v1, seed_config

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

TENANT = "mitra"
YOUTH = "Youth_Voice"
GENERAL = "General_Public_Voice"


def _run(body):
    """Runs `body(conn, pool)` on the scratch database (schema, tenant, seed prompts and seed configs applied)."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await conn.execute(SEED_PROMPTS_FILE.read_text(encoding="utf-8"))
            await seed_config(conn)
            pool = await asyncpg.create_pool(DSN, min_size=2, max_size=8)
            db.pool = pool
            return await body(conn, pool)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.close()
    return asyncio.run(main())


async def _program(conn, name="Relabel Programme"):
    """A fictitious program: it has NO extraction config of its own and the seeded ones belong to the seeded programs."""
    return await factories.add_program(conn, TENANT, name, "Youth")


def _seeded(persona):
    """The seeded program of a persona (seed_config.sql): its ACTIVE extraction config is the one the relabel reads."""
    p = PERSONAS[persona]
    return {"tenant": TENANT, "program_id": p["program_id"], "leader_id": p["leader_category_id"],
            "program_name": p["program_name"], "leader_name": persona}


async def _ingest(conn, program, bot_id, session_type, **options):
    """Ingests one conversation_rows session of `bot_id`; returns (submission id, the ingest result)."""
    event = factories.conversation_event(program, session_type, "mitra", "V1", bot_id=bot_id, **options)
    res = await insert_or_update_submission(conn, event)
    return event["submissionId"], res


async def _state(conn, sid):
    """Everything a relabel may touch for one session."""
    row = await conn.fetchrow(
        "SELECT s.status, fs.questionnaire_version AS version, fs.pii_detected, fs.abusive_language, "
        "fs.meta_data AS meta, fs.updated_at "
        "FROM feedback_submissions fs JOIN submissions s ON s.submission_id = fs.submission_id "
        "AND s.tenant_code = fs.tenant_code WHERE fs.submission_id = $1", sid)
    meta = json.loads(row["meta"]) if isinstance(row["meta"], str) else row["meta"]
    rows = await conn.fetch(
        "SELECT field_name, value, status FROM feedback_extracted_data WHERE submission_id = $1 ORDER BY field_name", sid)
    return {"status": row["status"], "version": row["version"], "pii": row["pii_detected"], "abusive": row["abusive_language"], "meta": meta,
            "updated_at": row["updated_at"], "extracted": [tuple(r) for r in rows],
            "llm_logs": await conn.fetchval("SELECT count(*) FROM llm_logs WHERE submission_id = $1", sid)}


def _unchanged_but_label(before, after, new_label):
    """`after` is `before` with only the label (and updated_at) moved."""
    assert after["version"] == new_label and after["updated_at"] > before["updated_at"]
    for key in ("status", "pii", "abusive", "meta", "extracted", "llm_logs"):
        assert after[key] == before[key], key


async def _pretend_extracted(conn, sid, status="success"):
    """What a finished extraction leaves behind: extracted fields, a PII flag, the extraction record in meta_data, an
    llm_logs row (audit trail) and the submission status."""
    await conn.execute(
        "INSERT INTO feedback_extracted_data (submission_id, tenant_code, field_name, value, status) "
        "VALUES ($1, $2, 'age', '17', 'extracted'), ($1, $2, 'gender', 'Female', 'extracted')", sid, TENANT)
    await conn.execute(
        "UPDATE feedback_submissions SET pii_detected = FALSE, abusive_language = FALSE, "
        "meta_data = meta_data || '{\"extraction\": {\"run_at\": \"2026-11-10T00:00:00Z\"}}'::jsonb "
        "WHERE submission_id = $1", sid)
    await conn.execute(
        "INSERT INTO llm_logs (submission_id, tenant_code, model_name, analysis_type, prompt_version_id, status) "
        "VALUES ($1, $2, 'fake-model', 'feedback_extraction', (SELECT id FROM prompt_version LIMIT 1), 'success')",
        sid, TENANT)
    await conn.execute("UPDATE submissions SET status = $2 WHERE submission_id = $1", sid, status)


# ------------------------------------------------------------------------------------------ the ingest

def test_general_public_103_then_89_relabels_but_keeps_the_finished_extraction():
    async def body(conn, pool):
        program = _seeded("General Public")
        g1, _ = await _ingest(conn, program, "103", GENERAL)
        g2, _ = await _ingest(conn, program, "103", GENERAL)
        await _pretend_extracted(conn, g1)
        await _pretend_extracted(conn, g2)
        before = {sid: await _state(conn, sid) for sid in (g1, g2)}
        assert all(b["version"] == "V1" and b["status"] == "success" and b["pii"] is False
                   and b["abusive"] is False for b in before.values())

        new, res = await _ingest(conn, program, "89", GENERAL)         # lower than the stored bot: V1 -> V2

        _unchanged_but_label(before[g1], await _state(conn, g1), "V2")
        _unchanged_but_label(before[g2], await _state(conn, g2), "V2")
        assert (await _state(conn, new))["version"] == "V1" and (await _state(conn, new))["status"] == "pending"
        assert sorted(res["restamped_submission_ids"]) == sorted([g1, g2])    # relabelled ...
        assert res["reset_submission_ids"] == []                               # ... but none to process again
        pending = await conn.fetch("SELECT submission_id FROM submissions WHERE status = 'pending'")
        assert [r["submission_id"] for r in pending] == [new]
    _run(body)


def test_a_session_still_processing_or_failed_keeps_its_status_when_only_the_label_moves():
    async def body(conn, pool):
        program = _seeded("General Public")
        running, _ = await _ingest(conn, program, "103", GENERAL)
        failed, _ = await _ingest(conn, program, "103", GENERAL)
        await conn.execute("UPDATE submissions SET status = 'processing' WHERE submission_id = $1", running)
        await conn.execute("UPDATE submissions SET status = 'failed' WHERE submission_id = $1", failed)
        await _ingest(conn, program, "89", GENERAL)
        assert (await _state(conn, running))["status"] == "processing"        # the in-flight extraction is not disturbed
        assert (await _state(conn, failed))["status"] == "failed"
        assert {(await _state(conn, s))["version"] for s in (running, failed)} == {"V2"}
    _run(body)


def test_youth_103_then_89_changes_the_asked_fields_so_the_sessions_are_reset_and_listed():
    async def body(conn, pool):
        program = _seeded("Youth")
        y1, _ = await _ingest(conn, program, "103", YOUTH)
        y2, _ = await _ingest(conn, program, "103", YOUTH)
        await _pretend_extracted(conn, y1)
        await _pretend_extracted(conn, y2)

        new, res = await _ingest(conn, program, "89", YOUTH)

        assert sorted(res["reset_submission_ids"]) == sorted([y1, y2])
        assert sorted(res["restamped_submission_ids"]) == sorted([y1, y2])
        for sid in (y1, y2):
            after = await _state(conn, sid)
            assert after["version"] == "V2" and after["status"] == "pending"
            assert after["pii"] is None and after["abusive"] is None and after["extracted"] == []
            assert "extraction" not in after["meta"] and after["meta"]["bot_id"] == "103"
            assert after["llm_logs"] == 1                                       # the audit trail stays
        assert (await _state(conn, new))["status"] == "pending"
    _run(body)


def test_a_mixed_persona_resets_only_what_changed_per_label_pair():
    """Three bots: Youth V1 -> V2 differs (annual <-> monthly income), V2 -> V3 differs too (V3 asks neither)."""
    async def body(conn, pool):
        program = _seeded("Youth")
        a, _ = await _ingest(conn, program, "103", YOUTH)
        b, _ = await _ingest(conn, program, "200", YOUTH)                      # V2
        await _pretend_extracted(conn, a)
        await _pretend_extracted(conn, b)
        _, res = await _ingest(conn, program, "89", YOUTH)                     # a: V1 -> V2, b: V2 -> V3
        assert sorted(res["reset_submission_ids"]) == sorted([a, b])
    _run(body)


def test_one_sweep_decides_per_label_pair_a_label_only_relabel_next_to_a_reset():
    """V1 and V2 ask the same fields, V3 asks one more: bot 89 arriving shifts a V1 -> V2 (keeps its extraction) and b V2 -> V3 (reset)."""
    async def body(conn, pool):
        program = await _program(conn)
        own = copy.deepcopy(extraction_config_v1("General Public"))
        own["fields"].append({"name": "only_v3", "kind": "free_text", "description": "asked since V3", "versions": ["V3"]})
        await factories.add_config(conn, program, "any", GENERAL, own, config_type="feedback_extraction")
        a, _ = await _ingest(conn, program, "103", GENERAL)                    # V1
        b, _ = await _ingest(conn, program, "200", GENERAL)                    # V2
        await _pretend_extracted(conn, a)
        await _pretend_extracted(conn, b)
        before_a = await _state(conn, a)
        _, res = await _ingest(conn, program, "89", GENERAL)                   # a: V1 -> V2, b: V2 -> V3
        assert sorted(res["restamped_submission_ids"]) == sorted([a, b])
        assert res["reset_submission_ids"] == [b]                              # only the pair whose fields differ
        _unchanged_but_label(before_a, await _state(conn, a), "V2")
        after_b = await _state(conn, b)
        assert after_b["version"] == "V3" and after_b["status"] == "pending" and after_b["extracted"] == []
    _run(body)


def test_a_persona_without_an_active_extraction_config_takes_the_safe_side():
    async def body(conn, pool):
        program = await _program(conn)
        a, _ = await _ingest(conn, program, "103", "No_Config_Voice")          # no extraction config for this persona
        await _pretend_extracted(conn, a)
        _, res = await _ingest(conn, program, "89", "No_Config_Voice")
        assert res["reset_submission_ids"] == [a]
        after = await _state(conn, a)
        assert after["version"] == "V2" and after["status"] == "pending" and after["extracted"] == []
    _run(body)


def test_the_extraction_config_of_the_sessions_own_program_decides_not_the_default():
    """The program has its own General config with a V2-only field: the program-less default would say 'same fields'."""
    async def body(conn, pool):
        program = await _program(conn)
        own = copy.deepcopy(extraction_config_v1("General Public"))
        own["fields"].append({"name": "only_v2", "kind": "free_text", "description": "asked since V2", "versions": ["V2"]})
        await factories.add_config(conn, program, "any", GENERAL, own, config_type="feedback_extraction")
        a, _ = await _ingest(conn, program, "103", GENERAL)
        await _pretend_extracted(conn, a)
        _, res = await _ingest(conn, program, "89", GENERAL)
        assert res["reset_submission_ids"] == [a]
    _run(body)


def test_the_extraction_config_of_the_sessions_own_program_with_equal_fields_keeps_the_extraction():
    async def body(conn, pool):
        program = await _program(conn)
        await factories.add_config(conn, program, "any", GENERAL, extraction_config_v1("General Public"),
                                   config_type="feedback_extraction")
        a, _ = await _ingest(conn, program, "103", GENERAL)
        await _pretend_extracted(conn, a)
        before = await _state(conn, a)
        _, res = await _ingest(conn, program, "89", GENERAL)
        assert res["restamped_submission_ids"] == [a] and res["reset_submission_ids"] == []
        _unchanged_but_label(before, await _state(conn, a), "V2")
    _run(body)


def test_a_malformed_extraction_config_takes_the_safe_side():
    async def body(conn, pool):
        program = await _program(conn)
        await factories.add_config(conn, program, "any", GENERAL, {"fields": "not a list"}, config_type="feedback_extraction")
        a, _ = await _ingest(conn, program, "103", GENERAL)
        _, res = await _ingest(conn, program, "89", GENERAL)
        assert res["reset_submission_ids"] == [a]
    _run(body)


def test_redelivering_the_same_event_relabels_and_resets_nothing_more():
    async def body(conn, pool):
        program = _seeded("General Public")
        a, _ = await _ingest(conn, program, "103", GENERAL)
        await _pretend_extracted(conn, a)
        event = factories.conversation_event(program, GENERAL, bot_id="89")
        first = await insert_or_update_submission(conn, event)
        assert first["restamped_submission_ids"] == [a]
        settled = await _state(conn, a)
        again = await insert_or_update_submission(conn, event)
        assert again["changed"] is False
        assert again["restamped_submission_ids"] == [] and again["reset_submission_ids"] == []
        assert await _state(conn, a) == settled
    _run(body)


def test_a_higher_bot_id_relabels_nothing_and_resets_nothing():
    async def body(conn, pool):
        program = _seeded("Youth")
        await _ingest(conn, program, "103", YOUTH)
        _, res = await _ingest(conn, program, "130", YOUTH)
        assert res["restamped_submission_ids"] == [] and res["reset_submission_ids"] == []
    _run(body)


def test_the_persona_that_a_moving_session_leaves_is_relabelled_by_the_same_rule():
    """A session that moves persona drops its bot id from the old one; the old persona's ranks shift, label-only for General."""
    async def body(conn, pool):
        program = _seeded("General Public")
        low, _ = await _ingest(conn, program, "89", GENERAL, sid="mover-1")
        high, _ = await _ingest(conn, program, "103", GENERAL)
        await _pretend_extracted(conn, high)
        before = await _state(conn, high)
        assert before["version"] == "V2"
        event = factories.conversation_event(program, YOUTH, bot_id="89", sid=low)
        event["sessionId"] = await conn.fetchval("SELECT session_id FROM submissions WHERE submission_id = $1", low)
        res = await insert_or_update_submission(conn, event)
        assert res["restamped_submission_ids"] == [high] and res["reset_submission_ids"] == []
        _unchanged_but_label(before, await _state(conn, high), "V1")
    _run(body)


# ------------------------------------------------------------------------------------------ the restamp tool

def test_restamp_repairs_a_general_public_persona_without_reprocessing_it():
    async def body(conn, pool):
        program = _seeded("General Public")       # the seeded upload config is what the tool needs
        a, _ = await _ingest(conn, program, "89", GENERAL)
        b, _ = await _ingest(conn, program, "103", GENERAL)
        for sid in (a, b):
            await _pretend_extracted(conn, sid)
        # both labels wrong (swapped), as a manual UPDATE or an old stamp would leave them
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V2' WHERE submission_id = $1", a)
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V1' WHERE submission_id = $1", b)
        before = {sid: await _state(conn, sid) for sid in (a, b)}

        result = await restamp_questionnaire_versions(
            GENERAL, tenant_code=TENANT, source="mitra", program_id=program["program_id"])
        (entry,) = result["session_types"].values()
        assert result["changed"] == 2 and entry["bot_sessions"]["changed"] == 2
        assert entry["bot_sessions"]["label_only"] == 2 and entry["bot_sessions"]["reprocess"] == []
        _unchanged_but_label(before[a], await _state(conn, a), "V1")
        _unchanged_but_label(before[b], await _state(conn, b), "V2")
        assert (await restamp_questionnaire_versions(
            GENERAL, tenant_code=TENANT, source="mitra", program_id=program["program_id"]))["changed"] == 0
    _run(body)


def test_restamp_repairs_a_youth_persona_and_resets_the_sessions_whose_fields_changed():
    async def body(conn, pool):
        program = _seeded("Youth")
        a, _ = await _ingest(conn, program, "89", YOUTH)
        b, _ = await _ingest(conn, program, "103", YOUTH)
        for sid in (a, b):
            await _pretend_extracted(conn, sid)
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V2' WHERE submission_id = $1", a)
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V1' WHERE submission_id = $1", b)

        dry = await restamp_questionnaire_versions(
            YOUTH, True, tenant_code=TENANT, source="mitra", program_id=program["program_id"])
        assert dry["changed"] == 2
        assert (await _state(conn, a))["version"] == "V2"                      # a dry run writes nothing

        result = await restamp_questionnaire_versions(
            YOUTH, tenant_code=TENANT, source="mitra", program_id=program["program_id"])
        (entry,) = result["session_types"].values()
        assert entry["bot_sessions"]["label_only"] == 0
        assert sorted(entry["bot_sessions"]["reprocess"]) == sorted([a, b])
        for sid, label in ((a, "V1"), (b, "V2")):
            after = await _state(conn, sid)
            assert after["version"] == label and after["status"] == "pending"
            assert after["pii"] is None and after["abusive"] is None and after["extracted"] == [] and "extraction" not in after["meta"]
            assert after["llm_logs"] == 1
    _run(body)


# ------------------------------------------------------------------------------------------ the abusive-language flag

def test_new_messages_and_a_persona_change_reset_the_abusive_language_flag_with_the_pii_flag():
    async def body(conn, pool):
        program = _seeded("General Public")
        sid, _ = await _ingest(conn, program, "103", GENERAL, sid="abuse-flag-1")
        await _pretend_extracted(conn, sid)
        grown = factories.conversation_event(
            program, GENERAL, bot_id="103", sid=sid,
            messages=factories.conversation_messages(answers=("41", "a later answer")))
        grown["sessionId"] = await conn.fetchval("SELECT session_id FROM submissions WHERE submission_id = $1", sid)
        await insert_or_update_submission(conn, grown)                          # new messages
        after = await _state(conn, sid)
        assert after["pii"] is None and after["abusive"] is None

        await conn.execute("DELETE FROM feedback_extracted_data WHERE submission_id = $1", sid)
        await _pretend_extracted(conn, sid)                                     # flags set again
        moved = factories.conversation_event(program, YOUTH, bot_id="103", sid=sid)    # another persona
        moved["sessionId"] = grown["sessionId"]
        await insert_or_update_submission(conn, moved)
        after = await _state(conn, sid)
        assert after["pii"] is None and after["abusive"] is None and "extraction" not in after["meta"]
    _run(body)
