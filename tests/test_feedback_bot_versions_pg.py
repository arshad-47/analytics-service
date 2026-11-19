"""
Real-Postgres tests of the rule "the questionnaire version of a Mitra bot session follows the ORDER OF THE BOT IDS of
its persona" (app/database/feedback_bot_versions.py, the ingest in app/database/operations.py and the repair in
app/database/feedback_operations.py). Opt-in like the other *_pg files: FEEDBACK_TEST_DATABASE_URL names a Postgres
server and tests/pg_support.py gives this file its own scratch database. No LLM, synthetic data only.

A persona is (tenant, program of the submission, session_type, source); the distinct bot ids stored for it are ranked by
number: lowest = V1, next = V2 ... The tests use fictitious programs (tests/factories.py) so that every case controls
exactly which persona a session belongs to.
"""
import asyncio
import json
import uuid

import asyncpg
import pytest

import factories
import pg_support
from app.database.db import SEED_PROMPTS_FILE, db
from app.database.feedback_bot_versions import apply_bot_version_ranks, lock_persona
from app.database.feedback_operations import (
    detect_questionnaire_versions,
    list_questionnaire_versions,
    restamp_questionnaire_versions,
    set_questionnaire_version,
)
from app.database.operations import insert_or_update_submission
from seed_support import seed_config

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


async def _program(conn, name="Bots Programme"):
    return await factories.add_program(conn, TENANT, name, "Youth")


async def _ingest(conn, program, bot_id, session_type=YOUTH, *, version="V1", source="mitra", sid=None, **options):
    """Ingests one conversation_rows session of `bot_id`; the event carries a deliberately WRONG date-window version
    (it is not what gets stored). Returns its submission id."""
    event = factories.conversation_event(program, session_type, source, version, bot_id=bot_id, sid=sid, **options)
    await insert_or_update_submission(conn, event)
    return event["submissionId"]


async def _labels(conn):
    rows = await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")
    return {r["submission_id"]: r["questionnaire_version"] for r in rows}


async def _state(conn, sid):
    """Everything a restamp may touch for one session."""
    row = await conn.fetchrow(
        "SELECT s.status, fs.questionnaire_version AS version, fs.pii_detected, fs.meta_data AS meta, fs.updated_at "
        "FROM feedback_submissions fs JOIN submissions s ON s.submission_id = fs.submission_id "
        "AND s.tenant_code = fs.tenant_code WHERE fs.submission_id = $1", sid)
    meta = json.loads(row["meta"]) if isinstance(row["meta"], str) else row["meta"]
    return {"status": row["status"], "version": row["version"], "pii": row["pii_detected"], "meta": meta,
            "updated_at": row["updated_at"],
            "extracted": await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE submission_id = $1", sid),
            "llm_logs": await conn.fetchval("SELECT count(*) FROM llm_logs WHERE submission_id = $1", sid)}


async def _pretend_extracted(conn, sid):
    """Gives a stored session what a finished extraction leaves behind: extracted fields, a PII flag, the extraction
    record in meta_data, an llm_logs row (audit trail) and status 'success'."""
    await conn.execute(
        "INSERT INTO feedback_extracted_data (submission_id, tenant_code, field_name, value, status) "
        "VALUES ($1, $2, 'age', '17', 'extracted'), ($1, $2, 'gender', 'Female', 'extracted')", sid, TENANT)
    await conn.execute(
        "UPDATE feedback_submissions SET pii_detected = FALSE, "
        "meta_data = meta_data || '{\"extraction\": {\"run_at\": \"2026-11-10T00:00:00Z\"}}'::jsonb "
        "WHERE submission_id = $1", sid)
    await conn.execute(
        "INSERT INTO llm_logs (submission_id, tenant_code, model_name, analysis_type, prompt_version_id, status) "
        "VALUES ($1, $2, 'fake-model', 'feedback_extraction', (SELECT id FROM prompt_version LIMIT 1), 'success')",
        sid, TENANT)
    await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", sid)


# ------------------------------------------------------------------------------------------------ the ingest

def test_bot_89_then_bot_103_are_v1_and_v2_whatever_version_the_event_says():
    async def body(conn, pool):
        program = await _program(conn)
        a = await _ingest(conn, program, "89", version="V2")        # the event's date-window version is not stored
        b = await _ingest(conn, program, "103", version="V1")
        assert await _labels(conn) == {a: "V1", b: "V2"}
    _run(body)


def test_bot_103_first_then_bot_89_restamps_103_and_sends_its_sessions_back_through_processing():
    async def body(conn, pool):
        program = await _program(conn)
        a1 = await _ingest(conn, program, "103")
        a2 = await _ingest(conn, program, "103")
        assert await _labels(conn) == {a1: "V1", a2: "V1"}
        await _pretend_extracted(conn, a1)
        await _pretend_extracted(conn, a2)
        before = await _state(conn, a1)
        assert (before["status"], before["pii"], before["extracted"], before["llm_logs"]) == ("success", False, 2, 1)
        assert "extraction" in before["meta"]

        new = await _ingest(conn, program, "89")                      # lower than the stored bot: ranks shift

        assert await _labels(conn) == {new: "V1", a1: "V2", a2: "V2"}
        for sid in (a1, a2):
            after = await _state(conn, sid)
            assert after["version"] == "V2" and after["status"] == "pending"
            assert after["pii"] is None                              # not checked any more
            assert "extraction" not in after["meta"] and after["meta"]["bot_id"] == "103"   # the rest of meta_data stays
            assert after["extracted"] == 0                           # the stale extracted fields are gone
            assert after["llm_logs"] == 1                            # the audit trail stays
        assert (await _state(conn, new))["status"] == "pending"
    _run(body)


def test_the_result_of_the_ingest_lists_the_other_sessions_it_relabelled():
    async def body(conn, pool):
        program = await _program(conn)
        a = await _ingest(conn, program, "103")
        event = factories.conversation_event(program, YOUTH, bot_id="89")
        res = await insert_or_update_submission(conn, event)
        assert res["restamped_submission_ids"] == [a]
        res = await insert_or_update_submission(conn, factories.conversation_event(program, YOUTH, bot_id="89"))
        assert res["restamped_submission_ids"] == []
    _run(body)


def test_a_new_bot_id_higher_than_the_stored_ones_shifts_nothing():
    async def body(conn, pool):
        program = await _program(conn)
        a = await _ingest(conn, program, "103")
        b = await _ingest(conn, program, "120")
        await _pretend_extracted(conn, a)
        await _pretend_extracted(conn, b)
        before_a, before_b = await _state(conn, a), await _state(conn, b)
        c = await _ingest(conn, program, "130")
        assert await _labels(conn) == {a: "V1", b: "V2", c: "V3"}
        assert await _state(conn, a) == before_a and await _state(conn, b) == before_b    # nothing touched
    _run(body)


def test_the_order_is_numeric_not_alphabetical():
    async def body(conn, pool):
        program = await _program(conn)
        ids = {bot: await _ingest(conn, program, bot) for bot in ("103", "9", "89")}
        assert await _labels(conn) == {ids["9"]: "V1", ids["89"]: "V2", ids["103"]: "V3"}
    _run(body)


def test_the_same_bot_again_and_the_same_event_again_change_nothing():
    async def body(conn, pool):
        program = await _program(conn)
        a = await _ingest(conn, program, "89")
        event = factories.conversation_event(program, YOUTH, bot_id="103")
        await insert_or_update_submission(conn, event)
        b = event["submissionId"]
        c = await _ingest(conn, program, "103")                       # a second session of the same bot
        await _pretend_extracted(conn, a)
        await _pretend_extracted(conn, b)
        before = {sid: await _state(conn, sid) for sid in (a, b)}
        assert await _labels(conn) == {a: "V1", b: "V2", c: "V2"}

        res = await insert_or_update_submission(conn, event)          # the very same event again
        assert res["changed"] is False and res["restamped_submission_ids"] == []
        assert {sid: await _state(conn, sid) for sid in (a, b)} == before
        d = await _ingest(conn, program, "89")                        # another session of an existing bot
        assert (await _labels(conn))[d] == "V1"
        assert {sid: await _state(conn, sid) for sid in (a, b)} == before
    _run(body)


def test_a_second_persona_ranks_independently():
    async def body(conn, pool):
        program = await _program(conn)
        y103 = await _ingest(conn, program, "103", YOUTH)
        g200 = await _ingest(conn, program, "200", GENERAL)           # a bot of another persona of the same program
        assert await _labels(conn) == {y103: "V1", g200: "V1"}
        g150 = await _ingest(conn, program, "150", GENERAL)           # shifts only the General persona
        await _pretend_extracted(conn, y103)
        before = await _state(conn, y103)
        y89 = await _ingest(conn, program, "89", YOUTH)               # shifts only the Youth persona
        labels = await _labels(conn)
        assert labels[y89] == "V1" and labels[y103] == "V2"
        assert labels[g150] == "V1" and labels[g200] == "V2"
        assert (await _state(conn, g200))["status"] == "pending" and before["status"] == "success"
    _run(body)


def test_a_different_program_ranks_independently():
    async def body(conn, pool):
        one, two = await _program(conn, "Programme One"), await _program(conn, "Programme Two")
        a = await _ingest(conn, one, "89")
        b = await _ingest(conn, one, "103")
        c = await _ingest(conn, two, "500")                           # alone in its program: V1, not V3
        assert await _labels(conn) == {a: "V1", b: "V2", c: "V1"}
        d = await _ingest(conn, two, "5")                             # lower than everything, in program two only
        assert await _labels(conn) == {a: "V1", b: "V2", c: "V2", d: "V1"}
    _run(body)


def test_the_source_is_part_of_the_persona():
    async def body(conn, pool):
        program = await _program(conn)
        a = await _ingest(conn, program, "103", source="mitra")
        b = await _ingest(conn, program, "89", source="other_app")
        assert await _labels(conn) == {a: "V1", b: "V1"}
    _run(body)


def test_a_session_that_changes_its_bot_id_is_ranked_again_and_its_extraction_is_dropped():
    async def body(conn, pool):
        program = await _program(conn)
        a = await _ingest(conn, program, "89")
        sid = await _ingest(conn, program, "103", sid="movable-1")
        event = factories.conversation_event(program, YOUTH, bot_id="103", sid=sid)
        await _pretend_extracted(conn, sid)
        event["data"]["botId"] = "50"                                 # a redelivery that names another bot
        event["sessionId"] = await conn.fetchval("SELECT session_id FROM submissions WHERE submission_id = $1", sid)
        await insert_or_update_submission(conn, event)
        assert await _labels(conn) == {sid: "V1", a: "V2"}            # 50 < 89 now
        after = await _state(conn, sid)
        assert after["extracted"] == 0 and after["pii"] is None and "extraction" not in after["meta"]
        assert after["status"] == "pending" and after["meta"]["bot_id"] == "50"
    _run(body)


def test_a_session_that_moves_to_another_persona_re_ranks_the_persona_it_left():
    async def body(conn, pool):
        program = await _program(conn)
        low = await _ingest(conn, program, "89", YOUTH)
        high = await _ingest(conn, program, "103", YOUTH)
        assert await _labels(conn) == {low: "V1", high: "V2"}
        await _pretend_extracted(conn, high)
        # the low bot's session is redelivered as a General Public session: the Youth persona loses bot 89
        event = factories.conversation_event(program, GENERAL, bot_id="89", sid=low)
        event["sessionId"] = await conn.fetchval("SELECT session_id FROM submissions WHERE submission_id = $1", low)
        await insert_or_update_submission(conn, event)
        assert await _labels(conn) == {low: "V1", high: "V1"}         # 103 is the only Youth bot now
        after = await _state(conn, high)
        assert after["status"] == "pending" and after["extracted"] == 0 and after["pii"] is None
    _run(body)


# ------------------------------------------------------------------------------------------ the boundary

@pytest.mark.parametrize("bad", [None, "", "  ", "abc", "0", "-3", "089", "8.5", " 89"])
def test_a_missing_or_malformed_bot_id_is_rejected_and_nothing_is_written(bad):
    async def body(conn, pool):
        program = await _program(conn)
        event = factories.conversation_event(program, YOUTH, bot_id="89")
        event["data"]["botId"] = bad
        with pytest.raises(ValueError, match="data.botId is invalid"):
            await insert_or_update_submission(conn, event)
        del event["data"]["botId"]
        with pytest.raises(ValueError, match="data.botId is invalid"):
            await insert_or_update_submission(conn, event)
        assert await conn.fetchval("SELECT count(*) FROM submissions WHERE tenant_code = 'mitra'") == 0
        assert await conn.fetchval("SELECT count(*) FROM feedback_submissions") == 0
    _run(body)


def test_google_forms_events_are_untouched_by_the_bot_rule():
    async def body(conn, pool):
        program = await _program(conn)
        ref = lambda: f"2026-11-10T08:00:00:{uuid.uuid4().hex}{uuid.uuid4().hex}"   # noqa: E731
        g1 = factories.conversation_event(program, YOUTH, "google_forms", "V7", adapter="wide_form", source_ref=ref(),
                                          sid=str(uuid.uuid4()))
        await insert_or_update_submission(conn, g1)
        assert "botId" not in g1["data"]                              # a form has no bot id: nothing is required
        await _ingest(conn, program, "103")
        await _ingest(conn, program, "89")                            # shifts the Mitra persona, not the form
        g2 = factories.conversation_event(program, YOUTH, "google_forms", "V3", adapter="wide_form", source_ref=ref(),
                                          sid=str(uuid.uuid4()))
        await insert_or_update_submission(conn, g2)
        labels = await _labels(conn)
        assert labels[g1["submissionId"]] == "V7" and labels[g2["submissionId"]] == "V3"   # the form's own version
        assert (await _state(conn, g1["submissionId"]))["status"] == "pending"
        await conn.execute("UPDATE submissions SET status = 'success' WHERE submission_id = $1", g1["submissionId"])
        await _ingest(conn, program, "5")
        assert (await _state(conn, g1["submissionId"]))["status"] == "success"           # never relabelled, never reset
    _run(body)


# ---------------------------------------------------------------------------------------------- the lock

def test_concurrent_ingests_of_two_new_bots_of_one_persona_end_with_consistent_labels():
    async def body(conn, pool):
        for round_number in range(12):
            program = await _program(conn, f"Race {round_number}")
            # one older session first, so that the two newcomers both have something to shift
            seed = await _ingest(conn, program, "500")

            async def ingest(bot_id):
                async with pool.acquire() as own:
                    return await _ingest(own, program, bot_id)

            ids = await asyncio.gather(ingest("103"), ingest("89"), ingest("300"))
            labels = await _labels(conn)
            expected = {"89": "V1", "103": "V2", "300": "V3", "500": "V4"}
            by_bot = {await conn.fetchval(
                "SELECT meta_data ->> 'bot_id' FROM feedback_submissions WHERE submission_id = $1", sid): sid
                for sid in (*ids, seed)}
            assert {bot: labels[sid] for bot, sid in by_bot.items()} == expected
    _run(body)


def test_an_ingest_waits_for_the_persona_lock_of_its_persona_only():
    # a transaction that holds the persona lock (and no row) blocks the ingest of a session of that persona until it
    # ends, so the ranks are always computed on a table that shows every committed bot; another persona is not blocked
    async def body(conn, pool):
        program = await _program(conn)
        await _ingest(conn, program, "103")
        async with pool.acquire() as holder:
            transaction = holder.transaction()
            await transaction.start()
            await lock_persona(holder, TENANT, program["program_id"], YOUTH, "mitra")

            async def ingest(bot_id, session_type):
                async with pool.acquire() as own:
                    return await _ingest(own, program, bot_id, session_type)

            blocked = asyncio.create_task(ingest("89", YOUTH))
            other_persona = await asyncio.wait_for(ingest("7", GENERAL), timeout=10)    # not blocked
            await asyncio.sleep(0.6)
            assert not blocked.done()                                                     # still waiting for the lock
            await transaction.commit()
            new = await asyncio.wait_for(blocked, timeout=10)
        labels = await _labels(conn)
        assert labels[new] == "V1" and labels[other_persona] == "V1"
        assert sorted(labels.values()) == ["V1", "V1", "V2"]
    _run(body)


def test_concurrent_ingests_do_not_deadlock_with_a_session_that_moves_persona():
    async def body(conn, pool):
        program = await _program(conn)
        movers = [await _ingest(conn, program, str(100 + n), YOUTH) for n in range(4)]
        sessions = {sid: await conn.fetchval("SELECT session_id FROM submissions WHERE submission_id = $1", sid)
                    for sid in movers}

        async def move(sid, to_type, bot_id):
            async with pool.acquire() as own:
                event = factories.conversation_event(program, to_type, bot_id=bot_id, sid=sid)
                event["sessionId"] = sessions[sid]
                await insert_or_update_submission(own, event)

        async def newcomer(bot_id, session_type):
            async with pool.acquire() as own:
                await _ingest(own, program, bot_id, session_type)

        await asyncio.wait_for(asyncio.gather(
            move(movers[0], GENERAL, "100"), move(movers[1], GENERAL, "101"),
            move(movers[2], YOUTH, "102"), newcomer("50", YOUTH), newcomer("51", GENERAL)), timeout=60)
        # whatever the interleaving, every persona's labels are the ranks of the bots it ends up holding
        rows = await conn.fetch(
            "SELECT fs.session_type, fs.meta_data ->> 'bot_id' AS bot, fs.questionnaire_version AS version "
            "FROM feedback_submissions fs")
        for session_type in (YOUTH, GENERAL):
            bots = sorted({int(r["bot"]) for r in rows if r["session_type"] == session_type})
            want = {str(bot): f"V{rank}" for rank, bot in enumerate(bots, start=1)}
            assert {r["bot"]: r["version"] for r in rows if r["session_type"] == session_type} == want
    _run(body)


# ------------------------------------------------------------------------------- the tools (restamp / set / list)

async def _persona_with_config(conn, name="Tool Programme"):
    program = await _program(conn, name)
    await factories.add_config(conn, program, "mitra", YOUTH)       # an upload config with the V1 baseline window
    return program


def test_restamp_repairs_the_bot_rank_labels_and_reprocesses_what_it_changed():
    async def body(conn, pool):
        program = await _persona_with_config(conn)
        a = await _ingest(conn, program, "89")
        b = await _ingest(conn, program, "103")
        c = await _ingest(conn, program, "103")
        for sid in (a, b, c):
            await _pretend_extracted(conn, sid)
        # a wrong label, as an old date-window stamp or a manual UPDATE would leave it
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V9' WHERE submission_id = ANY($1)",
                           [a, b])
        ok_before = await _state(conn, c)

        dry = await restamp_questionnaire_versions(YOUTH, True, tenant_code=TENANT, source="mitra", program_id=program["program_id"])
        assert dry["dry_run"] is True and dry["changed"] == 2
        assert (await _labels(conn))[a] == "V9"                       # a dry run writes nothing

        result = await restamp_questionnaire_versions(YOUTH, tenant_code=TENANT, source="mitra", program_id=program["program_id"])
        (entry,) = result["session_types"].values()
        assert result["changed"] == 2 and entry["bot_sessions"]["changed"] == 2
        assert sorted(entry["bot_sessions"]["reprocess"]) == sorted([a, b])
        assert await _labels(conn) == {a: "V1", b: "V2", c: "V2"}
        for sid in (a, b):
            after = await _state(conn, sid)
            assert after["status"] == "pending" and after["extracted"] == 0 and after["pii"] is None
            assert "extraction" not in after["meta"] and after["llm_logs"] == 1
        assert await _state(conn, c) == ok_before                     # c was right: not touched, not reprocessed

        again = await restamp_questionnaire_versions(YOUTH, tenant_code=TENANT, source="mitra", program_id=program["program_id"])
        assert again["changed"] == 0
    _run(body)


def test_restamp_never_puts_a_date_window_label_on_a_bot_session_but_still_relabels_sessions_without_a_bot_id():
    async def body(conn, pool):
        program = await _persona_with_config(conn)
        bot_session = await _ingest(conn, program, "89")
        legacy = await _ingest(conn, program, "89")
        await factories.as_window_stamped_session(conn, legacy, "V3")  # a session that has no bot id and a wrong label
        # a V2 window that started before both sessions: from the windows alone both would be V2
        await conn.execute(
            "UPDATE config_version SET config = jsonb_set(config, '{questionnaire_versions}', "
            "'[{\"key\": \"V1\", \"effective_from\": null}, {\"key\": \"V2\", \"effective_from\": \"2020-01-01T00:00:00+00:00\"}]'::jsonb) "
            "WHERE is_active AND config_id = (SELECT id FROM config WHERE program_id = $1::uuid AND config_type = 'feedback_upload')",
            program["program_id"])
        await restamp_questionnaire_versions(None, tenant_code=TENANT, source="mitra", program_id=program["program_id"])
        labels = await _labels(conn)
        assert labels[bot_session] == "V1"                            # the bot rank, not the window
        assert labels[legacy] == "V2"                                 # the window still serves a session without a bot
    _run(body)


def test_set_refuses_a_persona_whose_sessions_carry_bot_ids():
    async def body(conn, pool):
        program = await _persona_with_config(conn)
        await _ingest(conn, program, "89")
        for restamp in (True, False):
            with pytest.raises(ValueError, match="follows the ORDER OF THE BOT IDS"):
                await set_questionnaire_version(
                    YOUTH, "V2", "2026-12-01T10:30:00+05:30", tenant_code=TENANT, source="mitra",
                    program_id=program["program_id"], restamp=restamp)
        versions = await conn.fetchval(
            "SELECT config -> 'questionnaire_versions' FROM config_version v JOIN config c ON c.id = v.config_id "
            "WHERE c.program_id = $1::uuid AND v.is_active", program["program_id"])
        assert len(json.loads(versions) if isinstance(versions, str) else versions) == 1       # nothing was written
    _run(body)


def test_set_still_works_for_a_persona_without_bot_sessions():
    async def body(conn, pool):
        program = await _persona_with_config(conn)
        result = await set_questionnaire_version(
            YOUTH, "V2", "2026-12-01T10:30:00+05:30", tenant_code=TENANT, source="mitra",
            program_id=program["program_id"], created_by="test")
        assert result["changed"] is True
    _run(body)


def test_list_and_detect_show_the_bot_based_label_next_to_the_stamped_one():
    async def body(conn, pool):
        program = await _persona_with_config(conn)
        a = await _ingest(conn, program, "89")
        b = await _ingest(conn, program, "103")
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V5' WHERE submission_id = $1", b)
        listed = await list_questionnaire_versions(YOUTH, tenant_code=TENANT, source="mitra",
                                                   program_id=program["program_id"])
        (entry,) = listed.values()
        (persona,) = entry["bot_versions"]
        assert persona["program_id"] == program["program_id"]
        assert persona["bots"]["89"] == {"label": "V1", "sessions": 1, "stamped": {"V1": 1}}
        assert persona["bots"]["103"] == {"label": "V2", "sessions": 1, "stamped": {"V5": 1}}
        assert persona["sessions_to_relabel"] == 1
        detected = await detect_questionnaire_versions(YOUTH, tenant_code=TENANT, source="mitra",
                                                       program_id=program["program_id"])
        (found,) = detected.values()
        assert found["bot_versions"] == entry["bot_versions"]
        assert (await _labels(conn))[a] == "V1"                       # read-only: nothing changed
        assert (await _labels(conn))[b] == "V5"
    _run(body)


def test_apply_bot_version_ranks_is_idempotent_and_takes_the_persona_lock_itself():
    async def body(conn, pool):
        program = await _program(conn)
        a = await _ingest(conn, program, "103")
        await _ingest(conn, program, "89")
        async with conn.transaction():
            first = await apply_bot_version_ranks(conn, TENANT, program["program_id"], YOUTH, "mitra")
            second = await apply_bot_version_ranks(conn, TENANT, program["program_id"], YOUTH, "mitra")
        assert first["restamped"] == [] and second["restamped"] == []   # the ingest already ranked everything
        assert first["versions"] == {"V1": "89", "V2": "103"} and a in await _labels(conn)
    _run(body)
