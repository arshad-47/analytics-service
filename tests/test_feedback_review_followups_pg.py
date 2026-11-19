"""
Real-Postgres follow-ups of a code review of the ingest locking (opt-in like the other *_pg files). No LLM, SYNTHETIC data.

  * an identical replay of a session that waited for the persona lock must not delete the extraction that a concurrent,
    label-only relabel of that session left valid (the stored version is read under the lock);
  * two writers of ONE session run one after the other, so a session that moves twice re-ranks the persona it left;
  * the restamp sweep takes the personas of ALL its configs in one call (one key order), not config by config.
"""
import asyncio

import pytest

import factories
import pg_support
import test_feedback_bot_versions_pg as bv
import test_feedback_relabel_pg as rl
from app.database import feedback_operations as fo
from app.database.feedback_bot_versions import lock_persona
from app.database.operations import insert_or_update_submission

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(DSN is None or DSN == "", reason=pg_support.SKIP_REASON)

PARENT = "Parent_Voice"


async def _until_a_backend_waits_for_a_lock(conn, timeout: float = 10.0) -> None:
    """
    Returns once another backend of this database reports that it waits for a lock (pg_stat_activity), so the holder
    commits only after the contender really reached the lock (a fixed sleep can pass before it, for example while the
    pool is still opening its connection). Bounded: fails the test instead of hanging.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        waiting = await conn.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'")
        if waiting > 0:
            return
        await asyncio.sleep(0.05)
    pytest.fail("no backend reached a lock wait within %.0f seconds" % timeout)


def test_a_replay_that_waited_for_the_lock_keeps_the_extraction_a_label_only_relabel_left_valid():
    async def body(conn, pool):
        program = rl._seeded("General Public")           # V1 and V2 ask the same fields: a relabel keeps the extraction
        event = factories.conversation_event(program, rl.GENERAL, "mitra", "V1", bot_id="103")
        sid = event["submissionId"]
        await insert_or_update_submission(conn, event)
        await rl._pretend_extracted(conn, sid)
        assert (await rl._state(conn, sid))["version"] == "V1"

        async with pool.acquire() as holder:
            transaction = holder.transaction()
            await transaction.start()
            await lock_persona(holder, "mitra", program["program_id"], rl.GENERAL, "mitra")

            async def replay():
                async with pool.acquire() as own:
                    return await insert_or_update_submission(own, event)      # the same event again

            waiting = asyncio.create_task(replay())
            await _until_a_backend_waits_for_a_lock(conn)
            assert not waiting.done()                                         # it waits for the persona lock
            lower = factories.conversation_event(program, rl.GENERAL, "mitra", "V1", bot_id="89")
            await insert_or_update_submission(holder, lower)                  # bot 89 relabels S to V2, label only
            await transaction.commit()
            result = await asyncio.wait_for(waiting, timeout=10)

        after = await rl._state(conn, sid)
        assert after["version"] == "V2"
        assert len(after["extracted"]) == 2 and after["pii"] is False and after["status"] == "success"
        assert result["changed"] is False
    rl._run(body)


def test_a_session_that_moves_twice_re_ranks_the_persona_it_left_the_second_time():
    async def body(conn, pool):
        program = await bv._program(conn)
        s = await bv._ingest(conn, program, "101", bv.YOUTH)
        r = await bv._ingest(conn, program, "103", bv.GENERAL)
        session_id = await conn.fetchval("SELECT session_id FROM submissions WHERE submission_id = $1", s)

        def move(to_type):
            event = factories.conversation_event(program, to_type, bot_id="101", sid=s)
            event["sessionId"] = session_id
            return event

        async with pool.acquire() as first:
            transaction = first.transaction()
            await transaction.start()
            await insert_or_update_submission(first, move(bv.GENERAL))        # S: Youth -> General (uncommitted)

            async def second():
                async with pool.acquire() as own:
                    await insert_or_update_submission(own, move(PARENT))      # S: (General) -> Parent

            waiting = asyncio.create_task(second())
            await _until_a_backend_waits_for_a_lock(conn)
            assert not waiting.done()                                         # one session: one writer at a time
            await transaction.commit()
            await asyncio.wait_for(waiting, timeout=10)

        labels = await bv._labels(conn)
        assert labels[r] == "V1"                  # General holds only bot 103 now: it was re-ranked when S left it
        assert labels[s] == "V1"                  # and S is alone in Parent
    bv._run(body)


def test_the_restamp_sweep_locks_the_personas_of_every_config_in_one_call_first(monkeypatch):
    async def body(conn, pool):
        first = await bv._persona_with_config(conn, "Sweep Programme One")
        second = await bv._persona_with_config(conn, "Sweep Programme Two")
        for program in (first, second):
            await bv._ingest(conn, program, "89")
        calls = []
        real = fo.lock_personas

        async def recording(connection, personas):
            calls.append(list(personas))
            return await real(connection, personas)

        monkeypatch.setattr(fo, "lock_personas", recording)
        await fo.restamp_questionnaire_versions(tenant_code="mitra", source="mitra")
        assert len(calls[0]) == 2 and {p[1] for p in calls[0]} == {first["program_id"], second["program_id"]}
        assert len(calls) == 1          # and nothing more is locked later: no second read of the personas takes a lock
    bv._run(body)
