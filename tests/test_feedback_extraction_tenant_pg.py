"""
Real-Postgres tests of the extraction data path across TWO tenants that carry the SAME submission id, plus the two
database guarantees the extraction relies on:

  * tenant isolation: the rows that feed the LLM and the fingerprint (`_ROWS_SQL`), the config lookup, the closing
    keys, the PII flag write and the PII reset on an edit never touch or read another tenant's data;
  * the row lock of `save_extraction` (FOR UPDATE on the feedback_submissions row) really blocks a concurrent writer
    until the transaction ends (the guard that makes the stale-write check safe);
  * the prompt choice `ORDER BY version DESC` (highest ACTIVE version) against the real SQL, not against a fake.

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards).
Fake LLM only (no network). SYNTHETIC data only.
"""
import asyncio
import json
import uuid

import asyncpg
import pytest
import pg_support

from app.database import feedback_extraction_operations as ops
from app.database.db import SEED_PROMPTS_FILE, db
from app.database.feedback_extraction_operations import (
    ExtractionSetupError,
    extract_submission,
    load_prompt_by_name,
    load_submission_inputs,
    rows_fingerprint,
    save_extraction,
)
from app.database.operations import insert_or_update_submission
from app.services import feedback_extraction as fx
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, extraction_config_id, seed_config

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH = PERSONAS["Youth"]
MITRA_MARKER = "zzq-mitra-words"
OTHER_MARKER = "zzq-other-tenant-words"
OTHER_PROGRAM = str(uuid.uuid4())
OTHER_LEADER = str(uuid.uuid4())


def _run(body):
    """Runs `body(conn, pool)` on the scratch database: schema, tenants 'mitra' and 'other', prompts, clean seeds."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            for name, code in (("Mitra", "mitra"), ("Other", "other")):
                await conn.execute("INSERT INTO tenant (name, code) VALUES ($1, $2) ON CONFLICT (code) DO NOTHING", name, code)
            await conn.execute("DELETE FROM submissions")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await conn.execute(SEED_PROMPTS_FILE.read_text(encoding="utf-8"))
            await seed_config(conn)
            pool = await asyncpg.create_pool(DSN, min_size=2, max_size=6)
            db.pool = pool
            return await body(conn, pool)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions")
            await conn.close()
    return asyncio.run(main())


def _messages(marker, answer_count=1):
    rows = [{"sequence": 1, "questionKey": "AGE", "isFollowUp": False, "message": "How old are you?",
             "translatedMessage": None, "messageAt": None, "parentSequence": None}]
    for n in range(answer_count):
        rows.append({"sequence": 2 + n, "questionKey": "AGE", "isFollowUp": False, "message": f"{marker} {n}",
                     "translatedMessage": None, "messageAt": None, "parentSequence": 1})
    return rows


def _event(tenant, sid, marker, answer_count=1):
    if tenant == "mitra":
        tags = {"programId": YOUTH["program_id"], "programName": FEEDBACK_PROGRAM_NAME, "leaderCategoryId": YOUTH["leader_category_id"],
                "leaderCategoryName": "Youth", "state": None, "district": None, "source": "mitra"}
    else:
        tags = {"programId": OTHER_PROGRAM, "programName": "Other Programme", "leaderCategoryId": OTHER_LEADER,
                "leaderCategoryName": "Youth", "state": None, "district": None, "source": "mitra"}
    return {
        "submissionId": sid, "sessionId": f"sess-{tenant}-{sid}", "submissionType": "feedback", "tenantCode": tenant,
        "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z", "tags": tags,
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": "Youth_Voice", "questionnaireVersion": "V2", "botId": "101",
                 "language": "en", "completionStatus": "COMPLETED", "submissionDate": "2026-11-10T00:00:00+00:00",
                 "messages": _messages(marker, answer_count)},
    }


async def _two_tenants(conn, sid="shared-sub-1"):
    """The same submission id in 'mitra' (3 rows) and 'other' (4 rows with other words); mitra's Youth config has ONE field."""
    await insert_or_update_submission(conn, _event("mitra", sid, MITRA_MARKER, 2))
    await insert_or_update_submission(conn, _event("other", sid, OTHER_MARKER, 3))
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "Feedback Metadata Extraction",
           "fields": [{"name": "district", "kind": "free_text", "description": "District."}]}
    await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                       extraction_config_id("Youth_Voice"), json.dumps(raw))
    return sid


async def _add_extraction_config(conn, tenant, program_id, model, session_type="Youth_Voice"):
    raw = {"schema_version": 1, "persona": "Youth", "session_type": session_type, "prompt_name": "Feedback Metadata Extraction",
           "fields": [{"name": "district", "kind": "free_text", "description": "District."}], "llm": {"model": model}}
    config_id = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, session_type) "
        "VALUES ($1::uuid, 'any', $2, 'feedback_extraction', $3::uuid, $4, $5)",
        config_id, f"cfg-{config_id[:6]}", program_id, tenant, session_type)
    await conn.execute("INSERT INTO config_version (config_id, version, is_active, config) VALUES ($1::uuid, 1, TRUE, $2::jsonb)",
                       config_id, json.dumps(raw))


class FakeLlm:
    def __init__(self, pii=False):
        self.pii, self.prompts = pii, []

    def __call__(self, prompt, model=None, max_tokens=None, timeout=None):
        self.prompts.append(prompt)
        answer = {"fields": {"district": {"value": "Mysuru", "raw": "mysore"}},
                  "pii": {"detected": self.pii, "types": ["name"] if self.pii else []}}
        return json.dumps(answer), {"prompt_tokens": 1, "completion_tokens": 1}


async def _state(conn, tenant, sid):
    row = await conn.fetchrow("SELECT pii_detected, meta_data FROM feedback_submissions WHERE submission_id = $1 AND tenant_code = $2",
                              sid, tenant)
    meta = row["meta_data"]
    return row["pii_detected"], json.loads(meta) if isinstance(meta, str) else meta


# ------------------------------------------------------------------------------------------------ D4 tenant isolation

def test_the_rows_and_the_fingerprint_of_a_tenant_never_include_another_tenants_rows():
    async def body(conn, pool):
        sid = await _two_tenants(conn)
        mitra = await load_submission_inputs(conn, sid, "mitra")
        await _add_extraction_config(conn, "other", None, "other-tenant/model")          # so that 'other' can load too
        other = await load_submission_inputs(conn, sid, "other")
        assert len(mitra["rows"]) == 3 and len(other["rows"]) == 4                        # not 7 and not 7
        assert all(OTHER_MARKER not in str(r["message"]) for r in mitra["rows"])
        assert all(MITRA_MARKER not in str(r["message"]) for r in other["rows"])
        only_mitra = await conn.fetch(
            "SELECT sequence, parent_id, question_key, message, translated_message FROM feedbacks "
            "WHERE submission_id = $1 AND tenant_code = 'mitra' ORDER BY sequence", sid)
        assert mitra["fingerprint"] == rows_fingerprint(only_mitra) != other["fingerprint"]
    _run(body)


def test_an_extraction_of_one_tenant_sends_only_its_words_and_leaves_the_other_tenant_untouched():
    async def body(conn, pool):
        sid = await _two_tenants(conn)
        await conn.execute(
            "UPDATE feedback_submissions SET pii_detected = FALSE, meta_data = '{\"sentinel\": \"keep\"}'::jsonb "
            "WHERE submission_id = $1 AND tenant_code = 'other'", sid)
        before = await _state(conn, "other", sid)
        fake = FakeLlm(pii=True)
        await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert MITRA_MARKER in fake.prompts[0] and OTHER_MARKER not in fake.prompts[0]
        assert (await _state(conn, "mitra", sid))[0] is True
        assert await _state(conn, "other", sid) == before                                   # flag AND meta_data untouched
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE tenant_code = 'other'") == 0
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE tenant_code = 'mitra'") > 0
        assert await conn.fetchval("SELECT count(*) FROM llm_logs WHERE tenant_code = 'other'") == 0
    _run(body)


def test_the_pii_reset_after_an_edit_leaves_the_other_tenants_flag_alone():
    async def body(conn, pool):
        sid = await _two_tenants(conn)
        await conn.execute("UPDATE feedback_submissions SET pii_detected = FALSE WHERE submission_id = $1", sid)   # both tenants
        await insert_or_update_submission(conn, _event("mitra", sid, MITRA_MARKER, 3))        # mitra's session grew
        assert (await _state(conn, "mitra", sid))[0] is None                                 # reset
        assert (await _state(conn, "other", sid))[0] is False                                # the other tenant's flag is kept
    _run(body)


def test_the_config_lookup_never_uses_another_tenants_config():
    async def body(conn, pool):
        sid = await _two_tenants(conn)
        # another tenant's program-less config for the same session_type: a candidate for 'other' only
        await _add_extraction_config(conn, "other", None, "other-tenant/model")
        fake = FakeLlm()
        await extract_submission(pool, sid, "mitra", llm_function=fake, write=False)
        assert fake.prompts and OTHER_MARKER not in fake.prompts[0]
        # 'other' has only its program-less config; mitra's seeded Youth config belongs to mitra's program
        seen = []

        def spy(prompt, model=None, max_tokens=None, timeout=None):
            seen.append(model)
            return FakeLlm()(prompt, model=model)

        await extract_submission(pool, sid, "other", llm_function=spy, write=False)
        assert seen == ["other-tenant/model"]
        seen.clear()
        await extract_submission(pool, sid, "mitra", llm_function=spy, write=False)
        assert seen[0] != "other-tenant/model"
        # a tenant without a config of its own fails loudly instead of borrowing the other tenant's (both directions)
        await conn.execute("DELETE FROM config_version WHERE config_id IN (SELECT id FROM config WHERE tenant_code = 'mitra')")
        await conn.execute("DELETE FROM config WHERE tenant_code = 'mitra'")
        with pytest.raises(ExtractionSetupError):
            await extract_submission(pool, sid, "mitra", llm_function=FakeLlm(), write=False)
        await conn.execute("DELETE FROM config_version WHERE config_id IN (SELECT id FROM config WHERE tenant_code = 'other')")
        await conn.execute("DELETE FROM config WHERE tenant_code = 'other'")
        await _add_extraction_config(conn, "mitra", None, "mitra-default/model")
        with pytest.raises(ExtractionSetupError):
            await extract_submission(pool, sid, "other", llm_function=FakeLlm(), write=False)
    _run(body)


def test_load_submission_inputs_rejects_a_session_that_exists_only_in_another_tenant():
    async def body(conn, pool):
        await insert_or_update_submission(conn, _event("other", "only-other-1", OTHER_MARKER))
        with pytest.raises(ValueError, match="not a stored feedback session"):
            await load_submission_inputs(conn, "only-other-1", "mitra")
    _run(body)


# ------------------------------------------------------------------------------------------------ D6 the row lock

class _GatedConn:
    """Wraps a connection: the first read of the feedbacks rows INSIDE save_extraction's transaction (the fingerprint
    re-read, taken after the FOR UPDATE) waits at `gate`, so the test can try to write while the lock is held."""

    def __init__(self, conn, inside, gate):
        self._conn, self._inside, self._gate = conn, inside, gate

    def transaction(self, *a, **k):
        return self._conn.transaction(*a, **k)

    async def fetchval(self, *a, **k):
        return await self._conn.fetchval(*a, **k)

    async def fetch(self, sql, *args):
        rows = await self._conn.fetch(sql, *args)
        if "FROM feedbacks" in sql:
            self._inside.set()
            await self._gate.wait()
        return rows

    async def execute(self, *a, **k):
        return await self._conn.execute(*a, **k)

    async def fetchrow(self, *a, **k):
        return await self._conn.fetchrow(*a, **k)


def test_save_extraction_holds_a_row_lock_that_blocks_a_concurrent_writer_until_it_commits():
    async def body(conn, pool):
        sid = await _two_tenants(conn)
        inputs = await load_submission_inputs(conn, sid, "mitra")
        config = inputs["config_row"]["config"]
        result = fx.empty_result(config)
        prompt_version_id = await conn.fetchval("SELECT id FROM prompt_version LIMIT 1")
        inside, gate = asyncio.Event(), asyncio.Event()

        async def saver():
            async with pool.acquire() as saver_conn:
                await save_extraction(
                    _GatedConn(saver_conn, inside, gate), submission_id=sid, tenant_code="mitra",
                    input_fingerprint=inputs["fingerprint"], result=result, config_version_id=inputs["config_row"]["id"],
                    prompt_version_id=prompt_version_id, run_at=inputs["loaded_at"])

        task = asyncio.create_task(saver())

        async def writer(timeout="300ms"):
            async with pool.acquire() as writer_conn:
                await writer_conn.execute(f"SET lock_timeout = '{timeout}'")
                # what the ingest path does to a session it is about to change
                await writer_conn.fetchval(
                    "SELECT 1 FROM feedback_submissions WHERE submission_id = $1 AND tenant_code = 'mitra' FOR UPDATE", sid)

        try:
            await asyncio.wait_for(inside.wait(), 10)      # the transaction is open and holds the row lock
            with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                await writer()                              # blocked while save_extraction is between compare and commit
            await writer_conn_other_tenant(pool, sid)       # a different tenant's row is NOT blocked (the lock is per row)
        finally:
            gate.set()                                      # always release, also when an assertion above failed
            await asyncio.wait_for(task, 10)
        await writer("5s")                                  # after the commit the writer gets the lock
    _run(body)


async def writer_conn_other_tenant(pool, sid):
    async with pool.acquire() as c:
        await c.execute("SET lock_timeout = '300ms'")
        assert await c.fetchval(
            "SELECT 1 FROM feedback_submissions WHERE submission_id = $1 AND tenant_code = 'other' FOR UPDATE", sid) == 1


# ------------------------------------------------------------------------------------------------ D7 prompt version order

async def _add_prompt(conn, name, versions, analysis_type="feedback_metadata_extraction"):
    prompt_id = await conn.fetchval("INSERT INTO prompts (name, analysis_type) VALUES ($1, $2) RETURNING id", name, analysis_type)
    ids = {}
    for number, active in versions:
        ids[number] = await conn.fetchval(
            "INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id", prompt_id, number, f"S v{number}", f"U v{number}", active)
    return ids


def test_the_highest_active_prompt_version_wins_in_the_real_sql():
    async def body(conn, pool):
        ids = await _add_prompt(conn, "Order Test", [(1, True), (3, True), (2, True), (4, False)])
        async with pool.acquire() as c:
            row = await load_prompt_by_name(c, "Order Test")
        assert row["version"] == 3 and row["id"] == ids[3] and row["system_prompt"] == "S v3"     # not v1 (ASC), not inactive v4
        await conn.execute("UPDATE prompt_version SET is_active = FALSE WHERE id = $1", ids[3])
        async with pool.acquire() as c:
            assert (await load_prompt_by_name(c, "Order Test"))["version"] == 2
        await conn.execute("UPDATE prompt_version SET is_active = FALSE WHERE prompt_id = (SELECT id FROM prompts WHERE name = 'Order Test')")
        async with pool.acquire() as c:
            with pytest.raises(ExtractionSetupError, match="no active prompt version"):
                await load_prompt_by_name(c, "Order Test")
    _run(body)


def test_a_prompt_of_another_analysis_type_is_refused_by_the_real_lookup():
    async def body(conn, pool):
        await _add_prompt(conn, "Theme Like", [(1, True)], analysis_type="thematic_classification")
        async with pool.acquire() as c:
            with pytest.raises(ExtractionSetupError, match="analysis_type"):
                await load_prompt_by_name(c, "Theme Like")
    _run(body)
