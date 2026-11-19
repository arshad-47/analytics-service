"""
Real-Postgres tests of the per-persona extraction content: the seeds (one config and one prompt per persona, each
config's `prompt_name` resolves), the seeds never overwrite an edited config or prompt, and one whole extraction
per persona with a FAKE LLM. The configs come from seed_config.sql (tests/seed_support.py).

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards).
No network, no real LLM. SYNTHETIC data only.
"""
import asyncio
import json
import uuid

import asyncpg
import pytest
import pg_support

from app.database.db import SEED_PROMPTS_FILE, db
from app.database.feedback_extraction_operations import extract_submission, load_prompt_for_config
from app.database.operations import insert_or_update_submission
from app.services import feedback_extraction as fx
PROMPT_NAME = "Feedback Metadata Extraction"   # the program-neutral template prompt of seed_prompts.sql (not a code default any more)
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, extraction_config_id, extraction_config_v1, seed_config

# The persona prompts of seed_prompts.sql: the generic name, " - ", the persona name.
PERSONA_PROMPT_NAMES = {persona: f"{PROMPT_NAME} - {persona}" for persona in PERSONAS}

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")


def _run(body):
    """Runs `body(conn, pool)` on the scratch database: schema, tenant 'mitra', the real prompts and seeds, fresh each time."""
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
            await conn.execute("DELETE FROM prompts WHERE analysis_type = 'feedback_metadata_extraction'")
            await conn.execute(SEED_PROMPTS_FILE.read_text(encoding="utf-8"))
            await seed_config(conn)
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


def _as_dict(value):
    return json.loads(value) if isinstance(value, str) else value


async def _versions(conn, session_type):
    rows = await conn.fetch(
        "SELECT cv.version, cv.is_active, cv.config, cv.created_by FROM config_version cv "
        "WHERE cv.config_id = $1::uuid ORDER BY cv.version", extraction_config_id(session_type))
    return [{"version": r["version"], "is_active": r["is_active"], "config": _as_dict(r["config"]),
             "created_by": r["created_by"]} for r in rows]


async def _write_old_version(conn, session_type, config):
    """Makes `config` the active version (a new version row; the previous one stays as history)."""
    config_id = extraction_config_id(session_type)
    latest = await conn.fetchval("SELECT max(version) FROM config_version WHERE config_id = $1::uuid", config_id)
    await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", config_id)
    await conn.execute("INSERT INTO config_version (config_id, version, is_active, config) VALUES ($1::uuid, $2, TRUE, $3::jsonb)",
                       config_id, latest + 1, json.dumps(config))


# ---------------------------------------------------------------------------------------------- seeds

def test_seed_writes_one_active_config_and_one_prompt_per_persona_and_every_prompt_name_resolves():
    async def body(conn, pool):
        for persona, p in PERSONAS.items():
            versions = await _versions(conn, p["session_type"])
            assert [(v["version"], v["is_active"]) for v in versions] == [(1, True)]
            assert versions[0]["config"] == extraction_config_v1(persona)
            config = fx.parse_extraction_config(versions[0]["config"])
            resolved = await load_prompt_for_config(conn, config)       # no fallback: the exact name must resolve
            assert resolved["name"] == PERSONA_PROMPT_NAMES[persona] and resolved["version"] == 1
            assert "{{fields}}" in resolved["system_prompt"] and "{{conversation}}" in resolved["user_prompt"]
            assert persona in resolved["system_prompt"].split("\n")[0]
        rows = await conn.fetch("SELECT name, analysis_type FROM prompts WHERE analysis_type = 'feedback_metadata_extraction'")
        assert {r["name"] for r in rows} == {PROMPT_NAME, *PERSONA_PROMPT_NAMES.values()}
        assert await conn.fetchval(
            "SELECT count(*) FROM prompt_version pv JOIN prompts p ON p.id = pv.prompt_id "
            "WHERE p.analysis_type = 'feedback_metadata_extraction' AND pv.is_active AND pv.version = 1") == 5
    _run(body)


def test_reseeding_never_overwrites_an_edited_persona_prompt_or_an_edited_config():
    async def body(conn, pool):
        name = PERSONA_PROMPT_NAMES["Youth"]
        await conn.execute(
            "UPDATE prompt_version SET system_prompt = 'EDITED {{fields}}' "
            "WHERE prompt_id = (SELECT id FROM prompts WHERE name = $1) AND version = 1", name)
        edited = extraction_config_v1("Youth")
        edited["fields"] = edited["fields"][:3]
        await _write_old_version(conn, "Youth_Voice", edited)
        for _ in range(2):
            await conn.execute(SEED_PROMPTS_FILE.read_text(encoding="utf-8"))
            await seed_config(conn)
        text = await conn.fetchval(
            "SELECT system_prompt FROM prompt_version WHERE prompt_id = (SELECT id FROM prompts WHERE name = $1) AND version = 1", name)
        assert text == "EDITED {{fields}}"
        assert await conn.fetchval("SELECT count(*) FROM prompt_version WHERE prompt_id = (SELECT id FROM prompts WHERE name = $1)", name) == 1
        versions = await _versions(conn, "Youth_Voice")
        assert [(v["version"], v["is_active"]) for v in versions] == [(1, False), (2, True)]
        assert versions[1]["config"] == edited
        # the other personas' prompts are untouched by the edit
        assert await conn.fetchval(
            "SELECT count(*) FROM prompt_version WHERE system_prompt LIKE 'EDITED%'") == 1
    _run(body)


# ---------------------------------------------------------------------------------------------- whole extraction

class FakeLlm:
    def __init__(self, answer):
        self.answer = answer
        self.calls = []

    def __call__(self, prompt, model=None, max_tokens=None, timeout=None):
        self.calls.append({"prompt": prompt})
        return self.answer, {"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130, "cost": 0.0}


async def _add(conn, persona, messages):
    p = PERSONAS[persona]
    sid = f"x{uuid.uuid4().hex[:14]}"
    event = {
        "submissionId": sid, "sessionId": f"sess-{uuid.uuid4().hex}", "submissionType": "feedback", "tenantCode": "mitra",
        "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": p["program_id"], "programName": FEEDBACK_PROGRAM_NAME, "leaderCategoryId": p["leader_category_id"],
                 "leaderCategoryName": persona, "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": p["session_type"], "questionnaireVersion": "V1", "language": "en",
                 "completionStatus": "COMPLETED", "submissionDate": "2026-11-10T00:00:00+00:00", "messages": messages,
                 "botId": p["bot_id"]},
    }
    await insert_or_update_submission(conn, event)
    return sid


def _messages():
    def row(seq, text, key, parent):
        return {"sequence": seq, "questionKey": key, "isFollowUp": False, "message": text, "translatedMessage": None,
                "messageAt": None, "parentSequence": parent}
    return [row(1, "How old are you?", "AGE_IDENTIFICATION", None), row(2, "fourteen", "AGE_IDENTIFICATION", 1)]


def _with_masking(extraction_fake):
    """Answers the masking prompt (every answer unchanged, nothing found) and passes the rest to `extraction_fake`."""
    mark = "Analyse the following answers:\n"

    def call(prompt, model=None, max_tokens=None, timeout=None):
        if mark in prompt:
            pairs = json.loads(prompt.split(mark, 1)[1].strip())
            return json.dumps({k: {"masked_text": v["answer"], "pii_found": [], "abusive_language": False}
                               for k, v in pairs.items()}), {"prompt_tokens": 1, "completion_tokens": 1}
        return extraction_fake(prompt, model=model, max_tokens=max_tokens, timeout=timeout)
    return call


def test_each_persona_extracts_with_its_own_prompt_and_stores_every_dots_field_and_the_integer_as_a_decimal_string():
    async def body(conn, pool):
        for persona, p in PERSONAS.items():
            config = fx.parse_extraction_config(extraction_config_v1(persona))
            # the sessions of this test are stamped V1: the model is asked (and answers) only the fields V1 asks
            values = {f.name: {"value": None, "raw": None} for f in config.fields_for("V1")}
            if "age" in values:
                values["age"] = {"value": "014.5", "raw": "fourteen and a half"}      # the model's text; stored as its band
            values["state"] = {"value": "Kerala", "raw": "kerala"}
            answer = json.dumps({"fields": values, "pii": {"detected": False, "types": []}})
            sid = await _add(conn, persona, _messages())
            fake = FakeLlm(answer)
            await extract_submission(pool, sid, "mitra", llm_function=_with_masking(fake))
            # the seeded config switches the masking call on: it comes first, then the persona's own extraction prompt
            assert len(fake.calls) == 1
            assert f"Feedback Metadata Extraction and PII Check - {persona}" in fake.calls[0]["prompt"]
            rows = {r["field_name"]: r for r in await conn.fetch(
                "SELECT * FROM feedback_extracted_data WHERE submission_id = $1", sid)}
            assert set(rows) == set(config.field_names())
            assert rows["state"]["value"] == "Kerala" and rows["state"]["status"] == "extracted"
            if "age" in rows:
                expected_band = "≥ 12 and < 16" if persona == "Youth" else "≥ 12 and < 26"
                assert (rows["age"]["value"], rows["age"]["status"], rows["age"]["raw_value"]) == (
                    expected_band, "extracted", "fourteen and a half")
                assert list(rows["age"]["value_list"]) == [expected_band]
            if persona == "Youth":      # one monthly income field for every version: not answered here, so not_provided
                assert (rows["monthly_household_income"]["value"], rows["monthly_household_income"]["status"]) == ("Not answered", "not_provided")
                assert "annual_household_income" not in rows
                assert rows["disability_type"]["value"] == "N/A"       # no disability answer: the type does not apply
            prompt_version = await conn.fetchval(
                "SELECT pv.id FROM prompt_version pv JOIN prompts pr ON pr.id = pv.prompt_id WHERE pr.name = $1",
                PERSONA_PROMPT_NAMES[persona])
            assert {r["prompt_version_id"] for r in rows.values()} == {prompt_version}
    _run(body)
