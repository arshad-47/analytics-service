"""
Real-Postgres tests of the per-persona extraction engine: the config's `prompt_name` selects the prompt (highest
ACTIVE prompt_version of that exact name), a name that does not resolve fails loudly with no LLM call and no rows, a
config without the key uses the generic prompt, and the prompt_version actually used is the one recorded on the rows,
in meta_data.extraction and in llm_logs; integer fields are stored as canonical decimal strings; the CLI prints
aggregates only and `show-config` lists prompt name and field flags.

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards).
The tests build their own prompts and configs (they do not depend on the seeded field lists) and use a FAKE LLM: no
network. SYNTHETIC data only.
"""
import asyncio
import json
import uuid
from pathlib import Path

import asyncpg
import pytest
import pg_support

import app.services.llm as llm_module
from app.config import settings
from app.database.db import SEED_PROMPTS_FILE, db
from app.database.feedback_extraction_operations import extract_submission
from app.services.feedback_extraction import ExtractionConfigError
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, extraction_config_id, seed_config
PROMPT_NAME = "Feedback Metadata Extraction"   # the program-neutral template prompt of seed_prompts.sql (not a code default any more)

import importlib.util

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

_TOOL = Path(__file__).resolve().parent.parent / "tools" / "run_feedback_extraction.py"
_spec = importlib.util.spec_from_file_location("run_feedback_extraction_cli_engine_pg", _TOOL)
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

YOUTH = PERSONAS["Youth"]
YOUTH_CONFIG_ID = extraction_config_id("Youth_Voice")
MARKER = "zzq-engine-pg-marker"
CUSTOM_PROMPT = "Engine Test Youth Prompt"
SYSTEM = "CUSTOM-PROMPT for {{persona}} ({{field_names}})\n{{fields}}\nPII types:\n{{pii_types}}\nFormat:\n{{output_format}}"
USER = "BEGIN {{conversation_id}}\n{{conversation}}\nEND {{conversation_id}}"


def _config(**top):
    raw = {
        "schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": CUSTOM_PROMPT,
        "pii": {"types": ["name", "phone", "other"]},
        "fields": [
            {"name": "age", "kind": "integer", "description": "Age in years.", "min": 5, "max": 100},
            {"name": "gender", "kind": "category", "description": "Gender.", "categories": ["Male", "Female"]},
            {"name": "district", "kind": "free_text", "description": "District."},
            {"name": "disability", "kind": "category", "description": "Disability.", "categories": ["Yes", "No"],
             "sensitive": True},
        ],
    }
    raw.update(top)
    return raw


async def _set_config(conn, raw):
    """A NEW active config version for the Youth persona (history kept)."""
    await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH_CONFIG_ID)
    version = await conn.fetchval("SELECT coalesce(max(version), 0) + 1 FROM config_version WHERE config_id = $1::uuid",
                                  YOUTH_CONFIG_ID)
    await conn.execute("INSERT INTO config_version (config_id, version, is_active, config) VALUES ($1::uuid, $2, TRUE, $3::jsonb)",
                       YOUTH_CONFIG_ID, version, json.dumps(raw))
    return await conn.fetchval("SELECT id FROM config_version WHERE config_id = $1::uuid AND version = $2", YOUTH_CONFIG_ID, version)


async def _add_prompt(conn, name, versions, analysis_type="feedback_metadata_extraction"):
    """versions: [(number, is_active, tag)]; returns {number: prompt_version id}."""
    prompt_id = await conn.fetchval(
        "INSERT INTO prompts (name, analysis_type) VALUES ($1, $2) RETURNING id", name, analysis_type)
    ids = {}
    for number, active, tag in versions:
        ids[number] = await conn.fetchval(
            "INSERT INTO prompt_version (prompt_id, version, system_prompt, user_prompt, is_active) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id", prompt_id, number, SYSTEM.replace("CUSTOM-PROMPT", f"CUSTOM-PROMPT {tag}"),
            USER, active)
    return ids


def _run(body):
    """Runs `body(conn, pool)` on the scratch database with the schema, tenant 'mitra', prompts and seeds applied."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await conn.execute("DELETE FROM prompts WHERE name = $1", CUSTOM_PROMPT)
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
            await conn.execute("DELETE FROM prompts WHERE name = $1", CUSTOM_PROMPT)
            await conn.close()
    return asyncio.run(main())


async def _add_session(conn, answer="17 and a girl"):
    sid = f"x{uuid.uuid4().hex[:14]}"
    messages = [
        {"sequence": 1, "questionKey": "AGE", "isFollowUp": False, "message": "How old are you?",
         "translatedMessage": None, "messageAt": None, "parentSequence": None},
        {"sequence": 2, "questionKey": "AGE", "isFollowUp": False, "message": f"{MARKER} {answer}",
         "translatedMessage": None, "messageAt": None, "parentSequence": 1},
    ]
    event = {
        "submissionId": sid, "sessionId": f"sess-{uuid.uuid4().hex}", "submissionType": "feedback", "tenantCode": "mitra",
        "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": YOUTH["program_id"], "programName": FEEDBACK_PROGRAM_NAME, "leaderCategoryId": YOUTH["leader_category_id"],
                 "leaderCategoryName": "Youth", "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": YOUTH["session_type"], "questionnaireVersion": "V2", "language": "en",
                 "completionStatus": "COMPLETED", "botId": YOUTH["bot_id"], "submissionDate": "2026-11-10T00:00:00+00:00",
                 "messages": messages},
    }
    await insert_or_update_submission(conn, event)
    return sid


def _answer(age=17, gender="Female", district="Mysuru", disability=None):
    entry = lambda v: {"value": v, "raw": None if v is None else str(v)}  # noqa: E731
    return json.dumps({"fields": {"age": entry(age), "gender": entry(gender), "district": entry(district),
                                  "disability": entry(disability)}, "pii": {"detected": False, "types": []}})


class FakeLlm:
    def __init__(self, answer):
        self.answer, self.prompts = answer, []

    def __call__(self, prompt, model=None, max_tokens=None, timeout=None):
        self.prompts.append(prompt)
        return self.answer, {"prompt_tokens": 10, "completion_tokens": 5}


async def _rows(conn, sid):
    return {r["field_name"]: r for r in await conn.fetch(
        "SELECT * FROM feedback_extracted_data WHERE submission_id = $1", sid)}


# ----------------------------------------------------------------------------------------------------- prompt_name

def test_the_named_prompt_is_used_and_its_version_is_recorded_everywhere():
    async def body(conn, pool):
        ids = await _add_prompt(conn, CUSTOM_PROMPT, [(1, True, "v1")])
        config_version = await _set_config(conn, _config(prompt_name=CUSTOM_PROMPT))
        sid = await _add_session(conn)
        fake = FakeLlm(_answer())
        outcome = await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert len(fake.prompts) == 1 and fake.prompts[0].startswith("CUSTOM-PROMPT v1 for Youth (age, gender, district, disability)")
        assert "(sensitive: fill only when the respondent states it explicitly; never infer)" in fake.prompts[0]
        assert "1. age (a whole number from 5 to 100" in fake.prompts[0]
        rows = await _rows(conn, sid)
        assert {r["prompt_version_id"] for r in rows.values()} == {ids[1]} and outcome.prompt_version_id == ids[1]
        assert {r["config_version_id"] for r in rows.values()} == {config_version}
        meta = await conn.fetchval("SELECT meta_data->'extraction'->>'prompt_version_id' FROM feedback_submissions WHERE submission_id = $1", sid)
        assert meta == str(ids[1])
        assert await conn.fetchval("SELECT prompt_version_id::text FROM llm_logs WHERE submission_id = $1", sid) == str(ids[1])
        # integer stored as the canonical decimal string
        assert (rows["age"]["value"], rows["age"]["status"]) == ("17", "extracted")
        assert (rows["disability"]["value"], rows["disability"]["status"]) == ("Not answered", "not_provided")
    _run(body)


def test_the_highest_active_version_of_the_named_prompt_wins_and_a_new_version_needs_no_deploy():
    async def body(conn, pool):
        ids = await _add_prompt(conn, CUSTOM_PROMPT, [(1, True, "v1"), (3, False, "v3")])
        await _set_config(conn, _config(prompt_name=CUSTOM_PROMPT))
        sid = await _add_session(conn)
        fake = FakeLlm(_answer())
        await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert fake.prompts[-1].startswith("CUSTOM-PROMPT v1")
        # the documented recipe: a new prompt_version row, the previous one deactivated
        prompt_id = await conn.fetchval("SELECT id FROM prompts WHERE name = $1", CUSTOM_PROMPT)
        await conn.execute("UPDATE prompt_version SET is_active = FALSE WHERE prompt_id = $1", prompt_id)
        await conn.execute("UPDATE prompt_version SET is_active = TRUE WHERE prompt_id = $1 AND version = 3", prompt_id)
        await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert fake.prompts[-1].startswith("CUSTOM-PROMPT v3")
        rows = await _rows(conn, sid)
        assert {r["prompt_version_id"] for r in rows.values()} == {ids[3]}          # a re-run overwrites with the new version
    _run(body)


def test_a_name_that_does_not_resolve_fails_loudly_with_no_llm_call_and_no_rows():
    async def body(conn, pool):
        await _set_config(conn, _config(prompt_name="No Such Prompt"))
        sid = await _add_session(conn)
        fake = FakeLlm(_answer())
        with pytest.raises(RuntimeError, match="No prompt named 'No Such Prompt'"):
            await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert fake.prompts == []
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data") == 0
        assert await conn.fetchval("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", sid) is None
        # a prompt that exists but has no active version fails the same way (no fallback to the generic prompt)
        await _add_prompt(conn, CUSTOM_PROMPT, [(1, False, "v1")])
        await _set_config(conn, _config(prompt_name=CUSTOM_PROMPT))
        with pytest.raises(RuntimeError, match="has no active prompt version"):
            await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert fake.prompts == [] and await conn.fetchval("SELECT count(*) FROM llm_logs") == 0
    _run(body)


def test_a_config_without_prompt_name_fails_loudly_and_the_generic_prompt_is_not_used():
    async def body(conn, pool):
        raw = _config()
        del raw["prompt_name"]
        await _set_config(conn, raw)
        sid = await _add_session(conn)
        fake = FakeLlm(_answer())
        with pytest.raises(ExtractionConfigError, match="needs prompt_name"):
            await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert fake.prompts == [] and await _rows(conn, sid) == {}
        assert await conn.fetchval("SELECT count(*) FROM llm_logs") == 0
    _run(body)


def test_integer_values_are_stored_canonical_and_out_of_range_or_text_is_unmapped_with_the_raw_kept():
    async def body(conn, pool):
        await _add_prompt(conn, CUSTOM_PROMPT, [(1, True, "v1")])
        await _set_config(conn, _config(prompt_name=CUSTOM_PROMPT))
        for given, status, value in ((" 017 ", "extracted", "17"), (250, "unmapped", "Invalid response"),
                                     ("seventeen", "unmapped", "Invalid response"), (17.5, "unmapped", "Invalid response"),
                                     (None, "not_provided", "Not answered")):
            sid = await _add_session(conn)
            await extract_submission(pool, sid, "mitra", llm_function=FakeLlm(_answer(age=given)))
            age = (await _rows(conn, sid))["age"]
            assert (age["status"], age["value"]) == (status, value), given
            if status == "unmapped":
                assert age["raw_value"] is not None and age["raw_value"] != ""
    _run(body)


# ----------------------------------------------------------------------------------------------------- CLI

def _cli(capsys, monkeypatch, argv):
    monkeypatch.setattr(settings, "DATABASE_URL", DSN)
    if argv[:1] == ["run"] and "--tenant-code" not in argv:   # the tenant is required: there is no default tenant
        argv = argv + ["--tenant-code", "mitra"]
    code = cli.main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _cli_in_thread(capsys, monkeypatch, argv):
    """The CLI runs its own asyncio.run: do it on a thread so it does not nest in the running test loop."""
    return asyncio.get_running_loop().run_in_executor(None, lambda: _cli(capsys, monkeypatch, argv))


def test_cli_run_with_a_named_prompt_prints_aggregates_only(capsys, monkeypatch):
    def fake(prompt, model=None, max_tokens=None, timeout=None):
        assert prompt.startswith("CUSTOM-PROMPT v1")
        return _answer(age=17, district="Zzdistrict-secret"), {"prompt_tokens": 10, "completion_tokens": 5}
    monkeypatch.setattr(llm_module, "openrouter_chat_completion", fake)

    async def body(conn, pool):
        await _add_prompt(conn, CUSTOM_PROMPT, [(1, True, "v1")])
        await _set_config(conn, _config(prompt_name=CUSTOM_PROMPT))
        for _ in range(3):
            await _add_session(conn)
        return await _cli_in_thread(capsys, monkeypatch, ["run", "--session-type", "Youth_Voice"])

    code, out, err = _run(body)
    assert code == 0 and err == ""
    result = json.loads(out)
    assert result["submissions"]["processed"] == 3 and result["submissions"]["failed"] == 0
    assert result["fields"]["age"]["status"] == {"extracted": 3} and result["fields"]["age"]["distinct_values"] == 1
    assert (result["fields"]["age"]["min_value"], result["fields"]["age"]["max_value"]) == (17, 17)
    assert "top_values" not in result["fields"]["age"] and "top_values" not in result["fields"]["district"]
    assert MARKER not in out and "Zzdistrict" not in out and "How old" not in out


def test_cli_program_id_selects_only_the_sessions_and_configs_of_that_program(capsys, monkeypatch):
    calls = []

    def fake(prompt, model=None, max_tokens=None, timeout=None):
        calls.append(1)
        return _answer(), {"prompt_tokens": 1, "completion_tokens": 1}
    monkeypatch.setattr(llm_module, "openrouter_chat_completion", fake)

    async def body(conn, pool):
        await _add_prompt(conn, CUSTOM_PROMPT, [(1, True, "v1")])
        await _set_config(conn, _config(prompt_name=CUSTOM_PROMPT))
        await _add_session(conn)
        await _add_session(conn)
        moved = await _add_session(conn)
        parent_program = PERSONAS["Parent"]["program_id"]
        await conn.execute("UPDATE submissions SET program_id = $2::uuid WHERE submission_id = $1", moved, parent_program)
        youth = await _cli_in_thread(capsys, monkeypatch, ["run", "--dry-run", "--session-type", "Youth_Voice",
                                                           "--program-id", YOUTH["program_id"]])
        other = await _cli_in_thread(capsys, monkeypatch, ["run", "--dry-run", "--session-type", "Youth_Voice",
                                                           "--program-id", parent_program])
        shown = await _cli_in_thread(capsys, monkeypatch, ["show-config", "--session-type", "Youth_Voice",
                                                           "--program-id", parent_program])
        return youth, other, shown

    youth, other, shown = _run(body)
    assert json.loads(youth[1])["submissions"]["selected"] == 2
    assert json.loads(other[1])["submissions"]["selected"] == 1 and json.loads(other[1])["submissions"]["failed"] == 1   # no config of its own
    assert json.loads(shown[1]) == []                                      # the Youth config belongs to the Youth program
    assert len(calls) == 2                                                  # only the two Youth sessions called the model


def test_cli_run_with_an_unresolvable_prompt_name_counts_the_failure_by_type(capsys, monkeypatch):
    calls = []
    monkeypatch.setattr(llm_module, "openrouter_chat_completion", lambda *a, **k: calls.append(1))

    async def body(conn, pool):
        await _set_config(conn, _config(prompt_name="No Such Prompt"))
        await _add_session(conn)
        return await _cli_in_thread(capsys, monkeypatch, ["run", "--session-type", "Youth_Voice"])

    code, out, _ = _run(body)
    assert code == 0 and json.loads(out)["errors"] == {"ExtractionSetupError": 1} and calls == []


def test_cli_show_config_lists_the_prompt_name_and_the_field_flags(capsys, monkeypatch):
    async def body(conn, pool):
        await _add_prompt(conn, CUSTOM_PROMPT, [(1, True, "v1")])
        await _set_config(conn, _config(prompt_name=CUSTOM_PROMPT))
        named = await _cli_in_thread(capsys, monkeypatch, ["show-config", "--session-type", "Youth_Voice"])
        no_prompt = _config()
        del no_prompt["prompt_name"]
        await _set_config(conn, no_prompt)
        generic = await _cli_in_thread(capsys, monkeypatch, ["show-config", "--session-type", "Youth_Voice"])
        return named, generic

    named, generic = _run(body)
    shown = json.loads(named[1])
    assert named[0] == 0 and len(shown) == 1 and shown[0]["summary"]["prompt_name"] == CUSTOM_PROMPT
    assert shown[0]["summary"]["fields"] == [
        {"name": "age", "kind": "integer", "sensitive": False, "provisional": False, "multi": False, "versions": None, "applies_when": None},
        {"name": "gender", "kind": "category", "sensitive": False, "provisional": False, "multi": False, "versions": None, "applies_when": None},
        {"name": "district", "kind": "free_text", "sensitive": False, "provisional": False, "multi": False, "versions": None, "applies_when": None},
        {"name": "disability", "kind": "category", "sensitive": True, "provisional": False, "multi": False, "versions": None, "applies_when": None},
    ]
    assert json.loads(generic[1])[0]["summary"]["prompt_name"] == "(none)"
