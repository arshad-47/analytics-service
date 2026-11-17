"""
Tests for tools/run_feedback_extraction.py (run the feedback metadata extraction without Temporal; show-config).

Two parts: no database (the parser), and real Postgres (opt-in, FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards)): main([...]) end to end with a FAKE LLM (no network). The CLI opens its own pool with
db.connect(), which reads settings.DATABASE_URL, so the tests point that setting at the scratch database. The CLI must
print aggregates only: the synthetic marker text of the conversations never appears in its output. Synthetic data only.
"""
import asyncio
import importlib.util
import json
import uuid
from pathlib import Path

import asyncpg
import pytest
import pg_support

import app.services.llm as llm_module
from app.config import settings
from app.database.db import SEED_PROMPTS_FILE
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, extraction_config_id, extraction_config_v1, seed_config

_TOOL = Path(__file__).resolve().parent.parent / "tools" / "run_feedback_extraction.py"
_spec = importlib.util.spec_from_file_location("run_feedback_extraction_cli", _TOOL)
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

DSN = pg_support.SERVER_URL
needs_db = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

MARKER = "zzq-synthetic-marker"


# ====================================================================================== no database

def test_parser_accepts_the_documented_forms_with_defaults():
    with pytest.raises(SystemExit):   # the tenant is required: there is no default tenant
        cli.build_parser().parse_args(["run", "--session-type", "Youth_Voice"])
    args = cli.build_parser().parse_args(["run", "--session-type", "Youth_Voice", "--tenant-code", "t1"])
    assert (args.command, args.session_type, args.tenant_code, args.limit, args.submission_id) == (
        "run", "Youth_Voice", "t1", None, None)
    assert (args.only_unprocessed, args.dry_run, args.concurrency, args.top) == (False, False, 4, 15)
    args = cli.build_parser().parse_args([
        "run", "--session-type", "Parent_Voice", "--tenant-code", "t1", "--limit", "5", "--submission-id", "x1", "--only-unprocessed",
        "--dry-run", "--concurrency", "2", "--model", "m", "--max-tokens", "900", "--timeout", "30", "--top", "3"])
    assert (args.limit, args.submission_id, args.only_unprocessed, args.dry_run, args.concurrency) == (5, "x1", True, True, 2)
    assert (args.model, args.max_tokens, args.timeout, args.top) == ("m", 900, 30, 3)
    assert cli.build_parser().parse_args(["show-config"]).session_type is None
    assert cli.build_parser().parse_args(["show-config", "--session-type", "Youth_Voice"]).session_type == "Youth_Voice"


@pytest.mark.parametrize("argv", [
    [], ["bogus"], ["run"], ["run", "--session-type"], ["run", "--session-type", "Youth_Voice", "--limit", "0"],
    ["run", "--session-type", "Youth_Voice", "--concurrency", "-1"], ["run", "--session-type", "Youth_Voice", "--limit", "x"],
    ["show-config", "--dry-run"], ["show-config", "extra"],
])
def test_parser_rejects_everything_else(argv, capsys):
    with pytest.raises(SystemExit) as info:
        cli.build_parser().parse_args(argv)
    assert info.value.code == 2
    assert "usage:" in capsys.readouterr().err


# ====================================================================================== real Postgres

def _with_conn(body):
    async def main():
        conn = await asyncpg.connect(DSN)
        try:
            return await body(conn)
        finally:
            await conn.close()
    return asyncio.run(main())


def _four_field_raw(persona):
    """
    The classic four-field shape (state, district, gender, school_type) of a persona, built from its seed (the same
    helper idea as tests/test_feedback_extraction_pg.py). The seeds now hold each persona's DOTS columns and its own
    prompt; the CLI tests that only need the engine run with four fields and the program-neutral template prompt.
    """
    raw = extraction_config_v1(persona)
    by_name = {f["name"]: f for f in raw["fields"]}
    gender = by_name["gender"] if "gender" in by_name else by_name["child_gender"]
    raw["fields"] = [by_name["state"], by_name["district"], dict(gender, name="gender")]
    for school_name in ("institution_type", "school_type"):     # General Public has none
        if school_name in by_name:
            raw["fields"].append(dict(by_name[school_name], name="school_type"))
    raw["prompt_name"] = "Feedback Metadata Extraction"       # the program-neutral template prompt of seed_prompts.sql
    raw["pii"].pop("masking_prompt_name", None)               # one call per session here; the masking call has its own test
    return raw


def _reset(legacy):
    """Fresh schema, tenant, prompts and seeds; with `legacy` the four-field config is installed over the seeded one."""
    async def reset(conn):
        await conn.execute(pg_support.schema_sql())
        await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
        await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
        await conn.execute("DELETE FROM config_version")
        await conn.execute("DELETE FROM config")
        await conn.execute(SEED_PROMPTS_FILE.read_text(encoding="utf-8"))
        await seed_config(conn)
        if legacy:
            for persona_name, persona in PERSONAS.items():
                await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                                   extraction_config_id(persona["session_type"]), json.dumps(_four_field_raw(persona_name)))
    _with_conn(reset)


@pytest.fixture
def seeded(monkeypatch):
    """The scratch database with the CURRENT seeded configs (the DOTS columns of each persona); no LLM."""
    _reset(legacy=False)
    monkeypatch.setattr(settings, "DATABASE_URL", DSN)
    yield
    _with_conn(lambda conn: conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'"))


@pytest.fixture
def scratch(monkeypatch):
    """A freshly seeded scratch database (four-field configs) the CLI talks to; the LLM is a fake keyed on the conversation."""
    _reset(legacy=True)
    monkeypatch.setattr(settings, "DATABASE_URL", DSN)
    calls = []

    def fake(prompt, model=None, max_tokens=None, timeout=None):
        calls.append({"model": model, "max_tokens": max_tokens, "timeout": timeout})
        if "bad-answer" in prompt:
            return "this is not json", {"prompt_tokens": 50, "completion_tokens": 5}
        pii = "has-pii" in prompt
        state = ("Atlantis", MARKER) if "odd-state" in prompt else ("Karnataka", MARKER)
        answer = {"fields": {"state": {"value": state[0], "raw": state[1]}, "district": {"value": "Mysuru", "raw": MARKER},
                             "gender": {"value": "Female", "raw": MARKER}, "school_type": {"value": None, "raw": None}},
                  "pii": {"detected": pii, "types": ["name"] if pii else []}}
        return json.dumps(answer), {"prompt_tokens": 100, "completion_tokens": 20}

    monkeypatch.setattr(llm_module, "openrouter_chat_completion", fake)
    yield calls
    _with_conn(lambda conn: conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'"))


def _store(persona, flag="", answered=True, big=False):
    sid = f"e{uuid.uuid4().hex[:14]}"
    p = PERSONAS[persona]
    messages = [{"sequence": 1, "questionKey": "STATE_IDENTIFICATION", "isFollowUp": False,
                 "message": f"Which state? {flag}", "translatedMessage": None, "messageAt": None, "parentSequence": None}]
    if answered:
        messages.append({"sequence": 2, "questionKey": "STATE_IDENTIFICATION", "isFollowUp": False,
                         "message": f"{MARKER} {flag}" + ("x" * 20001 if big else ""), "translatedMessage": None, "messageAt": None, "parentSequence": 1})
    event = {
        "submissionId": sid, "sessionId": f"sess-{uuid.uuid4().hex}", "submissionType": "feedback", "tenantCode": "mitra",
        "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": p["program_id"], "programName": FEEDBACK_PROGRAM_NAME, "leaderCategoryId": p["leader_category_id"],
                 "leaderCategoryName": persona, "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": p["session_type"], "questionnaireVersion": "V2", "language": "en",
                 "completionStatus": "COMPLETED", "botId": p["bot_id"], "submissionDate": "2026-11-10T00:00:00+00:00",
                 "messages": messages},
    }

    async def go(conn):
        await insert_or_update_submission(conn, event)
    _with_conn(go)
    return sid


def _call(capsys, argv):
    if argv[:1] == ["run"] and "--tenant-code" not in argv:   # the tenant is required (no default tenant)
        argv = argv + ["--tenant-code", "mitra"]
    code = cli.main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _count(sql, *args):
    return _with_conn(lambda conn: conn.fetchval(sql, *args))


@needs_db
def test_the_optional_masking_call_counts_as_a_second_billed_call_in_the_aggregates(scratch, capsys, monkeypatch):
    sid = _store("Youth", "has-pii")
    raw = _four_field_raw("Youth")
    raw["pii"]["masking_prompt_name"] = "Feedback PII and Abusive-Language Detection"
    _with_conn(lambda conn: conn.execute(
        "UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
        extraction_config_id("Youth_Voice"), json.dumps(raw)))
    mark = "Analyse the following answers:\n"

    def fake(prompt, model=None, max_tokens=None, timeout=None):
        if mark in prompt:
            pairs = json.loads(prompt.split(mark, 1)[1].strip())
            return json.dumps({k: {"masked_text": "<PERSON>", "pii_found": [{"type": "PERSON"}], "abusive_language": False}
                               for k in pairs}), {"prompt_tokens": 7, "completion_tokens": 3}
        answer = {"fields": {"state": {"value": "Karnataka", "raw": MARKER}, "district": {"value": "Mysuru", "raw": MARKER},
                             "gender": {"value": "Female", "raw": MARKER}, "school_type": {"value": None, "raw": None}},
                  "pii": {"detected": False, "types": []}}
        return json.dumps(answer), {"prompt_tokens": 100, "completion_tokens": 20}

    monkeypatch.setattr(llm_module, "openrouter_chat_completion", fake)
    code, out, err = _call(capsys, ["run", "--session-type", "Youth_Voice"])
    assert code == 0 and err == "" and MARKER not in out and "<PERSON>" not in out
    result = json.loads(out)
    assert result["submissions"]["processed"] == 1 and result["submissions"]["llm_calls"] == 2
    assert result["tokens"] == {"prompt": 107, "completion": 23}
    assert result["pii"] == {"detected": 1, "not_detected": 0, "types": {"name": 1}}
    assert _count("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", sid) is True
    assert _count("SELECT count(*) FROM llm_logs") == 2


@needs_db
def test_run_writes_rows_prints_aggregates_only_and_never_respondent_text(scratch, capsys):
    clean = _store("Youth")
    flagged = _store("Youth", "has-pii")
    odd = _store("Youth", "odd-state")
    empty = _store("Youth", answered=False)
    other_persona = _store("Parent")
    code, out, err = _call(capsys, ["run", "--session-type", "Youth_Voice", "--concurrency", "2",
                                    "--model", "cli/model", "--max-tokens", "512", "--timeout", "7"])
    assert code == 0 and err == ""
    assert MARKER not in out and "Which state" not in out                      # aggregates only
    result = json.loads(out)
    assert result["session_type"] == "Youth_Voice" and result["mode"] == "write"
    assert result["submissions"] == {"selected": 4, "processed": 4, "failed": 0, "llm_calls": 3, "no_respondent_answer": 1,
                                     "oversized_respondent_message": 0, "conversation_too_long": 0}
    assert result["fields"]["state"]["status"] == {"extracted": 2, "not_provided": 1, "unmapped": 1}
    assert result["fields"]["state"]["top_values"] == {"Karnataka": 2}
    assert result["fields"]["school_type"]["status"] == {"not_provided": 4}
    assert result["fields"]["gender"]["top_values"] == {"Female": 3}
    # a free-text field: only the status counts and the NUMBER of distinct values, never the values the model copied
    assert result["fields"]["district"]["status"] == {"extracted": 3, "not_provided": 1}
    assert result["fields"]["district"]["distinct_values"] == 1 and "top_values" not in result["fields"]["district"]
    assert "Mysuru" not in out
    assert result["pii"] == {"detected": 1, "not_detected": 3, "types": {"name": 1}}
    assert result["tokens"] == {"prompt": 300, "completion": 60}
    assert {(c["model"], c["max_tokens"], c["timeout"]) for c in scratch} == {("cli/model", 512, 7)}
    assert _count(
        "SELECT count(*) FROM feedback_extracted_data d JOIN feedback_submissions fs "
        "ON fs.submission_id = d.submission_id AND fs.tenant_code = d.tenant_code WHERE fs.session_type = 'Youth_Voice'") == 16
    assert _count("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", flagged) is True
    assert _count("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", empty) is False
    assert _count("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", other_persona) is None
    assert _count("SELECT count(*) FROM llm_logs") == 3 and clean != odd


@needs_db
def test_only_unprocessed_limit_and_submission_id_select_the_right_sessions(scratch, capsys):
    ids = [_store("Youth") for _ in range(3)]
    code, out, _ = _call(capsys, ["run", "--session-type", "Youth_Voice", "--limit", "2"])
    assert json.loads(out)["submissions"]["selected"] == 2
    code, out, _ = _call(capsys, ["run", "--session-type", "Youth_Voice", "--only-unprocessed"])
    assert json.loads(out)["submissions"]["selected"] == 1                         # the one without pii_detected
    code, out, _ = _call(capsys, ["run", "--session-type", "Youth_Voice", "--only-unprocessed"])
    assert json.loads(out)["submissions"] == {"selected": 0, "processed": 0, "failed": 0, "llm_calls": 0, "no_respondent_answer": 0,
                                              "oversized_respondent_message": 0, "conversation_too_long": 0}
    code, out, _ = _call(capsys, ["run", "--session-type", "Youth_Voice", "--submission-id", ids[0]])
    assert json.loads(out)["submissions"]["selected"] == 1
    # a re-run overwrites: still one row per field per session
    assert _count("SELECT count(*) FROM feedback_extracted_data") == 12
    code, out, _ = _call(capsys, ["run", "--session-type", "Nobody_Voice"])
    assert code == 0 and json.loads(out)["submissions"]["selected"] == 0


@needs_db
def test_dry_run_prints_aggregates_and_writes_nothing(scratch, capsys):
    _store("Youth")
    _store("Youth", "has-pii")
    code, out, _ = _call(capsys, ["run", "--session-type", "Youth_Voice", "--dry-run"])
    result = json.loads(out)
    assert code == 0 and result["mode"].startswith("dry_run")
    assert result["submissions"]["llm_calls"] == 2 and result["pii"]["detected"] == 1
    assert len(scratch) == 2
    assert _count("SELECT count(*) FROM feedback_extracted_data") == 0
    assert _count("SELECT count(*) FROM llm_logs") == 0
    assert _count("SELECT count(*) FROM feedback_submissions WHERE pii_detected IS NOT NULL") == 0


@needs_db
def test_a_failing_session_is_counted_by_error_type_and_the_others_still_run(scratch, capsys):
    _store("Youth")
    bad = _store("Youth", "bad-answer")
    code, out, err = _call(capsys, ["run", "--session-type", "Youth_Voice"])
    result = json.loads(out)
    assert code == 0
    assert result["submissions"]["processed"] == 1 and result["submissions"]["failed"] == 1
    assert result["errors"] == {"ExtractionOutputError": 1}
    assert MARKER not in out and "this is not json" not in out
    assert _count("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", bad) is None
    assert _count("SELECT count(*) FROM llm_logs WHERE status = 'failed'") == 1


@needs_db
def test_show_config_prints_the_active_versions_for_every_persona_or_one(seeded, capsys):
    code, out, err = _call(capsys, ["show-config"])
    assert code == 0 and err == ""
    shown = json.loads(out)
    expected = {p["session_type"]: extraction_config_v1(name) for name, p in PERSONAS.items()}
    assert [c["session_type"] for c in shown] == sorted(expected)
    assert all(c["config_source"] == "any" and c["version"] == 1 for c in shown)
    for entry in shown:
        seed = expected[entry["session_type"]]
        # the summary follows the seeded config: its prompt name and every field with kind and flags
        assert entry["summary"]["prompt_name"] == seed["prompt_name"]
        assert entry["summary"]["fields"] == [
            {"name": f["name"], "kind": f["kind"], "sensitive": f.get("sensitive", False) is True,
             "provisional": f.get("provisional", False) is True} for f in seed["fields"]]
        assert [f["name"] for f in entry["config"]["fields"]] == [f["name"] for f in seed["fields"]]
        assert "categories" not in json.dumps(entry["summary"])           # the summary carries no band lists
    code, out, _ = _call(capsys, ["show-config", "--session-type", "Parent_Voice"])
    assert [c["session_type"] for c in json.loads(out)] == ["Parent_Voice"]


@needs_db
def test_show_config_names_the_template_prompt_of_a_four_field_config(scratch, capsys):
    code, out, _ = _call(capsys, ["show-config", "--session-type", "Youth_Voice"])
    summary = json.loads(out)[0]["summary"]
    assert code == 0 and summary["prompt_name"] == "Feedback Metadata Extraction"
    assert [f["name"] for f in summary["fields"]] == ["state", "district", "gender", "school_type"]


@needs_db
def test_an_oversized_respondent_message_is_counted_flagged_and_never_sent(scratch, capsys):
    big = _store("Youth", big=True)
    _store("Youth")
    code, out, _ = _call(capsys, ["run", "--session-type", "Youth_Voice"])
    result = json.loads(out)
    assert code == 0 and result["submissions"]["oversized_respondent_message"] == 1
    assert result["submissions"]["llm_calls"] == 1 and len(scratch) == 1
    assert result["pii"]["detected"] == 1 and result["pii"]["types"] == {"other": 1}
    assert MARKER not in out and "xxxxx" not in out
    assert _count("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", big) is True


@needs_db
def test_a_provider_error_is_counted_by_type_and_its_text_never_printed_or_stored(scratch, capsys, monkeypatch):
    _store("Youth")

    def down(prompt, **kwargs):
        raise RuntimeError(f"OpenRouter request failed with HTTP 500: echo {MARKER} 5550100123")
    monkeypatch.setattr(llm_module, "openrouter_chat_completion", down)
    code, out, err = _call(capsys, ["run", "--session-type", "Youth_Voice"])
    assert code == 0 and json.loads(out)["errors"] == {"LlmCallError": 1}
    assert MARKER not in out + err and "5550100123" not in out + err
    message = _count("SELECT error_message FROM llm_logs WHERE status = 'failed'")
    assert message == "LlmCallError: LLM call failed (RuntimeError, HTTP 500)"
