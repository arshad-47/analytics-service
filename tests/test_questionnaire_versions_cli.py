"""
Tests for tools/questionnaire_versions.py (the operator CLI that records questionnaire versions as data).

Two parts:
* no database: build_parser() accepts every documented form and rejects the rest, argparse errors exit with
  code 2 before anything runs, and main() wiring (JSON output, `error: ...` + exit code 1 for a ValueError, the
  arguments handed to the DB layer, created_by = getpass.getuser()) with the DB layer replaced by fakes;
* real Postgres (opt-in, FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards)):
  main([...]) end to end. The CLI opens its own pool with db.connect() / db.disconnect(), which reads
  settings.DATABASE_URL, so the tests point that setting at the scratch database. Every test starts from a fresh
  seed (config_version 1, only V1, for every persona). Synthetic data only.
"""
import asyncio
import getpass
import importlib.util
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pytest
import factories
import pg_support

from app.config import settings
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, seed_config

_TOOL = Path(__file__).resolve().parent.parent / "tools" / "questionnaire_versions.py"
_spec = importlib.util.spec_from_file_location("questionnaire_versions_cli", _TOOL)
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

DSN = pg_support.SERVER_URL
needs_db = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

V2_TEXT = "2026-11-03T18:23:03+05:30"
V2_UTC = datetime(2026, 11, 3, 12, 53, 3, tzinfo=timezone.utc)
V3_TEXT = "2026-12-01T10:30:00+05:30"
ST_ALL = ["Educator_Voice", "General_Public_Voice", "Parent_Voice", "Youth_Voice"]


# ====================================================================================== no database

SET_ARGS = ["set", "--session-type", "Youth_Voice", "--key", "V3", "--effective-from", V3_TEXT]


@pytest.mark.parametrize("argv,expected", [
    (["list"], {"command": "list", "session_type": None}),
    (["list", "--session-type", "Youth_Voice"], {"command": "list", "session_type": "Youth_Voice"}),
    (SET_ARGS, {"command": "set", "session_type": "Youth_Voice", "key": "V3", "effective_from": V3_TEXT,
                "note": None, "no_restamp": False, "dry_run": False}),
    (SET_ARGS + ["--note", "third edit"], {"command": "set", "note": "third edit", "no_restamp": False, "dry_run": False}),
    (SET_ARGS + ["--no-restamp"], {"command": "set", "no_restamp": True, "dry_run": False}),
    (SET_ARGS + ["--dry-run"], {"command": "set", "no_restamp": False, "dry_run": True}),
    (SET_ARGS + ["--note", "n", "--no-restamp", "--dry-run"], {"note": "n", "no_restamp": True, "dry_run": True}),
    (["set", "--effective-from", V3_TEXT, "--key", "V3", "--session-type", "Parent_Voice"],  # any order
     {"command": "set", "session_type": "Parent_Voice", "key": "V3"}),
    (["restamp"], {"command": "restamp", "session_type": None, "dry_run": False}),
    (["restamp", "--session-type", "Parent_Voice"], {"command": "restamp", "session_type": "Parent_Voice", "dry_run": False}),
    (["restamp", "--dry-run"], {"command": "restamp", "session_type": None, "dry_run": True}),
    (["restamp", "--session-type", "Educator_Voice", "--dry-run"],
     {"command": "restamp", "session_type": "Educator_Voice", "dry_run": True}),
])
def test_build_parser_accepts_every_documented_form(argv, expected):
    args = cli.build_parser().parse_args(argv)
    for name, value in expected.items():
        assert getattr(args, name) == value, (argv, name)


@pytest.mark.parametrize("argv", [
    [],                                                                   # no sub-command
    ["bogus"],
    ["list", "extra"],                                                    # stray positional
    ["list", "--session-type"],                                           # option without a value
    ["list", "--dry-run"],                                                # list has no dry run
    ["list", "--key", "V2"],
    ["set"],
    ["set", "--key", "V3", "--effective-from", V3_TEXT],                  # missing --session-type
    ["set", "--session-type", "Youth_Voice", "--effective-from", V3_TEXT],  # missing --key
    ["set", "--session-type", "Youth_Voice", "--key", "V3"],              # missing --effective-from
    ["set", "--session-type", "Youth_Voice", "--key", "V3", "--effective-from"],  # no value
    SET_ARGS + ["--restamp"],                                             # only --no-restamp exists
    SET_ARGS + ["--unknown"],
    ["restamp", "--no-restamp"],
    ["restamp", "--note", "x"],
    ["restamp", "--key", "V2"],
    ["--dry-run", "restamp"],                                             # sub-command options go after it
    ["--session-type", "Youth_Voice", "list"],
])
def test_build_parser_rejects_everything_else(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.build_parser().parse_args(argv)
    assert exc.value.code == 2
    assert "usage:" in capsys.readouterr().err


def test_main_exits_with_2_on_argparse_errors_before_running_anything(monkeypatch, capsys):
    async def boom(args):
        raise AssertionError("the CLI must not start before the arguments are valid")
    monkeypatch.setattr(cli, "_run", boom)
    for argv in ([], ["set", "--session-type", "Youth_Voice", "--effective-from", V3_TEXT],      # missing --key
                 ["set", "--key", "V3", "--effective-from", V3_TEXT],                              # missing --session-type
                 ["set", "--session-type", "Youth_Voice", "--key", "V3"],                          # missing date
                 ["nope"]):
        with pytest.raises(SystemExit) as exc:
            cli.main(argv)
        assert exc.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == "" and "required" in captured.err


PROGRAM_ID = "7b3c1a2e-0000-4000-8000-000000000001"


class _FakeDb:
    def __init__(self):
        self.events = []

    async def connect(self):
        self.events.append("connect")

    async def disconnect(self):
        self.events.append("disconnect")


@pytest.fixture
def fake_layer(monkeypatch):
    """Replaces the DB pool handling and the three operations of the module under test; records every call."""
    fake_db = _FakeDb()
    calls = []

    async def listing(session_type=None, **selectors):
        calls.append(("list", session_type) if all(v is None for v in selectors.values())
                     else ("list", session_type, selectors))
        return {"Youth_Voice": {"config_version": 1, "versions": [{"key": "V1", "effective_from": None}], "sessions": {}}}

    async def restamp(session_type=None, dry_run=False, **selectors):
        calls.append(("restamp", session_type, dry_run) if all(v is None for v in selectors.values())
                     else ("restamp", session_type, dry_run, selectors))
        return {"session_types": {}, "changed": 0, "dry_run": dry_run}

    async def setting(session_type, key, effective_from, **kwargs):
        calls.append(("set", session_type, key, effective_from, kwargs))
        if key == "BAD":
            raise ValueError("effective_from 'x' needs a UTC offset")
        return {"session_type": session_type, "changed": True, "zeta": 1, "alpha": datetime(2026, 1, 2, tzinfo=timezone.utc)}

    monkeypatch.setattr(cli, "db", fake_db)
    monkeypatch.setattr(cli.feedback_operations, "list_questionnaire_versions", listing)
    monkeypatch.setattr(cli.feedback_operations, "restamp_questionnaire_versions", restamp)
    monkeypatch.setattr(cli.feedback_operations, "set_questionnaire_version", setting)
    monkeypatch.setattr(getpass, "getuser", lambda: "operator-1")
    return fake_db, calls


def test_main_wiring_with_the_db_layer_faked(fake_layer, capsys):
    fake_db, calls = fake_layer
    assert cli.main(["list"]) == 0
    assert cli.main(["list", "--session-type", "Youth_Voice"]) == 0
    assert cli.main(["restamp", "--dry-run"]) == 0
    assert cli.main(["restamp", "--session-type", "Parent_Voice"]) == 0
    assert cli.main(SET_ARGS) == 0
    assert cli.main(SET_ARGS + ["--note", "why", "--no-restamp", "--dry-run"]) == 0
    assert cli.main(SET_ARGS + ["--tenant-code", "abc", "--program-name", "PTM", "--source", "ptm_app"]) == 0
    assert cli.main(["list", "--tenant-code", "abc", "--program-id", PROGRAM_ID, "--source", "ptm_app"]) == 0
    none = {"tenant_code": None, "program_id": None, "program_name": None, "source": None}
    assert calls == [
        ("list", None), ("list", "Youth_Voice"), ("restamp", None, True), ("restamp", "Parent_Voice", False),
        ("set", "Youth_Voice", "V3", V3_TEXT,
         {**none, "created_by": "operator-1", "change_note": None, "restamp": True, "dry_run": False}),
        ("set", "Youth_Voice", "V3", V3_TEXT,
         {**none, "created_by": "operator-1", "change_note": "why", "restamp": False, "dry_run": True}),
        ("set", "Youth_Voice", "V3", V3_TEXT,
         {"tenant_code": "abc", "program_id": None, "program_name": "PTM", "source": "ptm_app",
          "created_by": "operator-1", "change_note": None, "restamp": True, "dry_run": False}),
        ("list", None, {"tenant_code": "abc", "program_id": PROGRAM_ID, "program_name": None, "source": "ptm_app"}),
    ]
    assert fake_db.events == ["connect", "disconnect"] * 8   # the pool is opened and closed around every command
    out = capsys.readouterr().out
    assert out.count("\n  ") > 0                              # indented JSON


def test_detect_parser_defaults_and_options():
    args = cli.build_parser().parse_args(["detect"])
    assert (args.command, args.session_type, args.min_sessions, args.baseline_sessions, args.min_coverage) == \
        ("detect", None, 20, 100, 0.3)
    assert args.include_wording is False and cli.build_parser().parse_args(["detect", "--include-wording"]).include_wording is True
    args = cli.build_parser().parse_args(["detect", "--session-type", "Youth_Voice", "--min-sessions", "5",
                                          "--baseline-sessions", "50", "--min-coverage", "0.5"])
    assert (args.session_type, args.min_sessions, args.baseline_sessions, args.min_coverage) == ("Youth_Voice", 5, 50, 0.5)
    for bad in (["detect", "--dry-run"], ["detect", "--min-sessions", "x"], ["detect", "--key", "V2"]):
        with pytest.raises(SystemExit):
            cli.build_parser().parse_args(bad)


def test_detect_is_wired_to_the_read_only_operation_and_prints_its_result(monkeypatch, capsys):
    fake_db = _FakeDb()
    calls = []

    async def detecting(session_type=None, **kwargs):
        calls.append((session_type, kwargs))
        if kwargs["min_sessions"] == 0:
            raise ValueError("min_sessions and baseline_sessions must be at least 1")
        return {"Youth_Voice": {"proposal": None, "reason": "no new question found"}}

    monkeypatch.setattr(cli, "db", fake_db)
    monkeypatch.setattr(cli.feedback_operations, "detect_questionnaire_versions", detecting)
    assert cli.main(["detect", "--session-type", "Youth_Voice", "--min-sessions", "7"]) == 0
    assert calls == [("Youth_Voice", {"min_sessions": 7, "baseline_sessions": 100, "min_coverage": 0.3,
                                      "include_wording": False, "tenant_code": None, "program_id": None,
                                      "program_name": None, "source": None})]
    assert json.loads(capsys.readouterr().out)["Youth_Voice"]["proposal"] is None
    assert cli.main(["detect", "--min-sessions", "0"]) == 1
    assert capsys.readouterr().err.startswith("error: min_sessions")
    assert fake_db.events == ["connect", "disconnect"] * 2


def test_main_prints_sorted_indented_json_and_stringifies_unknown_types(fake_layer, capsys):
    assert cli.main(SET_ARGS) == 0
    out = capsys.readouterr().out
    assert out.endswith("}\n")
    parsed = json.loads(out)
    assert out == json.dumps(parsed, indent=2, sort_keys=True) + "\n"
    assert list(parsed) == sorted(parsed) and parsed["alpha"] == "2026-01-02 00:00:00+00:00"


def test_main_turns_a_value_error_into_an_error_line_and_exit_code_1(fake_layer, capsys):
    fake_db, _ = fake_layer
    args = ["set", "--session-type", "Youth_Voice", "--key", "BAD", "--effective-from", "x"]
    assert cli.main(args) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "error: effective_from 'x' needs a UTC offset\n"
    assert fake_db.events == ["connect", "disconnect"]       # the pool is closed on the error path, too


def test_main_does_not_swallow_other_errors(fake_layer, monkeypatch):
    async def broken(session_type=None, **selectors):
        raise RuntimeError("database is down")
    monkeypatch.setattr(cli.feedback_operations, "list_questionnaire_versions", broken)
    with pytest.raises(RuntimeError, match="database is down"):
        cli.main(["list"])


# ====================================================================================== real Postgres

def _with_conn(body):
    async def main():
        conn = await asyncpg.connect(DSN)
        try:
            return await body(conn)
        finally:
            await conn.close()
    return asyncio.run(main())


@pytest.fixture
def scratch(monkeypatch):
    """A freshly seeded scratch database that the CLI (db.connect() -> settings.DATABASE_URL) talks to."""
    async def reset(conn):
        await conn.execute(pg_support.schema_sql())
        await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
        await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
        await conn.execute("DELETE FROM config_version")
        await conn.execute("DELETE FROM config")
        await seed_config(conn)
    _with_conn(reset)
    monkeypatch.setattr(settings, "DATABASE_URL", DSN)
    monkeypatch.setattr(getpass, "getuser", lambda: "test-operator")
    yield
    _with_conn(lambda conn: conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'"))


def _store(persona, stored_version, started_at):
    """Stores one Mitra session through the real ingest path; returns its submission id."""
    sid = f"c{uuid.uuid4().hex[:14]}"
    event = {
        "submissionId": sid, "sessionId": f"sess-{uuid.uuid4().hex}", "submissionType": "feedback",
        "tenantCode": "mitra", "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": PERSONAS[persona]["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": PERSONAS[persona]["leader_category_id"], "leaderCategoryName": persona,
                 "state": None, "district": None, "source": "mitra"},
        "data": {"source": "mitra", "adapter": "conversation_rows", "sessionType": PERSONAS[persona]["session_type"],
                 "questionnaireVersion": stored_version, "language": "en", "completionStatus": "COMPLETED",
                 "botId": PERSONAS[persona]["bot_id"], "submissionDate": started_at.isoformat(),
                 "messages": [
                     {"sequence": 1, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "How old are you?",
                      "translatedMessage": None, "messageAt": None, "parentSequence": None},
                     {"sequence": 2, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "17",
                      "translatedMessage": None, "messageAt": None, "parentSequence": 1}]},
    }

    async def go(conn):
        await insert_or_update_submission(conn, event)
        # the CLI tests are about the date windows, which only apply to sessions without a bot id
        await factories.as_window_stamped_session(conn, sid, stored_version)
    _with_conn(go)
    return sid


def _labels():
    async def go(conn):
        rows = await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")
        return {r["submission_id"]: r["questionnaire_version"] for r in rows}
    return _with_conn(go)


def _config_versions(persona):
    async def go(conn):
        rows = await conn.fetch(
            "SELECT version, is_active, config, created_by, change_note FROM config_version "
            "WHERE config_id = $1::uuid ORDER BY version", PERSONAS[persona]["config_id"])
        return [{"version": r["version"], "is_active": r["is_active"], "created_by": r["created_by"],
                 "change_note": r["change_note"], "keys": [(e["key"], e["effective_from"])
                                                         for e in json.loads(r["config"])["questionnaire_versions"]]}
                for r in rows]
    return _with_conn(go)


async def _all_state(conn):
    ver = [tuple(r.values()) for r in await conn.fetch("SELECT * FROM config_version ORDER BY config_id, version")]
    fs = [tuple(r.values()) for r in await conn.fetch(
        "SELECT submission_id, questionnaire_version, updated_at FROM feedback_submissions ORDER BY submission_id")]
    return ver, fs


def _state():
    return _with_conn(_all_state)


def _call(capsys, argv):
    code = cli.main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


@needs_db
def test_list_prints_json_and_returns_0(scratch, capsys):
    _store("Youth", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    code, out, err = _call(capsys, ["list"])
    assert code == 0 and err == ""
    listed = json.loads(out)
    assert list(listed) == ST_ALL
    assert all(v["config_version"] == 1 and v["versions"] == [{"key": "V1", "effective_from": None}]
               for v in listed.values())
    assert listed["Youth_Voice"]["sessions"] == {"V1": 1} and listed["Parent_Voice"]["sessions"] == {}
    assert out == json.dumps(listed, indent=2, sort_keys=True) + "\n"

    code, out, err = _call(capsys, ["list", "--session-type", "Parent_Voice"])
    assert code == 0 and list(json.loads(out)) == ["Parent_Voice"]
    code, out, err = _call(capsys, ["list", "--session-type", "Nobody_Voice"])
    assert code == 0 and json.loads(out) == {}


@needs_db
def test_set_records_the_version_relabels_and_stores_created_by(scratch, capsys):
    before_v2 = _store("Youth", "V1", datetime(2026, 11, 1, 9, 0, 0, tzinfo=timezone.utc))
    after_v2 = _store("Youth", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    code, out, err = _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V2", "--effective-from", V2_TEXT])
    assert code == 0 and err == ""
    res = json.loads(out)
    assert res["changed"] is True and res["dry_run"] is False and res["config_version"] == 2
    assert res["session_type"] == "Youth_Voice"
    assert res["versions"] == [{"key": "V1", "effective_from": None}, {"key": "V2", "effective_from": V2_TEXT}]
    assert res["restamp"] == {"sessions": 2, "changed": 1, "versions": {"V1": 1, "V2": 1}}
    labels = _labels()
    assert (labels[before_v2], labels[after_v2]) == ("V1", "V2")
    rows = _config_versions("Youth")
    assert [(r["version"], r["is_active"]) for r in rows] == [(1, False), (2, True)]
    assert rows[1]["created_by"] == "test-operator"                      # getpass.getuser(), monkeypatched
    assert rows[1]["change_note"] == f"questionnaire version V2 effective from {V2_TEXT}"
    assert rows[1]["keys"] == [("V1", None), ("V2", V2_TEXT)]

    # appending V3 through the CLI as well (no code involved)
    code, out, _ = _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V3", "--effective-from", V3_TEXT])
    assert code == 0 and json.loads(out)["config_version"] == 3
    rows = _config_versions("Youth")
    assert [(r["version"], r["is_active"]) for r in rows] == [(1, False), (2, False), (3, True)]
    assert rows[2]["keys"] == [("V1", None), ("V2", V2_TEXT), ("V3", V3_TEXT)]


@needs_db
def test_set_stores_the_note(scratch, capsys):
    code, out, _ = _call(capsys, ["set", "--session-type", "Educator_Voice", "--key", "V2",
                                  "--effective-from", "2026-11-05T00:00:00Z", "--note", "bot questions rewritten"])
    assert code == 0 and json.loads(out)["versions"][1]["effective_from"] == "2026-11-05T00:00:00+00:00"
    rows = _config_versions("Educator")
    assert rows[1]["change_note"] == "bot questions rewritten" and rows[1]["created_by"] == "test-operator"


@needs_db
def test_set_no_restamp_leaves_the_labels(scratch, capsys):
    sid = _store("Youth", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    code, out, _ = _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V2",
                                  "--effective-from", V2_TEXT, "--no-restamp"])
    res = json.loads(out)
    assert code == 0 and res["changed"] is True and res["restamp"] is None and res["config_version"] == 2
    assert _labels()[sid] == "V1" and len(_config_versions("Youth")) == 2


@needs_db
def test_set_dry_run_writes_nothing(scratch, capsys):
    _store("Youth", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    before = _state()
    code, out, _ = _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V2",
                                  "--effective-from", V2_TEXT, "--dry-run", "--note", "ignored"])
    res = json.loads(out)
    assert code == 0 and res["dry_run"] is True and res["changed"] is True and res["config_version"] == 2
    assert res["restamp"] == {"sessions": 1, "changed": 1, "versions": {"V2": 1}}
    assert _state() == before and len(_config_versions("Youth")) == 1


@needs_db
def test_set_moves_an_existing_key_and_reports_no_change_for_the_same_value(scratch, capsys):
    sid = _store("Youth", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V2", "--effective-from", V2_TEXT])
    assert _labels()[sid] == "V2"
    code, out, _ = _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V2",
                                  "--effective-from", "2026-11-09T00:00:00+05:30"])
    res = json.loads(out)
    assert code == 0 and res["changed"] is True and res["config_version"] == 3 and _labels()[sid] == "V1"
    code, out, _ = _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V2",
                                  "--effective-from", "2026-11-09T00:00:00+05:30"])
    res = json.loads(out)
    assert code == 0 and res["changed"] is False and res["config_version"] == 3
    assert res["restamp_note"] == "no change to the version list" and len(_config_versions("Youth")) == 3


@needs_db
@pytest.mark.parametrize("argv,message", [
    (["set", "--session-type", "Youth_Voice", "--key", "V2", "--effective-from", "2026-11-03T18:23:03"], "UTC offset"),
    (["set", "--session-type", "Youth_Voice", "--key", "V2", "--effective-from", "tomorrow"], "not an ISO instant"),
    (["set", "--session-type", "Nobody_Voice", "--key", "V2", "--effective-from", V2_TEXT], "no active upload config with questionnaire versions"),
    (["set", "--session-type", "Youth_Voice", "--key", "V1", "--effective-from", V2_TEXT], "first one"),
    (["set", "--session-type", "Youth_Voice", "--key", "", "--effective-from", V2_TEXT], "non-empty"),
])
def test_value_errors_print_an_error_line_return_1_and_write_nothing(scratch, capsys, argv, message):
    _store("Youth", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    before = _state()
    code, out, err = _call(capsys, argv)
    assert code == 1 and out == ""
    assert err.startswith("error: ") and message in err and err.endswith("\n") and err.count("\n") == 1
    assert _state() == before


@needs_db
def test_a_second_set_that_is_not_after_the_first_is_refused(scratch, capsys):
    assert _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V2", "--effective-from", V2_TEXT])[0] == 0
    before = _state()
    code, out, err = _call(capsys, ["set", "--session-type", "Youth_Voice", "--key", "V3",
                                    "--effective-from", "2026-11-03T18:23:03+05:30"])
    assert code == 1 and out == "" and "strictly increasing" in err and _state() == before


@needs_db
def test_restamp_relabels_from_the_current_windows(scratch, capsys):
    sid = _store("Youth", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    p = _store("Parent", "V1", datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
    # record V2 for both without relabelling, then let `restamp` do it
    for st in ("Youth_Voice", "Parent_Voice"):
        assert _call(capsys, ["set", "--session-type", st, "--key", "V2", "--effective-from", V2_TEXT, "--no-restamp"])[0] == 0
    assert _labels() == {sid: "V1", p: "V1"}

    before = _state()
    code, out, _ = _call(capsys, ["restamp", "--dry-run"])
    res = json.loads(out)
    assert code == 0 and res["dry_run"] is True and res["changed"] == 2
    assert {k: res["session_types"]["Youth_Voice"][k] for k in ("sessions", "changed", "versions")} == {"sessions": 1, "changed": 1, "versions": {"V2": 1}}
    assert {k: res["session_types"]["General_Public_Voice"][k] for k in ("sessions", "changed", "versions")} == {"sessions": 0, "changed": 0, "versions": {}}
    assert _state() == before                                           # a dry run writes nothing

    code, out, _ = _call(capsys, ["restamp", "--session-type", "Youth_Voice"])
    res = json.loads(out)
    assert code == 0 and res["dry_run"] is False and list(res["session_types"]) == ["Youth_Voice"] and res["changed"] == 1
    assert _labels() == {sid: "V2", p: "V1"}                            # only the filtered persona

    code, out, _ = _call(capsys, ["restamp"])
    assert code == 0 and json.loads(out)["changed"] == 1 and _labels() == {sid: "V2", p: "V2"}
    code, out, _ = _call(capsys, ["restamp"])
    assert code == 0 and json.loads(out)["changed"] == 0                # idempotent


@needs_db
def test_restamp_with_an_invalid_stored_list_returns_1(scratch, capsys):
    async def corrupt(conn):
        await conn.execute("UPDATE config_version SET config = '{\"adapter\": \"conversation_rows\"}'::jsonb "
                           "WHERE config_id = $1::uuid", PERSONAS["Youth"]["config_id"])
    _with_conn(corrupt)
    code, out, err = _call(capsys, ["restamp"])
    assert code == 1 and out == "" and err.startswith("error: ") and "questionnaire_versions" in err


@needs_db
def test_the_cli_leaves_no_pool_open_and_can_be_called_repeatedly(scratch, capsys):
    from app.database.db import db
    registry_before = dict(db._pools)     # the pools are keyed by event loop; `db.pool` is None outside a running loop
    for _ in range(3):
        assert _call(capsys, ["list"])[0] == 0
    assert db.pool is None
    # `db.pool` is always None once asyncio.run() has returned, so it cannot show an omitted disconnect: the loop-keyed
    # registry can (a pool left open would still be registered under its finished loop)
    assert db._pools == registry_before


def test_the_tool_documents_the_60_second_consumer_cache():
    # B13: a change reaches the Kafka consumers only when their cache entry expires
    assert "60 seconds" in cli.__doc__ and "cache" in cli.__doc__
