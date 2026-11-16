"""
Tests for tools/feedback_config.py (show / migrate / set of the per-bot feedback settings in the upload config).

* no database: build_parser() accepts every documented form and rejects the rest (argparse errors exit with 2 before
  anything runs), parse_set_value, and main() wiring (JSON output, `error: ...` + exit code 1 for a ValueError, the
  arguments handed to the DB layer, created_by = getpass.getuser()) with the DB layer replaced by fakes;
* real Postgres (opt-in, FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards)):
  main([...]) end to end against a freshly seeded database (the CLI opens its own pool with db.connect(), which reads
  settings.DATABASE_URL, so the tests point that setting at the scratch database). Synthetic data only.
"""
import asyncio
import getpass
import importlib.util
import json
from pathlib import Path

import asyncpg
import pytest
import pg_support

from app.config import settings
from seed_support import SEED_UPLOAD_CONFIG_COUNT, seed_config

_TOOL = Path(__file__).resolve().parent.parent / "tools" / "feedback_config.py"
_spec = importlib.util.spec_from_file_location("feedback_config_cli", _TOOL)
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

DSN = pg_support.SERVER_URL
needs_db = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

SET = ["set", "--session-type", "Youth_Voice", "--source", "mitra"]


# ====================================================================================== no database

@pytest.mark.parametrize("argv,expected", [
    (["show"], {"command": "show", "session_type": None}),
    (["show", "--session-type", "Youth_Voice"], {"command": "show", "session_type": "Youth_Voice"}),
    (["migrate"], {"command": "migrate", "dry_run": False}),
    (["migrate", "--dry-run"], {"command": "migrate", "dry_run": True}),
    (["migrate-legacy", "--tenant-code", "abc"],
     {"command": "migrate-legacy", "tenant_code": "abc", "map": [], "map_file": None, "source": None,
      "retire_config": [], "dry_run": False}),
    (["migrate-legacy", "--tenant-code", "abc", "--map", "leader:Parent=Parent_Voice", "--map", "PTM=Teacher_Voice",
      "--source", "ptm_app", "--retire-config", "7b3c1a2e-0000-4000-8000-000000000001", "--dry-run",
      "--map-file", "m.json"],
     {"map": ["leader:Parent=Parent_Voice", "PTM=Teacher_Voice"], "source": "ptm_app", "dry_run": True,
      "retire_config": ["7b3c1a2e-0000-4000-8000-000000000001"], "map_file": "m.json"}),
    (SET + ["--key", "max_csv_upload_bytes", "--value", "50000000"],
     {"command": "set", "session_type": "Youth_Voice", "source": "mitra", "key": "max_csv_upload_bytes",
      "value": "50000000", "json": None, "unset": False, "dry_run": False, "tenant_code": None, "note": None}),
    (SET + ["--key", "process_steps", "--json", '[{"name": "x"}]', "--dry-run", "--note", "n"],
     {"key": "process_steps", "value": None, "json": '[{"name": "x"}]', "dry_run": True, "note": "n"}),
    (SET + ["--key", "expect_stages", "--unset"], {"key": "expect_stages", "unset": True}),
    (["set", "--key", "ingestion_schema", "--json", "{}", "--source", "google_forms", "--session-type", "Parent_Voice",
      "--tenant-code", "mitra"], {"source": "google_forms", "session_type": "Parent_Voice", "tenant_code": "mitra"}),
    # a source is a free label (no allow-list); the program is picked by id or by name
    (["set", "--key", "process_steps", "--json", "[]", "--source", "ptm_app", "--session-type", "Parent",
      "--tenant-code", "abc", "--program-name", "PTM 2026"],
     {"source": "ptm_app", "program_name": "PTM 2026", "program_id": None, "tenant_code": "abc"}),
    (["show", "--tenant-code", "abc", "--program-id", "7b3c1a2e-0000-4000-8000-000000000001", "--source", "ptm_app"],
     {"command": "show", "tenant_code": "abc", "program_id": "7b3c1a2e-0000-4000-8000-000000000001",
      "source": "ptm_app", "program_name": None}),
])
def test_build_parser_accepts_every_documented_form(argv, expected):
    args = cli.build_parser().parse_args(argv)
    for name, value in expected.items():
        assert getattr(args, name) == value, (argv, name)


@pytest.mark.parametrize("argv", [
    [], ["bogus"], ["show", "extra"], ["show", "--dry-run"], ["migrate", "--session-type", "Youth_Voice"],
    ["migrate", "--key", "x"],
    ["set"],
    ["set", "--source", "mitra", "--key", "process_steps", "--json", "[]"],                       # no --session-type
    ["set", "--session-type", "Youth_Voice", "--key", "process_steps", "--json", "[]"],            # no --source
    ["set", "--session-type", "Youth_Voice", "--source", "mitra", "--json", "[]"],                 # no --key
    SET + ["--key", "not_a_setting", "--value", "1"],                                              # unknown key
    ["set", "--session-type", "Youth_Voice", "--source", "mitra", "--key", "process_steps", "--json", "[]",
     "--program-id"],                                                                              # selector without value
    SET + ["--key", "process_steps", "--json"],                                                   # option without value
    SET + ["--key", "process_steps", "--json", "[]", "--unknown"],
])
def test_build_parser_rejects_everything_else(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.build_parser().parse_args(argv)
    assert exc.value.code == 2
    assert "usage:" in capsys.readouterr().err


@pytest.mark.parametrize("key,value,json_text,expected", [
    ("max_csv_upload_bytes", "50000000", None, 50000000),
    ("max_csv_upload_bytes", " 7 ", None, 7),
    ("max_csv_upload_bytes", None, "123", 123),
    ("process_steps", None, '[{"name": "x"}]', [{"name": "x"}]),
    ("expect_stages", None, "[]", []),
    ("ingestion_schema", None, '{"create": {}}', {"create": {}}),
])
def test_parse_set_value(key, value, json_text, expected):
    assert cli.parse_set_value(key, value, json_text) == expected


@pytest.mark.parametrize("key,value,json_text,needle", [
    ("max_csv_upload_bytes", None, None, "give --value"),
    ("max_csv_upload_bytes", "1", "1", "either --value or --json"),
    ("max_csv_upload_bytes", "lots", None, "not an integer"),
    ("max_csv_upload_bytes", "1.5", None, "not an integer"),
    ("process_steps", "5", None, "--value is only for max_csv_upload_bytes"),
    ("process_steps", None, "[{", "not valid JSON"),
])
def test_parse_set_value_errors(key, value, json_text, needle):
    with pytest.raises(ValueError, match=needle):
        cli.parse_set_value(key, value, json_text)


class _FakeDb:
    def __init__(self):
        self.events = []

    async def connect(self):
        self.events.append("connect")

    async def disconnect(self):
        self.events.append("disconnect")


@pytest.fixture
def fake_layer(monkeypatch):
    fake_db, calls = _FakeDb(), []

    async def listing(session_type=None, **selectors):
        calls.append(("show", session_type) if all(v is None for v in selectors.values())
                     else ("show", session_type, selectors))
        return {"configs": []}

    async def migrating(dry_run=False, created_by="cli"):
        calls.append(("migrate", dry_run, created_by))
        return {"configs": 0, "migrated": 0, "dry_run": dry_run}

    async def setting(session_type, source, key, value, **kwargs):
        calls.append(("set", session_type, source, key, value, kwargs))
        if value == 13:
            raise ValueError("Feedback config 'max_csv_upload_bytes' must be a positive integer (bytes), got 13")
        return {"changed": True}

    async def adapter_problems(selectors):
        calls.append(("adapter_problems", dict(selectors)))
        return {}

    async def adapter_after_set(session_type, source, key, value, **kwargs):
        calls.append(("adapter_after_set", session_type, source, key))
        return "adapter is wrong" if value == "wide_form" else None

    async def legacy(**kwargs):
        calls.append(("migrate-legacy", kwargs))
        return {"dry_run": kwargs["dry_run"], "configs": {}}

    monkeypatch.setattr(cli, "db", fake_db)
    monkeypatch.setattr(cli.feedback_legacy_upgrade, "adapter_problems_for", adapter_problems)
    monkeypatch.setattr(cli.feedback_legacy_upgrade, "adapter_problem_after_set", adapter_after_set)
    monkeypatch.setattr(cli.feedback_legacy_upgrade, "migrate_legacy", legacy)
    monkeypatch.setattr(cli.feedback_operations, "list_feedback_config", listing)
    monkeypatch.setattr(cli.feedback_operations, "migrate_feedback_config", migrating)
    monkeypatch.setattr(cli.feedback_operations, "set_feedback_config_value", setting)
    monkeypatch.setattr(getpass, "getuser", lambda: "operator-1")
    return fake_db, calls


def test_main_wiring_with_the_db_layer_faked(fake_layer, capsys):
    fake_db, calls = fake_layer
    assert cli.main(["show"]) == 0
    assert cli.main(["show", "--session-type", "Parent_Voice"]) == 0
    assert cli.main(["show", "--tenant-code", "abc", "--program-name", "PTM", "--source", "ptm_app"]) == 0
    assert cli.main(["migrate", "--dry-run"]) == 0
    assert cli.main(["migrate"]) == 0
    assert cli.main(SET + ["--key", "max_csv_upload_bytes", "--value", "50000000", "--note", "why", "--dry-run"]) == 0
    assert cli.main(SET + ["--key", "process_steps", "--json", '[{"name": "feedback_metadata_extraction"}]']) == 0
    assert cli.main(SET + ["--key", "expect_stages", "--unset"]) == 0
    assert [c for c in calls if c[0] in ("show", "migrate", "set")] == [
        ("show", None), ("show", "Parent_Voice"),
        ("show", None, {"tenant_code": "abc", "program_id": None, "program_name": "PTM", "source": "ptm_app"}),
        ("migrate", True, "operator-1"), ("migrate", False, "operator-1"),
        ("set", "Youth_Voice", "mitra", "max_csv_upload_bytes", 50000000,
         {"tenant_code": None, "program_id": None, "program_name": None, "unset": False,
          "created_by": "operator-1", "change_note": "why", "dry_run": True}),
        ("set", "Youth_Voice", "mitra", "process_steps", [{"name": "feedback_metadata_extraction"}],
         {"tenant_code": None, "program_id": None, "program_name": None, "unset": False,
          "created_by": "operator-1", "change_note": None, "dry_run": False}),
        ("set", "Youth_Voice", "mitra", "expect_stages", None,
         {"tenant_code": None, "program_id": None, "program_name": None, "unset": True,
          "created_by": "operator-1", "change_note": None, "dry_run": False}),
    ]
    assert fake_db.events == ["connect", "disconnect"] * 8
    out = capsys.readouterr().out
    assert out.startswith("{\n  ") and '"dry_run": true' in out  # indented JSON on stdout


def test_show_and_set_ask_the_adapter_validator_and_migrate_legacy_hands_its_arguments_on(fake_layer, tmp_path, capsys):
    fake_db, calls = fake_layer
    getpass_user = "operator-1"
    assert cli.main(["show", "--tenant-code", "abc"]) == 0
    assert ("adapter_problems", {"session_type": None, "source": None, "tenant_code": "abc", "program_id": None,
                                 "program_name": None}) in calls
    assert cli.main(SET + ["--key", "adapter", "--json", '"wide_form"']) == 1          # refused before any write
    assert "the config would be invalid after this change: adapter is wrong" in capsys.readouterr().err
    assert not [c for c in calls if c[0] == "set"]
    assert cli.main(SET + ["--key", "adapter", "--json", '"conversation_rows"']) == 0
    assert [c for c in calls if c[0] == "set"][0][3:5] == ("adapter", "conversation_rows")
    map_file = tmp_path / "map.json"
    map_file.write_text('{"leader:Youth": "Youth_Voice"}')
    assert cli.main(["migrate-legacy", "--tenant-code", "abc", "--map", "PTM=Teacher_Voice", "--map-file", str(map_file),
                     "--source", "mitra", "--retire-config", "7b3c1a2e-0000-4000-8000-000000000001",
                     "--dry-run"]) == 0
    kwargs = [c for c in calls if c[0] == "migrate-legacy"][0][1]
    assert kwargs == {"tenant_code": "abc", "session_type_map": {"PTM": "Teacher_Voice", "leader:Youth": "Youth_Voice"},
                      "source": "mitra", "retire_config_ids": ["7b3c1a2e-0000-4000-8000-000000000001"],
                      "dry_run": True, "created_by": getpass_user}
    # a malformed mapping or a missing file is an error line and exit 1, never a traceback
    assert cli.main(["migrate-legacy", "--tenant-code", "abc", "--map", "no-separator"]) == 1
    assert cli.main(["migrate-legacy", "--tenant-code", "abc", "--map-file", str(tmp_path / "missing.json")]) == 1
    assert capsys.readouterr().err.count("error: ") == 2


@pytest.mark.parametrize("argv,needle", [
    (SET + ["--key", "max_csv_upload_bytes", "--value", "13"], "positive integer"),   # the DB layer's ValueError
    (SET + ["--key", "max_csv_upload_bytes", "--value", "lots"], "not an integer"),  # the CLI's own parse error
    (SET + ["--key", "process_steps", "--json", "[{"], "not valid JSON"),
    (SET + ["--key", "process_steps"], "give --value"),
    (SET + ["--key", "process_steps", "--value", "5"], "--value is only for"),
    (SET + ["--key", "expect_stages", "--unset", "--json", "[]"], "--unset takes neither"),
])
def test_main_turns_a_value_error_into_an_error_line_and_exit_code_1(fake_layer, capsys, argv, needle):
    fake_db, _ = fake_layer
    assert cli.main(argv) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err.startswith("error: ") and needle in captured.err
    assert fake_db.events == ["connect", "disconnect"]  # the pool is closed on the error path, too


def test_main_exits_with_2_on_argparse_errors_before_running_anything(monkeypatch):
    async def boom(args):
        raise AssertionError("the CLI must not start before the arguments are valid")
    monkeypatch.setattr(cli, "_run", boom)
    for argv in ([], ["set", "--session-type", "Youth_Voice", "--source", "mitra"], ["nope"]):
        with pytest.raises(SystemExit) as exc:
            cli.main(argv)
        assert exc.value.code == 2


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


def _json(capsys):
    return json.loads(capsys.readouterr().out)


def _active_config(session_type="Youth_Voice", source="mitra"):
    async def go(conn):
        row = await conn.fetchrow(
            """SELECT v.version, v.config, v.created_by FROM config c JOIN config_version v ON v.config_id = c.id AND v.is_active
               WHERE c.session_type = $1 AND c.config_source = $2""", session_type, source)
        return row["version"], json.loads(row["config"]), row["created_by"]
    return _with_conn(go)


@needs_db
def test_show_prints_the_seeded_settings(scratch, capsys):
    assert cli.main(["show", "--session-type", "Youth_Voice"]) == 0
    configs = _json(capsys)["configs"]
    assert [(c["source"], c["config_version"]) for c in configs] == [("google_forms", 1), ("mitra", 1)]
    for c in configs:
        assert c["max_csv_upload_bytes"] == 104857600 and c["process_steps"] == []
        assert c["problems"] == {} and c["expect_stages"] is None
    assert cli.main(["show"]) == 0
    assert len(_json(capsys)["configs"]) == SEED_UPLOAD_CONFIG_COUNT


@needs_db
def test_migrate_dry_run_then_real_then_idempotent(scratch, capsys):
    async def strip(conn):
        await conn.execute("UPDATE config_version SET config = config - 'max_csv_upload_bytes' - 'ingestion_schema' - 'process_steps'")
    _with_conn(strip)

    assert cli.main(["migrate", "--dry-run"]) == 0
    dry = _json(capsys)
    assert (dry["migrated"], dry["dry_run"]) == (7, True) and _active_config()[0] == 1

    assert cli.main(["migrate"]) == 0
    real = _json(capsys)
    assert (real["configs"], real["migrated"], real["unchanged"], real["invalid"]) == (7, 7, 0, [])
    version, config, created_by = _active_config()
    assert version == 2 and created_by == "test-operator" and config["max_csv_upload_bytes"] == 104857600

    assert cli.main(["migrate"]) == 0
    again = _json(capsys)
    assert (again["migrated"], again["unchanged"]) == (0, 7)


@needs_db
def test_set_scalar_json_dry_run_unset_and_errors(scratch, capsys):
    assert cli.main(SET + ["--key", "max_csv_upload_bytes", "--value", "50000000", "--dry-run"]) == 0
    assert _json(capsys)["dry_run"] is True and _active_config()[0] == 1

    assert cli.main(SET + ["--key", "max_csv_upload_bytes", "--value", "50000000"]) == 0
    assert _json(capsys)["config_version"] == 2
    version, config, created_by = _active_config()
    assert (version, config["max_csv_upload_bytes"], created_by) == (2, 50000000, "test-operator")

    assert cli.main(SET + ["--key", "expect_stages", "--json", '["AGE_IDENTIFICATION", "THANK_YOU"]']) == 0
    assert _active_config()[1]["expect_stages"] == ["AGE_IDENTIFICATION", "THANK_YOU"]
    capsys.readouterr()
    assert cli.main(SET + ["--key", "expect_stages", "--unset"]) == 0
    assert "expect_stages" not in _active_config()[1]
    capsys.readouterr()

    before = _active_config()[0]
    assert cli.main(SET + ["--key", "process_steps", "--json", '[{"name": ""}]']) == 1
    assert "non-empty string 'name'" in capsys.readouterr().err
    assert cli.main(["set", "--session-type", "Nobody_Voice", "--source", "mitra", "--key", "max_csv_upload_bytes",
                     "--value", "5"]) == 1
    assert "no mitra upload config for session_type" in capsys.readouterr().err
    assert _active_config()[0] == before  # a rejected change writes nothing


def test_the_tool_documents_the_60_second_consumer_cache():
    # B13: a change reaches the Kafka consumers only when their cache entry expires
    assert "60 seconds" in cli.__doc__ and "cache" in cli.__doc__
