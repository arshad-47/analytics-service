"""
Unit tests (in-memory fakes, no database) of the startup health check of the feedback configs
(app/database/feedback_config_health.py) and of where Database.initialize_schema runs it. The real-Postgres side is
tests/test_seed_config_pg.py (the seed and its warning) and tests/test_feedback_process_steps_pg.py.
"""
import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock

import app.database.db as db_module
import app.database.feedback_config_health as health
from seed_support import mitra_config_v1
from conftest import FakeConn, FakePool


# startup check ------------------------------------------------------------------------------------------------------

def _config_rows(*configs):
    return [{"session_type": st, "config_source": "mitra", "tenant_code": "mitra", "config": cfg}
            for st, cfg in configs]


def test_startup_check_logs_an_error_per_invalid_config_and_nothing_for_valid_ones(caplog):
    old = mitra_config_v1("Parent")
    for key in ("max_csv_upload_bytes", "ingestion_schema", "process_steps"):
        del old[key]
    bad_type = {**mitra_config_v1("Educator"), "max_csv_upload_bytes": "100"}
    conn = FakeConn()
    conn.fetch = AsyncMock(return_value=_config_rows(
        ("Youth_Voice", mitra_config_v1("Youth")), ("Parent_Voice", old), ("Educator_Voice", json.dumps(bad_type))))
    with caplog.at_level(logging.ERROR):
        invalid = asyncio.run(health.check_feedback_configs(conn))
    assert [i["session_type"] for i in invalid] == ["Parent_Voice", "Educator_Voice"]
    assert set(invalid[0]["problems"]) == {"max_csv_upload_bytes", "ingestion_schema", "process_steps"}
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 2
    assert "Parent_Voice" in errors[0].getMessage() and "feedback_config.py migrate" in errors[0].getMessage()
    assert "Youth_Voice" not in caplog.text


def test_startup_check_never_raises(caplog):
    conn = FakeConn()
    conn.fetch = AsyncMock(side_effect=RuntimeError("relation config does not exist"))
    with caplog.at_level(logging.WARNING):
        asyncio.run(health.run_startup_checks(conn))
    assert "could not run" in caplog.text


def test_removed_env_vars_are_warned_by_name_only(tmp_path, caplog):
    env_file = tmp_path / ".env"
    env_file.write_text("PROCESS_CONFIG_FEEDBACK=[secret-steps]\nOTHER=1\nFEEDBACK_KAFKA_SCHEMA={\"x\": \"secret-schema\"}\n")
    environ = {"FEEDBACK_MAX_CSV_UPLOAD_BYTES": "secret-123", "PATH": "/bin"}
    with caplog.at_level(logging.WARNING):
        names = health.warn_removed_env_vars(environ, env_file)
    assert sorted(names) == ["FEEDBACK_KAFKA_SCHEMA", "FEEDBACK_MAX_CSV_UPLOAD_BYTES", "PROCESS_CONFIG_FEEDBACK"]
    for name in names:
        assert name in caplog.text
    for value in ("secret-steps", "secret-schema", "secret-123"):
        assert value not in caplog.text


def test_no_warning_when_the_removed_env_vars_are_absent(tmp_path, caplog):
    env_file = tmp_path / ".env"
    env_file.write_text("OTHER=1\n")
    with caplog.at_level(logging.WARNING):
        assert health.warn_removed_env_vars({"PATH": "/bin"}, env_file) == []
        assert health.warn_removed_env_vars({}, tmp_path / "missing.env") == []
    assert caplog.text == ""


def test_the_startup_check_looks_for_removed_env_vars_and_a_failure_of_that_part_hides_nothing(monkeypatch, caplog):
    warn = MagicMock()
    configs = AsyncMock(return_value=[])
    legacy = AsyncMock(return_value={})
    monkeypatch.setattr(health, "warn_removed_env_vars", warn)
    monkeypatch.setattr(health, "check_feedback_configs", configs)
    monkeypatch.setattr(health, "check_legacy_feedback_data", legacy)
    asyncio.run(health.run_startup_checks(FakeConn()))
    warn.assert_called_once_with()
    configs.assert_awaited_once()
    legacy.assert_awaited_once()

    warn.side_effect = RuntimeError("env unreadable")          # the first part fails: the other two still run
    with caplog.at_level(logging.WARNING):
        asyncio.run(health.run_startup_checks(FakeConn()))
    assert "could not run" in caplog.text
    assert configs.await_count == 2 and legacy.await_count == 2


def test_initialize_schema_applies_every_seed_and_then_runs_the_startup_check(monkeypatch):
    """The order matters: the check must see the seeded configs. Recorded through a fake connection."""
    events = []

    async def run():
        conn = FakeConn()

        async def execute(sql, *args):
            events.append(("sql", sql))
            return ""

        async def check(connection):
            events.append(("check", connection))

        conn.execute = execute
        conn.fetchval = AsyncMock(return_value=None)       # the seed's tenants are missing: a warning, nothing else
        monkeypatch.setattr(db_module, "check_feedback_config_health", check)
        database = db_module.Database()
        database.pool = FakePool(conn)
        await database.initialize_schema()
        return conn

    conn = asyncio.run(run())
    statements = [value for kind, value in events if kind == "sql"]
    seeds = [db_module.SEED_PROMPTS_FILE, db_module.SEED_THEMES_FILE, db_module.SEED_CONFIG_FILE]
    positions = [statements.index(path.read_text(encoding="utf-8")) for path in seeds]
    assert positions == sorted(positions) and len(set(positions)) == 3              # each seed ran once, in this order
    checks = [i for i, (kind, _) in enumerate(events) if kind == "check"]
    assert len(checks) == 1 and events[checks[0]][1] is conn
    seed_config_at = next(i for i, (kind, value) in enumerate(events)
                          if kind == "sql" and value == db_module.SEED_CONFIG_FILE.read_text(encoding="utf-8"))
    assert checks[0] > seed_config_at
