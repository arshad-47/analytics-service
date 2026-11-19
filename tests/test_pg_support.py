"""
Unit tests (no database) of tests/pg_support.py, the shared mechanism of the opt-in Postgres tests: the scratch
database names, the refusal when the server cannot create databases, the guard of the drop, and that the ONE schema
helper still equals what Database.initialize_schema executes (so it cannot drift from production silently).
"""
import asyncio
import re
from unittest.mock import AsyncMock, MagicMock

import pytest

import app.database.db as db_module
import pg_support
from conftest import FakeConn, FakePool

SECRET_URL = "postgresql://tester:s3cr3t-pw@db.example.test:5432/maintenance"


def test_scratch_names_are_prefixed_lower_case_unique_and_short():
    names = {pg_support.scratch_name("tests.test_feedback_extraction_personas_pg") for _ in range(20)}
    assert len(names) == 20
    for name in names:
        assert name.startswith("scratch_pg_") and len(name) <= 63
        assert re.fullmatch(r"[a-z0-9_]+", name) is not None
        assert "extraction_personas_pg" in name or "extraction_personas" in name
    long_name = pg_support.scratch_name("test_" + "very_long_module_name_" * 10)
    assert len(long_name) <= 63 and long_name.startswith("scratch_pg_")
    assert re.fullmatch(r"[a-z0-9_]+", pg_support.scratch_name("Test-Odd.Module Name!")) is not None


def test_with_database_changes_only_the_database_of_the_url():
    url = pg_support.with_database(SECRET_URL, "scratch_pg_x_1")
    assert url == "postgresql://tester:s3cr3t-pw@db.example.test:5432/scratch_pg_x_1"


@pytest.mark.parametrize("dsn", [SECRET_URL, "postgresql://u:p@h/postgres", "postgresql://u:p@h/scratch_pg", "postgresql://u:p@h/Scratch_pg_x",
                                 "postgresql://u:p@h/scratch_pg_x; DROP DATABASE postgres", "postgresql://u:p@h/"])
def test_the_drop_refuses_anything_that_is_not_a_scratch_database(dsn):
    with pytest.raises(pg_support.PgServerError, match="not a scratch_pg_"):
        pg_support.drop_scratch_database(dsn)


def _server(monkeypatch, *, can_create=True, connect_error=None):
    monkeypatch.setattr(pg_support, "SERVER_URL", SECRET_URL)
    monkeypatch.setattr(pg_support, "_server_checked", False)
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=can_create)
    conn.execute = AsyncMock(return_value="CREATE DATABASE")
    conn.close = AsyncMock()
    connect = AsyncMock(side_effect=connect_error) if connect_error is not None else AsyncMock(return_value=conn)
    monkeypatch.setattr(pg_support.asyncpg, "connect", connect)
    return conn, connect


def test_a_server_that_cannot_create_databases_is_refused_with_a_clear_message_and_no_url(monkeypatch):
    conn, _ = _server(monkeypatch, can_create=False)
    with pytest.raises(pg_support.PgServerError) as caught:
        pg_support.require_server_can_create_databases()
    message = str(caught.value)
    assert "CREATEDB" in message and pg_support.ENV_VAR in message
    assert "s3cr3t-pw" not in message and "db.example.test" not in message
    conn.execute.assert_not_awaited()                                 # nothing was created
    conn.close.assert_awaited_once()


def test_an_unreachable_server_is_reported_without_the_url_or_the_password(monkeypatch):
    _server(monkeypatch, connect_error=OSError("connection refused for s3cr3t-pw at db.example.test"))
    with pytest.raises(pg_support.PgServerError) as caught:
        pg_support.require_server_can_create_databases()
    assert "cannot connect" in str(caught.value) and "OSError" in str(caught.value)
    # the driver's own message may carry the host or the password: only the class of the error is kept
    assert "s3cr3t-pw" not in str(caught.value) and "db.example.test" not in str(caught.value)


def test_creating_a_scratch_database_checks_the_server_once_and_returns_a_dsn_on_the_same_server(monkeypatch):
    conn, connect = _server(monkeypatch, can_create=True)
    first = pg_support.create_scratch_database("tests.test_feedback_x_pg")
    second = pg_support.create_scratch_database("tests.test_feedback_y_pg")
    assert first != second and first.startswith("postgresql://tester:s3cr3t-pw@db.example.test:5432/scratch_pg_x_pg_")
    statements = [call.args[0] for call in conn.execute.await_args_list]
    assert [s.startswith('CREATE DATABASE "scratch_pg_') for s in statements] == [True, True]
    # one privilege check for the whole session, then one connection per CREATE DATABASE
    assert conn.fetchval.await_count == 1 and connect.await_count == 3


def test_the_dsn_attributes_are_found_only_when_a_module_holds_the_server_url(monkeypatch):
    monkeypatch.setattr(pg_support, "SERVER_URL", SECRET_URL)
    module = type("M", (), {"DSN": SECRET_URL, "DB_URL": "something else", "other": SECRET_URL})
    assert pg_support.dsn_attributes(module) == ["DSN"]
    monkeypatch.setattr(pg_support, "SERVER_URL", None)
    assert pg_support.dsn_attributes(module) == []


def test_current_dsn_is_refused_outside_a_scratch_module(monkeypatch):
    monkeypatch.setattr(pg_support, "_current_dsn", None)
    with pytest.raises(RuntimeError, match="no scratch database"):
        pg_support.current_dsn()


def test_the_one_schema_helper_is_exactly_what_initialize_schema_executes():
    executed = []

    async def run():
        conn = FakeConn()

        async def execute(sql, *args):
            executed.append(sql)
            return ""
        conn.execute = execute
        conn.fetchval = AsyncMock(return_value=1)
        database = db_module.Database()
        database.pool = FakePool(conn)
        await database.initialize_schema()
    asyncio.run(run())
    assert pg_support.schema_sql() in executed
    sql = pg_support.schema_sql()
    assert "IF NOT EXISTS IF NOT EXISTS" not in sql and "CREATE TABLE IF NOT EXISTS tenant" in sql
