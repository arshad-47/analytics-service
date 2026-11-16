"""
Support for the opt-in real-Postgres tests: ONE place for the server URL, the schema helper and the scratch databases.

FEEDBACK_TEST_DATABASE_URL names a Postgres SERVER (any database on it; the account needs CREATEDB). It is never
used for the tests' own statements. For every test module that has opt-in PG tests, the module-scoped autouse
fixture in tests/conftest.py creates a fresh, empty database `scratch_pg_<module>_<random>` on that server, points
the module's `DSN` / `DB_URL` at it, and drops it when the module is done. The destructive statements of the tests
(DELETE FROM config, DROP SCHEMA ...) can therefore only ever reach a database this suite created a moment ago, and
no module depends on what another one left behind (every module starts on an empty database).

How a test module opts in (see any tests/test_*_pg.py):

    import pg_support
    DSN = pg_support.SERVER_URL          # None when the variable is not set; the fixture swaps in the scratch DSN
    pytestmark = pytest.mark.skipif(DSN is None, reason=pg_support.SKIP_REASON)

and uses `DSN` (a module global) at CALL time, never copied at import time. Several modules share one runner by
reading `pg_support.current_dsn()`, which is the DSN of the module that is running right now.
"""
import asyncio
import os
import re
import secrets
from typing import List, Optional
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from app.database.db import SCHEMA_FILE

ENV_VAR = "FEEDBACK_TEST_DATABASE_URL"
_raw = os.environ.get(ENV_VAR)
SERVER_URL: Optional[str] = None if (_raw is None or _raw == "") else _raw
SKIP_REASON = f"{ENV_VAR} not set (it names a Postgres server on which the tests create scratch databases)"
DB_PREFIX = "scratch_pg_"
# names of the module globals the fixture replaces (a module declares which ones it has by defining them)
DSN_ATTRIBUTES = ("DSN", "DB_URL")

_current_dsn: Optional[str] = None
_server_checked = False


def schema_sql() -> str:
    """
    schema.sql with IF NOT EXISTS injected on the bare CREATE TABLE / CREATE INDEX statements, the way
    Database.initialize_schema does it. The ONE copy used by every PG test.
    """
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    sql = sql.replace("CREATE TABLE IF NOT EXISTS ", "__TABLE_ALREADY__")
    sql = sql.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ")
    sql = sql.replace("__TABLE_ALREADY__", "CREATE TABLE IF NOT EXISTS ")
    sql = sql.replace("CREATE INDEX IF NOT EXISTS ", "__INDEX_ALREADY__")
    sql = sql.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")
    return sql.replace("__INDEX_ALREADY__", "CREATE INDEX IF NOT EXISTS ")


def current_dsn() -> str:
    """The DSN of the scratch database of the module that is running now (set by the fixture)."""
    if _current_dsn is None:
        raise RuntimeError("no scratch database is active: a PG test ran outside the pg_scratch_database fixture")
    return _current_dsn


def dsn_attributes(module) -> List[str]:
    """Names of the module globals that hold the server URL (the modules that have opt-in PG tests)."""
    if SERVER_URL is None:
        return []
    return [name for name in DSN_ATTRIBUTES if getattr(module, name, None) == SERVER_URL]


def scratch_name(module_name: str) -> str:
    """`scratch_pg_<module>_<random>`: lower case letters, digits and underscores only, at most 63 characters."""
    short = re.sub(r"[^a-z0-9]+", "_", module_name.rsplit(".", 1)[-1].lower())
    for prefix in ("test_feedback_", "test_"):
        if short.startswith(prefix):
            short = short[len(prefix):]
            break
    suffix = secrets.token_hex(4)
    room = 63 - len(DB_PREFIX) - len(suffix) - 1
    return f"{DB_PREFIX}{short[:room].strip('_')}_{suffix}"


def with_database(url: str, name: str) -> str:
    """`url` pointing at database `name` instead (credentials and host untouched)."""
    return urlunsplit(urlsplit(url)._replace(path="/" + name))


class PgServerError(RuntimeError):
    """The server named by FEEDBACK_TEST_DATABASE_URL cannot be used for the tests (the message never carries the URL)."""


def _explain(exc: BaseException) -> str:
    """The class of the driver's error only: its text can name the host or even the credentials."""
    return type(exc).__name__


async def _admin(sql: str, *, check_privilege: bool = False):
    try:
        conn = await asyncpg.connect(SERVER_URL)
    except Exception as exc:  # the URL (and the password in it) is never shown
        raise PgServerError(f"cannot connect to the server named by {ENV_VAR}: {_explain(exc)}") from None
    try:
        if check_privilege:
            can_create = await conn.fetchval(
                "SELECT rolsuper OR rolcreatedb FROM pg_roles WHERE rolname = current_user")
            if can_create is not True:
                raise PgServerError(
                    f"the role of {ENV_VAR} cannot create databases (CREATEDB is required): the tests create one "
                    "scratch database per test module on that server and never touch the database the URL names")
            return None
        return await conn.execute(sql)
    finally:
        await conn.close()


def require_server_can_create_databases() -> None:
    """Refuses (once per session, with a clear message) when the server cannot be reached or cannot create databases."""
    global _server_checked
    if _server_checked:
        return
    asyncio.run(_admin("", check_privilege=True))
    _server_checked = True


def create_scratch_database(module_name: str) -> str:
    """Creates an empty `scratch_pg_*` database on the server and returns its DSN."""
    require_server_can_create_databases()
    name = scratch_name(module_name)
    asyncio.run(_admin(f'CREATE DATABASE "{name}"'))
    return with_database(SERVER_URL, name)


def drop_scratch_database(dsn: str) -> None:
    """Drops a database made by create_scratch_database (it refuses any name without the scratch prefix)."""
    name = urlsplit(dsn).path.lstrip("/")
    if not name.startswith(DB_PREFIX) or re.fullmatch(r"[a-z0-9_]+", name) is None:
        raise PgServerError("refusing to drop a database that is not a scratch_pg_* database")

    async def drop():
        conn = await asyncpg.connect(SERVER_URL)
        try:
            await conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
                name)
            await conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
        finally:
            await conn.close()
    asyncio.run(drop())


def activate(dsn: Optional[str]) -> None:
    """Makes `dsn` the one current_dsn() returns (None clears it)."""
    global _current_dsn
    _current_dsn = dsn


# ---------------------------------------------------------------------------------------------------------------------
# Cross-tenant fixtures. The ingest path refuses a session of tenant B that points at a program of tenant A, so a test
# that wants to prove "a lookup is scoped by tenant even when the program matches" writes such rows with plain SQL.
# ---------------------------------------------------------------------------------------------------------------------

async def insert_foreign_session(conn, *, tenant_code, submission_id, program_id, started_at, version, session_type,
                                 source="mitra", questions=("What is your age?",), session_id=None):
    """
    Stores one feedback session of `tenant_code` (created if missing) whose submissions.program_id is `program_id`
    (possibly a program of ANOTHER tenant), labelled `version`, with one bot question per entry of `questions`
    (no answers, no probes). Plain SQL, no validation: it is the shape a mis-tagged or hostile row would have.
    """
    await conn.execute(
        "INSERT INTO tenant (name, code) VALUES ($1, $1) ON CONFLICT (code) DO NOTHING", tenant_code)
    await conn.execute(
        "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type, status, submission_date, "
        "program_id) VALUES ($1, $2, $3, 'feedback', 'success', $4, $5::uuid)",
        session_id if session_id is not None else f"foreign-{secrets.token_hex(8)}", submission_id, tenant_code,
        started_at, program_id)
    await conn.execute(
        "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, questionnaire_version, "
        "completion_status) VALUES ($1, $2, $3, $4, $5, 'COMPLETED')",
        submission_id, tenant_code, source, session_type, version)
    for position, text in enumerate(questions, start=1):
        await conn.execute(
            "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, question_key, message) "
            "SELECT $1, $2, session_id, $3, $4, $5, $6 FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
            submission_id, tenant_code, source, position, f"Q{position}", text)


async def insert_foreign_config(conn, *, tenant_code, program_id, source, session_type, config,
                                config_type="feedback_upload"):
    """
    Stores an active version 1 of a config of `tenant_code` (created if missing) that points at `program_id`
    (possibly a program of ANOTHER tenant): the shape that makes a lookup that forgot the tenant pick the wrong row.
    Returns the config id.
    """
    import json
    await conn.execute(
        "INSERT INTO tenant (name, code) VALUES ($1, $1) ON CONFLICT (code) DO NOTHING", tenant_code)
    config_id = await conn.fetchval(
        "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
        "VALUES ($1, 'foreign test config', $2, $3::uuid, $4, $5) RETURNING id",
        source, config_type, program_id, tenant_code, session_type)
    await conn.execute(
        "INSERT INTO config_version (config_id, version, is_active, config, created_by) "
        "VALUES ($1, 1, TRUE, $2::jsonb, 'test')", config_id, json.dumps(config))
    return config_id
