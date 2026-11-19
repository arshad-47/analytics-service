"""
Real-Postgres integration tests for the feedback schema, seed and config lookup (slice 1a; session_type model).

A persona is identified by its session_type (Youth_Voice, ...), never by a bot id: config rows are unique
by (source, type, program, session_type), the bot id on a config is informational, and every feedback
submission carries session_type and questionnaire_version.

Skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (a role with CREATEDB), e.g.
  FEEDBACK_TEST_DATABASE_URL=postgresql://postgres@localhost:54329/postgres
tests/pg_support.py creates an EMPTY scratch database for this file and drops it afterwards. The tests apply
schema.sql themselves, exactly the way app/database/db.py does (IF NOT EXISTS injection), and never touch anything
outside that database. Test data is synthetic.
"""
import asyncio
import contextlib
import json
import uuid

import asyncpg
import pytest
import pg_support

import app.database.feedback_operations as feedback_ops
from app.database.db import db
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, SEED_CONFIG_COUNT, gforms_config_v1, mitra_config_v1, seed_config

DSN = pg_support.SERVER_URL

pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH = PERSONAS["Youth"]
PARENT = PERSONAS["Parent"]
EDUCATOR = PERSONAS["Educator"]
# personas with a Google Form (the Educator has none)
GFORMS_PERSONA_NAMES = [name for name, p in PERSONAS.items() if p["gforms_config_id"] is not None]


def run_db(body):
    """
    Runs `async def body(conn)` against the test database inside one transaction that is always
    rolled back, after the schema (twice) and the seed have been applied. DDL is transactional in
    Postgres, so the target database is left untouched even if it was empty.
    """
    async def runner():
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            sql = pg_support.schema_sql()
            await conn.execute(sql)
            await conn.execute(sql)
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT DO NOTHING")
            await seed_config(conn)
            return await body(conn)
        finally:
            await tx.rollback()
            await conn.close()
    return asyncio.run(runner())


class _ConnAsPool:
    """db.pool stand-in whose acquire() hands out the test's own (transactional) connection."""

    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


@contextlib.contextmanager
def _lookups_use(conn):
    """feedback_operations opens its own connections from db.pool; point it at the test transaction so the
    real SQL runs against the uncommitted (and afterwards rolled back) rows."""
    previous = db.pool
    db.pool = _ConnAsPool(conn)
    try:
        yield
    finally:
        db.pool = previous


async def _expect_violation(conn, exc, sql, *args):
    """Runs `sql` in a savepoint and asserts it raises `exc`; the outer transaction stays usable."""
    sp = conn.transaction()
    await sp.start()
    try:
        await conn.execute(sql, *args)
    except exc:
        await sp.rollback()
        return
    await sp.rollback()
    raise AssertionError(f"expected {exc.__name__} from: {sql}")


async def _add_submission(conn, sid="S1", session="TOKEN1"):
    await conn.execute(
        "INSERT INTO submissions (submission_id, session_id, tenant_code, submission_type) "
        "VALUES ($1, $2, 'mitra', 'feedback')", sid, session)


async def _add_feedback(conn, sid="S1", seq=1, parent=None, session="TOKEN1"):
    return await conn.fetchval(
        "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, message, parent_id) "
        "VALUES ($1, 'mitra', $2, 'mitra', $3, 'm', $4) RETURNING id", sid, session, seq, parent)


async def _add_feedback_submission(conn, sid="S1", status="COMPLETED", session_type="Youth_Voice", version="V1"):
    await conn.execute(
        "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, questionnaire_version, "
        "completion_status) VALUES ($1, 'mitra', 'mitra', $2, $3, $4)", sid, session_type, version, status)


async def _add_config(conn, source, session_type, program_id=YOUTH["program_id"], bot_id=None, name="extra"):
    return await conn.execute(
        "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type, bot_id) "
        "VALUES ($1, $2, 'feedback_upload', $3::uuid, 'mitra', $4, $5)", source, name, program_id, session_type, bot_id)


# 4 personas (Youth, Parent, General Public, Educator): 4 Mitra configs + 3 Google Forms configs (no Educator form)
# + 4 extraction configs, each with its version 1 (seed_config.sql)
SEED_COUNTS = {"leader_category": 4, "programs": 4, "config": 11, "config_version": 11}


def test_schema_applies_twice_and_seed_counts():
    async def body(conn):
        for table, expected in SEED_COUNTS.items():
            assert await conn.fetchval(f"SELECT count(*) FROM {table}") == expected, table
    run_db(body)


def test_seed_twice_leaves_the_same_rows():
    async def body(conn):
        await seed_config(conn)
        await seed_config(conn)
        for table, expected in SEED_COUNTS.items():
            assert await conn.fetchval(f"SELECT count(*) FROM {table}") == expected, table
        assert await conn.fetchval("SELECT count(*) FROM tenant") == 1
    run_db(body)


def test_seeded_rows_match_the_contract():
    async def body(conn):
        for persona, p in PERSONAS.items():
            lc = await conn.fetchrow("SELECT name, tenant_code FROM leader_category WHERE id = $1::uuid",
                                     p["leader_category_id"])
            assert (lc["name"], lc["tenant_code"]) == (persona, "mitra")
            prog = await conn.fetchrow("SELECT name, leaders_id::text AS leaders_id FROM programs WHERE id = $1::uuid",
                                       p["program_id"])
            assert prog["name"] == FEEDBACK_PROGRAM_NAME and prog["leaders_id"] == p["leader_category_id"]
            cfg = await conn.fetchrow(
                "SELECT config_source, config_type, program_id::text AS program_id, leader_id, session_type, bot_id, "
                "bot_name, tenant_code, name FROM config WHERE id = $1::uuid", p["config_id"])
            assert dict(cfg) == {
                "config_source": "mitra", "config_type": "feedback_upload", "program_id": p["program_id"],
                "leader_id": None, "session_type": p["session_type"], "bot_id": p["bot_id"],
                "bot_name": p["bot_name"], "tenant_code": "mitra", "name": p["config_name"]}
            ver = await conn.fetchrow(
                "SELECT version, is_active, config::text AS config, created_by, change_note "
                "FROM config_version WHERE config_id = $1::uuid", p["config_id"])
            assert (ver["version"], ver["is_active"], ver["created_by"], ver["change_note"]) == (
                1, True, "seed", "Initial format")
            assert json.loads(ver["config"]) == mitra_config_v1(persona)

            if p["gforms_config_id"] is None:
                assert persona == "Educator"  # no Google Form: no Google Forms config row at all
                assert await conn.fetchval(
                    "SELECT count(*) FROM config WHERE program_id = $1::uuid AND config_source = 'google_forms'",
                    p["program_id"]) == 0
                continue
            gf = await conn.fetchrow(
                "SELECT config_source, config_type, program_id::text AS program_id, leader_id, session_type, bot_id, "
                "bot_name, tenant_code, name FROM config WHERE id = $1::uuid", p["gforms_config_id"])
            assert dict(gf) == {
                "config_source": "google_forms", "config_type": "feedback_upload", "program_id": p["program_id"],
                "leader_id": None, "session_type": p["session_type"], "bot_id": None, "bot_name": None,
                "tenant_code": "mitra", "name": f"{persona} form CSV validator"}
            gver = await conn.fetchrow(
                "SELECT version, is_active, config::text AS config, created_by, change_note "
                "FROM config_version WHERE config_id = $1::uuid", p["gforms_config_id"])
            assert (gver["version"], gver["is_active"], gver["created_by"], gver["change_note"]) == (
                1, True, "seed", "Initial format")
            assert json.loads(gver["config"]) == gforms_config_v1(persona)
    run_db(body)


def test_google_forms_and_mitra_configs_of_one_program_coexist():
    async def body(conn):
        for name, p in PERSONAS.items():
            rows = await conn.fetch(
                "SELECT config_source, session_type, bot_id FROM config WHERE program_id = $1::uuid "
                "AND config_type = 'feedback_upload' ORDER BY config_source", p["program_id"])
            expected = [("mitra", p["session_type"], p["bot_id"])]
            if name in GFORMS_PERSONA_NAMES:
                expected.insert(0, ("google_forms", p["session_type"], None))
            assert [(r["config_source"], r["session_type"], r["bot_id"]) for r in rows] == expected
            # the persona's extraction config (source 'any') lives next to them under the same program
            assert await conn.fetchval(
                "SELECT count(*) FROM config WHERE program_id = $1::uuid AND config_type = 'feedback_extraction' "
                "AND config_source = 'any' AND session_type = $2", p["program_id"], p["session_type"]) == 1
        # the Google Forms row is the one a lookup by (source, type, program) finds, with its active version
        row = await conn.fetchrow(
            "SELECT c.id::text AS id, v.version FROM config c JOIN config_version v ON v.config_id = c.id AND v.is_active "
            "WHERE c.config_source = 'google_forms' AND c.config_type = 'feedback_upload' AND c.program_id = $1::uuid "
            "AND c.session_type = $2", YOUTH["program_id"], YOUTH["session_type"])
        assert (row["id"], row["version"]) == (YOUTH["gforms_config_id"], 1)
    run_db(body)


def test_seed_does_not_overwrite_edits_or_resurrect_v1():
    async def body(conn):
        await conn.execute("UPDATE config SET name = 'edited' WHERE id = $1::uuid", YOUTH["config_id"])
        # operator superseded v1: v1 inactive, v2 active
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid",
                           YOUTH["config_id"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 2, '{}'::jsonb)",
                           YOUTH["config_id"])
        # operator deleted Parent's v1 and kept a v2
        await conn.execute("DELETE FROM config_version WHERE config_id = $1::uuid", PARENT["config_id"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 2, '{}'::jsonb)",
                           PARENT["config_id"])
        await seed_config(conn)  # must not raise (one-active-version index) nor change anything
        assert await conn.fetchval("SELECT name FROM config WHERE id = $1::uuid", YOUTH["config_id"]) == "edited"
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   YOUTH["config_id"]) == 2
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   PARENT["config_id"]) == 1
    run_db(body)


def test_seed_does_not_overwrite_google_forms_edits():
    async def body(conn):
        # operator edited the Google Forms config row and superseded its v1 with an active v2
        await conn.execute("UPDATE config SET name = 'edited gf' WHERE id = $1::uuid", YOUTH["gforms_config_id"])
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid",
                           YOUTH["gforms_config_id"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 2, '{}'::jsonb)",
                           YOUTH["gforms_config_id"])
        await seed_config(conn)
        assert await conn.fetchval("SELECT name FROM config WHERE id = $1::uuid", YOUTH["gforms_config_id"]) == "edited gf"
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   YOUTH["gforms_config_id"]) == 2
        assert await conn.fetchval("SELECT count(*) FROM config") == SEED_CONFIG_COUNT
    run_db(body)


def test_seed_creates_the_tenant_and_its_rows_on_an_empty_database():
    async def runner():
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute(pg_support.schema_sql())
            assert await conn.fetchval("SELECT count(*) FROM tenant") == 0
            await seed_config(conn)  # no tenant row yet: the seed creates 'mitra', then everything that belongs to it
            assert await conn.fetchval("SELECT count(*) FROM tenant WHERE code = 'mitra'") == 1
            for table in ("leader_category", "programs", "config", "config_version"):
                assert await conn.fetchval(f"SELECT count(*) FROM {table}") > 0, table
            assert await conn.fetchval("SELECT count(*) FROM config") == SEED_CONFIG_COUNT
        finally:
            await tx.rollback()
            await conn.close()
    asyncio.run(runner())


def test_feedback_submissions_unique_submission_tenant():
    async def body(conn):
        await _add_submission(conn)
        await _add_feedback_submission(conn)
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, "
            "questionnaire_version, completion_status) VALUES ('S1', 'mitra', 'mitra', 'Youth_Voice', 'V1', 'COMPLETED')")
    run_db(body)


def test_feedback_submissions_source_ref_unique_but_nulls_allowed():
    async def body(conn):
        for i in (1, 2):
            await _add_submission(conn, f"S{i}", f"TOKEN{i}")
            await _add_feedback_submission(conn, f"S{i}")  # two NULL source_refs are fine
        await conn.execute("UPDATE feedback_submissions SET source_ref = 'r1' WHERE submission_id = 'S1'")
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "UPDATE feedback_submissions SET source_ref = 'r1' WHERE submission_id = 'S2'")
    run_db(body)


def test_completion_status_check_rejects_other_values():
    async def body(conn):
        await _add_submission(conn)
        await _expect_violation(
            conn, asyncpg.CheckViolationError,
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, "
            "questionnaire_version, completion_status) VALUES ('S1', 'mitra', 'mitra', 'Youth_Voice', 'V1', 'DONE')")
        await _add_feedback_submission(conn, status="IN_PROGRESS")
    run_db(body)


def test_feedbacks_unique_submission_tenant_sequence():
    async def body(conn):
        await _add_submission(conn)
        await _add_feedback(conn, seq=1)
        await _add_feedback(conn, seq=2)
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, message) "
            "VALUES ('S1', 'mitra', 'TOKEN1', 'mitra', 2, 'dup')")
    run_db(body)


def test_feedbacks_requires_existing_submission():
    async def body(conn):
        await _expect_violation(
            conn, asyncpg.ForeignKeyViolationError,
            "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, message) "
            "VALUES ('NOPE', 'mitra', 'T', 'mitra', 1, 'm')")
    run_db(body)


def test_duplicate_config_rejected_by_source_type_program_and_session_type():
    async def body(conn):
        # same (source, type, program, session_type) as the seeded Youth Mitra config: rejected whatever the
        # id, name or bot_id (the bot id takes no part in the key)
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type, bot_id) "
            "VALUES ('mitra', 'dup', 'feedback_upload', $1::uuid, 'mitra', 'Youth_Voice', $2)",
            YOUTH["program_id"], YOUTH["bot_id"])
        # the same Google Forms (source, type, program, session_type) twice
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ('google_forms', 'gf2', 'feedback_upload', $1::uuid, 'mitra', 'Youth_Voice')", YOUTH["program_id"])
        # NULL program (the source default) twice for one session_type
        await conn.execute(
            "INSERT INTO config (config_source, name, config_type, tenant_code, session_type) "
            "VALUES ('google_forms', 'default', 'feedback_upload', 'mitra', 'Youth_Voice')")
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, tenant_code, session_type) "
            "VALUES ('google_forms', 'default2', 'feedback_upload', 'mitra', 'Youth_Voice')")
    run_db(body)


def test_same_program_with_another_source_or_session_type_is_allowed():
    async def body(conn):
        # Educator has no Google Forms config yet: adding one for the same program (another source) is fine
        assert await conn.fetchval(
            "SELECT count(*) FROM config WHERE program_id = $1::uuid AND config_type = 'feedback_upload'",
            EDUCATOR["program_id"]) == 1
        await _add_config(conn, "google_forms", "Educator_Voice", EDUCATOR["program_id"], name="educator form")
        # another config_type for the same program / source / session_type
        await conn.execute(
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ('mitra', 'other type', 'something_else', $1::uuid, 'mitra', 'Youth_Voice')", YOUTH["program_id"])
        # another session_type under the same (source, type, program)
        await _add_config(conn, "mitra", "Youth_Voice_Pilot", YOUTH["program_id"], name="pilot")
        # ... and each of those is unique again
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ('mitra', 'pilot2', 'feedback_upload', $1::uuid, 'mitra', 'Youth_Voice_Pilot')", YOUTH["program_id"])
        # the same session_type in another program is another config
        await _add_config(conn, "mitra", "Youth_Voice", PARENT["program_id"], name="odd")
    run_db(body)


def test_config_uniqueness_is_not_keyed_by_bot_id():
    async def body(conn):
        # two configs that differ ONLY by bot_id (same session_type) collide: the bot id is informational
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type, bot_id) "
            "VALUES ('mitra', 'new bot', 'feedback_upload', $1::uuid, 'mitra', 'Youth_Voice', '999')",
            YOUTH["program_id"])
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type, bot_id) "
            "VALUES ('mitra', 'no bot', 'feedback_upload', $1::uuid, 'mitra', 'Youth_Voice', NULL)",
            YOUTH["program_id"])
        # the same bot_id under two session types is allowed
        await _add_config(conn, "mitra", "Youth_Voice_Pilot", YOUTH["program_id"], bot_id=YOUTH["bot_id"], name="same bot")
        await _add_config(conn, "mitra", "Youth_Voice_Other", YOUTH["program_id"], bot_id=YOUTH["bot_id"], name="same bot 2")
        rows = await conn.fetch(
            "SELECT session_type FROM config WHERE program_id = $1::uuid AND bot_id = $2 ORDER BY session_type",
            YOUTH["program_id"], YOUTH["bot_id"])
        assert [r["session_type"] for r in rows] == ["Youth_Voice", "Youth_Voice_Other", "Youth_Voice_Pilot"]
    run_db(body)


def test_config_session_type_is_required():
    async def body(conn):
        await _expect_violation(
            conn, asyncpg.NotNullViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code) "
            "VALUES ('google_forms', 'x', 'feedback_upload', $1::uuid, 'mitra')", EDUCATOR["program_id"])
        await _expect_violation(
            conn, asyncpg.NotNullViolationError,
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ('google_forms', 'x', 'feedback_upload', $1::uuid, 'mitra', NULL)", EDUCATOR["program_id"])
    run_db(body)


def test_feedback_submissions_session_type_and_questionnaire_version_are_required():
    async def body(conn):
        await _add_submission(conn)
        await _expect_violation(
            conn, asyncpg.NotNullViolationError,
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, questionnaire_version, "
            "completion_status) VALUES ('S1', 'mitra', 'mitra', 'V1', 'COMPLETED')")
        await _expect_violation(
            conn, asyncpg.NotNullViolationError,
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, "
            "completion_status) VALUES ('S1', 'mitra', 'mitra', 'Youth_Voice', 'COMPLETED')")
        await _add_feedback_submission(conn, session_type="Youth_Voice", version="V2")
        row = await conn.fetchrow(
            "SELECT session_type, questionnaire_version, meta_data FROM feedback_submissions WHERE submission_id = 'S1'")
        assert (row["session_type"], row["questionnaire_version"], row["meta_data"]) == ("Youth_Voice", "V2", None)
        # meta_data holds the informational bot fields
        await conn.execute(
            "UPDATE feedback_submissions SET meta_data = $1::jsonb WHERE submission_id = 'S1'",
            json.dumps({"bot_id": "90", "bot_name": "Synthetic bot"}))
        assert json.loads(await conn.fetchval(
            "SELECT meta_data::text FROM feedback_submissions WHERE submission_id = 'S1'")) == {
            "bot_id": "90", "bot_name": "Synthetic bot"}
    run_db(body)


def test_get_active_feedback_config_finds_each_persona_by_program_and_source():
    async def body(conn):
        with _lookups_use(conn):
            for persona, p in PERSONAS.items():
                mitra = await feedback_ops.get_active_feedback_config(p["program_id"], "mitra")
                assert mitra["config_id"] == p["config_id"]
                assert mitra["session_type"] == p["session_type"]
                assert (mitra["version"], mitra["config"]) == (1, mitra_config_v1(persona))
                with pytest.raises(TypeError):  # the source is always named: it is a free label, there is no default
                    await feedback_ops.get_active_feedback_config(p["program_id"])

                gforms = await feedback_ops.get_active_feedback_config(p["program_id"], "google_forms")
                if persona in GFORMS_PERSONA_NAMES:
                    assert gforms["config_id"] == p["gforms_config_id"]
                    assert gforms["session_type"] == p["session_type"]
                    assert gforms["config"] == gforms_config_v1(persona)
                else:
                    assert persona == "Educator" and gforms is None
            # Mitra and Google Forms of one program are told apart by source only; ids come back as str
            youth_mitra = await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "mitra")
            youth_gforms = await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "google_forms")
            assert youth_mitra["config_id"] != youth_gforms["config_id"]
            assert isinstance(youth_mitra["config_version_id"], str)
            # an unknown program or an unknown source finds nothing
            assert await feedback_ops.get_active_feedback_config(str(uuid.uuid4()), "mitra") is None
            assert await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "whatsapp") is None
    run_db(body)


def test_get_active_feedback_config_returns_only_the_active_version():
    async def body(conn):
        with _lookups_use(conn):
            await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH["config_id"])
            assert await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "mitra") is None  # none active
            await conn.execute(
                "INSERT INTO config_version (config_id, version, is_active, config) VALUES ($1::uuid, 2, TRUE, $2::jsonb)",
                YOUTH["config_id"], json.dumps({"marker": "v2"}))
            active = await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "mitra")
            assert (active["version"], active["config"], active["session_type"]) == (2, {"marker": "v2"}, "Youth_Voice")
            # the Google Forms config of the same program is untouched
            gforms = await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "google_forms")
            assert gforms["config_id"] == YOUTH["gforms_config_id"] and gforms["version"] == 1
    run_db(body)


def test_get_active_feedback_config_ignores_the_bot_id_of_the_config():
    async def body(conn):
        with _lookups_use(conn):
            # the bot the config was written for is informational: changing or clearing it changes nothing
            await conn.execute("UPDATE config SET bot_id = '999' WHERE id = $1::uuid", YOUTH["config_id"])
            assert (await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "mitra"))["config_id"] == YOUTH["config_id"]
            await conn.execute("UPDATE config SET bot_id = NULL, bot_name = NULL WHERE id = $1::uuid", YOUTH["config_id"])
            assert (await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "mitra"))["config_id"] == YOUTH["config_id"]
    run_db(body)


def test_config_source_is_a_free_label_no_check_constraint():
    async def body(conn):
        # review finding A1: a source is validated in code ([a-z0-9_]{1,40}), so the table accepts any label
        for source in ("csv", "ptm_app", "any"):
            await conn.execute(
                "INSERT INTO config (config_source, name, config_type, tenant_code, session_type) "
                "VALUES ($1, 'x', 'feedback_upload', 'mitra', 'Scratch_Voice')", source)
        assert await conn.fetchval(
            "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config'::regclass AND contype = 'c'") == 0
        await conn.execute("DELETE FROM config WHERE session_type = 'Scratch_Voice'")
    run_db(body)


def test_two_active_versions_of_one_config_rejected():
    async def body(conn):
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config_version (config_id, version, is_active, config) "
            "VALUES ($1::uuid, 2, TRUE, '{}'::jsonb)", YOUTH["config_id"])
        # an inactive second version is fine, and so is the same version number twice -> rejected
        await conn.execute(
            "INSERT INTO config_version (config_id, version, is_active, config) "
            "VALUES ($1::uuid, 2, FALSE, '{}'::jsonb)", YOUTH["config_id"])
        await _expect_violation(
            conn, asyncpg.UniqueViolationError,
            "INSERT INTO config_version (config_id, version, is_active, config) "
            "VALUES ($1::uuid, 2, FALSE, '{}'::jsonb)", YOUTH["config_id"])
    run_db(body)


def test_deleting_submission_cascades_to_feedback_tables():
    async def body(conn):
        await _add_submission(conn)
        await _add_feedback_submission(conn)
        q = await _add_feedback(conn, seq=1)
        await _add_feedback(conn, seq=2, parent=q)
        assert await conn.fetchval("SELECT count(*) FROM feedbacks") == 2
        await conn.execute("DELETE FROM submissions WHERE submission_id = 'S1' AND tenant_code = 'mitra'")
        assert await conn.fetchval("SELECT count(*) FROM feedback_submissions") == 0
        assert await conn.fetchval("SELECT count(*) FROM feedbacks") == 0
    run_db(body)


def test_deleting_parent_question_cascades_to_its_answers():
    async def body(conn):
        await _add_submission(conn)
        q = await _add_feedback(conn, seq=1)
        a = await _add_feedback(conn, seq=2, parent=q)
        await _add_feedback(conn, seq=3, parent=a)  # chained child
        other = await _add_feedback(conn, seq=4)
        await conn.execute("DELETE FROM feedbacks WHERE id = $1", q)
        remaining = [r["id"] for r in await conn.fetch("SELECT id FROM feedbacks")]
        assert remaining == [other]
    run_db(body)


def test_deleting_program_nulls_feedbacks_program_and_cascades_config():
    async def body(conn):
        await _add_submission(conn)
        fid = await _add_feedback(conn)
        await conn.execute("UPDATE feedbacks SET program_id = $1::uuid WHERE id = $2", YOUTH["program_id"], fid)
        await conn.execute("DELETE FROM programs WHERE id = $1::uuid", YOUTH["program_id"])
        assert await conn.fetchval("SELECT program_id FROM feedbacks WHERE id = $1", fid) is None
        assert await conn.fetchval("SELECT count(*) FROM config WHERE id = $1::uuid", YOUTH["config_id"]) == 0
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid",
                                   YOUTH["config_id"]) == 0
    run_db(body)


def test_feedback_indexes_exist():
    async def body(conn):
        names = {r["indexname"] for r in await conn.fetch(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")}
        assert {"idx_feedbacks_program_question", "idx_feedbacks_parent", "idx_feedbacks_source",
                "idx_feedback_submissions_session_type", "uq_config_tenant_source_type_program_session_type",
                "uq_config_version_active"} <= names
        assert "uq_config_source_type_program_bot" not in names  # the bot-keyed uniqueness is gone
        assert "uq_config_source_type_program_session_type" not in names  # and so is the first, tenant-less key
    run_db(body)
