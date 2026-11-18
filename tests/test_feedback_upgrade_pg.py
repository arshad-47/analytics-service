"""
Real-Postgres tests of the UPGRADE PATH from a database created before session_type / questionnaire versions / adapters
(release 1.3 shape: one config row per Mitra bot, no persona key on configs or stored sessions, config_source CHECK,
unique index on the bot id) to this release:

  * startup (schema.sql, `Database.initialize_schema`) never crashes on such a database, whatever it holds (review
    finding C1: two bot rows for one program raised UniqueViolation and every process crash-looped);
  * `config.config_source` is a free label (A1: the CHECK is gone, also under another constraint name);
  * the unique index spans the tenant (two tenants can each hold a program-less default config);
  * `tools/feedback_config.py migrate-legacy` (app/database/feedback_legacy_upgrade.py) brings the data over (C2): the
    persona key from the operator's mapping, the missing JSON keys through new config versions, the stored sessions'
    labels, the unique index; tenant-scoped, idempotent, dry-run, counts only;
  * the startup health check reports the legacy rows as ONE error naming the exact command, and reports invalid
    adapter content.

The legacy shape is built by applying the current schema.sql and then undoing exactly what the release added (verified
once against the schema of the previous release). Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards). Synthetic data only (invented tenant / program / persona names).
"""
import asyncio
import getpass
import importlib.util
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path

import asyncpg
import pytest
import pg_support

from app.api.services import feedback_uploads
from app.api.services.uploads import InvalidFeedbackFile, load_feedback_target
from app.config import settings
from app.database import feedback_config_health as health
from app.database import feedback_legacy_upgrade as up
from app.database import feedback_operations as fo
from app.database.db import db
from test_feedback_upgrade import legacy_gforms, legacy_mitra

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

_TOOL = Path(__file__).resolve().parent.parent / "tools" / "feedback_config.py"
_spec = importlib.util.spec_from_file_location("feedback_config_cli_upgrade", _TOOL)
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

TENANT = "acme"
V2_AT = "2026-11-03T00:00:00+00:00"
ALPHA = {"leader": str(uuid.uuid4()), "program": str(uuid.uuid4()), "name": "Alpha", "session_type": "Alpha_Voice"}
BETA = {"leader": str(uuid.uuid4()), "program": str(uuid.uuid4()), "name": "Beta", "session_type": "Beta_Voice"}
INDEX = up.CONFIG_UNIQUE_INDEX
LEGACY_INDEX = up.LEGACY_CONFIG_INDEX
NIL = "00000000-0000-0000-0000-000000000000"


async def _fresh(conn):
    await conn.execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
    await conn.execute(pg_support.schema_sql())


async def _downgrade(conn, check_name="config_config_source_check"):
    """Undo what this release added to the schema: the database as the previous release left it."""
    await conn.execute("DROP FUNCTION ensure_config_unique_index()")
    await conn.execute(f"DROP INDEX {INDEX}")
    await conn.execute("DROP TABLE feedback_extracted_data")
    await conn.execute("ALTER TABLE config DROP COLUMN session_type")
    await conn.execute("ALTER TABLE feedback_submissions DROP COLUMN session_type, "
                       "DROP COLUMN questionnaire_version, DROP COLUMN pii_detected")
    await conn.execute(f"ALTER TABLE config ADD CONSTRAINT {check_name} "
                       "CHECK (config_source IN ('mitra', 'google_forms'))")
    await conn.execute(f"CREATE UNIQUE INDEX {LEGACY_INDEX} ON config "
                       f"(config_source, config_type, COALESCE(program_id, '{NIL}'), COALESCE(bot_id, ''))")


def _beta_mitra():
    """A config whose V2 window was already written by hand (a partly upgraded persona)."""
    config = legacy_mitra()
    config["questionnaire_versions"] = [{"key": "V1", "effective_from": None}, {"key": "V2", "effective_from": V2_AT}]
    return config


async def _add_persona(conn, tenant, persona):
    await conn.execute("INSERT INTO leader_category (id, name, tenant_code) VALUES ($1::uuid, $2, $3)",
                       persona["leader"], persona["name"], tenant)
    await conn.execute("INSERT INTO programs (id, leaders_id, name, tenant_code) VALUES ($1::uuid, $2::uuid, $3, $4)",
                       persona["program"], persona["leader"], f"{persona['name']} Program", tenant)


async def _add_config(conn, tenant, persona, source, bot_id, config, name=None):
    config_id = await conn.fetchval(
        "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, bot_id) "
        "VALUES ($1, $2, 'feedback_upload', $3::uuid, $4, $5) RETURNING id",
        source, name or f"{persona['name']} {source} {bot_id}", persona["program"], tenant, bot_id)
    await conn.execute("INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
                       "VALUES ($1, 1, TRUE, $2::jsonb, 'seed', 'Initial format')", config_id, json.dumps(config))
    return config_id


async def _add_session(conn, tenant, persona, source, submission_id, start):
    await conn.execute(
        "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type, submission_date, program_id, "
        "leader_id, status) VALUES ($1, $2, $3, 'feedback', $4, $5::uuid, $6::uuid, 'success')",
        submission_id.ljust(32, "s"), submission_id, tenant, datetime.fromisoformat(start), persona["program"],
        persona["leader"])
    await conn.execute(
        "INSERT INTO feedback_submissions (submission_id, tenant_code, source, language, completion_status, source_ref, "
        "meta_data) VALUES ($1, $2, $3, 'en', 'COMPLETED', $4, '{\"bot_id\": \"90\"}'::jsonb)",
        submission_id, tenant, source, submission_id if source == "google_forms" else None)


async def _tenant(conn, code, name=None):
    await conn.execute("INSERT INTO tenant (name, code) VALUES ($1, $2) ON CONFLICT (code) DO NOTHING",
                       name or code.title(), code)


async def _legacy_rows(conn, *, second_bot=False):
    """Tenant `acme`: Alpha (Mitra bot + Google Forms form, 3 stored sessions) and Beta (Mitra bot with a V2 window,
    2 stored sessions). `second_bot`: Alpha also has the config of a second Mitra bot (the old one-row-per-bot design)."""
    await _tenant(conn, TENANT)
    await _add_persona(conn, TENANT, ALPHA)
    await _add_persona(conn, TENANT, BETA)
    ids = {
        "alpha_mitra": await _add_config(conn, TENANT, ALPHA, "mitra", "90", legacy_mitra()),
        "alpha_forms": await _add_config(conn, TENANT, ALPHA, "google_forms", None, legacy_gforms()),
        "beta_mitra": await _add_config(conn, TENANT, BETA, "mitra", "88", _beta_mitra()),
    }
    if second_bot:
        ids["alpha_mitra_2"] = await _add_config(conn, TENANT, ALPHA, "mitra", "101", legacy_mitra())
    await _add_session(conn, TENANT, ALPHA, "mitra", "a-1001", "2026-10-05T10:00:00+00:00")
    await _add_session(conn, TENANT, ALPHA, "mitra", "a-1002", "2026-11-10T10:00:00+00:00")
    await _add_session(conn, TENANT, ALPHA, "google_forms", "a-2001", "2026-10-06T10:00:00+00:00")
    await _add_session(conn, TENANT, BETA, "mitra", "b-1001", "2026-10-30T10:00:00+00:00")
    await _add_session(conn, TENANT, BETA, "mitra", "b-1002", "2026-11-05T10:00:00+00:00")
    return ids


def _run(body, *, legacy=True, rows=True, second_bot=False, check_name="config_config_source_check"):
    """Runs `body(conn, ids)` on the scratch database in the shape of the previous release (or a fresh one)."""
    async def main():
        conn = await asyncpg.connect(DSN)
        try:
            await _fresh(conn)
            if legacy:
                await _downgrade(conn, check_name)
            ids = await _legacy_rows(conn, second_bot=second_bot) if rows else {}
            return await body(conn, ids)
        finally:
            db.pool = None
            await conn.close()
    return asyncio.run(main())


async def _startup():
    """The real startup path of every process: schema.sql, the seeds, the health check."""
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    db.pool = pool
    try:
        await db.initialize_schema()
    finally:
        db.pool = None
        await pool.close()


async def _with_pool(coro_fn):
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    db.pool = pool
    try:
        return await coro_fn()
    finally:
        db.pool = None
        await pool.close()


async def _index_names(conn):
    return {r["indexname"] for r in await conn.fetch(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'config'")}


async def _active(conn, config_id):
    row = await conn.fetchrow("SELECT version, config FROM config_version WHERE config_id = $1 AND is_active", config_id)
    return row["version"], json.loads(row["config"])


async def _snapshot(conn):
    """Everything migrate-legacy may write: config rows, versions, the sessions' labels, the index names."""
    return (
        [tuple(r) for r in await conn.fetch("SELECT id::text, session_type, bot_id FROM config ORDER BY id")],
        [tuple(r) for r in await conn.fetch(
            "SELECT config_id::text, version, is_active, config::text FROM config_version ORDER BY config_id, version")],
        [tuple(r) for r in await conn.fetch(
            "SELECT submission_id, session_type, questionnaire_version FROM feedback_submissions ORDER BY submission_id")],
        sorted(await _index_names(conn)),
    )


# ------------------------------------------------------------------------------------------------ C1: startup

def test_the_old_schema_with_two_bot_rows_for_one_program_would_have_crashed_the_old_index_swap():
    """The reproduction of review finding C1: the statement the schema used to run unconditionally."""
    async def body(conn, ids):
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "CREATE UNIQUE INDEX uq_config_source_type_program_session_type ON config (config_source, config_type, "
                f"COALESCE(program_id, '{NIL}'), COALESCE(NULL::text, ''))")
    _run(body, second_bot=True)


def test_startup_does_not_crash_on_a_database_with_several_config_rows_per_program(caplog):
    async def body(conn, ids):
        versions_before = await conn.fetchval(
            "SELECT string_agg(config_id::text || version::text || is_active::text || config::text, '|' "
            "ORDER BY config_id, version) FROM config_version")
        names_before = await _index_names(conn)
        assert LEGACY_INDEX in names_before and INDEX not in names_before
        with caplog.at_level(logging.INFO, logger="analytics_service.db"):
            await _startup()
            await _startup()                              # a restart (and the other two processes) changes nothing
        names = await _index_names(conn)
        assert LEGACY_INDEX in names and INDEX not in names     # the old key stays until the new one can exist
        assert await conn.fetchval("SELECT ensure_config_unique_index()") is False
        # nothing was damaged: the rows and versions are as they were (the seed added no config: its tenant is absent)
        assert await conn.fetchval(
            "SELECT string_agg(config_id::text || version::text || is_active::text || config::text, '|' "
            "ORDER BY config_id, version) FROM config_version") == versions_before
        assert await conn.fetchval("SELECT count(*) FROM config WHERE session_type IS NULL") == 4
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and "unique index" in r.getMessage()]
        assert len(warnings) == 2 and all("Table config" in w and INDEX in w and up.LEGACY_MIGRATE_COMMAND in w
                                          for w in warnings)
    _run(body, second_bot=True)


def test_the_seed_message_names_the_missing_tenant_and_says_what_still_ran(caplog):
    async def body(conn, ids):
        with caplog.at_level(logging.INFO, logger="analytics_service.db"):
            await _startup()
        seed = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Config seed")]
        assert len(seed) == 1 and "'mitra'" in seed[0] and "do not exist" in seed[0] and "insert nothing" in seed[0]
        assert "skipped" not in seed[0]
        assert await conn.fetchval("SELECT count(*) FROM config WHERE tenant_code = 'mitra'") == 0
        await _tenant(conn, "mitra")
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="analytics_service.db"):
            await _startup()
        assert not [r for r in caplog.records if r.getMessage().startswith("Config seed")]
        assert await conn.fetchval("SELECT count(*) FROM config WHERE tenant_code = 'mitra'") > 0
    _run(body, rows=False)


def test_a_database_without_collisions_gets_the_new_index_and_loses_the_old_one_at_startup():
    async def body(conn, ids):
        await _startup()
        names = await _index_names(conn)
        assert INDEX in names and LEGACY_INDEX not in names and "uq_config_source_type_program_session_type" not in names
        # still nothing guessed: the persona keys stay NULL until the operator maps them
        assert await conn.fetchval("SELECT count(*) FROM config WHERE session_type IS NOT NULL") == 0
    _run(body)


def test_startup_copies_a_session_type_that_the_config_json_already_names_and_nothing_else():
    async def body(conn, ids):
        config = legacy_mitra()
        config["session_type"] = "Alpha_Voice"
        await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1",
                           ids["alpha_mitra"], json.dumps(config))
        bad = legacy_mitra()
        bad["session_type"] = "has a space"
        await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1",
                           ids["beta_mitra"], json.dumps(bad))
        await _startup()
        rows = {r["id"]: r["session_type"] for r in await conn.fetch("SELECT id, session_type FROM config")}
        assert rows[ids["alpha_mitra"]] == "Alpha_Voice"
        assert rows[ids["beta_mitra"]] is None and rows[ids["alpha_forms"]] is None
    _run(body)


def test_a_fresh_database_is_what_it_was_one_index_no_check_not_null_columns(caplog):
    async def body(conn, ids):
        with caplog.at_level(logging.WARNING, logger="analytics_service.db"):
            await _startup()
            await _startup()
        assert {n for n in await _index_names(conn) if n.startswith("uq_")} == {INDEX}
        definition = await conn.fetchval("SELECT indexdef FROM pg_indexes WHERE indexname = $1", INDEX)
        assert definition.startswith("CREATE UNIQUE INDEX") and "tenant_code, config_source, config_type" in definition
        assert f"COALESCE(program_id, '{NIL}'::uuid)" in definition and "COALESCE(session_type, ''::text)" in definition
        assert await conn.fetchval(
            "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config'::regclass AND contype = 'c'") == 0
        for table, column in (("config", "session_type"), ("feedback_submissions", "session_type"),
                              ("feedback_submissions", "questionnaire_version")):
            assert await conn.fetchval(
                "SELECT is_nullable FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
                table, column) == "NO"
        assert not [r for r in caplog.records if "Legacy feedback data" in r.getMessage()
                    or "unique index" in r.getMessage()]
    _run(body, legacy=False, rows=False)


# ------------------------------------------------------------------------------------------------ A1: free sources

@pytest.mark.parametrize("check_name", ["config_config_source_check", "config_source_whitelist"])
def test_the_source_check_is_dropped_on_an_old_database_whatever_it_is_called_and_a_source_is_a_free_label(check_name):
    async def body(conn, ids):
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute("UPDATE config SET config_source = 'ptm_app' WHERE id = $1", ids["alpha_mitra"])
        await _startup()
        await _startup()                                   # idempotent
        assert await conn.fetchval(
            "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config'::regclass AND contype = 'c'") == 0
        for source in ("ptm_app", "any", "mitra"):
            await conn.execute(
                "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
                "VALUES ($1, 'x', 'feedback_upload', NULL, $2, $3)", source, TENANT, f"S_{source}")
    _run(body, check_name=check_name)


# ------------------------------------------------------------------------------------------------ the unique index

def test_two_tenants_can_each_hold_a_program_less_default_config_but_one_tenant_only_one():
    async def body(conn, ids):
        await _startup()
        await _tenant(conn, "other")
        insert = ("INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
                  "VALUES ('mitra', 'default', 'feedback_upload', NULL, $1, 'Alpha_Voice')")
        await conn.execute(insert, TENANT)
        await conn.execute(insert, "other")                # the other tenant is not blocked by the first one's default
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(insert, TENANT)
        # a program-bound config is just as unique within its tenant and key
        bound = ("INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
                 "VALUES ('mitra', 'bound', 'feedback_upload', $1::uuid, $2, 'Alpha_Voice')")
        await conn.execute(bound, ALPHA["program"], TENANT)
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(bound, ALPHA["program"], TENANT)
        # a NULL session_type counts as one value: two such rows of one key collide as well
        nulls = ("INSERT INTO config (config_source, name, config_type, program_id, tenant_code) "
                 "VALUES ('mitra', 'null', 'feedback_upload', NULL, $1)")
        await conn.execute(nulls, "other")
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(nulls, "other")
    _run(body, rows=True)


def test_a_database_created_with_the_first_session_type_index_gets_the_tenant_wide_one():
    """The index of the commit before this fix had no tenant column and the same kind of name."""
    async def body(conn, ids):
        await conn.execute(f"DROP INDEX {LEGACY_INDEX}")
        await conn.execute("ALTER TABLE config ADD COLUMN session_type TEXT")
        await conn.execute(f"CREATE UNIQUE INDEX uq_config_source_type_program_session_type ON config "
                           f"(config_source, config_type, COALESCE(program_id, '{NIL}'), COALESCE(session_type, ''))")
        await _startup()
        names = await _index_names(conn)
        assert INDEX in names and "uq_config_source_type_program_session_type" not in names
    _run(body)


# ------------------------------------------------------------------------------------------------ C2: the migration

def _all_mapping():
    return {f"leader:{ALPHA['name']}": ALPHA["session_type"], f"leader:{BETA['name']}": BETA["session_type"]}


def test_an_old_database_is_unusable_until_migrated_and_the_health_check_says_so_with_the_command(caplog):
    async def body(conn, ids):
        with caplog.at_level(logging.ERROR, logger="analytics_service.db"):
            await _startup()
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        legacy = [e for e in errors if e.startswith("Legacy feedback data")]
        assert len(legacy) == 1                                      # ONE error, not one per config
        text = legacy[0]
        assert f"{up.LEGACY_MIGRATE_COMMAND} --tenant-code {TENANT}" in text and "--dry-run" in text
        assert f"--map {ALPHA['program']}=<SESSION_TYPE>" in text and f"--map {BETA['program']}=<SESSION_TYPE>" in text
        assert "5 stored session(s) have no session_type and 5 no questionnaire_version" in text
        assert "4 config(s) lack required keys" not in text and "3 config(s) lack required keys" in text
        assert len(errors) == 1                                      # the per-config lines are folded into it
        # what the report says is true: an upload and a sweep of the old data fail
        async def before():
            with pytest.raises(InvalidFeedbackFile):
                await load_feedback_target("Alpha Program", "Alpha", TENANT, "mitra")
            with pytest.raises(ValueError):
                async with db.pool.acquire() as c:
                    await fo.get_feedback_process_steps(c, "a-1001", TENANT)
        await _with_pool(before)
    _run(body)


def test_the_old_migrate_alone_leaves_restamp_aborting_and_migrate_legacy_fixes_it():
    """Review finding C2: `migrate` added `adapter` only, so every Mitra config looked versioned but had no versions."""
    async def body(conn, ids):
        await _startup()
        await _with_pool(fo.migrate_feedback_config)
        with pytest.raises(ValueError, match="questionnaire_versions"):
            await _with_pool(lambda: fo.restamp_questionnaire_versions(tenant_code=TENANT, dry_run=True))
        await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        result = await _with_pool(lambda: fo.restamp_questionnaire_versions(tenant_code=TENANT, dry_run=True))
        # CHANGED (was: 0): the legacy sessions carry bot id 90, so their label is the rank of that bot (V1, the only
        # bot of its persona) and not a window: Beta's b-1002, which is after the V2 window, would be relabelled V1
        assert result["changed"] == 1 and len(result["session_types"]) == 2
        assert sum(e["bot_sessions"]["changed"] for e in result["session_types"].values()) == 1
    _run(body)


def test_migrate_legacy_brings_the_old_database_over_and_a_second_run_changes_nothing(caplog):
    async def body(conn, ids):
        await _startup()
        before_versions = await conn.fetchval("SELECT count(*) FROM config_version")

        async def go():
            dry = await up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping(), dry_run=True,
                                          created_by="op")
            after_dry = await _snapshot(conn)
            real = await up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping(), created_by="op")
            return dry, after_dry, real
        snapshot_before = await _snapshot(conn)
        dry, after_dry, real = await _with_pool(go)

        # a dry run reports the work and writes nothing
        assert after_dry == snapshot_before
        assert dry["dry_run"] is True and dry["configs"]["new_versions"] == 3 and dry["configs"]["session_type_set"] == 3
        assert dry["unique_index"] == "exists"
        # the real run
        assert real["configs"] == {"total": 3, "session_type_set": 3, "new_versions": 3, "unchanged": 0, "unmapped": 0,
                                   "duplicates": 0, "conflicts": 0, "invalid": 0, "no_active_version": 0}
        assert real["sessions"] == {"session_type_set": 5, "questionnaire_version_set": 5,
                                    "session_type_unresolved": 0, "questionnaire_version_unresolved": 0}
        assert real["problems"] == [] and real["retired"] == 0
        rows = {r["id"]: r["session_type"] for r in await conn.fetch("SELECT id, session_type FROM config")}
        assert rows == {ids["alpha_mitra"]: "Alpha_Voice", ids["alpha_forms"]: "Alpha_Voice",
                        ids["beta_mitra"]: "Beta_Voice"}
        # new versions: the old one is kept as inactive history, the new one is active and carries the keys
        assert await conn.fetchval("SELECT count(*) FROM config_version") == before_versions + 3
        for name in ("alpha_mitra", "alpha_forms", "beta_mitra"):
            versions = await conn.fetch("SELECT version, is_active, created_by, change_note FROM config_version "
                                        "WHERE config_id = $1 ORDER BY version", ids[name])
            assert [(v["version"], v["is_active"]) for v in versions] == [(1, False), (2, True)]
            assert versions[1]["created_by"] == "op" and versions[1]["change_note"].startswith("legacy upgrade: added ")
        version, mitra = await _active(conn, ids["alpha_mitra"])
        assert version == 2 and mitra["session_type"] == "Alpha_Voice" and mitra["adapter"] == "conversation_rows"
        assert mitra["questionnaire_versions"] == [{"key": "V1", "effective_from": None}]
        assert mitra["opening_row"] == {"mode": "auto"} and mitra["completion_map"]["COMPLETED"] == "COMPLETED"
        assert mitra["column_map"]["stage"] == "stage" and mitra["process_steps"] == []
        assert mitra["columns"] == legacy_mitra()["columns"]               # nothing the config already had is touched
        _, beta = await _active(conn, ids["beta_mitra"])
        assert beta["questionnaire_versions"] == _beta_mitra()["questionnaire_versions"]    # an existing key is kept
        _, forms = await _active(conn, ids["alpha_forms"])
        assert forms["adapter"] == "wide_form" and {f["questionnaire_version"] for f in forms["forms"].values()} == {"V1"}
        # the stored sessions
        labels = {r["submission_id"]: (r["session_type"], r["questionnaire_version"]) for r in await conn.fetch(
            "SELECT submission_id, session_type, questionnaire_version FROM feedback_submissions")}
        assert labels == {"a-1001": ("Alpha_Voice", "V1"), "a-1002": ("Alpha_Voice", "V1"),
                          "a-2001": ("Alpha_Voice", "V1"),
                          "b-1001": ("Beta_Voice", "V1"), "b-1002": ("Beta_Voice", "V2")}   # by the config's V2 window
        # the unique index exists and the old one is gone
        assert INDEX in await _index_names(conn)

        # everything that was broken works now: upload target, file check, process steps, health
        async def after():
            target = await load_feedback_target("Alpha Program", "Alpha", TENANT, "mitra")
            assert target["active"]["session_type"] == "Alpha_Voice" and target["adapter"] == "conversation_rows"
            await load_feedback_target("Alpha Program", "Alpha", TENANT, "google_forms")
            roles = list(mitra["column_map"])
            header = ",".join(mitra["column_map"][r] for r in roles)
            row = ",".join({"session_type": "Alpha_Voice", "session_status": "COMPLETED", "session": "x" * 32,
                            "submission_id": "1", "language": "en", "sequence": "1", "message": "hi",
                            "created_at": "2026-10-05T10:00:00+00:00", "stage": "S", "sender_id": "5",
                            "bot_id": "90"}.get(r, "")
                           for r in roles)
            df = feedback_uploads.parse_feedback_csv(f"{header}\n{row}\n".encode())
            assert feedback_uploads.check_feedback_file(df, mitra, "Alpha_Voice") == []
            restamp = await fo.restamp_questionnaire_versions(tenant_code=TENANT)      # the documented follow-up works
            # CHANGED (was: 0, "the labels are already right"): the migration stamps the windows, and the sessions
            # carry bot id 90, so the follow-up `restamp` puts the bot-rank label on Beta's b-1002 (V2 -> V1)
            assert restamp["changed"] == 1
            async with db.pool.acquire() as c:
                assert await fo.get_feedback_process_steps(c, "a-1001", TENANT) == []
                assert await fo.get_feedback_process_steps(c, "b-1002", TENANT) == []
            caplog.clear()
            with caplog.at_level(logging.INFO, logger="analytics_service.db"):
                async with db.pool.acquire() as c:
                    state = await health.check_legacy_feedback_data(c)
                    assert up.legacy_state_is_clean(state) and not state["index_missing"]
                    assert await health.check_feedback_configs(c) == []
            assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        await _with_pool(after)

        # idempotent: a second run (and a restart) changes nothing
        snap = await _snapshot(conn)
        again = await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        assert again["configs"]["new_versions"] == 0 and again["configs"]["session_type_set"] == 0
        assert again["configs"]["unchanged"] == 3 and again["problems"] == []
        assert again["sessions"]["session_type_set"] == 0 and again["sessions"]["questionnaire_version_set"] == 0
        assert again["unique_index"] == "exists"
        await _startup()
        assert await _snapshot(conn) == snap
    _run(body)


def test_the_result_holds_counts_and_identifiers_only():
    async def body(conn, ids):
        await _startup()
        result = await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        text = json.dumps(result)
        for secret in ("Hello", "message", "Alpha_Voice", "THANK_YOU", "AGE_IDENTIFICATION"):
            assert secret not in text
    _run(body)


def test_a_program_the_mapping_does_not_cover_is_reported_as_unmapped_and_left_untouched():
    async def body(conn, ids):
        await _startup()
        before = await conn.fetchval("SELECT config::text FROM config_version WHERE config_id = $1 AND is_active",
                                     ids["beta_mitra"])
        result = await _with_pool(lambda: up.migrate_legacy(
            tenant_code=TENANT, session_type_map={f"leader:{ALPHA['name']}": ALPHA["session_type"]}))
        assert result["configs"]["unmapped"] == 1 and result["configs"]["new_versions"] == 2
        assert [p["config_id"] for p in result["problems"]] == [str(ids["beta_mitra"])]
        assert "--map" in result["problems"][0]["problem"]
        assert await conn.fetchval("SELECT session_type FROM config WHERE id = $1", ids["beta_mitra"]) is None
        assert await conn.fetchval("SELECT config::text FROM config_version WHERE config_id = $1 AND is_active",
                                   ids["beta_mitra"]) == before
        assert await conn.fetchval("SELECT count(*) FROM feedback_submissions WHERE session_type IS NULL") == 2
        # the rest of the work is done and the index (no collision) exists
        assert result["sessions"]["session_type_set"] == 3 and result["sessions"]["session_type_unresolved"] == 2
        assert result["unique_index"] == "exists"
    _run(body)


def test_a_bad_mapping_is_refused_before_anything_is_written():
    async def body(conn, ids):
        await _startup()
        before = await _snapshot(conn)
        for mapping, needle in (
                ({"leader:Nobody": "X_Voice"}, "matches no program"),
                ({"No Such Program": "X_Voice"}, "matches no program"),
                ({str(uuid.uuid4()): "X_Voice"}, "is not a program of the tenant"),
                ({"leader:Alpha": "has space"}, "must match")):
            with pytest.raises(ValueError, match=needle):
                if needle == "must match":
                    up.parse_session_type_map([f"{next(iter(mapping))}={mapping[next(iter(mapping))]}"])
                else:
                    await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=mapping))
        with pytest.raises(ValueError, match="does not exist"):
            await _with_pool(lambda: up.migrate_legacy(tenant_code="nobody", session_type_map={}))
        with pytest.raises(ValueError, match="tenant_code"):
            await _with_pool(lambda: up.migrate_legacy(tenant_code="a b", session_type_map={}))
        assert await _snapshot(conn) == before
    _run(body)


def test_a_session_type_that_the_column_the_config_and_the_mapping_disagree_on_is_a_conflict_not_a_guess():
    async def body(conn, ids):
        config = legacy_mitra()
        config["session_type"] = "Other_Voice"
        await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1",
                           ids["alpha_mitra"], json.dumps(config))
        await _startup()
        result = await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        assert result["configs"]["conflicts"] == 1
        assert [p["config_id"] for p in result["problems"]] == [str(ids["alpha_mitra"])]
        # startup copied the JSON's own value into the column (deterministic); the mapping did not override it
        assert await conn.fetchval("SELECT session_type FROM config WHERE id = $1", ids["alpha_mitra"]) == "Other_Voice"
        assert (await _active(conn, ids["alpha_mitra"]))[0] == 1
    _run(body)


def test_a_source_that_is_not_a_legacy_one_never_gets_an_invented_adapter():
    async def body(conn, ids):
        await conn.execute("ALTER TABLE config DROP CONSTRAINT config_config_source_check")
        await conn.execute("UPDATE config SET config_source = 'ptm_app' WHERE id = $1", ids["alpha_mitra"])
        await _startup()
        result = await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        assert result["configs"]["invalid"] == 1 and result["configs"]["new_versions"] == 2
        problem = next(p for p in result["problems"] if p["config_id"] == str(ids["alpha_mitra"]))
        assert "adapter" in problem["problem"] and problem["source"] == "ptm_app"
        assert (await _active(conn, ids["alpha_mitra"]))[0] == 1
    _run(body)


def test_the_migration_is_tenant_scoped():
    async def body(conn, ids):
        other_persona = {"leader": str(uuid.uuid4()), "program": str(uuid.uuid4()), "name": "Gamma",
                         "session_type": "Gamma_Voice"}
        await _tenant(conn, "other")
        await _add_persona(conn, "other", other_persona)
        other_config = await _add_config(conn, "other", other_persona, "mitra", "7", legacy_mitra())
        await _add_session(conn, "other", other_persona, "mitra", "g-1001", "2026-10-05T10:00:00+00:00")
        await _startup()
        snap_other = (await conn.fetchval("SELECT config::text FROM config_version WHERE config_id = $1", other_config))
        # another tenant's program can not be named
        with pytest.raises(ValueError, match="is not a program of the tenant"):
            await _with_pool(lambda: up.migrate_legacy(
                tenant_code=TENANT, session_type_map={other_persona["program"]: "Gamma_Voice"}))
        result = await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        assert result["configs"]["total"] == 3
        assert await conn.fetchval("SELECT session_type FROM config WHERE id = $1", other_config) is None
        assert await conn.fetchval("SELECT config::text FROM config_version WHERE config_id = $1", other_config) == snap_other
        assert await conn.fetchval(
            "SELECT session_type FROM feedback_submissions WHERE submission_id = 'g-1001'") is None
        # and the other tenant is migrated by its own run
        result = await _with_pool(lambda: up.migrate_legacy(
            tenant_code="other", session_type_map={f"leader:{other_persona['name']}": "Gamma_Voice"}))
        assert result["configs"]["session_type_set"] == 1 and result["sessions"]["session_type_set"] == 1
    _run(body)


def test_the_source_selector_limits_the_run_to_one_source():
    async def body(conn, ids):
        await _startup()
        result = await _with_pool(lambda: up.migrate_legacy(
            tenant_code=TENANT, session_type_map=_all_mapping(), source="google_forms"))
        assert result["configs"]["total"] == 1 and result["configs"]["new_versions"] == 1
        assert await conn.fetchval("SELECT count(*) FROM config WHERE session_type IS NOT NULL") == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM feedback_submissions WHERE session_type IS NOT NULL") == 1
    _run(body)


# ------------------------------------------------------------------------------------------------ C1/C2: duplicates

def test_two_bot_rows_for_one_program_are_reported_and_one_is_retired_by_name():
    async def body(conn, ids):
        await _startup()
        before = await _snapshot(conn)
        first = await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        # the two Alpha Mitra rows would share one key: neither is written, both are named, the index waits
        assert first["configs"]["duplicates"] == 2 and first["unique_index"] == "blocked"
        assert first["configs"]["new_versions"] == 2           # the configs that do not collide are migrated anyway
        assert sorted(p["config_id"] for p in first["problems"]) == sorted(
            [str(ids["alpha_mitra"]), str(ids["alpha_mitra_2"])])
        assert await conn.fetchval("SELECT session_type FROM config WHERE id = $1", ids["alpha_mitra"]) is None
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1", ids["alpha_mitra"]) == 1
        # a wrong id retires nothing; the persona's only config of a source can not be retired
        wrong = await _with_pool(lambda: up.migrate_legacy(
            tenant_code=TENANT, session_type_map=_all_mapping(), retire_config_ids=[str(uuid.uuid4())]))
        assert wrong["retire_not_found"] == 1 and wrong["retired"] == 0
        with pytest.raises(ValueError, match="only config of its source"):
            await _with_pool(lambda: up.migrate_legacy(
                tenant_code=TENANT, session_type_map=_all_mapping(), retire_config_ids=[str(ids["beta_mitra"])]))
        assert await conn.fetchval("SELECT count(*) FROM config WHERE id = $1", ids["beta_mitra"]) == 1   # rolled back
        # the same run that retires the duplicate finishes the job
        dry = await _with_pool(lambda: up.migrate_legacy(
            tenant_code=TENANT, session_type_map=_all_mapping(), retire_config_ids=[str(ids["alpha_mitra_2"])],
            dry_run=True))
        assert dry["retired"] == 1 and dry["unique_index"] == "created"
        assert await conn.fetchval("SELECT count(*) FROM config WHERE id = $1", ids["alpha_mitra_2"]) == 1   # dry run
        done = await _with_pool(lambda: up.migrate_legacy(
            tenant_code=TENANT, session_type_map=_all_mapping(), retire_config_ids=[str(ids["alpha_mitra_2"])]))
        assert done["retired"] == 1 and done["unique_index"] == "created" and done["problems"] == []
        # the first run already migrated the configs that did not collide; only the survivor of the pair is new
        assert done["configs"]["total"] == 3 and done["configs"]["new_versions"] == 1
        assert await conn.fetchval("SELECT count(*) FROM config WHERE id = $1", ids["alpha_mitra_2"]) == 0
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1", ids["alpha_mitra_2"]) == 0
        names = await _index_names(conn)
        assert INDEX in names and LEGACY_INDEX not in names
        assert await conn.fetchval("SELECT bot_id FROM config WHERE id = $1", ids["alpha_mitra"]) == "90"   # bot id kept
        # the next startup is quiet
        again = await _with_pool(lambda: up.migrate_legacy(
            tenant_code=TENANT, session_type_map=_all_mapping(), retire_config_ids=[str(ids["alpha_mitra_2"])]))
        assert again["retire_not_found"] == 1 and again["configs"]["new_versions"] == 0
        assert before is not None
    _run(body, second_bot=True)


def test_a_retired_id_of_another_tenant_or_a_non_upload_config_is_not_touched():
    async def body(conn, ids):
        await _startup()
        await _tenant(conn, "other")
        foreign = await conn.fetchval(
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ('mitra', 'x', 'feedback_upload', NULL, 'other', 'X_Voice') RETURNING id")
        other_type = await conn.fetchval(
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ('any', 'x', 'feedback_extraction', NULL, $1, 'X_Voice') RETURNING id", TENANT)
        result = await _with_pool(lambda: up.migrate_legacy(
            tenant_code=TENANT, session_type_map=_all_mapping(), retire_config_ids=[str(foreign), str(other_type)]))
        assert result["retired"] == 0 and result["retire_not_found"] == 2
        assert await conn.fetchval("SELECT count(*) FROM config WHERE id = ANY($1::uuid[])", [foreign, other_type]) == 2
    _run(body)


# ------------------------------------------------------------------------------------------------ the tool and the checks

@pytest.fixture
def tool_env(monkeypatch):
    monkeypatch.setattr(settings, "DATABASE_URL", DSN)
    monkeypatch.setattr(getpass, "getuser", lambda: "tool-operator")


def test_the_tool_runs_migrate_legacy_end_to_end_with_a_map_file(tool_env, tmp_path, capsys):
    async def prepare(conn, ids):
        await _startup()
    _run(prepare)
    map_file = tmp_path / "map.json"
    map_file.write_text(json.dumps(_all_mapping()))
    argv = ["migrate-legacy", "--tenant-code", TENANT, "--map-file", str(map_file)]
    assert cli.main(argv + ["--dry-run"]) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["dry_run"] is True and dry["configs"]["new_versions"] == 3
    assert cli.main(argv) == 0
    real = json.loads(capsys.readouterr().out)
    assert real["configs"]["session_type_set"] == 3 and real["sessions"]["session_type_set"] == 5

    assert cli.main(argv) == 0 and json.loads(capsys.readouterr().out)["configs"]["new_versions"] == 0

    async def authors():
        conn = await asyncpg.connect(DSN)
        try:
            return {r["created_by"] for r in await conn.fetch("SELECT created_by FROM config_version WHERE version = 2")}
        finally:
            await conn.close()
    assert asyncio.run(authors()) == {"tool-operator"}
    # a mapping typo is an error line and exit code 1, not a traceback
    assert cli.main(["migrate-legacy", "--tenant-code", TENANT, "--map", "leader:Typo=A_Voice"]) == 1
    assert "matches no program" in capsys.readouterr().err


def test_show_and_set_validate_the_adapter_content_and_the_health_check_reports_it(tool_env, capsys, caplog):
    async def prepare(conn, ids):
        await _startup()
        await _with_pool(lambda: up.migrate_legacy(tenant_code=TENANT, session_type_map=_all_mapping()))
        config = (await _active(conn, ids["alpha_mitra"]))[1]
        config["column_map"]["no_such_role"] = "x"
        await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1 AND is_active",
                           ids["alpha_mitra"], json.dumps(config))

    async def run_health():
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            async with pool.acquire() as c:
                return await health.check_feedback_configs(c)
        finally:
            await pool.close()

    _run(prepare)
    # show: the problem is listed under adapter_config, for that config only
    assert cli.main(["show", "--tenant-code", TENANT]) == 0
    configs = json.loads(capsys.readouterr().out)["configs"]
    bad = [c for c in configs if "adapter_config" in c["problems"]]
    assert [(c["session_type"], c["source"]) for c in bad] == [("Alpha_Voice", "mitra")]
    assert "no_such_role" in bad[0]["problems"]["adapter_config"]
    # the startup health check: one ERROR for that config
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="analytics_service.db"):
        invalid = asyncio.run(run_health())
    assert [(i["session_type"], i["source"]) for i in invalid] == [("Alpha_Voice", "mitra")]
    assert "adapter_config" in invalid[0]["problems"]
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1
    # set --key adapter: a switch the config would not survive is refused and nothing is written
    selectors = ["--session-type", "Beta_Voice", "--source", "mitra", "--tenant-code", TENANT, "--key", "adapter"]
    assert cli.main(["set", *selectors, "--json", '"wide_form"']) == 1
    assert "invalid after this change" in capsys.readouterr().err
    assert cli.main(["set", *selectors, "--json", '"conversation_rows"']) == 0        # the unchanged value is accepted
    assert json.loads(capsys.readouterr().out)["changed"] is False
