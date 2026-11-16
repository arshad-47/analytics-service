"""
Real-Postgres tests of seed_config.sql executed the way app/database/db.py executes it: the seed creates the 4 leader
categories, 4 programs, 11 configs and their 11 active version-1 rows; running it again changes nothing; it writes
nothing while tenant 'mitra' does not exist; it never overwrites or resurrects what a user changed (a new
config_version, a deactivated version 1, renamed rows, a config an operator created under another id); an already
seeded database is a no-op; the documented "new config_version" recipe keeps one active version per config; and
Database.initialize_schema runs it.

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards) (the tests wipe the seeded tables before each case). Synthetic data only.
"""
import asyncio
import json
import logging

import asyncpg
import pytest
import pg_support

from app.config import settings
from app.database.db import db
from seed_support import PERSONAS, extraction_config_id, extraction_config_v1, mitra_config_v1, seed_config, seed_config_blocks

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

BLOCKS = seed_config_blocks()
YOUTH = PERSONAS["Youth"]
PARENT = PERSONAS["Parent"]
COUNTS = {"leader_category": 4, "programs": 4, "config": 11, "config_version": 11}


def _run(body, tenant=True):
    """Runs `body(conn)` on the scratch database: schema applied, the seeded tables wiped, tenant 'mitra' present or not."""
    async def main():
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(pg_support.schema_sql())
            for table in ("config_version", "config", "programs", "leader_category", "tenant"):
                await conn.execute(f"DELETE FROM {table}")
            if tenant:
                await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra')")
            return await body(conn)
        finally:
            await conn.close()
    return asyncio.run(main())


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


async def _counts(conn):
    return {t: await conn.fetchval(f"SELECT count(*) FROM {t}") for t in COUNTS}


async def _snapshot(conn):
    """Every seeded row with its own ids and timestamps: an untouched row is byte-identical."""
    out = {}
    for table in ("leader_category", "programs", "config", "config_version"):
        rows = await conn.fetch(f"SELECT * FROM {table} ORDER BY 1")
        out[table] = [{k: (json.dumps(_json(v), sort_keys=True) if k == "config" and table == "config_version" else str(v))
                       for k, v in dict(r).items()} for r in rows]
    return out


async def _versions(conn, config_id):
    rows = await conn.fetch("SELECT version, is_active, config, created_by FROM config_version WHERE config_id = $1::uuid "
                            "ORDER BY version", config_id)
    return [(r["version"], r["is_active"], _json(r["config"]), r["created_by"]) for r in rows]


# ------------------------------------------------------------------------------------------------ what it writes

def test_the_seed_writes_4_categories_4_programs_11_configs_and_11_active_version_1_rows():
    async def body(conn):
        await seed_config(conn)
        assert await _counts(conn) == COUNTS
        rows = await conn.fetch("SELECT config_id::text, version, is_active, created_by FROM config_version")
        assert {(r["version"], r["is_active"], r["created_by"]) for r in rows} == {(1, True, "seed")}
        assert len({r["config_id"] for r in rows}) == 11
        assert await conn.fetchval("SELECT count(*) FROM tenant") == 1                 # never creates the tenant
    _run(body)


def test_every_row_equals_the_block_it_was_written_from_ids_names_flags_and_jsonb_content():
    async def body(conn):
        await seed_config(conn)
        for persona, p in PERSONAS.items():
            lc = await conn.fetchrow("SELECT name, tenant_code FROM leader_category WHERE id = $1::uuid", p["leader_category_id"])
            assert (lc["name"], lc["tenant_code"]) == (persona, "mitra")
            prog = await conn.fetchrow("SELECT name, leaders_id::text AS leader, tenant_code FROM programs WHERE id = $1::uuid",
                                       p["program_id"])
            assert (prog["name"], prog["leader"], prog["tenant_code"]) == ("1M Voice", p["leader_category_id"], "mitra")
        for block in BLOCKS:
            row = await conn.fetchrow(
                "SELECT config_source, name, config_type, program_id::text AS program_id, tenant_code, leader_id, session_type, "
                "bot_id, bot_name FROM config WHERE id = $1::uuid", block["id"])
            assert dict(row) == {
                "config_source": block["config_source"], "name": block["name"], "config_type": block["config_type"],
                "program_id": block["program_id"], "tenant_code": "mitra", "leader_id": None,
                "session_type": block["session_type"], "bot_id": block["bot_id"], "bot_name": block["bot_name"]}
            version = await conn.fetchrow(
                "SELECT version, is_active, created_by, change_note, config = $2::jsonb AS same FROM config_version "
                "WHERE config_id = $1::uuid", block["id"], json.dumps(block["config"]))
            assert (version["version"], version["is_active"], version["created_by"], version["change_note"]) == (
                1, True, "seed", block["change_note"])
            assert version["same"] is True                                             # jsonb equality inside Postgres
    _run(body)


def test_the_seeded_configs_are_the_ones_the_loaders_find():
    async def body(conn):
        await seed_config(conn)
        active = await conn.fetchval(
            "SELECT cv.config FROM config c JOIN config_version cv ON cv.config_id = c.id AND cv.is_active "
            "WHERE c.config_type = 'feedback_upload' AND c.config_source = 'mitra' AND c.session_type = $1", YOUTH["session_type"])
        assert _json(active) == mitra_config_v1("Youth")
        ext = await conn.fetchval("SELECT config FROM config_version WHERE config_id = $1::uuid", extraction_config_id("Parent_Voice"))
        assert _json(ext) == extraction_config_v1("Parent")
    _run(body)


# ------------------------------------------------------------------------------------------------ idempotent

def test_running_it_twice_or_thrice_changes_nothing_not_even_a_version_row_id_or_timestamp():
    async def body(conn):
        await seed_config(conn)
        first = await _snapshot(conn)
        await seed_config(conn)
        await seed_config(conn)
        assert await _snapshot(conn) == first
        assert await _counts(conn) == COUNTS
    _run(body)


def test_the_file_is_one_script_that_one_execute_runs_inside_a_transaction_and_rolls_back_cleanly():
    async def body(conn):
        tx = conn.transaction()
        await tx.start()
        await seed_config(conn)
        assert await _counts(conn) == COUNTS
        await tx.rollback()
        assert await _counts(conn) == {t: 0 for t in COUNTS}
    _run(body)


# ------------------------------------------------------------------------------------------------ tenant missing

def test_without_the_tenant_it_writes_nothing_and_does_not_fail():
    async def body(conn):
        await seed_config(conn)
        assert await _counts(conn) == {t: 0 for t in COUNTS}
        assert await conn.fetchval("SELECT count(*) FROM tenant") == 0
    _run(body, tenant=False)


def test_the_seed_applies_once_the_tenant_exists_later():
    async def body(conn):
        await seed_config(conn)
        assert await _counts(conn) == {t: 0 for t in COUNTS}
        await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra')")
        await seed_config(conn)
        assert await _counts(conn) == COUNTS
    _run(body, tenant=False)


def test_another_tenant_alone_does_not_trigger_the_seed():
    async def body(conn):
        await conn.execute("INSERT INTO tenant (name, code) VALUES ('Other', 'other')")
        await seed_config(conn)
        assert await _counts(conn) == {t: 0 for t in COUNTS}
    _run(body, tenant=False)


# ------------------------------------------------------------------------------------------------ never overwrites

def test_a_user_edit_a_new_active_version_with_version_1_kept_inactive_is_never_touched():
    async def body(conn):
        await seed_config(conn)
        cid = extraction_config_id("Youth_Voice")
        edited = extraction_config_v1("Youth")
        edited["fields"] = edited["fields"][:3]
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", cid)
        await conn.execute("INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
                           "VALUES ($1::uuid, 2, TRUE, $2::jsonb, 'ops', 'fewer fields')", cid, json.dumps(edited))
        before = await _snapshot(conn)
        await seed_config(conn)
        await seed_config(conn)
        assert await _snapshot(conn) == before
        versions = await _versions(conn, cid)
        assert [(v[0], v[1]) for v in versions] == [(1, False), (2, True)] and versions[1][2] == edited
    _run(body)


def test_version_1_removed_while_a_later_version_exists_is_not_resurrected_and_the_active_index_holds():
    async def body(conn):
        await seed_config(conn)
        await conn.execute("DELETE FROM config_version WHERE config_id = $1::uuid", PARENT["config_id"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 2, '{}'::jsonb)", PARENT["config_id"])
        await seed_config(conn)                               # an unguarded v1 insert would raise on uq_config_version_active
        assert [(v[0], v[1]) for v in await _versions(conn, PARENT["config_id"])] == [(2, True)]
    _run(body)


def test_a_config_whose_every_version_was_deleted_gets_version_1_again_like_a_fresh_one():
    async def body(conn):
        await seed_config(conn)
        await conn.execute("DELETE FROM config_version WHERE config_id = $1::uuid", PARENT["config_id"])
        await seed_config(conn)
        assert [(v[0], v[1], v[3]) for v in await _versions(conn, PARENT["config_id"])] == [(1, True, "seed")]
    _run(body)


def test_renamed_rows_and_an_edited_name_survive():
    async def body(conn):
        await seed_config(conn)
        await conn.execute("UPDATE config SET name = 'edited', bot_id = '999' WHERE id = $1::uuid", YOUTH["config_id"])
        await conn.execute("UPDATE programs SET name = 'renamed' WHERE id = $1::uuid", YOUTH["program_id"])
        await conn.execute("UPDATE leader_category SET name = 'Renamed' WHERE id = $1::uuid", YOUTH["leader_category_id"])
        await seed_config(conn)
        assert await conn.fetchval("SELECT name FROM config WHERE id = $1::uuid", YOUTH["config_id"]) == "edited"
        assert await conn.fetchval("SELECT bot_id FROM config WHERE id = $1::uuid", YOUTH["config_id"]) == "999"
        assert await conn.fetchval("SELECT name FROM programs WHERE id = $1::uuid", YOUTH["program_id"]) == "renamed"
        assert await conn.fetchval("SELECT name FROM leader_category WHERE id = $1::uuid", YOUTH["leader_category_id"]) == "Renamed"
        assert await _counts(conn) == COUNTS
    _run(body)


def test_a_config_an_operator_created_under_another_id_for_the_same_key_is_left_alone_and_nothing_fails():
    async def body(conn):
        # the program and the leader category exist (an older seed), the Youth Mitra config was created by hand
        await seed_config(conn)
        await conn.execute("DELETE FROM config WHERE id = $1::uuid", YOUTH["config_id"])
        manual = await conn.fetchval(
            "INSERT INTO config (config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ('mitra', 'manual', 'feedback_upload', $1::uuid, 'mitra', $2) RETURNING id::text",
            YOUTH["program_id"], YOUTH["session_type"])
        await conn.execute("INSERT INTO config_version (config_id, version, config) VALUES ($1::uuid, 1, '{\"manual\": true}'::jsonb)", manual)
        await seed_config(conn)
        assert await conn.fetchval("SELECT count(*) FROM config WHERE id = $1::uuid", YOUTH["config_id"]) == 0
        assert [(v[0], v[2]) for v in await _versions(conn, manual)] == [(1, {"manual": True})]
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid", YOUTH["config_id"]) == 0
    _run(body)


def test_an_already_seeded_database_is_a_no_op_for_a_partial_seed_too():
    async def body(conn):
        # a database that holds only some of the rows (an older seed): the rest is added, the existing rows stay
        await seed_config(conn)
        await conn.execute("DELETE FROM config WHERE session_type = 'Educator_Voice'")
        await conn.execute("UPDATE config SET name = 'kept' WHERE id = $1::uuid", PARENT["config_id"])
        await seed_config(conn)
        assert await _counts(conn) == COUNTS
        assert await conn.fetchval("SELECT name FROM config WHERE id = $1::uuid", PARENT["config_id"]) == "kept"
    _run(body)


# ------------------------------------------------------------------------------------------------ the new-version recipe

def test_the_documented_recipe_writes_a_new_active_version_and_the_seed_keeps_it():
    """docs/feedback-extraction-bands.md, 'Changing a band or a field': deactivate the active version, insert the next one."""
    async def body(conn):
        await seed_config(conn)
        cid = extraction_config_id("Educator_Voice")
        new = extraction_config_v1("Educator")
        new["fields"][4]["categories"] = new["fields"][4]["categories"][:-1]
        async with conn.transaction():
            await conn.execute("UPDATE config_version SET is_active = FALSE, updated_at = now() "
                               "WHERE config_id = $1::uuid AND is_active", cid)
            await conn.execute(
                "INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
                "SELECT $1::uuid, COALESCE(MAX(version), 0) + 1, TRUE, $2::jsonb, 'ops', 'drop the last band' "
                "FROM config_version WHERE config_id = $1::uuid", cid, json.dumps(new))
        await seed_config(conn)
        versions = await _versions(conn, cid)
        assert [(v[0], v[1], v[3]) for v in versions] == [(1, False, "seed"), (2, True, "ops")]
        assert versions[1][2] == new
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE is_active") == 11     # one active per config
        assert await conn.fetchval(
            "SELECT count(*) FROM (SELECT config_id FROM config_version WHERE is_active GROUP BY 1 HAVING count(*) > 1) t") == 0
    _run(body)


# ------------------------------------------------------------------------------------------------ the real startup path

def test_initialize_schema_runs_the_seed_and_warns_when_the_tenant_is_missing(monkeypatch, caplog):
    async def body(conn):
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        monkeypatch.setattr(settings, "RESET_DB", False)
        db.pool = pool
        try:
            with caplog.at_level(logging.WARNING, logger="analytics_service.db"):
                await db.initialize_schema()
            assert await _counts(conn) == {t: 0 for t in COUNTS}
            # truthful: the statements are guarded by the tenant (they insert nothing), the rest of the seed still ran
            seed_logs = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Config seed")]
            assert len(seed_logs) == 1 and "'mitra'" in seed_logs[0] and "insert nothing" in seed_logs[0]
            assert not any("skipped" in r.getMessage() for r in caplog.records)
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra')")
            await db.initialize_schema()
            await db.initialize_schema()                                        # every start runs it again
            assert await _counts(conn) == COUNTS
        finally:
            db.pool = None
            await pool.close()
    _run(body, tenant=False)
