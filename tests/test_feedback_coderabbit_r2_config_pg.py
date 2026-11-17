"""
Real-Postgres tests for the second CodeRabbit round on PR 36 (config / schema side). Synthetic data only.
Opt-in like every PG test: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server.
"""
import asyncio
import json

import asyncpg
import pytest

import factories
import pg_support
from app.database.db import db

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

TENANT = "acme"


def _run(body):
    """Runs `body(conn)` on the scratch database: the current schema, a clean slate, db.pool pointing at it."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
            await conn.execute(pg_support.schema_sql())
            pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
            db.pool = pool
            return await body(conn)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.close()
    return asyncio.run(main())


async def _legacy_config(conn, session_type, json_session_type, *, program_id=None, source="mitra"):
    """A config row of a database from before session_type existed (column NULL) or a labelled row; its active
    version names `json_session_type` (or nothing when None)."""
    config_id = factories.uid()
    await conn.execute(
        "INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, session_type) "
        "VALUES ($1::uuid, $2, 'x', 'feedback_upload', $3::uuid, $4, $5)",
        config_id, source, program_id, TENANT, session_type)
    body = {} if json_session_type is None else {"session_type": json_session_type}
    await conn.execute(
        "INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
        "VALUES ($1::uuid, 1, TRUE, $2::jsonb, 'test', 'test')", config_id, json.dumps(body))
    return config_id


async def _legacy_world(conn):
    await conn.execute("INSERT INTO tenant (name, code) VALUES ('Acme', $1)", TENANT)
    await conn.execute("ALTER TABLE config ALTER COLUMN session_type DROP NOT NULL")


def test_backfill_never_raises_when_the_target_key_is_already_held_by_a_labelled_row():
    async def body(conn):
        await _legacy_world(conn)
        labelled = await _legacy_config(conn, "Youth_Voice", "Youth_Voice")
        legacy = await _legacy_config(conn, None, "Youth_Voice")
        assert await conn.fetchval("SELECT ensure_config_unique_index()") is True
        assert await conn.fetchval("SELECT ensure_config_unique_index()") is True   # and on every later start
        rows = {str(r["id"]): r["session_type"] for r in await conn.fetch("SELECT id, session_type FROM config")}
        assert rows == {labelled: "Youth_Voice", legacy: None}        # the NULL row is left for migrate-legacy
    _run(body)


def test_two_unlabelled_rows_naming_one_key_both_stay_null_and_the_index_waits_for_migrate_legacy():
    async def body(conn):
        await _legacy_world(conn)
        await conn.execute("DROP INDEX uq_config_tenant_source_type_program_session_type")
        first = await _legacy_config(conn, None, "Youth_Voice")
        second = await _legacy_config(conn, None, "Youth_Voice")
        other = await _legacy_config(conn, None, "Parent_Voice")
        assert await conn.fetchval("SELECT ensure_config_unique_index()") is False
        rows = {str(r["id"]): r["session_type"] for r in await conn.fetch("SELECT id, session_type FROM config")}
        assert rows == {first: None, second: None, other: "Parent_Voice"}
    _run(body)


def test_a_fresh_database_backfills_nothing_and_a_unique_row_is_still_labelled():
    async def body(conn):
        await _legacy_world(conn)
        row = await _legacy_config(conn, None, "Youth_Voice")
        assert await conn.fetchval("SELECT ensure_config_unique_index()") is True
        assert await conn.fetchval("SELECT session_type FROM config WHERE id = $1::uuid", row) == "Youth_Voice"
    _run(body)


# ------------------------------------------------------------------------- set: column_map / completion_map, lock

import importlib.util
from pathlib import Path

from app.services.feedback_adapter_config import CONVERSATION_ROWS_ONLY_KEYS
from app.database import feedback_legacy_upgrade as up
from app.database import feedback_operations as fo

_TOOL = Path(__file__).resolve().parent.parent / "tools" / "feedback_config.py"
_spec = importlib.util.spec_from_file_location("feedback_config_cli_r2", _TOOL)
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

SESSION_TYPE = "Parent_Voice"
SOURCE = "mitra"


async def _persona(conn, config=None):
    program = await factories.add_program(conn, TENANT, "Alpha", "Parent")
    config_id = await factories.add_config(
        conn, program, SOURCE, SESSION_TYPE, config if config is not None else factories.upload_config(session_type=SESSION_TYPE))
    return program, config_id


async def _active(conn, config_id):
    row = await conn.fetchrow(
        "SELECT version, config FROM config_version WHERE config_id = $1::uuid AND is_active", config_id)
    return row["version"], json.loads(row["config"])


def _cli(*argv, monkeypatch):
    """The CLI's real `_run` on the test pool (connect / disconnect are the test's business)."""
    async def noop():
        return None
    monkeypatch.setattr(cli.db, "connect", noop)
    monkeypatch.setattr(cli.db, "disconnect", noop)
    return cli._run(cli.build_parser().parse_args(list(argv)))


def test_the_setter_changes_and_unsets_column_map_and_completion_map(monkeypatch):
    async def body(conn):
        _, config_id = await _persona(conn)
        res = await fo.set_feedback_config_value(
            SESSION_TYPE, SOURCE, "column_map", {"message": "Chat Text"}, tenant_code=TENANT)
        assert res["changed"] is True and res["key"] == "column_map"
        assert (await _active(conn, config_id))[1]["column_map"] == {"message": "Chat Text"}
        res = await fo.set_feedback_config_value(
            SESSION_TYPE, SOURCE, "completion_map", {"done": "COMPLETED", "open": "IN_PROGRESS"}, tenant_code=TENANT)
        assert res["changed"] is True
        version, config = await _active(conn, config_id)
        assert config["completion_map"] == {"done": "COMPLETED", "open": "IN_PROGRESS"} and version == 3
        res = await fo.set_feedback_config_value(
            SESSION_TYPE, SOURCE, "column_map", None, tenant_code=TENANT, unset=True)
        assert res["changed"] is True and "column_map" not in (await _active(conn, config_id))[1]
        # an invalid value is still refused, and a required key still cannot be unset
        with pytest.raises(ValueError, match="column_map"):
            await fo.set_feedback_config_value(SESSION_TYPE, SOURCE, "column_map", {"nope": "X"}, tenant_code=TENANT)
        with pytest.raises(ValueError, match="required and cannot be unset"):
            await fo.set_feedback_config_value(SESSION_TYPE, SOURCE, "adapter", None, tenant_code=TENANT, unset=True)
        with pytest.raises(ValueError, match="key must be one of"):
            await fo.set_feedback_config_value(SESSION_TYPE, SOURCE, "bogus", 1, tenant_code=TENANT)
    _run(body)


def test_the_cli_accepts_column_map_and_completion_map(monkeypatch):
    async def body(conn):
        _, config_id = await _persona(conn)
        res = await _cli("set", "--session-type", SESSION_TYPE, "--source", SOURCE, "--tenant-code", TENANT,
                         "--key", "column_map", "--json", '{"message": "Chat Text"}', monkeypatch=monkeypatch)
        assert res["changed"] is True
        res = await _cli("set", "--session-type", SESSION_TYPE, "--source", SOURCE, "--tenant-code", TENANT,
                         "--key", "completion_map", "--json", '{"done": "COMPLETED"}', monkeypatch=monkeypatch)
        assert res["changed"] is True
        config = (await _active(conn, config_id))[1]
        assert config["column_map"] == {"message": "Chat Text"} and config["completion_map"] == {"done": "COMPLETED"}
        res = await _cli("set", "--session-type", SESSION_TYPE, "--source", SOURCE, "--tenant-code", TENANT,
                         "--key", "completion_map", "--unset", monkeypatch=monkeypatch)
        assert res["changed"] is True and "completion_map" not in (await _active(conn, config_id))[1]
    _run(body)


def test_the_cli_choices_are_the_settable_keys():
    from app.services.feedback_config_validation import SETTABLE_CONFIG_KEYS
    assert tuple(cli.KEYS) == tuple(SETTABLE_CONFIG_KEYS) and {"column_map", "completion_map"} <= set(cli.KEYS)


def test_switching_to_wide_form_is_refused_by_the_cli_check_and_by_the_setter_alone(monkeypatch):
    async def body(conn):
        config = factories.upload_config(session_type=SESSION_TYPE, column_map={"message": "Chat Text"})
        _, config_id = await _persona(conn, config)
        with pytest.raises(ValueError, match="would be invalid"):
            await fo.set_feedback_config_value(SESSION_TYPE, SOURCE, "adapter", "wide_form", tenant_code=TENANT)
        with pytest.raises(ValueError, match="would be invalid"):
            await _cli("set", "--session-type", SESSION_TYPE, "--source", SOURCE, "--tenant-code", TENANT,
                       "--key", "adapter", "--json", '"wide_form"', monkeypatch=monkeypatch)
        assert (await _active(conn, config_id))[0] == 1                       # nothing written
    _run(body)


def test_the_adapter_check_runs_under_the_lock_on_what_a_concurrent_migrate_legacy_wrote(monkeypatch):
    """The CLI's pre-check passes (no column_map yet); then a concurrent writer adds column_map / completion_map as a
    new config version; the setter must read that version under the lock and refuse to write wide_form on top of it."""
    async def body(conn):
        bare = {k: v for k, v in factories.upload_config(session_type=SESSION_TYPE).items()
                if k not in CONVERSATION_ROWS_ONLY_KEYS}
        _, config_id = await _persona(conn, bare)
        real_check = up.adapter_problem_after_set

        async def check_then_concurrent_change(*args, **kwargs):
            reason = await real_check(*args, **kwargs)
            assert reason is None                                           # outdated by the next statement
            version, config = await _active(conn, config_id)
            await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", config_id)
            await conn.execute(
                "INSERT INTO config_version (config_id, version, is_active, config, created_by) "
                "VALUES ($1::uuid, $2, TRUE, $3::jsonb, 'migrate-legacy')", config_id, version + 1,
                json.dumps({**config, "column_map": {"message": "Chat Text"}, "completion_map": {"done": "COMPLETED"}}))
            return reason

        monkeypatch.setattr(up, "adapter_problem_after_set", check_then_concurrent_change)
        with pytest.raises(ValueError, match="would be invalid"):
            await _cli("set", "--session-type", SESSION_TYPE, "--source", SOURCE, "--tenant-code", TENANT,
                       "--key", "adapter", "--json", '"wide_form"', monkeypatch=monkeypatch)
        version, config = await _active(conn, config_id)
        assert version == 2 and config["adapter"] == "conversation_rows" and "column_map" in config
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE config_id = $1::uuid", config_id) == 2
    _run(body)


# ------------------------------------------- sessions governed by the default when the own config has no active version

from datetime import datetime, timezone

from app.database.feedback_operations import (
    detect_questionnaire_versions,
    get_active_feedback_config,
    list_questionnaire_versions,
    restamp_questionnaire_versions,
)

V2_TEXT = "2026-11-03T00:00:00+00:00"


async def _add_default(conn, config):
    config_id = factories.uid()
    await conn.execute(
        "INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, session_type) "
        "VALUES ($1::uuid, $2, 'default', 'feedback_upload', NULL, $3, $4)", config_id, SOURCE, TENANT, SESSION_TYPE)
    await conn.execute(
        "INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
        "VALUES ($1::uuid, 1, TRUE, $2::jsonb, 'test', 'test')", config_id, json.dumps(config))
    return config_id


async def _labels(conn):
    return {r["submission_id"]: r["questionnaire_version"]
            for r in await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")}


async def _world_with_inactive_own_config(conn, *, own_versions="inactive"):
    """Alpha has an own config row whose only version is NOT active (or no version at all); Beta has none; the tenant's
    default has a V2 window. One session of each program, started after the window, both stored as V1."""
    tenant = TENANT
    await conn.execute("INSERT INTO tenant (name, code) VALUES ('Acme', $1)", tenant)
    alpha = await factories.add_program(conn, tenant, "Alpha", "Parent")
    beta = await factories.add_program(conn, tenant, "Beta", "Parent")
    alpha_config = await factories.add_config(conn, alpha, SOURCE, SESSION_TYPE)
    if own_versions == "inactive":
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", alpha_config)
    else:
        await conn.execute("DELETE FROM config_version WHERE config_id = $1::uuid", alpha_config)
    default = factories.upload_config(session_type=SESSION_TYPE)
    default["questionnaire_versions"] = [{"key": "V1", "effective_from": None}, {"key": "V2", "effective_from": V2_TEXT}]
    await _add_default(conn, default)
    started = datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc)
    s_alpha = await factories.store_session(conn, alpha, SESSION_TYPE, SOURCE, "V1", started)
    s_beta = await factories.store_session(conn, beta, SESSION_TYPE, SOURCE, "V1", started)
    return alpha, beta, s_alpha, s_beta


@pytest.mark.parametrize("own_versions", ["inactive", "none"])
def test_a_program_whose_own_config_has_no_active_version_is_governed_by_the_default(own_versions):
    async def body(conn):
        alpha, beta, s_alpha, s_beta = await _world_with_inactive_own_config(conn, own_versions=own_versions)
        # ingestion resolves the default for Alpha (the lookup rule) ...
        found = await get_active_feedback_config(alpha["program_id"], SOURCE, tenant_code=TENANT)
        assert found is not None and found["program_id"] is None
        # ... so list / detect / restamp must govern Alpha's sessions through the default too
        listed = list(await list_questionnaire_versions(tenant_code=TENANT))
        assert len(listed) == 1
        assert (await list_questionnaire_versions(tenant_code=TENANT))[listed[0]]["sessions"] == {"V1": 2}
        assert len(await detect_questionnaire_versions(tenant_code=TENANT)) == 1
        dry = await restamp_questionnaire_versions(tenant_code=TENANT, dry_run=True)
        assert dry["changed"] == 2 and await _labels(conn) == {s_alpha: "V1", s_beta: "V1"}
        real = await restamp_questionnaire_versions(tenant_code=TENANT)
        assert real["changed"] == 2 and await _labels(conn) == {s_alpha: "V2", s_beta: "V2"}
        assert (await restamp_questionnaire_versions(tenant_code=TENANT))["changed"] == 0
    _run(body)


def test_a_program_with_an_active_own_config_is_still_never_governed_by_the_default():
    async def body(conn):
        alpha, beta, s_alpha, s_beta = await _world_with_inactive_own_config(conn)
        await conn.execute("UPDATE config_version SET is_active = TRUE WHERE config_id IN "
                           "(SELECT id FROM config WHERE program_id = $1::uuid)", alpha["program_id"])
        await restamp_questionnaire_versions(tenant_code=TENANT)
        assert await _labels(conn) == {s_alpha: "V1", s_beta: "V2"}     # Alpha keeps its own (V1-only) windows
    _run(body)


# ------------------------------------------------------------------ result keys of two programs of one name

def test_two_programs_with_the_same_name_keep_separate_results_in_list_detect_and_restamp():
    async def body(conn):
        await conn.execute("INSERT INTO tenant (name, code) VALUES ('Acme', $1)", TENANT)
        first = await factories.add_program(conn, TENANT, "Alpha", "Parent")
        second = await factories.add_program(conn, TENANT, "Alpha", "Parent")      # same name, tenant, persona
        for program in (first, second):
            await factories.add_config(conn, program, SOURCE, SESSION_TYPE)
        for program in (first, second):
            await factories.store_session(conn, program, SESSION_TYPE, SOURCE, "V1",
                                          datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
        for result in (await list_questionnaire_versions(tenant_code=TENANT),
                       (await restamp_questionnaire_versions(tenant_code=TENANT, dry_run=True))["session_types"]):
            assert len(result) == 2, sorted(result)
            assert {v["program_id"] for v in result.values()} == {first["program_id"], second["program_id"]}
            assert all(first["program_id"] in k or second["program_id"] in k for k in result)
        detected = await detect_questionnaire_versions(tenant_code=TENANT)
        assert len(detected) == 2
        assert {v["program_id"] for v in detected.values()} == {first["program_id"], second["program_id"]}
    _run(body)


def test_a_single_persona_keeps_its_plain_session_type_key():
    async def body(conn):
        await conn.execute("INSERT INTO tenant (name, code) VALUES ('Acme', $1)", TENANT)
        program = await factories.add_program(conn, TENANT, "Alpha", "Parent")
        await factories.add_config(conn, program, SOURCE, SESSION_TYPE)
        assert list(await list_questionnaire_versions(tenant_code=TENANT)) == [SESSION_TYPE]
    _run(body)


# ------------------------------------------- restamp locks the config row BEFORE it reads the windows (inline #4)

def test_a_restamp_waits_for_the_config_lock_and_labels_from_the_windows_committed_meanwhile():
    async def body(conn):
        await conn.execute("INSERT INTO tenant (name, code) VALUES ('Acme', $1)", TENANT)
        program = await factories.add_program(conn, TENANT, "Alpha", "Parent")
        old = factories.upload_config(session_type=SESSION_TYPE)
        old["questionnaire_versions"] = [{"key": "V1", "effective_from": None},
                                         {"key": "V2", "effective_from": "2026-12-01T00:00:00+00:00"}]
        config_id = await factories.add_config(conn, program, SOURCE, SESSION_TYPE, old)
        sid = await factories.store_session(conn, program, SESSION_TYPE, SOURCE, "V1",
                                            datetime(2026, 11, 5, 9, 0, 0, tzinfo=timezone.utc))
        holder = await asyncpg.connect(DSN)
        try:
            tx = holder.transaction()
            await tx.start()
            await holder.fetchrow("SELECT id FROM config WHERE id = $1::uuid FOR UPDATE", config_id)
            task = asyncio.ensure_future(restamp_questionnaire_versions(tenant_code=TENANT))
            await asyncio.sleep(0.7)
            assert not task.done(), "restamp did not wait for the config row lock"
            # what set_questionnaire_version commits meanwhile: V2 now starts BEFORE the session
            new = {**old, "questionnaire_versions": [{"key": "V1", "effective_from": None},
                                                     {"key": "V2", "effective_from": "2026-11-01T00:00:00+00:00"}]}
            await holder.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", config_id)
            await holder.execute(
                "INSERT INTO config_version (config_id, version, is_active, config, created_by) "
                "VALUES ($1::uuid, 2, TRUE, $2::jsonb, 'holder')", config_id, json.dumps(new))
            await holder.execute("UPDATE feedback_submissions SET questionnaire_version = 'V2' WHERE submission_id = $1", sid)
            await tx.commit()
            result = await asyncio.wait_for(task, 10)
        finally:
            await holder.close()
        assert result["changed"] == 0                                     # it read the NEW windows: V2 already right
        assert await _labels(conn) == {sid: "V2"}                         # the old windows would have written V1 back
    _run(body)
