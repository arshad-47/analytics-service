"""
Real-Postgres tests for the per-bot feedback settings that live in the persona's upload config
(max_csv_upload_bytes, ingestion_schema, process_steps, optional expect_stages): the seeds, the process-steps lookup of
a stored submission, the tenant / session_type filters of get_active_feedback_config, and list / migrate / set
(app/database/feedback_operations.py), each change a NEW config_version with the old one kept as history.

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards) (the tests wipe the feedback rows and the configs before each case). The file applies schema.sql
itself, creates the tenant 'mitra', runs the real seed and stores sessions through the real ingest path. Synthetic
data only.
"""
import asyncio
import json
import uuid

import asyncpg
import pytest
import pg_support

from app.database.db import db
from app.database.feedback_operations import (
    get_active_feedback_config,
    get_feedback_process_steps,
    get_process_steps,
    list_feedback_config,
    migrate_feedback_config,
    set_feedback_config_value,
    set_questionnaire_version,
)
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, seed_config
from app.services.feedback_constants import (
    DEFAULT_INGESTION_SCHEMA,
    DEFAULT_MAX_CSV_UPLOAD_BYTES,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
)
from seed_support import PERSONAS, SEED_UPLOAD_CONFIG_COUNT

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH = PERSONAS["Youth"]
STEPS = []  # the seed / migrate default: extraction is opt-in (an LLM call per submission)
EXTRACTION = [{"name": "feedback_metadata_extraction"}]  # extraction switched on for a persona
SEEDED_CONFIGS = SEED_UPLOAD_CONFIG_COUNT   # the upload configs of the seed (one per persona and source)


def _run(body):
    """Runs `body(conn)` on a scratch database: schema applied, tenant 'mitra', clean seed, db.pool pointing at it."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await seed_config(conn)
            # this file is about the feedback_upload settings: the extraction configs of the seed are left out
            await conn.execute("DELETE FROM config WHERE config_type <> 'feedback_upload'")
            pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
            db.pool = pool
            return await body(conn)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.close()
    return asyncio.run(main())


def _event(source="mitra", session_type=None):
    session_type = session_type or YOUTH["session_type"]
    sid = f"c{uuid.uuid4().hex[:14]}"
    ref = {"sourceRef": f"2026-10-02T10:09:20|{uuid.uuid4().hex}"} if source == "google_forms" else {}
    if source == "google_forms":
        sid = str(uuid.uuid4())
    return {
        "submissionId": sid, "sessionId": sid if source == "google_forms" else f"sess-{uuid.uuid4().hex}",
        "submissionType": "feedback", "tenantCode": "mitra", "eventType": "create",
        "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": YOUTH["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": YOUTH["leader_category_id"], "leaderCategoryName": "Youth",
                 "state": None, "district": None, "source": source},
        "data": {"source": source, "adapter": "wide_form" if source == "google_forms" else "conversation_rows", "sessionType": session_type, "questionnaireVersion": "V1", "language": "en",
                 "completionStatus": "COMPLETED", "submissionDate": "2026-11-10T08:00:00.000Z", **ref,
                 "messages": [
                     {"sequence": 1, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "Age?",
                      "translatedMessage": None, "messageAt": None, "parentSequence": None},
                     {"sequence": 2, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "17",
                      "translatedMessage": None, "messageAt": None, "parentSequence": 1}]},
    }


async def _store(conn, source="mitra"):
    event = _event(source)
    await insert_or_update_submission(conn, event)
    return event["submissionId"]


async def _versions(conn, session_type, source="mitra"):
    rows = await conn.fetch(
        """SELECT v.version, v.is_active, v.config, v.created_by, v.change_note FROM config c
           JOIN config_version v ON v.config_id = c.id
           WHERE c.session_type = $1 AND c.config_source = $2 ORDER BY v.version""", session_type, source)
    return [{**dict(r), "config": json.loads(r["config"])} for r in rows]


async def _strip_keys(conn, *keys):
    """Make every active config look like an older database's: the keys that moved out of .env are absent."""
    for key in keys:
        await conn.execute("UPDATE config_version SET config = config - $1::text", key)


# seeds ----------------------------------------------------------------------------------------

def test_seeds_write_the_three_settings_for_every_persona_and_source():
    async def body(conn):
        rows = await conn.fetch("SELECT c.session_type, c.config_source, v.config FROM config c "
                                "JOIN config_version v ON v.config_id = c.id AND v.is_active")
        assert len(rows) == SEEDED_CONFIGS
        for row in rows:
            config = json.loads(row["config"])
            assert config["max_csv_upload_bytes"] == DEFAULT_MAX_CSV_UPLOAD_BYTES == 104857600
            assert config["process_steps"] == STEPS
            assert config["ingestion_schema"] == DEFAULT_INGESTION_SCHEMA
            assert "expect_stages" not in config
        listing = await list_feedback_config()
        assert len(listing["configs"]) == SEEDED_CONFIGS and all(c["problems"] == {} for c in listing["configs"])
    _run(body)


def test_reseeding_does_not_overwrite_a_changed_config():
    async def body(conn):
        await set_feedback_config_value("Youth_Voice", "mitra", "max_csv_upload_bytes", 5)
        await seed_config(conn)  # ON CONFLICT DO NOTHING: the edited config survives a restart
        versions = await _versions(conn, "Youth_Voice")
        assert [v["version"] for v in versions] == [1, 2]
        assert versions[1]["is_active"] and versions[1]["config"]["max_csv_upload_bytes"] == 5
    _run(body)


# process steps of a stored submission ---------------------------------------------------------

def test_process_steps_of_a_stored_submission_come_from_its_persona_config():
    async def body(conn):
        mitra_id = await _store(conn)
        forms_id = await _store(conn, "google_forms")
        assert await get_feedback_process_steps(conn, mitra_id, "mitra") == STEPS
        assert await get_feedback_process_steps(conn, forms_id, "mitra") == STEPS
        assert await get_process_steps(conn, "feedback", mitra_id, "mitra") == STEPS
    _run(body)


def test_a_new_config_version_changes_the_steps_and_each_source_is_separate():
    async def body(conn):
        mitra_id = await _store(conn)
        forms_id = await _store(conn, "google_forms")
        custom = [{"name": "feedback_metadata_extraction", "llm_model": "m", "max_tokens": 512}]
        await set_feedback_config_value("Youth_Voice", "mitra", "process_steps", custom)
        assert await get_feedback_process_steps(conn, mitra_id, "mitra") == custom
        assert await get_feedback_process_steps(conn, forms_id, "mitra") == STEPS  # the Google Forms config is its own
        await set_feedback_config_value("Youth_Voice", "mitra", "process_steps", [])
        assert await get_feedback_process_steps(conn, mitra_id, "mitra") == []     # empty: success at ingestion
    _run(body)


def test_steps_lookup_errors_are_clear():
    async def body(conn):
        with pytest.raises(ValueError, match="No stored feedback submission 'nope'"):
            await get_feedback_process_steps(conn, "nope", "mitra")
        mitra_id = await _store(conn)
        await _strip_keys(conn, "process_steps")
        with pytest.raises(ValueError, match="missing 'process_steps'.*feedback_config.py migrate"):
            await get_feedback_process_steps(conn, mitra_id, "mitra")
        await conn.execute("UPDATE config_version SET is_active = FALSE")
        with pytest.raises(ValueError, match="No active mitra feedback_upload config"):
            await get_feedback_process_steps(conn, mitra_id, "mitra")
    _run(body)


def test_steps_lookup_is_tenant_scoped():
    async def body(conn):
        mitra_id = await _store(conn)
        with pytest.raises(ValueError, match="No stored feedback submission"):
            await get_feedback_process_steps(conn, mitra_id, "other-tenant")
    _run(body)


def test_the_steps_of_a_persona_come_from_the_config_of_the_same_tenant_even_when_the_program_matches():
    """
    A config row of tenant 'other' that points at a MITRA program (plain SQL) with other steps: the lookup of the
    steps for tenant 'other' returns ITS steps, the one for 'mitra' returns the mitra steps; neither borrows the
    other's row.
    """
    async def body(conn):
        foreign_steps = [{"name": "feedback_metadata_extraction", "columns": ["only-other"]}]
        foreign_config = dict(await get_active_feedback_config(YOUTH["program_id"], "mitra", tenant_code="mitra"))["config"]
        foreign_config["process_steps"] = foreign_steps
        await pg_support.insert_foreign_config(
            conn, tenant_code="other", program_id=YOUTH["program_id"], source="mitra", session_type="Youth_Voice",
            config=foreign_config)
        try:
            from app.database.feedback_operations import _persona_process_steps
            assert await _persona_process_steps(conn, "mitra", "Youth_Voice", "mitra", YOUTH["program_id"]) == STEPS
            assert await _persona_process_steps(conn, "mitra", "Youth_Voice", "other", YOUTH["program_id"]) == foreign_steps
        finally:
            await conn.execute("DELETE FROM config WHERE tenant_code = 'other'")
            await conn.execute("DELETE FROM tenant WHERE code = 'other'")
    _run(body)


# the config lookup the consumer uses ------------------------------------------------------------

def test_active_config_lookup_can_be_narrowed_by_tenant_and_session_type():
    async def body(conn):
        base = await get_active_feedback_config(YOUTH["program_id"], "mitra")
        assert base["session_type"] == "Youth_Voice"
        assert await get_active_feedback_config(YOUTH["program_id"], "mitra", tenant_code="mitra",
                                                session_type="Youth_Voice") == base
        assert await get_active_feedback_config(YOUTH["program_id"], "mitra", tenant_code="other") is None
        assert await get_active_feedback_config(YOUTH["program_id"], "mitra", session_type="Parent_Voice") is None
        assert (await get_active_feedback_config(YOUTH["program_id"], "google_forms",
                                                 session_type="Youth_Voice"))["config"]["process_steps"] == STEPS
    _run(body)


# migrate ----------------------------------------------------------------------------------------

def test_migrate_adds_the_missing_keys_as_a_new_version_and_keeps_history():
    async def body(conn):
        await _strip_keys(conn, *REQUIRED_FEEDBACK_CONFIG_KEYS)
        before = {k: v for k, v in (await _versions(conn, "Youth_Voice"))[0]["config"].items()}
        assert "process_steps" not in before

        dry = await migrate_feedback_config(dry_run=True)
        assert (dry["configs"], dry["migrated"], dry["unchanged"], dry["dry_run"]) == (SEEDED_CONFIGS, SEEDED_CONFIGS, 0, True)
        assert [v["version"] for v in await _versions(conn, "Youth_Voice")] == [1]  # nothing written

        result = await migrate_feedback_config()
        assert (result["configs"], result["migrated"], result["unchanged"], result["invalid"]) == (SEEDED_CONFIGS, SEEDED_CONFIGS, 0, [])
        assert all(d["added"] == list(REQUIRED_FEEDBACK_CONFIG_KEYS) and d["config_version"] == 2 for d in result["details"])
        assert all(set(d) == {"session_type", "source", "tenant_code", "program_id", "added", "config_version"} for d in result["details"])  # no content

        v1, v2 = await _versions(conn, "Youth_Voice")
        assert (v1["version"], v1["is_active"], v1["config"]) == (1, False, before)  # history untouched
        assert v2["is_active"] and v2["created_by"] == "cli"
        assert v2["config"]["process_steps"] == STEPS and v2["config"]["max_csv_upload_bytes"] == DEFAULT_MAX_CSV_UPLOAD_BYTES
        # every other key is carried over unchanged
        assert {k: v for k, v in v2["config"].items() if k not in REQUIRED_FEEDBACK_CONFIG_KEYS} == before
        assert await conn.fetchval("SELECT count(*) FROM config_version WHERE is_active") == SEEDED_CONFIGS
    _run(body)


def test_migrate_is_idempotent():
    async def body(conn):
        await _strip_keys(conn, "process_steps")
        first = await migrate_feedback_config()
        second = await migrate_feedback_config()
        assert first["migrated"] == SEEDED_CONFIGS and first["details"][0]["added"] == ["process_steps"]
        assert (second["migrated"], second["unchanged"], second["details"]) == (0, SEEDED_CONFIGS, [])
        assert [v["version"] for v in await _versions(conn, "Youth_Voice")] == [1, 2]
    _run(body)


def test_migrate_on_a_current_database_changes_nothing():
    async def body(conn):
        result = await migrate_feedback_config()
        assert (result["migrated"], result["unchanged"]) == (0, SEEDED_CONFIGS)
        assert await conn.fetchval("SELECT count(*) FROM config_version") == SEEDED_CONFIGS
    _run(body)


def test_migrate_never_overwrites_an_existing_key_and_reports_an_invalid_one():
    async def body(conn):
        await _strip_keys(conn, "process_steps", "ingestion_schema")
        await conn.execute(
            """UPDATE config_version SET config = jsonb_set(config, '{max_csv_upload_bytes}', '"lots"')
               WHERE config_id = (SELECT id FROM config WHERE session_type = 'Youth_Voice' AND config_source = 'mitra')""")
        await conn.execute(
            """UPDATE config_version SET config = jsonb_set(config, '{max_csv_upload_bytes}', '777')
               WHERE config_id = (SELECT id FROM config WHERE session_type = 'Parent_Voice' AND config_source = 'mitra')""")
        result = await migrate_feedback_config()
        assert result["invalid"] == [{"session_type": "Youth_Voice", "source": "mitra", "tenant_code": "mitra",
                                      "program_id": YOUTH["program_id"], "key": "max_csv_upload_bytes",
                                      "problem": result["invalid"][0]["problem"]}]
        assert "positive integer" in result["invalid"][0]["problem"]
        youth = (await _versions(conn, "Youth_Voice"))[-1]["config"]
        parent = (await _versions(conn, "Parent_Voice"))[-1]["config"]
        assert youth["max_csv_upload_bytes"] == "lots"      # reported, not overwritten
        assert parent["max_csv_upload_bytes"] == 777        # a valid custom value survives
        assert parent["process_steps"] == STEPS and parent["ingestion_schema"] == DEFAULT_INGESTION_SCHEMA
    _run(body)


def test_migrate_skips_a_config_without_an_active_version():
    async def body(conn):
        await _strip_keys(conn, "process_steps")
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = "
                           "(SELECT id FROM config WHERE session_type = 'Educator_Voice')")
        result = await migrate_feedback_config()
        assert result["configs"] == SEEDED_CONFIGS - 1 and result["migrated"] == SEEDED_CONFIGS - 1
    _run(body)


def test_questionnaire_version_change_after_migrate_keeps_the_new_settings():
    async def body(conn):
        await _strip_keys(conn, *REQUIRED_FEEDBACK_CONFIG_KEYS)
        await migrate_feedback_config()
        await set_questionnaire_version("Youth_Voice", "V2", "2026-11-03T18:23:03+05:30")
        latest = (await _versions(conn, "Youth_Voice"))[-1]
        assert latest["version"] == 3 and latest["is_active"]
        assert latest["config"]["process_steps"] == STEPS and len(latest["config"]["questionnaire_versions"]) == 2
    _run(body)


# set --------------------------------------------------------------------------------------------

def test_set_writes_a_new_version_with_the_value_and_keeps_the_rest():
    async def body(conn):
        result = await set_feedback_config_value("Youth_Voice", "mitra", "max_csv_upload_bytes", 50_000_000,
                                                 created_by="op", change_note="smaller files")
        assert result == {"session_type": "Youth_Voice", "source": "mitra", "key": "max_csv_upload_bytes",
                          "tenant_code": "mitra", "program_id": YOUTH["program_id"], "program_name": YOUTH["program_name"],
                          "changed": True, "config_version": 2, "missing_keys": [], "dry_run": False}
        v1, v2 = await _versions(conn, "Youth_Voice")
        assert (v1["is_active"], v2["is_active"]) == (False, True)
        assert v1["config"]["max_csv_upload_bytes"] == 104857600  # history kept
        assert v2["config"]["max_csv_upload_bytes"] == 50_000_000 and v2["created_by"] == "op"
        assert v2["change_note"] == "smaller files"
        assert {k: v for k, v in v1["config"].items() if k != "max_csv_upload_bytes"} == \
               {k: v for k, v in v2["config"].items() if k != "max_csv_upload_bytes"}
        active = await get_active_feedback_config(YOUTH["program_id"], "mitra")
        assert active["version"] == 2 and active["config"]["max_csv_upload_bytes"] == 50_000_000
        # the other source and the other personas are untouched
        assert [v["version"] for v in await _versions(conn, "Youth_Voice", "google_forms")] == [1]
        assert [v["version"] for v in await _versions(conn, "Parent_Voice")] == [1]
    _run(body)


def test_set_json_values_and_expect_stages_lifecycle():
    async def body(conn):
        schema = json.loads(json.dumps(DEFAULT_INGESTION_SCHEMA))
        schema["create"]["required"].append("data.botId")
        assert (await set_feedback_config_value("Youth_Voice", "mitra", "ingestion_schema", schema))["changed"]
        assert (await set_feedback_config_value("Youth_Voice", "mitra", "expect_stages", ["A", "B"]))["config_version"] == 3
        config = (await _versions(conn, "Youth_Voice"))[-1]["config"]
        assert config["ingestion_schema"]["create"]["required"][-1] == "data.botId" and config["expect_stages"] == ["A", "B"]
        removed = await set_feedback_config_value("Youth_Voice", "mitra", "expect_stages", None, unset=True)
        assert removed["changed"] and "expect_stages" not in (await _versions(conn, "Youth_Voice"))[-1]["config"]
        again = await set_feedback_config_value("Youth_Voice", "mitra", "expect_stages", None, unset=True)
        assert again["changed"] is False and again["config_version"] == 4
    _run(body)


def test_set_with_the_stored_value_writes_nothing():
    async def body(conn):
        result = await set_feedback_config_value("Youth_Voice", "mitra", "process_steps", STEPS)
        assert result["changed"] is False and result["config_version"] == 1
        assert [v["version"] for v in await _versions(conn, "Youth_Voice")] == [1]
    _run(body)


def test_set_dry_run_validates_and_rolls_back():
    async def body(conn):
        result = await set_feedback_config_value("Youth_Voice", "mitra", "max_csv_upload_bytes", 9, dry_run=True)
        assert result["changed"] is True and result["config_version"] == 2 and result["dry_run"] is True
        versions = await _versions(conn, "Youth_Voice")
        assert [v["version"] for v in versions] == [1] and versions[0]["is_active"]
        assert versions[0]["config"]["max_csv_upload_bytes"] == 104857600
    _run(body)


@pytest.mark.parametrize("key,value,needle", [
    ("max_csv_upload_bytes", "100", "positive integer"),
    ("max_csv_upload_bytes", 0, "positive integer"),
    ("max_csv_upload_bytes", True, "positive integer"),
    ("process_steps", [{"nom": "x"}], "non-empty string 'name'"),
    ("process_steps", {"name": "x"}, "must be a list"),
    ("ingestion_schema", {"create": {"required": []}, "update": {}, "delete": {}}, "must include"),
    ("expect_stages", ["A", "A"], "must not repeat"),
    ("columns", {}, "key must be one of"),
])
def test_set_rejects_an_invalid_value_and_writes_nothing(key, value, needle):
    async def body(conn):
        with pytest.raises(ValueError, match=needle):
            await set_feedback_config_value("Youth_Voice", "mitra", key, value)
        assert await conn.fetchval("SELECT count(*) FROM config_version") == SEEDED_CONFIGS
    _run(body)


def test_set_rejects_unknown_targets_and_unsetting_a_required_key():
    async def body(conn):
        with pytest.raises(ValueError, match="no mitra upload config for session_type 'Nobody_Voice'"):
            await set_feedback_config_value("Nobody_Voice", "mitra", "max_csv_upload_bytes", 5)
        # a source is a free label: an unknown one is just "no config", an unsafe / blank one is refused outright
        with pytest.raises(ValueError, match="no whatsapp upload config for session_type 'Youth_Voice'"):
            await set_feedback_config_value("Youth_Voice", "whatsapp", "max_csv_upload_bytes", 5)
        with pytest.raises(ValueError, match="source must be a non-empty string"):
            await set_feedback_config_value("Youth_Voice", "  ", "max_csv_upload_bytes", 5)
        with pytest.raises(ValueError, match="required and cannot be unset"):
            await set_feedback_config_value("Youth_Voice", "mitra", "process_steps", None, unset=True)
        with pytest.raises(ValueError, match="no google_forms upload config for session_type 'Educator_Voice'"):
            await set_feedback_config_value("Educator_Voice", "google_forms", "max_csv_upload_bytes", 5)
        assert await conn.fetchval("SELECT count(*) FROM config_version") == SEEDED_CONFIGS
    _run(body)


def test_set_reports_the_settings_an_older_config_still_lacks():
    async def body(conn):
        await _strip_keys(conn, "process_steps", "ingestion_schema")
        result = await set_feedback_config_value("Youth_Voice", "mitra", "max_csv_upload_bytes", 5)
        assert result["missing_keys"] == ["ingestion_schema", "process_steps"]
        listing = await list_feedback_config("Youth_Voice")
        mitra = next(c for c in listing["configs"] if c["source"] == "mitra")
        assert sorted(mitra["problems"]) == ["ingestion_schema", "process_steps"] and mitra["process_steps"] is None
        assert mitra["max_csv_upload_bytes"] == 5
    _run(body)


def test_concurrent_sets_serialise_on_the_config_row_and_leave_one_active_version():
    async def body(conn):
        results = await asyncio.gather(
            set_feedback_config_value("Youth_Voice", "mitra", "max_csv_upload_bytes", 11),
            set_feedback_config_value("Youth_Voice", "mitra", "process_steps", EXTRACTION),  # differs from the [] default
            set_feedback_config_value("Youth_Voice", "mitra", "expect_stages", ["X"]),
        )
        assert sorted(r["config_version"] for r in results) == [2, 3, 4] and all(r["changed"] for r in results)
        versions = await _versions(conn, "Youth_Voice")
        assert [v["version"] for v in versions] == [1, 2, 3, 4] and [v["is_active"] for v in versions] == [False, False, False, True]
        final = versions[-1]["config"]  # no lost update: all three changes are in the active version
        assert final["max_csv_upload_bytes"] == 11 and final["process_steps"] == EXTRACTION and final["expect_stages"] == ["X"]
    _run(body)
