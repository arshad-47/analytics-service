"""
Schema + seed-data tests for the `feedback` report type (docs/feedback-upload-plan.md, slice 1a).

Pure in-memory: the seeded personas and configs are read from seed_config.sql through tests/seed_support.py, and
schema.sql is only read as text. The tests of the seed file itself (idempotency, tenant-missing no-op, never
overwrites) are tests/test_seed_config.py (text) and tests/test_seed_config_pg.py (real Postgres, opt-in); the
real-Postgres counterpart of this file is tests/test_feedback_pg_integration.py.
"""
import re

import pytest

from app.database.db import SCHEMA_FILE
from app.services.feedback_versions import parse_versions
from seed_support import PERSONAS, gforms_config_v1, load_seed_config, mitra_config_v1

EXPECTED_PERSONAS = ["Youth", "Parent", "General Public", "Educator"]
EXPECTED_SESSION_TYPES = {
    "Youth": "Youth_Voice", "Parent": "Parent_Voice",
    "General Public": "General_Public_Voice", "Educator": "Educator_Voice",
}
GFORMS_PERSONAS_EXPECTED = ["Youth", "Parent", "General Public"]
BASELINE = [{"key": "V1", "effective_from": None}]


def test_personas_are_the_four_leader_categories():
    assert list(PERSONAS) == EXPECTED_PERSONAS
    assert {name: p["session_type"] for name, p in PERSONAS.items()} == EXPECTED_SESSION_TYPES
    assert [name for name, p in PERSONAS.items() if p["gforms_config_id"] is not None] == GFORMS_PERSONAS_EXPECTED
    assert PERSONAS["Educator"]["gforms_config_id"] is None


def test_educator_has_a_mitra_config_and_no_google_forms_config():
    assert PERSONAS["Educator"]["config_id"] is not None and PERSONAS["Educator"]["gforms_config_id"] is None
    assert load_seed_config("feedback_upload", "mitra", "Educator_Voice")["session_type"] == "Educator_Voice"
    with pytest.raises(KeyError):
        load_seed_config("feedback_upload", "google_forms", "Educator_Voice")


def test_educator_ids_are_fixed():
    # a pin: these ids are already in the databases seeded before seed_config.sql existed
    educator = PERSONAS["Educator"]
    assert educator["leader_category_id"] == "117e0bb9-c0de-414a-a7f0-5ee62e48477e"
    assert educator["program_id"] == "b09ca040-f2cb-4c31-b0a8-07ce0acb69a2"
    assert educator["config_id"] == "4bc09524-71e3-4f0e-95b9-9d936e6e8697"
    assert educator["session_type"] == "Educator_Voice"
    ids = [v for p in PERSONAS.values() for k, v in p.items()
           if k in ("leader_category_id", "program_id", "config_id", "gforms_config_id") and v is not None]
    assert len(ids) == len(set(ids)) == 15  # every fixed id is distinct (4*3 + 3 Google Forms)


def test_seeded_mitra_configs_carry_versions_opening_row_and_session_type():
    for persona, p in PERSONAS.items():
        cfg = mitra_config_v1(persona)
        assert cfg["session_type"] == p["session_type"]
        assert cfg["opening_row"] == {"mode": "auto"}
        assert cfg["questionnaire_versions"] == BASELINE
        assert cfg["adapter"] == "conversation_rows"
        assert cfg["column_map"]["session_type"] == "session_type"      # the explicit map (headers of the 1MV export)
        assert "columns" not in cfg                                     # replaced by column_map (never read)
        versions = parse_versions(cfg)
        assert versions == [("V1", None)]


def test_seeded_versions_are_exactly_the_baseline_for_all_four_personas():
    # no dates in code or seeds: later versions are recorded in the database with tools/questionnaire_versions.py
    assert set(PERSONAS) == {"Youth", "Parent", "General Public", "Educator"}
    for persona in PERSONAS:
        cfg = mitra_config_v1(persona)
        assert cfg["questionnaire_versions"] == [{"key": "V1", "effective_from": None}], persona
        assert parse_versions(cfg) == [("V1", None)], persona
    assert all("questionnaire_versions" not in p for p in PERSONAS.values())


def test_seeded_questionnaire_versions_are_independent_copies():
    first = mitra_config_v1("Youth")
    first["questionnaire_versions"].append({"key": "V9", "effective_from": "2030-01-01T00:00:00+00:00"})
    assert mitra_config_v1("Youth")["questionnaire_versions"] == BASELINE


def test_seeded_gforms_configs_carry_session_type_and_form_versions():
    for persona in GFORMS_PERSONAS_EXPECTED:
        cfg = gforms_config_v1(persona)
        assert cfg["session_type"] == PERSONAS[persona]["session_type"]
        assert cfg["forms"], persona
        for form in cfg["forms"].values():
            assert form["questionnaire_version"] == "V1"


def test_gforms_config_for_educator_is_not_available():
    with pytest.raises(KeyError):
        gforms_config_v1("Educator")


# schema.sql text checks
SCHEMA_TEXT = SCHEMA_FILE.read_text(encoding="utf-8")


def test_schema_has_the_four_feedback_tables():
    for table in ("feedbacks", "feedback_submissions", "config", "config_version"):
        assert re.search(rf"^CREATE TABLE {table} \(", SCHEMA_TEXT, re.M), table


def test_schema_creates_the_extraction_table_and_pii_flag():
    # Phase 3 (metadata extraction): the long-form result table and the whole-conversation PII flag.
    assert re.search(r"^CREATE TABLE feedback_extracted_data \(", SCHEMA_TEXT, re.M)
    assert "pii_detected      BOOLEAN" in SCHEMA_TEXT
    assert "ALTER TABLE feedback_submissions ADD COLUMN IF NOT EXISTS pii_detected BOOLEAN;" in SCHEMA_TEXT
    assert "config_source IN" not in SCHEMA_TEXT      # a source is a free label validated in code (review finding A1)


def test_schema_feedback_tables_come_after_their_fk_targets():
    pos = {t: SCHEMA_TEXT.index(f"CREATE TABLE {t} (") for t in
           ("programs", "submissions", "feedback_submissions", "feedbacks", "config", "config_version")}
    assert pos["feedback_submissions"] > pos["submissions"]
    assert pos["feedbacks"] > pos["submissions"]
    assert pos["config"] > pos["programs"]
    assert pos["config_version"] > pos["config"]


def test_schema_unique_indexes_use_if_not_exists():
    # db.py injects IF NOT EXISTS only for plain CREATE TABLE / CREATE INDEX, never for UNIQUE ones.
    unique = re.findall(r"^CREATE UNIQUE INDEX\b.*$", SCHEMA_TEXT, re.M)
    assert len(unique) >= 1          # the config key index is created inside ensure_config_unique_index() instead
    for line in unique:
        assert line.startswith("CREATE UNIQUE INDEX IF NOT EXISTS "), line


def test_schema_feedback_indexes_present():
    for name in ("idx_feedbacks_program_question", "idx_feedbacks_parent", "idx_feedbacks_source",
                 "uq_config_version_active", "idx_feedback_submissions_session_type"):
        assert re.search(rf"CREATE (UNIQUE )?INDEX IF NOT EXISTS {name}\b", SCHEMA_TEXT), name
    # the config key index is created by ensure_config_unique_index() (it must never raise on old data)
    assert "uq_config_tenant_source_type_program_session_type" in SCHEMA_TEXT
    assert "COALESCE(program_id, '00000000-0000-0000-0000-000000000000')" in SCHEMA_TEXT
    assert "COALESCE(session_type, '')" in SCHEMA_TEXT
    assert "COALESCE(bot_id, '')" not in SCHEMA_TEXT
    # the old bot-keyed unique index is dropped, not recreated
    assert "DROP INDEX IF EXISTS uq_config_source_type_program_bot" in SCHEMA_TEXT
    assert not re.search(r"CREATE UNIQUE INDEX[^;]*uq_config_source_type_program_bot", SCHEMA_TEXT)
    assert "tenant_code, config_source, config_type" in SCHEMA_TEXT
    assert re.search(r"ON config_version \(config_id\) WHERE is_active", SCHEMA_TEXT)


def _table_body(table):
    match = re.search(rf"^CREATE TABLE {table} \((.*?)^\);", SCHEMA_TEXT, re.M | re.S)
    assert match, table
    return match.group(1)


def test_schema_session_type_and_version_columns_are_not_null():
    assert re.search(r"^\s*session_type\s+TEXT NOT NULL", _table_body("config"), re.M)
    fs = _table_body("feedback_submissions")
    assert re.search(r"^\s*session_type\s+TEXT NOT NULL", fs, re.M)
    assert re.search(r"^\s*questionnaire_version\s+TEXT NOT NULL", fs, re.M)


def test_schema_has_idempotent_alter_lines_for_older_databases():
    for table, column in (("config", "session_type"), ("feedback_submissions", "session_type"),
                          ("feedback_submissions", "questionnaire_version")):
        assert re.search(rf"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} TEXT", SCHEMA_TEXT), (table, column)
