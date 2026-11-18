"""
Pure tests of seed_config.sql (repo root), the ONE place that seeds the leader categories, programs, `config` and
`config_version` rows: its format (the file is parsed by tests/seed_support.py through the `-- PERSONA` and
`-- CONFIG` marker lines), the 11 seeded configs, fixed ids, the never-overwrite / no-op-without-tenant guards in the
SQL text, and that every seeded config is accepted by the code that reads it. No database, no network.
The real-Postgres counterpart (the file executed: idempotent, tenant missing, edits survive) is
tests/test_seed_config_pg.py.
"""
import json
import re
import uuid
from pathlib import Path

import pytest

from app.api.services import feedback_gforms
from app.database import db as db_module
from app.services import feedback_config_validation as validation
from app.services import feedback_extraction as fx
from app.services.feedback_constants import feedback_config_key_defaults
from app.services.feedback_versions import parse_versions
from seed_support import (
    PERSONAS,
    SEED_CONFIG_FILE,
    seed_config_blocks,
    seed_config_sql,
)

SQL = seed_config_sql()
BLOCKS = seed_config_blocks()
EXPECTED_BLOCKS = sorted(
    [("feedback_upload", "mitra", st) for st in ("Educator_Voice", "General_Public_Voice", "Parent_Voice", "Youth_Voice")]
    + [("feedback_upload", "google_forms", st) for st in ("General_Public_Voice", "Parent_Voice", "Youth_Voice")]
    + [("feedback_extraction", "any", st) for st in ("Educator_Voice", "General_Public_Voice", "Parent_Voice", "Youth_Voice")]
)
# the documented derivation of an extraction config's id (the header of seed_config.sql): uuid5 of this namespace and
# "<config_type>:<session_type>"
EXTRACTION_NAMESPACE = uuid.UUID("0d6f3a52-7b1e-4c55-9a8e-5f1d2c9e4b70")


def _without_comments():
    return "\n".join(line for line in SQL.split("\n") if not line.startswith("--"))


def _statements():
    """Every statement of the file with its comment lines removed (the dollar-quoted JSON stays whole)."""
    text = re.sub(r"\$cfg\$.*?\$cfg\$", "$cfg$JSON$cfg$", SQL, flags=re.DOTALL)
    text = "\n".join(line for line in text.split("\n") if not line.startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip() != ""]


# ------------------------------------------------------------------------------------------------ file and format

def test_the_file_is_at_the_repo_root_next_to_the_other_seeds_and_the_app_reads_it():
    root = Path(__file__).resolve().parent.parent
    assert SEED_CONFIG_FILE == root / "seed_config.sql"
    for name in ("schema.sql", "seed_prompts.sql", "seed_themes.sql", "seed_config.sql"):
        assert (root / name).is_file(), name
    assert db_module.SEED_CONFIG_FILE == SEED_CONFIG_FILE
    # which files the image needs is derived from the repo in tests/test_feedback_upgrade.py (no literal COPY line is pinned)
    dockerfile = (root / "Dockerfile").read_text()
    for name in ("schema.sql", "seed_prompts.sql", "seed_themes.sql", "seed_config.sql"):
        assert re.search(rf"^COPY [^\n]*\b{re.escape(name)}\b", dockerfile, re.M), name


def test_the_file_starts_with_a_header_comment_that_explains_it():
    header = SQL.split("\n\n")[0]
    assert header.startswith("-- ====")
    for phrase in ("SEED CONFIG", "NO-OP", "ON CONFLICT DO NOTHING", "no version row at", "idempotent", "uuid5",
                   "-- CONFIG <config_type>", "docs/feedback-extraction-bands.md"):
        assert phrase in header or phrase in SQL[:3500], phrase


def test_there_are_exactly_11_config_blocks_and_each_marker_is_regular():
    markers = re.findall(r"^-- CONFIG (\S+) (\S+) (\S+)$", SQL, re.M)
    assert len(markers) == len(BLOCKS) == 11
    assert sorted(markers) == EXPECTED_BLOCKS
    assert len(set(markers)) == 11
    # 4 Mitra upload + 3 Google Forms upload + 4 extraction
    assert sum(1 for m in markers if m[:2] == ("feedback_upload", "mitra")) == 4
    assert sum(1 for m in markers if m[:2] == ("feedback_upload", "google_forms")) == 3
    assert sum(1 for m in markers if m[:2] == ("feedback_extraction", "any")) == 4


def test_four_persona_blocks_each_with_a_leader_category_and_a_program():
    markers = re.findall(r"^-- PERSONA (.+) (\S+)$", SQL, re.M)
    assert [m[0] for m in markers] == ["Youth", "Parent", "General Public", "Educator"]
    assert SQL.count("INSERT INTO leader_category") == 4 and SQL.count("INSERT INTO programs") == 4
    assert {name: p["session_type"] for name, p in PERSONAS.items()} == dict(markers)
    for p in PERSONAS.values():
        assert p["program_name"] == "1M Voice"


def test_every_config_json_is_dollar_quoted_two_space_indented_and_stable():
    raws = re.findall(r"\$cfg\$\n(.*?)\n\$cfg\$::jsonb", SQL, re.DOTALL)
    assert len(raws) == 11
    for raw, block in zip(raws, BLOCKS):
        assert json.loads(raw) == block["config"]
        assert json.dumps(json.loads(raw), indent=2, ensure_ascii=False) == raw      # a diff of the file stays readable
        assert raw.startswith("{\n  ")
    assert SQL.count("$cfg$") == 22


def test_the_file_has_no_other_dollar_quote_and_only_inserts():
    assert re.findall(r"\$[A-Za-z_]*\$", SQL) == ["$cfg$"] * 22
    statements = _statements()
    assert len(statements) == 4 * 2 + 11 * 2
    for statement in statements:
        assert statement.startswith("INSERT INTO "), statement[:60]


# ------------------------------------------------------------------------------------------------ never overwrites

def test_no_statement_can_update_delete_or_replace_anything():
    for word in ("DO UPDATE", "DELETE", "UPDATE ", "TRUNCATE", "DROP ", "ALTER ", "INSERT INTO tenant", "REPLACE"):
        assert word not in re.sub(r"\$cfg\$.*?\$cfg\$", "", SQL, flags=re.DOTALL), word


def test_every_insert_is_on_conflict_do_nothing():
    for statement in _statements():
        assert re.search(r"ON CONFLICT( \([a-z_, ]+\))? DO NOTHING$", statement), statement[:80]


def test_every_insert_is_a_no_op_without_the_tenant_and_the_version_only_while_the_config_has_no_version():
    for statement in _statements():
        if statement.startswith("INSERT INTO leader_category") or statement.startswith("INSERT INTO programs"):
            assert "EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')" in statement
        elif statement.startswith("INSERT INTO config "):
            assert "EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')" in statement
            assert "EXISTS (SELECT 1 FROM programs WHERE id = " in statement
        elif statement.startswith("INSERT INTO config_version"):
            config_id = re.search(r"SELECT '([0-9a-f-]{36})'::uuid, 1, TRUE", statement).group(1)
            assert f"EXISTS (SELECT 1 FROM config WHERE id = '{config_id}'::uuid)" in statement
            assert f"NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '{config_id}'::uuid)" in statement
            assert "ON CONFLICT (config_id, version) DO NOTHING" in statement
        else:
            raise AssertionError(statement[:80])


def test_the_seed_depends_on_no_other_seed_and_never_creates_the_tenant():
    code = _without_comments()
    assert "prompts" not in code.replace("prompt_name", "")      # not on seed_prompts.sql (only a prompt name string)
    assert "themes" not in code
    assert "INSERT INTO tenant" not in code


# ------------------------------------------------------------------------------------------------ ids and rows

def test_every_id_is_a_fixed_distinct_uuid_and_the_config_rows_agree_with_their_markers():
    ids = [p[k] for p in PERSONAS.values() for k in ("leader_category_id", "program_id")]
    for block in BLOCKS:
        ids.append(block["id"])
        assert block["version_config_id"] == block["id"]
        assert (block["config_type"], block["config_source"], block["session_type"]) == (
            block["type_in_row"], block["source_in_row"], block["session_type_in_row"])
        assert block["tenant_code"] == "mitra"
        assert block["created_by"] == "seed" and block["change_note"] != ""
        assert block["config"]["session_type"] == block["session_type"]
    assert len(ids) == 19 and len(set(ids)) == 19
    for value in ids:
        assert str(uuid.UUID(value)) == value


def test_extraction_config_ids_are_uuid5_of_the_config_type_and_the_session_type():
    for block in BLOCKS:
        if block["config_type"] == "feedback_extraction":
            assert block["id"] == str(uuid.uuid5(EXTRACTION_NAMESPACE, f"feedback_extraction:{block['session_type']}"))


def test_each_config_belongs_to_the_program_of_its_persona_and_is_unique_per_source_type_program_session_type():
    program_of = {p["session_type"]: p["program_id"] for p in PERSONAS.values()}
    keys = set()
    for block in BLOCKS:
        assert block["program_id"] == program_of[block["session_type"]]
        keys.add((block["config_source"], block["config_type"], block["program_id"], block["session_type"]))
    assert len(keys) == 11                                    # the partial unique index of schema.sql would accept all


def test_mitra_rows_record_the_bot_informationally_and_the_other_sources_have_none():
    for block in BLOCKS:
        if block["config_type"] == "feedback_upload" and block["config_source"] == "mitra":
            assert block["bot_id"] is not None and block["bot_name"] is not None
        else:
            assert block["bot_id"] is None and block["bot_name"] is None


def test_the_educator_has_no_google_forms_config():
    assert ("feedback_upload", "google_forms", "Educator_Voice") not in {
        (b["config_type"], b["config_source"], b["session_type"]) for b in BLOCKS}


def test_no_hard_coded_dates_in_the_seed():
    assert re.search(r"\b20\d\d-\d\d-\d\d", SQL) is None
    for block in BLOCKS:
        if block["config_source"] == "mitra":
            assert block["config"]["questionnaire_versions"] == [{"key": "V1", "effective_from": None}]


# ------------------------------------------------------------------------------------------------ the readers accept them

@pytest.mark.parametrize("block", [b for b in BLOCKS if b["config_type"] == "feedback_upload"],
                         ids=lambda b: f"{b['config_source']}-{b['session_type']}")
def test_every_upload_config_has_the_required_settings_at_their_defaults(block):
    config = block["config"]
    defaults = feedback_config_key_defaults()
    validation.validate_feedback_config_keys(config)       # raises on a missing or invalid required setting
    assert config["max_csv_upload_bytes"] == defaults["max_csv_upload_bytes"]
    assert config["ingestion_schema"] == defaults["ingestion_schema"]
    assert config["process_steps"] == [{"name": "feedback_metadata_extraction"}]


@pytest.mark.parametrize("persona", list(PERSONAS))
def test_the_mitra_configs_carry_the_baseline_version_the_opening_row_rule_and_the_persona_question(persona):
    config = next(b["config"] for b in BLOCKS if (b["config_source"], b["session_type"], b["config_type"]) == (
        "mitra", PERSONAS[persona]["session_type"], "feedback_upload"))
    assert parse_versions(config) == [("V1", None)]
    assert config["opening_row"] == {"mode": "auto"} and config["sequence_offset"] == 1
    assert config["questions"][0]["key"] == PERSONAS[persona]["first_question_key"]


@pytest.mark.parametrize("block", [b for b in BLOCKS if b["config_source"] == "google_forms"],
                         ids=lambda b: b["session_type"])
def test_the_google_forms_configs_are_accepted_by_the_gforms_parser(block):
    config = block["config"]
    assert config["adapter"] == "wide_form" and config["forms"]
    for form in config["forms"].values():
        assert form["questionnaire_version"] == "V1" and len(form["questions"]) > 0
    feedback_gforms._check_config(config)          # raises on a config the Google Forms path would reject


@pytest.mark.parametrize("block", [b for b in BLOCKS if b["config_type"] == "feedback_extraction"],
                         ids=lambda b: b["session_type"])
def test_every_extraction_config_is_accepted_by_the_extraction_parser(block):
    parsed = fx.parse_extraction_config(block["config"])
    assert parsed.session_type == block["session_type"]
    assert parsed.prompt_name.startswith("Feedback Metadata Extraction - ")
    # CHANGED: the seeded configs set llm.max_tokens (see tests/test_feedback_seed_llm.py)
    assert block["config"]["llm"] == {"max_tokens": 10000} and block["config"]["pii"]["types"][-1]["name"] == "other"
    assert block["change_note"].startswith("Initial extraction config")
