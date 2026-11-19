"""
Unit tests (no database) of the legacy-upgrade helpers of app/database/feedback_legacy_upgrade.py and of the files
the upgrade path depends on: what a legacy config lacks, the defaults the migration writes, the operator's mapping
(never guessed), the commands the startup health check prints, schema.sql's unique-index function and the Dockerfile
(the image must carry the tools the README and the app's messages tell operators to run). Synthetic data only.
"""
import copy
import json
import re
import uuid
from pathlib import Path

import pytest

from app.database import feedback_legacy_upgrade as up
from app.services.feedback_adapter_config import validate_adapter_config
from app.services.feedback_constants import SAFE_CODE_RE
from seed_support import gforms_config_v1, mitra_config_v1

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = (ROOT / "schema.sql").read_text(encoding="utf-8")

NEW_MITRA_KEYS = ("session_type", "adapter", "column_map", "opening_row", "questionnaire_versions", "completion_map",
                  "max_csv_upload_bytes", "ingestion_schema", "process_steps")


def legacy_mitra():
    """The shape of a release-1.3 Mitra config: none of the keys added since, plus the dead keys it carried."""
    config = copy.deepcopy(mitra_config_v1("Parent"))
    for key in NEW_MITRA_KEYS:
        config.pop(key, None)
    config["columns"] = {"required": ["bot_id", "session_status"], "optional": [], "ignored": []}
    config["completion_values"] = ["COMPLETED", "IN_PROGRESS"]
    return config


def legacy_gforms():
    config = copy.deepcopy(gforms_config_v1("Parent"))
    for key in ("session_type", "adapter", "max_csv_upload_bytes", "ingestion_schema", "process_steps"):
        config.pop(key, None)
    for form in config["forms"].values():
        form.pop("questionnaire_version", None)
    return config


# ------------------------------------------------------------------------------------------------ what is missing

def test_a_legacy_mitra_config_lacks_the_required_keys_and_the_optional_defaults():
    gaps = up.legacy_gaps(legacy_mitra(), "mitra")
    assert gaps["required"] == ["adapter", "max_csv_upload_bytes", "ingestion_schema", "process_steps",
                                "session_type", "questionnaire_versions"]
    assert gaps["defaults"] == ["opening_row", "column_map", "completion_map"]


def test_a_legacy_google_forms_config_lacks_the_form_versions_and_never_the_conversation_keys():
    gaps = up.legacy_gaps(legacy_gforms(), "google_forms")
    forms = legacy_gforms()["forms"]
    assert gaps["required"][:5] == ["adapter", "max_csv_upload_bytes", "ingestion_schema", "process_steps",
                                    "session_type"]
    assert gaps["required"][5:] == [f"forms[{key}].questionnaire_version" for key in forms]
    assert gaps["defaults"] == []


def test_a_current_config_has_no_gaps():
    assert up.legacy_gaps(mitra_config_v1("Parent"), "mitra") == {"required": [], "defaults": []}
    assert up.legacy_gaps(gforms_config_v1("Parent"), "google_forms") == {"required": [], "defaults": []}


def test_the_adapter_of_an_unknown_source_is_not_invented():
    config = legacy_mitra()
    additions, unresolved = up.legacy_additions(config, "ptm_app", "Teacher_Voice")
    assert unresolved == ["adapter"] and "adapter" not in additions
    # a present but invalid adapter is never replaced either
    config["adapter"] = "csv_v2"
    assert up.legacy_adapter(config, "mitra") is None
    assert "adapter" not in up.legacy_additions(config, "mitra", "Teacher_Voice")[0]


# ------------------------------------------------------------------------------------------------ what is added

def test_the_documented_defaults_are_added_to_a_legacy_mitra_config_and_the_result_validates():
    config = legacy_mitra()
    additions, unresolved = up.legacy_additions(config, "mitra", "Parent_Voice")
    assert unresolved == []
    assert additions["session_type"] == "Parent_Voice"
    assert additions["adapter"] == "conversation_rows"
    assert additions["questionnaire_versions"] == [{"key": "V1", "effective_from": None}]
    assert additions["opening_row"] == {"mode": "auto"}
    assert additions["column_map"] == {role: role for role in additions["column_map"]}
    assert additions["completion_map"] == {"COMPLETED": "COMPLETED", "IN_PROGRESS": "IN_PROGRESS"}
    assert additions["process_steps"] == [] and additions["max_csv_upload_bytes"] == 104857600
    assert validate_adapter_config({**config, **additions}) == "conversation_rows"


def test_existing_keys_are_never_overwritten_and_a_second_pass_adds_nothing():
    config = legacy_mitra()
    config["opening_row"] = {"mode": "none"}
    config["questionnaire_versions"] = [{"key": "V1", "effective_from": None},
                                        {"key": "V2", "effective_from": "2026-11-03T00:00:00+00:00"}]
    additions, _ = up.legacy_additions(config, "mitra", "Parent_Voice")
    assert "opening_row" not in additions and "questionnaire_versions" not in additions
    merged = {**config, **additions}
    assert merged["opening_row"] == {"mode": "none"}
    assert up.legacy_additions(merged, "mitra", "Parent_Voice") == ({}, [])


def test_a_legacy_completion_values_list_becomes_the_identity_map_and_a_foreign_vocabulary_is_refused():
    config = legacy_mitra()
    config["completion_values"] = ["COMPLETED"]
    assert up.legacy_additions(config, "mitra", "Parent_Voice")[0]["completion_map"] == {"COMPLETED": "COMPLETED"}
    config["completion_values"] = ["DONE", "PARTIAL"]
    with pytest.raises(ValueError, match="completion_map"):
        up.legacy_additions(config, "mitra", "Parent_Voice")


def test_every_form_of_a_google_forms_config_gets_the_default_version_and_the_input_is_untouched():
    config = legacy_gforms()
    before = copy.deepcopy(config)
    additions, unresolved = up.legacy_additions(config, "google_forms", "Parent_Voice")
    assert unresolved == [] and config == before
    assert additions["adapter"] == "wide_form" and additions["session_type"] == "Parent_Voice"
    assert {f["questionnaire_version"] for f in additions["forms"].values()} == {"V1"}
    assert "questionnaire_versions" not in additions and "opening_row" not in additions
    assert up.form_versions({**config, **additions}) == ["V1"]
    assert up.form_versions(config) == []


# ------------------------------------------------------------------------------------------------ the operator's mapping

def test_the_mapping_parses_entries_and_a_file_and_refuses_unsafe_or_conflicting_values():
    program = str(uuid.uuid4())
    assert up.parse_session_type_map([f"{program}=Parent_Voice", "leader:General Public=General_Public_Voice",
                                      "1M Voice=Youth_Voice"]) == {
        program: "Parent_Voice", "leader:General Public": "General_Public_Voice", "1M Voice": "Youth_Voice"}
    assert up.parse_session_type_map(["a=B"], json.dumps({"c": "D"})) == {"a": "B", "c": "D"}
    assert up.parse_session_type_map(["a=B"], json.dumps({"a": "B"})) == {"a": "B"}
    for bad in (["no-separator"], ["=Youth_Voice"], ["a="], ["a=has space"], ["a=../x"], ["a=" + "x" * 65]):
        with pytest.raises(ValueError):
            up.parse_session_type_map(bad)
    with pytest.raises(ValueError, match="two different"):
        up.parse_session_type_map(["a=B", "a=C"])
    for text in ("[]", "not json", '{"a": 5}', '{" a": "B"}'):
        with pytest.raises(ValueError):
            up.parse_session_type_map([], text)


def test_the_mapping_resolves_by_id_leader_or_unique_name_and_never_skips_a_typo():
    youth, parent, other = (str(uuid.uuid4()) for _ in range(3))
    programs = [{"id": uuid.UUID(youth), "name": "1M Voice", "leader_name": "Youth"},
                {"id": uuid.UUID(parent), "name": "1M Voice", "leader_name": "Parent"},
                {"id": uuid.UUID(other), "name": "Pilot", "leader_name": "Youth"}]
    resolve = up._resolve_mapping
    assert resolve({youth: "Youth_Voice"}, programs) == {youth: "Youth_Voice"}
    assert resolve({"leader:Youth": "Youth_Voice"}, programs) == {youth: "Youth_Voice", other: "Youth_Voice"}
    assert resolve({"Pilot": "Pilot_Voice"}, programs) == {other: "Pilot_Voice"}
    with pytest.raises(ValueError, match="2 programs of the tenant have that name"):
        resolve({"1M Voice": "Youth_Voice"}, programs)                      # a shared name is ambiguous, never guessed
    for key in ("Nope", "leader:Nope", str(uuid.uuid4())):
        with pytest.raises(ValueError, match="matches no program|is not a program"):
            resolve({key: "X"}, programs)
    with pytest.raises(ValueError, match="two mapping keys"):
        resolve({youth: "A", "leader:Youth": "B"}, programs)
    assert resolve({youth: "A", "leader:Youth": "A"}, programs) == {youth: "A", other: "A"}


# ------------------------------------------------------------------------------------------------ the health command

def _state(**over):
    state = {"null_session_type_programs": {}, "configs_missing_keys": [], "sessions_without_session_type": {},
             "sessions_without_questionnaire_version": {}, "index_missing": False, "duplicate_groups": 0}
    state.update(over)
    return state


def test_the_command_names_the_tenant_and_every_program_that_needs_a_persona_key():
    assert up.legacy_state_is_clean(_state())
    state = _state(null_session_type_programs={"acme": ["p1", "p2"], "zed": ["p3"]},
                   sessions_without_session_type={"acme": 4})
    assert not up.legacy_state_is_clean(state)
    assert up.legacy_commands(state) == [
        "python tools/feedback_config.py migrate-legacy --tenant-code acme --map p1=<SESSION_TYPE> "
        "--map p2=<SESSION_TYPE> --dry-run",
        "python tools/feedback_config.py migrate-legacy --tenant-code zed --map p3=<SESSION_TYPE> --dry-run"]


def test_a_tenant_with_only_missing_keys_needs_no_mapping_in_its_command():
    state = _state(configs_missing_keys=[{"tenant_code": "acme", "source": "mitra", "program_id": "p1",
                                          "session_type": "A_Voice", "keys": ["adapter"]}])
    assert up.legacy_commands(state) == ["python tools/feedback_config.py migrate-legacy --tenant-code acme --dry-run"]


# ------------------------------------------------------------------------------------------------ schema.sql

def test_the_unique_index_is_created_by_a_function_that_never_raises_and_spans_the_tenant():
    body = re.search(r"CREATE OR REPLACE FUNCTION ensure_config_unique_index\(\).*?\$fn\$;", SCHEMA, re.S).group(0)
    assert up.CONFIG_UNIQUE_INDEX in body and "EXCEPTION WHEN unique_violation THEN" in body and "RETURN FALSE" in body
    assert re.search(r"ON config \(tenant_code, config_source, config_type,\s+COALESCE\(program_id, "
                     r"'00000000-0000-0000-0000-000000000000'\), COALESCE\(session_type, ''\)\)", body)
    # the older keys are dropped only after the new index exists
    assert body.index("CREATE UNIQUE INDEX") < body.index(f"DROP INDEX IF EXISTS {up.LEGACY_CONFIG_INDEX}")
    assert "DROP INDEX IF EXISTS uq_config_source_type_program_session_type" in body
    assert "SELECT ensure_config_unique_index();" in SCHEMA
    # the only deterministic backfill uses the same persona-key rule as the application
    assert re.search(r"~ '\^" + re.escape(SAFE_CODE_RE) + r"\$'", body)
    # no statement of the file creates the new unique index outside the function (that would crash startup again)
    assert len(re.findall(r"CREATE UNIQUE INDEX[^;]*" + up.CONFIG_UNIQUE_INDEX, SCHEMA)) == 1


def test_config_source_has_no_check_constraint_in_the_schema_and_old_ones_are_dropped():
    table = re.search(r"^CREATE TABLE config \((.*?)^\);", SCHEMA, re.M | re.S).group(1)
    assert "CHECK (" not in table.split("config_source", 1)[1].split("\n", 1)[0]
    assert "config_source IN" not in SCHEMA
    assert "ALTER TABLE config DROP CONSTRAINT %I" in SCHEMA


def test_the_not_null_comments_say_that_an_older_database_has_nulls():
    for table, column in (("feedback_submissions", "session_type"), ("feedback_submissions", "questionnaire_version"),
                          ("config", "session_type")):
        body = re.search(rf"^CREATE TABLE {table} \((.*?)^\);", SCHEMA, re.M | re.S).group(1)
        line = next(l for l in body.split("\n") if l.strip().startswith(column + " "))
        assert "migrate-legacy" in line and "NOT NULL" in line, line


# ------------------------------------------------------------------------------------------------ the image

def _copied_paths(dockerfile: str):
    """Every source path of a COPY instruction (flags ignored); a directory path ends with '/'."""
    paths = []
    for line in dockerfile.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0] == "COPY":
            sources = [p for p in parts[1:-1] if not p.startswith("--")]
            paths.extend(sources)
    return paths


def _in_image(path: str, copied) -> bool:
    return any(path == c or (c.endswith("/") and path.startswith(c)) or path.rstrip("/") == c.rstrip("/")
               for c in copied)


# Tools the README tells the operator to run on the machine that holds the export, not in the container.
WORKSTATION_TOOLS = {"tools/split_mitra_export.py"}


def _tools_named_in_the_app():
    """tools/<name>.py references in what operators READ in the app: its messages and the DLQ reason."""
    texts = [p.read_text(encoding="utf-8") for p in (ROOT / "app").rglob("*.py")]
    named = set()
    for text in texts:
        named.update(re.findall(r"\btools/([A-Za-z0-9_]+\.py)", text))
    return {f"tools/{name}" for name in named if (ROOT / "tools" / name).is_file()}


def _first_party_imports():
    """Top-level modules / packages of the repo root that app/ and main.py import (derived by parsing them)."""
    import ast
    sources = [ROOT / "main.py", *(ROOT / "app").rglob("*.py")]
    found = set()
    for path in sources:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                found.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module is not None:
                found.add(node.module.split(".")[0])
    paths = set()
    for name in found:
        if (ROOT / f"{name}.py").is_file():
            paths.add(f"{name}.py")
        elif (ROOT / name).is_dir() and (ROOT / name / "__init__.py").is_file():
            paths.add(f"{name}/")
    return paths


def _runtime_files():
    """What the image needs from the repo: every file db.py reads from the repo root (its BASE_DIR / "x" constants),
    every first-party module app/ and main.py import, and the launcher script."""
    db_source = (ROOT / "app" / "database" / "db.py").read_text(encoding="utf-8")
    files = {name for name in re.findall(r'BASE_DIR / "([^"]+)"', db_source)}
    return files | _first_party_imports() | {"run_all.sh"}


def test_the_image_carries_every_file_the_app_reads_and_every_tool_operators_are_told_to_run():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    copied = _copied_paths(dockerfile)
    assert "app/" in copied
    missing = [f for f in sorted(_runtime_files()) if not _in_image(f, copied)]
    assert missing == [], f"the Dockerfile does not COPY: {missing}"
    assert "app/" in _first_party_imports()          # the derivation finds the app package (so it is not vacuous)
    named = _tools_named_in_the_app() - WORKSTATION_TOOLS
    # the tools of the documented operations are among them (a rename must update the docs, the messages and this list)
    assert {"tools/feedback_config.py", "tools/questionnaire_versions.py",
            "tools/run_feedback_extraction.py"} <= named
    not_copied = [t for t in sorted(named) if not _in_image(t, copied)]
    assert not_copied == [], f"README / messages tell operators to run these, the image lacks them: {not_copied}"


def test_the_dockerignore_does_not_exclude_tools_or_the_seeds():
    ignored = [line.strip() for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()]
    for needed in ("tools", "tools/", "schema.sql", "seed_config.sql", "seed_prompts.sql", "seed_themes.sql"):
        assert needed not in ignored
