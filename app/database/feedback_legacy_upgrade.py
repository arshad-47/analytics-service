"""
Upgrade of a database that was created BEFORE the feedback `session_type` / questionnaire versions / adapters
existed (the schema of release 1.3: one config row per Mitra bot, no persona key on configs or stored sessions).

What startup does on such a database (schema.sql, never raises): add the nullable columns, keep the old unique index
until a new one can exist, create the new one (`uq_config_tenant_source_type_program_session_type`) when no two config
rows collide. What it CANNOT do is invent the persona key of a config or of a stored session: inference is not
generic. That is the job of ONE explicit, idempotent, tenant-scoped operation, `migrate_legacy`
(`python tools/feedback_config.py migrate-legacy`), which

  1. (optional, explicit) retires config rows the operator names (`retire_config_ids`): the old design kept one
     config row per bot, so a persona that ran a second bot has two rows for one (source, program) and only ONE
     can stay; the others are deleted (their versions with them) and the operator re-records the V2 window with
     tools/questionnaire_versions.py;
  2. gives every legacy upload config its `session_type` (column and JSON key): from the column, else from the JSON,
     else from the operator's mapping (program id | program name | `leader:<category name>` -> session_type). A
     config the mapping does not cover is reported as unmapped and left untouched; nothing is guessed;
  3. writes ONE new config version per config (the config row is locked first, the old version is deactivated and
     kept as history) that adds ONLY the keys that are missing, with these defaults:
         every config      session_type, max_csv_upload_bytes / ingestion_schema / process_steps (the seed defaults),
                           adapter (derived from the legacy source label: mitra -> conversation_rows, google_forms ->
                           wide_form; any other source is reported, never invented)
         conversation_rows questionnaire_versions [{"key": "V1", "effective_from": null}], opening_row {"mode":
                           "auto"} (accepts files with and without the bot's opening row), column_map (every role
                           reads the column of the same name), completion_map (COMPLETED / IN_PROGRESS map to
                           themselves, or the legacy completion_values list as an identity map)
         wide_form         forms[*].questionnaire_version "V1"
     An existing key is never overwritten; a config whose result would not validate is not written (reported);
  4. fills feedback_submissions.session_type (the persona of the config of the session's program and source, only
     when that config is the only one of its tenant / source / program) and questionnaire_version (conversation_rows:
     stamped from the config's windows and the session's first message; wide_form: the version all of the config's
     forms share) where they are NULL. A value that is already set is never changed;
  5. creates the unique index once the data allows it (`ensure_config_unique_index()` of schema.sql).

It reports counts and identifiers only (program ids, config ids, key names), never config content or respondent data.
With dry_run everything is computed and rolled back. A second run changes nothing.

This module also holds the READ-ONLY detection (`find_legacy_state`) the startup health check uses to tell the
operator, in one ERROR line, the exact command to run.
"""
import copy
import json
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.services.feedback_adapter_config import (
    DEFAULT_COLUMN_MAP,
    resolve_completion_map,
    validate_adapter_config,
)
from app.services.feedback_config_validation import validate_feedback_config_keys
from app.services.feedback_constants import (
    ADAPTER_CONVERSATION_ROWS,
    ADAPTER_WIDE_FORM,
    CONFIG_KEY_ADAPTER,
    CONFIG_TYPE_FEEDBACK_UPLOAD,
    LEGACY_ADAPTER_BY_CONFIG_SOURCE,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
    SAFE_CODE_RE,
    feedback_config_key_defaults,
)

# The unique index of schema.sql (ensure_config_unique_index): one config per tenant, source, type, program, persona.
CONFIG_UNIQUE_INDEX = "uq_config_tenant_source_type_program_session_type"
# The index of the old design (one config per bot), kept by schema.sql until the new one exists.
LEGACY_CONFIG_INDEX = "uq_config_source_type_program_bot"

LEGACY_MIGRATE_COMMAND = "python tools/feedback_config.py migrate-legacy"

KEY_SESSION_TYPE = "session_type"
KEY_QUESTIONNAIRE_VERSIONS = "questionnaire_versions"
KEY_OPENING_ROW = "opening_row"
KEY_COLUMN_MAP = "column_map"
KEY_COMPLETION_MAP = "completion_map"
KEY_FORMS = "forms"
KEY_FORM_VERSION = "questionnaire_version"

# Documented defaults of the keys a legacy config lacks (the seed writes the same values into a new database).
DEFAULT_QUESTIONNAIRE_VERSION = "V1"
DEFAULT_QUESTIONNAIRE_VERSIONS: List[Dict[str, Any]] = [{"key": DEFAULT_QUESTIONNAIRE_VERSION, "effective_from": None}]
DEFAULT_OPENING_ROW: Dict[str, str] = {"mode": "auto"}

LEADER_KEY_PREFIX = "leader:"

_SAFE_CODE = re.compile(SAFE_CODE_RE)


# ---------------------------------------------------------------------------------------------
# pure: what a legacy config lacks
# ---------------------------------------------------------------------------------------------

def _is_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def legacy_adapter(config: Mapping[str, Any], config_source: str) -> Optional[str]:
    """The adapter a config uses: its own valid `adapter`, else the one its legacy source implied, else None."""
    own = config.get(CONFIG_KEY_ADAPTER)
    if own in (ADAPTER_CONVERSATION_ROWS, ADAPTER_WIDE_FORM):
        return own
    if CONFIG_KEY_ADAPTER in config:
        return None  # present but invalid: reported by the validators, never replaced
    return LEGACY_ADAPTER_BY_CONFIG_SOURCE.get(config_source)


def legacy_gaps(config: Mapping[str, Any], config_source: str) -> Dict[str, List[str]]:
    """
    {"required": [...], "defaults": [...]}: the keys a legacy upload config lacks. `required` ones stop the persona
    working (uploads rejected, events dead-lettered); `defaults` are optional keys the migration also writes so the
    config looks like a new one (opening_row, column_map, completion_map). Pure; names only.
    """
    required: List[str] = []
    defaults: List[str] = []
    for key in REQUIRED_FEEDBACK_CONFIG_KEYS:
        if key not in config:
            required.append(key)
    if KEY_SESSION_TYPE not in config:
        required.append(KEY_SESSION_TYPE)
    adapter = legacy_adapter(config, config_source)
    if adapter == ADAPTER_CONVERSATION_ROWS:
        if KEY_QUESTIONNAIRE_VERSIONS not in config:
            required.append(KEY_QUESTIONNAIRE_VERSIONS)
        for key in (KEY_OPENING_ROW, KEY_COLUMN_MAP, KEY_COMPLETION_MAP):
            if key not in config:
                defaults.append(key)
    elif adapter == ADAPTER_WIDE_FORM:
        forms = config.get(KEY_FORMS)
        if isinstance(forms, dict):
            for form_key, form in forms.items():
                if isinstance(form, dict) and KEY_FORM_VERSION not in form:
                    required.append(f"{KEY_FORMS}[{form_key}].{KEY_FORM_VERSION}")
    return {"required": required, "defaults": defaults}


def legacy_additions(config: Mapping[str, Any], config_source: str, session_type: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    (additions, unresolved): the key -> value pairs to add to `config` (never an existing key) and the keys that
    are missing but cannot be derived (the adapter of a source that is not a legacy one; a form that is not an
    object). The returned `forms` replaces the config's `forms` with the same forms plus their version.
    """
    additions: Dict[str, Any] = {}
    unresolved: List[str] = []
    defaults = feedback_config_key_defaults()
    for key, value in defaults.items():
        if key not in config:
            additions[key] = value
    if KEY_SESSION_TYPE not in config:
        additions[KEY_SESSION_TYPE] = session_type
    if CONFIG_KEY_ADAPTER not in config:
        adapter = LEGACY_ADAPTER_BY_CONFIG_SOURCE.get(config_source)
        if adapter is None:
            unresolved.append(CONFIG_KEY_ADAPTER)
        else:
            additions[CONFIG_KEY_ADAPTER] = adapter
    adapter = legacy_adapter(config, config_source)
    if CONFIG_KEY_ADAPTER in additions:
        adapter = additions[CONFIG_KEY_ADAPTER]
    if adapter == ADAPTER_CONVERSATION_ROWS:
        if KEY_QUESTIONNAIRE_VERSIONS not in config:
            additions[KEY_QUESTIONNAIRE_VERSIONS] = copy.deepcopy(DEFAULT_QUESTIONNAIRE_VERSIONS)
        if KEY_OPENING_ROW not in config:
            additions[KEY_OPENING_ROW] = dict(DEFAULT_OPENING_ROW)
        if KEY_COLUMN_MAP not in config:
            additions[KEY_COLUMN_MAP] = dict(DEFAULT_COLUMN_MAP)
        if KEY_COMPLETION_MAP not in config:
            additions[KEY_COMPLETION_MAP] = resolve_completion_map(dict(config))  # ValueError: legacy list invalid
    elif adapter == ADAPTER_WIDE_FORM:
        forms = config.get(KEY_FORMS)
        if isinstance(forms, dict):
            new_forms = copy.deepcopy(forms)
            changed = False
            for form_key, form in new_forms.items():
                if not isinstance(form, dict):
                    unresolved.append(f"{KEY_FORMS}[{form_key}]")
                elif KEY_FORM_VERSION not in form:
                    form[KEY_FORM_VERSION] = DEFAULT_QUESTIONNAIRE_VERSION
                    changed = True
            if changed:
                additions[KEY_FORMS] = new_forms
    return additions, unresolved


def adapter_config_problem(config: Any) -> Optional[str]:
    """The reason the adapter-related keys (adapter, column_map, completion_map, keys of the other adapter) are
    invalid, or None when they are valid. A config without `adapter` is "missing", reported by the callers."""
    if not isinstance(config, dict) or CONFIG_KEY_ADAPTER not in config:
        return None
    try:
        validate_adapter_config(config)
    except ValueError as exc:
        return str(exc)
    return None


def form_versions(config: Mapping[str, Any]) -> List[str]:
    """The distinct questionnaire_version values of the forms of a wide_form config (sorted)."""
    forms = config.get(KEY_FORMS)
    if not isinstance(forms, dict):
        return []
    return sorted({f[KEY_FORM_VERSION] for f in forms.values()
                   if isinstance(f, dict) and _is_text(f.get(KEY_FORM_VERSION))})


# ---------------------------------------------------------------------------------------------
# the operator's mapping
# ---------------------------------------------------------------------------------------------

def parse_session_type_map(entries: Sequence[str] = (), file_text: Optional[str] = None) -> Dict[str, str]:
    """
    The operator's mapping as {key: session_type}: `--map KEY=SESSION_TYPE` entries and/or a JSON object (text of
    `--map-file`). KEY is a program id (UUID), `leader:<leader category name>` or a program name. Raises ValueError
    for a malformed entry, an unsafe session_type, or the same key mapped to two values.
    """
    mapping: Dict[str, str] = {}

    def put(key: Any, value: Any) -> None:
        if not _is_text(key) or key != key.strip():
            raise ValueError("a mapping key must be a non-empty program id, program name or leader:<name>")
        if not isinstance(value, str) or _SAFE_CODE.fullmatch(value) is None:
            raise ValueError(f"the session_type of mapping key {key!r} must match {SAFE_CODE_RE}")
        if key in mapping and mapping[key] != value:
            raise ValueError(f"mapping key {key!r} is given two different session types")
        mapping[key] = value

    for entry in entries:
        key, separator, value = entry.rpartition("=")
        if separator == "":
            raise ValueError(f"--map {entry!r} must look like KEY=SESSION_TYPE")
        put(key, value)
    if file_text is not None:
        try:
            loaded = json.loads(file_text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"the mapping file is not valid JSON: {exc}")
        if not isinstance(loaded, dict):
            raise ValueError("the mapping file must be a JSON object of KEY -> SESSION_TYPE")
        for key, value in loaded.items():
            put(key, value)
    return mapping


def _resolve_mapping(mapping: Mapping[str, str], programs: List[Mapping[str, Any]]) -> Dict[str, str]:
    """program id (str) -> session_type. A key that matches no program, or a program name shared by several
    programs, is an error (listing program ids): a typo or an ambiguity must never silently skip a persona."""
    by_program: Dict[str, str] = {}

    def assign(program_id: str, session_type: str, key: str) -> None:
        if by_program.get(program_id, session_type) != session_type:
            raise ValueError(f"program {program_id} is matched by two mapping keys with different session types "
                             f"(one of them {key!r})")
        by_program[program_id] = session_type

    for key, session_type in mapping.items():
        if key.startswith(LEADER_KEY_PREFIX):
            leader = key[len(LEADER_KEY_PREFIX):]
            matched = [p for p in programs if p["leader_name"] == leader]
            if len(matched) == 0:
                raise ValueError(f"mapping key {key!r} matches no program of the tenant (leader category name)")
        else:
            try:
                program_id = str(uuid.UUID(key))
            except ValueError:
                matched = [p for p in programs if p["name"] == key]
                if len(matched) == 0:
                    raise ValueError(f"mapping key {key!r} matches no program of the tenant (program name)")
                if len(matched) > 1:
                    ids = ", ".join(str(p["id"]) for p in matched)
                    raise ValueError(
                        f"mapping key {key!r}: {len(matched)} programs of the tenant have that name ({ids}); "
                        f"use a program id or leader:<leader category name>")
            else:
                matched = [p for p in programs if str(p["id"]) == program_id]
                if len(matched) == 0:
                    raise ValueError(f"mapping key {key!r} is not a program of the tenant")
        for program in matched:
            assign(str(program["id"]), session_type, key)
    return by_program


# ---------------------------------------------------------------------------------------------
# read-only detection (startup health check)
# ---------------------------------------------------------------------------------------------

async def find_legacy_state(conn) -> Dict[str, Any]:
    """
    Read-only. What is still legacy in the database:
      "null_session_type_programs": {tenant: [program id, ...]}   feedback_upload configs with a NULL session_type
      "configs_missing_keys": [{"tenant_code", "source", "program_id", "session_type", "keys": [...]}]
      "sessions_without_session_type" / "sessions_without_questionnaire_version": {tenant: n}
      "index_missing": bool, "duplicate_groups": n   (the unique index could not be created)
    Identifiers and counts only.
    """
    rows = await conn.fetch(
        """
        SELECT c.id, c.tenant_code, c.config_source, c.program_id, c.session_type, v.config
        FROM config c
        LEFT JOIN config_version v ON v.config_id = c.id AND v.is_active
        WHERE c.config_type = $1
        ORDER BY c.tenant_code, c.config_source, c.program_id NULLS FIRST, c.id
        """,
        CONFIG_TYPE_FEEDBACK_UPLOAD,
    )
    null_programs: Dict[str, List[str]] = {}
    missing: List[Dict[str, Any]] = []
    for row in rows:
        if row["session_type"] is None:
            programs = null_programs.setdefault(row["tenant_code"], [])
            program = "-" if row["program_id"] is None else str(row["program_id"])
            if program not in programs:
                programs.append(program)
        config = row["config"]
        if config is None:
            continue
        if isinstance(config, str):
            config = json.loads(config)
        if not isinstance(config, dict):
            continue
        keys = legacy_gaps(config, row["config_source"])["required"]
        if len(keys) > 0:
            missing.append({"tenant_code": row["tenant_code"], "source": row["config_source"],
                            "program_id": None if row["program_id"] is None else str(row["program_id"]),
                            "session_type": row["session_type"], "keys": keys})
    without_type = {r["tenant_code"]: r["n"] for r in await conn.fetch(
        "SELECT tenant_code, count(*) AS n FROM feedback_submissions WHERE session_type IS NULL GROUP BY tenant_code")}
    without_version = {r["tenant_code"]: r["n"] for r in await conn.fetch(
        "SELECT tenant_code, count(*) AS n FROM feedback_submissions WHERE questionnaire_version IS NULL "
        "GROUP BY tenant_code")}
    index_exists = await conn.fetchval(
        "SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1", CONFIG_UNIQUE_INDEX)
    duplicate_groups = 0
    if index_exists is None:
        duplicate_groups = await conn.fetchval(
            """
            SELECT count(*) FROM (
                SELECT 1 FROM config
                GROUP BY tenant_code, config_source, config_type,
                         COALESCE(program_id, '00000000-0000-0000-0000-000000000000'), COALESCE(session_type, '')
                HAVING count(*) > 1) d
            """
        )
    return {
        "null_session_type_programs": null_programs,
        "configs_missing_keys": missing,
        "sessions_without_session_type": without_type,
        "sessions_without_questionnaire_version": without_version,
        "index_missing": index_exists is None,
        "duplicate_groups": duplicate_groups,
    }


def legacy_state_is_clean(state: Mapping[str, Any]) -> bool:
    return (len(state["null_session_type_programs"]) == 0 and len(state["configs_missing_keys"]) == 0
            and len(state["sessions_without_session_type"]) == 0
            and len(state["sessions_without_questionnaire_version"]) == 0)


def legacy_commands(state: Mapping[str, Any]) -> List[str]:
    """The exact dry-run command per tenant that still has legacy rows (placeholders only where the operator must
    decide: the session_type of each program whose config has none)."""
    tenants = sorted({*state["null_session_type_programs"], *(m["tenant_code"] for m in state["configs_missing_keys"]),
                      *state["sessions_without_session_type"], *state["sessions_without_questionnaire_version"]})
    commands = []
    for tenant in tenants:
        parts = [LEGACY_MIGRATE_COMMAND, "--tenant-code", tenant]
        for program in state["null_session_type_programs"].get(tenant, []):
            parts.append(f"--map {program}=<SESSION_TYPE>")
        parts.append("--dry-run")
        commands.append(" ".join(parts))
    return commands


# ---------------------------------------------------------------------------------------------
# the migration
# ---------------------------------------------------------------------------------------------

def _label(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {"config_id": str(row["id"]), "source": row["config_source"],
            "program_id": None if row["program_id"] is None else str(row["program_id"])}


async def migrate_legacy(
    *,
    tenant_code: str,
    session_type_map: Optional[Mapping[str, str]] = None,
    source: Optional[str] = None,
    retire_config_ids: Sequence[str] = (),
    dry_run: bool = False,
    created_by: str = "cli",
) -> Dict[str, Any]:
    """
    The one-off upgrade of a legacy database for ONE tenant (module docstring). Returns counts and identifiers:

        {"tenant_code", "dry_run",
         "retired": n, "retire_not_found": n,    (an id that is not (any more) a config of the tenant: skipped)
         "configs": {"total", "session_type_set", "new_versions", "unchanged", "unmapped", "duplicates",
                     "conflicts", "invalid", "no_active_version"},
         "sessions": {"session_type_set", "questionnaire_version_set", "session_type_unresolved",
                      "questionnaire_version_unresolved"},
         "unique_index": "exists" | "created" | "blocked",
         "problems": [{"config_id", "source", "program_id", "problem": <short text>}]}

    Raises ValueError for a bad tenant, mapping or retire id (nothing is written; one transaction).
    """
    from app.database import feedback_operations as fo
    from app.database.db import db
    from app.services.feedback_versions import parse_versions, stamp_version

    if not isinstance(tenant_code, str) or _SAFE_CODE.fullmatch(tenant_code) is None:
        raise ValueError(f"tenant_code must match {SAFE_CODE_RE}")
    mapping = dict(session_type_map) if session_type_map is not None else {}
    retire_ids = []
    for raw in retire_config_ids:
        try:
            retire_ids.append(uuid.UUID(str(raw)))
        except ValueError:
            raise ValueError("--retire-config must be a config id (UUID)")
    if source is not None and (not isinstance(source, str) or source.strip() == ""):
        raise ValueError("source must be a non-empty string when given")

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {
        "tenant_code": tenant_code, "dry_run": dry_run, "retired": 0, "retire_not_found": 0,
        "configs": {"total": 0, "session_type_set": 0, "new_versions": 0, "unchanged": 0, "unmapped": 0,
                    "duplicates": 0, "conflicts": 0, "invalid": 0, "no_active_version": 0},
        "sessions": {"session_type_set": 0, "questionnaire_version_set": 0, "session_type_unresolved": 0,
                     "questionnaire_version_unresolved": 0},
        "unique_index": "exists", "problems": [],
    }
    counts = result["configs"]
    problems = result["problems"]

    def problem(row: Mapping[str, Any], text: str, counter: str) -> None:
        counts[counter] += 1
        problems.append({**_label(row), "problem": text})

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                if await conn.fetchval("SELECT 1 FROM tenant WHERE code = $1", tenant_code) is None:
                    raise ValueError(f"tenant {tenant_code!r} does not exist")

                # 1. retire the configs the operator named
                for config_id in retire_ids:
                    row = await conn.fetchrow(
                        "SELECT id, config_source, program_id FROM config WHERE id = $1 AND tenant_code = $2 "
                        "AND config_type = $3 FOR UPDATE", config_id, tenant_code, CONFIG_TYPE_FEEDBACK_UPLOAD)
                    if row is None:
                        # already retired by an earlier run (a second run changes nothing); a wrong id leaves the
                        # duplicates it was meant for in the report
                        result["retire_not_found"] += 1
                        continue
                    siblings = await conn.fetchval(
                        "SELECT count(*) FROM config WHERE tenant_code = $1 AND config_type = $2 "
                        "AND config_source = $3 AND program_id IS NOT DISTINCT FROM $4::uuid AND id <> $5",
                        tenant_code, CONFIG_TYPE_FEEDBACK_UPLOAD, row["config_source"], row["program_id"], config_id)
                    if siblings == 0:
                        raise ValueError(f"--retire-config {config_id} is the only config of its source and "
                                         f"program: retiring it would leave the persona without one")
                    await conn.execute("DELETE FROM config WHERE id = $1", config_id)
                    result["retired"] += 1

                # 2. the operator's mapping -> program ids
                programs = await conn.fetch(
                    """
                    SELECT p.id, p.name, l.name AS leader_name
                    FROM programs p LEFT JOIN leader_category l ON l.id = p.leaders_id
                    WHERE p.tenant_code = $1
                    """, tenant_code)
                by_program = _resolve_mapping(mapping, programs)

                # 3. every config of the tenant (the unique key is checked over all of them)
                all_rows = await conn.fetch(
                    "SELECT id, config_source, config_type, program_id, session_type FROM config "
                    "WHERE tenant_code = $1 ORDER BY config_source, program_id NULLS FIRST, id", tenant_code)
                targets = [r for r in all_rows if r["config_type"] == CONFIG_TYPE_FEEDBACK_UPLOAD
                           and (source is None or r["config_source"] == source)]
                counts["total"] = len(targets)

                # pass 1: resolve the session_type of every target (read, then lock + read the active version)
                plan: List[Dict[str, Any]] = []
                for row in targets:
                    active = await fo._lock_and_read_active(conn, row["id"])
                    if active is None:
                        counts["no_active_version"] += 1
                        continue
                    config = active["config"]
                    candidates = {}
                    if row["session_type"] is not None:
                        candidates["column"] = row["session_type"]
                    if KEY_SESSION_TYPE in config:
                        candidates["json"] = config[KEY_SESSION_TYPE]
                    program_key = None if row["program_id"] is None else str(row["program_id"])
                    if program_key is not None and program_key in by_program:
                        candidates["mapping"] = by_program[program_key]
                    for origin, value in candidates.items():
                        if not isinstance(value, str) or _SAFE_CODE.fullmatch(value) is None:
                            candidates = {"invalid": origin}
                            break
                    if "invalid" in candidates:
                        problem(row, f"the session_type of the {candidates['invalid']} is not a valid persona key",
                                "invalid")
                        continue
                    if len(set(candidates.values())) > 1:
                        problem(row, "the session_type of the column, the config and the mapping disagree: "
                                     + ", ".join(sorted(candidates)), "conflicts")
                        continue
                    if len(candidates) == 0:
                        problem(row, "no session_type: map its program with --map", "unmapped")
                        continue
                    plan.append({"row": row, "active": active, "session_type": next(iter(candidates.values()))})

                # the unique key after the assignment: two configs that end up with the same key can not both stay
                taken: Dict[Tuple, List[Any]] = {}
                planned_ids = {p["row"]["id"] for p in plan}
                for row in all_rows:
                    if row["id"] in planned_ids:
                        session_type = next(p["session_type"] for p in plan if p["row"]["id"] == row["id"])
                    else:
                        session_type = row["session_type"]
                    key = (row["config_source"], row["config_type"], row["program_id"], session_type)
                    taken.setdefault(key, []).append(row["id"])
                duplicated = {rid for ids in taken.values() if len(ids) > 1 for rid in ids}

                # pass 2: write
                done: List[Dict[str, Any]] = []
                for item in plan:
                    row, active, session_type = item["row"], item["active"], item["session_type"]
                    if row["id"] in duplicated:
                        problem(row, "another config of this tenant has the same source, program and session_type: "
                                     "keep one and retire the others with --retire-config <config id>", "duplicates")
                        continue
                    config = active["config"]
                    try:
                        additions, unresolved = legacy_additions(config, row["config_source"], session_type)
                        if len(unresolved) > 0:
                            raise ValueError("missing and cannot be derived: " + ", ".join(unresolved)
                                             + " (set it with tools/feedback_config.py set)")
                        new_config = {**config, **additions}
                        validate_feedback_config_keys(new_config)
                        validate_adapter_config(new_config)
                        if legacy_adapter(new_config, row["config_source"]) == ADAPTER_CONVERSATION_ROWS:
                            parse_versions(new_config)
                    except ValueError as exc:
                        problem(row, str(exc), "invalid")
                        continue
                    changed = False
                    if row["session_type"] is None:
                        await conn.execute("UPDATE config SET session_type = $1, updated_at = now() WHERE id = $2",
                                           session_type, row["id"])
                        counts["session_type_set"] += 1
                        changed = True
                    if len(additions) > 0:
                        await fo._write_new_config_version(
                            conn, row["id"], active["version_id"], new_config, created_by,
                            "legacy upgrade: added " + ", ".join(sorted(additions)))
                        counts["new_versions"] += 1
                        changed = True
                    if not changed:
                        counts["unchanged"] += 1
                    done.append({"row": row, "session_type": session_type, "config": new_config})

                # 4. the stored sessions of the configs that are now complete
                for item in done:
                    row, session_type, config = item["row"], item["session_type"], item["config"]
                    peers = sum(1 for r in all_rows if r["config_type"] == CONFIG_TYPE_FEEDBACK_UPLOAD
                                and r["config_source"] == row["config_source"]
                                and r["program_id"] == row["program_id"])
                    if peers == 1:
                        tag = await conn.execute(
                            """
                            UPDATE feedback_submissions fs SET session_type = $1, updated_at = now()
                            FROM submissions s
                            WHERE s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                              AND fs.tenant_code = $2 AND fs.source = $3 AND fs.session_type IS NULL
                              AND s.program_id IS NOT DISTINCT FROM $4::uuid
                            """, session_type, tenant_code, row["config_source"], row["program_id"])
                        result["sessions"]["session_type_set"] += int(tag.rsplit(" ", 1)[1])
                    adapter = legacy_adapter(config, row["config_source"])
                    if adapter == ADAPTER_WIDE_FORM:
                        versions = form_versions(config)
                        if len(versions) == 1:
                            tag = await conn.execute(
                                """
                                UPDATE feedback_submissions fs SET questionnaire_version = $1, updated_at = now()
                                WHERE fs.tenant_code = $2 AND fs.source = $3 AND fs.session_type = $4
                                  AND fs.questionnaire_version IS NULL
                                  AND EXISTS (SELECT 1 FROM submissions s
                                              WHERE s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                                                AND s.program_id IS NOT DISTINCT FROM $5::uuid)
                                """, versions[0], tenant_code, row["config_source"], session_type, row["program_id"])
                            result["sessions"]["questionnaire_version_set"] += int(tag.rsplit(" ", 1)[1])
                    elif adapter == ADAPTER_CONVERSATION_ROWS:
                        windows = parse_versions(config)
                        sessions = await conn.fetch(
                            """
                            SELECT fs.submission_id, s.submission_date
                            FROM feedback_submissions fs
                            JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                            WHERE fs.tenant_code = $1 AND fs.source = $2 AND fs.session_type = $3
                              AND fs.questionnaire_version IS NULL AND s.submission_date IS NOT NULL
                              AND s.program_id IS NOT DISTINCT FROM $4::uuid
                            """, tenant_code, row["config_source"], session_type, row["program_id"])
                        if len(sessions) > 0:
                            ids = [s["submission_id"] for s in sessions]
                            labels = [stamp_version(windows, s["submission_date"]) for s in sessions]
                            await conn.execute(
                                """
                                UPDATE feedback_submissions fs SET questionnaire_version = t.version, updated_at = now()
                                FROM unnest($1::text[], $2::text[]) AS t(submission_id, version)
                                WHERE fs.submission_id = t.submission_id AND fs.tenant_code = $3
                                  AND fs.source = $4 AND fs.session_type = $5 AND fs.questionnaire_version IS NULL
                                """, ids, labels, tenant_code, row["config_source"], session_type)
                            result["sessions"]["questionnaire_version_set"] += len(ids)
                result["sessions"]["session_type_unresolved"] = await conn.fetchval(
                    "SELECT count(*) FROM feedback_submissions WHERE tenant_code = $1 AND session_type IS NULL",
                    tenant_code)
                result["sessions"]["questionnaire_version_unresolved"] = await conn.fetchval(
                    "SELECT count(*) FROM feedback_submissions WHERE tenant_code = $1 AND questionnaire_version IS NULL",
                    tenant_code)

                # 5. the unique index, once the data allows it
                existed = await conn.fetchval(
                    "SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1",
                    CONFIG_UNIQUE_INDEX) is not None
                exists_now = await conn.fetchval("SELECT ensure_config_unique_index()")
                result["unique_index"] = "exists" if existed else ("created" if exists_now else "blocked")
                if dry_run:
                    raise fo._DryRunRollback()
    except fo._DryRunRollback:
        pass
    return result


# ---------------------------------------------------------------------------------------------
# `show` and `set` of the tool: the adapter content is validated there too
# ---------------------------------------------------------------------------------------------

async def adapter_problems_for(selectors: Mapping[str, Any]) -> Dict[Tuple[str, str, str, Optional[str]], str]:
    """
    {(tenant_code, program_id, session_type, source): reason} for the ACTIVE configs matching the selectors
    whose adapter-related keys (adapter, column_map, completion_map, keys of the other adapter) are invalid.
    A config without `adapter` is not listed here (it shows as missing under `problems`). Read-only.
    """
    from app.database import feedback_operations as fo
    from app.database.db import db

    if not db.pool:
        await db.connect()
    found: Dict[Tuple, str] = {}
    async with db.pool.acquire() as conn:
        rows = await fo._find_upload_configs(conn, **selectors)
    for row in rows:
        reason = adapter_config_problem(fo._as_dict(row["config"]))
        if reason is not None:
            program = None if row["program_id"] is None else str(row["program_id"])
            found[(row["tenant_code"], program, row["session_type"], row["config_source"])] = reason
    return found


async def adapter_problem_after_set(
    session_type: str,
    source: str,
    key: str,
    value: Any,
    *,
    unset: bool = False,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
) -> Optional[str]:
    """
    For `tools/feedback_config.py set`: the reason the persona's config would NOT validate as a whole after the
    change, or None. Only the adapter block (adapter, column_map, completion_map) interacts, so only a change to one
    of them is checked (switching to wide_form while column_map is still there is refused). Read-only, no lock: this
    is the early, friendly check; the write itself (feedback_operations.set_feedback_config_value) validates the
    resulting block AGAIN on the config it re-reads under the row lock, which is the one that counts.
    Returns None when the selectors do not identify exactly one active config (the write reports that itself).
    """
    from app.database import feedback_operations as fo
    from app.database.db import db

    if key not in fo._ADAPTER_BLOCK_KEYS:
        return None
    if not db.pool:
        await db.connect()
    async with db.pool.acquire() as conn:
        rows = await fo._find_upload_configs(
            conn, session_type=session_type, source=source, tenant_code=tenant_code, program_id=program_id,
            program_name=program_name)
    if len(rows) != 1:
        return None
    new_config = {k: v for k, v in fo._as_dict(rows[0]["config"]).items() if not (unset and k == key)}
    if not unset:
        new_config[key] = value
    return adapter_config_problem(new_config)
