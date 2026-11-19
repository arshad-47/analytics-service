"""
Database operations of the `feedback` upload: lookups (program/leader resolution, the active upload config,
the stored state used by the delta check) and the management of questionnaire versions (set / list / relabel /
detect a new version from the questions the stored sessions were asked).

`feedback` is a generic report type: many programs, many sources. The key of everything here is
(tenant, program, session_type, source): a session_type (the persona key) is NOT unique across the programs of a
tenant and a source is a free label. ONE RULE finds the config of a session or an upload (the same one the extraction
uses, app/database/feedback_extraction_operations.py): the config of the session's OWN program wins; when the program
has none, the tenant's PROGRAM-LESS default config (program_id NULL) for that source and persona is used; a config of
ANOTHER program is never used. When more than one candidate is left at the winning level the lookup raises
AmbiguousFeedbackConfigError instead of guessing. The tools that EDIT a config (set / migrate / the version `set`) stay
strictly exact: they act on the one config row they were told and never on a fallback. The lookups return None for a
miss (never fabricated); the tools accept --tenant-code / --program-id / --program-name / --source to say which one is
meant and refuse otherwise.

Unlike the story/discussion lookup in app/api/services/uploads.py, both the leader category and the
program are matched by NAME AND TENANT in ONE join (get_program_and_leader).

The feedback settings that differ per bot (adapter, max_csv_upload_bytes, ingestion_schema, process_steps, optional
expect_stages) live in the persona's active upload config; this module reads them (get_feedback_process_steps,
get_process_steps) and lists / migrates / changes them (list_feedback_config, migrate_feedback_config,
set_feedback_config_value, each change a NEW config_version, the old one kept as history).

Config changes reach the Kafka consumers through a 60 s cache (app/services/feedback_config_cache.py): after `set`
an event can still be validated and stamped under the old config for up to 60 seconds.
"""
import copy
import json
import logging
import shlex
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from app.config import settings
from app.database.db import db
from app.database.feedback_bot_versions import BOT_SESSION_SQL, apply_bot_version_ranks, lock_personas
from app.database.operations import update_submission_status
from app.services.feedback_adapter_config import (
    CONFIG_KEY_COLUMN_MAP,
    CONFIG_KEY_COMPLETION_MAP,
    validate_adapter_config,
    wide_form_identity_problem,
)
from app.services.feedback_config_validation import (
    SETTABLE_CONFIG_KEYS,
    config_setting_problems,
    validate_config_value,
    validate_process_steps,
)
from app.services.feedback_constants import (
    ADAPTER_CONVERSATION_ROWS,
    ADAPTER_WIDE_FORM,
    CONFIG_KEY_ADAPTER,
    CONFIG_KEY_EXPECT_STAGES,
    CONFIG_KEY_PROCESS_STEPS,
    CONFIG_TYPE_FEEDBACK_UPLOAD,
    FEEDBACK_REPORT_TYPE,
    LEGACY_ADAPTER_BY_CONFIG_SOURCE,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
    feedback_config_key_defaults,
)

logger = logging.getLogger("analytics_service.database.feedback_operations")

# Ids per ANY($n::text[]) query; keeps a very large upload's lookup to a few round trips.
_LOOKUP_CHUNK_SIZE = 5000

# The config key the questionnaire versions live under (a source whose config has it is versioned).
_VERSIONS_KEY = "questionnaire_versions"


class AmbiguousFeedbackConfigError(ValueError):
    """More than one row matches a lookup that must name exactly one (a program, or a persona's upload config).
    The message lists the identifiers of the candidates (never content) and says what to pass to pick one."""


def _as_dict(value: Any) -> Dict[str, Any]:
    """asyncpg returns JSONB as str unless a codec is registered; accept both."""
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError(f"config_version.config must be a JSON object, got {type(value).__name__}")
    return value


def _selector(name: str, value: Optional[str]) -> Optional[str]:
    """A tool selector (tenant, source, session_type, program name): None = not given; otherwise a non-empty string.
    A blank value is an error, never read as "not given" (that would widen the lookup silently)."""
    if value is None:
        return None
    if not isinstance(value, str) or value.strip() == "":
        raise ValueError(f"{name} must be a non-empty string when given")
    return value


def _program_id_selector(program_id: Optional[str]) -> Optional[str]:
    if program_id is None:
        return None
    try:
        return str(uuid.UUID(str(program_id)))
    except ValueError:
        raise ValueError("program_id must be a UUID when given")


def _config_label(row: Mapping[str, Any]) -> str:
    """tenant/program/session_type/source of a config row: identifiers only. The program shows as `name (id)`: two
    programs of one tenant can share a name, so the name alone would give both configs the same label (and the same
    result key); a program-less default shows as `-`."""
    if row["program_id"] is None:
        program = "-"
    elif row["program_name"] is None:
        program = str(row["program_id"])
    else:
        program = f"{row['program_name']} ({row['program_id']})"
    return f"{row['tenant_code']}/{program}/{row['session_type']}/{row['config_source']}"


def _result_keys(rows: List[Mapping[str, Any]]) -> Dict[Any, str]:
    """Result key of each config row: its session_type when that is unique in the result (the usual case), else
    its full label tenant/program/session_type/source, so that two programs never share one key."""
    counts: Dict[str, int] = {}
    for row in rows:
        counts[row["session_type"]] = counts.get(row["session_type"], 0) + 1
    return {row["config_id"]: (row["session_type"] if counts[row["session_type"]] == 1 else _config_label(row))
            for row in rows}


def _identity(row: Mapping[str, Any]) -> Dict[str, Any]:
    """The key of a config row as returned to the tools."""
    return {
        "tenant_code": row["tenant_code"],
        "program_id": None if row["program_id"] is None else str(row["program_id"]),
        "program_name": row["program_name"],
        "session_type": row["session_type"],
        "source": row["config_source"],
    }


async def _find_upload_configs(
    conn,
    *,
    session_type: Optional[str] = None,
    source: Optional[str] = None,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
    versioned_only: bool = False,
    require_active: bool = True,
) -> List[Mapping[str, Any]]:
    """
    The feedback_upload configs that match every selector given (None = not narrowed), with their ACTIVE version
    ({"config_id", "tenant_code", "program_id", "program_name", "session_type", "config_source", "version_id",
    "version", "config"}; the version columns are None for a config without an active version unless
    `require_active`). `versioned_only` keeps the configs whose active version carries `questionnaire_versions`
    (the sources that have questionnaire versions). Ordered by tenant, program, session_type, source.
    """
    return await conn.fetch(
        """
        SELECT c.id AS config_id, c.tenant_code, c.program_id, p.name AS program_name, c.session_type,
               c.config_source, v.id AS version_id, v.version, v.config
        FROM config c
        LEFT JOIN programs p ON p.id = c.program_id
        LEFT JOIN config_version v ON v.config_id = c.id AND v.is_active
        WHERE c.config_type = $1
          AND ($2::text IS NULL OR c.session_type = $2)
          AND ($3::text IS NULL OR c.config_source = $3)
          AND ($4::text IS NULL OR c.tenant_code = $4)
          AND ($5::uuid IS NULL OR c.program_id = $5::uuid)
          AND ($6::text IS NULL OR p.name = $6)
          AND (NOT $7::boolean OR (v.id IS NOT NULL
               AND (jsonb_exists(v.config, $8) OR v.config ->> $10 = $11)))
          AND (NOT $9::boolean OR v.id IS NOT NULL)
        ORDER BY c.tenant_code, p.name NULLS FIRST, c.session_type, c.program_id NULLS FIRST, c.config_source, c.id
        """,
        CONFIG_TYPE_FEEDBACK_UPLOAD,
        _selector("session_type", session_type),
        _selector("source", source),
        _selector("tenant_code", tenant_code),
        _program_id_selector(program_id),
        _selector("program_name", program_name),
        versioned_only,
        _VERSIONS_KEY,
        require_active,
        CONFIG_KEY_ADAPTER,
        ADAPTER_CONVERSATION_ROWS,
    )


def _exactly_one(rows: List[Mapping[str, Any]], none_message: str, noun: str, hint: str) -> Mapping[str, Any]:
    """The only row of `rows`; ValueError(none_message) when there is none, AmbiguousFeedbackConfigError listing the
    candidates' identifiers (`noun` = what they are) when there are several."""
    if len(rows) == 0:
        raise ValueError(none_message)
    if len(rows) > 1:
        shown = ", ".join(_config_label(r) for r in rows[:5]) + (", ..." if len(rows) > 5 else "")
        raise AmbiguousFeedbackConfigError(f"{len(rows)} {noun} match ({shown}); {hint}")
    return rows[0]


_PICK_HINT = "pass --tenant-code, --program-id (or --program-name) and --source to pick one"


async def get_program_and_leader(
    program_name: str,
    leader_category: str,
    tenant_code: str,
) -> Optional[Dict[str, Any]]:
    """
    Resolve a feedback upload's program + persona (leader category), by name and tenant, in ONE join: the program
    named `program_name` whose leader category is named `leader_category`, both in `tenant_code`. A leader category
    of the same name that owns no such program (another program's persona, a duplicate created by an event) is
    never in the way, whatever order the rows are stored in.

    Returns {"program": {"id", "name"}, "leader": {"id", "name"}, "tenant_code"} or None when no program matches.
    Raises AmbiguousFeedbackConfigError when more than one program row matches (two programs of one name under
    leader categories of one name): the caller must not guess.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id AS program_id, p.name AS program_name, l.id AS leader_id, l.name AS leader_name
            FROM programs p
            JOIN leader_category l ON l.id = p.leaders_id AND l.tenant_code = p.tenant_code
            WHERE p.name = $1 AND l.name = $2 AND p.tenant_code = $3
            ORDER BY p.created_at, p.id
            LIMIT 2
            """,
            program_name,
            leader_category,
            tenant_code,
        )
    if len(rows) == 0:
        return None
    if len(rows) > 1:
        raise AmbiguousFeedbackConfigError(
            f"more than one program named {program_name!r} with a leader category named {leader_category!r} exists "
            f"in tenant {tenant_code!r}; the program cannot be resolved by name"
        )
    row = rows[0]
    return {
        "program": {"id": str(row["program_id"]), "name": row["program_name"]},
        "leader": {"id": str(row["leader_id"]), "name": row["leader_name"]},
        "tenant_code": tenant_code,
    }


async def get_active_feedback_config(
    program_id: str,
    config_source: str,
    *,
    tenant_code: Optional[str] = None,
    session_type: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Load the ACTIVE config_version of the feedback_upload `config` row that serves `program_id` for `config_source`
    (the source is always named: it is a free label, there is no default). tenant_code / session_type narrow the
    lookup when given (the Kafka consumer knows both from the event).

    THE RULE (shared with the extraction and the process steps): the config of the program's OWN row wins; when the
    program has none, the tenant's PROGRAM-LESS default config (program_id NULL, same tenant as the program) for that
    source (and session_type when given) is used; a config of ANOTHER program is never used. A default of another
    tenant never applies: the tenant is the program's tenant (and must equal `tenant_code` when that is given).

    A program is normally one persona with one session_type, but the schema allows several (one config per
    session_type), and a tenant can hold the defaults of several personas: when `session_type` is not given and more
    than one config is left at the winning level, this raises AmbiguousFeedbackConfigError instead of returning an
    arbitrary one.

    Returns {"config_id", "config_version_id", "version", "session_type", "program_id", "config": {...}} (ids as
    str; `program_id` is None for the program-less default) or None when no such config / no active version exists.
    """
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        # Own configs first, then the defaults of the program's tenant. At most two rows are needed to tell whether
        # the winning level is unique: the first row says which level wins, a second row of the same level is the
        # ambiguity.
        rows = await conn.fetch(
            """
            SELECT c.id AS config_id, c.session_type, c.program_id, v.id AS config_version_id, v.version, v.config
            FROM config c
            JOIN config_version v ON v.config_id = c.id AND v.is_active
            WHERE c.config_source = $1
              AND c.config_type = $2
              AND ($4::text IS NULL OR c.tenant_code = $4)
              AND ($5::text IS NULL OR c.session_type = $5)
              AND (c.program_id = $3::uuid
                   OR (c.program_id IS NULL
                       AND c.tenant_code = (SELECT p.tenant_code FROM programs p WHERE p.id = $3::uuid)))
            ORDER BY (c.program_id IS NOT NULL) DESC, c.session_type, c.id
            LIMIT 2
            """,
            config_source,
            CONFIG_TYPE_FEEDBACK_UPLOAD,
            program_id,
            tenant_code,
            session_type,
        )
    if len(rows) == 0:
        return None
    if len(rows) > 1 and (rows[0]["program_id"] is None) == (rows[1]["program_id"] is None):
        level = "for program " + str(program_id) if rows[0]["program_id"] is not None else (
            f"as program-less default for the tenant of program {program_id}")
        raise AmbiguousFeedbackConfigError(
            f"more than one active {config_source} {CONFIG_TYPE_FEEDBACK_UPLOAD} config exists {level} "
            f"({', '.join(sorted(r['session_type'] for r in rows))}); pass session_type to pick one"
        )
    row = rows[0]
    return {
        "config_id": str(row["config_id"]),
        "config_version_id": str(row["config_version_id"]),
        "version": row["version"],
        "session_type": row["session_type"],
        "program_id": None if row["program_id"] is None else str(row["program_id"]),
        "config": _as_dict(row["config"]),
    }


async def _persona_process_steps(conn, source: str, session_type: str, tenant_code: str, program_id) -> List[Dict[str, Any]]:
    """The validated process_steps of the ACTIVE feedback_upload config of one persona, found by THE RULE (see
    get_active_feedback_config): the config of exactly (source, session_type, tenant, program) of the submission, else
    the tenant's program-less default for (source, session_type). Another program's config is never used (a
    session_type is not unique across programs, so that would run another program's steps); a submission without a
    program only matches a program-less config. Raises ValueError when there is no active config or its
    process_steps is missing / invalid. The message carries identifiers only, no submission content."""
    row = await conn.fetchrow(
        """
        SELECT v.version, v.config
        FROM config c
        JOIN config_version v ON v.config_id = c.id AND v.is_active
        WHERE c.config_type = $1 AND c.config_source = $2 AND c.session_type = $3 AND c.tenant_code = $4
          AND (c.program_id = $5::uuid OR c.program_id IS NULL)
        ORDER BY (c.program_id IS NOT NULL) DESC, c.id
        LIMIT 1
        """,
        CONFIG_TYPE_FEEDBACK_UPLOAD, source, session_type, tenant_code, program_id,
    )
    if row is None:
        raise ValueError(
            f"No active {source} feedback_upload config for session_type {session_type!r}, program {program_id} "
            f"(tenant {tenant_code!r}) and no program-less default; cannot resolve the process steps"
        )
    config = _as_dict(row["config"])
    if CONFIG_KEY_PROCESS_STEPS not in config:
        raise ValueError(
            f"Feedback config of session_type {session_type!r} ({source}) is missing "
            f"'{CONFIG_KEY_PROCESS_STEPS}' (add it with: python tools/feedback_config.py migrate; or migrate-legacy for a database created before session_type existed)"
        )
    return validate_process_steps(config[CONFIG_KEY_PROCESS_STEPS])


async def get_feedback_process_steps(
    conn, submission_id: str, tenant_code: str, memo: Optional[Dict[Any, Any]] = None
) -> List[Dict[str, Any]]:
    """
    The process steps of a stored feedback submission: the submission's persona (feedback_submissions.session_type
    + source) selects its feedback_upload config (config_source = the submission's source, same tenant) and the
    ACTIVE config_version's `process_steps` is returned, validated (a list of {"name", optional "columns",
    "llm_model", "max_tokens", "llm_timeout_seconds"}). An empty list is valid: the submission is then marked
    success at ingestion. Raises ValueError when the submission is not a stored feedback submission, its persona
    has no active upload config, or the config lacks / has invalid process_steps.

    The config is found by THE RULE of get_active_feedback_config: the one of the submission's own (tenant, program,
    session_type, source), else the tenant's program-less default for (session_type, source); another program's
    config is never used. `memo` (a dict the caller owns for ONE sweep) remembers the persona lookup keyed by
    (session_type, source, tenant, program), errors included, so a sweep over many submissions of the same persona
    reads the config once.
    """
    sub = await conn.fetchrow(
        """
        SELECT fs.session_type, fs.source, s.program_id
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.submission_id = $1 AND fs.tenant_code = $2
        """,
        submission_id, tenant_code,
    )
    if sub is None:
        raise ValueError(f"No stored feedback submission {submission_id!r} for tenant {tenant_code!r}")
    key = (sub["session_type"], sub["source"], tenant_code, str(sub["program_id"]) if sub["program_id"] is not None else None)
    if memo is not None and key in memo:
        steps, error = memo[key]
        if error is not None:
            raise ValueError(f"{error} (submission {submission_id!r})")
        return copy.deepcopy(steps)
    try:
        steps = await _persona_process_steps(conn, sub["source"], sub["session_type"], tenant_code, sub["program_id"])
    except ValueError as exc:
        if memo is not None:
            memo[key] = (None, str(exc))
        raise ValueError(f"{exc} (submission {submission_id!r})") from exc
    if memo is not None:
        memo[key] = (steps, None)
    return copy.deepcopy(steps)


async def get_process_steps(
    conn, submission_type: str, submission_id: str, tenant_code: str, memo: Optional[Dict[Any, Any]] = None
) -> List[Dict[str, Any]]:
    """
    The process steps of a submission: feedback reads its persona's upload config (get_feedback_process_steps),
    story / discussion keep reading settings (PROCESS_CONFIG_STORY / PROCESS_CONFIG_DISCUSSION); an unknown type
    has none. `memo` is passed through (see get_feedback_process_steps).
    """
    if isinstance(submission_type, str) and FEEDBACK_REPORT_TYPE in submission_type.lower().strip():
        return await get_feedback_process_steps(conn, submission_id, tenant_code, memo)
    return settings.get_process_config(submission_type)


async def mark_submission_config_failed(conn, submission_id: str, tenant_code: str, reason: str) -> None:
    """
    A feedback submission whose persona config cannot give its process steps is marked 'failed' with the reason in
    its process_status (no retry_attempt is recorded, so the bulk retry picks it up once the config is fixed)
    instead of staying 'pending': a pending row that can never be resolved would keep returning at the head of
    every batch sweep and starve the other submissions. Identifiers and the config problem only, no content.
    """
    await update_submission_status(
        conn, submission_id, tenant_code, "failed",
        {
            "status": "failed",
            "error_message": f"Cannot resolve the process steps of the persona config: {reason}",
            "steps": [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "failed_at_step": None,
        },
    )


async def get_known_source_refs(tenant_code: str, refs: List[str]) -> set:
    """
    Which of `refs` (Google Forms natural keys) are already stored for the tenant: the subset of
    feedback_submissions.source_ref values. One query per chunk of _LOOKUP_CHUNK_SIZE refs.
    """
    unique_refs = sorted(set(refs))
    if len(unique_refs) == 0:
        return set()
    if not db.pool:
        await db.connect()

    known: set = set()
    async with db.pool.acquire() as conn:
        for start in range(0, len(unique_refs), _LOOKUP_CHUNK_SIZE):
            rows = await conn.fetch(
                """
                SELECT source_ref
                FROM feedback_submissions
                WHERE tenant_code = $1 AND source_ref = ANY($2::text[])
                """,
                tenant_code,
                unique_refs[start:start + _LOOKUP_CHUNK_SIZE],
            )
            known.update(row["source_ref"] for row in rows)
    return known


async def get_mitra_session_state(tenant_code: str, submission_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Stored state of the Mitra sessions with these submission ids, for the delta check:
    {submission_id: {"session_id", "max_sequence", "message_count", "completion_status"}}.
    Sessions that are not stored are absent. A stored session with no feedbacks rows reports
    message_count 0 and max_sequence 0.
    """
    unique_ids = sorted(set(submission_ids))
    if len(unique_ids) == 0:
        return {}
    if not db.pool:
        await db.connect()

    state: Dict[str, Dict[str, Any]] = {}
    async with db.pool.acquire() as conn:
        for start in range(0, len(unique_ids), _LOOKUP_CHUNK_SIZE):
            rows = await conn.fetch(
                """
                SELECT fs.submission_id,
                       s.session_id,
                       fs.completion_status,
                       COUNT(f.id) AS message_count,
                       COALESCE(MAX(f.sequence), 0) AS max_sequence
                FROM feedback_submissions fs
                JOIN submissions s
                  ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                LEFT JOIN feedbacks f
                  ON f.submission_id = fs.submission_id AND f.tenant_code = fs.tenant_code
                WHERE fs.tenant_code = $1 AND fs.submission_id = ANY($2::text[])
                GROUP BY fs.submission_id, s.session_id, fs.completion_status
                """,
                tenant_code,
                unique_ids[start:start + _LOOKUP_CHUNK_SIZE],
            )
            for row in rows:
                state[row["submission_id"]] = {
                    "session_id": row["session_id"],
                    "max_sequence": int(row["max_sequence"]),
                    "message_count": int(row["message_count"]),
                    "completion_status": row["completion_status"],
                }
    return state


class _DryRunRollback(Exception):
    """Raised inside a transaction to roll it back after a dry run computed its result."""


def _note(change_note: Optional[str], default: str) -> str:
    """The caller's change note, or `default` when none was given (None or blank)."""
    if change_note is None or change_note.strip() == "":
        return default
    return change_note


def _sessions_of_config_sql(config_program: str, config_type: str) -> str:
    """
    SQL condition (over `fs` = feedback_submissions and `s` = submissions) that selects the stored sessions a config
    ROW governs, by THE RULE (the one get_active_feedback_config applies): the sessions of exactly its program; and,
    for a PROGRAM-LESS default config (`config_program` is NULL), also the sessions of every program that has no
    config of its own for the session's (tenant, session_type, source). "Has a config of its own" means a config row
    WITH AN ACTIVE VERSION: a program whose own row has no active version is served by the default at ingestion, so
    its sessions belong to the default here too. A session whose program has an active config of its own (even one
    without questionnaire windows) is never governed by the default. `config_program` / `config_type` are the
    placeholders ("$4::uuid", "$6") of the caller's query.
    """
    return (
        f"(s.program_id IS NOT DISTINCT FROM {config_program} OR ({config_program} IS NULL AND NOT EXISTS ("
        f"SELECT 1 FROM config own JOIN config_version own_v ON own_v.config_id = own.id AND own_v.is_active "
        f"WHERE own.config_type = {config_type} AND own.tenant_code = fs.tenant_code "
        f"AND own.session_type = fs.session_type AND own.config_source = fs.source AND own.program_id = s.program_id)))"
    )


async def _restamp_sessions(conn, cfg: Mapping[str, Any], versions) -> Dict[str, Any]:
    """
    Relabel the stored sessions of ONE config (`cfg`: a row of _find_upload_configs) from the given version windows
    (the list parsed by parse_versions) using each session's start (submissions.submission_date, its first real
    message). A Mitra bot session (one with a bot id) is NEVER relabelled from a date: its label is the rank of its bot
    id (_restamp_bot_sessions). Only the sessions the config governs (see _sessions_of_config_sql) are read or written: those of
    exactly the config's (tenant, program, session_type, source); a config without a program (the tenant's default)
    also governs the sessions of the programs that have no config of their own for that (session_type, source), and
    never those of a program that has one. Writes through `conn` (the caller owns the transaction). Returns
    {"sessions", "changed", "versions"}.
    """
    from app.services.feedback_versions import stamp_version

    rows = await conn.fetch(
        f"""
        SELECT fs.submission_id, fs.tenant_code, fs.questionnaire_version, s.submission_date
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.source = $1 AND fs.session_type = $2 AND fs.tenant_code = $3
          AND {_sessions_of_config_sql("$4::uuid", "$5")} AND s.submission_date IS NOT NULL
          AND NOT {BOT_SESSION_SQL}
        """,
        cfg["config_source"], cfg["session_type"], cfg["tenant_code"], cfg["program_id"], CONFIG_TYPE_FEEDBACK_UPLOAD,
    )
    counts: Dict[str, int] = {}
    changed_ids: List[str] = []
    changed_versions: List[str] = []
    for row in rows:
        key = stamp_version(versions, row["submission_date"])
        counts[key] = counts.get(key, 0) + 1
        if row["questionnaire_version"] != key:
            changed_ids.append(row["submission_id"])
            changed_versions.append(key)
    if len(changed_ids) > 0:
        await conn.execute(
            """
            UPDATE feedback_submissions fs
            SET questionnaire_version = t.version, updated_at = now()
            FROM unnest($1::text[], $2::text[]) AS t(submission_id, version)
            WHERE fs.submission_id = t.submission_id AND fs.tenant_code = $3
              AND fs.source = $4 AND fs.session_type = $5
            """,
            changed_ids, changed_versions, cfg["tenant_code"], cfg["config_source"], cfg["session_type"],
        )
    return {"sessions": len(rows), "changed": len(changed_ids), "versions": counts}


async def _governed_bot_rows(conn, cfg: Mapping[str, Any]):
    """program_id, bot_id, questionnaire_version and the session count of the bot sessions ONE config governs."""
    return await conn.fetch(
        f"""
        SELECT s.program_id::text AS program_id, fs.meta_data ->> 'bot_id' AS bot_id, fs.questionnaire_version,
               count(*) AS n
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.source = $1 AND fs.session_type = $2 AND fs.tenant_code = $3
          AND {_sessions_of_config_sql("$4::uuid", "$5")} AND {BOT_SESSION_SQL}
        GROUP BY 1, 2, 3
        ORDER BY 1 NULLS FIRST, 2, 3
        """,
        cfg["config_source"], cfg["session_type"], cfg["tenant_code"], cfg["program_id"], CONFIG_TYPE_FEEDBACK_UPLOAD,
    )


async def _bot_rank_overview(conn, cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """
    Read-only. For each program among the bot sessions ONE config governs (a persona = tenant, program, session_type,
    source): the bot-id-based label next to the stamped one:
    {"program_id", "bots": {bot_id: {"label": rank label, "sessions": n, "stamped": {stored label: n}}},
     "sessions_to_relabel": n}. Bots are listed in numeric order. [] when the config governs no bot session.
    """
    from app.services.feedback_versions import rank_bot_versions

    per_program: Dict[Optional[str], List[Mapping[str, Any]]] = {}
    for row in await _governed_bot_rows(conn, cfg):
        per_program.setdefault(row["program_id"], []).append(row)
    overview: List[Dict[str, Any]] = []
    for program, rows in per_program.items():
        ranks = rank_bot_versions(row["bot_id"] for row in rows)
        bots: Dict[str, Dict[str, Any]] = {}
        stale = 0
        for bot_id in sorted(ranks, key=int):
            mine = [row for row in rows if row["bot_id"] == bot_id]
            bots[bot_id] = {
                "label": ranks[bot_id],
                "sessions": sum(int(row["n"]) for row in mine),
                "stamped": {row["questionnaire_version"]: int(row["n"]) for row in mine},
            }
            stale += sum(int(row["n"]) for row in mine if row["questionnaire_version"] != ranks[bot_id])
        overview.append({"program_id": program, "bots": bots, "sessions_to_relabel": stale})
    return overview


async def _bot_personas(conn, cfg: Mapping[str, Any]) -> List[tuple]:
    """The personas (tenant, program, session_type, source) among the bot sessions ONE config governs."""
    programs = sorted({row["program_id"] for row in await _governed_bot_rows(conn, cfg)}, key=lambda p: (p is not None, p))
    return [(cfg["tenant_code"], program, cfg["session_type"], cfg["config_source"]) for program in programs]


async def _restamp_bot_sessions(conn, cfg: Mapping[str, Any], personas: List[Any]) -> Dict[str, Any]:
    """
    Recompute the bot-rank labels of every persona (program) among the bot sessions ONE config governs and relabel
    what differs (apply_bot_version_ranks: a relabelled session whose asked fields do not change only gets the new
    label, one whose fields change is sent back through processing). This is the manual repair of the ingest rule.
    Writes through `conn` (the caller owns the transaction). `personas` is the set the caller read AND locked (in one
    fixed order, with every other config's personas): this function takes no lock of its own, because a persona found
    by a second read (one committed meanwhile) could be locked out of that order and deadlock with an ingest.
    Returns {"personas": n, "sessions": n, "changed": n (every relabelled session), "label_only": n (relabelled
    without reprocessing), "reprocess": [submission_id] (set back to 'pending'; real-time mode does not start them
    by itself here, batch mode or the retry sweep picks them up)}.
    """
    sessions = 0
    relabelled = 0
    reprocess: List[str] = []
    label_only = 0
    for persona in personas:
        applied = await apply_bot_version_ranks(conn, *persona)
        sessions += applied["sessions"]
        relabelled += len(applied["restamped"])
        reprocess.extend(applied["reset"])
        label_only += len(applied["label_only"])
    return {"personas": len(personas), "sessions": sessions, "changed": relabelled, "label_only": label_only,
            "reprocess": reprocess}


async def restamp_questionnaire_versions(
    session_type: Optional[str] = None,
    dry_run: bool = False,
    *,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Relabel the questionnaire_version of the stored sessions from the ACTIVE upload config of their persona
    (config.questionnaire_versions), for every source whose config has questionnaire versions (a config without
    them is not touched; window labels only for sessions WITHOUT a bot id). Needed after anything that changes the windows outside set_questionnaire_version (that one
    relabels by itself), because re-uploading an unchanged session is a no-op and never refreshes the label. Each
    config relabels only the sessions it governs: those of exactly its (tenant, program, session_type, source); a
    program-less default config also those of the programs that have no config of their own for that (session_type,
    source), never those of a program that has one.

    The selectors (tenant_code, program_id / program_name, source) narrow the configs. With NO session_type this
    is the explicit sweep over every matching config; WITH a session_type it must identify exactly one config
    (AmbiguousFeedbackConfigError, listing the candidates, when several programs / tenants / sources share it).

    Mitra bot sessions (a bot id stored) are NOT relabelled from the windows: their label is the rank of their bot id
    inside the persona (lowest bot id = V1, see app/database/feedback_bot_versions.py). This function recomputes those
    labels too (it is the manual repair of the ingest rule): a session whose label changes AND whose active extraction
    config asks different fields for the old and the new label is sent back through processing (extraction deleted,
    PII flag reset, status 'pending'); one that asks the same fields only gets the new label.

    Returns {"session_types": {key: {"sessions", "changed", "versions": {label: n}, "bot_sessions": {"personas",
    "sessions", "changed", "label_only", "reprocess": [submission_id]}, tenant_code, program_id, program_name, session_type,
    source}}, "changed": total (window and bot relabels), "dry_run": bool}; `key` is the session_type, or
    tenant/program/session_type/source when the session_type alone would collide. With dry_run nothing is written.
    One transaction. Raises ValueError when an active config has an invalid questionnaire_versions list.
    """
    from app.services.feedback_versions import parse_versions

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {"session_types": {}, "changed": 0, "dry_run": dry_run}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                configs = await _find_upload_configs(
                    conn, session_type=session_type, source=source, tenant_code=tenant_code, program_id=program_id,
                    program_name=program_name, versioned_only=True)
                if session_type is not None and len(configs) > 1:
                    _exactly_one(configs, "", "versioned upload configs", _PICK_HINT)
                keys = _result_keys(configs)
                # Every persona of every config, locked once in ONE key order before anything else: configs are visited by
                # program name but personas are locked by key, so locking them config by config could take two personas in
                # opposite orders from a concurrent ingest that moves a session between them (a deadlock).
                personas_of: Dict[Any, List[Any]] = {}
                for cfg in configs:
                    personas_of[cfg["config_id"]] = await _bot_personas(conn, cfg)
                await lock_personas(conn, [p for cfg in configs for p in personas_of[cfg["config_id"]]])
                for cfg in configs:
                    # Lock the config row, THEN read its active windows, and keep the lock until the commit: the
                    # windows read by _find_upload_configs above can be older than a set_questionnaire_version that
                    # commits (new windows + its own relabelling) before this transaction reaches the sessions; a
                    # restamp from those old windows would then write the old labels over the new ones.
                    active = await _lock_and_read_active(conn, cfg["config_id"])
                    if active is None:
                        continue  # its active version went away since the listing: nothing to restamp from
                    versions = parse_versions(active["config"])
                    summary = await _restamp_sessions(conn, cfg, versions)
                    # bot sessions follow their bot ids, not the windows: recompute those labels too (the repair)
                    bot_summary = await _restamp_bot_sessions(conn, cfg, personas_of[cfg["config_id"]])
                    result["session_types"][keys[cfg["config_id"]]] = {
                        **summary, "bot_sessions": bot_summary, **_identity(cfg)}
                    result["changed"] += summary["changed"] + bot_summary["changed"]
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result


async def list_questionnaire_versions(
    session_type: Optional[str] = None,
    *,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read-only. The recorded questionnaire versions of each persona's active upload config (every source whose config
    has questionnaire versions), with the number of stored sessions per label, counted over the sessions the config
    governs (its tenant, program, session_type, source; for a program-less default also the sessions of the programs
    that have no config of their own for it): {key: {"config_version": n, "versions": [{"key", "effective_from"}],
    "sessions": {label: n}, "bot_versions": [...], tenant_code, program_id, program_name, session_type, source}}.
    `bot_versions` is the bot-id-based label of each bot per program next to the stamped labels (the version of a
    bot session follows the order of the bot ids, not the windows). `key` is the session_type,
    or tenant/program/session_type/source when the session_type alone would collide.
    """
    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {}
    async with db.pool.acquire() as conn:
        configs = await _find_upload_configs(
            conn, session_type=session_type, source=source, tenant_code=tenant_code, program_id=program_id,
            program_name=program_name, versioned_only=True)
        keys = _result_keys(configs)
        for cfg in configs:
            counts = await conn.fetch(
                f"""
                SELECT fs.questionnaire_version, count(*) AS n
                FROM feedback_submissions fs
                JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                WHERE fs.source = $1 AND fs.session_type = $2 AND fs.tenant_code = $3
                  AND {_sessions_of_config_sql("$4::uuid", "$5")}
                GROUP BY 1
                """,
                cfg["config_source"], cfg["session_type"], cfg["tenant_code"], cfg["program_id"],
                CONFIG_TYPE_FEEDBACK_UPLOAD,
            )
            result[keys[cfg["config_id"]]] = {
                "config_version": cfg["version"],
                "versions": _as_dict(cfg["config"]).get(_VERSIONS_KEY),
                "sessions": {row["questionnaire_version"]: int(row["n"]) for row in counts},
                # the bot-id-based label of each bot next to what is stamped (see _bot_rank_overview)
                "bot_versions": await _bot_rank_overview(conn, cfg),
                **_identity(cfg),
            }
    return result


async def _fetch_session_questions(conn, cfg: Mapping[str, Any], closing_keys: List[str]):
    """The main (not probe, not closing) bot questions of every stored session ONE config governs (exactly its
    tenant, program, session_type and source; for a program-less default also the sessions of the programs without a
    config of their own, see _sessions_of_config_sql)."""
    from app.services.feedback_version_detect import SessionQuestions

    rows = await conn.fetch(
        f"""
        SELECT fb.submission_id, s.submission_date, fs.questionnaire_version, fb.message, fb.translated_message,
               fs.meta_data ->> 'bot_id' AS bot_id
        FROM feedbacks fb
        JOIN feedback_submissions fs ON fs.submission_id = fb.submission_id AND fs.tenant_code = fb.tenant_code
        JOIN submissions s ON s.submission_id = fb.submission_id AND s.tenant_code = fb.tenant_code
        WHERE fs.source = $1 AND fs.session_type = $2 AND fs.tenant_code = $3
          AND {_sessions_of_config_sql("$5::uuid", "$6")}
          AND s.submission_date IS NOT NULL
          AND fb.parent_id IS NULL AND NOT fb.is_follow_up
          AND (fb.question_key IS NULL OR NOT (fb.question_key = ANY($4::text[])))
        ORDER BY fb.submission_id, fb.sequence
        """,
        cfg["config_source"], cfg["session_type"], cfg["tenant_code"], closing_keys, cfg["program_id"],
        CONFIG_TYPE_FEEDBACK_UPLOAD,
    )
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        english = row["translated_message"]
        text = row["message"] if english is None or english.strip() == "" else english
        entry = grouped.setdefault(row["submission_id"], {
            "start": row["submission_date"], "stamped": row["questionnaire_version"], "texts": [],
            "bot_id": row["bot_id"]})
        entry["texts"].append(text)
    return [SessionQuestions(sid, e["start"], e["stamped"], tuple(e["texts"]), e["bot_id"]) for sid, e in grouped.items()]


def _selector_flags(cfg: Mapping[str, Any]) -> str:
    """The command line flags that name exactly one config (for the `next_step` hint of detect)."""
    flags = f"--session-type {shlex.quote(cfg['session_type'])} --tenant-code {shlex.quote(cfg['tenant_code'])}"
    if cfg["program_id"] is not None:
        flags += f" --program-id {cfg['program_id']}"
    return f"{flags} --source {shlex.quote(cfg['config_source'])}"


async def detect_questionnaire_versions(
    session_type: Optional[str] = None,
    *,
    min_sessions: int = 20,
    baseline_sessions: int = 100,
    min_coverage: float = 0.3,
    include_wording: bool = False,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read-only. For each persona's active upload config (every source whose config has questionnaire versions),
    compare the main questions the stored sessions that config governs (its tenant, program, session_type, source;
    a program-less default also governs the programs that have no config of their own) were asked with what each recorded version is known to ask (app/services/feedback_version_detect.py) and
    propose a new version and its start when a new question set appeared. Nothing is written: a person records the
    proposal with set_questionnaire_version. Returns {key: detect() result + "next_step" + the config's identity};
    `key` is the session_type, or tenant/program/session_type/source when the session_type alone would collide.
    """
    from app.services.feedback_version_detect import detect
    from app.services.feedback_versions import parse_versions

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {}
    async with db.pool.acquire() as conn:
        configs = await _find_upload_configs(
            conn, session_type=session_type, source=source, tenant_code=tenant_code, program_id=program_id,
            program_name=program_name, versioned_only=True)
        keys = _result_keys(configs)
        for cfg in configs:
            config = _as_dict(cfg["config"])
            version_keys = [key for key, _ in parse_versions(config)]
            closing = config.get("closing_keys")
            sessions = await _fetch_session_questions(conn, cfg, closing if isinstance(closing, list) else [])
            found = detect(sessions, version_keys, min_sessions=min_sessions, baseline_sessions=baseline_sessions,
                           min_coverage=min_coverage, include_wording=include_wording)
            proposal = found["proposal"]
            found["next_step"] = None if proposal is None else (
                f"python tools/questionnaire_versions.py set {_selector_flags(cfg)} "
                f"--key {shlex.quote(proposal['key'])} --effective-from {proposal['effective_from']} --dry-run")
            # Read-only cross-check: the bot-id-based label of each bot next to the stamped one above
            found["bot_versions"] = await _bot_rank_overview(conn, cfg)
            found.update(_identity(cfg))
            result[keys[cfg["config_id"]]] = found
    return result


async def set_questionnaire_version(
    session_type: str,
    key: str,
    effective_from: str,
    *,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
    source: Optional[str] = None,
    created_by: str = "cli",
    change_note: Optional[str] = None,
    restamp: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Record that questionnaire version `key` of a persona's bot is in force from `effective_from` (an ISO instant
    with a UTC offset, for example 2026-12-01T10:30:00+05:30). A NEW key is appended after the last one (so V3, V4 ...
    need no code); an EXISTING key (not the first) gets its date moved. The windows live in the persona's upload
    config: this writes a new config_version (the previous one is deactivated, so it stays as history and can be
    re-activated to roll back) and, unless restamp is False, relabels the stored sessions of the persona in the same
    transaction (only the sessions that config governs, see restamp_questionnaire_versions). With dry_run
    everything is computed and rolled back.

    The persona is identified by session_type plus the selectors (tenant_code, program_id / program_name, source),
    each of which narrows the upload configs that have questionnaire versions. It must end up as exactly ONE
    config: when several remain (another tenant, another program with the same session_type, another source)
    AmbiguousFeedbackConfigError lists them and nothing is written.

    Returns {"session_type", "tenant_code", "program_id", "program_name", "source", "changed", "config_version",
    "versions", "restamp", "dry_run"}; changed is False (and nothing is written) when the list would not change.
    Raises ValueError for an unknown persona, a missing UTC offset, a date that is not after the previous version /
    before the next one, the first version, or a persona whose stored sessions carry bot ids (the version of a bot
    session follows the order of the bot ids, so a window would change nothing for it).
    """
    from app.services.feedback_versions import parse_versions

    if not isinstance(key, str) or key.strip() == "":
        raise ValueError("the version key must be a non-empty string")
    key = key.strip()
    if not isinstance(effective_from, str):
        raise ValueError(f"effective_from {effective_from!r} is not an ISO instant (for example 2026-12-01T10:30:00+05:30)")
    try:
        instant = datetime.fromisoformat(
            effective_from[:-1] + "+00:00" if effective_from.endswith(("Z", "z")) else effective_from
        )
    except ValueError:
        raise ValueError(f"effective_from {effective_from!r} is not an ISO instant (for example 2026-12-01T10:30:00+05:30)")
    if instant.tzinfo is None:
        raise ValueError(f"effective_from {effective_from!r} needs a UTC offset (for example +05:30)")
    effective_text = instant.isoformat()
    session_type = _selector("session_type", session_type)
    if session_type is None:
        raise ValueError("session_type is required")

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {"session_type": session_type, "tenant_code": None, "program_id": None,
                              "program_name": None, "source": None, "changed": False, "dry_run": dry_run,
                              "config_version": None, "versions": None, "restamp": None}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                candidates = await _find_upload_configs(
                    conn, session_type=session_type, source=source, tenant_code=tenant_code, program_id=program_id,
                    program_name=program_name, versioned_only=True)
                target = _exactly_one(
                    candidates,
                    f"no active upload config with questionnaire versions for session_type {session_type!r}",
                    "upload configs with questionnaire versions", _PICK_HINT)
                result.update({k: v for k, v in _identity(target).items() if k != "session_type"})
                bot_sessions = sum(int(row["n"]) for row in await _governed_bot_rows(conn, target))
                if bot_sessions > 0:
                    raise ValueError(
                        f"the {bot_sessions} stored session(s) of this persona carry a bot id: the questionnaire "
                        f"version of a Mitra bot session follows the ORDER OF THE BOT IDS of its persona (lowest bot "
                        f"id = V1, next = V2 ...), not a date window, so recording a window here would change "
                        f"nothing for them. A new questionnaire is a new bot id: upload its sessions. To recompute "
                        f"the labels run: python tools/questionnaire_versions.py restamp {_selector_flags(target)}"
                    )
                # Lock the config row FIRST and read its active version afterwards in a separate statement: a
                # statement that waits for the lock keeps its snapshot for the join, so a single locking SELECT
                # over config + config_version could hand back a version a concurrent writer just deactivated.
                active = await _lock_and_read_active(conn, target["config_id"])
                if active is None:
                    raise ValueError(f"no active upload config for session_type {session_type!r}")
                config = active["config"]
                parse_versions(config)  # the stored list must be valid before we touch it
                entries = [dict(e) for e in config[_VERSIONS_KEY]]
                position = next((i for i, e in enumerate(entries) if e["key"] == key), None)
                if position == 0:
                    raise ValueError(f"version {key!r} is the first one and starts from the beginning; it has no date")
                if position is None:
                    entries.append({"key": key, "effective_from": effective_text})
                else:
                    # the same instant written with another offset (or Z) is not a change
                    existing = parse_versions(config)[position][1]
                    if existing != instant:
                        entries[position]["effective_from"] = effective_text
                new_config = {**config, _VERSIONS_KEY: entries}
                versions = parse_versions(new_config)  # unique keys, offsets, strictly increasing

                result["versions"] = entries
                if entries == config[_VERSIONS_KEY]:
                    result["config_version"] = active["version"]
                    result["restamp"] = await _restamp_sessions(conn, target, versions) if restamp else None
                    result["restamp_note"] = "no change to the version list"
                else:
                    result["config_version"] = await _write_new_config_version(
                        conn, target["config_id"], active["version_id"], new_config, created_by,
                        _note(change_note, f"questionnaire version {key} effective from {effective_text}"),
                    )
                    result["changed"] = True
                    result["restamp"] = await _restamp_sessions(conn, target, versions) if restamp else None
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result


# ---------------------------------------------------------------------------------------------
# The per-bot settings of an upload config: show / migrate / set (tools/feedback_config.py)
# ---------------------------------------------------------------------------------------------

# The keys `show` / list_feedback_config display (the required settings and the optional expect_stages) ...
_CONFIG_SETTING_KEYS = (*REQUIRED_FEEDBACK_CONFIG_KEYS, CONFIG_KEY_EXPECT_STAGES)
# ... and the keys `set` may change: SETTABLE_CONFIG_KEYS (also the optional adapter keys column_map / completion_map).
# The keys whose value interacts with the adapter: the resulting config is validated as a whole when one changes.
_ADAPTER_BLOCK_KEYS = (CONFIG_KEY_ADAPTER, CONFIG_KEY_COLUMN_MAP, CONFIG_KEY_COMPLETION_MAP)


async def _lock_and_read_active(conn, config_id) -> Optional[Dict[str, Any]]:
    """
    Lock the config row FIRST and read its active version afterwards in a separate statement (same reasoning as
    set_questionnaire_version: a statement that waits for the lock keeps its snapshot, so one locking SELECT over
    config + config_version could return a version a concurrent writer just deactivated).
    Returns {"version_id", "version", "config"} or None when the config has no active version.
    """
    await conn.fetchrow("SELECT id FROM config WHERE id = $1 FOR UPDATE", config_id)
    row = await conn.fetchrow(
        "SELECT id AS version_id, version, config FROM config_version WHERE config_id = $1 AND is_active", config_id
    )
    if row is None:
        return None
    return {"version_id": row["version_id"], "version": row["version"], "config": _as_dict(row["config"])}


async def _write_new_config_version(conn, config_id, old_version_id, new_config: Dict[str, Any],
                                    created_by: str, change_note: str) -> int:
    """Deactivate the active version (kept as history) and insert the next one. Returns its version number."""
    latest = await conn.fetchval("SELECT MAX(version) FROM config_version WHERE config_id = $1", config_id)
    await conn.execute("UPDATE config_version SET is_active = FALSE, updated_at = now() WHERE id = $1", old_version_id)
    await conn.execute(
        """
        INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
        VALUES ($1, $2, TRUE, $3::jsonb, $4, $5)
        """,
        config_id, latest + 1, json.dumps(new_config), created_by, change_note,
    )
    return latest + 1


_setting_problems = config_setting_problems  # shared with the startup check (app/database/feedback_config_health.py)


async def list_feedback_config(
    session_type: Optional[str] = None,
    *,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read-only. The per-bot settings of every ACTIVE feedback_upload config (all sources), optionally narrowed by
    session_type, tenant_code, program_id / program_name and source:
    {"configs": [{"session_type", "source", "tenant_code", "program_id", "program_name", "config_version",
    "adapter", "max_csv_upload_bytes", "ingestion_schema", "process_steps", "expect_stages",
    "problems": {key: why}}]}. A missing key shows as None and is listed under problems.
    """
    if not db.pool:
        await db.connect()
    async with db.pool.acquire() as conn:
        rows = await _find_upload_configs(
            conn, session_type=session_type, source=source, tenant_code=tenant_code, program_id=program_id,
            program_name=program_name)
    configs = []
    for row in rows:
        config = _as_dict(row["config"])
        entry: Dict[str, Any] = {**_identity(row), "config_version": row["version"]}
        for key in _CONFIG_SETTING_KEYS:
            entry[key] = config.get(key)
        entry["problems"] = _setting_problems(config)
        configs.append(entry)
    return {"configs": configs}


async def migrate_feedback_config(dry_run: bool = False, *, created_by: str = "cli") -> Dict[str, Any]:
    """
    Older databases keep the config version they were seeded with (seeds are ON CONFLICT DO NOTHING), which lacks
    the settings that were added later. For each ACTIVE feedback_upload config (every tenant, program and source:
    this is the explicit sweep) that lacks any required setting, write a NEW config_version (the old one is
    deactivated and kept as history) with the missing keys added: max_csv_upload_bytes / ingestion_schema /
    process_steps from the seed defaults, and `adapter` for a legacy row from the adapter its legacy source implied
    (LEGACY_ADAPTER_BY_CONFIG_SOURCE). A missing key with neither (for example `adapter` of a source that is not a
    legacy one) is NEVER invented: it is reported under `invalid` and the config is left as it is for that key.
    Existing keys are never overwritten (an invalid existing key is only reported). Idempotent: a second run changes
    nothing. One transaction; with dry_run it is computed and rolled back.

    Returns {"configs": n, "migrated": m, "unchanged": k, "invalid": [..], "details": [{"session_type", "source",
    "tenant_code", "program_id", "added": [keys], "config_version"}], "dry_run": bool}; no config content is returned.
    """
    if not db.pool:
        await db.connect()

    defaults = feedback_config_key_defaults()
    result: Dict[str, Any] = {"configs": 0, "migrated": 0, "unchanged": 0, "invalid": [], "details": [],
                              "dry_run": dry_run}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                targets = await conn.fetch(
                    """
                    SELECT c.id AS config_id, c.session_type, c.config_source, c.tenant_code, c.program_id
                    FROM config c
                    WHERE c.config_type = $1
                    ORDER BY c.tenant_code, c.program_id NULLS FIRST, c.session_type, c.config_source, c.id
                    """,
                    CONFIG_TYPE_FEEDBACK_UPLOAD,
                )
                for target in targets:
                    active = await _lock_and_read_active(conn, target["config_id"])
                    if active is None:
                        continue  # a config without an active version has nothing to migrate
                    result["configs"] += 1
                    config = active["config"]
                    who = {"session_type": target["session_type"], "source": target["config_source"],
                           "tenant_code": target["tenant_code"],
                           "program_id": None if target["program_id"] is None else str(target["program_id"])}
                    for key, why in _setting_problems(config).items():
                        if why != "missing":
                            result["invalid"].append({**who, "key": key, "problem": why})
                    additions: Dict[str, Any] = {}
                    for key in REQUIRED_FEEDBACK_CONFIG_KEYS:
                        if key in config:
                            continue
                        if key in defaults:
                            additions[key] = defaults[key]
                        elif key == CONFIG_KEY_ADAPTER and target["config_source"] in LEGACY_ADAPTER_BY_CONFIG_SOURCE:
                            additions[key] = LEGACY_ADAPTER_BY_CONFIG_SOURCE[target["config_source"]]
                        else:
                            result["invalid"].append({
                                **who, "key": key,
                                "problem": "missing and has no default for this source; set it with "
                                           "tools/feedback_config.py set"})
                    # A wide_form (Google Forms) config also needs its session_type and every form's
                    # questionnaire_version: ingestion rejects every response without them. They cannot be
                    # derived (the version of a form is the operator's to state), so they are reported, never
                    # invented here (tools/feedback_config.py migrate-legacy adds them to a legacy config on the operator's mapping).
                    if {**config, **additions}.get(CONFIG_KEY_ADAPTER) == ADAPTER_WIDE_FORM:
                        identity_problem = wide_form_identity_problem(config)
                        if identity_problem is not None:
                            result["invalid"].append({**who, "key": "wide_form_identity", "problem": identity_problem})
                    if len(additions) == 0:
                        result["unchanged"] += 1
                        continue
                    added = list(additions)
                    version = await _write_new_config_version(
                        conn, target["config_id"], active["version_id"], {**config, **additions}, created_by,
                        f"added {', '.join(added)}",
                    )
                    result["migrated"] += 1
                    result["details"].append({**who, "added": added, "config_version": version})
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result


async def set_feedback_config_value(
    session_type: str,
    source: str,
    key: str,
    value: Any,
    *,
    tenant_code: Optional[str] = None,
    program_id: Optional[str] = None,
    program_name: Optional[str] = None,
    unset: bool = False,
    created_by: str = "cli",
    change_note: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Change one per-bot setting (SETTABLE_CONFIG_KEYS: adapter, max_csv_upload_bytes, ingestion_schema, process_steps,
    expect_stages, column_map, completion_map) of a persona's upload config without SQL: the value is validated
    (app/services/feedback_config_validation.py), then a NEW config_version is written (the old one is deactivated and
    kept as history). `unset` removes an optional setting (expect_stages, column_map, completion_map). A change to
    adapter / column_map / completion_map is also validated as the whole adapter block (validate_adapter_config) on
    the config read under the row lock, so a concurrent change cannot slip an incompatible combination in. Nothing is written when the value is already what is stored. With dry_run everything is computed
    and rolled back.

    The config is identified by session_type + source (both required: a source is a free label) plus the selectors
    tenant_code and program_id / program_name; it must be exactly ONE config, otherwise
    AmbiguousFeedbackConfigError lists the candidates and nothing is written.

    Returns {"session_type", "source", "tenant_code", "program_id", "program_name", "key", "changed",
    "config_version", "missing_keys", "dry_run"}; missing_keys lists required settings the config still lacks (run
    `migrate`). Raises ValueError for an unknown key, an invalid value, or when no (or more than one) active config
    matches.
    """
    session_type = _selector("session_type", session_type)
    source = _selector("source", source)
    if session_type is None or source is None:
        raise ValueError("session_type and source are required")
    if key not in SETTABLE_CONFIG_KEYS:
        raise ValueError(f"key must be one of: {', '.join(SETTABLE_CONFIG_KEYS)}")
    if unset:
        if key in REQUIRED_FEEDBACK_CONFIG_KEYS:
            optional = [k for k in SETTABLE_CONFIG_KEYS if k not in REQUIRED_FEEDBACK_CONFIG_KEYS]
            raise ValueError(f"'{key}' is required and cannot be unset; only {', '.join(optional)} can")
    else:
        value = validate_config_value(key, value)

    if not db.pool:
        await db.connect()

    result: Dict[str, Any] = {"session_type": session_type, "source": source, "tenant_code": None,
                              "program_id": None, "program_name": None, "key": key, "changed": False,
                              "config_version": None, "missing_keys": None, "dry_run": dry_run}
    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                targets = await _find_upload_configs(
                    conn, session_type=session_type, source=source, tenant_code=tenant_code, program_id=program_id,
                    program_name=program_name, require_active=False)
                target = _exactly_one(
                    targets, f"no {source} upload config for session_type {session_type!r}",
                    f"{source} upload configs for session_type {session_type!r}",
                    "pass --tenant-code and --program-id (or --program-name) to pick one")
                result.update({k: v for k, v in _identity(target).items() if k not in ("session_type", "source")})
                active = await _lock_and_read_active(conn, target["config_id"])
                if active is None:
                    raise ValueError(f"no active {source} upload config for session_type {session_type!r}")
                config = active["config"]
                new_config = {k: v for k, v in config.items() if not (unset and k == key)}
                if not unset:
                    new_config[key] = value
                result["missing_keys"] = [k for k in REQUIRED_FEEDBACK_CONFIG_KEYS if k not in new_config]
                if key in _ADAPTER_BLOCK_KEYS and CONFIG_KEY_ADAPTER in new_config:
                    # Checked HERE, under the row lock, on the config as it will be written: a pre-check made before
                    # the lock (the CLI's) can be outdated by a migrate-legacy that added column_map / completion_map.
                    try:
                        validate_adapter_config(new_config)
                    except ValueError as exc:
                        raise ValueError(f"the config would be invalid after this change: {exc}")
                if new_config == config:
                    result["config_version"] = active["version"]
                else:
                    result["config_version"] = await _write_new_config_version(
                        conn, target["config_id"], active["version_id"], new_config, created_by,
                        _note(change_note, f"removed {key}" if unset else f"set {key}"),
                    )
                    result["changed"] = True
                if dry_run:
                    raise _DryRunRollback()
    except _DryRunRollback:
        pass
    return result
