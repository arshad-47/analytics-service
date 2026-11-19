"""
Startup health check of the feedback upload configs (run once per process from Database.initialize_schema, so the
web, consumer and worker startup all log it).

A database that predates the per-bot settings (max_csv_upload_bytes, ingestion_schema, process_steps) keeps the
config version it was seeded with, and looks healthy until the first event arrives and is dead-lettered. This check
makes that visible at startup:

  * one ERROR per ACTIVE feedback_upload config that has an invalid required setting or one whose adapter / column_map /
    completion_map content is invalid (same validators as `python tools/feedback_config.py show`); a config with no
    session_type yet is a legacy row and is covered by the next item instead of by its own line;
  * ONE ERROR for the legacy data of a database created before session_type / questionnaire versions / adapters
    existed (configs with a NULL session_type, configs and stored sessions that lack the new keys / labels), with the
    exact dry-run command per tenant (`migrate-legacy`, app/database/feedback_legacy_upgrade.py);
  * one WARNING when the unique index of `config` could not be created because two config rows collide (the old
    design kept one row per bot); startup carried on without it;
  * one WARNING when one of the removed environment variables is still set (they are ignored now).

Only names, session types, sources and ids are logged, never values or config content. The check never raises: a
failure to run it is a warning, not a startup failure.
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from dotenv import dotenv_values

from app.database.feedback_legacy_upgrade import (
    CONFIG_UNIQUE_INDEX,
    LEGACY_MIGRATE_COMMAND,
    adapter_config_problem,
    find_legacy_state,
    legacy_commands,
    legacy_state_is_clean,
)
from app.services.feedback_config_validation import config_setting_problems
from app.services.feedback_constants import CONFIG_TYPE_FEEDBACK_UPLOAD

logger = logging.getLogger("analytics_service.db")

REPO_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"

# Settings that moved into each persona's feedback_upload config (see app/services/feedback_constants.py).
REMOVED_ENV_VARS = ("FEEDBACK_MAX_CSV_UPLOAD_BYTES", "FEEDBACK_KAFKA_SCHEMA", "PROCESS_CONFIG_FEEDBACK")


def removed_env_vars_in_use(
    environ: Optional[Mapping[str, str]] = None, env_file: Optional[Path] = None
) -> List[str]:
    """Names (never values) of the removed variables that are set in the process environment or in the .env file."""
    environ = os.environ if environ is None else environ
    env_file = REPO_ENV_FILE if env_file is None else env_file
    found = [name for name in REMOVED_ENV_VARS if name in environ]
    if env_file.is_file():
        in_file = dotenv_values(env_file)
        found.extend(name for name in REMOVED_ENV_VARS if name in in_file and name not in found)
    return found


def warn_removed_env_vars(environ: Optional[Mapping[str, str]] = None, env_file: Optional[Path] = None) -> List[str]:
    """Logs one WARNING naming the removed variables still set; returns the names."""
    names = removed_env_vars_in_use(environ, env_file)
    if len(names) > 0:
        logger.warning(
            "Ignoring removed environment variable(s) %s: these settings now live in each persona's feedback_upload "
            "config (see `python tools/feedback_config.py show`); remove them from the environment / .env.",
            ", ".join(names),
        )
    return names


async def check_feedback_configs(conn) -> List[Dict[str, Any]]:
    """
    Logs one ERROR per ACTIVE feedback_upload config (any source) with a missing / invalid required setting and
    returns [{"session_type", "source", "tenant_code", "problems": {key: why}}] for those configs.
    """
    rows = await conn.fetch(
        """
        SELECT c.session_type, c.config_source, c.tenant_code, v.config
        FROM config c
        JOIN config_version v ON v.config_id = c.id AND v.is_active
        WHERE c.config_type = $1
        ORDER BY c.session_type, c.config_source, c.tenant_code
        """,
        CONFIG_TYPE_FEEDBACK_UPLOAD,
    )
    invalid = []
    for row in rows:
        config = row["config"]
        if isinstance(config, str):
            config = json.loads(config)
        if isinstance(config, dict):
            problems = config_setting_problems(config)
            if "adapter" not in problems:  # a missing / unknown adapter is already listed above
                reason = adapter_config_problem(config)
                if reason is not None:
                    problems["adapter_config"] = reason
        else:
            problems = {"config": f"must be a JSON object, got {type(config).__name__}"}
        if len(problems) == 0:
            continue
        entry = {
            "session_type": row["session_type"], "source": row["config_source"],
            "tenant_code": row["tenant_code"], "problems": problems,
        }
        invalid.append(entry)
        if row["session_type"] is None:
            continue  # a legacy row: reported by check_legacy_feedback_data with the command that fixes it
        logger.error(
            "Feedback upload config of session_type %r (%s, tenant %r) is invalid: %s. Feedback events for this "
            "persona will be dead-lettered and its uploads rejected until it is fixed: run "
            "`python tools/feedback_config.py migrate` (adds missing keys; or migrate-legacy for a database created before session_type existed) or `show` (details).",
            entry["session_type"], entry["source"], entry["tenant_code"],
            "; ".join(f"{key}: {why}" for key, why in problems.items()),
        )
    return invalid


async def check_legacy_feedback_data(conn) -> Dict[str, Any]:
    """
    Logs ONE ERROR when the database still holds legacy feedback data (configs without session_type or with missing
    required keys, stored sessions without session_type / questionnaire_version), naming the exact dry-run command
    per tenant, and ONE WARNING when the unique index of `config` is missing because configs collide. Returns the
    state of find_legacy_state. Counts, ids and key names only.
    """
    state = await find_legacy_state(conn)
    if not legacy_state_is_clean(state):
        null_configs = sum(len(v) for v in state["null_session_type_programs"].values())
        logger.error(
            "Legacy feedback data: %d program(s) have an upload config without session_type, %d config(s) lack "
            "required keys (%s), %d stored session(s) have no session_type and %d no questionnaire_version. "
            "Uploads for these personas are rejected and their pending sessions fail until the database is "
            "upgraded. Run once per tenant, check the counts, then run again without --dry-run: %s "
            "(replace <SESSION_TYPE> with the persona key of each program; README \"Upgrading an existing database\")",
            null_configs, len(state["configs_missing_keys"]),
            ", ".join(sorted({k for m in state["configs_missing_keys"] for k in m["keys"]})) or "none",
            sum(state["sessions_without_session_type"].values()),
            sum(state["sessions_without_questionnaire_version"].values()),
            " | ".join(legacy_commands(state)),
        )
    if state["index_missing"]:
        logger.warning(
            "Table config has no unique index %s: %d group(s) of config rows share the same tenant, source, type, "
            "program and session_type (or session_type is still NULL), so it could not be created and startup "
            "carried on without it. Keep one config row per group and retire the others with `%s --tenant-code "
            "<tenant> --retire-config <config id> --dry-run` (the index is created by the same command once "
            "the data allows it).",
            CONFIG_UNIQUE_INDEX, state["duplicate_groups"], LEGACY_MIGRATE_COMMAND,
        )
    return state


async def run_startup_checks(conn) -> None:
    """The whole startup check; never raises (each part on its own: one failing part does not hide the others)."""
    try:
        warn_removed_env_vars()
    except Exception as exc:  # a health check must not stop the service from starting
        logger.warning("Feedback config startup check could not run: %s", exc)
    try:
        await check_feedback_configs(conn)
    except Exception as exc:
        logger.warning("Feedback config startup check could not run: %s", exc)
    try:
        await check_legacy_feedback_data(conn)
    except Exception as exc:
        logger.warning("Legacy feedback data check could not run: %s", exc)
