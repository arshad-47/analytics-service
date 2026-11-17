"""
Database side of the feedback metadata extraction, shared by the Temporal activity
(app/temporal/feedback_extraction_activity.py) and the CLI (tools/run_feedback_extraction.py).

`extract_submission` is the one entry point: it loads a stored session, its active extraction config version and
the active prompt version the config selects (`prompt_name`, required), makes ONE LLM call with no database connection held, and (unless it is a dry run)
writes the field rows, the PII flag and the llm log in ONE transaction. Re-running a session overwrites its rows.
Nothing in here logs or stores conversation text; error messages carry no respondent text and no provider text.

A session can grow (a delta event) while the LLM call is running. The write transaction therefore locks the
feedback_submissions row, re-reads the session and compares a content fingerprint with the one taken at load: if it
differs NOTHING is written (the PII flag stays as the ingest left it, NULL) and StaleExtractionError is raised, so
the step is retried on the new content. A flag computed from older text is never stored.
"""
import asyncio
import hashlib
import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from app.config import settings
from app.database.operations import insert_llm_log, update_submission_status
from app.services.feedback_extraction import (
    ExtractionConfig,
    ExtractionOutputError,
    ExtractionResult,
    ExtractionRun,
    extract,
    parse_extraction_config,
)
from app.services.feedback_extraction_constants import (
    ACTIVITY_MARGIN_SECONDS,
    ANALYSIS_TYPE,
    CONFIG_TYPE_FEEDBACK_EXTRACTION,
    MIN_LLM_CALL_SECONDS,
)
from app.services.llm import split_llm_usage

logger = logging.getLogger("analytics_service.feedback_extraction")

# openrouter_chat_completion(prompt, *, model, max_tokens, timeout) -> (text, usage)
LlmFunction = Callable[..., Tuple[str, Dict[str, Any]]]


class ExtractionSetupError(RuntimeError):
    """
    A deterministic setup problem: no extraction config for the session, a prompt that does not exist or has no active
    version, an activity budget too small for any LLM call. Retrying cannot fix it (the activity reports it as
    non-retryable). The message names configuration, never data.
    """


class ExtractionBudgetExhaustedError(RuntimeError):
    """
    The activity's time budget was used up BEFORE the LLM call (a slow connection, status update, input or prompt
    load): too little is left for a useful call and the margin that keeps the write inside the budget. Transient, so
    the activity reports it as RETRYABLE (a fresh attempt gets a fresh budget): failing the step on a database stall
    would be worse than retrying, and no call was made, so nothing was billed and nothing was written.
    """


class StaleExtractionError(RuntimeError):
    """The session changed while its extraction was running; nothing was written. Retry (the workflow retries)."""


class LlmCallError(RuntimeError):
    """
    The LLM call failed. The message holds ONLY the exception class name and, when it could be read from the
    message prefix, the HTTP status: the provider's error body (which can echo the prompt) is dropped.
    `status` is that HTTP status (None for a timeout or connection error).
    """

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


_HTTP_STATUS_RE = re.compile(r"^[^:]{0,100}?\bHTTP (\d{3})\b")


def sanitize_llm_error(exc: BaseException) -> LlmCallError:
    """`LLM call failed (<ExceptionClass>[, HTTP <code>])`: nothing else of the original error survives."""
    detail = type(exc).__name__
    match = _HTTP_STATUS_RE.match(str(exc))
    if match is not None:
        detail += f", HTTP {match.group(1)}"
    return LlmCallError(f"LLM call failed ({detail})", int(match.group(1)) if match is not None else None)


def rows_fingerprint(rows) -> str:
    """
    Content fingerprint of a session's feedbacks rows: the row count and a hash over (sequence, parent_id,
    question_key, message, translated_message) ordered by sequence, every part length-prefixed (so no value can
    shift into its neighbour). Any message edit, added message or parent-link change changes it.
    """
    ordered = sorted(rows, key=lambda r: r["sequence"])
    digest = hashlib.sha256()
    digest.update(f"rows={len(ordered)}\n".encode("utf-8"))
    for row in ordered:
        for part in (row["sequence"], row["parent_id"], row["question_key"], row["message"], row["translated_message"]):
            if part is None:
                digest.update(b"-:")
            else:
                encoded = str(part).encode("utf-8")
                digest.update(f"{len(encoded)}:".encode("ascii") + encoded)
        digest.update(b"\n")
    return digest.hexdigest()


_ROWS_SQL = """
    SELECT sequence, parent_id, question_key, message, translated_message
    FROM feedbacks
    WHERE submission_id = $1 AND tenant_code = $2
    ORDER BY sequence
"""


def _as_json(value: Any) -> Any:
    """asyncpg returns JSONB as text unless a codec is registered; accept both."""
    return json.loads(value) if isinstance(value, str) else value


# ------------------------------------------------------------------------------------------------ loading

async def load_prompt_by_name(conn, name: str) -> Dict[str, Any]:
    """
    The ACTIVE prompt version of the prompts row whose name is exactly `name`: the highest `version` among its
    is_active versions. Raises ExtractionSetupError (a RuntimeError) when the name does not exist, has no active version, or the prompt is not
    a feedback_metadata_extraction prompt (a config must not point at, for example, a theme prompt). The message
    names the prompt (configuration), never data.
    """
    if not isinstance(name, str) or len(name) == 0:
        raise ValueError("prompt name must be a non-empty string")
    prompt = await conn.fetchrow("SELECT id, analysis_type FROM prompts WHERE name = $1", name)
    if prompt is None:
        raise ExtractionSetupError(f"No prompt named {name!r} exists in the database.")
    if prompt["analysis_type"] != ANALYSIS_TYPE:
        raise ExtractionSetupError(
            f"Prompt {name!r} has analysis_type {prompt['analysis_type']!r}, not {ANALYSIS_TYPE!r}.")
    row = await conn.fetchrow(
        """
        SELECT pv.id, pv.version, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        WHERE pv.prompt_id = $1 AND pv.is_active = TRUE
        ORDER BY pv.version DESC
        LIMIT 1
        """,
        prompt["id"],
    )
    if row is None:
        raise ExtractionSetupError(f"Prompt {name!r} has no active prompt version.")
    resolved = dict(row)
    resolved["name"] = name
    return resolved


async def load_prompt_for_config(conn, config: ExtractionConfig) -> Dict[str, Any]:
    """
    The prompt a program's extraction config selects: its `prompt_name` (required by the config parser). A name that
    does not resolve fails loudly; there is no fallback to any other prompt.
    """
    if config.prompt_name is None:
        raise ExtractionSetupError("the extraction config names no prompt (prompt_name is required)")
    return await load_prompt_by_name(conn, config.prompt_name)


async def load_extraction_config(conn, tenant_code: str, session_type: str, program_id: Any, source: str) -> Dict[str, Any]:
    """
    The active version of the extraction config of a session: config_type 'feedback_extraction', the session's
    session_type, config_source 'any' or the session's own source, and the session's EXACT program or, failing
    that, the persona's program-less default of the tenant. A config of another program is never used (a second
    program that reuses a persona name must have its own extraction config; without one the session fails loudly).
    Among the candidates the exact program wins, then a source-specific config over 'any', then the highest version.
    Returns {id, config_id, version, config: ExtractionConfig}; raises when there is none or it is malformed.
    """
    row = await conn.fetchrow(
        """
        SELECT cv.id, cv.config_id, cv.version, cv.config
        FROM config c
        JOIN config_version cv ON cv.config_id = c.id AND cv.is_active
        WHERE c.config_type = $1 AND c.tenant_code = $2 AND c.session_type = $3
          AND c.config_source IN ('any', $4)
          AND (c.program_id = $5::uuid OR c.program_id IS NULL)
        ORDER BY (c.program_id IS NOT NULL) DESC, (c.config_source = $4) DESC, cv.version DESC, c.id
        LIMIT 1
        """,
        CONFIG_TYPE_FEEDBACK_EXTRACTION, tenant_code, session_type, source, program_id,
    )
    if row is None:
        raise ExtractionSetupError(
            f"No active {CONFIG_TYPE_FEEDBACK_EXTRACTION} config for session_type {session_type!r} in the session's "
            f"program (tenant {tenant_code!r}); create one for that program (or a program-less default)."
        )
    config = parse_extraction_config(_as_json(row["config"]))
    if config.session_type != session_type:
        raise ExtractionSetupError(
            f"extraction config version {row['id']} is for session_type {config.session_type!r}, "
            f"not {session_type!r}."
        )
    return {"id": row["id"], "config_id": row["config_id"], "version": row["version"], "config": config}


async def load_closing_keys(conn, tenant_code: str, session_type: str, program_id: Any, source: str) -> List[str]:
    """
    The thank-you / closing question keys of the upload config that applies to THIS session: the same tenant,
    session_type and program (or the persona's program-less default) and the session's own source ('any' also
    qualifies). Exactly one config is used (exact program, then own source over 'any', then the highest version):
    the keys of another program or another source are never merged in.
    """
    row = await conn.fetchrow(
        """
        SELECT cv.config
        FROM config c
        JOIN config_version cv ON cv.config_id = c.id AND cv.is_active
        WHERE c.config_type = 'feedback_upload' AND c.tenant_code = $1 AND c.session_type = $2
          AND c.config_source IN ('any', $3)
          AND (c.program_id = $4::uuid OR c.program_id IS NULL)
        ORDER BY (c.program_id IS NOT NULL) DESC, (c.config_source = $3) DESC, cv.version DESC, c.id
        LIMIT 1
        """,
        tenant_code, session_type, source, program_id,
    )
    if row is None:
        return []
    config = _as_json(row["config"])
    found = config.get("closing_keys") if isinstance(config, dict) else None
    keys: List[str] = []
    if isinstance(found, list):
        keys.extend(k for k in found if isinstance(k, str) and k not in keys)
    return keys


def persona_identity(session_type: Any, source: Any, program_id: Any) -> Tuple[Any, Any, Optional[str]]:
    """
    What selects the extraction config, the prompt and the closing keys of a session: session_type, source and
    program. Compared (with the message fingerprint) under the write locks: a redelivery that moves a session to
    another persona without touching its messages must not let the old persona's fields overwrite it.
    """
    return (session_type, source, None if program_id is None else str(program_id))


async def load_submission_inputs(conn, submission_id: str, tenant_code: str) -> Dict[str, Any]:
    """
    The session's identity, its conversation rows (with their content fingerprint and the time they were read), its
    extraction config version and its closing keys.
    """
    head = await conn.fetchrow(
        """
        SELECT fs.session_type, fs.questionnaire_version, fs.source, s.program_id
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.submission_id = $1 AND fs.tenant_code = $2
        """,
        submission_id, tenant_code,
    )
    if head is None:
        raise ValueError(f"submission {submission_id} (tenant {tenant_code}) is not a stored feedback session")
    loaded_at = datetime.now(timezone.utc)
    rows = await conn.fetch(_ROWS_SQL, submission_id, tenant_code)
    config_row = await load_extraction_config(
        conn, tenant_code, head["session_type"], head["program_id"], head["source"])
    closing = await load_closing_keys(conn, tenant_code, head["session_type"], head["program_id"], head["source"])
    return {
        "session_type": head["session_type"],
        "questionnaire_version": head["questionnaire_version"],
        "identity": persona_identity(head["session_type"], head["source"], head["program_id"]),
        "rows": [dict(r) for r in rows],
        "fingerprint": rows_fingerprint(rows),
        "loaded_at": loaded_at,
        "config_row": config_row,
        "closing_keys": closing,
    }


# ------------------------------------------------------------------------------------------------ writing

async def save_extraction(
    conn,
    *,
    submission_id: str,
    tenant_code: str,
    input_fingerprint: str,
    result: ExtractionResult,
    config_version_id: Any,
    prompt_version_id: Any,
    run_at: datetime,
    llm_log: Optional[Mapping[str, Any]] = None,
    input_identity: Optional[Tuple[Any, Any, Optional[str]]] = None,
) -> None:
    """
    ONE transaction: lock the submissions row, then the feedback_submissions row (the order of the ingest path,
    app/database/operations.py: submissions first), re-read the session and compare its fingerprint with
    `input_fingerprint` (taken when the input was loaded) and, when `input_identity` is given (persona_identity of
    the loaded session: session_type, source, program), the identity as well; when either differs raise
    StaleExtractionError and write nothing. Otherwise upsert one row per field (a re-run overwrites; a field that is no longer configured is
    deleted), set feedback_submissions.pii_detected and merge the `extraction` object into its meta_data, and
    write the llm log (when the LLM was called). `run_at` is the time the input was loaded. The meta_data object
    holds ids, PII types and the time, no text. session_type / questionnaire_version are not copied into the rows.
    """
    names = [f.field_name for f in result.fields]
    confidences = [None if f.confidence is None else Decimal(str(f.confidence)) for f in result.fields]
    extraction_meta = {
        "config_version_id": str(config_version_id),
        "prompt_version_id": str(prompt_version_id),
        "pii_types": list(result.pii_types),
        "run_at": run_at.isoformat(),
    }
    async with conn.transaction():
        # The ingest path takes these same row locks, in this same order (submissions, then feedback_submissions),
        # when it touches the session, so after these statements the messages and the persona cannot change
        # until we commit.
        master = await conn.fetchrow(
            "SELECT program_id FROM submissions WHERE submission_id = $1 AND tenant_code = $2 FOR NO KEY UPDATE",
            submission_id, tenant_code,
        )
        feedback = None if master is None else await conn.fetchrow(
            "SELECT session_type, source FROM feedback_submissions WHERE submission_id = $1 AND tenant_code = $2 "
            "FOR UPDATE",
            submission_id, tenant_code,
        )
        if feedback is None:
            raise ValueError(f"submission {submission_id} (tenant {tenant_code}) is not a stored feedback session")
        current = rows_fingerprint(await conn.fetch(_ROWS_SQL, submission_id, tenant_code))
        if current != input_fingerprint:
            raise StaleExtractionError(
                f"feedback session {submission_id} changed while its extraction was running; nothing was written, "
                f"it must run again on the new content")
        if input_identity is not None and persona_identity(
                feedback["session_type"], feedback["source"], master["program_id"]) != input_identity:
            raise StaleExtractionError(
                f"feedback session {submission_id} was moved to another persona (session_type, source or program) "
                f"while its extraction was running; nothing was written, it must run again for the new persona")
        await conn.execute(
            """
            INSERT INTO feedback_extracted_data (
                submission_id, tenant_code, field_name,
                value, raw_value, status, confidence, config_version_id, prompt_version_id, extracted_at
            )
            SELECT $1, $2, t.field_name, t.value, t.raw_value, t.status, t.confidence,
                   $3::uuid, $4::uuid, $5::timestamptz
            FROM unnest($6::text[], $7::text[], $8::text[], $9::text[], $10::numeric[])
                 AS t(field_name, value, raw_value, status, confidence)
            ON CONFLICT (submission_id, tenant_code, field_name) DO UPDATE SET
                value = EXCLUDED.value,
                raw_value = EXCLUDED.raw_value,
                status = EXCLUDED.status,
                confidence = EXCLUDED.confidence,
                config_version_id = EXCLUDED.config_version_id,
                prompt_version_id = EXCLUDED.prompt_version_id,
                extracted_at = EXCLUDED.extracted_at,
                updated_at = now()
            """,
            submission_id, tenant_code,
            str(config_version_id), str(prompt_version_id), run_at,
            names, [f.value for f in result.fields], [f.raw_value for f in result.fields],
            [f.status for f in result.fields], confidences,
        )
        await conn.execute(
            """
            DELETE FROM feedback_extracted_data
            WHERE submission_id = $1 AND tenant_code = $2 AND field_name <> ALL($3::text[])
            """,
            submission_id, tenant_code, names,
        )
        await conn.execute(
            """
            UPDATE feedback_submissions
            SET pii_detected = $3,
                meta_data = COALESCE(meta_data, '{}'::jsonb) || jsonb_build_object('extraction', $4::jsonb),
                updated_at = now()
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code, result.pii_detected, json.dumps(extraction_meta),
        )
        if llm_log is not None:
            await insert_llm_log(conn, submission_id, tenant_code, **llm_log)


def _safe_error_text(error: BaseException) -> str:
    """
    What may be stored in llm_logs.error_message: our own errors (built without respondent or provider text) with
    their message, anything else only by class name.
    """
    if isinstance(error, (ExtractionOutputError, LlmCallError, StaleExtractionError)):
        return f"{type(error).__name__}: {str(error)[:300]}"
    return type(error).__name__


async def _log_failed_call(pool, submission_id, tenant_code, model, prompt_version_id, error: Exception, usage) -> None:
    """Best effort: a failure of the logging must not hide the real error."""
    try:
        prompt_tokens, completion_tokens, usage_meta = split_llm_usage(usage)
        async with pool.acquire() as conn:
            await insert_llm_log(
                conn, submission_id, tenant_code, model, ANALYSIS_TYPE, str(prompt_version_id),
                prompt_tokens, completion_tokens, "failed",
                error_message=_safe_error_text(error),
                meta_data=usage_meta if len(usage_meta) > 0 else None,
            )
    except Exception as log_err:  # noqa: BLE001
        logger.error("Failed to log the extraction error to llm_logs: %s", type(log_err).__name__)


# ------------------------------------------------------------------------------------------------ the entry point

def resolve_llm_settings(config: ExtractionConfig, overrides: Mapping[str, Any]) -> Dict[str, Any]:
    """
    model / max_tokens / timeout: the workflow step override (llm_model, max_tokens, llm_timeout_seconds) wins,
    then the extraction config's `llm`, then the global .env settings. Only `is None` means unset.
    """
    def pick(override_key: str, config_key: str, default: Any) -> Any:
        override = overrides.get(override_key)
        if override is not None:
            return override
        configured = config.llm.get(config_key)
        return configured if configured is not None else default

    model = pick("llm_model", "model", settings.OPENROUTER_MODEL)
    max_tokens = pick("max_tokens", "max_tokens", settings.LLM_MAX_TOKENS)
    timeout = pick("llm_timeout_seconds", "timeout_seconds", settings.LLM_TIMEOUT_SECONDS)
    if not isinstance(model, str) or len(model.strip()) == 0:
        raise ValueError("llm model must be a non-empty string")
    for label, value in (("max_tokens", max_tokens), ("timeout", timeout)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"llm {label} must be a positive integer")
    return {"model": model, "max_tokens": max_tokens, "timeout": timeout}


# Preparation can use time the workflow's budget did not plan for; measured from the start of extract_submission.
_monotonic = time.monotonic


def clamp_llm_timeout(timeout: int, activity_budget_seconds: int, elapsed_seconds: float = 0.0) -> int:
    """
    The LLM timeout that fits what is left of the Temporal activity's budget:
    min(timeout, budget - elapsed - ACTIVITY_MARGIN_SECONDS), so the call ends before the margin that the write
    needs, whatever the connection, status update, input load and prompt load already used (`elapsed_seconds`,
    measured by extract_submission from its own start).
    The workflow sizes the budget from the step's llm_timeout_seconds or the .env setting only (workflow code cannot
    read the database), so a longer `llm.timeout_seconds` of the extraction config can exceed it; that must not fail
    every submission, so the call is shortened instead (one WARNING with the two numbers, never any text). Giving a
    longer call a bigger budget stays the job of `llm_timeout_seconds` on the workflow step. A reduction caused only
    by the elapsed preparation time (the timeout fits the budget on its own) is logged at INFO.
    Two refusals, both before any call: a budget with nothing left after the margin is ExtractionSetupError
    (deterministic, retrying cannot help, non-retryable); a budget that was fine but whose remainder the preparation
    used up (less than MIN_LLM_CALL_SECONDS, or than the timeout itself when that is shorter) is
    ExtractionBudgetExhaustedError (transient, RETRYABLE: a database stall is not a reason to fail the step).
    """
    planned = activity_budget_seconds - ACTIVITY_MARGIN_SECONDS
    if planned <= 0:
        raise ExtractionSetupError(
            f"the activity budget ({activity_budget_seconds} s) leaves no time for an LLM call after the "
            f"{ACTIVITY_MARGIN_SECONDS} s margin; set llm_timeout_seconds on the workflow step")
    spent = math.floor(max(elapsed_seconds, 0.0))   # whole seconds only: a fraction of one is covered by the margin
    available = planned - spent
    if available < min(timeout, MIN_LLM_CALL_SECONDS):
        raise ExtractionBudgetExhaustedError(
            f"{spent} s of the {activity_budget_seconds} s activity budget went on "
            f"preparation, too little is left for an LLM call after the {ACTIVITY_MARGIN_SECONDS} s margin; "
            f"the step will be retried")
    if timeout <= available:
        return timeout
    if timeout <= planned:
        logger.info(
            "LLM timeout of %s s shortened to %s s: %s s of the %s s activity budget went on preparation",
            timeout, available, spent, activity_budget_seconds)
    else:
        logger.warning(
            "LLM timeout of %s s does not fit the activity budget of %s s; using %s s "
            "(set llm_timeout_seconds on the workflow step to allow a longer call)",
            timeout, activity_budget_seconds, available)
    return available


@dataclass
class SubmissionOutcome:
    submission_id: str
    tenant_code: str
    session_type: str
    questionnaire_version: str
    run: ExtractionRun
    model: str
    config_version_id: Any
    prompt_version_id: Any
    written: bool
    field_kinds: Dict[str, str] = field(default_factory=dict)   # field name -> 'category' | 'free_text' | 'integer'
    prompt_name: Optional[str] = None                           # the prompt that was used


async def extract_submission(
    pool,
    submission_id: str,
    tenant_code: str,
    *,
    llm_function: Optional[LlmFunction] = None,
    llm_overrides: Optional[Mapping[str, Any]] = None,
    write: bool = True,
    mark_processing: bool = False,
    activity_budget_seconds: Optional[int] = None,
) -> SubmissionOutcome:
    """
    Extracts the metadata of one stored feedback session.

    Phase 1 (own connection): optionally mark the submission 'processing', load the session (with its content
    fingerprint and load time), config and the prompt the config selects (the prompt_version actually used is the
    one recorded on the rows and in the llm log).
    Phase 2: ONE LLM call on a worker thread, no connection held. Phase 3 (own connection, one transaction): lock
    the session, verify it did not change since phase 1 (else StaleExtractionError, nothing written, retry), write
    the rows, the PII flag and the llm log. `write=False` is a dry run: the LLM is called, nothing is written and
    no llm log is made. A failed LLM call or an invalid answer writes a 'failed' llm log (not on a dry run) and raises.
    A conversation without a respondent answer makes no LLM call (all fields not_provided, pii_detected FALSE). A
    failed LLM call raises LlmCallError carrying only the exception class and HTTP status, never the provider text.
    A 'failed' llm log is written only when a call was really made: a setup or prompt error before the call is not
    logged (and costs nothing). `activity_budget_seconds` (the Temporal activity's start_to_close_timeout, when run as
    an activity) clamps an effective LLM timeout that does not fit the budget to budget - (time spent in this
    function so far) - ACTIVITY_MARGIN_SECONDS (a WARNING names both numbers); a budget with nothing left raises
    ExtractionSetupError (non-retryable), a budget the preparation used up raises ExtractionBudgetExhaustedError
    (retryable), both before any call.
    """
    if llm_function is None:
        from app.services.llm import openrouter_chat_completion
        llm_function = openrouter_chat_completion
    overrides = llm_overrides if llm_overrides is not None else {}
    started = _monotonic()   # the activity budget runs from here: the preparation below is part of it

    async with pool.acquire() as conn:
        if mark_processing:
            await update_submission_status(conn, submission_id, tenant_code, "processing")
        inputs = await load_submission_inputs(conn, submission_id, tenant_code)
        config: ExtractionConfig = inputs["config_row"]["config"]
        prompt = await load_prompt_for_config(conn, config)
    llm = resolve_llm_settings(config, overrides)
    if activity_budget_seconds is not None:
        llm["timeout"] = clamp_llm_timeout(llm["timeout"], activity_budget_seconds, _monotonic() - started)
    prompt_version_id = prompt["id"]
    config_version_id = inputs["config_row"]["id"]

    called = {"made": False}

    def call_llm(text: str) -> Tuple[str, Dict[str, Any]]:
        called["made"] = True
        try:
            return llm_function(text, model=llm["model"], max_tokens=llm["max_tokens"], timeout=llm["timeout"])
        except Exception as exc:  # noqa: BLE001
            failure = sanitize_llm_error(exc)
        # Raised outside the handler: no __cause__ / __context__, so the provider's error text (which can echo the
        # prompt) reaches neither llm_logs nor the Temporal failure history.
        raise failure

    try:
        run = await asyncio.to_thread(
            extract, inputs["rows"], config, prompt["system_prompt"], prompt["user_prompt"], call_llm,
            inputs["closing_keys"],
        )
    except Exception as exc:
        if write and called["made"]:
            await _log_failed_call(pool, submission_id, tenant_code, llm["model"], prompt_version_id, exc,
                                   getattr(exc, "usage", {}))
        raise

    if write:
        llm_log = None
        if run.llm_called:
            prompt_tokens, completion_tokens, usage_meta = split_llm_usage(run.usage)
            llm_log = {
                "model_name": llm["model"], "analysis_type": ANALYSIS_TYPE, "prompt_version_id": str(prompt_version_id),
                "prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens, "status": "success",
                "meta_data": usage_meta if len(usage_meta) > 0 else None,
            }
        stale: Optional[StaleExtractionError] = None
        async with pool.acquire() as conn:
            try:
                await save_extraction(
                    conn, submission_id=submission_id, tenant_code=tenant_code,
                    input_fingerprint=inputs["fingerprint"], input_identity=inputs["identity"],
                    result=run.result, config_version_id=config_version_id, prompt_version_id=prompt_version_id,
                    run_at=inputs["loaded_at"], llm_log=llm_log,
                )
            except StaleExtractionError as exc:
                stale = exc
        if stale is not None:
            # Nothing was written (the transaction rolled back). The call still cost tokens: log it as failed.
            # This runs AFTER the connection above went back to the pool: _log_failed_call takes its own, and
            # holding one while waiting for another would deadlock a pool whose connections all hold a stale result.
            if run.llm_called:
                stale.usage = dict(run.usage)   # for the callers that total the usage of failed sessions (the CLI)
                await _log_failed_call(pool, submission_id, tenant_code, llm["model"], prompt_version_id, stale, run.usage)
            raise stale
    return SubmissionOutcome(
        submission_id, tenant_code, inputs["session_type"], inputs["questionnaire_version"], run, llm["model"],
        config_version_id, prompt_version_id, write, {f.name: f.kind for f in config.fields}, prompt["name"],
    )
