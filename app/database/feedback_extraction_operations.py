"""
Database side of the feedback metadata extraction, shared by the Temporal activity
(app/temporal/feedback_extraction_activity.py) and the CLI (tools/run_feedback_extraction.py).

`extract_submission` is the one entry point: it loads a stored session, its active extraction config version and
the active prompt version the config selects (`prompt_name`, required), makes ONE LLM call with no database connection held, and (unless it is a dry run)
writes the field rows, the PII flag and the llm log in ONE transaction. Re-running a session overwrites its rows.
Nothing in here logs conversation text; error messages carry no respondent text and no provider text. The text stored
besides the extracted values: the MASKED question and answer JSON of the optional masking call and the model's
justifications (meta_data.extraction.masking, feedback_extracted_data.justification); the complete answers of the two
calls of a session (meta_data.extraction.responses, replaced by every re-run); and a truncated copy
of the answer of a call that failed to give a usable one (llm_logs.meta_data.response).

A session can grow (a delta event) while the LLM call is running. The write transaction therefore locks the
feedback_submissions row, re-reads the session and compares a content fingerprint with the one taken at load: if it
differs NOTHING is written (the PII flag stays as the ingest left it, NULL) and StaleExtractionError is raised, so
the step is retried on the new content. A flag computed from older text is never stored.
"""
import asyncio
import copy
import hashlib
import json
import logging
import math
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from app.config import settings
from app.database.operations import insert_llm_log, update_submission_status
from app.services.feedback_extraction import (
    ExtractionConfig,
    ExtractionOutputError,
    ExtractionResult,
    ExtractionRun,
    extract,
    parse_extraction_config,
    submission_geography,
)
from app.services.feedback_extraction_constants import (
    ACTIVITY_MARGIN_SECONDS,
    ANALYSIS_TYPE,
    CONFIG_TYPE_FEEDBACK_EXTRACTION,
    FAILED_RESPONSE_MAX_CHARS,
    KIND_CATEGORY,
    MIN_LLM_CALL_SECONDS,
    PII_MASKING_ANALYSIS_TYPE,
    STAGE_METADATA_EXTRACTION,
    STAGE_PII_MASKING,
)
from app.services.feedback_pii_masking import MaskingStep, masking_meta
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

async def load_prompt_by_name(conn, name: str, analysis_type: str = ANALYSIS_TYPE) -> Dict[str, Any]:
    """
    The ACTIVE prompt version of the prompts row whose name is exactly `name`: the highest `version` among its
    is_active versions. Raises ExtractionSetupError (a RuntimeError) when the name does not exist, has no active version, or the prompt is not
    of `analysis_type` (default feedback_metadata_extraction; a config must not point at, for example, a theme
    prompt). The message names the prompt (configuration), never data.
    """
    if not isinstance(name, str) or len(name) == 0:
        raise ValueError("prompt name must be a non-empty string")
    prompt = await conn.fetchrow("SELECT id, analysis_type FROM prompts WHERE name = $1", name)
    if prompt is None:
        raise ExtractionSetupError(f"No prompt named {name!r} exists in the database.")
    if prompt["analysis_type"] != analysis_type:
        raise ExtractionSetupError(
            f"Prompt {name!r} has analysis_type {prompt['analysis_type']!r}, not {analysis_type!r}.")
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


def asked_fields(config: Optional[ExtractionConfig], questionnaire_version: Any) -> Tuple[str, ...]:
    """The names of the fields the session's questionnaire version asks (empty when there is no config to tell)."""
    if config is None:
        return ()
    return tuple(f.name for f in config.fields_for(questionnaire_version))


def persona_identity(session_type: Any, source: Any, program_id: Any,
                     asked: Tuple[str, ...] = ()) -> Tuple[Any, Any, Optional[str], Tuple[str, ...]]:
    """
    What selects the extraction config, the prompt, the closing keys and the questions asked of a session: session_type,
    source, program and the FIELDS its questionnaire version asks (`asked_fields`). Compared (with the message
    fingerprint) under the write locks: a redelivery that moves a session to another persona, or a relabel that changes
    which fields are asked, without touching its messages must not let the old values overwrite it. A relabel that
    asks the same fields (every General Public relabel) changes nothing, so it must NOT discard a finished call.
    """
    return (session_type, source, None if program_id is None else str(program_id), asked)


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
        "identity": persona_identity(head["session_type"], head["source"], head["program_id"],
                                     asked_fields(config_row["config"], head["questionnaire_version"])),
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
    masking: Optional[Mapping[str, Any]] = None,
    extra_llm_logs: Sequence[Mapping[str, Any]] = (),
    responses: Optional[Mapping[str, str]] = None,
    config: Optional[ExtractionConfig] = None,
) -> None:
    """
    ONE transaction: lock the submissions row, then the feedback_submissions row (the order of the ingest path,
    app/database/operations.py: submissions first), re-read the session and compare its fingerprint with
    `input_fingerprint` (taken when the input was loaded) and, when `input_identity` is given (persona_identity of
    the loaded session: session_type, source, program), the identity as well; when either differs raise
    StaleExtractionError and write nothing. Otherwise upsert one row per field (a re-run overwrites; a field that is no longer configured is
    deleted), set feedback_submissions.pii_detected and merge the `extraction` object into its meta_data, and
    write the llm log (when the LLM was called). `run_at` is the time the input was loaded. The meta_data object
    holds ids, PII types and the time; when the masking call ran (`masking`) it also holds that call's MASKED answers and
    findings (never the original text). `responses` ({'masking': text, 'extraction': text}, the complete answers of the
    calls) is stored under `extraction.responses` for every session, flagged ones included (the data stays in our
    system; what an export may contain is decided by its query). The whole `extraction`
    object is replaced by a re-run, so older responses never linger. session_type / questionnaire_version are not copied
    into the rows.
    """
    names = [f.field_name for f in result.fields]
    justifications = [f.justification for f in result.fields]
    value_lists = [json.dumps(list(f.value_list)) for f in result.fields]
    confidences = [None if f.confidence is None else Decimal(str(f.confidence)) for f in result.fields]
    extraction_meta = {
        "config_version_id": str(config_version_id),
        "prompt_version_id": str(prompt_version_id),
        "pii_types": list(result.pii_types),
        "run_at": run_at.isoformat(),
    }
    if masking is not None:
        extraction_meta["masking"] = dict(masking)   # the MASKED answers, pii types and abuse flag of the masking call
    kept_responses = {k: v for k, v in (responses or {}).items() if isinstance(v, str)}
    if len(kept_responses) > 0:
        extraction_meta["responses"] = kept_responses
    async with conn.transaction():
        # The ingest path takes these same row locks, in this same order (submissions, then feedback_submissions),
        # when it touches the session, so after these statements the messages and the persona cannot change
        # until we commit.
        master = await conn.fetchrow(
            "SELECT program_id FROM submissions WHERE submission_id = $1 AND tenant_code = $2 FOR NO KEY UPDATE",
            submission_id, tenant_code,
        )
        feedback = None if master is None else await conn.fetchrow(
            "SELECT session_type, source, questionnaire_version FROM feedback_submissions "
            "WHERE submission_id = $1 AND tenant_code = $2 "
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
                feedback["session_type"], feedback["source"], master["program_id"],
                asked_fields(config, feedback["questionnaire_version"])) != input_identity:
            raise StaleExtractionError(
                f"feedback session {submission_id} was moved to another persona (session_type, source, program) or "
                f"relabelled to a questionnaire version that asks other fields "
                f"while its extraction was running; nothing was written, it must run again for the new persona")
        await conn.execute(
            """
            INSERT INTO feedback_extracted_data (
                submission_id, tenant_code, field_name,
                value, value_list, raw_value, status, confidence, justification, config_version_id, prompt_version_id,
                extracted_at
            )
            SELECT $1, $2, t.field_name, t.value, ARRAY(SELECT jsonb_array_elements_text(t.value_list::jsonb)),
                   t.raw_value, t.status, t.confidence, t.justification, $3::uuid, $4::uuid, $5::timestamptz
            FROM unnest($6::text[], $7::text[], $8::text[], $9::text[], $10::numeric[], $11::text[], $12::text[])
                 AS t(field_name, value, raw_value, status, confidence, justification, value_list)
            ON CONFLICT (submission_id, tenant_code, field_name) DO UPDATE SET
                value = EXCLUDED.value,
                value_list = EXCLUDED.value_list,
                raw_value = EXCLUDED.raw_value,
                status = EXCLUDED.status,
                confidence = EXCLUDED.confidence,
                justification = EXCLUDED.justification,
                config_version_id = EXCLUDED.config_version_id,
                prompt_version_id = EXCLUDED.prompt_version_id,
                extracted_at = EXCLUDED.extracted_at,
                updated_at = now()
            """,
            submission_id, tenant_code,
            str(config_version_id), str(prompt_version_id), run_at,
            names, [f.value for f in result.fields], [f.raw_value for f in result.fields],
            [f.status for f in result.fields], confidences, justifications, value_lists,
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
                abusive_language = $5,
                meta_data = COALESCE(meta_data, '{}'::jsonb) || jsonb_build_object('extraction', $4::jsonb),
                updated_at = now()
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code, result.pii_detected, json.dumps(extraction_meta),
            None if masking is None else bool(masking.get("abusive_language")),
        )
        if config is not None and len(config.submission_columns) > 0:
            # Same transaction as the extraction: the platform-facing columns never disagree with the extracted rows.
            # Column names come from the fixed whitelist inside submission_geography, never from the config text.
            geography = submission_geography(config, result)
            assignments = ", ".join(f"{column} = ${index}" for index, column in enumerate(geography, start=3))
            await conn.execute(
                f"UPDATE submissions SET {assignments}, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code, *geography.values(),
            )
        for log in extra_llm_logs:
            await insert_llm_log(conn, submission_id, tenant_code, **log)
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


def failed_response_meta(response: Any) -> Dict[str, Any]:
    """
    {'response': <the model's answer cut to FAILED_RESPONSE_MAX_CHARS>[, 'response_truncated': True]} for the llm log of
    a call whose answer could not be used, {} when there is none (a timeout or a refused call has no answer). This is
    the one place a failed answer is kept, so a bad answer can be read without calling the model again.
    """
    if not isinstance(response, str) or len(response) == 0:
        return {}
    meta: Dict[str, Any] = {"response": response[:FAILED_RESPONSE_MAX_CHARS]}
    if len(response) > FAILED_RESPONSE_MAX_CHARS:
        meta["response_truncated"] = True
    return meta


async def _log_failed_call(pool, submission_id, tenant_code, model, prompt_version_id, error: Optional[Exception], usage,
                           analysis_type: str = ANALYSIS_TYPE, response: Any = None) -> None:
    """
    Best effort: a failure of the logging must not hide the real error. `error` None logs the call as a success
    (the masking call that worked before the extraction call failed: it still cost tokens). `response` (the answer of
    a call that was made but not usable) is kept, cut, in the log's meta_data.
    """
    try:
        prompt_tokens, completion_tokens, usage_meta = split_llm_usage(usage)
        usage_meta = {**usage_meta, **failed_response_meta(response)}
        async with pool.acquire() as conn:
            await insert_llm_log(
                conn, submission_id, tenant_code, model, analysis_type, str(prompt_version_id),
                prompt_tokens, completion_tokens, "success" if error is None else "failed",
                error_message=None if error is None else _safe_error_text(error),
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


# The stage timings have a clock of their own: the activity-budget arithmetic above reads `_monotonic` at exact points
# (tests feed it a fixed sequence), and reporting a stage must never consume or shift those readings.
_stage_clock = time.monotonic


class StageTracker:
    """
    What the stages of the ONE Temporal activity did, in order: [{'name': 'pii_masking', 'status': 'success',
    'seconds': 1.3, 'prompt_tokens': 812, 'completion_tokens': 140}, {'name': 'metadata_extraction', ...}]. A status is
    'running' (started), 'success', 'failed' or 'skipped' (with a 'reason'). Temporal shows one activity for the
    step, so this record is the stage-by-stage view of it: it is the activity's result, the step's entry in
    submissions.process_status and, while the activity runs, its heartbeat details (`on_stage`, called with a copy of
    the whole list after every change; Temporal throttles heartbeats, so a short stage can be missed live).
    `update` is called on the worker thread that makes the LLM calls and from the event loop: it is thread-safe, never
    carries text (names, statuses, seconds and token counts only), and an error in `on_stage` never reaches the
    extraction.
    """

    def __init__(self, submission_id: str, on_stage: Optional[Callable[[List[Dict[str, Any]]], None]] = None, loop=None):
        self._submission_id = submission_id
        self._on_stage = on_stage
        self._loop = loop
        self._loop_thread = threading.get_ident()   # built on the event loop's thread (extract_submission is a coroutine)
        self._lock = threading.Lock()
        self._stages: List[Dict[str, Any]] = []
        self._started: Dict[str, float] = {}

    @property
    def stages(self) -> List[Dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._stages)

    def update(self, stage: str, status: str, usage: Optional[Mapping[str, Any]] = None, reason: Optional[str] = None) -> None:
        now = _stage_clock()
        with self._lock:
            if status == "running":
                self._started[stage] = now
                record: Dict[str, Any] = {"name": stage, "status": "running"}
                self._stages.append(record)
            else:
                record = next((r for r in reversed(self._stages) if r["name"] == stage), None)
                if record is None:       # a skipped stage never ran
                    record = {"name": stage}
                    self._stages.append(record)
                record["status"] = status
                if stage in self._started:
                    record["seconds"] = round(now - self._started[stage], 2)
                if usage is not None:
                    prompt_tokens, completion_tokens, _ = split_llm_usage(dict(usage))
                    record["prompt_tokens"] = prompt_tokens
                    record["completion_tokens"] = completion_tokens
                if reason is not None:
                    record["reason"] = reason
            snapshot = copy.deepcopy(self._stages)
        logger.info("feedback extraction stage %s %s for submission %s%s", stage, status, self._submission_id,
                    "" if "seconds" not in record else f" in {record['seconds']} s")
        if self._on_stage is None:
            return
        try:
            if self._loop is not None and threading.get_ident() != self._loop_thread:
                # The Temporal SDK's heartbeat must be called on the event loop's thread; the calls run on a worker
                # thread. call_soon_threadsafe carries this thread's context, so the activity is still the current one.
                self._loop.call_soon_threadsafe(self._relay, snapshot)
            else:
                self._relay(snapshot)
        except Exception:  # noqa: BLE001 - reporting must never fail the extraction
            pass

    def _relay(self, snapshot: List[Dict[str, Any]]) -> None:
        try:
            self._on_stage(snapshot)
        except Exception:  # noqa: BLE001
            pass


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
    stages: List[Dict[str, Any]] = field(default_factory=list)  # what each LLM stage did, see StageTracker


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
    on_stage: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
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
    `on_stage` (the Temporal activity passes its heartbeat) is called with the stage list (see StageTracker) after each
    stage change; the final list is `SubmissionOutcome.stages`, and `exc.stages` of an error raised after the calls
    began. The complete answers of the calls are stored with the session (not when it is flagged) and the answer of a
    call that was not usable is kept, cut, in its failed llm log.
    """
    if llm_function is None:
        from app.services.llm import openrouter_chat_completion
        llm_function = openrouter_chat_completion
    overrides = llm_overrides if llm_overrides is not None else {}
    started = _monotonic()   # the activity budget runs from here: the preparation below is part of it
    tracker = StageTracker(submission_id, on_stage, asyncio.get_running_loop())

    async with pool.acquire() as conn:
        if mark_processing:
            await update_submission_status(conn, submission_id, tenant_code, "processing")
        inputs = await load_submission_inputs(conn, submission_id, tenant_code)
        config: ExtractionConfig = inputs["config_row"]["config"]
        prompt = await load_prompt_for_config(conn, config)
        masking_name = config.pii_policy.masking_prompt_name
        masking_prompt = None if masking_name is None else await load_prompt_by_name(
            conn, masking_name, PII_MASKING_ANALYSIS_TYPE)
    llm = resolve_llm_settings(config, overrides)
    configured_timeout = llm["timeout"]
    if activity_budget_seconds is not None:
        llm["timeout"] = clamp_llm_timeout(llm["timeout"], activity_budget_seconds, _monotonic() - started)
    prompt_version_id = prompt["id"]
    config_version_id = inputs["config_row"]["id"]
    masking_version_id = None if masking_prompt is None else masking_prompt["id"]

    # One record per call: whether it was made and, when it came back, its usage (to log a call that worked even
    # when a later one failed; a masking call that worked before a failed extraction call still cost tokens).
    calls: Dict[str, Dict[str, Any]] = {
        "extraction": {"made": False, "usage": None}, "masking": {"made": False, "usage": None}}

    def make_call(purpose: str) -> Callable[[str], Tuple[str, Dict[str, Any]]]:
        def call_llm(text: str) -> Tuple[str, Dict[str, Any]]:
            timeout = llm["timeout"]
            if masking_prompt is not None and activity_budget_seconds is not None:
                # Two calls share one activity budget: each gets what is left of it, never more than configured.
                timeout = clamp_llm_timeout(configured_timeout, activity_budget_seconds, _monotonic() - started)
            calls[purpose]["made"] = True
            try:
                answer, usage = llm_function(text, model=llm["model"], max_tokens=llm["max_tokens"], timeout=timeout)
            except Exception as exc:  # noqa: BLE001
                failure = sanitize_llm_error(exc)
                failure.purpose = purpose
            else:
                calls[purpose]["usage"] = usage if isinstance(usage, dict) else {}
                return answer, usage
            # Raised outside the handler: no __cause__ / __context__, so the provider's error text (which can echo
            # the prompt) reaches neither llm_logs nor the Temporal failure history.
            raise failure
        return call_llm

    masking_step = None if masking_prompt is None else MaskingStep(
        masking_prompt["system_prompt"], masking_prompt["user_prompt"], make_call("masking"))

    async def log_call(purpose: str, error: Optional[Exception], usage: Any, response: Any = None) -> None:
        version = masking_version_id if purpose == "masking" else prompt_version_id
        await _log_failed_call(pool, submission_id, tenant_code, llm["model"], version, error, usage,
                               PII_MASKING_ANALYSIS_TYPE if purpose == "masking" else ANALYSIS_TYPE, response)

    try:
        run = await asyncio.to_thread(
            extract, inputs["rows"], config, prompt["system_prompt"], prompt["user_prompt"], make_call("extraction"),
            inputs["closing_keys"], masking_step, tracker.update, inputs["questionnaire_version"],
        )
    except Exception as exc:
        exc.stages = tracker.stages
        if write:
            failing = getattr(exc, "purpose", "extraction")
            if failing != "masking" and calls["masking"]["usage"] is not None:
                await log_call("masking", None, calls["masking"]["usage"])
            if calls[failing]["made"]:
                await log_call(failing, exc, getattr(exc, "usage", {}), getattr(exc, "response", None))
        raise
    if not run.llm_called:
        # No call was made (no respondent answer, an oversized message ...): say so, so the stage list is never empty.
        for skipped in ([STAGE_PII_MASKING] if masking_step is not None else []) + [STAGE_METADATA_EXTRACTION]:
            tracker.update(skipped, "skipped", reason=run.skip_reason)

    if write:
        llm_log = None
        if run.llm_called:
            prompt_tokens, completion_tokens, usage_meta = split_llm_usage(run.usage)
            llm_log = {
                "model_name": llm["model"], "analysis_type": ANALYSIS_TYPE, "prompt_version_id": str(prompt_version_id),
                "prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens, "status": "success",
                "meta_data": usage_meta if len(usage_meta) > 0 else None,
            }
        masking_log = None
        masking_stored = None
        if run.masking is not None:
            m_prompt_tokens, m_completion_tokens, m_usage_meta = split_llm_usage(run.masking.usage)
            masking_log = {
                "model_name": llm["model"], "analysis_type": PII_MASKING_ANALYSIS_TYPE,
                "prompt_version_id": str(masking_version_id), "prompt_tokens": m_prompt_tokens,
                "completion_tokens": m_completion_tokens, "status": "success",
                "meta_data": m_usage_meta if len(m_usage_meta) > 0 else None,
            }
            masking_stored = masking_meta(run.masking, masking_version_id)
        stale: Optional[StaleExtractionError] = None
        async with pool.acquire() as conn:
            try:
                await save_extraction(
                    conn, submission_id=submission_id, tenant_code=tenant_code,
                    input_fingerprint=inputs["fingerprint"], input_identity=inputs["identity"],
                    result=run.result, config_version_id=config_version_id, prompt_version_id=prompt_version_id,
                    run_at=inputs["loaded_at"], llm_log=llm_log,
                    masking=masking_stored, extra_llm_logs=() if masking_log is None else (masking_log,),
                    responses={"masking": None if run.masking is None else run.masking.response,
                               "extraction": run.response},
                    config=config,
                )
            except StaleExtractionError as exc:
                stale = exc
        if stale is not None:
            stale.stages = tracker.stages
            # Nothing was written (the transaction rolled back). The call still cost tokens: log it as failed.
            # This runs AFTER the connection above went back to the pool: _log_failed_call takes its own, and
            # holding one while waiting for another would deadlock a pool whose connections all hold a stale result.
            if run.llm_called:
                stale.usage = dict(run.usage)   # for the callers that total the usage of failed sessions (the CLI)
                if run.masking is not None:
                    stale.prior_usage = dict(run.masking.usage)
                if run.masking is not None:
                    await log_call("masking", None, run.masking.usage)
                await _log_failed_call(pool, submission_id, tenant_code, llm["model"], prompt_version_id, stale, run.usage)
            raise stale
    return SubmissionOutcome(
        submission_id, tenant_code, inputs["session_type"], inputs["questionnaire_version"], run, llm["model"],
        config_version_id, prompt_version_id, write, {f.name: (KIND_CATEGORY if len(f.bands) > 0 else f.kind) for f in config.fields}, prompt["name"],
        tracker.stages,
    )
