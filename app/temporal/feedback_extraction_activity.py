import logging
from typing import Any, Dict, List, Optional

from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.database.db import db
from app.database.feedback_extraction_operations import (
    ExtractionSetupError,
    LlmCallError,
    StaleExtractionError,
    _safe_error_text,
    extract_submission,
)
from app.services.feedback_extraction import ExtractionConfigError, ExtractionOutputError, PromptRenderError
from app.services.feedback_extraction_constants import ANALYSIS_TYPE

logger = logging.getLogger("analytics_service.temporal.feedback_extraction")

# A provider answer with one of these statuses is worth another attempt; every other 4xx (bad request, bad key,
# model not found, content refused) will answer the same way again and each attempt is a paid call.
_RETRYABLE_HTTP_STATUSES = frozenset({408, 409, 425, 429})
# A bad model answer (not JSON, wrong shape) is retried ONCE: a second sample may be fine, a third rarely is.
_OUTPUT_ERROR_MAX_ATTEMPTS = 2


def classify_failure(exc: BaseException, attempt: int) -> Optional[ApplicationError]:
    """
    None when Temporal's retry policy may retry `exc`; otherwise a NON-retryable ApplicationError carrying the
    failure's type and its safe text (no respondent or provider text; see feedback_extraction_operations).

    Deterministic, retrying cannot help: a malformed or missing extraction config, an unknown prompt variable, a
    missing prompt, a session that is not stored, an activity budget too small for any LLM call, a 4xx answer of the
    provider other than 408/409/425/429, and a bad model answer on the second attempt. Retryable: provider 5xx and
    429, timeouts and connection errors, a session that grew during the call (StaleExtractionError), the first bad
    model answer, and ExtractionBudgetExhaustedError (a slow database stall used the activity's budget before the
    call: transient, no call was made, a fresh attempt gets a fresh budget).
    """
    deterministic = isinstance(exc, (ExtractionConfigError, PromptRenderError, ExtractionSetupError, ValueError))
    if isinstance(exc, LlmCallError):
        deterministic = exc.status is not None and 400 <= exc.status < 500 and exc.status not in _RETRYABLE_HTTP_STATUSES
    elif isinstance(exc, ExtractionOutputError):
        deterministic = attempt >= _OUTPUT_ERROR_MAX_ATTEMPTS
    elif isinstance(exc, StaleExtractionError):
        deterministic = False
    if not deterministic:
        return None
    text = _safe_error_text(exc) if isinstance(exc, (ExtractionOutputError, LlmCallError, StaleExtractionError)) \
        else f"{type(exc).__name__}: {str(exc)[:300]}"
    return ApplicationError(text, type=type(exc).__name__, non_retryable=True)


def _activity_budget_seconds() -> Optional[int]:
    """The start_to_close_timeout this activity attempt runs under, None outside a Temporal activity (tests, CLI)."""
    try:
        timeout = activity.info().start_to_close_timeout
    except RuntimeError:
        return None
    return int(timeout.total_seconds()) if timeout is not None else None


def _report_stages(stages: List[Dict[str, Any]]) -> None:
    """
    The stage list as this attempt's heartbeat details: the Temporal UI shows them on the pending activity while it
    runs (and between retries), so "pii_masking success, metadata_extraction running" is visible live. Temporal throttles
    heartbeats, so a very short stage can be coalesced; the complete list is in the activity's result. Outside a
    Temporal activity (tests, the CLI) this does nothing.
    """
    try:
        activity.heartbeat({"stages": stages})
    except RuntimeError:
        pass


def _failed_stage(exc: BaseException) -> Optional[str]:
    stages = getattr(exc, "stages", None)
    if not isinstance(stages, list):
        return None
    return next((s.get("name") for s in reversed(stages) if isinstance(s, dict) and s.get("status") == "failed"), None)


def _attempt() -> int:
    try:
        return int(activity.info().attempt)
    except RuntimeError:
        return 1


@activity.defn
async def feedback_metadata_extraction_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity of the `feedback_metadata_extraction` workflow step.

    One LLM call per stored feedback session (Mitra or Google Forms alike) receives the complete conversation and
    returns the DOTS metadata fields (state, district, gender, school_type ...) plus a whole-conversation PII
    flag. The field rows go to feedback_extracted_data and the flag to feedback_submissions.pii_detected, in one
    transaction together with the llm log; a re-run overwrites. The core is
    app.database.feedback_extraction_operations.extract_submission, shared with tools/run_feedback_extraction.py.

    params: submission_id, tenant_code and the optional per-step overrides llm_model / max_tokens /
    llm_timeout_seconds (None = use the extraction config's llm settings, then the global .env settings).
    The activity's time limit is sized by the workflow from the step's llm_timeout_seconds or the global setting
    (workflow code cannot read the database); a longer `llm.timeout_seconds` of the extraction config is CLAMPED to
    the budget minus the margin and minus the time the preparation already used (a WARNING names both numbers)
    instead of failing the run; when the preparation used up the budget the step is retried. To give a longer call
    a bigger budget, set llm_timeout_seconds on the workflow step.
    Inside this ONE activity the stages run in order: the optional model-based PII / abusive-language masking
    (`pii_masking`), then the extraction (`metadata_extraction`). Temporal shows a single activity, so the stage view
    is: heartbeat details while it runs, `stages` in the result when it completes, the failed stage in the error text
    and details, and the same list in the workflow step's entry of submissions.process_status.
    A conversation without a respondent answer makes no LLM call. Like the other steps this does not set the
    submission's terminal status; the workflow does that once every step has completed.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    analysis_type = params.get("analysis_type")
    if analysis_type is not None and analysis_type != ANALYSIS_TYPE:
        raise ValueError(f"feedback_metadata_extraction_activity cannot run analysis_type {analysis_type!r}")
    overrides = {
        "llm_model": params.get("llm_model"),
        "max_tokens": params.get("max_tokens"),
        "llm_timeout_seconds": params.get("llm_timeout_seconds"),
    }
    try:
        outcome = await extract_submission(
            db.pool, submission_id, tenant_code,
            llm_overrides=overrides, write=True, mark_processing=True,
            activity_budget_seconds=_activity_budget_seconds(),
            on_stage=_report_stages,
        )
    except Exception as exc:
        # Never the message of an extraction error at INFO or above beyond its type: it is built without
        # respondent text, but the provider's error body is not ours to repeat.
        failed_stage = _failed_stage(exc)
        logger.error("Feedback metadata extraction failed for submission %s%s: %s", submission_id,
                     "" if failed_stage is None else f" in stage {failed_stage}", type(exc).__name__)
        # The workflow's own exception handler marks the step and the submission failed once this propagates.
        # A failure that retrying cannot fix is raised as non-retryable: no second and third paid attempt. Its text
        # names the stage that failed and its details carry the stage list (both visible in the Temporal UI).
        non_retryable = classify_failure(exc, _attempt())
        if non_retryable is not None:
            stages = getattr(exc, "stages", None)
            text = non_retryable.message if failed_stage is None else f"{non_retryable.message} (stage: {failed_stage})"
            raise ApplicationError(text, {"stages": stages} if isinstance(stages, list) else {},
                                   type=non_retryable.type, non_retryable=True) from None
        raise

    result = outcome.run.result
    return {
        "status": "success",
        "llm_called": outcome.run.llm_called,
        "pii_detected": result.pii_detected,
        "fields": {f.field_name: f.status for f in result.fields},
        # One entry per LLM stage (pii_masking when configured, then metadata_extraction): status, seconds, tokens.
        "stages": outcome.stages,
    }
