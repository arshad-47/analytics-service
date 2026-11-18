import json
import logging
import re
import urllib.parse
from typing import Dict, Any, Optional, List
import asyncpg
from datetime import datetime, timezone

from app.database.feedback_bot_versions import apply_bot_version_ranks, label_for_incoming_bot, lock_personas
from app.services.feedback_adapter_config import is_safe_code, is_safe_id, source_label_problem
from app.services.feedback_constants import (
    ADAPTERS,
    COMPLETION_COMPLETED,
    COMPLETION_STATUSES,
    FEEDBACK_REPORT_TYPE,
    MAX_SEQUENCE,
    NATURAL_KEY_ADAPTERS,
)
from app.services.feedback_versions import bot_id_problem
from app.services.ingestion_validation import is_uuid

logger = logging.getLogger("analytics_service.operations")

def _normalize_string_list(value: Any) -> Optional[str]:
    """Helper to convert list or string to database TEXT."""
    if value is None:
        return None
    if isinstance(value, list):
        return "\n".join(str(item) for item in value)
    return str(value)

def _normalize_statement_list(value: Any) -> Optional[List[str]]:
    """Helper to convert a value into a Postgres TEXT[] value — one array element
    per discrete statement. Used for discussion_submissions.challenges/solutions;
    thematic_activity.py and pii_and_abusive_activity.py (which also reads/writes
    this column) get back a native Python list with no decoding needed, since
    asyncpg handles the TEXT[] binding automatically in both directions.

    A native array is used instead of delimiter-joining a single TEXT value because
    a delimiter character (e.g. "|") occurring naturally inside one statement would
    otherwise corrupt the round-trip by splitting that statement into multiple
    fragments — a Postgres array has no such ambiguity."""
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]

def _normalize_url_list(value: Any) -> Optional[List[str]]:
    """Helper to convert list or single string URL to database TEXT[].
    Returns None when the key was genuinely absent (vs. explicitly empty), so
    partial updates can distinguish "not provided" from "cleared" via COALESCE."""
    if value is None:
        return None
    if isinstance(value, list):
        return [str(url) for url in value]
    if not value:
        return []
    return [str(value)]

def _strip_base_url(url: str) -> str:
    """Strips scheme+host from an absolute URL, keeping only the path (e.g. /bucket/folder/file.pdf)."""
    url_str = str(url).strip()
    parsed = urllib.parse.urlparse(url_str)
    if parsed.scheme and parsed.netloc:
        return parsed.path
    return url_str

def _normalize_media_url_list(value: Any) -> Optional[List[str]]:
    """Like _normalize_url_list, but stores only the path — never the base URL."""
    normalized = _normalize_url_list(value)
    return None if normalized is None else [_strip_base_url(url) for url in normalized]

def _normalize_pdf_urls(value: Any) -> tuple:
    """Handle pdfUrls in new object format {original, masked}.
    Returns (original_urls, masked_urls); each is None when absent so COALESCE
    preserves the existing column on partial updates."""
    if value is None:
        return None, None
    if isinstance(value, dict):
        original = _normalize_media_url_list(value.get("original"))
        masked = _normalize_media_url_list(value.get("masked"))
        return original, masked
    # Legacy fallback: plain string or list
    return _normalize_media_url_list(value), None

def clean_statement(text: str) -> str:
    """
    Cleans a raw statement by removing commas, question marks, and bracket
    characters, then collapsing any resulting extra whitespace.
    """
    text = re.sub(r"[,?\(\)\[\]\{\}]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def insert_statement_with_parent_check(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    submission_type: str,
    statement_type: str,
    raw_text: str,
) -> None:
    """
    Preserves the original raw_text in raw_statement and stores its cleaned form
    in cleaned_statement. Deduplication compares cleaned_statement to cleaned_statement
    (both punctuation-stripped) so minor formatting differences don't create false roots.

    Uses a check-before-insert pattern to avoid an extra UPDATE round-trip:
      old: INSERT → SELECT → UPDATE  (3 round-trips on duplicates)
      new: SELECT → INSERT           (2 round-trips max, 1 on uniques)
    """
    cleaned = clean_statement(raw_text)
    original = raw_text.strip()
    if not cleaned or not original:
        return

    # Dedup check against cleaned_statement — comparing cleaned-to-cleaned so
    # punctuation variants (trailing comma, brackets, etc.) are treated as the same statement.
    # - AND parent_id IS NULL ensures we only link to root statements, preventing deep chains.
    # - ORDER BY created_at ASC guarantees the oldest root is chosen when duplicates exist.
    existing_id = await conn.fetchval(
        """
        SELECT id
        FROM statements
        WHERE LOWER(cleaned_statement) = LOWER($1)
          AND parent_id IS NULL
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        """,
        cleaned,
    )

    # Insert with both original and cleaned text; parent_id pre-set if a duplicate root was found.
    new_id = await conn.fetchval(
        """
        INSERT INTO statements
            (submission_id, tenant_code, submission_type, statement_type, raw_statement, cleaned_statement, parent_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        """,
        str(submission_id), tenant_code, submission_type, statement_type, original, cleaned, existing_id,
    )

    if existing_id:
        logger.info(
            f"Statement [{new_id}] matched existing root statement [{existing_id}] — parent_id assigned at insert."
        )
    else:
        logger.info(f"Statement [{new_id}] stored with no duplicate found.")


class TenantOwnershipError(ValueError):
    """An event names a leader category / program id that already belongs to ANOTHER tenant."""


def _present(value: Any) -> bool:
    """A value the event really carries: not None and not the empty string (explicit, never truthiness)."""
    return value is not None and value != ""


def _no_row_written(command_tag: Any) -> bool:
    """True when an INSERT ... ON CONFLICT DO UPDATE ... WHERE ran but wrote no row (command tag 'INSERT 0 0'): the
    conflicting row exists and its WHERE (same tenant) was false."""
    return isinstance(command_tag, str) and command_tag.startswith("INSERT") and command_tag.endswith(" 0")


async def upsert_metadata(conn: asyncpg.Connection, tags: Dict[str, Any], tenant_code: str) -> tuple:
    """
    Safely upserts parent metadata tables: tenant, leader_category, and programs.
    Reads from the new top-level `tags` dict with flattened keys.
    Returns (program_id, leader_id) as UUIDs.

    Tenant isolation: an existing tenant is never renamed (a first event only creates it), and the conflict update of
    a leader category / program applies ONLY to a row of the same tenant. An event of tenant A that names an id owned
    by tenant B raises TenantOwnershipError (nothing of B is changed) instead of renaming B's rows.
    """
    # 1. Create the tenant if it is new; never rename an existing one.
    tenant_name = tenant_code.capitalize()
    await conn.execute(
        """
        INSERT INTO tenant (name, code)
        VALUES ($1, $2)
        ON CONFLICT (code) DO NOTHING
        """,
        tenant_name, tenant_code
    )

    leader_id = None
    program_id = None

    # 2. Upsert Leader Category (from flattened tags)
    leader_id = tags.get("leaderCategoryId")
    if _present(leader_id):
        result = await conn.execute(
            """
            INSERT INTO leader_category (id, name, description, tenant_code)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                updated_at = now()
            WHERE leader_category.tenant_code = EXCLUDED.tenant_code
            """,
            leader_id,
            tags.get("leaderCategoryName", ""),
            None,  # TODO: description can be added once the PM provides it
            tenant_code
        )
        if _no_row_written(result):
            raise TenantOwnershipError(
                f"leaderCategoryId {leader_id} belongs to another tenant than {tenant_code!r}; rejecting the event."
            )

    # 3. Upsert Programs (depends on leader category)
    program_id = tags.get("programId")
    if _present(program_id) and not _present(leader_id):
        # No leader category in the event, so the program is not written (programs.leaders_id is NOT NULL). The id is
        # still stored on the submission, and submissions.program_id has no tenant in its foreign key: an id that
        # belongs to ANOTHER tenant must be refused here, exactly as the upsert below refuses it. A program that does
        # not exist yet keeps the old behaviour (the id is returned unchanged).
        owner = await conn.fetchrow("SELECT tenant_code FROM programs WHERE id = $1", program_id)
        if owner is not None and owner["tenant_code"] != tenant_code:
            raise TenantOwnershipError(
                f"programId {program_id} belongs to another tenant than {tenant_code!r}; rejecting the event."
            )
    if _present(program_id) and _present(leader_id):
        result = await conn.execute(
            """
            INSERT INTO programs (id, leaders_id, name, description, tenant_code)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE SET
                leaders_id = EXCLUDED.leaders_id,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                updated_at = now()
            WHERE programs.tenant_code = EXCLUDED.tenant_code
            """,
            program_id,
            leader_id,
            tags.get("programName", ""),
            None,  # TODO: description can be added once the PM provides it
            tenant_code
        )
        if _no_row_written(result):
            raise TenantOwnershipError(
                f"programId {program_id} belongs to another tenant than {tenant_code!r}; rejecting the event."
            )

    return program_id, leader_id

async def upsert_participant_metrics(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    submission_type: str,
    participants_data: List[Dict[str, Any]],
) -> None:
    """
    Upserts role-based participant counts from Kafka `participantsData` into the
    dynamic KPI metrics tables (metric_definitions + submission_metrics).
    participants_data is always a full snapshot (not a delta), so existing metrics
    for this submission are cleared and replaced rather than merged.
    """
    await conn.execute(
        "DELETE FROM submission_metrics WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant_code
    )

    for entry in participants_data:
        role = entry.get("role")
        count = entry.get("count")
        if not role or count is None:
            continue
        metric_code = str(role).strip()

        await conn.execute(
            """
            INSERT INTO metric_definitions (code, label, value_type, submission_type, is_active)
            VALUES ($1, $2, 'numeric', $3, TRUE)
            ON CONFLICT (code) DO UPDATE SET
                label = EXCLUDED.label,
                value_type = EXCLUDED.value_type,
                submission_type = EXCLUDED.submission_type,
                is_active = EXCLUDED.is_active
            """,
            metric_code, metric_code, submission_type
        )

        await conn.execute(
            """
            INSERT INTO submission_metrics (submission_id, tenant_code, metric_code, numeric_value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (submission_id, tenant_code, metric_code) DO UPDATE SET
                numeric_value = EXCLUDED.numeric_value
            """,
            submission_id, tenant_code, metric_code, int(count)
        )

async def delete_submission(conn: asyncpg.Connection, submission_id: str, tenant_code: str) -> bool:
    """
    Deletes a submission and cascades down to specific table rows.
    """
    result = await conn.execute(
        "DELETE FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant_code
    )
    # result string format: e.g. "DELETE 1"
    deleted = result.startswith("DELETE") and not result.endswith("0")
    if deleted:
        logger.info(f"Deleted submission {submission_id} under tenant {tenant_code}")
    else:
        logger.warning(f"Submission {submission_id} not found under tenant {tenant_code} for deletion")
    return deleted

class FeedbackUpdateNotSupportedError(ValueError):
    """Raised for an `update` event on a feedback submission (not supported in this release)."""


def is_feedback_type(submission_type: Any) -> bool:
    """True when `submission_type` selects the feedback branch (same normalisation as the story/discussion checks)."""
    return isinstance(submission_type, str) and FEEDBACK_REPORT_TYPE in submission_type.lower().strip()


class _FeedbackAlreadyStored(Exception):
    """Internal signal: a natural-key (wide_form) response with this source_ref is already stored. Raised inside the
    transaction so everything written so far rolls back, then handled as a no-op by the caller."""


def _is_source_ref_violation(error: asyncpg.exceptions.UniqueViolationError) -> bool:
    """True for a unique violation on feedback_submissions (tenant_code, source_ref)."""
    return error.table_name == "feedback_submissions" and "source_ref" in (error.constraint_name or "")


async def _find_stored_gforms_response(
    conn: asyncpg.Connection, tenant_code: str, source_ref: str
) -> Optional[Dict[str, Any]]:
    """The stored submission of a natural-key (wide_form) response, looked up by its natural key (None if new)."""
    return await conn.fetchrow(
        """
        SELECT s.id, s.submission_id, s.tenant_code, s.status
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.tenant_code = $1 AND fs.source_ref = $2
        """,
        tenant_code, source_ref,
    )


def _already_stored_result(stored: Dict[str, Any]) -> Dict[str, Any]:
    """The unchanged result for a natural-key response that is already stored (nothing was written)."""
    return {
        "id": stored["id"],
        "submission_id": stored["submission_id"],
        "tenant_code": stored["tenant_code"],
        "status": stored["status"],
        "changed": False,
        "messages_inserted": 0,
        "messages_updated": 0,
        "parents_set": 0,
    }


def _parse_feedback_timestamp(value: Any, field: str) -> Optional[datetime]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 string or null, got {type(value).__name__}")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as e:
        raise ValueError(f"{field} is not a valid ISO-8601 timestamp: {value!r}") from e


def _optional_text(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string or null, got {type(value).__name__}")
    return value


def _parse_feedback_messages(raw_messages: Any) -> List[Dict[str, Any]]:
    """
    Boundary re-check of data.messages for the feedback branch (the ingestion schema validator is
    the primary gate; this keeps the DB writes safe if an event ever reaches them unvalidated).
    Returns the messages ordered by sequence. Raises ValueError with the offending index on any problem.
    """
    if not isinstance(raw_messages, list) or len(raw_messages) == 0:
        raise ValueError("data.messages must be a non-empty list")
    parsed: List[Dict[str, Any]] = []
    seen_sequences = set()
    for idx, raw in enumerate(raw_messages):
        where = f"data.messages[{idx}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{where} must be an object, got {type(raw).__name__}")
        sequence = raw.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1 or sequence > MAX_SEQUENCE:
            raise ValueError(f"{where}.sequence must be an integer from 1 to {MAX_SEQUENCE}")
        if sequence in seen_sequences:
            raise ValueError(f"{where}.sequence {sequence} appears more than once in the event")
        seen_sequences.add(sequence)
        message = raw.get("message")
        if not isinstance(message, str) or len(message.strip()) == 0:
            raise ValueError(f"{where}.message must be a non-empty string (sequence {sequence})")
        is_follow_up = raw.get("isFollowUp", False)
        if not isinstance(is_follow_up, bool):
            raise ValueError(f"{where}.isFollowUp must be a boolean, got {is_follow_up!r}")
        parent_sequence = raw.get("parentSequence")
        if parent_sequence is not None:
            if isinstance(parent_sequence, bool) or not isinstance(parent_sequence, int):
                raise ValueError(f"{where}.parentSequence must be an integer or null")
            if parent_sequence >= sequence:
                raise ValueError(
                    f"{where}.parentSequence {parent_sequence} must be lower than its own sequence {sequence}"
                )
        parsed.append({
            "sequence": sequence,
            "question_key": _optional_text(raw.get("questionKey"), f"{where}.questionKey"),
            "is_follow_up": is_follow_up,
            "message": message,
            "translated_message": _optional_text(raw.get("translatedMessage"), f"{where}.translatedMessage"),
            "message_at": _parse_feedback_timestamp(raw.get("messageAt"), f"{where}.messageAt"),
            "parent_sequence": parent_sequence,
        })
    parsed.sort(key=lambda m: m["sequence"])
    return parsed


async def _ingest_feedback_submission(
    conn: asyncpg.Connection,
    event_payload: Dict[str, Any],
    event_type: str,
) -> Dict[str, Any]:
    """
    Feedback branch of insert_or_update_submission. One transaction:
      1. submissions upsert (conditional: an identical re-delivery rewrites nothing)
      2. feedback_submissions upsert (a COMPLETED session is never downgraded to IN_PROGRESS)
      3. feedbacks upsert by (submission_id, tenant_code, sequence); existing rows are rewritten only when
         a stored field actually changed, so ids are kept and updated_at moves only on real changes
      4. parent_id pass from parentSequence (idempotent; null parentSequence leaves parent_id untouched)
    Both `create` (a first upload, or a re-upload of a grown session) and a repeated event take this path:
    a create on an existing session reconciles instead of being skipped.
    A source is a free label (the consumer already tied the event to an active upload config of that source). The
    identity rules come from data.adapter, the adapter of that config: a natural-key adapter (wide_form) identifies a
    response by data.sourceRef (feedback_submissions.source_ref, unique per tenant) and its responses are immutable: a
    response whose sourceRef is already stored is a no-op that returns the stored submission (whatever submissionId
    the event carries), and this also covers a concurrent writer that stored it first (the unique violation rolls
    this event back and is treated as already stored). Such an event never updates an existing submission. For every
    other adapter (conversation_rows) sourceRef must be absent and a session grows by id.
    Returns the usual dict plus `changed` (False when nothing was written) and per-table counts.
    """
    submission_id = str(event_payload["submissionId"])
    tenant_code = event_payload["tenantCode"]
    submission_type = event_payload["submissionType"]
    session_id = event_payload.get("sessionId")
    tags = event_payload.get("tags", {})
    if not is_safe_id(submission_id) or not is_safe_code(tenant_code):
        raise ValueError("feedback event has an unsafe submissionId or tenantCode; rejecting it.")
    ident = f"submission {submission_id} (tenant {tenant_code}, session {session_id})"

    if event_type != "create":
        raise FeedbackUpdateNotSupportedError(
            f"eventType {event_type!r} is not supported for feedback submissions in this release "
            f"(only create and delete); rejecting {ident}."
        )
    if not isinstance(session_id, str) or len(session_id.strip()) == 0:
        raise ValueError(f"feedback event requires a non-empty sessionId; rejecting {ident}.")
    if not is_safe_id(session_id):
        raise ValueError(f"feedback event has an unsafe sessionId; rejecting submission {submission_id}.")

    data = event_payload.get("data", {})
    source = data.get("source")
    source_problem = source_label_problem(source)
    if source_problem is not None:
        raise ValueError(f"data.source is invalid ({source_problem}); rejecting {ident}.")
    adapter = data.get("adapter")
    if adapter not in ADAPTERS:
        raise ValueError(f"data.adapter must be one of {ADAPTERS}; rejecting {ident}.")
    is_natural_key = adapter in NATURAL_KEY_ADAPTERS
    completion_status = data.get("completionStatus")
    if completion_status not in COMPLETION_STATUSES:
        raise ValueError(
            f"data.completionStatus must be one of {COMPLETION_STATUSES}, got {completion_status!r}; rejecting {ident}."
        )
    language = _optional_text(data.get("language"), "data.language")
    session_type = data.get("sessionType")
    if not isinstance(session_type, str) or len(session_type.strip()) == 0:
        raise ValueError(f"data.sessionType must be a non-empty string; rejecting {ident}.")
    questionnaire_version = data.get("questionnaireVersion")
    if not isinstance(questionnaire_version, str) or len(questionnaire_version.strip()) == 0:
        raise ValueError(f"data.questionnaireVersion must be a non-empty string; rejecting {ident}.")
    # The bot of a Mitra session (adapter conversation_rows) decides its questionnaire version: the stored version is
    # the rank of its bot id among the bot ids of the persona, so the bot id is REQUIRED there and must be a positive
    # integer written in digits (a numeric order must be well defined). Rejected here, before anything is written.
    # A natural-key (wide_form) event carries no bot; botName is informational.
    if not is_natural_key:
        bot_problem = bot_id_problem(data.get("botId"))
        if bot_problem is not None:
            raise ValueError(f"data.botId is invalid for adapter {adapter} ({bot_problem}); rejecting {ident}.")
    bot_info = {
        key: value.strip()
        for key, value in (("bot_id", data.get("botId")), ("bot_name", data.get("botName")))
        if isinstance(value, str) and len(value.strip()) > 0
    }
    source_ref = data.get("sourceRef")
    if is_natural_key:
        if not isinstance(source_ref, str) or len(source_ref.strip()) == 0:
            raise ValueError(f"data.sourceRef must be a non-empty string for adapter {adapter}; rejecting {ident}.")
        if submission_id != session_id:
            raise ValueError(
                f"an event of adapter {adapter} must carry the same value as submissionId and sessionId; rejecting {ident}."
            )
    elif source_ref is not None:
        raise ValueError(f"data.sourceRef must be absent or null for adapter {adapter}; rejecting {ident}.")
    messages = _parse_feedback_messages(data.get("messages"))

    submission_date = _parse_feedback_timestamp(data.get("submissionDate"), "data.submissionDate")
    if submission_date is None:
        submission_date = datetime.now(timezone.utc)
    state = tags.get("state")
    district = tags.get("district")

    if is_natural_key:
        # Already stored (by natural key)? Then there is nothing to do, whatever submissionId this event carries.
        stored = await _find_stored_gforms_response(conn, tenant_code, source_ref)
        if stored is not None:
            logger.info(
                f"Natural-key (wide_form) response already stored as submission {stored['submission_id']} under tenant "
                f"{tenant_code}; ignoring {ident}."
            )
            return _already_stored_result(stored)
        # A new response must get a new submission: reusing an existing id would modify a stored one.
        id_taken = await conn.fetchval(
            "SELECT 1 FROM submissions WHERE submission_id = $1 AND tenant_code = $2", submission_id, tenant_code
        )
        if id_taken is not None:
            raise ValueError(
                f"submission {submission_id} already exists under tenant {tenant_code} with a different "
                f"sourceRef; an event of adapter {adapter} never updates an existing submission; rejecting {ident}."
            )

    try:
        return await _write_feedback_submission(
            conn, event_payload, ident, source, source_ref, language, completion_status, messages,
            submission_date, state, district, session_type.strip(), questionnaire_version.strip(), bot_info,
            is_natural_key,
        )
    except _FeedbackAlreadyStored:
        pass
    except asyncpg.exceptions.UniqueViolationError as e:
        if not (is_natural_key and _is_source_ref_violation(e)):
            raise
    # Only reached when a concurrent writer stored the same response first (the transaction was rolled back).
    stored = await _find_stored_gforms_response(conn, tenant_code, source_ref)
    if stored is None:
        raise ValueError(f"{adapter} response {source_ref!r} conflicted but is not stored; rejecting {ident}.")
    logger.info(
        f"Natural-key (wide_form) response lost a write race: already stored as submission {stored['submission_id']} "
        f"under tenant {tenant_code}; ignoring {ident}."
    )
    return _already_stored_result(stored)


def _persona_key(session_type: Any, source: Any, program_id: Any) -> tuple:
    """What selects the extraction config of a session: session_type, source, program (a UUID in any spelling)."""
    return (session_type, source, None if program_id is None else str(program_id).lower())


async def _stored_persona(conn: asyncpg.Connection, submission_id: str, tenant_code: str) -> Optional[tuple]:
    """
    _persona_key of the stored feedback session, or None when it is not stored yet. Locks the submissions row, then the
    feedback_submissions row, in the order of the extraction save (feedback_extraction_operations.save_extraction).
    """
    program_text = await conn.fetchval(
        "SELECT program_id::text FROM submissions WHERE submission_id = $1 AND tenant_code = $2 FOR NO KEY UPDATE",
        submission_id, tenant_code,
    )
    stored = await conn.fetchval(
        "SELECT json_build_array(session_type, source)::text FROM feedback_submissions "
        "WHERE submission_id = $1 AND tenant_code = $2 FOR UPDATE",
        submission_id, tenant_code,
    )
    if stored is None:
        return None
    session_type, source = json.loads(stored)
    return _persona_key(session_type, source, program_text)


async def _write_feedback_submission(
    conn: asyncpg.Connection,
    event_payload: Dict[str, Any],
    ident: str,
    source: str,
    source_ref: Optional[str],
    language: Optional[str],
    completion_status: str,
    messages: List[Dict[str, Any]],
    submission_date: datetime,
    state: Any,
    district: Any,
    session_type: str,
    questionnaire_version: str,
    bot_info: Dict[str, str],
    is_natural_key: bool,
) -> Dict[str, Any]:
    """The transactional write of _ingest_feedback_submission (see there); one transaction, all or nothing."""
    submission_id = str(event_payload["submissionId"])
    tenant_code = event_payload["tenantCode"]
    submission_type = event_payload["submissionType"]
    session_id = event_payload.get("sessionId")
    tags = event_payload.get("tags", {})

    async with conn.transaction():
        # A Mitra bot session's questionnaire version is the rank of its bot id inside its persona (tenant, program,
        # session_type, source). The persona advisory lock is the FIRST thing this transaction takes, before the metadata
        # upserts, any row lock and the read of the bot ids, so two concurrent ingests of two new bots of one persona can
        # not each rank without the other and no ingest can wait on the lock while holding a row another one needs (see
        # app/database/feedback_bot_versions.py). A session that moves persona changes the ranks of its OLD persona too,
        # so both are locked, in key order.
        bot_id = None if is_natural_key else bot_info["bot_id"]
        stored_version_before = None
        old_bot_persona = None
        if bot_id is not None:
            event_program = tags.get("programId")
            event_program = event_program.strip().lower() if isinstance(event_program, str) and len(event_program.strip()) > 0 else None
            if event_program is not None and not is_uuid(event_program):
                raise ValueError(f"tags.programId must be a UUID; rejecting {ident}.")
            before = await conn.fetchrow(
                """
                SELECT fs.session_type, fs.source, fs.questionnaire_version, s.program_id::text AS program_id,
                       (fs.meta_data ->> 'bot_id') IS NOT NULL AS has_bot
                FROM feedback_submissions fs
                JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
                WHERE fs.submission_id = $1 AND fs.tenant_code = $2
                """,
                submission_id, tenant_code,
            )
            # the stored program stays when the event names none (the submissions upsert below COALESCEs)
            effective_program = event_program if event_program is not None else (None if before is None else before["program_id"])
            personas_to_lock = [(tenant_code, effective_program, session_type, source)]
            if before is not None:
                stored_version_before = before["questionnaire_version"]
                if before["has_bot"]:
                    old_bot_persona = (tenant_code, before["program_id"], before["session_type"], before["source"])
                    personas_to_lock.append(old_bot_persona)
            await lock_personas(conn, personas_to_lock)

        program_id, leader_id = await upsert_metadata(conn, tags, tenant_code)

        if bot_id is not None:
            questionnaire_version = await label_for_incoming_bot(
                conn, tenant_code, effective_program, session_type, source, submission_id, bot_id
            )

        # The persona the stored session had before this event (session_type, source, program): it selects the
        # extraction config, prompt and closing keys. Read under the row locks in the order the extraction save takes
        # them (submissions, then feedback_submissions), so a persona change below is seen exactly once.
        persona_before = await _stored_persona(conn, submission_id, tenant_code)

        # 1. submissions. Unlike story/discussion this must not reset status/updated_at on every conflict,
        # so the update is conditional and keeps identity fields (session_id, submission_date) once set.
        try:
            sub_row = await conn.fetchrow(
                """
                INSERT INTO submissions (
                    session_id, submission_id, tenant_code, submission_type,
                    state, district, submission_date, program_id, leader_id, status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending')
                ON CONFLICT (submission_id, tenant_code) DO UPDATE SET
                    session_id = COALESCE(submissions.session_id, EXCLUDED.session_id),
                    state = COALESCE(EXCLUDED.state, submissions.state),
                    district = COALESCE(EXCLUDED.district, submissions.district),
                    submission_date = COALESCE(submissions.submission_date, EXCLUDED.submission_date),
                    program_id = COALESCE(EXCLUDED.program_id, submissions.program_id),
                    leader_id = COALESCE(EXCLUDED.leader_id, submissions.leader_id),
                    updated_at = now()
                WHERE submissions.session_id IS NULL
                   OR (EXCLUDED.state IS NOT NULL AND EXCLUDED.state IS DISTINCT FROM submissions.state)
                   OR (EXCLUDED.district IS NOT NULL AND EXCLUDED.district IS DISTINCT FROM submissions.district)
                   OR submissions.submission_date IS NULL
                   OR (EXCLUDED.program_id IS NOT NULL AND EXCLUDED.program_id IS DISTINCT FROM submissions.program_id)
                   OR (EXCLUDED.leader_id IS NOT NULL AND EXCLUDED.leader_id IS DISTINCT FROM submissions.leader_id)
                RETURNING id, status, session_id, (xmax = 0) AS inserted
                """,
                session_id, submission_id, tenant_code, submission_type,
                state, district, submission_date, program_id, leader_id,
            )
        except asyncpg.exceptions.UniqueViolationError as e:
            if e.constraint_name == "submissions_session_id_key":
                raise ValueError(
                    f"session_id {session_id!r} is already associated with a different submission; "
                    f"rejecting {ident}."
                ) from e
            raise
        submission_changed = sub_row is not None
        if sub_row is None:
            sub_row = await conn.fetchrow(
                "SELECT id, status, session_id, FALSE AS inserted FROM submissions "
                "WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code,
            )
        if sub_row["session_id"] != session_id:
            raise ValueError(
                f"submission {submission_id} under tenant {tenant_code} already belongs to session "
                f"{sub_row['session_id']!r}, not {session_id!r}; rejecting the event."
            )
        db_sub_uuid = sub_row["id"]
        db_sub_status = sub_row["status"]

        # 2. feedback_submissions. COMPLETED is terminal: an incoming IN_PROGRESS (a stale or out-of-order
        # event) never downgrades it. source/language are refreshed; language is kept when the event has none.
        fs_row = await conn.fetchrow(
            """
            INSERT INTO feedback_submissions (
                submission_id, tenant_code, source, session_type, questionnaire_version,
                language, completion_status, source_ref, meta_data
            )
            VALUES ($1, $2, $3, $8, $9, $4, $5, $7, $10::jsonb)
            ON CONFLICT (submission_id, tenant_code) DO UPDATE SET
                source = EXCLUDED.source,
                session_type = EXCLUDED.session_type,
                questionnaire_version = EXCLUDED.questionnaire_version,
                language = COALESCE(EXCLUDED.language, feedback_submissions.language),
                completion_status = CASE
                    WHEN feedback_submissions.completion_status = $6 THEN feedback_submissions.completion_status
                    ELSE EXCLUDED.completion_status END,
                meta_data = COALESCE(feedback_submissions.meta_data, '{}'::jsonb) || COALESCE(EXCLUDED.meta_data, '{}'::jsonb),
                updated_at = now()
            WHERE feedback_submissions.source IS DISTINCT FROM EXCLUDED.source
               OR feedback_submissions.session_type IS DISTINCT FROM EXCLUDED.session_type
               OR feedback_submissions.questionnaire_version IS DISTINCT FROM EXCLUDED.questionnaire_version
               OR COALESCE(EXCLUDED.language, feedback_submissions.language) IS DISTINCT FROM feedback_submissions.language
               OR (feedback_submissions.completion_status <> $6
                   AND feedback_submissions.completion_status IS DISTINCT FROM EXCLUDED.completion_status)
               OR (COALESCE(feedback_submissions.meta_data, '{}'::jsonb) || COALESCE(EXCLUDED.meta_data, '{}'::jsonb))
                  IS DISTINCT FROM COALESCE(feedback_submissions.meta_data, '{}'::jsonb)
            RETURNING completion_status, (xmax = 0) AS inserted
            """,
            submission_id, tenant_code, source, language, completion_status, COMPLETION_COMPLETED, source_ref,
            session_type, questionnaire_version, json.dumps(bot_info) if len(bot_info) > 0 else None,
        )
        if is_natural_key and (fs_row is None or not fs_row["inserted"]):
            # Natural-key (wide_form) responses are immutable: a payload row that already existed (a concurrent writer
            # stored this response between the pre-check and here) is never touched. Roll everything back.
            raise _FeedbackAlreadyStored()
        payload_changed = fs_row is not None

        # 3. feedbacks: one batched upsert. The WHERE leaves unchanged rows untouched (ids and updated_at kept).
        upserted = await conn.fetch(
            """
            INSERT INTO feedbacks (
                submission_id, tenant_code, session_id, program_id, source, submission_type,
                sequence, is_follow_up, question_key, message, translated_message, language, message_at
            )
            SELECT $1, $2, $3, $4::uuid, $5, $6,
                   t.sequence, t.is_follow_up, t.question_key, t.message, t.translated_message, $7, t.message_at
            FROM unnest(
                $8::int[], $9::boolean[], $10::text[], $11::text[], $12::text[], $13::timestamptz[]
            ) AS t(sequence, is_follow_up, question_key, message, translated_message, message_at)
            ON CONFLICT (submission_id, tenant_code, sequence) DO UPDATE SET
                is_follow_up = EXCLUDED.is_follow_up,
                question_key = EXCLUDED.question_key,
                message = EXCLUDED.message,
                translated_message = EXCLUDED.translated_message,
                language = EXCLUDED.language,
                message_at = EXCLUDED.message_at,
                updated_at = now()
            WHERE feedbacks.is_follow_up IS DISTINCT FROM EXCLUDED.is_follow_up
               OR feedbacks.question_key IS DISTINCT FROM EXCLUDED.question_key
               OR feedbacks.message IS DISTINCT FROM EXCLUDED.message
               OR feedbacks.translated_message IS DISTINCT FROM EXCLUDED.translated_message
               OR feedbacks.language IS DISTINCT FROM EXCLUDED.language
               OR feedbacks.message_at IS DISTINCT FROM EXCLUDED.message_at
            RETURNING sequence, (xmax = 0) AS inserted
            """,
            submission_id, tenant_code, session_id, program_id, source, FEEDBACK_REPORT_TYPE, language,
            [m["sequence"] for m in messages],
            [m["is_follow_up"] for m in messages],
            [m["question_key"] for m in messages],
            [m["message"] for m in messages],
            [m["translated_message"] for m in messages],
            [m["message_at"] for m in messages],
        )
        messages_inserted = sum(1 for r in upserted if r["inserted"])
        messages_updated = len(upserted) - messages_inserted

        # 4. parent links. A parentSequence must resolve inside this session (parents may come from this
        # event or from rows stored by an earlier upload); an unresolvable one rolls the whole event back.
        with_parent = [m for m in messages if m["parent_sequence"] is not None]
        parents_set = 0
        if len(with_parent) > 0:
            child_seqs = [m["sequence"] for m in with_parent]
            parent_seqs = [m["parent_sequence"] for m in with_parent]
            unresolved = await conn.fetch(
                """
                SELECT m.sequence, m.parent_sequence
                FROM unnest($3::int[], $4::int[]) AS m(sequence, parent_sequence)
                LEFT JOIN feedbacks p
                       ON p.submission_id = $1 AND p.tenant_code = $2 AND p.sequence = m.parent_sequence
                WHERE p.id IS NULL
                """,
                submission_id, tenant_code, child_seqs, parent_seqs,
            )
            if len(unresolved) > 0:
                missing = ", ".join(f"{r['sequence']}->{r['parent_sequence']}" for r in unresolved)
                raise ValueError(
                    f"feedback messages reference a parentSequence that does not exist "
                    f"(sequence->parentSequence: {missing}); rejecting {ident}."
                )
            parent_rows = await conn.fetch(
                """
                UPDATE feedbacks f SET parent_id = p.id, updated_at = now()
                FROM unnest($3::int[], $4::int[]) AS m(sequence, parent_sequence)
                JOIN feedbacks p
                  ON p.submission_id = $1 AND p.tenant_code = $2 AND p.sequence = m.parent_sequence
                WHERE f.submission_id = $1 AND f.tenant_code = $2 AND f.sequence = m.sequence
                  AND f.parent_id IS DISTINCT FROM p.id
                RETURNING f.id
                """,
                submission_id, tenant_code, child_seqs, parent_seqs,
            )
            parents_set = len(parent_rows)

        persona_changed = False
        if persona_before is not None:
            program_now = await conn.fetchval(
                "SELECT program_id FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code,
            )
            persona_changed = persona_before != _persona_key(session_type, source, program_now)
        # The bot rank of the session itself changed (a redelivery with another bot id, or a lower bot id among the
        # other sessions): what the older label's extraction wrote no longer describes it either.
        version_changed = stored_version_before is not None and stored_version_before != questionnaire_version
        if persona_changed or version_changed:
            # The session moved to another persona (a redelivery that changed only session_type, source or program):
            # what the OLD persona's extraction wrote no longer describes it. Drop its extracted fields and its
            # extraction record, and go back to "not checked" (the PII flag), in this same transaction; the session
            # goes back through processing below (the feedback_submissions row changed), so the new persona's
            # extraction runs from scratch. llm_logs are an audit trail and stay.
            await conn.execute(
                "DELETE FROM feedback_extracted_data WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code,
            )
            await conn.execute(
                "UPDATE feedback_submissions SET pii_detected = NULL, abusive_language = NULL, "
                "meta_data = meta_data - 'extraction' "
                "WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code,
            )
            # The state / district copied from the old extraction describe the old persona's answers: cleared,
            # the new extraction writes them again (config `copy_to_submission`).
            await conn.execute(
                "UPDATE submissions SET state = NULL, district = NULL "
                "WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code,
            )
        elif len(upserted) > 0 or parents_set > 0:
            # New or edited messages, or a changed parent link (a bot row can become a respondent row): the PII check
            # of the older text no longer describes the session. Back to "not checked" (NULL) until the extraction
            # runs again, so an export never trusts a stale flag.
            await conn.execute(
                "UPDATE feedback_submissions SET pii_detected = NULL, abusive_language = NULL "
                "WHERE submission_id = $1 AND tenant_code = $2 "
                "AND (pii_detected IS NOT NULL OR abusive_language IS NOT NULL)",
                submission_id, tenant_code,
            )

        # The ranks of the persona(s) after this session: relabel every stored session whose label changed (a new bot id
        # lower than stored ones shifts them) and send it back through processing; the old persona of a session that
        # moved loses that session's bot id, which can shift its ranks too.
        restamped: List[str] = []
        reset: List[str] = []
        if bot_id is not None:
            program_final = await conn.fetchval(
                "SELECT program_id::text FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code,
            )
            personas = [(tenant_code, program_final, session_type, source)]
            if old_bot_persona is not None and old_bot_persona != personas[0]:
                personas.append(old_bot_persona)
            for persona in personas:
                applied = await apply_bot_version_ranks(conn, *persona)
                restamped.extend(applied["restamped"])
                reset.extend(applied["reset"])

        changed = submission_changed or payload_changed or len(upserted) > 0 or parents_set > 0
        if changed and db_sub_status != "pending":
            # New content: send the submission back through processing (same effect as a story update).
            await conn.execute(
                "UPDATE submissions SET status = 'pending', updated_at = now() "
                "WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code,
            )
            db_sub_status = "pending"

    logger.info(
        f"Successfully ingested {submission_type} submission {submission_id} under tenant {tenant_code}: "
        f"changed={changed}, messages_inserted={messages_inserted}, messages_updated={messages_updated}, "
        f"parents_set={parents_set}"
    )
    return {
        "id": db_sub_uuid,
        "submission_id": submission_id,
        "tenant_code": tenant_code,
        "status": db_sub_status,
        "changed": changed,
        "messages_inserted": messages_inserted,
        "messages_updated": messages_updated,
        "parents_set": parents_set,
        # other stored sessions of the persona whose questionnaire version moved with the bot ranks
        "restamped_submission_ids": [sid for sid in restamped if sid != submission_id],
        # the subset of those that was SET BACK TO 'pending' (a field is asked in one version only, so the stored
        # extraction no longer describes it): the consumer dispatches exactly these after the commit. The others only
        # got a new label and keep their extraction and status.
        "reset_submission_ids": [sid for sid in reset if sid != submission_id],
    }


async def insert_or_update_submission(
    conn: asyncpg.Connection,
    event_payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Performs transactional write for submission master and type-specific tables.
    Handles the new Kafka event format with top-level `tags` and delta-only `newValues`.
    """
    submission_id = str(event_payload["submissionId"])
    tenant_code = event_payload["tenantCode"]
    submission_type = event_payload["submissionType"]
    session_id = event_payload.get("sessionId")
    event_type = event_payload.get("eventType", "create").lower().strip()
    tags = event_payload.get("tags", {})

    if is_feedback_type(submission_type):
        return await _ingest_feedback_submission(conn, event_payload, event_type)

    # Resolve the data payload based on event type.
    # For updates, newValues is delta-only — do NOT merge in oldValues. Downstream
    # activities (e.g. pii_and_abusive_activity) overwrite these same columns with
    # masked text after ingestion; filling gaps from the producer's raw oldValues
    # would silently revert already-masked fields. Absent fields stay None here and
    # are preserved via COALESCE in the SQL below, leaving them untouched in the DB.
    if event_type == "update":
        data = event_payload.get("newValues", {})
    else:
        data = event_payload.get("data", {})

    # Start database transaction if not already handled
    async with conn.transaction():
        # Upsert parent metadata tables (reads from flattened tags)
        program_id, leader_id = await upsert_metadata(conn, tags, tenant_code)

        # Parse submission date (report created at).
        # Both discussion and story submissions use data.submissionDate for report
        # created at. This field is only present on create events (absent from
        # partial update payloads), so COALESCE in the SQL below preserves the
        # original DB value on updates — no special-casing needed.
        sub_date_str = data.get("submissionDate")

        if sub_date_str:
            submission_date = datetime.fromisoformat(sub_date_str.replace("Z", "+00:00"))
        elif event_type != "update":
            submission_date = datetime.now(timezone.utc)
        else:
            submission_date = None

        # Read state/district/organization from tags (new format)
        state = tags.get("state")
        district = tags.get("district")
        organization = tags.get("organization")

        # 1. Upsert Master Submission record
        # Note: status is initialized as 'pending' for new creates
        try:
            submission_uuid_row = await conn.fetchrow(
                """
                INSERT INTO submissions (
                    session_id, submission_id, tenant_code, submission_type, user_id, user_name, role,
                    state, district, organization, submission_date, program_id, leader_id, status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (submission_id, tenant_code) DO UPDATE SET
                    session_id = COALESCE(EXCLUDED.session_id, submissions.session_id),
                    user_id = COALESCE(EXCLUDED.user_id, submissions.user_id),
                    user_name = COALESCE(EXCLUDED.user_name, submissions.user_name),
                    role = COALESCE(EXCLUDED.role, submissions.role),
                    state = COALESCE(EXCLUDED.state, submissions.state),
                    district = COALESCE(EXCLUDED.district, submissions.district),
                    organization = COALESCE(EXCLUDED.organization, submissions.organization),
                    submission_date = COALESCE(EXCLUDED.submission_date, submissions.submission_date),
                    program_id = COALESCE(EXCLUDED.program_id, submissions.program_id),
                    leader_id = COALESCE(EXCLUDED.leader_id, submissions.leader_id),
                    status = EXCLUDED.status,
                    updated_at = now()
                RETURNING id, status;
                """,
                session_id,
                submission_id,
                tenant_code,
                submission_type,
                data.get("userId"),
                data.get("userName"),
                data.get("designation"), # Designation maps to role
                state,
                district,
                organization,
                submission_date,
                program_id,
                leader_id,
                "pending"
            )
        except asyncpg.exceptions.UniqueViolationError as e:
            if e.constraint_name == "submissions_session_id_key":
                raise ValueError(
                    f"session_id {session_id!r} is already associated with a different submission; "
                    f"rejecting submission {submission_id} under tenant {tenant_code}."
                ) from e
            raise

        db_sub_uuid = submission_uuid_row["id"]
        db_sub_status = submission_uuid_row["status"]

        # 2. Upsert specific payload type
        normalized_type = submission_type.lower().strip()
        if "story" in normalized_type:
            # Upsert story submission
            # Update if exists, else insert
            row_exists = await conn.fetchval(
                "SELECT 1 FROM story_submissions WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code
            )
            challenges_joined = _normalize_statement_list(data.get("challenges"))
            action_steps_joined = _normalize_statement_list(data.get("actionSteps"))
            image_urls = _normalize_media_url_list(data.get("imageUrls"))
            pdf_urls, masked_pdf_urls = _normalize_pdf_urls(data.get("pdfUrls"))

            if row_exists:
                await conn.execute(
                    """
                    UPDATE story_submissions SET
                        title = COALESCE($3, title),
                        objective = COALESCE($4, objective),
                        challenge = COALESCE($5, challenge),
                        action_steps = COALESCE($6, action_steps),
                        impact = COALESCE($7, impact),
                        duration = COALESCE($8, duration),
                        blurb = COALESCE($9, blurb),
                        content = COALESCE($10, content),
                        image_urls = COALESCE($11, image_urls),
                        pdf_urls = COALESCE($12, pdf_urls),
                        masked_pdf_urls = COALESCE($13, masked_pdf_urls),
                        transcript_link = COALESCE($14, transcript_link),
                        updated_at = now()
                    WHERE submission_id = $1 AND tenant_code = $2
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    data.get("objective"),
                    challenges_joined,
                    action_steps_joined,
                    data.get("impact"),
                    data.get("duration"),
                    data.get("blurb"),
                    data.get("content"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink")
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO story_submissions (
                        submission_id, tenant_code, title, objective, challenge, action_steps,
                        impact, duration, blurb, content, image_urls, pdf_urls, masked_pdf_urls, transcript_link
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    data.get("objective"),
                    challenges_joined,
                    action_steps_joined,
                    data.get("impact"),
                    data.get("duration"),
                    data.get("blurb"),
                    data.get("content"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink")
                )

            # Extract challenges for story submission into statements table
            raw_story_challenges = data.get("challenges")
            if isinstance(raw_story_challenges, list):
                await conn.execute(
                    "DELETE FROM statements WHERE submission_id = $1 AND tenant_code = $2 AND statement_type = $3",
                    submission_id, tenant_code, "challenge"
                )
                for raw_challenge in raw_story_challenges:
                    if raw_challenge:
                        await insert_statement_with_parent_check(
                            conn,
                            submission_id,
                            tenant_code,
                            submission_type=submission_type,
                            statement_type="challenge",
                            raw_text=str(raw_challenge),
                        )

        elif "discussion" in normalized_type:
            # Upsert discussion submission
            row_exists = await conn.fetchval(
                "SELECT 1 FROM discussion_submissions WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code
            )
            challenges_joined = _normalize_statement_list(data.get("challenges"))
            solutions_joined = _normalize_statement_list(data.get("solutions"))
            image_urls = _normalize_media_url_list(data.get("imageUrls"))
            pdf_urls, masked_pdf_urls = _normalize_pdf_urls(data.get("pdfUrls"))

            # Parse discussion date (date the discussion took place) from discussionDate.
            # This is distinct from submissionDate (report created at) which is stored
            # in the parent submissions table.
            disc_date_str = data.get("discussionDate")
            discussion_date = (
                datetime.fromisoformat(disc_date_str.replace("Z", "+00:00"))
                if disc_date_str else None
            )

            # Extract PRI member info (nested object or legacy flat keys)
            pri_info = (
                data.get("pri_member_information")
                or data.get("priMemberInformation")
                or data.get("pri_member_info")
                or data.get("priMemberInfo")
            )
            if isinstance(pri_info, dict):
                pri_member_name = pri_info.get("name") or data.get("priMemberName")
                pri_member_designation = pri_info.get("designation") or data.get("priMemberDesignation")
            else:
                pri_member_name = data.get("priMemberName")
                pri_member_designation = data.get("priMemberDesignation")

            # Extract School Representative info (nested object or legacy flat keys)
            school_info = (
                data.get("school_representative_information")
                or data.get("schoolRepresentativeInformation")
                or data.get("school_representative_info")
                or data.get("schoolRepresentativeInfo")
            )
            if isinstance(school_info, dict):
                school_rep_name = school_info.get("name") or data.get("schoolRepresentativeName")
                school_rep_designation = school_info.get("designation") or data.get("schoolRepresentativeDesignation")
            else:
                school_rep_name = data.get("schoolRepresentativeName")
                school_rep_designation = data.get("schoolRepresentativeDesignation")

            if row_exists:
                await conn.execute(
                    """
                    UPDATE discussion_submissions SET
                        title = COALESCE($3, title),
                        challenges = COALESCE($4, challenges),
                        solutions = COALESCE($5, solutions),
                        author = COALESCE($6, author),
                        language = COALESCE($7, language),
                        image_urls = COALESCE($8, image_urls),
                        pdf_urls = COALESCE($9, pdf_urls),
                        masked_pdf_urls = COALESCE($10, masked_pdf_urls),
                        transcript_link = COALESCE($11, transcript_link),
                        discussion_date = COALESCE($12, discussion_date),
                        pri_member_name = COALESCE($13, pri_member_name),
                        pri_member_designation = COALESCE($14, pri_member_designation),
                        school_representative_name = COALESCE($15, school_representative_name),
                        school_representative_designation = COALESCE($16, school_representative_designation),
                        updated_at = now()
                    WHERE submission_id = $1 AND tenant_code = $2
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    challenges_joined,
                    solutions_joined,
                    data.get("author"),
                    data.get("language"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink"),
                    discussion_date,
                    pri_member_name,
                    pri_member_designation,
                    school_rep_name,
                    school_rep_designation,
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO discussion_submissions (
                        submission_id, tenant_code, title, challenges, solutions,
                        author, language, image_urls, pdf_urls, masked_pdf_urls, transcript_link, discussion_date,
                        pri_member_name, pri_member_designation, school_representative_name, school_representative_designation
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    challenges_joined,
                    solutions_joined,
                    data.get("author"),
                    data.get("language"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink"),
                    discussion_date,
                    pri_member_name,
                    pri_member_designation,
                    school_rep_name,
                    school_rep_designation,
                )

            # Dynamic KPI metrics: participantsData is a full snapshot when present;
            # absent on a partial update means it didn't change, so leave existing
            # submission_metrics rows untouched.
            participants_data = data.get("participantsData")
            if participants_data is not None:
                await upsert_participant_metrics(
                    conn, submission_id, tenant_code, submission_type, participants_data
                )

            # Extract challenges and solutions for discussion submission into statements table
            raw_challenges = data.get("challenges")
            raw_solutions = data.get("solutions")
            
            if isinstance(raw_challenges, list):
                await conn.execute(
                    "DELETE FROM statements WHERE submission_id = $1 AND tenant_code = $2 AND statement_type = $3",
                    submission_id, tenant_code, "challenge"
                )
                for raw_challenge in raw_challenges:
                    if raw_challenge:
                        await insert_statement_with_parent_check(
                            conn,
                            submission_id,
                            tenant_code,
                            submission_type=submission_type,
                            statement_type="challenge",
                            raw_text=str(raw_challenge),
                        )

            if isinstance(raw_solutions, list):
                await conn.execute(
                    "DELETE FROM statements WHERE submission_id = $1 AND tenant_code = $2 AND statement_type = $3",
                    submission_id, tenant_code, "solution"
                )
                for raw_solution in raw_solutions:
                    if raw_solution:
                        await insert_statement_with_parent_check(
                            conn,
                            submission_id,
                            tenant_code,
                            submission_type=submission_type,
                            statement_type="solution",
                            raw_text=str(raw_solution),
                        )

        logger.info(f"Successfully ingested {submission_type} submission {submission_id} under tenant {tenant_code}")
        return {
            "id": db_sub_uuid,
            "submission_id": submission_id,
            "tenant_code": tenant_code,
            "status": db_sub_status
        }

async def update_submission_status(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    status: str,
    process_status: Optional[Dict[str, Any]] = None
) -> None:
    """
    Updates the execution status and status metadata on the master submissions table.
    """
    if process_status:
        process_status_json = json.dumps(process_status)
        await conn.execute(
            """
            UPDATE submissions 
            SET status = $3, process_status = $4, updated_at = now()
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code, status, process_status_json
        )
    else:
        await conn.execute(
            """
            UPDATE submissions 
            SET status = $3, updated_at = now()
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code, status
        )

async def mark_pending_submission_config_failed(
    conn: asyncpg.Connection, submission_id: str, tenant_code: str, reason: str
) -> bool:
    """
    A feedback submission whose persona config cannot give its process steps is marked 'failed' with the reason in its
    process_status (no retry_attempt is recorded, so the bulk retry picks it up once the config is fixed) instead of
    staying 'pending': a pending row that can never be resolved would keep returning at the head of every batch
    sweep and starve the other submissions. Identifiers and the config problem only, no content.

    The write only applies to a row that is STILL 'pending'. The caller read it as pending and then awaited the config
    lookup; in that window the Kafka consumer or a workflow may have moved the row to 'processing' (or it finished),
    and a blind write would replace that state with 'failed', which a later retry sweep would take for a dead row and
    start a second run on top of the live one. Returns True when the row was marked, False when it was no longer
    pending (nothing is written then). Nothing legitimately replaces a non-pending row here: the consumer calls this
    for a row it has just stored as pending, and the pending-queue sweep only selects pending rows.
    """
    process_status = {
        "status": "failed",
        "error_message": f"Cannot resolve the process steps of the persona config: {reason}",
        "steps": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "failed_at_step": None,
    }
    result = await conn.execute(
        """
        UPDATE submissions
        SET status = 'failed', process_status = $3, updated_at = now()
        WHERE submission_id = $1 AND tenant_code = $2 AND status = 'pending'
        """,
        submission_id, tenant_code, json.dumps(process_status),
    )
    return result == "UPDATE 1"


async def insert_llm_log(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    model_name: str,
    analysis_type: str,
    prompt_version_id: str,
    prompt_tokens: int,
    completion_tokens: int,
    status: str,
    error_message: Optional[str] = None,
    meta_data: Optional[Dict[str, Any]] = None
) -> None:
    """
    Logs metadata about individual LLM executions to the database for token tracking.
    """
    meta_json = json.dumps(meta_data) if meta_data else None
    await conn.execute(
        """
        INSERT INTO llm_logs (
            submission_id, tenant_code, model_name, analysis_type, prompt_version_id,
            prompt_tokens, completion_tokens, status, error_message, meta_data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        submission_id,
        tenant_code,
        model_name,
        analysis_type,
        prompt_version_id,
        prompt_tokens,
        completion_tokens,
        status,
        error_message,
        meta_json
    )

async def insert_analysis_result(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    analysis_type: str,
    statement_id: Optional[Any] = None,
    analysis_column: Optional[List[str]] = None,
    statement_type: Optional[str] = None,
    ml_model_name: Optional[str] = None,
    ml_model_version: Optional[str] = None,
    model_confidence_score: Optional[float] = None,
    confidence_score: Optional[float] = None,
    model_prediction: Optional[str] = None,
    llm_confidence_score: Optional[float] = None,
    llm_prediction: Optional[str] = None,
    threshold: Optional[float] = None,
    theme_id: Optional[Any] = None,
    justification: Optional[str] = None,
    multi_theme_mapped: bool = False,
    category_type: Optional[str] = None,
    meta_data: Optional[Dict[str, Any]] = None,
    improvement_environment: Optional[str] = None,
) -> None:
    """
    Single unified database function to insert rows into analysis_results.
    Handles all analysis types:
      - statement_category (SetFit + LLM fallback)
      - thematic classification
      - environmental & future analysis steps
    Supports alias mapping (e.g. statement_type -> analysis_column, similarity_score -> model_confidence_score).
    """
    # Map statement_type to analysis_column if analysis_column not explicitly passed
    if analysis_column is None and statement_type is not None:
        analysis_column = [statement_type]
        
    if llm_prediction is None and improvement_environment is not None:
        llm_prediction = improvement_environment

    # Map legacy confidence score to both model_confidence_score and llm_confidence_score only if explicitly not set
    if model_confidence_score is None and confidence_score is not None:
        model_confidence_score = confidence_score
        
    if llm_confidence_score is None and confidence_score is not None:
        llm_confidence_score = confidence_score
    # NOTE: similarity_score (cosine embedding similarity) is intentionally NOT aliased
    # to model_confidence_score — they are distinct concepts.

    meta_json = json.dumps(meta_data) if meta_data else None

    await conn.execute(
        """
        INSERT INTO analysis_results (
            submission_id, tenant_code, statement_id,
            analysis_type, analysis_column,
            ml_model_name, ml_model_version,
            model_confidence_score, model_prediction,
            llm_confidence_score, llm_prediction,
            threshold, theme_id,
            justification, multi_theme_mapped,
            category_type, meta_data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """,
        str(submission_id),
        tenant_code,
        str(statement_id) if statement_id else None,
        analysis_type,
        analysis_column,
        ml_model_name,
        ml_model_version,
        model_confidence_score,
        model_prediction,
        llm_confidence_score,
        llm_prediction,
        threshold,
        str(theme_id) if theme_id else None,
        justification,
        multi_theme_mapped,
        category_type,
        meta_json,
    )


async def insert_ranking_result(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    criteria_data: Dict[str, Any],
    composite_score: float,
    tier: Optional[str],
    overall_summary: Optional[str],
    meta_data: Optional[Dict[str, Any]] = None
) -> None:
    """
    Saves LLM-generated story rating/ranking output to the ranking table.
    """
    await conn.execute(
        """
        INSERT INTO ranking (
            submission_id, tenant_code, criteria_data, composite_score, tier, overall_summary, meta_data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        submission_id,
        tenant_code,
        json.dumps(criteria_data),
        composite_score,
        tier,
        overall_summary,
        json.dumps(meta_data) if meta_data else None
    )


async def get_submission_type_and_payload(conn: asyncpg.Connection, submission_id: str, tenant_code: str) -> tuple:
    """
    Retrieves the submission type and payload details for story or discussion submissions.
    """
    sub_row = await conn.fetchrow(
        "SELECT submission_type FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant_code
    )
    if not sub_row:
        raise ValueError(f"Submission {submission_id} not found in database.")
    
    sub_type = sub_row["submission_type"].lower().strip()
    
    if "story" in sub_type:
        payload_row = await conn.fetchrow(
            """
            SELECT title, objective, challenge, action_steps, impact, duration, blurb, content, image_urls, pdf_urls, pii_masked_at, abusive_masked_at
            FROM story_submissions
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code
        )
    elif "discussion" in sub_type:
        payload_row = await conn.fetchrow(
            """
            SELECT title, challenges, solutions, author, language, image_urls, pii_masked_at, abusive_masked_at 
            FROM discussion_submissions 
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code
        )
    else:
        raise ValueError(f"Unsupported submission type: {sub_type}")

    if not payload_row:
        raise ValueError(f"Payload details not found for {submission_id} under type {sub_type}.")

    return sub_type, dict(payload_row)

# CSV Upload Tracking (csv_uploads)
async def check_duplicate_file(
    program_name: str,
    leader_category: str,
    report_type: str,
    file_name: str,
    file_size: int,
) -> bool:
    """Return True if a matching file already exists in the tracker."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT 1 FROM csv_uploads
            WHERE program_name = $1
              AND leader_category = $2
              AND report_type = $3
              AND file_name = $4
              AND file_size = $5
              AND status != 'failed'
            LIMIT 1
            """,
            program_name,
            leader_category,
            report_type,
            file_name,
            file_size,
        )
        return exists is not None


async def insert_upload_record(
    report_type: str,
    program_name: str,
    leader_category: str,
    cloud_storage_path: str,
    file_name: Optional[str] = None,
    file_size: Optional[int] = None,
    meta_data: Optional[Dict[str, Any]] = None,
    status: str = "pending",
) -> int:
    """Insert a new row with the given status. Returns the new row's id."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO csv_uploads
                    (report_type, program_name, leader_category, cloud_storage_path,
                     file_name, file_size, meta_data, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                RETURNING id
                """,
                report_type,
                program_name,
                leader_category,
                cloud_storage_path,
                file_name,
                file_size,
                json.dumps(meta_data or {}),
                status,
            )
            record_id = row["id"]
            logger.info("Inserted csv_uploads record %s (status=%s)", record_id, status)
            return record_id
        except asyncpg.UniqueViolationError:
            from app.api.exceptions import DuplicateFile
            raise DuplicateFile("FILE ALREADY EXISTS")


async def get_record(record_id: int) -> Optional[dict]:
    """Fetch a single tracker record by id."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM csv_uploads WHERE id = $1",
            record_id,
        )
        return dict(row) if row else None


async def update_status(
    record_id: int,
    status: str,
    meta_data: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Update status, optionally merging new keys into meta_data.
    """
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        if meta_data is not None:
            await conn.execute(
                """
                UPDATE csv_uploads
                SET status = $1,
                    meta_data = COALESCE(meta_data, '{}'::jsonb) || $2::jsonb
                WHERE id = $3
                """,
                status,
                json.dumps(meta_data),
                record_id,
            )
        else:
            await conn.execute(
                "UPDATE csv_uploads SET status = $1 WHERE id = $2",
                status,
                record_id,
            )

async def list_by_status(status: str) -> list:
    """List all tracker records with a given status."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM csv_uploads WHERE status = $1 ORDER BY created_at",
            status,
        )
        return [dict(r) for r in rows]


async def try_claim_for_processing(record_id: int) -> Optional[str]:
    """
    Atomically set status to 'in_progress' if record exists and its status is not 'in_progress'.
    Returns:
      - 'success' if successfully claimed/updated.
      - 'in_progress' if it is already in progress.
      - None if the record does not exist.
    """
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE csv_uploads
            SET status = 'in_progress'
            WHERE id = $1 AND status != 'in_progress'
            RETURNING status
            """,
            record_id,
        )
        if row:
            logger.info("Atomically claimed record %s for processing", record_id)
            return "success"

        # If update did not match any row, determine if it doesn't exist or is in_progress
        exists = await conn.fetchval(
            "SELECT 1 FROM csv_uploads WHERE id = $1 LIMIT 1",
            record_id,
        )
        if not exists:
            return None
        return "in_progress"


async def fetch_statements_for_submission(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
) -> List[Dict[str, Any]]:
    """
    Fetches all original (non-duplicate) statements for a given submission.
    Skips rows that have a parent_id set (i.e., duplicates).
    Returns rows ordered by creation time.
    """
    rows = await conn.fetch(
        """
        SELECT id, raw_statement, statement_type, submission_type
        FROM statements s
        WHERE s.submission_id = $1
          AND s.tenant_code = $2
          AND s.parent_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM analysis_results existing
              WHERE existing.statement_id = s.id
                AND existing.analysis_type = 'statement_category'
          )
        ORDER BY s.created_at ASC
        """,
        str(submission_id), tenant_code
    )
    return [dict(row) for row in rows]


async def fetch_challenge_and_solution_statements_for_submission(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
) -> List[Dict[str, Any]]:
    """
    Returns statements that the statement_category step classified as 'Challenge' or 'Solution or Action',
    ready for thematic classification.

    Effective-category resolution rule (applied in SQL):
      - If llm_prediction IS NOT NULL  → use llm_prediction
      - Else                           → use model_prediction
    Only rows where the effective category is 'Challenge' or 'Solution or Action' are returned.

    Each row contains:
      statement_id  (UUID, FK into statements)
      raw_statement (the cleaned text to classify)
      statement_type ('challenge' / 'solution' / …)
    """
    rows = await conn.fetch(
        """
        SELECT
            ar.statement_id,
            s.raw_statement,
            s.statement_type,
            s.submission_type
        FROM analysis_results ar
        JOIN statements s ON s.id = ar.statement_id
        WHERE ar.submission_id  = $1
          AND ar.tenant_code    = $2
          AND ar.analysis_type  = 'statement_category'
          AND s.parent_id IS NULL
          AND (
              (ar.llm_prediction IS NULL     AND LOWER(ar.model_prediction) IN ('challenge', 'solution or action'))
           OR (ar.llm_prediction IS NOT NULL AND LOWER(ar.llm_prediction)   IN ('challenge', 'solution or action'))
          )
          AND NOT EXISTS (
              SELECT 1 FROM analysis_results existing
              WHERE existing.statement_id = s.id
                AND existing.analysis_type = 'thematic_classification'
          )
        ORDER BY ar.created_at ASC
        """,
        str(submission_id), tenant_code,
    )
    return [dict(row) for row in rows]

async def reclaim_stale_in_progress(stale_minutes: int = 30) -> int:
    """
    Reset any csv_uploads rows that have been stuck at status='in_progress'
    for longer than stale_minutes back to 'pending' so they can be retried.

    A record can get stuck when the process that called process_csv_inline was
    killed (OOM, pod restart, deploy) before it could write a terminal status.
    Without this reclaim, POST /v1/process/csv/{id} returns 409 forever on
    those records.

    Returns the number of rows reclaimed (0 if none).
    """
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE csv_uploads
            SET status = 'pending',
                meta_data = jsonb_set(
                    COALESCE(meta_data, '{}')::jsonb,
                    '{reclaimed_at}',
                    to_jsonb(now()::text)
                )
            WHERE status = 'in_progress'
              AND updated_at < NOW() - ($1::integer * interval '1 minute')
            """,
            stale_minutes,
        )
    # asyncpg returns "UPDATE N" as a string
    try:
        return int(result.split()[-1])
    except (AttributeError, ValueError, IndexError):
        return 0

async def copy_parent_analysis_results(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    analysis_type: str,
) -> int:
    """
    Copies analysis_results from parent statements to child statements within a submission.
    This effectively "pulls" historical AI classification results forward when a new
    submission contains statements that were deduplicated against an older submission.
    """
    query = """
        INSERT INTO analysis_results (
            submission_id, tenant_code, statement_id, analysis_type, analysis_column,
            ml_model_name, ml_model_version, model_confidence_score, model_prediction,
            llm_confidence_score, llm_prediction, threshold, theme_id,
            justification, multi_theme_mapped, category_type, meta_data
        )
        SELECT 
            $1 AS submission_id,
            $2 AS tenant_code,
            child.id AS statement_id,
            ar.analysis_type,
            ARRAY[child.statement_type] AS analysis_column,
            ar.ml_model_name,
            ar.ml_model_version,
            ar.model_confidence_score,
            ar.model_prediction,
            ar.llm_confidence_score,
            ar.llm_prediction,
            ar.threshold,
            ar.theme_id,
            ar.justification,
            ar.multi_theme_mapped,
            ar.category_type,
            jsonb_build_object('deduped_from', child.parent_id::text) AS meta_data
        FROM statements child
        JOIN analysis_results ar ON ar.statement_id = child.parent_id
        WHERE child.submission_id = $1
          AND child.tenant_code = $2
          AND child.parent_id IS NOT NULL
          AND ar.analysis_type = $3
          AND NOT EXISTS (
              SELECT 1 FROM analysis_results existing
              WHERE existing.statement_id = child.id
                AND existing.analysis_type = $3
          )
    """
    result = await conn.execute(query, submission_id, tenant_code, analysis_type)
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


def _as_datetime(value: Any) -> Any:
    """An ISO-8601 string (a trailing 'Z' means UTC, which datetime.fromisoformat only accepts from Python 3.11) as a
    datetime; anything else (a datetime, None) unchanged."""
    if isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    return value


async def fetch_retryable_submissions(
    conn: asyncpg.Connection,
    tenant_code: Optional[str] = None,
    limit: int = None,
    failed_before: Optional[Any] = None,
    cursor_updated_at: Optional[Any] = None,
    cursor_submission_id: Optional[str] = None,
    max_retry_attempts: int = 5,
    stuck_processing_min_age_minutes: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches retryable submissions (status = 'failed', or stuck status = 'processing'
    older than stuck_processing_min_age_minutes) using keyset pagination.
    Only submissions with fewer than max_retry_attempts recorded retries are returned.
    """
    if limit is None:
        raise ValueError("limit must be provided by the caller; do not rely on a hardcoded default.")
    if stuck_processing_min_age_minutes is None:
        from app.config import settings
        stuck_processing_min_age_minutes = settings.STUCK_PROCESSING_MIN_AGE_MINUTES

    # The cutoff and the keyset cursor may arrive as ISO strings (an API / JSON hand-off); asyncpg only accepts a
    # datetime for a timestamptz parameter.
    cutoff_dt = _as_datetime(failed_before)
    cursor_dt = _as_datetime(cursor_updated_at)
    rows = await conn.fetch(
        """
        SELECT submission_id, session_id, tenant_code, submission_type, status,
               process_status, updated_at
        FROM submissions
        WHERE (
            status = 'failed'
            OR (status = 'processing' AND updated_at < now() - make_interval(mins => $7::int))
          )
          AND ($1::text IS NULL OR tenant_code = $1)
          AND ($3::timestamptz IS NULL OR updated_at < $3)
          AND (
              $4::timestamptz IS NULL
              OR (updated_at, submission_id) > ($4, $5)
          )
          AND (process_status->>'retry_attempt' IS NULL OR (process_status->>'retry_attempt')::int < $6)
        ORDER BY updated_at ASC, submission_id ASC
        LIMIT $2
        """,
        tenant_code,
        limit,
        cutoff_dt,
        cursor_dt,
        cursor_submission_id,
        max_retry_attempts,
        stuck_processing_min_age_minutes,
    )
    return [dict(row) for row in rows]


async def fetch_retryable_submission_for_retry(
    conn: asyncpg.Connection,
    submission_id: str,
    session_id: str,
    tenant_code: str,
) -> Optional[Dict[str, Any]]:
    """
    Returns the single submission row needed to build a step-retry payload,
    or None if no matching row exists.
    Only columns required for retry dispatch are selected.
    """
    row = await conn.fetchrow(
        """
        SELECT submission_id, session_id, tenant_code, submission_type, status, process_status, updated_at
        FROM submissions
        WHERE submission_id = $1
          AND session_id    = $2
          AND tenant_code   = $3
        """,
        submission_id,
        session_id,
        tenant_code,
    )
    return dict(row) if row else None


async def claim_submission_for_retry(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
) -> Optional[Dict[str, Any]]:
    """
    Atomically claims a submission for retry by incrementing
    process_status->>'retry_attempt' and touching updated_at.
    Guarded to only claim submissions with status in ('failed', 'processing')
    and retry_attempt < 5.
    Returns dict with 'retry_attempt' and updated 'process_status', or None if rejected.
    """
    row = await conn.fetchrow(
        """
        UPDATE submissions
        SET updated_at = now(),
            process_status = jsonb_set(
                COALESCE(process_status, '{}'::jsonb),
                '{retry_attempt}',
                to_jsonb(COALESCE((process_status->>'retry_attempt')::int, 0) + 1)
            )
        WHERE submission_id = $1
          AND tenant_code = $2
          AND status IN ('failed', 'processing')
          AND COALESCE((process_status->>'retry_attempt')::int, 0) < 5
        RETURNING (process_status->>'retry_attempt')::int AS retry_attempt, process_status;
        """,
        submission_id,
        tenant_code,
    )
    if not row:
        return None

    ps = row["process_status"]
    if isinstance(ps, str):
        try:
            ps = json.loads(ps)
        except Exception:
            ps = {}
    return {
        "retry_attempt": row["retry_attempt"] or 1,
        "process_status": ps or {},
    }


async def rollback_submission_claim(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
) -> None:
    """
    Rolls back / decrements retry_attempt if start_workflow fails due to WorkflowAlreadyStartedError,
    preventing in-flight workflows from prematurely burning one of their 5 retry attempts.
    """
    await conn.execute(
        """
        UPDATE submissions
        SET process_status = jsonb_set(
            COALESCE(process_status, '{}'::jsonb),
            '{retry_attempt}',
            to_jsonb(GREATEST(COALESCE((process_status->>'retry_attempt')::int, 1) - 1, 0))
        )
        WHERE submission_id = $1 AND tenant_code = $2;
        """,
        submission_id,
        tenant_code,
    )


async def reconcile_completed_submission(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
) -> None:
    """
    Reconciles a submission stuck in 'processing' whose Temporal workflow has COMPLETED.
    Updates DB status to 'success'.
    """
    await conn.execute(
        """
        UPDATE submissions
        SET status = 'success',
            process_status = jsonb_set(
                COALESCE(process_status, '{}'::jsonb),
                '{status}',
                '"success"'::jsonb
            ),
            updated_at = now()
        WHERE submission_id = $1 AND tenant_code = $2;
        """,
        submission_id,
        tenant_code,
    )

