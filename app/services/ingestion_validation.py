import uuid
from typing import Optional

from app.config import settings
from app.services.feedback_adapter_config import is_safe_code, is_safe_id, shown, source_label_problem
from app.services.feedback_constants import (
    ADAPTERS,
    COMPLETION_COMPLETED,
    COMPLETION_STATUSES,
    DEFAULT_INGESTION_SCHEMA,
    FEEDBACK_REPORT_TYPE,
    MAX_SEQUENCE,
    NATURAL_KEY_ADAPTERS,
)


def _get_nested(obj: dict, dotted_path: str):
    """Walks a dotted path (e.g. 'data.pdfUrls.original') through nested dicts.
    Returns (value, found) — found is False if any segment is missing or not a dict."""
    current = obj
    for segment in dotted_path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return None, False
        current = current[segment]
    return current, True


def _is_empty(value) -> bool:
    """True for None, a blank or whitespace-only string, {}, and a list whose
    items are all empty (so [] and ["  ", ""] both count) — explicitly NOT for
    0 or False, which are falsy-but-valid values (unlike a bare `not value` check)."""
    if value is None:
        return True
    if isinstance(value, str):
        return len(value.strip()) == 0
    if isinstance(value, (list, tuple)):
        return all(_is_empty(item) for item in value)
    if isinstance(value, dict) and len(value) == 0:
        return True
    return False


def _emptiness_label(value) -> str:
    """Distinguishes *why* a value tripped _is_empty(), for precise problem messages."""
    return "null" if value is None else "empty"


def _is_int(value) -> bool:
    """True for a real int; bool is an int subclass in Python, so it is excluded explicitly."""
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_feedback_messages(messages) -> list:
    """Validates the data.messages array of a feedback create event element by element and
    returns every problem found (not just the first). The presence / non-empty check of
    data.messages itself is done by the schema's required paths, so an empty or null array is
    not reported again here."""
    if _is_empty(messages):
        return []
    if not isinstance(messages, list):
        return [f"'data.messages' must be a list, got {type(messages).__name__}"]

    problems = []
    sequences = {}  # valid sequence -> number of rows carrying it
    for index, row in enumerate(messages):
        label = f"data.messages[{index}]"
        if not isinstance(row, dict):
            problems.append(f"'{label}' must be an object, got {type(row).__name__}")
            continue

        if "sequence" not in row:
            problems.append(f"'{label}.sequence' is missing")
        elif not _is_int(row["sequence"]):
            problems.append(f"'{label}.sequence' must be an integer, got {type(row['sequence']).__name__}")
        elif row["sequence"] < 1:
            problems.append(f"'{label}.sequence' must be >= 1, got {shown(row['sequence'])}")
        elif row["sequence"] > MAX_SEQUENCE:
            problems.append(f"'{label}.sequence' must be <= {MAX_SEQUENCE} (a 32-bit integer)")
        else:
            sequences[row["sequence"]] = sequences.get(row["sequence"], 0) + 1

        if "message" not in row:
            problems.append(f"'{label}.message' is missing")
        elif _is_empty(row["message"]):
            problems.append(f"'{label}.message' is {_emptiness_label(row['message'])}")
        elif not isinstance(row["message"], str):
            problems.append(f"'{label}.message' must be a string, got {type(row['message']).__name__}")

        if "isFollowUp" in row and not isinstance(row["isFollowUp"], bool):
            problems.append(f"'{label}.isFollowUp' must be a boolean, got {type(row['isFollowUp']).__name__}")

        if "parentSequence" in row and row["parentSequence"] is not None and not _is_int(row["parentSequence"]):
            problems.append(
                f"'{label}.parentSequence' must be null or an integer, got {type(row['parentSequence']).__name__}"
            )

        # translatedMessage and messageAt are optional (null allowed); only a wrong type is a problem.
        for optional_text in ("translatedMessage", "messageAt"):
            if optional_text in row and row[optional_text] is not None and not isinstance(row[optional_text], str):
                problems.append(
                    f"'{label}.{optional_text}' must be null or a string, got {type(row[optional_text]).__name__}"
                )

    for sequence, count in sorted(sequences.items()):
        if count > 1:
            problems.append(f"'data.messages' has duplicate sequence {sequence} ({count} rows)")

    # Parent links: the parent must exist in this event and come before the answer.
    for index, row in enumerate(messages):
        if not isinstance(row, dict) or not _is_int(row.get("parentSequence")):
            continue
        label = f"data.messages[{index}]"
        parent = row["parentSequence"]
        if parent not in sequences:
            problems.append(f"'{label}.parentSequence' {parent} does not match any message sequence in the event")
        elif _is_int(row.get("sequence")) and parent >= row["sequence"]:
            problems.append(
                f"'{label}.parentSequence' {parent} must be lower than the row's own sequence {row['sequence']}"
            )

    has_answer = any(isinstance(row, dict) and row.get("parentSequence") is not None for row in messages)
    if not has_answer:
        problems.append("'data.messages' has no message with a parentSequence (at least one answer is required)")

    return problems


def is_uuid(value) -> bool:
    """True for a string that parses as a UUID."""
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def _id_text(value):
    """The text of a submission / session id (a string, or an integer that the consumer turns into one), else None."""
    if isinstance(value, str):
        return value
    if _is_int(value):
        return str(value)
    return None


def feedback_identifier_problems(event: dict, *, create: bool) -> list:
    """
    Boundary check of the identifiers a feedback event carries, BEFORE they are used as a config-cache key, in an SQL
    parameter or in a log line: submissionId / sessionId (1..128 characters of [A-Za-z0-9._:-]), tenantCode and
    data.sessionType (1..64 of [A-Za-z0-9._-]), data.source (a source label), tags.programId (a UUID). Only fields
    that are present and non-empty are checked (the required-path check reports the missing ones). A value is never
    echoed unless it is a short safe token, so a bad identifier can not forge a log line or reach the DLQ reason.
    """
    problems = []
    for path in ("submissionId", "sessionId"):
        value, found = _get_nested(event, path)
        if found and not _is_empty(value) and not is_safe_id(_id_text(value)):
            problems.append(
                f"'{path}' must be 1 to 128 characters of letters, digits, '.', '_', ':', ',' and '-' (no spaces or control "
                f"characters)"
            )
    tenant, found = _get_nested(event, "tenantCode")
    if found and not _is_empty(tenant) and not is_safe_code(tenant):
        problems.append("'tenantCode' must be 1 to 64 characters of letters, digits, '.', '_' and '-'")
    if not create:
        return problems
    session_type, found = _get_nested(event, "data.sessionType")
    if found and not _is_empty(session_type) and isinstance(session_type, str) and not is_safe_code(session_type):
        problems.append("'data.sessionType' must be 1 to 64 characters of letters, digits, '.', '_' and '-'")
    source, found = _get_nested(event, "data.source")
    if found and not _is_empty(source):
        source_problem = source_label_problem(source)
        if source_problem is not None:
            problems.append(f"'data.source' is invalid: {source_problem}")
    # tags.programId selects the persona's config (a uuid column): a non-UUID must be rejected here, with
    # identifiers, instead of failing as a database error on every retry.
    program_id, found = _get_nested(event, "tags.programId")
    if found and not _is_empty(program_id) and not is_uuid(program_id):
        problems.append("'tags.programId' must be a UUID")
    return problems


def _validate_feedback_adapter_rules(event: dict, adapter: str, completion) -> list:
    """Rules that depend on data.adapter (the identity semantics of the source, chosen by its config, never by the
    source name): a natural-key adapter (wide_form) requires sourceRef, a generated UUID as submissionId AND
    sessionId (equal) and COMPLETED; every other adapter (conversation_rows) takes sourceRef absent / null."""
    problems = []
    source_ref, ref_found = _get_nested(event, "data.sourceRef")

    if adapter in NATURAL_KEY_ADAPTERS:
        if not ref_found:
            problems.append("'data.sourceRef' is missing")
        elif _is_empty(source_ref):
            problems.append(f"'data.sourceRef' is {_emptiness_label(source_ref)}")
        elif not isinstance(source_ref, str):
            problems.append(f"'data.sourceRef' must be a string, got {type(source_ref).__name__}")

        submission_id = event.get("submissionId")
        session_id = event.get("sessionId")
        if "submissionId" in event and not _is_empty(submission_id) and not is_uuid(submission_id):
            problems.append(f"'submissionId' must be a UUID for adapter {adapter}")
        if "sessionId" in event and not _is_empty(session_id) and not is_uuid(session_id):
            problems.append(f"'sessionId' must be a UUID for adapter {adapter}")
        if (is_uuid(submission_id) and is_uuid(session_id)) and submission_id != session_id:
            problems.append(f"'sessionId' must equal 'submissionId' for adapter {adapter}")

        if completion in COMPLETION_STATUSES and completion != COMPLETION_COMPLETED:
            problems.append(f"'data.completionStatus' must be {COMPLETION_COMPLETED} for adapter {adapter}, got {shown(completion)}")
    else:
        if ref_found and source_ref is not None:
            problems.append(f"'data.sourceRef' must be absent or null for adapter {adapter}")

    return problems


def _validate_feedback_create(event: dict) -> list:
    """Feedback-specific checks that the generic required-path mechanism cannot express."""
    problems = []

    if event.get("submissionType") != FEEDBACK_REPORT_TYPE:
        problems.append(f"'submissionType' must be {FEEDBACK_REPORT_TYPE!r}, got {event.get('submissionType')!r}")

    completion, found = _get_nested(event, "data.completionStatus")
    if found and not _is_empty(completion) and completion not in COMPLETION_STATUSES:
        problems.append(
            f"'data.completionStatus' must be one of {', '.join(COMPLETION_STATUSES)}, got {shown(completion)}"
        )

    # The source is a free label (valid when it has an active upload config: the consumer resolves that before it
    # gets here); the identity rules come from the adapter of that config, which the event carries and the consumer
    # cross-checks against the config.
    problems.extend(feedback_identifier_problems(event, create=True))
    adapter, found = _get_nested(event, "data.adapter")
    if not found:
        problems.append("'data.adapter' is missing")
    elif _is_empty(adapter):
        problems.append(f"'data.adapter' is {_emptiness_label(adapter)}")
    elif not isinstance(adapter, str) or adapter not in ADAPTERS:
        problems.append(f"'data.adapter' must be one of {', '.join(ADAPTERS)}, got {shown(adapter)}")
    else:
        problems.extend(_validate_feedback_adapter_rules(event, adapter, completion))

    # sessionType (the persona key) and questionnaireVersion are required (presence and emptiness come from the
    # configured schema); here only their type, which the generic mechanism cannot express.
    for path in ("data.sessionType", "data.questionnaireVersion"):
        value, found = _get_nested(event, path)
        if found and not _is_empty(value) and not isinstance(value, str):
            problems.append(f"'{path}' must be a string, got {type(value).__name__}")
    # the bot is informational (Mitra only): a string or null
    for path in ("data.botId", "data.botName"):
        value, found = _get_nested(event, path)
        if found and value is not None and not isinstance(value, str):
            problems.append(f"'{path}' must be a string or null, got {type(value).__name__}")

    messages, found = _get_nested(event, "data.messages")
    if found:
        problems.extend(_validate_feedback_messages(messages))

    return problems


def validate_ingestion_schema(
    event: dict,
    submission_type: str,
    event_type: str,
    feedback_schema: Optional[dict] = None,
) -> list:
    """
    Validates a Kafka event against the configured required-fields schema for its
    (submissionType, eventType) combination. Returns a list of problem descriptions;
    an empty list means the event is valid.

    Story / discussion schemas come from settings. A feedback schema is NOT in settings: it is the
    `ingestion_schema` of the persona's upload config and the caller passes it in as `feedback_schema`
    (the upload service loads it once per upload, the consumer caches it); no database is read here. A
    feedback create without a schema is a problem, never a silent default. A feedback delete event carries
    no persona, so without a schema it is checked against the fixed envelope fields only.

    Shared between app/kafka/consumer.py (validates events arriving off the Kafka
    topic) and app/api/services/uploads.py (validates each CSV-derived
    event against the same schema before it's ever published to Kafka).
    """
    normalized_type = submission_type.lower().strip() if isinstance(submission_type, str) else ""
    is_story_or_discussion = "story" in normalized_type or "discussion" in normalized_type
    is_feedback = FEEDBACK_REPORT_TYPE in normalized_type and not is_story_or_discussion
    if event_type in ("create", "update") and not is_story_or_discussion and not is_feedback:
        return [f"Unrecognized submissionType {submission_type!r}; no ingestion schema to validate against"]
    if is_feedback and event_type == "update":
        return ["feedback does not support update events in this release"]

    if is_feedback:
        if feedback_schema is not None:
            schema = feedback_schema
        elif event_type == "delete":
            schema = {"delete": {"required": DEFAULT_INGESTION_SCHEMA["delete"]["required"]}}
        elif event_type == "create":
            return ["No feedback ingestion schema was supplied: it comes from the 'ingestion_schema' of the "
                    "persona's feedback_upload config"]
        else:
            # any other eventType: the persona's schema is not needed to tell it has no section
            schema = {}
    else:
        try:
            schema = settings.get_kafka_ingestion_schema(submission_type)
        except ValueError as e:
            return [str(e)]

    event_schema = schema.get(event_type)
    if event_schema is None:
        return [f"No ingestion schema section defined for eventType {event_type!r}"]

    problems = []
    for path in event_schema.get("required", []):
        value, found = _get_nested(event, path)
        if not found:
            problems.append(f"'{path}' is missing")
        elif _is_empty(value):
            problems.append(f"'{path}' is {_emptiness_label(value)}")

    if event_type == "update" and event_schema.get("newValuesNoEmpty"):
        new_values = event.get("newValues")
        if isinstance(new_values, dict):
            for key, value in new_values.items():
                if _is_empty(value):
                    problems.append(f"'newValues.{key}' is {_emptiness_label(value)}")

    if is_feedback and event_type == "create":
        problems.extend(_validate_feedback_create(event))
    elif is_feedback and event_type == "delete":
        problems.extend(feedback_identifier_problems(event, create=False))

    return problems
