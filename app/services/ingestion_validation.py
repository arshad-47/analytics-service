from app.config import settings


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


def validate_ingestion_schema(event: dict, submission_type: str, event_type: str) -> list:
    """
    Validates a Kafka event against the configured required-fields schema for its
    (submissionType, eventType) combination. Returns a list of problem descriptions;
    an empty list means the event is valid.

    Shared between app/kafka/consumer.py (validates events arriving off the Kafka
    topic) and app/api/services/uploads.py (validates each CSV-derived
    event against the same schema before it's ever published to Kafka).
    """
    normalized_type = submission_type.lower().strip() if isinstance(submission_type, str) else ""
    if event_type in ("create", "update") and "story" not in normalized_type and "discussion" not in normalized_type:
        return [f"Unrecognized submissionType {submission_type!r}; no ingestion schema to validate against"]

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

    return problems
