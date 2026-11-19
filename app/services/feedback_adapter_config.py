"""
Pure validators (no I/O) for the ADAPTER-related keys of a feedback_upload config, and the identifier rules that
keep source labels, tenant codes and ids safe at the boundary.

An upload config says which adapter reads its CSV (`adapter`) and how that adapter maps the file:

    "adapter": "conversation_rows" | "wide_form"        REQUIRED. Chosen by the config, never by the source name.

    conversation_rows (one row per message):
      "column_map":      {"<role>": "<CSV column name>", ...}   OPTIONAL. role -> header; a role that is not listed
                         keeps the default header (the role name itself, DEFAULT_COLUMN_MAP). Required roles:
                         session_type, session_status, session, submission_id, language, sequence, message,
                         created_at, stage, sender_id. Optional roles: bot_id, bot_name, translated_message,
                         conversation_row_id, receiver_id, updated_at. Headers are matched case-insensitively with
                         surrounding spaces ignored; two roles can not share a header.
      "completion_map":  {"<status value in the file>": "COMPLETED" | "IN_PROGRESS", ...}   OPTIONAL. Translates
                         the source's own status words into the platform's two states; a file value that is not a
                         key is a session problem. Default: COMPLETED and IN_PROGRESS map to themselves. (A legacy
                         `completion_values` list is still read as an identity map of those two words only.)

    wide_form (one row per respondent):
      "default_completion": "COMPLETED"                     OPTIONAL (default COMPLETED); a form response is final.

A key that belongs to the OTHER adapter (for example `column_map` in a wide_form config, or `forms` in a
conversation_rows config) is rejected: copying a config across adapters must fail loudly, not be silently ignored.

Every validator returns the checked value and raises ValueError that names the key; the message never contains
file content. Keys that no code ever read (`id_strategy`, `layout`, `ignore_extra_columns`, `columns`) were removed
from the seed; configs that still carry them keep working (they are ignored).
"""
import re
from typing import Any, Dict, Optional, Tuple

from app.services.feedback_constants import (
    ADAPTER_CONVERSATION_ROWS,
    ADAPTER_WIDE_FORM,
    ADAPTERS,
    CONFIG_KEY_ADAPTER,
    COMPLETION_COMPLETED,
    COMPLETION_STATUSES,
    MAX_SOURCE_LENGTH,
    SAFE_CODE_RE,
    SAFE_ID_RE,
    SOURCE_LABEL_RE,
)
from app.services.feedback_extraction_constants import CONFIG_SOURCE_ANY

# ---- conversation_rows column roles ----
COLUMN_ROLES_REQUIRED: Tuple[str, ...] = (
    "session_type", "session_status", "session", "submission_id", "language",
    "sequence", "message", "created_at", "stage", "sender_id",
)
COLUMN_ROLES_OPTIONAL: Tuple[str, ...] = (
    "bot_id", "bot_name", "translated_message", "conversation_row_id", "receiver_id", "updated_at",
)
COLUMN_ROLES: Tuple[str, ...] = COLUMN_ROLES_REQUIRED + COLUMN_ROLES_OPTIONAL
# The documented default: every role is read from the CSV column of the same name.
DEFAULT_COLUMN_MAP: Dict[str, str] = {role: role for role in COLUMN_ROLES}

CONFIG_KEY_COLUMN_MAP = "column_map"
CONFIG_KEY_COMPLETION_MAP = "completion_map"
CONFIG_KEY_LEGACY_COMPLETION_VALUES = "completion_values"
CONFIG_KEY_DEFAULT_COMPLETION = "default_completion"

# Keys that only one adapter reads. A config of the other adapter must not carry them.
CONVERSATION_ROWS_ONLY_KEYS: Tuple[str, ...] = (
    CONFIG_KEY_COLUMN_MAP, CONFIG_KEY_COMPLETION_MAP, CONFIG_KEY_LEGACY_COMPLETION_VALUES,
    "bot_sender_ids", "message_map", "sequence_offset", "opening_row", "questions", "closing_keys",
)
WIDE_FORM_ONLY_KEYS: Tuple[str, ...] = (
    "timestamp_column", "forms", CONFIG_KEY_DEFAULT_COMPLETION, "ignored_columns", "ignored_column_patterns",
)

_SOURCE_LABEL = re.compile(SOURCE_LABEL_RE)
_SAFE_CODE = re.compile(SAFE_CODE_RE)
_SAFE_ID = re.compile(SAFE_ID_RE)
_SHOWN_TOKEN = re.compile(r"[A-Za-z0-9._:-]{1,40}")


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and len(value.strip()) > 0


# ---------------------------------------------------------------------------------------------
# Identifier rules (source label, tenant / persona codes, ids): validated at the boundary
# ---------------------------------------------------------------------------------------------

def is_safe_source_label(value: Any) -> bool:
    """A source label: a string of 1..40 lower-case letters, digits or underscore. 'any' is reserved (it is the
    config source of the extraction configs, which are not uploaded to)."""
    return isinstance(value, str) and _SOURCE_LABEL.fullmatch(value) is not None and value != CONFIG_SOURCE_ANY


def source_label_problem(value: Any) -> Optional[str]:
    """None for a valid source label, else a message (the value itself is never echoed)."""
    if not isinstance(value, str) or len(value) == 0:
        return "source is missing"
    if value == CONFIG_SOURCE_ANY:
        return f"source {CONFIG_SOURCE_ANY!r} is reserved"
    if _SOURCE_LABEL.fullmatch(value) is None:
        return (f"source must be 1 to {MAX_SOURCE_LENGTH} characters of lower-case letters, digits and underscore "
                f"([a-z0-9_])")
    return None


def is_safe_code(value: Any) -> bool:
    """A tenant code or persona key: 1..64 characters of [A-Za-z0-9._-]."""
    return isinstance(value, str) and _SAFE_CODE.fullmatch(value) is not None


def is_safe_id(value: Any) -> bool:
    """A submission / session id: 1..128 characters of [A-Za-z0-9._:,-] (no whitespace, no control character)."""
    return isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None


def shown(value: Any) -> str:
    """How a value may be quoted in a problem message: only a short safe token (so no respondent text and no
    control character reaches a DLQ header or a log line); anything else is not shown."""
    if value is None or isinstance(value, bool):
        return repr(value)
    if isinstance(value, int) and abs(value) < 10 ** 12:
        return repr(value)
    if isinstance(value, str) and _SHOWN_TOKEN.fullmatch(value) is not None:
        return repr(value)
    return "<value not shown>"


# ---------------------------------------------------------------------------------------------
# adapter
# ---------------------------------------------------------------------------------------------

def validate_adapter(value: Any) -> str:
    """The `adapter` of a config: one of ADAPTERS (unknown or missing fails loudly)."""
    if not isinstance(value, str) or value not in ADAPTERS:
        raise ValueError(
            f"Feedback config '{CONFIG_KEY_ADAPTER}' must be one of: {', '.join(ADAPTERS)} (got {shown(value)})"
        )
    return value


def config_adapter(config: Dict[str, Any]) -> str:
    """The validated adapter of a config. Raises ValueError when the key is missing or unknown."""
    if not isinstance(config, dict) or CONFIG_KEY_ADAPTER not in config:
        raise ValueError(
            f"Feedback config is missing '{CONFIG_KEY_ADAPTER}' (one of: {', '.join(ADAPTERS)}); it says how the "
            "source's CSV is shaped (for a config that predates it run: python tools/feedback_config.py migrate; or migrate-legacy for a database created before session_type existed)"
        )
    return validate_adapter(config[CONFIG_KEY_ADAPTER])


# ---------------------------------------------------------------------------------------------
# conversation_rows: column_map, completion_map
# ---------------------------------------------------------------------------------------------

def validate_column_map(value: Any) -> Dict[str, str]:
    """The optional `column_map` as written in the config: an object of known role -> non-empty header."""
    key = CONFIG_KEY_COLUMN_MAP
    if not isinstance(value, dict):
        raise ValueError(f"Feedback config '{key}' must be an object of role -> CSV column name")
    unknown = sorted(str(role) for role in value if role not in COLUMN_ROLES)
    if len(unknown) > 0:
        raise ValueError(
            f"Feedback config '{key}' has unknown role(s): {', '.join(unknown)} (roles: {', '.join(COLUMN_ROLES)})"
        )
    for role, header in value.items():
        if not _non_empty_str(header):
            raise ValueError(f"Feedback config '{key}.{role}' must be a non-empty CSV column name")
    resolve_column_map({key: value})  # also rejects two roles that share one header
    return value


def resolve_column_map(config: Dict[str, Any]) -> Dict[str, str]:
    """role -> CSV header for every role: the config's `column_map` over DEFAULT_COLUMN_MAP (headers stripped).
    Raises ValueError when two roles resolve to the same header (compared case-insensitively)."""
    roles = dict(DEFAULT_COLUMN_MAP)
    configured = config.get(CONFIG_KEY_COLUMN_MAP)
    if configured is not None:
        if not isinstance(configured, dict):
            raise ValueError(f"Feedback config '{CONFIG_KEY_COLUMN_MAP}' must be an object of role -> CSV column name")
        for role, header in configured.items():
            if role in COLUMN_ROLES and _non_empty_str(header):
                roles[role] = header.strip()
    owners: Dict[str, str] = {}
    for role, header in roles.items():
        folded = header.lower()
        if folded in owners:
            raise ValueError(
                f"Feedback config '{CONFIG_KEY_COLUMN_MAP}': roles {owners[folded]} and {role} resolve to the same "
                f"column name"
            )
        owners[folded] = role
    return roles


def required_column_headers(config: Dict[str, Any]) -> Dict[str, str]:
    """role -> header of the roles a conversation_rows file must carry."""
    roles = resolve_column_map(config)
    return {role: roles[role] for role in COLUMN_ROLES_REQUIRED}


def validate_completion_map(value: Any) -> Dict[str, str]:
    """The optional `completion_map`: a non-empty object of source status word -> COMPLETED | IN_PROGRESS."""
    key = CONFIG_KEY_COMPLETION_MAP
    if not isinstance(value, dict) or len(value) == 0:
        raise ValueError(
            f"Feedback config '{key}' must be a non-empty object of source status -> "
            f"{' | '.join(COMPLETION_STATUSES)}"
        )
    for source_value, target in value.items():
        if not _non_empty_str(source_value) or source_value != source_value.strip():
            raise ValueError(f"Feedback config '{key}' has a key that is blank or has surrounding spaces")
        if target not in COMPLETION_STATUSES:
            raise ValueError(
                f"Feedback config '{key}' maps {shown(source_value)} to {shown(target)}; "
                f"every target must be one of: {', '.join(COMPLETION_STATUSES)}"
            )
    return value


def resolve_completion_map(config: Dict[str, Any]) -> Dict[str, str]:
    """source status word -> platform status, for a conversation_rows config: `completion_map`; else the legacy
    `completion_values` list as an identity map (only the platform's own two words are valid there); else both
    platform words map to themselves."""
    if CONFIG_KEY_COMPLETION_MAP in config:
        return dict(validate_completion_map(config[CONFIG_KEY_COMPLETION_MAP]))
    if CONFIG_KEY_LEGACY_COMPLETION_VALUES in config:
        legacy = config[CONFIG_KEY_LEGACY_COMPLETION_VALUES]
        if not isinstance(legacy, list) or len(legacy) == 0 or not all(v in COMPLETION_STATUSES for v in legacy):
            raise ValueError(
                f"Feedback config '{CONFIG_KEY_LEGACY_COMPLETION_VALUES}' may only list "
                f"{' / '.join(COMPLETION_STATUSES)}; to translate another vocabulary use "
                f"'{CONFIG_KEY_COMPLETION_MAP}' (source status -> {' | '.join(COMPLETION_STATUSES)})"
            )
        return {v: v for v in legacy}
    return {status: status for status in COMPLETION_STATUSES}


# ---------------------------------------------------------------------------------------------
# wide_form
# ---------------------------------------------------------------------------------------------

def validate_default_completion(value: Any) -> str:
    """A form response is final: its completion can only be COMPLETED."""
    if value != COMPLETION_COMPLETED:
        raise ValueError(
            f"Feedback config '{CONFIG_KEY_DEFAULT_COMPLETION}' must be {COMPLETION_COMPLETED} for adapter "
            f"{ADAPTER_WIDE_FORM} (a response is immutable), got {shown(value)}"
        )
    return value


def wide_form_identity_problem(config: Dict[str, Any]) -> Optional[str]:
    """
    The reason a wide_form config cannot ingest its responses, or None. The builder copies `session_type` and each
    form's `questionnaire_version` into every event, and storage refuses an event without them (both must be
    non-empty strings), so a config that lacks either would be accepted at upload and then fail every response.
    The values are never invented here (the questionnaire version of a form is the operator's to state). The
    message names keys and forms only, never a value.
    """
    session_type = config.get("session_type")
    if not isinstance(session_type, str) or len(session_type.strip()) == 0:
        return "Feedback config 'session_type' must be a non-empty string"
    forms = config.get("forms")
    if not isinstance(forms, dict):
        return "Feedback config 'forms' must be an object keyed by form"
    for form_key, form in forms.items():
        version = form.get("questionnaire_version") if isinstance(form, dict) else None
        if not isinstance(version, str) or len(version.strip()) == 0:
            return (f"Feedback config form {form_key!r} must carry 'questionnaire_version' as a non-empty string "
                    f"(set it per form; it is stamped on every response of the form)")
    return None


# ---------------------------------------------------------------------------------------------
# the whole adapter block
# ---------------------------------------------------------------------------------------------

def validate_adapter_config(config: Dict[str, Any]) -> str:
    """Validates every adapter-related key of an upload config and returns its adapter. Raises ValueError."""
    adapter = config_adapter(config)
    if adapter == ADAPTER_CONVERSATION_ROWS:
        foreign = [k for k in WIDE_FORM_ONLY_KEYS if k in config]
        if len(foreign) > 0:
            raise ValueError(
                f"Feedback config of adapter {adapter} has key(s) that only adapter {ADAPTER_WIDE_FORM} reads: "
                f"{', '.join(foreign)}"
            )
        if CONFIG_KEY_COLUMN_MAP in config:
            validate_column_map(config[CONFIG_KEY_COLUMN_MAP])
        resolve_completion_map(config)
    else:
        foreign = [k for k in CONVERSATION_ROWS_ONLY_KEYS if k in config]
        if len(foreign) > 0:
            raise ValueError(
                f"Feedback config of adapter {adapter} has key(s) that only adapter {ADAPTER_CONVERSATION_ROWS} "
                f"reads: {', '.join(foreign)}"
            )
        if CONFIG_KEY_DEFAULT_COMPLETION in config:
            validate_default_completion(config[CONFIG_KEY_DEFAULT_COMPLETION])
    return adapter
