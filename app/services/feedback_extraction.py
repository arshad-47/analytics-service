"""
Pure core of the feedback metadata extraction: no database, no network, no Temporal.

One LLM call per stored feedback session receives the COMPLETE conversation (Mitra or Google Forms, any
questionnaire version) and returns, as JSON, a value for each configured metadata field plus a whole-conversation
PII flag. This module

  * parses and validates the extraction config (`parse_extraction_config`),
  * turns `feedbacks` rows into the conversation text (`build_conversation`),
  * renders the prompt templates (`render_prompt`),
  * validates the model's answer (`parse_model_response`): the answer is UNTRUSTED, it is parsed as strict JSON
    and every field is checked against the allowed values; a value outside them is never coerced, it is kept as
    'unmapped' with the raw value,
  * and runs the whole sequence with an injected LLM callable (`extract`), so everything is unit-testable.

The respondents include children and the conversation is DATA, never instructions: it is delimited with an id
derived from its own text, every message is collapsed to one line, and the template tells the model to ignore
instructions inside it. Nothing here logs conversation text, and no error message contains it.
"""
import functools
import hashlib
import json
import logging
import math
import re
import unicodedata
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from app.services.feedback_extraction_constants import (
    DEFAULT_MAX_BOT_MESSAGE_CHARS,
    DEFAULT_MAX_CONVERSATION_CHARS,
    DEFAULT_MAX_RAW_CHARS,
    DEFAULT_MAX_RESPONDENT_MESSAGE_CHARS,
    DEFAULT_MAX_VALUE_CHARS,
    DEFAULT_PII_MIN_DIGIT_RUN,
    DEFAULT_PII_MIN_DIGIT_RUN_SHORT_GROUPS,
    DEFAULT_PII_URL_TLDS,
    DEFAULT_PLACEHOLDERS,
    EXTRACTION_CONFIG_SCHEMA_VERSION,
    FIELD_KINDS as _CONSTANT_FIELD_KINDS,
    KIND_CATEGORY,
    KIND_FREE_TEXT,
    LIMIT_BOUNDS,
    MASK_EMAIL,
    MASK_HANDLE,
    MASK_LINK,
    MASK_NUMBER,
    MAX_DIGIT_GAP,
    MIN_COMMA_JOIN_DIGITS,
    MAX_LLM_TIMEOUT_SECONDS,
    MAX_PII_TYPE_DESCRIPTION_CHARS,
    MAX_PII_TYPES,
    MAX_PLACEHOLDER_CHARS,
    MAX_PLACEHOLDERS,
    MAX_PROMPT_VARIABLE_CHARS,
    MAX_PROMPT_VARIABLES,
    MAX_URL_TLDS,
    PII_DIGIT_RUN_BOUNDS,
    PII_TYPE_DESCRIPTIONS,
    PII_TYPES,
    STATUS_EXTRACTED,
    STATUS_NOT_PROVIDED,
    STATUS_UNMAPPED,
    YEAR_MAX,
    YEAR_MIN,
)

logger = logging.getLogger("analytics_service.feedback_extraction")

# The engine owns the 'integer' kind (a whole number inside [min, max], for example an age). Defined here so the
# parser does not depend on the order in which the seed constants are edited; if the constants module also lists
# it, the tuple below stays free of duplicates.
KIND_INTEGER = "integer"
FIELD_KINDS: Tuple[str, ...] = tuple(dict.fromkeys((*_CONSTANT_FIELD_KINDS, KIND_INTEGER)))
# The bounds of an integer field and the longest digit string of a model value that is even looked at: a number with
# more digits than this is out of any sensible range and becomes 'unmapped' without being converted.
MAX_INTEGER_BOUND = 10 ** 15
_MAX_INTEGER_DIGITS = 18
SENSITIVE_RULE = "(sensitive: fill only when the respondent states it explicitly; never infer)"


class ExtractionConfigError(ValueError):
    """The extraction config (a config_version.config JSON) is malformed."""


class PromptRenderError(ValueError):
    """A prompt template uses a variable that does not exist (or the config cannot fill one)."""


class ExtractionOutputError(ValueError):
    """The model's answer is not what the output contract allows. The message never contains model or respondent text."""

    def __init__(self, message: str):
        super().__init__(message)
        # Token usage of the call that produced the bad answer (set by `extract`), so it can still be logged.
        self.usage: Dict[str, Any] = {}


# ------------------------------------------------------------------------------------------------ config

@dataclass(frozen=True)
class Limits:
    """The size limits of one program's extraction (config block `limits`; defaults in the constants module)."""
    max_respondent_message_chars: int = DEFAULT_MAX_RESPONDENT_MESSAGE_CHARS
    max_bot_message_chars: int = DEFAULT_MAX_BOT_MESSAGE_CHARS
    max_conversation_chars: int = DEFAULT_MAX_CONVERSATION_CHARS
    max_value_chars: int = DEFAULT_MAX_VALUE_CHARS
    max_raw_chars: int = DEFAULT_MAX_RAW_CHARS


@dataclass(frozen=True)
class PiiPolicy:
    """The deterministic PII backstop of one program (config block `pii`, keys other than `types`)."""
    min_digit_run: int = DEFAULT_PII_MIN_DIGIT_RUN
    min_digit_run_short_groups: int = DEFAULT_PII_MIN_DIGIT_RUN_SHORT_GROUPS
    url_tlds: Tuple[str, ...] = DEFAULT_PII_URL_TLDS
    mask_before_call: bool = True   # mask what the check finds BEFORE the text goes to the model (see `mask_respondent_pii`)
    masking_prompt_name: Optional[str] = None   # exact prompts.name of the model-based masking call; None = no such call


DEFAULT_LIMITS = Limits()
DEFAULT_PII_POLICY = PiiPolicy()


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str                         # 'category' | 'free_text' | 'integer'
    description: str
    categories: Tuple[str, ...] = ()  # allowed values (category fields only)
    guidance: Optional[Dict[str, str]] = None  # category -> how to recognise it (shown to the model)
    provisional: bool = False         # product has not confirmed the bands
    min_value: Optional[int] = None   # integer fields only: smallest accepted value (inclusive)
    max_value: Optional[int] = None   # integer fields only: largest accepted value (inclusive)
    sensitive: bool = False           # a prompt rule (fill only when stated) AND raw_value is not stored (see keeps_raw)
    store_raw: Optional[bool] = None  # None = the default (not sensitive); True keeps raw_value of a sensitive field

    @property
    def keeps_raw(self) -> bool:
        """Whether the respondent's own words (raw_value) of this field are stored. Sensitive fields: only on request."""
        return self.store_raw if self.store_raw is not None else not self.sensitive


def _placeholder_key(text: str) -> str:
    """'N/A', 'Not provided.', ...: case-insensitive, spaces and surrounding punctuation ignored."""
    return re.sub(r"\s+", " ", text.replace("’", "'")).strip(" \t.,;:!?\"'()[]{}<>-_*~`").casefold()


DEFAULT_PLACEHOLDER_KEYS: FrozenSet[str] = frozenset(_placeholder_key(p) for p in DEFAULT_PLACEHOLDERS)


@dataclass(frozen=True)
class ExtractionConfig:
    persona: str
    session_type: str
    fields: Tuple[FieldSpec, ...]
    pii_types: Tuple[str, ...]
    llm: Dict[str, Any]               # optional model / max_tokens / timeout_seconds, each None when unset
    prompt_name: Optional[str] = None  # exact prompts.name of this program's prompt; always set by the parser
    pii_type_descriptions: Mapping[str, str] = field(default_factory=dict)   # type -> description given in the config
    limits: Limits = DEFAULT_LIMITS
    pii_policy: PiiPolicy = DEFAULT_PII_POLICY
    placeholders: FrozenSet[str] = DEFAULT_PLACEHOLDER_KEYS    # normalised keys (see _placeholder_key)
    prompt_variables: Mapping[str, str] = field(default_factory=dict)        # extra {{variables}} of the prompt

    def field_names(self) -> Tuple[str, ...]:
        return tuple(f.name for f in self.fields)


_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_TOP_KEYS = {"schema_version", "persona", "session_type", "source_note", "fields", "pii", "llm", "prompt_name",
             "prompt_variables", "limits", "placeholders"}
_FIELD_KEYS = {"name", "kind", "description", "categories", "guidance", "provisional", "min", "max", "sensitive",
               "store_raw"}
_LLM_KEYS = {"model", "max_tokens", "timeout_seconds"}
_PII_KEYS = {"types", "min_digit_run", "min_digit_run_short_groups", "url_tlds", "mask_before_call", "masking_prompt_name"}
# The variables the engine itself fills; a config's prompt_variables may not shadow them.
RESERVED_PROMPT_VARIABLES = frozenset({
    "persona", "fields", "field_names", "pii_types", "output_format", "conversation_id", "conversation"})
_TLD_RE = re.compile(r"^[^\W\d_]{2,24}$")


def _norm(text: str) -> str:
    """Comparison key of a band: case-insensitive, whitespace collapsed. Used only to find the canonical spelling."""
    return re.sub(r"\s+", " ", text).strip().casefold()


def _is_text(value: Any) -> bool:
    return isinstance(value, str) and len(value.strip()) > 0


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_plain_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_pii(pii_raw: Any) -> Tuple[Tuple[str, ...], Dict[str, str], PiiPolicy]:
    if not isinstance(pii_raw, dict):
        raise ExtractionConfigError("pii must be an object")
    unknown = sorted(set(pii_raw) - _PII_KEYS)
    if len(unknown) > 0:
        raise ExtractionConfigError(f"pii has unknown key(s): {unknown}; allowed: {sorted(_PII_KEYS)}")
    pii_types: Tuple[str, ...] = PII_TYPES
    descriptions: Dict[str, str] = {}
    if "types" in pii_raw:
        types = pii_raw["types"]
        if not isinstance(types, list) or len(types) == 0:
            raise ExtractionConfigError("pii.types must be a non-empty list of lower-case names or {name, description} objects")
        names: List[str] = []
        for index, item in enumerate(types):
            where = f"pii.types[{index}]"
            if isinstance(item, dict):
                if set(item) != {"name", "description"}:
                    raise ExtractionConfigError(f"{where} must have exactly the keys 'name' and 'description'")
                name, description = item["name"], item["description"]
                if not _is_text(description) or len(description.strip()) > MAX_PII_TYPE_DESCRIPTION_CHARS:
                    raise ExtractionConfigError(
                        f"{where}.description must be a non-empty string of at most {MAX_PII_TYPE_DESCRIPTION_CHARS} characters")
                descriptions[name if isinstance(name, str) else ""] = description.strip()
            else:
                name = item
            if not _is_text(name) or name != name.strip().lower() or _NAME_RE.match(name) is None:
                raise ExtractionConfigError(f"{where}: a pii type name must be a lower-case word like 'phone' ({_NAME_RE.pattern})")
            names.append(name)
        if len(set(names)) != len(names) or "other" not in names:
            raise ExtractionConfigError("pii.types must be unique and include 'other'")
        pii_types = tuple(names)

    def bounded(key: str, default: int, low: int, high: int) -> int:
        if key not in pii_raw:
            return default
        value = pii_raw[key]
        if not _is_plain_int(value) or value < low or value > high:
            raise ExtractionConfigError(f"pii.{key} must be an integer from {low} to {high}")
        return value

    low, high = PII_DIGIT_RUN_BOUNDS
    min_run = bounded("min_digit_run", DEFAULT_PII_MIN_DIGIT_RUN, low, high)
    min_short = bounded("min_digit_run_short_groups", max(DEFAULT_PII_MIN_DIGIT_RUN_SHORT_GROUPS, min_run), low, high)
    if min_short < min_run:
        raise ExtractionConfigError("pii.min_digit_run_short_groups must not be smaller than pii.min_digit_run")
    tlds = DEFAULT_PII_URL_TLDS
    if "url_tlds" in pii_raw:
        raw_tlds = pii_raw["url_tlds"]
        if (not isinstance(raw_tlds, list) or len(raw_tlds) > MAX_URL_TLDS
                or any(not isinstance(t, str) or t != t.lower() or _TLD_RE.match(t) is None for t in raw_tlds)):
            raise ExtractionConfigError(
                f"pii.url_tlds must be a list (at most {MAX_URL_TLDS}) of lower-case letters-only top-level domains "
                f"such as 'com'; an empty list switches the bare-domain check off")
        tlds = tuple(dict.fromkeys(raw_tlds))
    mask_before_call = True
    if "mask_before_call" in pii_raw:
        mask_before_call = pii_raw["mask_before_call"]
        if not isinstance(mask_before_call, bool):
            raise ExtractionConfigError("pii.mask_before_call must be a boolean (true or false)")
    masking_prompt_name: Optional[str] = None
    if "masking_prompt_name" in pii_raw and pii_raw["masking_prompt_name"] is not None:
        masking_prompt_name = pii_raw["masking_prompt_name"]
        if not _is_text(masking_prompt_name) or masking_prompt_name != masking_prompt_name.strip():
            raise ExtractionConfigError(
                "pii.masking_prompt_name must be null (no model-based masking) or the exact name of a prompts row "
                "of analysis_type feedback_pii_and_abusive_language_detection, without surrounding spaces")
    return pii_types, descriptions, PiiPolicy(min_run, min_short, tlds, mask_before_call, masking_prompt_name)


def _parse_limits(limits_raw: Any) -> Limits:
    if not isinstance(limits_raw, dict):
        raise ExtractionConfigError("limits must be an object")
    unknown = sorted(set(limits_raw) - set(LIMIT_BOUNDS))
    if len(unknown) > 0:
        raise ExtractionConfigError(f"limits has unknown key(s): {unknown}; allowed: {sorted(LIMIT_BOUNDS)}")
    values: Dict[str, int] = {}
    for key, value in limits_raw.items():
        low, high = LIMIT_BOUNDS[key]
        if not _is_plain_int(value) or value < low or value > high:
            raise ExtractionConfigError(f"limits.{key} must be an integer from {low} to {high}")
        values[key] = value
    limits = Limits(**values)
    if limits.max_value_chars > limits.max_raw_chars and "max_value_chars" in values:
        raise ExtractionConfigError("limits.max_value_chars must not be larger than limits.max_raw_chars")
    if limits.max_respondent_message_chars > limits.max_conversation_chars:
        raise ExtractionConfigError("limits.max_respondent_message_chars must not be larger than limits.max_conversation_chars")
    return limits


def _parse_placeholders(raw: Any) -> FrozenSet[str]:
    if (not isinstance(raw, list) or len(raw) > MAX_PLACEHOLDERS
            or any(not _is_text(p) or len(p) > MAX_PLACEHOLDER_CHARS for p in raw)):
        raise ExtractionConfigError(
            f"placeholders must be a list (at most {MAX_PLACEHOLDERS}) of non-empty strings of at most "
            f"{MAX_PLACEHOLDER_CHARS} characters; it replaces the built-in English list")
    return frozenset(_placeholder_key(p) for p in raw) - {""}


def _parse_prompt_variables(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict) or len(raw) > MAX_PROMPT_VARIABLES:
        raise ExtractionConfigError(f"prompt_variables must be an object of at most {MAX_PROMPT_VARIABLES} entries")
    parsed: Dict[str, str] = {}
    for name, value in raw.items():
        if not isinstance(name, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None:
            raise ExtractionConfigError("prompt_variables names must match [a-z][a-z0-9_]{0,62}")
        if name in RESERVED_PROMPT_VARIABLES:
            raise ExtractionConfigError(f"prompt_variables.{name} is a variable the engine fills itself")
        if not _is_text(value) or len(value) > MAX_PROMPT_VARIABLE_CHARS:
            raise ExtractionConfigError(
                f"prompt_variables.{name} must be a non-empty string of at most {MAX_PROMPT_VARIABLE_CHARS} characters")
        parsed[name] = value.strip()
    return parsed


def parse_extraction_config(raw: Any) -> ExtractionConfig:
    """Validates a config_version.config JSON (a dict or its JSON text) and returns it parsed. Never defaults silently."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError as exc:
            raise ExtractionConfigError("extraction config is not valid JSON") from exc
    if not isinstance(raw, dict):
        raise ExtractionConfigError("extraction config must be a JSON object")
    unknown = sorted(set(raw) - _TOP_KEYS)
    if len(unknown) > 0:
        raise ExtractionConfigError(f"extraction config has unknown key(s): {unknown}")
    if raw.get("schema_version") != EXTRACTION_CONFIG_SCHEMA_VERSION or isinstance(raw.get("schema_version"), bool):
        raise ExtractionConfigError(f"extraction config schema_version must be {EXTRACTION_CONFIG_SCHEMA_VERSION}")
    persona = raw.get("persona")
    session_type = raw.get("session_type")
    if not _is_text(persona):
        raise ExtractionConfigError("extraction config needs a non-empty persona")
    if not _is_text(session_type):
        raise ExtractionConfigError("extraction config needs a non-empty session_type")

    # Required: a config that names no prompt would otherwise be answered with some other program's wording.
    if "prompt_name" not in raw:
        raise ExtractionConfigError(
            "extraction config needs prompt_name (the exact name of a prompts row of analysis_type "
            "feedback_metadata_extraction); there is no default prompt")
    prompt_name = raw["prompt_name"]
    if not _is_text(prompt_name) or prompt_name != prompt_name.strip():
        raise ExtractionConfigError(
            "extraction config prompt_name must be a non-empty string without surrounding spaces "
            "(the exact name of a prompts row)")

    raw_fields = raw.get("fields")
    if not isinstance(raw_fields, list) or len(raw_fields) == 0:
        raise ExtractionConfigError("extraction config needs a non-empty list of fields")
    fields: List[FieldSpec] = []
    seen_names = set()
    for index, item in enumerate(raw_fields):
        where = f"fields[{index}]"
        if not isinstance(item, dict):
            raise ExtractionConfigError(f"{where} must be an object")
        unknown = sorted(set(item) - _FIELD_KEYS)
        if len(unknown) > 0:
            raise ExtractionConfigError(f"{where} has unknown key(s): {unknown}")
        name = item.get("name")
        if not isinstance(name, str) or _NAME_RE.match(name) is None:
            raise ExtractionConfigError(f"{where}.name must match {_NAME_RE.pattern}")
        if name in seen_names:
            raise ExtractionConfigError(f"duplicate field name {name!r}")
        seen_names.add(name)
        kind = item.get("kind")
        if kind not in FIELD_KINDS:
            raise ExtractionConfigError(f"{where}.kind must be one of {FIELD_KINDS}")
        if not _is_text(item.get("description")):
            raise ExtractionConfigError(f"{where}.description must be a non-empty string")
        provisional = item.get("provisional", False)
        if not isinstance(provisional, bool):
            raise ExtractionConfigError(f"{where}.provisional must be a boolean")
        sensitive = item.get("sensitive", False)
        if not isinstance(sensitive, bool):
            raise ExtractionConfigError(f"{where}.sensitive must be a boolean")
        store_raw: Optional[bool] = None
        if "store_raw" in item:
            store_raw = item["store_raw"]
            if not isinstance(store_raw, bool):
                raise ExtractionConfigError(f"{where}.store_raw must be a boolean")
        min_value: Optional[int] = None
        max_value: Optional[int] = None
        categories: Tuple[str, ...] = ()
        guidance = None
        if kind == KIND_CATEGORY:
            cats = item.get("categories")
            if not isinstance(cats, list) or len(cats) == 0:
                raise ExtractionConfigError(f"{where}.categories must be a non-empty list for a category field")
            seen_keys = set()
            for cat in cats:
                if not _is_text(cat) or cat != cat.strip():
                    raise ExtractionConfigError(f"{where}.categories must be non-empty strings without surrounding spaces")
                if _norm(cat) in seen_keys:
                    raise ExtractionConfigError(f"{where}.categories has a duplicate value (case-insensitive)")
                seen_keys.add(_norm(cat))
            categories = tuple(cats)
            guidance = item.get("guidance")
            if guidance is not None:
                if not isinstance(guidance, dict) or any(
                        not isinstance(k, str) or not _is_text(v) for k, v in guidance.items()):
                    raise ExtractionConfigError(f"{where}.guidance must map a category to a non-empty string")
                stray = sorted(set(guidance) - set(categories))
                if len(stray) > 0:
                    raise ExtractionConfigError(f"{where}.guidance names value(s) that are not categories: {stray}")
                guidance = dict(guidance)
            if "min" in item or "max" in item:
                raise ExtractionConfigError(f"{where}: a category field has no min or max")
        elif kind == KIND_INTEGER:
            if "categories" in item or "guidance" in item:
                raise ExtractionConfigError(f"{where}: an integer field has no categories or guidance")
            min_value, max_value = item.get("min"), item.get("max")
            if not _is_plain_int(min_value) or not _is_plain_int(max_value):
                raise ExtractionConfigError(f"{where}: an integer field needs integer min and max (not booleans)")
            if abs(min_value) > MAX_INTEGER_BOUND or abs(max_value) > MAX_INTEGER_BOUND:
                raise ExtractionConfigError(f"{where}: min and max must be within +-{MAX_INTEGER_BOUND}")
            if min_value > max_value:
                raise ExtractionConfigError(f"{where}: min must not be greater than max")
        else:
            if "categories" in item or "guidance" in item:
                raise ExtractionConfigError(f"{where}: a free_text field has no categories or guidance")
            if "min" in item or "max" in item:
                raise ExtractionConfigError(f"{where}: a free_text field has no min or max")
        fields.append(FieldSpec(name, kind, item["description"].strip(), categories, guidance, provisional,
                                min_value, max_value, sensitive, store_raw))

    pii_types: Tuple[str, ...] = PII_TYPES
    pii_descriptions: Dict[str, str] = {}
    pii_policy = DEFAULT_PII_POLICY
    if raw.get("pii") is not None:
        pii_types, pii_descriptions, pii_policy = _parse_pii(raw["pii"])
    limits = _parse_limits(raw["limits"]) if raw.get("limits") is not None else DEFAULT_LIMITS
    placeholders = _parse_placeholders(raw["placeholders"]) if raw.get("placeholders") is not None \
        else DEFAULT_PLACEHOLDER_KEYS
    prompt_variables = _parse_prompt_variables(raw["prompt_variables"]) if raw.get("prompt_variables") is not None else {}

    llm_raw = raw.get("llm")
    llm: Dict[str, Any] = {"model": None, "max_tokens": None, "timeout_seconds": None}
    if llm_raw is not None:
        if not isinstance(llm_raw, dict):
            raise ExtractionConfigError("llm must be an object")
        unknown = sorted(set(llm_raw) - _LLM_KEYS)
        if len(unknown) > 0:
            raise ExtractionConfigError(f"llm has unknown key(s): {unknown}")
        if llm_raw.get("model") is not None and not _is_text(llm_raw["model"]):
            raise ExtractionConfigError("llm.model must be a non-empty string or null")
        for key in ("max_tokens", "timeout_seconds"):
            if llm_raw.get(key) is not None and not _is_positive_int(llm_raw[key]):
                raise ExtractionConfigError(f"llm.{key} must be a positive integer or null")
        if llm_raw.get("timeout_seconds") is not None and llm_raw["timeout_seconds"] > MAX_LLM_TIMEOUT_SECONDS:
            raise ExtractionConfigError(f"llm.timeout_seconds must not exceed {MAX_LLM_TIMEOUT_SECONDS}")
        for key in _LLM_KEYS:
            if key in llm_raw:
                llm[key] = llm_raw[key]
    return ExtractionConfig(persona.strip(), session_type.strip(), tuple(fields), pii_types, llm, prompt_name,
                            pii_descriptions, limits, pii_policy, placeholders, prompt_variables)


# ------------------------------------------------------------------------------------------------ conversation

ROLE_BOT = "Bot"
ROLE_RESPONDENT = "Respondent"
# Control characters become SPACES (not nothing), so "Asha\x0bKumar" stays two words.
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
_SPACE_RE = re.compile(r"\s+")
_TRUNCATED = " [truncated]"


@dataclass(frozen=True)
class Turn:
    sequence: int
    role: str
    text: str


@dataclass(frozen=True)
class Conversation:
    turns: Tuple[Turn, ...]
    text: str                 # one line per turn: "[sequence] Role: text"
    conversation_id: str      # derived from the text; delimits it in the prompt
    respondent_answers: int   # number of non-blank respondent answers
    oversized_respondent_message: bool = False  # a respondent message longer than limits.max_respondent_message_chars
    too_long: bool = False    # the whole conversation text is longer than limits.max_conversation_chars


def _clean(value: Any) -> str:
    """One line, control characters turned into spaces, whitespace collapsed; None and non-strings are blank. Never cut."""
    if not isinstance(value, str):
        return ""
    return _SPACE_RE.sub(" ", _CONTROL_RE.sub(" ", value)).strip()


def _cap_bot(text: str, limit: int) -> str:
    """Bot messages are the questionnaire's own text and carry no respondent PII: a very long one is cut."""
    if len(text) > limit:
        return text[:limit] + _TRUNCATED
    return text


def build_conversation(rows: Sequence[Mapping[str, Any]], closing_keys: Sequence[str] = (),
                       limits: Limits = DEFAULT_LIMITS) -> Conversation:
    """
    Builds the conversation text from the `feedbacks` rows of one session (any order; sorted by `sequence`).

    A row is an answer (Respondent) exactly when `parent_id` is set; every other row is a Bot row (question,
    probe, opening message, closing message). The English column is `translated_message`: a bot row shows it
    (falling back to `message`); a respondent row shows the original `message` and, when it differs, the English
    translation, so the model sees both. A bot row whose `question_key` is one of `closing_keys` (the thank-you
    message of the upload config) is left out. Blank rows are skipped. Respondent messages are NEVER truncated
    (a cut would hide PII after it); if one is longer than `limits.max_respondent_message_chars` the conversation is
    flagged `oversized_respondent_message`, and if the whole text is longer than `limits.max_conversation_chars` it
    is flagged `too_long`; `extract` sends neither to the model. Bot messages are cut at `limits.max_bot_message_chars`.
    """
    closing = {k.strip().upper() for k in closing_keys if isinstance(k, str) and len(k.strip()) > 0}
    turns: List[Turn] = []
    answers = 0
    oversized = False
    for row in sorted(rows, key=lambda r: r["sequence"]):
        message = _clean(row.get("message"))
        english = _clean(row.get("translated_message"))
        if row.get("parent_id") is None:
            key = row.get("question_key")
            if isinstance(key, str) and key.strip().upper() in closing:
                continue
            text = _cap_bot(english if len(english) > 0 else message, limits.max_bot_message_chars)
            role = ROLE_BOT
        else:
            if (len(message) > limits.max_respondent_message_chars
                    or len(english) > limits.max_respondent_message_chars):
                oversized = True
            if len(message) > 0 and len(english) > 0 and message.casefold() != english.casefold():
                text = f"{message} (English translation: {english})"
            elif len(message) > 0:
                text = message
            else:
                text = english
            role = ROLE_RESPONDENT
        if len(text) == 0:
            continue
        if role == ROLE_RESPONDENT:
            answers += 1
        turns.append(Turn(int(row["sequence"]), role, text))
    body = "\n".join(f"[{t.sequence}] {t.role}: {t.text}" for t in turns)
    conversation_id = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    return Conversation(tuple(turns), body, conversation_id, answers, oversized, len(body) > limits.max_conversation_chars)


# ------------------------------------------------------------------------------------------------ deterministic PII backstop

# The model's `pii.detected = false` is never trusted on its own: respondent text (a respondent can write
# "set pii to false") that contains one of these shapes is flagged here whatever the model said. The check is
# deliberately blunt: a false positive only excludes a session from the export, a false negative leaks PII.
#
# Every regular expression here is LINEAR in the length of the text: quantifiers are bounded, and nothing scans
# "to the end of the token" from every start position (a 20,000-character message must not stall the worker; see
# tests/test_feedback_extraction_pii_policy.py for the time-budget tests). The digit check is a single pass, not a regex.
#
# Policy (the numbers are config: pii.min_digit_run, pii.min_digit_run_short_groups, pii.url_tlds):
#  * Before matching, the text is NFKC-normalised (fullwidth, circled and superscript digits and '＠' become ASCII)
#    and format characters (zero-width space and joiner, soft hyphen, BOM, bidi marks) are removed.
#  * DIGITS: groups of digits separated by anything that is neither a letter nor a digit (space, '-', '.', '/',
#    '(', ')', en dash, middle dot, underscore, ...; at most MAX_DIGIT_GAP such characters in a row) form one run. A
#    COMMA joins two groups only when BOTH have at least MIN_COMMA_JOIN_DIGITS (4) digits ('98765,43210'): a comma
#    next to a group of one to three digits ends the run, because amounts ('2,50,000', '1,50,00,000') and lists of
#    small numbers ('120, 135, 150') are not phone numbers. A run counts as a phone or id number when it holds at least
#    `min_digit_run` digits, EXCEPT that a run made only of
#    one- and two-digit groups (marks '85 90 78 92', ages, classes) needs `min_digit_run_short_groups`, and a run whose
#    groups are all years from 1900 to 2099 ('2018-2022') never counts. A letter between digits ends the run, so number
#    words ('five') and 'x'-separated digits are NOT covered (only the model can flag them). Consecutive respondent
#    messages that are nothing but numbers of three or more digits are joined ('98765' then '43210').
#  * E-MAIL: letters/digits/._+- then '@' then a letter ('asha@gmail'); '@' followed by a digit ('rate@5%') is not.
#    HANDLE: '@' at the start of a word followed by a word character and at least one more.
#  * URL: 'http(s)://' or 'www.' followed by text, or a bare 'name.tld' with a tld of the config list. A two-letter tld
#    followed by a space and a letter ('learn.in the village': a missing space after a full stop) is not a URL.
_EMAIL_RE = re.compile(r"[\w.+-]{1,64}@[^\W\d_]")
_HANDLE_RE = re.compile(r"(?<![^\s(\[{\"'<])@\w[\w.]+")


@functools.lru_cache(maxsize=64)
def _url_regex(tlds: Tuple[str, ...]) -> "Optional[re.Pattern[str]]":
    long_tlds = [re.escape(t) for t in tlds if len(t) > 2]
    short_tlds = [re.escape(t) for t in tlds if len(t) <= 2]
    alternatives: List[str] = []
    if len(long_tlds) > 0:
        alternatives.append(r"(?:" + "|".join(long_tlds) + r")\b")
    if len(short_tlds) > 0:
        alternatives.append(r"(?:" + "|".join(short_tlds) + r")\b(?!\s+[^\W\d_])")
    scheme = r"(?:https?://|www\.)\S+"
    if len(alternatives) == 0:
        return re.compile(scheme, re.IGNORECASE)
    bare = r"(?<![\w@.])[\w-]{1,63}(?:\.[\w-]{1,63}){0,8}\.(?:" + "|".join(alternatives) + ")"
    return re.compile(scheme + "|" + bare, re.IGNORECASE)


PII_KIND_NUMBER = "phone"      # a long digit run: a phone number or an id number (the kind is informational)
PII_KIND_EMAIL = "email"       # an email address or an @handle (the 'email' type covers social handles)
PII_KIND_URL = "other"


def _precheck_text(value: Any) -> str:
    """The text the pre-check looks at: one line, NFKC-normalised, without format characters."""
    text = _clean(value)
    if len(text) == 0 or text.isascii():
        return text
    text = unicodedata.normalize("NFKC", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Cf")
    return _clean(text)


def _scan_digit_runs(text: str) -> List[Tuple[List[str], int, int]]:
    """
    The runs of digit groups of `text` as (groups, start, end): `start` is the index of the first digit of the first
    group and `end` the index after the last digit of the last group (see the policy above). One pass, linear.
    """
    runs: List[Tuple[List[str], int, int]] = []
    groups: List[str] = []
    run_start = 0
    n = len(text)
    i = 0
    while i < n:
        if not text[i].isdecimal():
            i += 1
            continue
        j = i + 1
        while j < n and text[j].isdecimal():
            j += 1
        if len(groups) == 0:
            run_start = i
        groups.append(text[i:j])
        k = j
        while k < n and k - j < MAX_DIGIT_GAP and not text[k].isalnum():
            k += 1
        if k > j and k < n and text[k].isdecimal():
            joins = True
            if "," in text[j:k]:
                # a comma joins only two groups of four or more digits ('98765,43210'); '2,50,000' and '120, 135' do not
                m = k + 1
                while m < n and text[m].isdecimal():
                    m += 1
                joins = j - i >= MIN_COMMA_JOIN_DIGITS and m - k >= MIN_COMMA_JOIN_DIGITS
            if joins:
                i = k          # a separator (1..MAX_DIGIT_GAP characters) and then more digits: the run goes on
                continue
        runs.append((groups, run_start, j))
        groups = []
        i = j
    return runs


def _digit_runs(text: str) -> List[List[str]]:
    """The runs of digit groups of `text` (see the policy above). One pass, linear."""
    return [groups for groups, _start, _end in _scan_digit_runs(text)]


def _is_year_group(group: str) -> bool:
    return len(group) == 4 and YEAR_MIN <= int(group) <= YEAR_MAX


class _RunStats:
    """
    What `_run_is_number` needs to know about a run of digit groups, kept as running counts so that a run that grows
    group by group (the numbers-only chain of consecutive messages) is judged in O(1) per added group instead of
    rescanning everything accumulated so far.
    """
    __slots__ = ("count", "digits", "all_years", "all_short")

    def __init__(self, groups: Sequence[str] = ()) -> None:
        self.count = 0              # number of groups
        self.digits = 0             # their total digits
        self.all_years = True       # every group is a year from YEAR_MIN to YEAR_MAX (also true for no group)
        self.all_short = True       # every group has one or two digits (also true for no group)
        self.add(groups)

    def add(self, groups: Sequence[str]) -> None:
        for g in groups:
            self.count += 1
            self.digits += len(g)
            if self.all_years and not _is_year_group(g):
                self.all_years = False
            if self.all_short and len(g) > 2:
                self.all_short = False

    def is_number(self, policy: PiiPolicy) -> bool:
        if self.all_years:
            return False
        short_groups = self.count > 1 and self.all_short
        needed = policy.min_digit_run_short_groups if short_groups else policy.min_digit_run
        return self.digits >= needed


def _run_is_number(groups: Sequence[str], policy: PiiPolicy) -> bool:
    return _RunStats(groups).is_number(policy)


def _is_numbers_only(text: str, groups: Sequence[str]) -> bool:
    """A message that is nothing but numbers of three or more digits and separators (a number answered in parts)."""
    return all(len(g) >= 3 for g in groups) and all(ch.isdecimal() or not ch.isalnum() for ch in text)


_TOKEN_RE = re.compile(r"[^ ]+")
_Span = Tuple[int, int, str]    # start, end (exclusive) in the pre-check text, and the placeholder that replaces it


def _number_span(text: str, start: int, end: int) -> _Span:
    """The span of a number to mask: the run, widened over a '+' or '(' written directly before it ('+91', '(022')."""
    while start > 0 and text[start - 1] in "+(":
        start -= 1
    return (start, end, MASK_NUMBER)


def _scan_text(text: str, policy: PiiPolicy, url_re: "re.Pattern[str]") -> Tuple[List[str], List[_Span], List[Tuple[List[str], int, int]]]:
    """
    One pre-check text (see `_precheck_text`): the kinds found, the spans to mask and the digit runs (the caller
    needs the runs to chain numbers-only messages). Linear: every pass over the text is bounded per character.
    """
    kinds: List[str] = []
    spans: List[_Span] = []
    runs = _scan_digit_runs(text)
    for groups, start, end in runs:
        if _run_is_number(groups, policy):
            kinds.append(PII_KIND_NUMBER)
            spans.append(_number_span(text, start, end))
    scanned = text
    if "@" in text:
        # E-mail addresses and handles: the whole space-delimited token is masked. The URL check below does not see
        # such a token (blanked, so positions stay: the domain of 'asha@gmail.com' is not a link of its own), but ONLY
        # a token that matched: a token that holds an '@' without being an address or handle ('https://x.org/@asha_k',
        # 'medium.com/@asha_k': a '/' precedes the '@') stays visible to the URL scan, or it would reach the model.
        pieces: List[str] = []
        last = 0
        blanked = False
        for match in _TOKEN_RE.finditer(text):
            token = match.group()
            if "@" not in token:
                continue
            if _EMAIL_RE.search(token) is not None:
                kinds.append(PII_KIND_EMAIL)
                spans.append((match.start(), match.end(), MASK_EMAIL))
            elif _HANDLE_RE.search(token) is not None:
                kinds.append(PII_KIND_EMAIL)
                spans.append((match.start(), match.end(), MASK_HANDLE))
            else:
                continue
            pieces.append(text[last:match.start()])
            pieces.append(" " * len(token))
            last = match.end()
            blanked = True
        if blanked:
            pieces.append(text[last:])
            scanned = "".join(pieces)
    last_end = 0
    for match in url_re.finditer(scanned):
        if match.start() < last_end:
            continue
        end = scanned.find(" ", match.end())
        last_end = len(scanned) if end < 0 else end     # the whole token goes ('www.x.in/asha', 'mysite.in/page')
        kinds.append(PII_KIND_URL)
        spans.append((match.start(), last_end, MASK_LINK))
    return kinds, spans, runs


def _apply_spans(text: str, spans: Sequence[_Span]) -> str:
    """`text` with each span replaced by its placeholder; overlapping spans become one (the first one's placeholder)."""
    pieces: List[str] = []
    position = 0
    current: Optional[List[Any]] = None
    merged: List[List[Any]] = []
    for start, end, placeholder in sorted(spans, key=lambda s: (s[0], s[1])):
        if current is not None and start <= current[1]:
            current[1] = max(current[1], end)
        else:
            current = [start, end, placeholder]
            merged.append(current)
    for start, end, placeholder in merged:
        pieces.append(text[position:start])
        pieces.append(placeholder)
        position = end
    pieces.append(text[position:])
    return "".join(pieces)


_PII_COLUMNS = ("message", "translated_message")


def _scan_rows(rows: Sequence[Mapping[str, Any]], policy: PiiPolicy):
    """
    The one pass behind `pii_precheck_kinds` and `mask_respondent_pii` over the RESPONDENT rows (parent_id set),
    original and translated column each on its own. Returns (kinds, found) where `found` maps
    (index into `rows`, column) -> (pre-check text, spans). Texts without a hit are not in `found`.
    """
    found: Dict[Tuple[int, str], Tuple[str, List[_Span]]] = {}
    kinds: List[str] = []

    def add(kind: str) -> None:
        if kind not in kinds:
            kinds.append(kind)

    respondent = sorted(((index, row) for index, row in enumerate(rows) if row.get("parent_id") is not None),
                        key=lambda pair: pair[1]["sequence"])
    url_re = _url_regex(tuple(policy.url_tlds))
    for column in _PII_COLUMNS:
        chain = _RunStats()                         # digit groups of consecutive numbers-only messages (running counts)
        members: List[Tuple[Tuple[int, str], str, int, int]] = []   # (key, text, start, end) of those messages
        masked_members = 0                          # how many of `members` already carry their number span
        for index, row in respondent:
            text = _precheck_text(row.get(column))
            if len(text) == 0:
                continue
            row_kinds, spans, runs = _scan_text(text, policy, url_re)
            key = (index, column)
            if PII_KIND_NUMBER in row_kinds:
                add(PII_KIND_NUMBER)
            if len(runs) == 1 and _is_numbers_only(text, runs[0][0]):
                chain.add(runs[0][0])
                members.append((key, text, runs[0][1], runs[0][2]))
                if len(members) > 1 and chain.is_number(policy):
                    add(PII_KIND_NUMBER)
                    # every message of the chain is a part of the number, the ones before this message too
                    for m_key, m_text, m_start, m_end in members[masked_members:]:
                        m_span = _number_span(m_text, m_start, m_end)
                        if m_key == key:
                            spans.append(m_span)
                        else:
                            found.setdefault(m_key, (m_text, []))[1].append(m_span)
                    masked_members = len(members)
            else:
                chain, members, masked_members = _RunStats(), [], 0
            for kind in row_kinds:
                add(kind)
            if len(spans) > 0:
                found.setdefault(key, (text, []))[1].extend(spans)
    return tuple(kinds), found


def pii_precheck_kinds(rows: Sequence[Mapping[str, Any]], policy: PiiPolicy = DEFAULT_PII_POLICY) -> Tuple[str, ...]:
    """
    The kinds of identifying shapes found in the RESPONDENT rows (parent_id set), original and translated column
    each checked on its own: a long run of digits, an email address, an '@handle', a URL (the policy is described
    above). Returns the kinds in a stable order (empty when none). Never returns or logs the text itself.
    """
    return _scan_rows(rows, policy)[0]


def mask_respondent_pii(rows: Sequence[Mapping[str, Any]], policy: PiiPolicy = DEFAULT_PII_POLICY
                        ) -> Tuple[List[Mapping[str, Any]], Tuple[str, ...]]:
    """
    (rows for the prompt, kinds): the same check as `pii_precheck_kinds`, and every hit REPLACED in a copy of the
    rows by a short placeholder ('[number removed]', '[email removed]', '[handle removed]', '[link removed]'), so that
    what the check finds never reaches the model. Both columns (`message`, `translated_message`) of every respondent
    row are masked; a number answered in parts is masked in every message of the chain. Only a column that holds a
    hit changes (it comes back normalised the way the check read it: NFKC, no format characters, one line); every other
    column, every bot row and the input rows themselves stay exactly as they were. The input is never modified and the
    text is never logged.
    """
    kinds, found = _scan_rows(rows, policy)
    if len(found) == 0:
        return list(rows), kinds
    masked: List[Mapping[str, Any]] = list(rows)
    for (index, column), (text, spans) in found.items():
        if masked[index] is rows[index]:
            masked[index] = dict(rows[index])
        masked[index][column] = _apply_spans(text, spans)
    return masked, kinds


def apply_pii_kinds(result: "ExtractionResult", kinds: Sequence[str], config: ExtractionConfig) -> "ExtractionResult":
    """Forces pii_detected TRUE (and adds the matching types, or 'other') when `kinds` (from the check) is not empty."""
    if len(kinds) == 0:
        return result
    types = list(result.pii_types)
    for kind in kinds:
        label = kind if kind in config.pii_types else "other"
        if label not in types:
            types.append(label)
    if not result.pii_detected:
        # Only the kinds are logged, never the text: how often the model said false where the pre-check disagrees.
        logger.info("PII pre-check overrode the model's pii.detected=false, kinds=%s", list(kinds))
    return replace(result, pii_detected=True, pii_types=tuple(types))


def apply_pii_precheck(result: "ExtractionResult", rows: Sequence[Mapping[str, Any]],
                       config: ExtractionConfig) -> "ExtractionResult":
    """Forces pii_detected TRUE (and adds the matching types) when the deterministic pre-check finds something."""
    return apply_pii_kinds(result, pii_precheck_kinds(rows, config.pii_policy), config)


def minimise_flagged(result: "ExtractionResult", config: ExtractionConfig) -> "ExtractionResult":
    """
    A session flagged pii_detected keeps no respondent words: raw_value is NULL for every field and the value of a
    free_text field (it is text the respondent wrote) is NULL as well; the status stays, so a count of what was
    answered is still possible. Category and integer values (a band from the config, a number inside a range) stay.
    Such sessions are excluded from the export anyway; this makes sure a query that forgets the exclusion cannot
    return the text that made the flag TRUE.
    """
    if not result.pii_detected:
        return result
    kinds = {spec.name: spec.kind for spec in config.fields}
    fields = tuple(
        replace(f, raw_value=None, value=None if kinds.get(f.field_name) == KIND_FREE_TEXT else f.value)
        for f in result.fields)
    return replace(result, fields=fields)


# ------------------------------------------------------------------------------------------------ prompt

_VAR_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def _render_fields(config: ExtractionConfig) -> str:
    lines: List[str] = []
    for number, spec in enumerate(config.fields, start=1):
        if spec.kind == KIND_CATEGORY:
            lines.append(f"{number}. {spec.name} (choose exactly one allowed value): {spec.description}")
            lines.append("   Allowed values: " + "; ".join(spec.categories))
            if spec.guidance is not None and len(spec.guidance) > 0:
                lines.append("   How to recognise them: " + "; ".join(f"{k} = {v}" for k, v in spec.guidance.items()))
        elif spec.kind == KIND_INTEGER:
            lines.append(
                f"{number}. {spec.name} (a whole number from {spec.min_value} to {spec.max_value}; return it as a "
                f"JSON number, null when not stated): {spec.description}")
        else:
            lines.append(f"{number}. {spec.name} (free text): {spec.description}")
        if spec.sensitive:
            lines.append("   " + SENSITIVE_RULE)
    return "\n".join(lines)


def _pii_type_description(config: ExtractionConfig, name: str) -> str:
    """The description the config gives the type, else the neutral built-in one for the default names, else 'as named'."""
    if name in config.pii_type_descriptions:
        return config.pii_type_descriptions[name]
    if name in PII_TYPE_DESCRIPTIONS:
        return PII_TYPE_DESCRIPTIONS[name]
    return "as named"


def _render_pii_types(config: ExtractionConfig) -> str:
    return "\n".join(f"- {t}: {_pii_type_description(config, t)}" for t in config.pii_types)


def _render_output_format(config: ExtractionConfig) -> str:
    example = {
        "fields": {n: {"value": None, "raw": None, "confidence": None} for n in config.field_names()},
        "pii": {"detected": False, "types": []},
    }
    return json.dumps(example, indent=2)


def _fill(template: str, variables: Mapping[str, str]) -> str:
    """Single pass: a variable's value is never scanned again, so text inside it cannot inject another variable."""
    def replace(match: "re.Match[str]") -> str:
        name = match.group(1)
        if name not in variables:
            raise PromptRenderError(f"prompt template uses an unknown variable {{{{{name}}}}}")
        return variables[name]
    return _VAR_RE.sub(replace, template)


def render_prompt(system_template: str, user_template: str, config: ExtractionConfig, conversation: Conversation) -> str:
    """
    The full prompt (system part, blank line, user part) as one string, the way the other activities send it.
    Variables: persona, fields, field_names, pii_types, output_format, conversation_id, conversation, plus the
    config's `prompt_variables`. All of them are filled in ONE pass (`_fill`): a value is never scanned again, so
    neither the conversation nor a configured variable can pull in another variable.
    """
    variables = {
        **config.prompt_variables,
        "persona": config.persona,
        "fields": _render_fields(config),
        "field_names": ", ".join(config.field_names()),
        "pii_types": _render_pii_types(config),
        "output_format": _render_output_format(config),
        "conversation_id": conversation.conversation_id,
        "conversation": conversation.text,
    }
    return _fill(system_template, variables) + "\n\n" + _fill(user_template, variables)


# ------------------------------------------------------------------------------------------------ model answer

@dataclass(frozen=True)
class FieldResult:
    field_name: str
    value: Optional[str]       # the band / canonical value; None when not provided or unmapped
    raw_value: Optional[str]   # as the model returned it; kept for unmapped
    status: str                # 'extracted' | 'not_provided' | 'unmapped'
    confidence: Optional[float]


@dataclass(frozen=True)
class ExtractionResult:
    fields: Tuple[FieldResult, ...]
    pii_detected: bool
    pii_types: Tuple[str, ...]


@dataclass(frozen=True)
class ExtractionRun:
    result: ExtractionResult
    usage: Dict[str, Any]
    llm_called: bool           # False when no call was made (see skip_reason)
    conversation_turns: int
    skip_reason: Optional[str] = None   # SKIP_NO_ANSWER | SKIP_OVERSIZED | SKIP_TOO_LONG when llm_called is False
    masking: Optional[Any] = None       # MaskingResult of the model-based masking call (app/services/feedback_pii_masking.py)


SKIP_NO_ANSWER = "no_respondent_answer"
SKIP_OVERSIZED = "oversized_respondent_message"
SKIP_TOO_LONG = "conversation_too_long"


def empty_result(config: ExtractionConfig) -> ExtractionResult:
    """A conversation with no respondent answer: nothing was provided and there is nothing that could be PII."""
    return ExtractionResult(
        tuple(FieldResult(n, None, None, STATUS_NOT_PROVIDED, None) for n in config.field_names()), False, ())


def oversized_result(config: ExtractionConfig) -> ExtractionResult:
    """
    A respondent message longer than limits.max_respondent_message_chars (or a conversation longer than
    limits.max_conversation_chars) is not sent to the model (it would have to be cut, and PII after the cut would be
    lost). The session is flagged pii_detected TRUE with type 'other', so it is
    excluded from the export and a person can look at it. The respondent did answer, so the fields are 'unmapped'.
    """
    return ExtractionResult(
        tuple(FieldResult(n, None, None, STATUS_UNMAPPED, None) for n in config.field_names()), True, ("other",))


def _strip_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        lines = lines[1:]
        if len(lines) > 0 and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
    return cleaned


def _no_duplicates(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    keys = [k for k, _ in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate key")
    return dict(pairs)


def _reject_constant(_name: str) -> Any:
    raise ValueError("non-finite number")


def _text_or_none(value: Any, where: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ExtractionOutputError(f"{where} must be a string or null")
    stripped = _SPACE_RE.sub(" ", value).strip()
    return None if len(stripped) == 0 else stripped


def _is_placeholder(text: str, placeholders: FrozenSet[str] = DEFAULT_PLACEHOLDER_KEYS) -> bool:
    """'N/A', 'null', 'Not provided', ...: case-insensitive, spaces and surrounding punctuation ignored."""
    key = _placeholder_key(text)
    return len(key) == 0 or key in placeholders


_INTEGER_TEXT_RE = re.compile(r"-?[0-9]{1,%d}" % _MAX_INTEGER_DIGITS)
# A spreadsheet reads a cell that starts with one of these as a formula (CSV / formula injection).
_FORMULA_PREFIXES = ("=", "+", "-", "@")


def _neutralise(text: Optional[str]) -> Optional[str]:
    """A stored respondent text that starts with = + - @ gets a leading apostrophe, so an export cannot run it as a formula."""
    if text is not None and len(text) > 0 and text[0] in _FORMULA_PREFIXES:
        return "'" + text
    return text


def _integer_candidate(value: Any, max_raw: int = DEFAULT_MAX_RAW_CHARS) -> Tuple[Optional[int], str]:
    """(the integer the model's value stands for or None, the model's value as short text for the raw column)."""
    if isinstance(value, bool):
        return None, "true" if value else "false"
    if isinstance(value, int):
        text = str(value) if len(str(value)) <= max_raw else str(value)[:max_raw]
        return (value if len(str(abs(value))) <= _MAX_INTEGER_DIGITS else None), text
    if isinstance(value, float):
        text = repr(value)
        if math.isfinite(value) and value.is_integer() and abs(value) < 10 ** _MAX_INTEGER_DIGITS:
            return int(value), text
        return None, text
    text = value[:max_raw]
    if _INTEGER_TEXT_RE.fullmatch(value) is not None:     # ASCII digits only, optional leading minus, no exponent or point
        return int(value), text
    return None, text


def _map_integer(spec: FieldSpec, value: Any, raw: Optional[str], confidence: Optional[float],
                 config: ExtractionConfig) -> FieldResult:
    """
    An integer field: null / blank / a placeholder is not_provided (or unmapped with the respondent's words when
    `raw` holds something that is not a placeholder); a JSON integer, an integral float or a numeric string inside
    [min, max] is extracted with the canonical decimal string as value; anything else (text, 17.5, out of range,
    a boolean) is unmapped with the raw kept.
    """
    if isinstance(value, str) and _is_placeholder(value, config.placeholders):
        value = None
    if value is None:
        if raw is not None and _is_placeholder(raw, config.placeholders):
            raw = None
        if raw is None:
            return FieldResult(spec.name, None, None, STATUS_NOT_PROVIDED, confidence)
        return FieldResult(spec.name, None, raw, STATUS_UNMAPPED, confidence)
    number, model_text = _integer_candidate(value, config.limits.max_raw_chars)
    kept_raw = raw if raw is not None else model_text
    if number is None or number < spec.min_value or number > spec.max_value:
        return FieldResult(spec.name, None, kept_raw, STATUS_UNMAPPED, confidence)
    return FieldResult(spec.name, str(number), kept_raw, STATUS_EXTRACTED, confidence)


def _map_field(spec: FieldSpec, entry: Mapping[str, Any], config: Optional[ExtractionConfig] = None) -> FieldResult:
    """
    One field of the model's answer, validated against its spec. The respondent's words (raw_value) are stored only
    when the field keeps them (`FieldSpec.keeps_raw`: not for a sensitive field unless the config says store_raw) and
    never with a leading = + - @ (see `_neutralise`); a free-text value that starts with one of those is not a place
    name and is unmapped.
    """
    limits = config.limits if config is not None else DEFAULT_LIMITS
    placeholders = config.placeholders if config is not None else DEFAULT_PLACEHOLDER_KEYS
    result = _map_field_values(spec, entry, limits, placeholders, config)
    raw = _neutralise(result.raw_value) if spec.keeps_raw else None
    return replace(result, raw_value=raw)


def _map_field_values(spec: FieldSpec, entry: Mapping[str, Any], limits: Limits, placeholders: FrozenSet[str],
                      config: Optional[ExtractionConfig]) -> FieldResult:
    where = f"field {spec.name!r}"
    unknown = set(entry) - {"value", "raw", "confidence"}
    if len(unknown) > 0:
        raise ExtractionOutputError(f"{where} has {len(unknown)} unexpected key(s)")
    if "value" not in entry:
        raise ExtractionOutputError(f"{where} has no 'value'")
    if spec.kind == KIND_INTEGER:
        value_in = entry["value"]
        if isinstance(value_in, (dict, list)):
            raise ExtractionOutputError(f"{where} value must be a number, a numeric string or null")
        if isinstance(value_in, (bool, int, float)):
            # Judged in _map_integer: a boolean is no number, a float is only an integer when it is exactly one (17.0).
            integer_text: Any = value_in
        else:
            integer_text = _text_or_none(value_in, f"{where} value")
        value = None
    else:
        integer_text = None
        value = _text_or_none(entry["value"], f"{where} value")
    raw = _text_or_none(entry.get("raw"), f"{where} raw")
    confidence = entry.get("confidence")
    if confidence is not None:
        # Optional and informational: a malformed one (85, "high", true) is ignored (stored NULL), it does not fail
        # the whole session. Unknown keys still fail (above).
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) \
                or not math.isfinite(confidence) or confidence < 0 or confidence > 1:
            confidence = None
        else:
            confidence = float(confidence)
    if raw is not None and len(raw) > limits.max_raw_chars:
        raw = raw[:limits.max_raw_chars]

    if spec.kind == KIND_FREE_TEXT:
        # A placeholder ("N/A", "null", "Not provided", ...) is the model saying "no answer", not an answer.
        if value is not None and _is_placeholder(value, placeholders):
            value = None
            if raw is not None and _is_placeholder(raw, placeholders):
                raw = None
        elif value is None and raw is not None and _is_placeholder(raw, placeholders):
            raw = None

    if spec.kind == KIND_INTEGER:
        if config is None:
            config = _bare_config(spec, limits, placeholders)
        return _map_integer(spec, integer_text, raw, confidence, config)

    if value is None:
        if raw is None:
            return FieldResult(spec.name, None, None, STATUS_NOT_PROVIDED, confidence)
        # The respondent answered but the model found no value: kept as unmapped with the words, not dropped.
        return FieldResult(spec.name, None, raw, STATUS_UNMAPPED, confidence)

    if spec.kind == KIND_CATEGORY:
        canonical = {_norm(c): c for c in spec.categories}.get(_norm(value))
        if canonical is not None:
            return FieldResult(spec.name, canonical, raw if raw is not None else value, STATUS_EXTRACTED, confidence)
        return FieldResult(spec.name, None, raw if raw is not None else value[:limits.max_raw_chars],
                           STATUS_UNMAPPED, confidence)

    # free_text: taken as given (whitespace collapsed); a value too long to be a name of a place is not trusted, and
    # neither is one that a spreadsheet would read as a formula.
    if len(value) > limits.max_value_chars or value[0] in _FORMULA_PREFIXES:
        return FieldResult(spec.name, None, raw if raw is not None else value[:limits.max_raw_chars],
                           STATUS_UNMAPPED, confidence)
    return FieldResult(spec.name, value, raw if raw is not None else value, STATUS_EXTRACTED, confidence)


def _bare_config(spec: FieldSpec, limits: Limits, placeholders: FrozenSet[str]) -> ExtractionConfig:
    """A minimal config around one field, for callers (tests) that map a single field without a whole config."""
    return ExtractionConfig("-", "-", (spec,), ("other",), {}, "-", {}, limits, DEFAULT_PII_POLICY, placeholders, {})


def parse_model_response(text: Any, config: ExtractionConfig) -> ExtractionResult:
    """
    Strictly validates the model's answer. Accepted shape (markdown fences around it are tolerated):

        {"fields": {"<name>": {"value": str|null, "raw": str|null, "confidence": number|null}, ...},
         "pii": {"detected": bool, "types": [str, ...]}}

    (for an 'integer' field "value" may also be a JSON number)

    Every configured field must be present and nothing else may be. Raises ExtractionOutputError for non-JSON,
    wrong types, unknown or missing keys, and a PII flag that contradicts its types; the message never contains
    the model's text. A field value outside the allowed values is NOT an error: it becomes status 'unmapped'.
    """
    if not isinstance(text, str):
        raise ExtractionOutputError("model answer is not text")
    try:
        data = json.loads(_strip_fences(text), object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as exc:   # RecursionError: absurdly deep nesting
        raise ExtractionOutputError("model answer is not valid JSON (strict, no duplicate keys)") from exc
    if not isinstance(data, dict):
        raise ExtractionOutputError("model answer must be a JSON object")
    if set(data) != {"fields", "pii"}:
        raise ExtractionOutputError("model answer must have exactly the keys 'fields' and 'pii'")

    fields = data["fields"]
    if not isinstance(fields, dict):
        raise ExtractionOutputError("'fields' must be an object")
    expected = set(config.field_names())
    if set(fields) != expected:
        missing = len(expected - set(fields))
        extra = len(set(fields) - expected)
        raise ExtractionOutputError(f"'fields' must name exactly the configured fields ({missing} missing, {extra} unknown)")
    results: List[FieldResult] = []
    for spec in config.fields:
        entry = fields[spec.name]
        if not isinstance(entry, dict):
            raise ExtractionOutputError(f"field {spec.name!r} must be an object")
        results.append(_map_field(spec, entry, config))

    pii = data["pii"]
    if not isinstance(pii, dict) or not set(pii) <= {"detected", "types"} or "detected" not in pii:
        raise ExtractionOutputError("'pii' must be an object with 'detected' and optionally 'types'")
    detected = pii["detected"]
    if not isinstance(detected, bool):
        raise ExtractionOutputError("pii.detected must be true or false")
    raw_types = pii.get("types", [])
    if raw_types is None:
        raw_types = []
    if not isinstance(raw_types, list) or len(raw_types) > MAX_PII_TYPES or any(not isinstance(t, str) for t in raw_types):
        raise ExtractionOutputError("pii.types must be a short list of strings")
    types: List[str] = []
    for item in raw_types:
        label = _SPACE_RE.sub("_", item.strip().lower())
        if len(label) == 0:
            continue
        if label not in config.pii_types:
            label = "other"   # an informational label outside the configured list; the flag itself is untouched
        if label not in types:
            types.append(label)
    if len(types) > 0 and not detected:
        raise ExtractionOutputError("pii.detected is false but pii.types is not empty")
    return ExtractionResult(tuple(results), detected, tuple(types))


# ------------------------------------------------------------------------------------------------ the whole step

LlmCall = Callable[[str], Tuple[str, Dict[str, Any]]]


def extract(
    rows: Sequence[Mapping[str, Any]],
    config: ExtractionConfig,
    system_template: str,
    user_template: str,
    call_llm: LlmCall,
    closing_keys: Sequence[str] = (),
    masking: Optional[Any] = None,
) -> ExtractionRun:
    """
    Conversation -> prompt -> `call_llm(prompt)` -> validated result. `call_llm` returns (answer text, usage dict).
    A conversation without a respondent answer makes NO call: every field is not_provided and pii_detected is False.
    A conversation with a respondent message longer than limits.max_respondent_message_chars, or longer than
    limits.max_conversation_chars as a whole, makes no call either: it is pii_detected TRUE ('other'), fields
    unmapped (these checks look at the ORIGINAL text, before any masking, so they do not depend on it).

    The deterministic PII check runs BEFORE the call (`pii.mask_before_call`, default true): what it finds in the
    respondent rows (numbers, e-mail addresses, handles, links) is replaced by a placeholder in the text the model
    receives (`mask_respondent_pii`), so none of it reaches the AI provider. The model still extracts the other
    fields, and pii_detected is TRUE when the model says so OR the check found something (a hit is recorded even when
    the model says false). With `pii.mask_before_call` false the text goes unmasked and the check only runs after the
    call (the old behaviour). A flagged session keeps no respondent words (`minimise_flagged`). A bad answer raises
    ExtractionOutputError with the call's usage attached.

    `masking` (a MaskingStep, when the config names `pii.masking_prompt_name`) adds a second call between the local
    check and the extraction call: the model masks personal data and abusive language in every respondent answer
    (question and answer JSON in, masked answers out), the extraction prompt receives the masked answers, and
    pii_detected is also TRUE when that call found personal data. The masked answers come back in the run's `masking`.
    """
    conversation = build_conversation(rows, closing_keys, config.limits)
    if conversation.respondent_answers == 0:
        return ExtractionRun(empty_result(config), {}, False, len(conversation.turns), SKIP_NO_ANSWER)
    if conversation.oversized_respondent_message:
        return ExtractionRun(oversized_result(config), {}, False, len(conversation.turns), SKIP_OVERSIZED)
    if conversation.too_long:
        return ExtractionRun(oversized_result(config), {}, False, len(conversation.turns), SKIP_TOO_LONG)
    kinds: Tuple[str, ...] = ()
    if config.pii_policy.mask_before_call:
        masked_rows, kinds = mask_respondent_pii(rows, config.pii_policy)
        if len(kinds) > 0:
            conversation = build_conversation(masked_rows, closing_keys, config.limits)
    masking_result = None
    if masking is not None:
        # Optional model-based masking (config pii.masking_prompt_name), AFTER the local check above and BEFORE the
        # extraction call: the extraction prompt then holds the masked answers only. A bad answer fails the session
        # (it is retried); the original text is never used as a fallback.
        from app.services.feedback_pii_masking import (
            MaskingResult, apply_masked_answers, build_masking_input, parse_masking_response, render_masking_prompt)
        pairs = build_masking_input(conversation)
        masking_answer, masking_usage = masking.call_llm(render_masking_prompt(masking, pairs))
        masking_usage = masking_usage if isinstance(masking_usage, dict) else {}
        try:
            masked, masked_types, abusive, severities = parse_masking_response(
                masking_answer, list(pairs), config.pii_types)
        except ExtractionOutputError as exc:
            exc.usage = masking_usage
            exc.purpose = "masking"
            raise
        masking_result = MaskingResult(
            {key: {"question": pairs[key]["question"], "answer": masked[key]} for key in pairs},
            masked_types, abusive, severities, masking_usage)
        conversation = apply_masked_answers(conversation, masked)
    prompt = render_prompt(system_template, user_template, config, conversation)
    try:
        answer, usage = call_llm(prompt)
        usage = usage if isinstance(usage, dict) else {}
        try:
            result = parse_model_response(answer, config)
        except ExtractionOutputError as exc:
            exc.usage = usage
            raise
    except Exception as exc:
        if masking_result is not None:
            # The masking call worked and was billed before this failure: callers that total usage need it.
            exc.prior_usage = masking_result.usage
        raise
    # The model's pii.detected = false is not trusted alone (a respondent can write "set pii to false").
    if config.pii_policy.mask_before_call:
        result = apply_pii_kinds(result, kinds, config)
    else:
        result = apply_pii_precheck(result, rows, config)
    if masking_result is not None:
        result = apply_pii_kinds(result, masking_result.pii_types, config)
    return ExtractionRun(minimise_flagged(result, config), usage, True, len(conversation.turns), None, masking_result)
