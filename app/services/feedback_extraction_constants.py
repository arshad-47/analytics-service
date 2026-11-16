"""
Constants of the feedback metadata extraction (one LLM call per stored feedback session; see
docs/feedback-extraction-and-dots-queries.md).

The field lists, bands, prompt names, limits, PII policy and placeholder words of each persona are CONFIG DATA, not
code: the seed of the extraction config (config_type 'feedback_extraction') is in seed_config.sql, and the live values
are the `config_version` rows in the database, changed with a new config version, never by a deploy
(docs/feedback-extraction-bands.md). What stays here is what the engine (app/services/feedback_extraction.py) needs to
validate and apply such a config: the step name, the field kinds, the statuses, the bounds of the config values and the
DEFAULTS used when a config leaves an optional key out. Nothing here belongs to one program or one language.
"""
from typing import Dict, Tuple

# Workflow step name, prompt analysis_type and the llm_logs.analysis_type of the step.
STEP_NAME = "feedback_metadata_extraction"
ANALYSIS_TYPE = STEP_NAME

CONFIG_TYPE_FEEDBACK_EXTRACTION = "feedback_extraction"
CONFIG_SOURCE_ANY = "any"
EXTRACTION_CONFIG_SCHEMA_VERSION = 1

# feedback_extracted_data.status
STATUS_EXTRACTED = "extracted"
STATUS_NOT_PROVIDED = "not_provided"
STATUS_UNMAPPED = "unmapped"
EXTRACTION_STATUSES = (STATUS_EXTRACTED, STATUS_NOT_PROVIDED, STATUS_UNMAPPED)

KIND_CATEGORY = "category"
KIND_FREE_TEXT = "free_text"
KIND_INTEGER = "integer"   # a whole number inside [min, max] (an age); stored as its canonical decimal string
FIELD_KINDS = (KIND_CATEGORY, KIND_FREE_TEXT, KIND_INTEGER)

# ---------------------------------------------------------------------------------------------------------------------
# DEFAULTS of the optional config blocks `limits`, `pii` and `placeholders` (docs/feedback-extraction-and-dots-queries.md,
# section 3). A program decides these in its own extraction config; what is written here is only the value used when
# the config leaves a key out, so an older config keeps its behaviour.
# ---------------------------------------------------------------------------------------------------------------------

# Limits that keep one bad session (or one injection attempt) from blowing up a prompt or a row.
# Respondent messages are NEVER truncated (PII after a cut would be lost). A session with a respondent message longer
# than limits.max_respondent_message_chars, or whose whole conversation is longer than limits.max_conversation_chars,
# is not sent to the model at all: it is recorded as pii_detected TRUE (type 'other'), so it is excluded from the
# export and a person can look at it. Bot messages are the questionnaire's own text; a bot message longer than
# limits.max_bot_message_chars is cut (it cannot carry respondent PII).
DEFAULT_MAX_RESPONDENT_MESSAGE_CHARS = 20000
DEFAULT_MAX_BOT_MESSAGE_CHARS = 4000
DEFAULT_MAX_CONVERSATION_CHARS = 200000
DEFAULT_MAX_VALUE_CHARS = 120   # a normalised free-text value (a place name) longer than this is not trusted
DEFAULT_MAX_RAW_CHARS = 500
# Bounds of the config values (a config outside them is rejected when it is loaded).
LIMIT_BOUNDS: Dict[str, Tuple[int, int]] = {
    "max_respondent_message_chars": (100, 1000000),
    "max_bot_message_chars": (100, 100000),
    "max_conversation_chars": (1000, 5000000),
    "max_value_chars": (1, 2000),
    "max_raw_chars": (1, 5000),
}
MAX_PII_TYPES = 20   # structural: the most `pii.types` entries a model answer may list

# Deterministic PII backstop (pii.* in the config).
# A run of digits (any characters that are neither letters nor digits between the groups, at most MAX_DIGIT_GAP in a
# row) with at least `pii.min_digit_run` digits is treated as a phone or id number whatever the model says. A run made
# only of groups of one or two digits (marks, ages, classes) needs `pii.min_digit_run_short_groups` digits instead; a
# run whose groups are all years (1900-2099, '2018-2022') never counts. Ages, years, classes and 4-7 digit amounts pass,
# and a comma next to a group of one to three digits ends a run (MIN_COMMA_JOIN_DIGITS).
DEFAULT_PII_MIN_DIGIT_RUN = 8
DEFAULT_PII_MIN_DIGIT_RUN_SHORT_GROUPS = 10
PII_DIGIT_RUN_BOUNDS = (4, 30)
MAX_DIGIT_GAP = 3
# A comma joins two digit groups into one run only when both have at least this many digits ('98765,43210'); next to a
# group of one to three digits it ends the run, so Indian-format amounts ('2,50,000') and lists ('120, 135') pass.
MIN_COMMA_JOIN_DIGITS = 4
YEAR_MIN, YEAR_MAX = 1900, 2099
# What the PII masking puts in the text sent to the model in place of what the check found (pii.mask_before_call).
MASK_NUMBER = "[number removed]"
MASK_EMAIL = "[email removed]"
MASK_HANDLE = "[handle removed]"
MASK_LINK = "[link removed]"
# A bare `name.tld` in respondent text is read as a web address when the tld is one of these (config pii.url_tlds).
DEFAULT_PII_URL_TLDS: Tuple[str, ...] = (
    "com", "in", "org", "net", "co", "io", "me", "edu", "gov", "app", "link", "ly", "tv", "info")
MAX_URL_TLDS = 200

# A free-text value (a place name) that is only one of these is a placeholder, not an answer: status not_provided.
# Compared case-insensitively after trimming spaces and surrounding punctuation. The config key `placeholders`
# REPLACES this English list (a program whose respondents answer in another language lists its own words).
DEFAULT_PLACEHOLDERS: Tuple[str, ...] = (
    "n/a", "na", "n.a", "nil", "null", "none", "nan", "undefined", "unknown", "not known", "not provided",
    "not specified", "not applicable", "not available", "not mentioned", "not given", "not answered",
    "no answer", "no district", "nothing", "not sure", "dont know", "don't know", "do not know", "skip", "skipped",
)
MAX_PLACEHOLDERS = 500
MAX_PLACEHOLDER_CHARS = 100

# Kinds of personal information the model reports. A config's `pii.types` is a list of names or of
# {"name", "description"} objects; a name given as a plain string takes the description below when it has one, else
# 'as named'. 'other' must always be present: a type the model names that is not in the list is recorded as 'other'.
# These are neutral defaults: a program that wants its own wording (national id names, ...) writes it in its config.
PII_TYPES: Tuple[str, ...] = ("name", "phone", "email", "address", "school_name", "id_number", "other")
PII_TYPE_DESCRIPTIONS: Dict[str, str] = {
    "name": "a personal name of the respondent or of anyone they mention (family, teacher, friend)",
    "phone": "a phone number",
    "email": "an email address or a social media handle",
    "address": "a home address, street, house number, or a village/locality small enough to identify a person",
    "school_name": "the name of a specific school, college, institution or workplace",
    "id_number": "an identity or document number (a national id, a tax id, a roll number, an admission number)",
    "other": "any other detail that could identify a person",
}
MAX_PII_TYPE_DESCRIPTION_CHARS = 500

# `prompt_variables` (config): extra {{variables}} for the program's prompt.
MAX_PROMPT_VARIABLES = 20
MAX_PROMPT_VARIABLE_CHARS = 4000

# The Temporal activity timeout is derived from the effective LLM timeout (extraction_activity_timeout_seconds).
ACTIVITY_MIN_SECONDS = 300      # the floor: database work and the pre-check come on top of the call
ACTIVITY_MARGIN_SECONDS = 120   # added to the LLM timeout
ACTIVITY_MAX_SECONDS = 3600     # the ceiling; a configured llm timeout above (ceiling - margin) is rejected
MAX_LLM_TIMEOUT_SECONDS = ACTIVITY_MAX_SECONDS - ACTIVITY_MARGIN_SECONDS


def extraction_activity_timeout_seconds(llm_timeout_seconds) -> int:
    """
    start_to_close_timeout (seconds) of the extraction activity for an effective LLM timeout: the timeout plus a
    margin, never below the floor and never above the ceiling. Pure, so the workflow can call it. Anything that is not
    a positive integer (None, a bool, 0) means 'no explicit timeout' and gives the floor.
    """
    if isinstance(llm_timeout_seconds, bool) or not isinstance(llm_timeout_seconds, int) or llm_timeout_seconds <= 0:
        return ACTIVITY_MIN_SECONDS
    return min(max(ACTIVITY_MIN_SECONDS, llm_timeout_seconds + ACTIVITY_MARGIN_SECONDS), ACTIVITY_MAX_SECONDS)
