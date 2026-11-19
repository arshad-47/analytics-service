"""
Pure parsing, validation and event building for the `conversation_rows` adapter of `feedback` uploads (one CSV row per
message: a bot conversation export such as Mitra's). The config's `adapter` selects this module; the CSV column names
come from the config's optional `column_map` (default: the role names), never from code.

No DB, Kafka, network or settings access here: everything the functions need is passed in, so the
module can be tested in isolation. The Kafka event shape and the version-1 config JSON are defined
in app/services/feedback_constants.py; the rules are in docs/feedback-upload-plan.md.

Flow used by the upload service:
    df = parse_feedback_csv(file_bytes)                      # ValueError -> reject the upload
    errors = check_feedback_file(df, config, expected_session_type)  # file-level; non-empty -> reject whole file
    events, skipped = build_feedback_events(df, config, ...)   # one event per good session; bad ones skipped
"""
import csv
import io
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from app.services.datetime_parsing import (
    DEFAULT_ACCEPTED_FORMATS,
    DEFAULT_DAY_FIRST_FORMATS,
    TEXT_MONTH_FORMATS,
    parse_datetime_text,
)
from app.services.feedback_adapter_config import (
    config_adapter,
    is_safe_id,
    required_column_headers,
    resolve_column_map,
    resolve_completion_map,
    validate_adapter_config,
)
from app.services.feedback_config_validation import validate_feedback_config_keys
from app.services.feedback_versions import BOT_ID_RULE, is_valid_bot_id, parse_versions, stamp_version
from app.services.feedback_constants import (
    ADAPTER_CONVERSATION_ROWS,
    CONFIG_KEY_EXPECT_STAGES,
    FEEDBACK_REPORT_TYPE,
    MAX_SEQUENCE,
    SYNTHESISED_FIRST_QUESTION_SEQUENCE,
)

logger = logging.getLogger("analytics_service.api.services.feedback_uploads")

_INT_RE = re.compile(r"^[0-9]+$")
# C0 control characters except TAB (0x09), LF (0x0a) and CR (0x0d), plus DEL (0x7f). Zero-width and right-to-left marks
# (U+200B..U+200F, U+202A..U+202E), emoji and every other printable character are NOT in the class: they are part of
# what a respondent typed. NUL (0x00) is in the class for frames built without parse_feedback_csv.
_CONTROL_CHARS_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# Optional config keys of this adapter, each with a default in code (a config that does not set them behaves as before
# plus these defaults): `empty_message_placeholder` and `datetime.formats`.
DEFAULT_EMPTY_MESSAGE_PLACEHOLDER = "[no text]"
CONFIG_KEY_EMPTY_MESSAGE_PLACEHOLDER = "empty_message_placeholder"
# The date formats read (ISO 8601, day-first numeric, written-out month) live in app/services/datetime_parsing.py, shared with
# the story / discussion upload; `datetime.formats` of the config adds more (see _datetime_formats).
# Row numbers listed in a problem message (the rest is only counted). Problems carry counts, row numbers and column
# names, NEVER a cell value: a misaligned or hostile file can hold respondent text in any column, and the problems go to
# the DLQ, the logs, csv_uploads.meta_data and the 400 body.
_SAMPLE_SIZE = 3


# ---------------------------------------------------------------------------------------------
# Parsing and file-level checks
# ---------------------------------------------------------------------------------------------

# The key of df.attrs that carries the number of NUL bytes parse_feedback_csv removed (0 when none).
NUL_BYTES_ATTR = "nul_bytes_removed"


def parse_feedback_csv(file_bytes: bytes) -> pd.DataFrame:
    """
    Parse an uploaded Mitra CSV with every cell as a string.

    `dtype=str, keep_default_na=False` keeps the answers "NA" / "None" (pandas would turn them into
    NaN) and leaves empty cells as "". A UTF-8 BOM is dropped. Raises ValueError when the file cannot
    be parsed or has no data rows.
    """
    if file_bytes is None or len(file_bytes) == 0:
        raise ValueError("The uploaded CSV file is empty.")
    # pandas' C parser ends a cell at a NUL byte and silently drops the rest of it ("options<NUL> today" would be read
    # as "options"), so the bytes are removed BEFORE parsing and counted; the count goes to the upload report through
    # df.attrs. Only the count is logged, never text.
    nul_bytes = file_bytes.count(b"\x00")
    if nul_bytes > 0:
        file_bytes = file_bytes.replace(b"\x00", b"")
        logger.warning("Removed %d NUL byte(s) from the uploaded feedback CSV before parsing", nul_bytes)
        if len(file_bytes) == 0:
            raise ValueError("The uploaded CSV file is empty.")
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        raise ValueError("The uploaded CSV file is empty.")
    except UnicodeDecodeError as exc:
        raise ValueError(f"The uploaded CSV file is not valid UTF-8 text: {exc}")
    except (pd.errors.ParserError, csv.Error) as exc:
        raise ValueError(f"The uploaded CSV file could not be parsed: {exc}")
    df.columns = [str(c).strip() for c in df.columns]
    if len(df) == 0:
        raise ValueError("The uploaded CSV file has a header but no data rows.")
    df.attrs[NUL_BYTES_ATTR] = nul_bytes
    return df


def _column_lookup(df: pd.DataFrame) -> Dict[str, str]:
    """lower-cased, stripped column name -> actual column name (first one wins)."""
    lookup: Dict[str, str] = {}
    for col in df.columns:
        lookup.setdefault(str(col).strip().lower(), col)
    return lookup


def _bot_id_header(config: Dict[str, Any]) -> str:
    """
    The CSV header of the bot id (config["column_map"]["bot_id"], default 'bot_id'; the Mitra export names it per
    config, for example company_bot_id). A Mitra file MUST carry it: a session's questionnaire version is the rank
    of its bot id inside the persona, so a file without it can not be stamped.
    """
    return resolve_column_map(config)["bot_id"]


def check_feedback_file(df: pd.DataFrame, config: Dict[str, Any], expected_session_type: str) -> List[str]:
    """
    File-level checks. Returns error strings; an empty list means the file may be processed.

    - every required role (config["column_map"] over the default role names; session_type, session_status, session,
      submission_id, language, sequence, message, created_at, stage, sender_id) has its CSV column, matched
      case-insensitively; so has the bot_id role (the questionnaire version of a bot session is the rank of its bot
      id, see _bot_id_header);
    - every row's session_type is present and equals `expected_session_type` (a file holding another
      persona is rejected whole; only counts are reported, never the cell values). The values of the bot id are
      checked per session by build_feedback_events: one persona runs several bots over time and the same file may
      hold V1 and V2 sessions, each session carrying exactly one bot id.
    Blank submission_id / session cells are session-level problems (see build_feedback_events), not
    file-level ones. Extra columns are fine.
    """
    errors: List[str] = []
    lookup = _column_lookup(df)
    headers = required_column_headers(config)

    missing = [header for header in (*headers.values(), _bot_id_header(config)) if header.lower() not in lookup]
    if missing:
        errors.append("Missing required column(s): " + ", ".join(missing))

    bot_col = lookup.get(_bot_id_header(config).lower())
    if bot_col is not None and int((df[bot_col].astype(str).str.strip() != "").sum()) == 0:
        errors.append("bot_id is blank in every row; every session needs the bot id that asked its questions")

    type_col = lookup.get(headers["session_type"].lower())
    if type_col is None:
        return errors

    expected = str(expected_session_type).strip()
    session_types = df[type_col].astype(str).str.strip()
    blank_rows = int((session_types == "").sum())
    other_rows = int(((session_types != "") & (session_types != expected)).sum())
    if other_rows > 0:
        distinct = len({t for t in session_types.unique().tolist() if t != "" and t != expected})
        errors.append(
            f"{other_rows} row(s) carry a session_type other than {expected} ({distinct} other value(s)); only "
            f"session_type {expected} is allowed for this upload"
        )
    if blank_rows > 0:
        errors.append(f"session_type is blank in {blank_rows} row(s); every row must carry session_type {expected}")
    return errors


def report_unexpected_stages(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    The optional `expect_stages` check of a Mitra file (item 11): when the config lists expect_stages, count the stage
    values that are neither in expect_stages nor in closing_keys and the sessions whose rows carry one. REPORT ONLY: it
    never skips or rejects a session (stage names legitimately change between questionnaire versions). Blank stages
    (the opening row) are ignored; a session is (submission_id, session).

    COUNTS ONLY, never a stage value. A stage the config does not list is, by definition, not a value the service can
    vouch for: a misaligned or hostile file can hold anything in that column (a personal name such as `John_Smith`
    fits any "looks like a stage key" pattern; so does a phone number with a separator), and the report goes into the
    upload response and csv_uploads.meta_data. No filter on the shape of the text can tell a new stage key from a
    name, so no text from the file is copied. The operator who sees a non-zero count looks at the stage column of
    the file itself (or adds the new stage to expect_stages).
    Returns None when the config has no expect_stages, else
    {"sessions", "sessions_with_unexpected_stages", "unexpected_stage_count" (distinct unexpected values)}.
    """
    if CONFIG_KEY_EXPECT_STAGES not in config:
        return None
    known = set(config[CONFIG_KEY_EXPECT_STAGES]) | set(config.get("closing_keys", []))
    lookup = _column_lookup(df)
    roles = resolve_column_map(config)
    stage_col, sub_col, sess_col = (lookup.get(roles[r].lower()) for r in ("stage", "submission_id", "session"))
    if stage_col is None or sub_col is None or sess_col is None:
        return {"sessions": 0, "sessions_with_unexpected_stages": 0, "unexpected_stage_count": 0,
                "note": "stage, submission_id or session column is missing"}
    frame = pd.DataFrame({
        "sub": df[sub_col].astype(str).str.strip(),
        "sess": df[sess_col].astype(str).str.strip(),
        "stage": df[stage_col].astype(str).str.strip(),
    })
    sessions = len(frame[["sub", "sess"]].drop_duplicates())
    unknown = frame[(frame["stage"] != "") & (~frame["stage"].isin(known))].drop_duplicates()
    return {
        "sessions": sessions,
        "sessions_with_unexpected_stages": len(unknown[["sub", "sess"]].drop_duplicates()),
        "unexpected_stage_count": int(unknown["stage"].nunique()),
    }


# ---------------------------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------------------------

def _is_blank(value: Optional[str]) -> bool:
    return value is None or value.strip() == ""


def _rows(positions: List[int]) -> str:
    """'data row(s) 3, 7, 9 ...' from 0-based frame positions (1-based data row numbers, header not counted)."""
    numbers = ", ".join(str(i + 1) for i in positions[:_SAMPLE_SIZE])
    return f"data row(s) {numbers}" + (" ..." if len(positions) > _SAMPLE_SIZE else "")


def _distinct(values: List[str]) -> List[str]:
    seen: Dict[str, None] = {}
    for v in values:
        seen.setdefault(v, None)
    return list(seen)


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _resolve_timezone(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        raise ValueError(f"config.datetime.fallback_timezone {name!r} is not a known timezone")


_parse_datetime = parse_datetime_text   # (the name this module has always used)


def _clean_text(text: str) -> str:
    """The text with every C0 control character (except TAB, LF, CR) and DEL replaced by one space each."""
    return _CONTROL_CHARS_RE.sub(" ", text)


def _datetime_formats(config: Dict[str, Any]) -> Tuple[str, ...]:
    """The formats read after ISO 8601: the defaults (day-first and written-out month) followed by the optional config
    `datetime.formats` (a list of strptime patterns). Raises ValueError naming the key when the list is malformed."""
    block = config.get("datetime", {})
    if not isinstance(block, dict):
        raise ValueError("Feedback config 'datetime' must be an object")
    extra = block.get("formats")
    if extra is None:
        return DEFAULT_ACCEPTED_FORMATS
    if not isinstance(extra, list) or not all(isinstance(f, str) and len(f.strip()) > 0 for f in extra):
        raise ValueError("Feedback config 'datetime.formats' must be a list of non-empty strptime patterns")
    for pattern in extra:
        try:
            datetime.strptime(datetime(2026, 1, 2, 3, 4, 5).strftime(pattern), pattern)
        except ValueError:
            raise ValueError("Feedback config 'datetime.formats' has a pattern that does not read its own output")
    return DEFAULT_ACCEPTED_FORMATS + tuple(f for f in extra if f not in DEFAULT_ACCEPTED_FORMATS)


def _folded_completion_map(completion_map: Dict[str, str]) -> Dict[str, str]:
    """lower-cased key -> platform status, so a status word is read ignoring case (the file value is trimmed already).
    Raises ValueError when two keys differ only by case and map to different statuses: which one a changed spelling
    means would be a guess."""
    folded: Dict[str, str] = {}
    for word, target in completion_map.items():
        key = word.strip().lower()
        if key in folded and folded[key] != target:
            raise ValueError(
                "Feedback config 'completion_map' has two keys that differ only by case and map to different statuses"
            )
        folded[key] = target
    return folded


def _empty_message_placeholder(config: Dict[str, Any]) -> str:
    """The text kept for a respondent row without any text (optional config `empty_message_placeholder`)."""
    value = config.get(CONFIG_KEY_EMPTY_MESSAGE_PLACEHOLDER, DEFAULT_EMPTY_MESSAGE_PLACEHOLDER)
    if not isinstance(value, str) or len(value.strip()) == 0 or _CONTROL_CHARS_RE.search(value) is not None:
        raise ValueError(
            f"Feedback config '{CONFIG_KEY_EMPTY_MESSAGE_PLACEHOLDER}' must be a non-empty string without control characters"
        )
    return value


def _normalise_answer(text: str) -> str:
    return " ".join(text.strip().lower().split())


def _check_config(config: Dict[str, Any]) -> None:
    if config_adapter(config) != ADAPTER_CONVERSATION_ROWS:
        raise ValueError(
            f"Feedback config adapter must be {ADAPTER_CONVERSATION_ROWS!r} for this builder, got {config['adapter']!r}"
        )
    for key in ("session_type", "bot_sender_ids", "message_map", "sequence_offset", "questions",
                "questionnaire_versions"):
        if key not in config:
            raise ValueError(f"Feedback config is missing '{key}'")
    if not isinstance(config["session_type"], str) or config["session_type"].strip() == "":
        raise ValueError("Feedback config 'session_type' must be a non-empty string")
    opening_mode = config.get("opening_row", {}).get("mode", "none")
    if opening_mode not in ("auto", "none"):
        raise ValueError(f"Feedback config opening_row.mode must be 'auto' or 'none', got {opening_mode!r}")
    parse_versions(config)
    for kind in ("bot", "respondent"):
        entry = config["message_map"].get(kind)
        if not isinstance(entry, dict) or "message" not in entry or "translated_message" not in entry:
            raise ValueError(f"Feedback config message_map.{kind} must map 'message' and 'translated_message'")
    if len(config["questions"]) == 0:
        raise ValueError("Feedback config has an empty 'questions' catalog")
    # adapter / max_csv_upload_bytes / ingestion_schema / process_steps (+ optional expect_stages) live in the config too
    validate_feedback_config_keys(config)
    validate_adapter_config(config)
    _datetime_formats(config)
    _empty_message_placeholder(config)
    _folded_completion_map(resolve_completion_map(config))


# ---------------------------------------------------------------------------------------------
# Event building
# ---------------------------------------------------------------------------------------------

def build_feedback_events(
    df: pd.DataFrame,
    config: Dict[str, Any],
    *,
    program_info: Dict[str, Any],
    leader_info: Dict[str, Any],
    tenant_code: str,
    source: str,
    published_at: Optional[str] = None,
    report: Optional[Dict[str, int]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build one Kafka event per good session.

    `report`, when given, is filled with WARNING counts (a warning never skips a session):
    rows_without_message_time and respondent_rows_without_text count rows of ACCEPTED sessions (a row whose created_at
    is missing or unparsable gets messageAt null; a respondent row without any text is kept with the config's
    empty_message_placeholder), cells_with_control_characters counts the message / translated_message cells of the
    whole file whose control characters were replaced by spaces. Counts only, no text.
    Events are returned ordered by bot id ascending (numeric), first appearance first inside one bot id: the consumer
    ranks the bot ids of a persona by number as the sessions arrive, so a late lower bot id must not come after the
    sessions of a higher one inside one upload.

    program_info / leader_info are {"id": ..., "name": ...}. Returns (events, skipped): events follow
    the contract in app/services/feedback_constants.py; skipped lists every session that failed a
    session-level check as {"submissionId", "sessionId", "problems": [...]} (None for a blank id),
    with ALL of its problems. Other sessions still produce events.

    Rules (see the plan, section 3 and the Sequence numbering paragraph):
    - identity of a session = (submission_id, session); each id must map to exactly one of the other. A row with a
      blank submission_id (or session) belongs to the session its other token names, and that WHOLE session is
      skipped (never accepted without that row);
    - opening row: when config["opening_row"]["mode"] is "auto" and the first CSV row (sequence 1) has a
      blank sender_id, stage and created_at, it is the bot's opening message: kept as the first bot row at
      stored sequence 1 (no message time), with the question_key of the next row's stage, and every other
      row keeps its CSV sequence. Otherwise (old-shape file): stored sequence = CSV sequence +
      config["sequence_offset"] and the unlogged first question is synthesised at sequence 1 from
      config["questions"] (the entry whose key is the stage of the session's first CSV row), only when the
      CSV starts at sequence 1 (tail-only sessions are skipped);
    - submissionDate = the first real message with a valid created_at (not the opening row); questionnaireVersion = the version
      of config["questionnaire_versions"] in force at that instant; sessionType must equal
      config["session_type"]; botId is required (one per session, a positive integer in digits) and decides the
      STORED questionnaire version through its rank in the persona (the version stamped here from the windows stays
      in the event as input and is not what is stored for these sessions); botName is only recorded;
    - a sender_id outside config["bot_sender_ids"] is a respondent; parentSequence is null for bot
      rows and, for a respondent row, the stored sequence of the nearest preceding bot row
      (including the synthesised question). Nothing relies on sequence parity;
    - text: stored message = conversation language, translatedMessage = English, via
      config["message_map"] (bot rows swap the CSV columns).
    """
    _check_config(config)
    lookup = _column_lookup(df)
    roles = resolve_column_map(config)
    missing = [header for header in (*required_column_headers(config).values(), _bot_id_header(config))
               if header.lower() not in lookup]
    if missing:
        raise ValueError("Feedback CSV is missing required column(s): " + ", ".join(missing))

    fallback_tz = _resolve_timezone(config.get("datetime", {}).get("fallback_timezone", "UTC"))
    day_first_formats = _datetime_formats(config)
    placeholder = _empty_message_placeholder(config)
    counts = {"rows_without_message_time": 0, "respondent_rows_without_text": 0, "cells_with_control_characters": 0}
    published = published_at if published_at is not None else _format_utc(datetime.now(timezone.utc))

    def column(name: str) -> List[str]:
        """The cells of a role (its header comes from column_map); any other name is read as a CSV column name."""
        header = roles[name] if name in roles else name
        actual = lookup.get(header.strip().lower())
        if actual is None:
            return [""] * len(df)
        return df[actual].astype(str).tolist()

    submission_ids = [v.strip() for v in column("submission_id")]
    session_ids = [v.strip() for v in column("session")]
    statuses = [v.strip() for v in column("session_status")]
    bot_ids = [v.strip() for v in column("bot_id")]
    bot_names = [v.strip() for v in column("bot_name")]
    session_types = [v.strip() for v in column("session_type")]
    languages = [v.strip() for v in column("language")]
    sequences = [v.strip() for v in column("sequence")]
    stages = [v.strip() for v in column("stage")]
    senders = [v.strip() for v in column("sender_id")]
    created = column("created_at")
    # The control characters of a cell are replaced before anything reads it: a cell that holds only control
    # characters is then blank, and the emptiness check below sees the same text that is stored. The count is made per
    # cell of the two text columns (a column the message_map reads under another name is cleaned when it is read).
    cells_cleaned = 0

    def text_column(name: str) -> List[str]:
        nonlocal cells_cleaned
        cleaned: List[str] = []
        for cell in column(name):
            fixed = _clean_text(cell)
            if fixed != cell:
                cells_cleaned += 1
            cleaned.append(fixed)
        return cleaned

    csv_text = {
        "message": text_column("message"),
        "translated_message": text_column("translated_message"),
    }
    # message_map names CSV columns; resolve each to the extracted list (any other column is read ad hoc)
    mapped_columns: Dict[str, List[str]] = {}

    def mapped(name: str) -> List[str]:
        if name not in mapped_columns:
            mapped_columns[name] = csv_text[name] if name in csv_text else text_column(name)
        return mapped_columns[name]

    # Group row positions by session identity, in file order.
    groups: Dict[Tuple[str, str], List[int]] = {}
    for i in range(len(df)):
        groups.setdefault((submission_ids[i], session_ids[i]), []).append(i)

    sessions_of_submission: Dict[str, List[str]] = {}
    submissions_of_session: Dict[str, List[str]] = {}
    for (sub, sess) in groups:
        if sub != "" and sess != "":
            sessions_of_submission.setdefault(sub, []).append(sess)
            submissions_of_session.setdefault(sess, []).append(sub)
    # Rows that lack ONE of the two ids but carry the other form their own group, apart from the rows of the same
    # session that carry both. Left alone, the complete rows would be accepted and the incomplete ones reported
    # separately, so one answer would vanish from an accepted session. The rows are counted per token here, and the
    # session that has both ids is the one that is skipped (and reported).
    blank_sub_rows_of_session: Dict[str, int] = {}
    blank_sess_rows_of_submission: Dict[str, int] = {}
    for (sub, sess), idx in groups.items():
        if sub == "" and sess != "":
            blank_sub_rows_of_session[sess] = len(idx)
        elif sess == "" and sub != "":
            blank_sess_rows_of_submission[sub] = len(idx)

    opening_mode = config.get("opening_row", {}).get("mode", "none")
    versions = parse_versions(config)
    expected_session_type = str(config["session_type"]).strip()
    bot_sender_ids = {str(s).strip() for s in config["bot_sender_ids"]}
    completion_map = resolve_completion_map(config)
    folded_completion = _folded_completion_map(completion_map)
    closing_keys = set(config.get("closing_keys", []))
    offset = int(config["sequence_offset"])
    catalog = {q["key"]: q for q in config["questions"]}
    bot_map = config["message_map"]["bot"]
    respondent_map = config["message_map"]["respondent"]

    events: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for (sub, sess), idx in groups.items():
        problems: List[str] = []
        sub_id = sub if sub != "" else None
        sess_id = sess if sess != "" else None

        # ---- identity ----
        # Rows with one blank id whose session has other rows with both ids are reported by that session (below).
        if sub == "" and sess != "" and sess in submissions_of_session:
            continue
        if sess == "" and sub != "" and sub in sessions_of_submission:
            continue
        if sub == "":
            problems.append(f"submission_id is blank in {len(idx)} row(s)")
        if sess == "":
            problems.append(f"session is blank in {len(idx)} row(s)")
        if sub != "" and sess != "" and sess in blank_sub_rows_of_session:
            problems.append(f"submission_id is blank in {blank_sub_rows_of_session[sess]} row(s)")
        if sub != "" and sess != "" and sub in blank_sess_rows_of_submission:
            problems.append(f"session is blank in {blank_sess_rows_of_submission[sub]} row(s)")
        if sub != "" and len(sessions_of_submission.get(sub, [])) > 1:
            problems.append(f"submission_id maps to {len(sessions_of_submission[sub])} different sessions")
        if sess != "" and len(submissions_of_session.get(sess, [])) > 1:
            problems.append(f"session maps to {len(submissions_of_session[sess])} different submission_ids")
        # Ids reach DLQ headers, log lines and cache keys: a value outside [A-Za-z0-9._:-]{1,128} is refused
        # and never echoed (it could be respondent text from a misaligned file, or a forged log line).
        if sub != "" and not is_safe_id(sub):
            problems.append(f"submission_id has unsupported characters or is too long in {len(idx)} row(s)")
            sub_id = None
        if sess != "" and not is_safe_id(sess):
            problems.append(f"session has unsupported characters or is too long in {len(idx)} row(s)")
            sess_id = None
        if sub == "" or sess == "" or sub_id is None or sess_id is None:
            skipped.append({"submissionId": sub_id, "sessionId": sess_id, "problems": problems})
            continue

        # ---- sequence ----
        seq_by_pos: Dict[int, int] = {}
        blank_seq = 0
        bad_seq: List[str] = []
        non_positive: List[int] = []
        too_large: List[int] = []
        for i in idx:
            raw = sequences[i]
            if raw == "":
                blank_seq += 1
            elif _INT_RE.match(raw) is None:
                bad_seq.append(i)
            else:
                # Normalise ONCE: the digits without leading zeros. int() of a string above 4300 digits raises
                # ValueError on Python 3.10.7+ and leading zeros count, so "0" * 5000 + "1" must never reach int(raw);
                # the length is checked on the normalised digits and every conversion uses them.
                digits = raw.lstrip("0")
                if len(digits) > 10:
                    too_large.append(i)  # beyond the 32-bit stored column
                else:
                    value = 0 if digits == "" else int(digits)
                    if value > MAX_SEQUENCE:
                        too_large.append(i)
                    elif value < 1:
                        non_positive.append(i)
                    else:
                        seq_by_pos[i] = value
        if blank_seq > 0:
            problems.append(f"sequence is missing in {blank_seq} row(s)")
        if len(bad_seq) > 0:
            problems.append(f"sequence is not an integer in {len(bad_seq)} row(s) ({_rows(bad_seq)})")
        if len(non_positive) > 0:
            problems.append(f"sequence must be 1 or greater in {len(non_positive)} row(s) ({_rows(non_positive)})")
        if len(too_large) > 0:
            problems.append(
                f"sequence is above {MAX_SEQUENCE} (the largest stored sequence) in {len(too_large)} row(s) "
                f"({_rows(too_large)})"
            )
        if len(seq_by_pos) > 0 and max(seq_by_pos.values()) + max(offset, 0) > MAX_SEQUENCE:
            problems.append(f"sequence plus the configured sequence_offset is above {MAX_SEQUENCE}")
        seen_seq: Dict[int, int] = {}
        for seq in seq_by_pos.values():
            seen_seq[seq] = seen_seq.get(seq, 0) + 1
        duplicates = sorted(s for s, n in seen_seq.items() if n > 1)
        if len(duplicates) > 0:
            problems.append(f"duplicate sequence(s) in the session ({len(duplicates)} sequence number(s) repeated)")

        # The opening row: sequence 1 with blank sender_id, stage and created_at (new-shape export).
        opening_pos: Optional[int] = None
        if opening_mode == "auto":
            at_one = [i for i, q in seq_by_pos.items() if q == 1]
            if len(at_one) == 1 and senders[at_one[0]] == "" and stages[at_one[0]] == "" \
                    and created[at_one[0]].strip() == "":
                opening_pos = at_one[0]

        # ---- text ----
        # A respondent row without any text (a voice note without a transcript) is kept with the placeholder: the
        # answer exists, only its text does not, and dropping the row, or the session with it, would lose the rest.
        # An empty BOT row (or an opening row) is a broken export and stays a session problem.
        empty_rows = [i for i in idx
                      if _is_blank(csv_text["message"][i]) and _is_blank(csv_text["translated_message"][i])]
        placeholder_rows = {i for i in empty_rows if senders[i] != "" and senders[i] not in bot_sender_ids}
        empty_rows = [i for i in empty_rows if i not in placeholder_rows]
        if len(empty_rows) > 0:
            problems.append(
                f"message and translated_message are both empty in {len(empty_rows)} row(s) ({_rows(empty_rows)})"
            )

        # ---- sender ----
        blank_senders = sum(1 for i in idx if senders[i] == "" and i != opening_pos)
        if blank_senders > 0:
            problems.append(f"sender_id is blank in {blank_senders} row(s)")

        # ---- constant-within-session columns ----
        # Matched ignoring case and surrounding spaces ('completed' is COMPLETED); an exact key of the map wins.
        status_values = _distinct([statuses[i].lower() for i in idx])
        if len(status_values) > 1:
            problems.append(f"session_status differs within the session ({len(status_values)} distinct values)")
        invalid_status = [v for v in status_values if v not in folded_completion]
        if len(invalid_status) > 0:
            problems.append(
                f"session_status has {len(invalid_status)} value(s) that are not in the config's completion_map "
                f"(accepted: " + ", ".join(completion_map) + ")"
            )
        language_values = _distinct([languages[i] for i in idx])
        if "" in language_values:
            problems.append(f"language is blank in {sum(1 for i in idx if languages[i] == '')} row(s)")
        non_blank_languages = [v for v in language_values if v != ""]
        if len(non_blank_languages) > 1:
            problems.append(f"language differs within the session ({len(non_blank_languages)} distinct values)")
        # The questionnaire version of a session is the rank of its bot id inside the persona, so a session needs
        # exactly ONE bot id, written as a positive integer in digits. A blank cell on some row is tolerated (an
        # opening row may carry none) as long as the other rows agree; the values are never echoed.
        bot_values = _distinct([bot_ids[i] for i in idx if bot_ids[i] != ""])
        if len(bot_values) == 0:
            problems.append("bot_id is blank in every row (a Mitra session needs its bot id)")
        elif len(bot_values) > 1:
            problems.append(f"bot_id differs within the session ({len(bot_values)} distinct values)")
        elif not is_valid_bot_id(bot_values[0]):
            problems.append(f"bot_id must be {BOT_ID_RULE}")
        type_values = _distinct([session_types[i] for i in idx])
        if "" in type_values:
            problems.append(f"session_type is blank in {sum(1 for i in idx if session_types[i] == '')} row(s)")
        non_blank_types = [v for v in type_values if v != ""]
        if len(non_blank_types) > 1:
            problems.append(f"session_type differs within the session ({len(non_blank_types)} distinct values)")
        elif len(non_blank_types) == 1 and non_blank_types[0] != expected_session_type:
            problems.append(f"session_type does not match the config's {expected_session_type!r}")

        # ---- created_at ----
        parsed_at: Dict[int, datetime] = {}
        bad_dates: List[int] = []
        for i in idx:
            if i == opening_pos:
                continue  # the opening row has no time by design
            dt = _parse_datetime(created[i], fallback_tz, day_first_formats)
            if dt is None:
                bad_dates.append(i)
            else:
                parsed_at[i] = dt
        # One bad time must not cost the session its other answers: such a row gets messageAt null and is counted as
        # a warning. Only a session in which NO row has a time is skipped (submissionDate and the questionnaire
        # version need one), with the problem text this check always had.
        if len(bad_dates) > 0 and len(parsed_at) == 0:
            problems.append(f"created_at is missing or unparsable in {len(bad_dates)} row(s) ({_rows(bad_dates)})")

        if len(problems) > 0:
            skipped.append({"submissionId": sub_id, "sessionId": sess_id, "problems": problems})
            continue

        # ---- ordering, tail-only check, opening row or synthesised first question ----
        ordered = sorted(idx, key=lambda i: seq_by_pos[i])
        first = ordered[0]
        first_seq = seq_by_pos[first]
        if first_seq != 1:
            skipped.append({
                "submissionId": sub_id, "sessionId": sess_id,
                "problems": [f"CSV starts at sequence {first_seq}, not 1: sessions exported from the middle "
                             f"(tail-only) are not supported"],
            })
            continue

        language = languages[first]
        completion_status = folded_completion[statuses[first].lower()]

        def row_texts(i: int, is_bot: bool) -> Tuple[str, Optional[str]]:
            """(text in the conversation language, English text) of a row; bot rows swap the CSV columns."""
            if i in placeholder_rows:
                return placeholder, None
            text_map = bot_map if is_bot else respondent_map
            conversation_text = mapped(text_map["message"])[i]
            english_text = mapped(text_map["translated_message"])[i]
            if _is_blank(conversation_text):
                # only the English text exists: it is the message; the English column is not a second copy of it
                conversation_text = english_text
                english_text = None
            elif _is_blank(english_text):
                english_text = None   # the CSV gave no English text: stored as NULL, never a copy of the message
            return conversation_text, english_text

        messages: List[Dict[str, Any]] = []
        if opening_pos is not None:
            # New-shape export: the opening message is the first bot row as it stands; the first real
            # message starts the session. Sequences are the CSV ones.
            rows = ordered[1:]
            if len(rows) == 0:
                skipped.append({
                    "submissionId": sub_id, "sessionId": sess_id,
                    "problems": ["the session has only the opening row (no real message)"],
                })
                continue
            session_start = parsed_at[next(i for i in rows if i in parsed_at)]
            opening_key = stages[rows[0]] if stages[rows[0]] != "" else None
            opening_text, opening_english = row_texts(opening_pos, True)
            messages.append({
                "sequence": first_seq,
                "questionKey": opening_key,
                "isFollowUp": False,
                "message": opening_text,
                "translatedMessage": opening_english,
                "messageAt": None,
                "parentSequence": None,
            })
            current_parent = first_seq
            # a bot row right after the opening row is not a probe of it
            previous_bot_key: Optional[str] = opening_key if senders[rows[0]] not in bot_sender_ids else None
            row_offset = 0
        else:
            first_key = stages[first]
            question = catalog.get(first_key)
            if question is None:
                skipped.append({
                    "submissionId": sub_id, "sessionId": sess_id,
                    "problems": [f"the first row's stage has no entry in the questions catalog "
                                 f"(expected one of: {', '.join(sorted(catalog))})"],
                })
                continue
            # the first row's own time when it has one, else the earliest-sequence row that has one
            session_start = parsed_at[next(i for i in ordered if i in parsed_at)]
            synth_text = question["text"]
            synth_seq = SYNTHESISED_FIRST_QUESTION_SEQUENCE
            messages.append({
                "sequence": synth_seq,
                "questionKey": first_key,
                "isFollowUp": False,
                "message": synth_text,
                "translatedMessage": None,
                "messageAt": _format_utc(parsed_at[first]) if first in parsed_at else None,
                "parentSequence": None,
            })
            rows = ordered
            current_parent = synth_seq
            previous_bot_key = first_key
            row_offset = offset

        submission_date = _format_utc(session_start)
        for i in rows:
            stored_seq = seq_by_pos[i] + row_offset
            key = stages[i] if stages[i] != "" else None
            is_bot = senders[i] in bot_sender_ids
            conversation_text, english_text = row_texts(i, is_bot)
            if i not in parsed_at:
                counts["rows_without_message_time"] += 1
            if i in placeholder_rows:
                counts["respondent_rows_without_text"] += 1

            if is_bot:
                parent: Optional[int] = None
                # A bot row whose stage equals the previous bot row's stage is a probe / follow-up
                # asked on the same question (checked on the real export: 9,303 of 48,013 bot rows,
                # only 93 of them re-send the same text). Closing rows never count.
                follow_up = key is not None and key == previous_bot_key and key not in closing_keys
                previous_bot_key = key
                current_parent = stored_seq
            else:
                parent = current_parent
                follow_up = False

            messages.append({
                "sequence": stored_seq,
                "questionKey": key,
                "isFollowUp": follow_up,
                "message": conversation_text,
                "translatedMessage": english_text,
                "messageAt": _format_utc(parsed_at[i]) if i in parsed_at else None,
                "parentSequence": parent,
            })

        state, district = _location_tags(messages, config.get("location"))
        events.append({
            "submissionId": sub,
            "submissionType": FEEDBACK_REPORT_TYPE,
            "sessionId": sess,
            "tenantCode": tenant_code,
            "eventType": "create",
            "eventPublishedAt": published,
            "tags": {
                "programId": program_info["id"],
                "programName": program_info["name"],
                "leaderCategoryId": leader_info["id"],
                "leaderCategoryName": leader_info["name"],
                "state": state,
                "district": district,
                "source": source,
            },
            "data": {
                "source": source,
                "adapter": ADAPTER_CONVERSATION_ROWS,
                "sessionType": expected_session_type,
                "questionnaireVersion": stamp_version(versions, session_start),
                "botId": bot_values[0],
                "botName": next((bot_names[i] for i in idx if bot_names[i] != ""), None),
                "language": language,
                "completionStatus": completion_status,
                "submissionDate": submission_date,
                "messages": messages,
            },
        })

    # Bot id ascending (numeric; a valid bot id has no leading zero, so length then text is the numeric order), first
    # appearance first inside one bot id (sort is stable). See the docstring for why.
    events.sort(key=lambda event: (len(event["data"]["botId"]), event["data"]["botId"]))
    counts["cells_with_control_characters"] = cells_cleaned
    if report is not None:
        report.update(counts)
    return events, skipped


def _location_tags(messages: List[Dict[str, Any]], location: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    state / district from the respondent's answer to the configured stage, via config aliases.

    config["location"] = {"state": {"stage": "STATE_IDENTIFICATION", "aliases": {"<normalised>": "<canonical>"}},
                          "district": {...}}
    The answer's English text (translatedMessage, else message) is normalised (strip, lowercase,
    collapse whitespace) and looked up in the aliases; None when the block or the answer is absent or
    the answer is not recognised.
    """
    if location is None:
        return None, None
    return _location_value(messages, location.get("state")), _location_value(messages, location.get("district"))


def _location_value(messages: List[Dict[str, Any]], rule: Optional[Dict[str, Any]]) -> Optional[str]:
    if rule is None:
        return None
    stage = rule.get("stage")
    aliases = rule.get("aliases", {})
    for msg in messages:
        if msg["parentSequence"] is None or msg["questionKey"] != stage:
            continue
        text = msg["translatedMessage"] if msg["translatedMessage"] is not None else msg["message"]
        return aliases.get(_normalise_answer(text))
    return None
