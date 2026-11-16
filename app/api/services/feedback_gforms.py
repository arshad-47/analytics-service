"""
Pure parsing, form matching and event building for the `wide_form` adapter of `feedback` uploads (a form export:
one row per respondent, one column per question; Google Forms is the first). The config's `adapter` selects this
module, never the source name: any source whose config says `wide_form` is read this way.

The ORIGINAL wide Google Forms CSV (one row per respondent, one column per question) is uploaded as is.
This module reshapes each response into question/answer rows, gives it a natural key (`source_ref`),
and builds the same Kafka event shape as the Mitra path (app/services/feedback_constants.py). Only
responses whose `source_ref` is not stored yet become events (delta upload).

No DB, Kafka, network or settings access here: everything the functions need is passed in. The rules
are in docs/feedback-upload-plan.md (Identifiers, Incremental uploads, section 4).

Flow used by the upload service:
    df = parse_gforms_csv(file_bytes)                        # ValueError -> reject the upload
    errors = check_gforms_file(df, config)                   # file-level; non-empty -> reject whole file
    responses, skipped, stats = prepare_gforms_responses(df, config)   # skipped = bad responses
    # look up source_ref of `responses` in feedback_submissions -> known_refs
    events, delta = build_gforms_events(responses, known_refs=known_refs, ..., config=config)

Config shape (`config_version.config` of a wide_form config; the `feedback_upload google_forms` blocks of seed_config.sql):
    {"adapter": "wide_form", "timestamp_column": "Timestamp",
     "datetime": {"formats": ["%m/%d/%Y %H:%M:%S"], "fallback_timezone": "Asia/Kolkata"},
     "default_completion": "COMPLETED",
     "ignored_columns": ["Email Address"], "ignored_column_patterns": ["^Column \\d+$"],
     "language": {"default": "en", "script_detect": {"kn": "[\\u0C80-\\u0CFF]"}, "detect_kinds": ["open"]},   # detect_kinds optional
     "forms": {"<form_key>": {"name": "...", "questions": [
         {"headers": ["<normalised English header>", ...], "question_key": "...", "text": "...", "kind": "metadata|open"}]}}}
"""
import csv
import hashlib
import io
import json
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from app.api.services.feedback_uploads import _location_tags
from app.services.feedback_adapter_config import (
    config_adapter,
    validate_adapter_config,
    validate_default_completion,
    wide_form_identity_problem,
)
from app.services.feedback_constants import (
    ADAPTER_WIDE_FORM,
    COMPLETION_COMPLETED,
    FEEDBACK_REPORT_TYPE,
    GFORMS_SEQUENCE_OFFSET,
)

_WHITESPACE_RE = re.compile(r"\s+")
_TRAILING_JUNK = ' /"”“'
_MAX_LISTED = 20
_DEFAULT_LANGUAGE = "en"


# ---------------------------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------------------------

def parse_gforms_csv(file_bytes: bytes) -> pd.DataFrame:
    """
    Parse an uploaded Google Forms CSV with every cell as a string.

    `dtype=str, keep_default_na=False` keeps answers such as "NA" / "None" (pandas would turn them
    into NaN) and leaves empty cells as "". A UTF-8 BOM is dropped. Column names are kept exactly as
    exported (trailing spaces, newlines and Kannada text included; see normalise_header). Raises
    ValueError when the file is empty, has a header but no data rows, or cannot be parsed.
    """
    if file_bytes is None or len(file_bytes) == 0:
        raise ValueError("The uploaded CSV file is empty.")
    try:
        df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        raise ValueError("The uploaded CSV file is empty.")
    except UnicodeDecodeError as exc:
        raise ValueError(f"The uploaded CSV file is not valid UTF-8 text: {exc}")
    except (pd.errors.ParserError, csv.Error) as exc:
        raise ValueError(f"The uploaded CSV file could not be parsed: {exc}")
    df.columns = [str(c) for c in df.columns]
    if len(df) == 0:
        raise ValueError("The uploaded CSV file has a header but no data rows.")
    return df


def _is_foreign_script_letter(ch: str) -> bool:
    """A letter or combining mark of a script other than Latin (Kannada, Devanagari, ...). Latin letters with
    diacritics (é), punctuation (the typographic apostrophe, an en dash) and digits are not."""
    if unicodedata.category(ch)[0] not in ("L", "M"):
        return False
    if ord(ch) < 128:
        return False
    if unicodedata.category(ch)[0] == "M":
        return True  # a combining mark (also the vowel signs of Indic scripts); a mark on a Latin letter is rare in a header
    return not unicodedata.name(ch, "").startswith("LATIN")


def normalise_header(header: str) -> str:
    """
    Reduce an exported column header to its English (Latin-script) text.

    Collapse all whitespace (newlines, tabs, NBSP) to single spaces and strip; cut at the first letter of another script
    (the Kannada part of a bilingual header). Punctuation and Latin letters outside ASCII are kept, so
    "Child\u2019s name" and "Child\u2019s class" stay different questions. A header that STARTS with another script
    ("<Hindi> / Child class") keeps the Latin text that follows the last foreign letter. Then strip trailing spaces
    and any trailing '/', '"' and the typographic quotes, and collapse whitespace again.
    """
    text = _WHITESPACE_RE.sub(" ", header).strip()
    cut = next((pos for pos, ch in enumerate(text) if _is_foreign_script_letter(ch)), None)
    if cut is not None:
        if len(text[:cut].strip(_TRAILING_JUNK)) > 0:
            text = text[:cut]
        else:
            last = max(pos for pos, ch in enumerate(text) if _is_foreign_script_letter(ch))
            text = text[last + 1:].lstrip(_TRAILING_JUNK)
    text = text.rstrip(_TRAILING_JUNK)
    return _WHITESPACE_RE.sub(" ", text).strip()


def _normalise_answer(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


# ---------------------------------------------------------------------------------------------
# Config access
# ---------------------------------------------------------------------------------------------

def _check_config(config: Dict[str, Any]) -> None:
    if config_adapter(config) != ADAPTER_WIDE_FORM:
        raise ValueError(
            f"Feedback config adapter must be {ADAPTER_WIDE_FORM!r} for this builder, got {config['adapter']!r}"
        )
    validate_adapter_config(config)
    for key in ("timestamp_column", "datetime", "forms"):
        if key not in config:
            raise ValueError(f"Google Forms config is missing '{key}'")
    # ingestion stores a response only with a sessionType and a questionnaireVersion: refuse the config now
    # (at upload) instead of accepting the file and failing every response later
    identity_problem = wide_form_identity_problem(config)
    if identity_problem is not None:
        raise ValueError(identity_problem)
    formats = config["datetime"].get("formats")
    if not isinstance(formats, list) or len(formats) == 0:
        raise ValueError("Google Forms config datetime.formats must be a non-empty list")
    if not isinstance(config["forms"], dict):
        raise ValueError("Google Forms config 'forms' must be an object keyed by form")
    for form_key, form in config["forms"].items():
        questions = form.get("questions")
        if not isinstance(questions, list) or len(questions) == 0:
            raise ValueError(f"Google Forms config form {form_key!r} has no questions")
        for q in questions:
            for field in ("headers", "question_key", "text"):
                if field not in q:
                    raise ValueError(f"Google Forms config form {form_key!r} has a question without '{field}'")


def _resolve_timezone(name: str):
    try:
        return ZoneInfo(name)
    except Exception:
        raise ValueError(f"config.datetime.fallback_timezone {name!r} is not a known timezone")


def _default_language(config: Dict[str, Any]) -> str:
    return config.get("language", {}).get("default", _DEFAULT_LANGUAGE)


def _script_detectors(config: Dict[str, Any]) -> List[Tuple[str, "re.Pattern[str]"]]:
    detect = config.get("language", {}).get("script_detect", {})
    try:
        return [(lang, re.compile(pattern)) for lang, pattern in detect.items()]
    except re.error as exc:
        raise ValueError(f"config.language.script_detect has an invalid pattern: {exc}")


def _compiled_patterns(config: Dict[str, Any]) -> List["re.Pattern[str]"]:
    try:
        return [re.compile(p) for p in config.get("ignored_column_patterns", [])]
    except re.error as exc:
        raise ValueError(f"config.ignored_column_patterns has an invalid pattern: {exc}")


def _header_map(form: Dict[str, Any]) -> Dict[str, int]:
    """normalised header -> index of the question in the form (first question wins on a clash)."""
    mapping: Dict[str, int] = {}
    for idx, question in enumerate(form["questions"]):
        for header in question["headers"]:
            mapping.setdefault(normalise_header(header), idx)
    return mapping


# ---------------------------------------------------------------------------------------------
# Column classification and form matching
# ---------------------------------------------------------------------------------------------

def _classify_columns(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[Optional[str], List[Tuple[str, str]]]:
    """
    (timestamp column or None, [(raw header, normalised header)] of the question columns).
    Ignored columns (config ignored_columns / ignored_column_patterns) are not returned.
    """
    timestamp_norm = normalise_header(config["timestamp_column"])
    ignored = {normalise_header(c) for c in config.get("ignored_columns", [])}
    patterns = _compiled_patterns(config)

    timestamp_col: Optional[str] = None
    data_cols: List[Tuple[str, str]] = []
    for col in df.columns:
        norm = normalise_header(col)
        if norm == timestamp_norm and timestamp_col is None:
            timestamp_col = col
        elif norm in ignored or any(p.search(norm) is not None for p in patterns):
            continue
        else:
            data_cols.append((col, norm))
    return timestamp_col, data_cols


def _shown(values: List[str]) -> str:
    shown = ", ".join(repr(v) for v in values[:_MAX_LISTED])
    return shown + (" ..." if len(values) > _MAX_LISTED else "")


def match_gforms_form(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[Optional[str], List[str]]:
    """
    Pick the form definition that explains the file's columns.

    Every non-ignored column's normalised header must appear in some question's headers of the form,
    and the Timestamp column must be present. Returns (form_key, []) on a match, else (None, errors).
    errors list the unmapped headers (as exported), a missing Timestamp column, two columns that map
    to the same question, or an ambiguity. When several forms fit, the one with the most questions
    matched wins, then the one with the fewest questions the file lacks (a tighter fit); an exact tie
    on both is reported as ambiguous.
    """
    _check_config(config)
    timestamp_col, data_cols = _classify_columns(df, config)

    errors: List[str] = []
    if timestamp_col is None:
        errors.append(f"Missing required column: {config['timestamp_column']!r}")
    if len(data_cols) == 0:
        errors.append("The file has no question columns")

    # per form: (form_key, matched, unmatched questions, problems)
    fits: List[Tuple[str, int, int, List[str]]] = []
    for form_key, form in config["forms"].items():
        mapping = _header_map(form)
        unmapped = [raw for raw, norm in data_cols if norm not in mapping]
        by_question: Dict[int, List[str]] = {}
        for raw, norm in data_cols:
            if norm in mapping:
                by_question.setdefault(mapping[norm], []).append(raw)
        problems: List[str] = []
        if len(unmapped) > 0:
            problems.append(f"Unmapped column(s) for form {form_key!r}: {_shown(unmapped)}")
        for q_idx, cols in sorted(by_question.items()):
            if len(cols) > 1:
                problems.append(
                    f"Columns {_shown(cols)} all map to question {form['questions'][q_idx]['question_key']!r} "
                    f"of form {form_key!r}"
                )
        matched = len(by_question)
        fits.append((form_key, matched, len(form["questions"]) - matched, problems))

    if len(fits) == 0:
        errors.append("The Google Forms config defines no forms")
        return None, errors

    candidates = [f for f in fits if len(f[3]) == 0 and f[1] > 0]
    if len(errors) == 0 and len(candidates) > 0:
        best_score = max((f[1], -f[2]) for f in candidates)
        best = [f for f in candidates if (f[1], -f[2]) == best_score]
        if len(best) == 1:
            return best[0][0], []
        names = ", ".join(repr(f[0]) for f in best)
        return None, [f"The file matches several forms equally well ({names}); cannot choose one"]

    if len(candidates) == 0:
        # report the closest form's problems: fewest problems, then most questions matched
        closest = sorted(fits, key=lambda f: (len(f[3]), -f[1]))[0]
        errors.extend(closest[3])
        if len(fits) > 1 and len(closest[3]) > 0:
            errors.append(f"The file matches none of the {len(fits)} configured forms (closest: {closest[0]!r})")
    return None, errors


def check_gforms_file(df: pd.DataFrame, config: Dict[str, Any]) -> List[str]:
    """File-level errors ([] = the file may be processed). Wraps match_gforms_form."""
    _, errors = match_gforms_form(df, config)
    return errors


# ---------------------------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------------------------

def _parse_timestamp(value: str, formats: List[str], fallback_tz) -> Optional[datetime]:
    """Aware datetime (naive values are read in `fallback_tz`), or None when no format matches."""
    text = _normalise_answer(value)
    if text == "":
        return None
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=fallback_tz)
        try:
            parsed.astimezone(timezone.utc)
        except (OverflowError, ValueError):
            return None  # year 0001 / 9999 at an offset: not representable in UTC
        return parsed
    return None


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _answer_hash(answers: List[Dict[str, str]]) -> str:
    """SHA-256 over the sorted non-blank (question_key, normalised answer) pairs."""
    pairs = sorted((a["question_key"], _normalise_answer(a["answer"])) for a in answers)
    payload = json.dumps(pairs, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def prepare_gforms_responses(
    df: pd.DataFrame, config: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """
    Reshape the wide frame into one response per row.

    Returns (responses, skipped, stats):
    - response = {"form_key", "source_ref", "timestamp_iso", "submission_date", "language", "answers": [{"question_key",
      "text", "answer"}]}; answers follow the form's question order and blank cells are omitted;
    - skipped = [{"submissionId": None, "sessionId": None, "problems": [...], "row": <1-based data row>}]
      for a response with a blank or unparsable Timestamp or with no non-blank answer at all;
    - stats = {"rows_read", "collapsed_duplicates"}: rows with the same source_ref inside the file
      (same second and same answers, i.e. a double submit) collapse into the first one.
    Raises ValueError when the file does not match a form (run check_gforms_file first) or the config
    is invalid.
    """
    form_key, errors = match_gforms_form(df, config)
    if form_key is None:
        raise ValueError("The file does not match a configured Google Forms form: " + "; ".join(errors))
    form = config["forms"][form_key]
    questions = form["questions"]

    timestamp_col, data_cols = _classify_columns(df, config)
    mapping = _header_map(form)
    column_of_question: Dict[int, str] = {mapping[norm]: raw for raw, norm in data_cols}
    # question order of the form, only for questions that have a column in this file
    ordered = [(idx, questions[idx], column_of_question[idx]) for idx in sorted(column_of_question)]

    formats = list(config["datetime"]["formats"])
    fallback_tz = _resolve_timezone(config["datetime"].get("fallback_timezone", "UTC"))
    default_language = _default_language(config)
    detectors = _script_detectors(config)
    detect_kinds = config.get("language", {}).get("detect_kinds")

    timestamps = df[timestamp_col].astype(str).tolist()
    cells = {col: df[col].astype(str).tolist() for _, _, col in ordered}

    responses: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    collapsed = 0

    for i in range(len(df)):
        row_number = i + 1
        problems: List[str] = []

        raw_ts = timestamps[i]
        parsed = _parse_timestamp(raw_ts, formats, fallback_tz)
        if _normalise_answer(raw_ts) == "":
            problems.append(f"{config['timestamp_column']} is blank")
        elif parsed is None:
            problems.append(
                f"{config['timestamp_column']} does not match any of the accepted formats: " + ", ".join(formats)
            )

        answers: List[Dict[str, str]] = []
        kinds: List[Optional[str]] = []
        for _, question, col in ordered:
            value = cells[col][i]
            if _normalise_answer(value) == "":
                continue
            kinds.append(question.get("kind"))
            answers.append({"question_key": question["question_key"], "text": question["text"],
                            "answer": value.strip()})
        if len(answers) == 0:
            problems.append("The response has no non-blank answer")

        if len(problems) > 0:
            skipped.append({"submissionId": None, "sessionId": None, "problems": problems, "row": row_number})
            continue

        timestamp_iso = parsed.strftime("%Y-%m-%dT%H:%M:%S")
        source_ref = f"{timestamp_iso}:{_answer_hash(answers)}"
        if source_ref in seen:
            collapsed += 1
            continue
        seen.add(source_ref)

        language = default_language
        probe = [a["answer"] for a, kind in zip(answers, kinds) if detect_kinds is None or kind in detect_kinds]
        for lang, pattern in detectors:
            if any(pattern.search(text) is not None for text in probe):
                language = lang
                break

        responses.append({
            "form_key": form_key,
            "source_ref": source_ref,
            "timestamp_iso": timestamp_iso,
            "submission_date": _format_utc(parsed),
            "language": language,
            "answers": answers,
        })

    return responses, skipped, {"rows_read": len(df), "collapsed_duplicates": collapsed}


# ---------------------------------------------------------------------------------------------
# Event building
# ---------------------------------------------------------------------------------------------

def build_gforms_events(
    responses: List[Dict[str, Any]],
    *,
    known_refs: Set[str],
    program_info: Dict[str, Any],
    leader_info: Dict[str, Any],
    tenant_code: str,
    source: str,
    published_at: Optional[str] = None,
    id_factory: Optional[Callable[[], str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    One Kafka event per response whose source_ref is NOT in `known_refs` (the delta).

    program_info / leader_info are {"id", "name"}; `source` is the upload's source label (the events carry it and
    data.adapter = wide_form). Returns (events, {"new", "already_stored"}).
    submissionId = sessionId = id_factory() (a uuid4 string by default), drawn only for new responses.
    Messages are numbered 1..n: per answer a question row (odd; parentSequence null) then its answer
    row (even; parentSequence = the question's sequence); messageAt = the response's submission_date.
    translatedMessage of an answer is the answer itself when the response language equals the config's
    default language, else None (translation comes later). The question row carries the English text
    in both message and translatedMessage.

    `config` (optional, keyword) supplies session_type, the forms' questionnaire_version, default_completion,
    language.default and the optional `location` block (state / district via aliases, same semantics as the Mitra builder). Without it
    the completion is COMPLETED, the default language 'en' and state / district None.
    """
    cfg: Dict[str, Any] = config if config is not None else {}
    factory = id_factory if id_factory is not None else (lambda: str(uuid.uuid4()))
    published = published_at if published_at is not None else _format_utc(datetime.now(timezone.utc))
    # a form response is final: only COMPLETED is valid (validated; an IN_PROGRESS config fails loudly here)
    completion = (validate_default_completion(cfg["default_completion"]) if "default_completion" in cfg
                  else COMPLETION_COMPLETED)
    default_language = _default_language(cfg)
    session_type = cfg.get("session_type")
    forms = cfg.get("forms", {})

    events: List[Dict[str, Any]] = []
    already_stored = 0

    for response in responses:
        if response["source_ref"] in known_refs:
            already_stored += 1
            continue

        language = response["language"]
        submitted = response["submission_date"]
        is_default_language = language == default_language
        # the questionnaire version is a property of the form the response came from
        questionnaire_version = forms.get(response.get("form_key"), {}).get("questionnaire_version")

        messages: List[Dict[str, Any]] = []
        for n, answer in enumerate(response["answers"]):
            question_seq = 2 * n + 1 + GFORMS_SEQUENCE_OFFSET
            messages.append({
                "sequence": question_seq,
                "questionKey": answer["question_key"],
                "isFollowUp": False,
                "message": answer["text"],
                "translatedMessage": answer["text"],
                "messageAt": submitted,
                "parentSequence": None,
            })
            messages.append({
                "sequence": question_seq + 1,
                "questionKey": answer["question_key"],
                "isFollowUp": False,
                "message": answer["answer"],
                "translatedMessage": answer["answer"] if is_default_language else None,
                "messageAt": submitted,
                "parentSequence": question_seq,
            })

        state, district = _location_tags(messages, cfg.get("location"))
        new_id = factory()
        events.append({
            "submissionId": new_id,
            "submissionType": FEEDBACK_REPORT_TYPE,
            "sessionId": new_id,
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
                "adapter": ADAPTER_WIDE_FORM,
                "sessionType": session_type,
                "questionnaireVersion": questionnaire_version,
                "language": language,
                "completionStatus": completion,
                "submissionDate": submitted,
                "sourceRef": response["source_ref"],
                "messages": messages,
            },
        })

    return events, {"new": len(events), "already_stored": already_stored}
