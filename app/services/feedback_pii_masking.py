"""
Model-based PII and abusive-language masking of a feedback session (optional step of the metadata extraction).

It reuses the contract of the story / discussion step `pii_and_abusive_language_detection` (a prompt in the
`prompts` table, one JSON object in, one JSON object out, `masked_text` / `pii_found` / `abusive_language` per
entry, strict coverage: a missing entry fails the step instead of reporting success), with a prompt of its own for
respondents of the feedback programs (`PII_MASKING_ANALYSIS_TYPE`, chosen by `pii.masking_prompt_name` of the
program's extraction config). Which program uses it, and which prompt, is config data.

Input to the model: ONE object keyed by the SEQUENCE number of each respondent answer (unique even when the bot
repeats a question), each value the question the answer replies to and the answer:

    {"3": {"question": "What is your name?", "answer": "Asha Verma"}, ...}

Output expected: the same keys, each {"masked_text": "...", "pii_found": [{"type": ...}], "abusive_language": bool,
"abusive_spans": [{"severity": "mild|moderate|severe"}]}. The masked answers replace the respondent turns of the
conversation that the extraction prompt sees, and are kept (masked) in feedback_submissions.meta_data.extraction.masking.
The deterministic local check (`mask_respondent_pii`) runs BEFORE this call, so numbers, e-mail addresses, handles and
links never reach the model even here. Nothing in this module logs text.
"""
import json
import re
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from app.services.feedback_extraction import (
    Conversation,
    ExtractionConfig,
    ExtractionOutputError,
    ROLE_BOT,
    ROLE_RESPONDENT,
    Turn,
    _clean,
    _justification,
    _no_duplicates,
    _reject_constant,
    _strip_fences,
)

import hashlib

# The model's own PII tag words (the prompt replaces text by <PERSON>, <LOCATION>, <ID>, <PHONE>, <EMAIL>) mapped to
# the pii type names of the extraction config; a name the config does not list becomes 'other'.
MASKING_TYPE_TO_PII_TYPE: Dict[str, str] = {
    "person": "name", "name": "name", "location": "address", "address": "address",
    "phone": "phone", "email": "email", "id": "other",
}
ABUSE_SEVERITIES: Tuple[str, ...] = ("mild", "moderate", "severe")
_PII_TAG_RE = re.compile(r"<(PERSON|LOCATION|ID|PHONE|EMAIL)>")
_TAG_TO_TYPE = {"PERSON": "person", "LOCATION": "location", "ID": "id", "PHONE": "phone", "EMAIL": "email"}


@dataclass(frozen=True)
class MaskingResult:
    """What the masking call found. `conversation` holds the MASKED answers (never the original text)."""
    conversation: Dict[str, Dict[str, str]]       # sequence -> {"question": ..., "answer": <masked>}
    pii_types: Tuple[str, ...]                     # config pii type names (in order of first appearance)
    abusive_language: bool
    abusive_severities: Tuple[str, ...]
    usage: Dict[str, Any] = field(default_factory=dict)
    findings: Tuple[Dict[str, Any], ...] = ()      # what was found, per answer, with the model's justification (no text)
    response: Optional[str] = None                 # the answer of the call as the model returned it


@dataclass(frozen=True)
class MaskingStep:
    """The prompt of the masking call and the function that makes it (same signature as the extraction call)."""
    system_template: str
    user_template: str
    call_llm: Callable[[str], Tuple[str, Dict[str, Any]]]


def build_masking_input(conversation: Conversation) -> Dict[str, Dict[str, str]]:
    """
    {sequence: {"question", "answer"}} for every respondent turn, in order. The question is the nearest bot turn
    before the answer (an answer's parent is the nearest preceding question, so a multi-message answer repeats the
    same question); a respondent turn with no bot turn before it has an empty question.
    """
    pairs: Dict[str, Dict[str, str]] = {}
    question = ""
    for turn in conversation.turns:
        if turn.role == ROLE_BOT:
            question = turn.text
        elif turn.role == ROLE_RESPONDENT:
            pairs[str(turn.sequence)] = {"question": question, "answer": turn.text}
    return pairs


def render_masking_prompt(step: MaskingStep, pairs: Mapping[str, Mapping[str, str]]) -> str:
    """
    The story / discussion placeholders: `{columns}` (system prompt) is the list of keys the model must answer
    for, `{{text}}` (user prompt) the input object. Each is substituted in its own template, once, so respondent
    text can never inject into the other placeholder.
    """
    system = step.system_template.replace("{columns}", json.dumps(list(pairs), ensure_ascii=False))
    user = step.user_template.replace("{{text}}", json.dumps(pairs, ensure_ascii=False))
    return f"{system}\n\n{user}"


def _entry(raw: Any) -> Optional[Mapping[str, Any]]:
    """The model sometimes wraps one entry in a one-element list; accept that and nothing else."""
    if isinstance(raw, list) and len(raw) == 1:
        raw = raw[0]
    return raw if isinstance(raw, dict) else None


def _confidence(value: Any) -> Optional[float]:
    """A finding's confidence: a number from 0 to 1, anything else is ignored (informational, like the field's)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if 0 <= value <= 1 else None


def _finding(answer: str, kind: str, label_key: str, label: Any, item: Any) -> Dict[str, Any]:
    """
    One entry of meta_data.extraction.masking.findings: which answer, what kind of finding, its type or severity and,
    when the model gave them, its confidence and justification. NEVER the matched text (the prompt forbids it and
    nothing here reads it). The justification is the model's `justification` key; `reason` (the key of the older prompt
    text and of the story prompt) is read when `justification` is absent.
    """
    entry: Dict[str, Any] = {"answer": answer, "kind": kind, label_key: label.strip().lower() if isinstance(label, str) else None}
    if isinstance(item, dict):
        confidence = _confidence(item.get("confidence"))
        if confidence is not None:
            entry["confidence"] = confidence
        text = _justification(item["justification"] if "justification" in item else item.get("reason"))
        if text is not None:
            entry["justification"] = text
    return entry


def parse_masking_response(text: Any, expected: Sequence[str], pii_types: Sequence[str]
                           ) -> Tuple[Dict[str, str], Tuple[str, ...], bool, Tuple[str, ...]]:
    """(masked text per key, pii type names, abusive flag, abuse severities): `parse_masking_details` without the findings."""
    return parse_masking_details(text, expected, pii_types)[:4]


def parse_masking_details(text: Any, expected: Sequence[str], pii_types: Sequence[str]
                          ) -> Tuple[Dict[str, str], Tuple[str, ...], bool, Tuple[str, ...], Tuple[Dict[str, Any], ...]]:
    """
    (masked text per key, pii type names, abusive flag, abuse severities, findings). STRICT: the answer must be one JSON object
    with an entry for EVERY expected key and a string `masked_text` in each; anything else raises
    ExtractionOutputError (the message names the key, never any text) and the original text is never used as a
    fallback, so a session is never reported masked when the model did not return its masked text. Extra keys are
    ignored. A PII tag word in a masked text counts as PII found even when `pii_found` says nothing. `findings` lists
    every item of `pii_found` / `abusive_spans` per answer with its justification and confidence (see `_finding`).
    """
    if not isinstance(text, str):
        raise ExtractionOutputError("the masking answer is not text")
    try:
        parsed = json.loads(_strip_fences(text), object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        raise ExtractionOutputError("the masking answer is not valid JSON (or has a duplicate key)") from None
    if not isinstance(parsed, dict):
        raise ExtractionOutputError("the masking answer must be one JSON object")
    masked: Dict[str, str] = {}
    types: List[str] = []
    abusive = False
    severities: List[str] = []
    findings: List[Dict[str, Any]] = []

    def add_type(word: Any) -> None:
        key = word.strip().lower() if isinstance(word, str) else "other"
        name = MASKING_TYPE_TO_PII_TYPE.get(key, "other")
        name = name if name in pii_types else "other"
        if name not in types:
            types.append(name)

    for key in expected:
        entry = _entry(parsed.get(key))
        if entry is None:
            raise ExtractionOutputError(f"the masking answer has no entry for answer {key}")
        masked_text = entry.get("masked_text")
        if not isinstance(masked_text, str):
            raise ExtractionOutputError(f"the masking entry for answer {key} has no masked_text string")
        masked[key] = _clean(masked_text)
        found = entry.get("pii_found")
        if isinstance(found, list):
            for item in found:
                kind = item.get("type") if isinstance(item, dict) else None
                add_type(kind)
                findings.append(_finding(key, "pii", "type", kind, item))
        elif found is True:
            add_type(None)
        for tag in _PII_TAG_RE.findall(masked_text):
            add_type(_TAG_TO_TYPE[tag])
        if entry.get("abusive_language") is True:
            abusive = True
        spans = entry.get("abusive_spans")
        if isinstance(spans, list):
            for span in spans:
                severity = span.get("severity") if isinstance(span, dict) else None
                if isinstance(severity, str) and severity.strip().lower() in ABUSE_SEVERITIES:
                    abusive = True
                    findings.append(_finding(key, "abusive_language", "severity", severity, span))
                    if severity.strip().lower() not in severities:
                        severities.append(severity.strip().lower())
    return (masked, tuple(types), abusive, tuple(sorted(severities, key=ABUSE_SEVERITIES.index)), tuple(findings))


_SPACE_SPLIT_RE = re.compile(r"\s+")
_EDGE_PUNCTUATION = ",.;:!?()[]{}<>\"'/\\|*~`"
MIN_SCRUB_WORD_CHARS = 3


def _words(text: str) -> List[str]:
    """Words split at white space with the punctuation at their edges removed. NOT a word-character match: in Indic scripts
    the vowel signs are not word characters and would cut a word into pieces too short to compare."""
    return [w for w in (part.strip(_EDGE_PUNCTUATION).casefold() for part in _SPACE_SPLIT_RE.split(text)) if len(w) > 0]


def _removed_words(original: str, masked: str) -> FrozenSet[str]:
    """The words of the answer that are NOT in its masked text: what the masking took out (names, places, ids)."""
    kept = set(_words(masked))
    return frozenset(w for w in _words(original) if len(w) >= MIN_SCRUB_WORD_CHARS and w not in kept)


def scrub_findings(findings: Sequence[Mapping[str, Any]], pairs: Mapping[str, Mapping[str, str]],
                   masked: Mapping[str, str]) -> Tuple[Dict[str, Any], ...]:
    """
    The findings with every justification that repeats a word the masking took out of its answer DROPPED (the finding
    itself stays): the masked copy is the part that may be shared, and the prompt asks the model to describe a finding
    ('a person name'), never to quote it; a model that quotes anyway must not put the matched text back into meta_data.
    """
    removed: Dict[str, FrozenSet[str]] = {}
    scrubbed: List[Dict[str, Any]] = []
    for finding in findings:
        entry = dict(finding)
        justification = entry.get("justification")
        key = entry.get("answer")
        if isinstance(justification, str) and key in pairs and key in masked:
            if key not in removed:
                removed[key] = _removed_words(pairs[key]["answer"], masked[key])
            if len(removed[key] & set(_words(justification))) > 0:
                del entry["justification"]
        scrubbed.append(entry)
    return tuple(scrubbed)


def apply_masked_answers(conversation: Conversation, masked: Mapping[str, str]) -> Conversation:
    """The same conversation with the text of every respondent turn replaced by its masked text (bot turns unchanged)."""
    turns = tuple(
        replace(t, text=masked[str(t.sequence)]) if t.role == ROLE_RESPONDENT and str(t.sequence) in masked else t
        for t in conversation.turns)
    body = "\n".join(f"[{t.sequence}] {t.role}: {t.text}" for t in turns)
    return replace(conversation, turns=turns, text=body,
                   conversation_id=hashlib.sha256(body.encode("utf-8")).hexdigest()[:16])


def masking_meta(result: MaskingResult, prompt_version_id: Any) -> Dict[str, Any]:
    """
    The object stored under feedback_submissions.meta_data.extraction.masking: masked text, and what was found with the
    model's justification per finding (never the matched text). The raw answer of the call is stored separately, under
    meta_data.extraction.responses, and only for a session that is not flagged.
    """
    return {
        "prompt_version_id": str(prompt_version_id),
        "pii_types": list(result.pii_types),
        "abusive_language": result.abusive_language,
        "abusive_severities": list(result.abusive_severities),
        "findings": [dict(f) for f in result.findings],
        "conversation": result.conversation,
    }
