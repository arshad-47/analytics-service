"""
Detect a new questionnaire version of a feedback bot from the questions the stored sessions were asked
(pure functions, no I/O; app/database/feedback_operations.py feeds them).

The bot's scripted question of each step (not the LLM-written probes) is compared with what each version is known
to ask. This only PROPOSES a version and the instant it started: a person records it with
tools/questionnaire_versions.py set. The recorded windows stay the single source of truth for a session's label,
so a label never changes by itself.

How a version's known questions are found: the main questions asked by at least two of the first
`baseline_sessions` sessions stamped with that version (one session suffices when there are fewer than 10).
Limit: a version whose window holds fewer sessions than `baseline_sessions` before the next change learns that
change as its own (lower --baseline-sessions for a small deployment), and a window recorded far too early
pollutes its own reference, so detect finds new question sets but does not audit recorded dates.
When every new question is a close wording variant of a known one (similarity of at least WORDING_SIMILARITY, for
example "you are feeling" against "you're feeling"), nothing is proposed, because a rephrased line is usually not a new
questionnaire; the variants are still listed ("wording_variants") so a real small change, such as a unit changed
inside one question, is visible. `include_wording=True` proposes anyway.
A question text that no version knows, is asked in at least `min_sessions` sessions and in at least
`min_coverage` of the sessions since it first appeared is NEW. The proposed start is the best single step in
time: the instant that leaves the fewest new-question sessions before it and old-question sessions after it.
Only the LATEST recorded window can get a proposal: a new version is appended after the last one (tools/
questionnaire_versions.py set refuses windows that do not increase), so a start inside an older window could not
be recorded. The latest window is the sessions stamped with the last recorded key; a new question that was asked
only in sessions before it is a historical change: reported ("historical_changes"), never proposed. Wording
variants are diagnostics, not evidence of where a version starts, unless `include_wording=True`.
"""
import re
from collections import Counter
from difflib import SequenceMatcher
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

_WORD = re.compile(r"\w+")
_VERSION_KEY = re.compile(r"^V(\d+)$")

MIN_PURITY = 0.9          # share of the new-question sessions that must fall after the proposed start
MIN_SHARE_AFTER = 0.7     # share of the sessions after the proposed start that must carry a new question
_NEED_TWO_SESSIONS_FROM = 10
_LIST_LIMIT = 40
WORDING_SIMILARITY = 0.9  # a new text this close to a known one is a wording variant of it


@dataclass(frozen=True)
class SessionQuestions:
    session_id: str
    start: datetime        # first real message (offset-aware)
    stamped: str           # the version label stored on the session
    texts: Tuple[str, ...]  # its main (not probe, not closing) question texts, in the order asked
    bot_id: Optional[str] = None  # recorded for evidence only; it never decides a label


def normalise(text: str) -> str:
    """Lower-case words joined by single spaces: wording changes count, spacing and punctuation do not."""
    return " ".join(_WORD.findall(text.lower()))


def next_key(keys: Sequence[str]) -> str:
    """V2 after V1; after a key that is not V<n>, the count of keys plus one."""
    match = _VERSION_KEY.match(keys[-1])
    return f"V{int(match.group(1)) + 1}" if match else f"V{len(keys) + 1}"


def detect(
    sessions: Sequence[SessionQuestions],
    version_keys: Sequence[str],
    *,
    min_sessions: int = 20,
    baseline_sessions: int = 100,
    min_coverage: float = 0.3,
    include_wording: bool = False,
) -> Dict[str, Any]:
    """
    Returns {"sessions", "versions": {key: {"sessions", "reference_questions", "bot_ids"}},
    "new_questions", "dropped_questions", "historical_changes", "wording_variants", "proposal", "reason"}. `proposal` is None when nothing new was found or
    the new questions are not concentrated after one instant (versions running side by side); `reason` says why.
    """
    if len(version_keys) == 0:
        raise ValueError("at least one recorded version key is required")
    if min_sessions < 1 or baseline_sessions < 1:
        raise ValueError("min_sessions and baseline_sessions must be at least 1")
    if not 0 < min_coverage <= 1:
        raise ValueError("min_coverage must be in (0, 1]")

    ordered = sorted(sessions, key=lambda s: (s.start, s.session_id))
    total = len(ordered)
    display: Dict[str, str] = {}
    sets: List[Set[str]] = []
    for session in ordered:
        found: Set[str] = set()
        for text in session.texts:
            norm = normalise(text)
            if norm != "":
                found.add(norm)
                display.setdefault(norm, text.strip())
        sets.append(found)

    catalogues: Dict[str, Set[str]] = {}
    for key in version_keys:
        sample = [i for i, s in enumerate(ordered) if s.stamped == key][:baseline_sessions]
        support = Counter(t for i in sample for t in sets[i])
        need = 2 if len(sample) >= _NEED_TWO_SESSIONS_FROM else 1
        catalogues[key] = {t for t, n in support.items() if n >= need}
    known: Set[str] = set().union(*catalogues.values())

    versions: Dict[str, Any] = {
        key: {
            "sessions": sum(1 for s in ordered if s.stamped == key),
            "reference_questions": len(catalogues[key]),
            "bot_ids": dict(Counter(s.bot_id for s in ordered if s.stamped == key and s.bot_id is not None)),
        }
        for key in version_keys
    }

    occurrences: Dict[str, List[int]] = {}
    for i, found in enumerate(sets):
        for t in found:
            occurrences.setdefault(t, []).append(i)

    new_texts: Dict[str, Dict[str, Any]] = {}
    for t, where in occurrences.items():
        if t in known:
            continue
        coverage = len(where) / (total - where[0])
        if len(where) >= min_sessions and coverage >= min_coverage:
            nearest, nearest_text = 0.0, None
            for k in sorted(known):
                ratio = SequenceMatcher(None, t, k).ratio()
                if ratio > nearest:
                    nearest, nearest_text = ratio, k
            entry = {"text": display[t], "sessions": len(where), "coverage": round(coverage, 2),
                     "first_seen": ordered[where[0]].start.isoformat(), "nearest_known_similarity": round(nearest, 2)}
            if nearest_text is not None and nearest >= WORDING_SIMILARITY:
                entry["similar_to"] = display[nearest_text]
            new_texts[t] = entry

    result: Dict[str, Any] = {
        "sessions": total,
        "versions": versions,
        "new_questions": sorted(new_texts.values(), key=lambda q: (q["first_seen"], q["text"]))[:_LIST_LIMIT],
        "new_questions_total": len(new_texts),
        "dropped_questions": [],
        "historical_changes": [],
        "wording_variants": [],
        "proposal": None,
        "reason": None,
    }
    if len(new_texts) == 0:
        result["reason"] = "no new question found: every sustained question is already known to a recorded version"
        return result

    variants = [q for q in new_texts.values() if "similar_to" in q]
    result["wording_variants"] = sorted(variants, key=lambda q: (q["first_seen"], q["text"]))[:_LIST_LIMIT]
    if len(variants) == len(new_texts) and not include_wording:
        result["reason"] = ("the only new questions are close wording variants of known ones (see wording_variants); "
                            "not proposed as a new version. Use --include-wording if one of them is a real change")
        return result

    # The latest recorded window: from the first session stamped with the last key (a single recorded version is the
    # whole history). With several versions and no session stamped with the last one there is no window to propose
    # in (the last window starts after every stored session, or the sessions still need a restamp).
    last_key = version_keys[-1]
    stamped_last = [i for i, s in enumerate(ordered) if s.stamped == last_key]
    if len(version_keys) == 1:
        window_from = 0
    elif len(stamped_last) > 0:
        window_from = stamped_last[0]
    else:
        window_from = total
    # a new text is a candidate only when some session of the latest window asked it; the rest is history
    candidates = {t for t in new_texts if occurrences[t][-1] >= window_from}
    historical = [q for t, q in new_texts.items() if t not in candidates]
    result["historical_changes"] = sorted(historical, key=lambda q: (q["first_seen"], q["text"]))[:_LIST_LIMIT]
    if len(candidates) == 0:
        result["reason"] = ("the new questions were asked only before the latest recorded version started (see "
                            "historical_changes); a version cannot be appended inside an older window, so nothing "
                            "is proposed")
        return result
    # boundary evidence: the questions a session must carry to count as "new". A close wording variant is not
    # evidence of a new questionnaire (it is listed in wording_variants), unless include_wording asks for it.
    evidence = {t for t in candidates if include_wording or "similar_to" not in new_texts[t]}
    if len(evidence) == 0:
        result["reason"] = ("the only new questions of the latest window are close wording variants of known ones "
                            "(see wording_variants); not proposed as a new version. Use --include-wording if one of "
                            "them is a real change")
        return result
    novel = [i >= window_from and any(t in evidence for t in found) for i, found in enumerate(sets)]
    novel_total = sum(novel)
    # best single step: fewest new-question sessions before it plus old-question sessions from it on. The step is an
    # INSTANT and a session is labelled new when its start is at or after it, so a cut may only fall where the start
    # instant strictly increases: sessions that start at the same instant stay on one side (otherwise `set` would
    # relabel more sessions than the details report).
    errors = total - novel_total                 # step at 0
    best_errors, best_at = errors, 0
    for j in range(total):
        errors += (1 if novel[j] else 0) - (0 if novel[j] else 1)
        cut = j + 1
        if cut < total and ordered[cut - 1].start == ordered[cut].start:
            continue                             # a tie: no instant separates these two sessions
        if errors < best_errors:
            best_errors, best_at = errors, cut
    # The step may start at a session with a new question. Sessions of one instant cannot be told apart, so the whole
    # instant is skipped only when NONE of its sessions carries new-question evidence; when some do, the step stays at
    # the instant (its old-only sessions are counted in the details, not allowed to hide the rest).
    while best_at < total:
        group_end = best_at + 1
        while group_end < total and ordered[group_end].start == ordered[best_at].start:
            group_end += 1
        if any(novel[best_at:group_end]):
            break
        best_at = group_end
    if best_at >= total:
        result["reason"] = "no instant separates the new-question sessions from the rest"
        return result
    if best_at <= window_from:
        result["reason"] = ("the new questions are asked from the very first session of the latest recorded version, "
                            "so they cannot be a later version of it; check which version the first sessions "
                            "belong to")
        return result

    after = total - best_at
    novel_after = sum(novel[best_at:])
    purity = novel_after / novel_total
    share_after = novel_after / after
    start = ordered[best_at].start
    result["dropped_questions"] = sorted(
        display[t] for t, where in occurrences.items()
        if t in known and sum(1 for i in where if i < best_at) >= min_sessions and all(i < best_at for i in where)
    )[:_LIST_LIMIT]
    details = {
        "sessions_before": best_at,
        "sessions_from_start": after,
        "new_question_sessions_before": novel_total - novel_after,
        "old_question_sessions_from_start": after - novel_after,
        "purity": round(purity, 3),
        "share_after": round(share_after, 3),
    }
    if purity < MIN_PURITY or share_after < MIN_SHARE_AFTER:
        result["reason"] = ("the new questions are not concentrated after one instant (old and new question sets "
                            "look to be running side by side); nothing is proposed")
        result["proposal"] = None
        result["details"] = details
        return result
    # Evidence only: a bot id that changes at the same instant as the questions corroborates the proposal; the same
    # id on both sides (a bot edited in place) does not weaken it.
    before_bots = {s.bot_id for s in ordered[:best_at] if s.bot_id is not None}
    after_bots = {s.bot_id for s in ordered[best_at:] if s.bot_id is not None}
    result["proposal"] = {
        "key": next_key(list(version_keys)),
        "effective_from": start.isoformat(),
        **details,
        "bot_ids_before": sorted(before_bots),
        "bot_ids_from_start": sorted(after_bots),
        "bot_id_changed_at_start": len(before_bots) > 0 and len(after_bots) > 0 and before_bots.isdisjoint(after_bots),
    }
    return result
