"""
Questionnaire versions of the feedback bots (pure functions, no I/O).

The upload config of a persona (any source whose adapter versions its questionnaire) carries an ordered list
    "questionnaire_versions": [{"key": "V1", "effective_from": null},
                               {"key": "V2", "effective_from": "2030-01-15T09:00:00+05:30"}]
and a session's version is the last entry whose effective_from is not after the session's first real
message. This is how a Google Forms response and a manual override get their label, and the date-window label
is still required in every event and checked as input.

Mitra bot sessions (adapter conversation_rows) are different: a new questionnaire is always a NEW bot id, so the
STORED label follows the order of the bot ids of the persona (rank_bot_versions below), never a date: within one
persona (tenant, program, session_type, source) the lowest bot id is V1, the next V2, and so on. Numeric order, so
the ranks only depend on which bots exist, not on when their sessions arrived.
"""
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

VersionList = List[Tuple[str, Optional[datetime]]]


def parse_versions(config: Dict[str, Any]) -> VersionList:
    """
    config["questionnaire_versions"] -> [(key, aware datetime | None)]. The first entry starts from the
    beginning (null); the others carry an offset-aware ISO instant, strictly increasing. Raises ValueError.
    """
    entries = config.get("questionnaire_versions")
    if not isinstance(entries, list) or len(entries) == 0:
        raise ValueError("Feedback config 'questionnaire_versions' must be a non-empty list")
    parsed: VersionList = []
    seen_keys = set()
    previous: Optional[datetime] = None
    for position, entry in enumerate(entries):
        key = entry.get("key") if isinstance(entry, dict) else None
        if not isinstance(key, str) or key.strip() == "":
            raise ValueError(f"Feedback config questionnaire_versions[{position}] needs a non-empty 'key'")
        if key in seen_keys:
            raise ValueError(f"Feedback config questionnaire_versions has the key {key!r} twice")
        seen_keys.add(key)
        raw = entry.get("effective_from")
        if position == 0:
            if raw is not None:
                raise ValueError(
                    "Feedback config questionnaire_versions[0].effective_from must be null (from the beginning)"
                )
            parsed.append((key, None))
            continue
        if not isinstance(raw, str):
            raise ValueError(f"Feedback config questionnaire_versions[{position}].effective_from must be an ISO instant")
        try:
            effective = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw)
        except ValueError:
            raise ValueError(
                f"Feedback config questionnaire_versions[{position}].effective_from {raw!r} is not an ISO instant"
            )
        if effective.tzinfo is None:
            raise ValueError(
                f"Feedback config questionnaire_versions[{position}].effective_from {raw!r} needs a UTC offset"
            )
        if previous is not None and effective <= previous:
            raise ValueError("Feedback config questionnaire_versions must have strictly increasing effective_from")
        previous = effective
        parsed.append((key, effective))
    return parsed


def stamp_version(versions: VersionList, session_start: datetime) -> str:
    """The last version whose effective_from is not after the session's first real message."""
    for key, effective in reversed(versions):
        if effective is None or effective <= session_start:
            return key
    return versions[0][0]


# A bot id is a positive integer written in digits, without a sign, a leading zero or spaces: then two spellings can
# never denote one bot (89 and 089), and the numeric order the ranks rest on is well defined. At most 18 digits so it
# also fits a bigint for the SQL side (app/database/feedback_bot_versions.py applies the same pattern).
BOT_ID_PATTERN = r"[1-9][0-9]{0,17}"
_BOT_ID = re.compile(BOT_ID_PATTERN)

BOT_ID_RULE = "a positive integer written in digits (no sign, no leading zero, at most 18 digits)"


def is_valid_bot_id(value: Any) -> bool:
    """True for a bot id string that is a positive integer written in digits (see BOT_ID_PATTERN)."""
    return isinstance(value, str) and _BOT_ID.fullmatch(value) is not None


def bot_id_problem(value: Any) -> Optional[str]:
    """None for a valid bot id, else a message that names the rule. The value is never echoed (it comes from a file)."""
    if value is None:
        return "bot id is missing"
    if not isinstance(value, str):
        return f"bot id must be a string, got {type(value).__name__}"
    if len(value.strip()) == 0:
        return "bot id is blank"
    if not is_valid_bot_id(value):
        return f"bot id must be {BOT_ID_RULE}"
    return None


def rank_bot_versions(bot_ids: Iterable[str]) -> Dict[str, str]:
    """
    {bot id: 'V<rank>'} for the distinct bot ids of ONE persona: ranked by numeric value ascending, so the lowest
    bot id is V1, the next V2, and so on. Duplicates are ignored. Raises ValueError for a bot id that is not valid
    (is_valid_bot_id): the callers validate at the boundary, so this never has to guess what an odd id means.
    """
    distinct = set()
    for bot_id in bot_ids:
        if not is_valid_bot_id(bot_id):
            raise ValueError(f"cannot rank bot ids: {bot_id_problem(bot_id)}")
        distinct.add(bot_id)
    ordered = sorted(distinct, key=int)
    return {bot_id: f"V{position}" for position, bot_id in enumerate(ordered, start=1)}
