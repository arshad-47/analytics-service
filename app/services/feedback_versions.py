"""
Questionnaire versions of the feedback bots (pure functions, no I/O).

The upload config of a persona (any source whose adapter versions its questionnaire) carries an ordered list
    "questionnaire_versions": [{"key": "V1", "effective_from": null},
                               {"key": "V2", "effective_from": "2030-01-15T09:00:00+05:30"}]
and a session's version is the last entry whose effective_from is not after the session's first real
message. The version comes from the date, never from the bot id: one bot is edited several times and a
persona runs several bots over time.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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
