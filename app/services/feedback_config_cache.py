"""
A small in-memory, short-TTL cache of the feedback settings a Kafka consumer needs per event (the persona's
`ingestion_schema` and `process_steps`), so a burst of events does not query the database once per event.

The key is (tenant_code, program_id, session_type, source); an entry remembers the config_version id it was loaded
from. After `ttl_seconds` the entry is reloaded, so a new config version (written by tools/feedback_config.py or a
questionnaire version change) is picked up within one TTL and replaces the old entry (`replaced_versions` counts
how often that happened). A loader that returns None (no active config) or raises ValueError (an invalid config)
is cached too, for the same TTL, so a burst for an unconfigured persona is also one query; any other exception
(database down) is not cached and propagates.

The cache holds at most `max_entries` keys (default 1024): when a new key would exceed it, the oldest-stored entries
are evicted (`evictions` counts them). The keys are built from event content that has not been validated yet, so
the cache also bounds what a key may be made of: every component (tenant, program, session_type, source) must be a
label (SAFE_CODE_RE from feedback_constants for the tenant, the program id and the session_type: 1 to 64 characters
of letters, digits, '.', '_', '-'; SOURCE_LABEL_RE for the source). A key that is not is refused with
InvalidCacheKeyError (a ValueError whose message names the component and never carries the value) before anything is
loaded or stored, so neither the number nor the size of the keys can be inflated from outside.

STALENESS (by design): a config change (tools/feedback_config.py set, a questionnaire version change) is picked up by
each consumer process only when its cached entry expires, so for up to `ttl_seconds` (the consumer uses 60 s) events
are still validated and stamped under the OLD config; the upload route reads the config uncached and can therefore
disagree with the consumer for that minute. Tightening a schema takes effect after the TTL, not at once.
"""
import re
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from app.services.feedback_constants import SAFE_CODE_RE, SOURCE_LABEL_RE

CacheKey = Tuple[str, str, str, str]

DEFAULT_MAX_ENTRIES = 1024

# What each key component may be (the same label rules the ingestion validation applies to an event).
_CODE = re.compile(SAFE_CODE_RE)
_SOURCE = re.compile(SOURCE_LABEL_RE)
_KEY_PARTS = (("tenant_code", _CODE), ("program_id", _CODE), ("session_type", _CODE), ("source", _SOURCE))


class InvalidCacheKeyError(ValueError):
    """A key component is not a label (wrong type, empty, too long or with other characters). Never carries the value."""


def _check_key(key: Tuple[Any, ...]) -> None:
    for (name, pattern), part in zip(_KEY_PARTS, key):
        if not isinstance(part, str):
            raise InvalidCacheKeyError(f"{name} must be a string, got {type(part).__name__}")
        if pattern.fullmatch(part) is None:
            raise InvalidCacheKeyError(
                f"{name} is not a valid label (letters, digits and a few separators, at most 64 characters; "
                f"got {len(part)} characters)"
            )


class FeedbackConfigCache:
    def __init__(
        self,
        loader: Callable[[str, str, str, str], Awaitable[Optional[Dict[str, Any]]]],
        ttl_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        max_entries: int = DEFAULT_MAX_ENTRIES,
    ):
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self._loader = loader
        self._ttl = ttl_seconds
        self._clock = clock
        self._max_entries = max_entries
        # key -> (expires_at, value | None, error message | None)
        self._entries: Dict[CacheKey, Tuple[float, Optional[Dict[str, Any]], Optional[str]]] = {}
        self.loads = 0
        self.replaced_versions = 0
        self.evictions = 0

    def _store(self, key: CacheKey, entry: Tuple[float, Optional[Dict[str, Any]], Optional[str]]) -> None:
        """Insert as the newest entry; evict the oldest ones while the cache is over its size."""
        self._entries.pop(key, None)
        self._entries[key] = entry
        while len(self._entries) > self._max_entries:
            del self._entries[next(iter(self._entries))]
            self.evictions += 1

    def invalidate(self) -> None:
        self._entries.clear()

    async def get(self, tenant_code: str, program_id: str, session_type: str, source: str) -> Optional[Dict[str, Any]]:
        """The loaded settings (a dict that carries "config_version_id"), or None when the persona has no active
        config. Raises ValueError (with the cached message) when its config is invalid."""
        key: CacheKey = (tenant_code, program_id, session_type, source)
        _check_key(key)
        now = self._clock()
        entry = self._entries.get(key)
        if entry is not None and entry[0] > now:
            if entry[2] is not None:
                raise ValueError(entry[2])
            return entry[1]

        self.loads += 1
        try:
            value = await self._loader(*key)
        except ValueError as exc:
            self._store(key, (now + self._ttl, None, str(exc)))
            raise
        if (entry is not None and entry[1] is not None and value is not None
                and entry[1].get("config_version_id") != value.get("config_version_id")):
            self.replaced_versions += 1
        self._store(key, (now + self._ttl, value, None))
        return value
