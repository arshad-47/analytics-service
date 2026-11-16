"""
Unit tests (in-memory) of the consumer's feedback config cache (app/services/feedback_config_cache.py):
one load per persona per TTL, a new config version replaces the old entry after the TTL, "no config" and an
invalid config are cached for the TTL too, an infrastructure error is never cached.
"""
import asyncio
from unittest.mock import AsyncMock

import pytest

from app.services.feedback_config_cache import FeedbackConfigCache, InvalidCacheKeyError

KEY = ("mitra", "program-1", "Youth_Voice", "mitra")


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _cache(results, ttl=60.0):
    """results: list of values / exceptions the loader hands out in order."""
    clock = Clock()
    calls = []

    async def loader(*key):
        calls.append(key)
        result = results[len(calls) - 1]
        if isinstance(result, Exception):
            raise result
        return result

    return FeedbackConfigCache(loader, ttl_seconds=ttl, clock=clock), clock, calls


def test_a_burst_inside_the_ttl_loads_once():
    cache, clock, calls = _cache([{"config_version_id": "v1"}])
    for _ in range(100):
        assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v1"}
        clock.now += 0.1
    assert calls == [KEY] and cache.loads == 1


def test_entry_expires_after_the_ttl_and_a_new_version_replaces_it():
    cache, clock, calls = _cache([{"config_version_id": "v1"}, {"config_version_id": "v2"}, {"config_version_id": "v2"}])
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v1"
    clock.now += 59.9
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v1"  # still inside the TTL
    clock.now += 0.2
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v2"  # reloaded: the new version wins
    assert cache.replaced_versions == 1
    clock.now += 61
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v2"
    assert cache.replaced_versions == 1  # same version again: not a replacement
    assert cache.loads == 3


def test_keys_are_independent():
    cache, _, calls = _cache([{"config_version_id": "a"}, {"config_version_id": "b"}])
    asyncio.run(cache.get("mitra", "p", "Youth_Voice", "mitra"))
    asyncio.run(cache.get("mitra", "p", "Youth_Voice", "google_forms"))
    asyncio.run(cache.get("mitra", "p", "Youth_Voice", "mitra"))
    assert [c[3] for c in calls] == ["mitra", "google_forms"]


def test_missing_config_is_cached_for_the_ttl():
    cache, clock, calls = _cache([None, {"config_version_id": "v1"}])
    assert asyncio.run(cache.get(*KEY)) is None
    assert asyncio.run(cache.get(*KEY)) is None
    assert len(calls) == 1
    clock.now += 61
    assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v1"}  # the config was added meanwhile


def test_invalid_config_is_cached_and_raises_every_time():
    cache, clock, calls = _cache([ValueError("bad schema"), {"config_version_id": "v2"}])
    for _ in range(3):
        with pytest.raises(ValueError, match="bad schema"):
            asyncio.run(cache.get(*KEY))
    assert len(calls) == 1
    clock.now += 61
    assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v2"}  # fixed config is picked up


def test_infrastructure_errors_are_not_cached():
    cache, _, calls = _cache([ConnectionError("db down"), {"config_version_id": "v1"}])
    with pytest.raises(ConnectionError):
        asyncio.run(cache.get(*KEY))
    assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v1"}  # next event tries again immediately
    assert len(calls) == 2


def test_invalidate_forces_a_reload():
    cache, _, calls = _cache([{"config_version_id": "v1"}, {"config_version_id": "v2"}])
    asyncio.run(cache.get(*KEY))
    cache.invalidate()
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v2"


@pytest.mark.parametrize("ttl", [0, -1])
def test_ttl_must_be_positive(ttl):
    with pytest.raises(ValueError):
        FeedbackConfigCache(lambda *k: None, ttl_seconds=ttl)


# B12: key components come from an event that has not been validated yet --------------------------------------------

BIG = "x" * 1_000_000


@pytest.mark.parametrize("position,bad", [
    (0, BIG), (1, BIG), (2, BIG), (3, BIG),                           # a megabyte in any component
    (0, "x" * 65), (2, "x" * 65), (3, "x" * 41),                      # one over the limit
    (0, ""), (1, ""), (2, ""), (3, ""),                               # empty
    (0, "mi tra"), (2, "Youth Voice"), (2, "Youth\nVoice"), (2, "Youth_Voice\n"), (0, "mitra\r"),
    (2, "../Youth"), (2, "Youth/Voice"), (3, "Mitra"), (3, "goo gle"), (3, "a-b"),   # sources are lower-case labels
    (0, None), (1, 7), (2, ["Youth_Voice"]), (3, b"mitra"),           # not a string
])
def test_a_key_that_is_not_a_label_is_refused_without_being_stored_or_loaded(position, bad):
    cache, _, calls = _cache([{"config_version_id": "v1"}])
    key = list(KEY)
    key[position] = bad
    with pytest.raises(InvalidCacheKeyError) as raised:
        asyncio.run(cache.get(*key))
    assert isinstance(raised.value, ValueError)            # the consumer already turns a ValueError into a DLQ problem
    assert len(str(raised.value)) < 200                    # the message never carries the value
    assert "xxxx" not in str(raised.value)
    assert calls == [] and cache.loads == 0                # the loader (database) was not asked
    assert len(cache._entries) == 0                        # nothing was inserted: no negative entry either
    assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v1"}   # the cache still works


def test_a_burst_of_oversize_keys_pins_no_memory():
    cache, _, calls = _cache([])
    for i in range(50):
        with pytest.raises(InvalidCacheKeyError):
            asyncio.run(cache.get("mitra", "p", BIG + str(i), "mitra"))
    assert len(cache._entries) == 0 and calls == [] and cache.evictions == 0


def test_normal_labels_are_accepted():
    cache, _, calls = _cache([{"config_version_id": "v1"}])
    key = ("mitra", "8f14e45f-ceea-467f-a0e6-6c31f1a1e6b5", "Youth_Voice.v2-x", "ptm_app")
    assert asyncio.run(cache.get(*key)) == {"config_version_id": "v1"} and calls == [key]


def test_the_module_documents_the_staleness_window():
    import app.services.feedback_config_cache as module
    assert "STALENESS" in module.__doc__ and "ttl_seconds" in module.__doc__


# the cache is bounded ----------------------------------------------------------------------------------------------------

def test_config_cache_is_bounded_and_evicts_the_oldest_entries():
    async def run():
        loader = AsyncMock(side_effect=lambda t, p, s, src: {"config_version_id": f"{t}-{p}"})
        cache = FeedbackConfigCache(loader, ttl_seconds=60, max_entries=3)
        for n in range(5):
            await cache.get("t", str(n), "S", "mitra")
        assert len(cache._entries) == 3 and cache.evictions == 2
        assert list(k[1] for k in cache._entries) == ["2", "3", "4"]  # the oldest two were evicted
        loads = cache.loads
        await cache.get("t", "4", "S", "mitra")      # still cached
        assert cache.loads == loads
        await cache.get("t", "0", "S", "mitra")      # evicted: loaded again
        assert cache.loads == loads + 1
        # errors and misses count against the same bound
        bad = FeedbackConfigCache(AsyncMock(side_effect=ValueError("bad")), ttl_seconds=60, max_entries=2)
        for n in range(4):
            with pytest.raises(ValueError):
                await bad.get("t", str(n), "S", "mitra")
        assert len(bad._entries) == 2
    asyncio.run(run())


def test_config_cache_default_bound_is_1024_and_must_be_positive():
    assert FeedbackConfigCache(AsyncMock())._max_entries == 1024
    with pytest.raises(ValueError):
        FeedbackConfigCache(AsyncMock(), max_entries=0)
