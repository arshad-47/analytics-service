"""
Test-owned questionnaire version windows.

Production code carries NO version dates: a seeded persona config only has the baseline
`[{"key": "V1", "effective_from": None}]`, and later versions are recorded in the database (new config
version) with tools/questionnaire_versions.py. The tests that exercise the stamping mechanism therefore
inject their own windows into a copy of the baseline config with `config_with_windows`. The dates below
are synthetic test data, not configuration.
"""
import copy
from datetime import datetime

from seed_support import mitra_config_v1

# Start of the (test) V2 window per persona, an ISO instant with a UTC offset.
TEST_V2_START = {
    "Youth": "2026-11-03T18:23:03+05:30",
    "General Public": "2026-11-03T18:26:37+05:30",
    "Parent": "2026-11-03T18:52:54+05:30",
    "Educator": "2026-11-07T09:00:00+05:30",
}

# Start of the (test) V3 window per persona: always later than the V2 start.
TEST_V3_START = {
    "Youth": "2026-12-01T10:30:00+05:30",
    "General Public": "2026-12-01T10:30:00+05:30",
    "Parent": "2026-12-01T10:30:00+05:30",
    "Educator": "2026-12-15T10:30:00+05:30",
}


def config_with_windows(persona: str, *entries) -> dict:
    """
    A deep copy of the persona's baseline Mitra config whose `questionnaire_versions` is the baseline V1
    entry followed by `entries`, each a (key, effective_from) pair. With no entries it is the baseline.
    """
    config = copy.deepcopy(mitra_config_v1(persona))
    versions = config["questionnaire_versions"][:1]
    versions += [{"key": key, "effective_from": effective_from} for key, effective_from in entries]
    config["questionnaire_versions"] = versions
    return config


def config_with_v2(persona: str) -> dict:
    """Baseline V1 plus the persona's test V2 window."""
    return config_with_windows(persona, ("V2", TEST_V2_START[persona]))


def config_with_v2_v3(persona: str) -> dict:
    """Baseline V1, the persona's test V2 window and its later test V3 window."""
    return config_with_windows(persona, ("V2", TEST_V2_START[persona]), ("V3", TEST_V3_START[persona]))


def v2_instant(persona: str) -> datetime:
    return datetime.fromisoformat(TEST_V2_START[persona])


def v3_instant(persona: str) -> datetime:
    return datetime.fromisoformat(TEST_V3_START[persona])
