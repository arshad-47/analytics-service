"""
Unit tests for app/services/feedback_versions.py (questionnaire version parsing and stamping; pure functions).
"""
from datetime import datetime, timedelta, timezone

import pytest

from seed_support import PERSONAS, mitra_config_v1
from app.services.feedback_versions import parse_versions, stamp_version
from feedback_test_windows import TEST_V2_START, TEST_V3_START, config_with_v2, config_with_v2_v3, config_with_windows

IST = timezone(timedelta(hours=5, minutes=30))
V2_AT = "2026-11-03T18:23:03+05:30"
V3_AT = "2026-12-01T09:00:00+05:30"


def cfg(*entries):
    return {"questionnaire_versions": [{"key": k, "effective_from": f} for k, f in entries]}


# ---------------------------------------------------------------------------------- parse_versions: valid

def test_parse_two_versions():
    versions = parse_versions(cfg(("V1", None), ("V2", V2_AT)))
    assert versions == [("V1", None), ("V2", datetime(2026, 11, 3, 18, 23, 3, tzinfo=IST))]


def test_parse_single_version():
    assert parse_versions(cfg(("V1", None))) == [("V1", None)]


def test_parse_three_versions_keeps_order():
    versions = parse_versions(cfg(("V1", None), ("V2", V2_AT), ("V3", V3_AT)))
    assert [k for k, _ in versions] == ["V1", "V2", "V3"]
    assert versions[0][1] is None
    assert versions[1][1] < versions[2][1]


def test_parse_returns_aware_datetimes():
    versions = parse_versions(cfg(("V1", None), ("V2", V2_AT)))
    assert versions[1][1].tzinfo is not None
    assert versions[1][1].utcoffset() == timedelta(hours=5, minutes=30)


@pytest.mark.parametrize("suffix", ["Z", "z"])
def test_z_suffix_is_accepted_as_utc(suffix):
    versions = parse_versions(cfg(("V1", None), ("V2", "2026-11-03T12:53:03" + suffix)))
    assert versions[1][1] == datetime(2026, 11, 3, 12, 53, 3, tzinfo=timezone.utc)
    assert versions[1][1] == datetime(2026, 11, 3, 18, 23, 3, tzinfo=IST)


def test_offsets_other_than_ist_are_compared_as_instants():
    # 2026-11-03T12:00Z is 17:30 IST: later than 17:00+05:30, earlier than 18:00+05:30
    parse_versions(cfg(("V1", None), ("V2", "2026-11-03T17:00:00+05:30"), ("V3", "2026-11-03T12:00:00Z")))
    with pytest.raises(ValueError):
        parse_versions(cfg(("V1", None), ("V2", "2026-11-03T18:00:00+05:30"), ("V3", "2026-11-03T12:00:00Z")))


def test_fractional_seconds_are_accepted():
    versions = parse_versions(cfg(("V1", None), ("V2", "2026-11-03T18:23:03.500+05:30")))
    assert versions[1][1].microsecond == 500000


@pytest.mark.parametrize("persona", list(PERSONAS))
def test_every_real_persona_config_parses(persona):
    versions = parse_versions(mitra_config_v1(persona))
    assert versions[0][1] is None
    assert [k for k, _ in versions][0] == "V1"


# ---------------------------------------------------------------------------------- parse_versions: invalid

@pytest.mark.parametrize("config", [
    {},
    {"questionnaire_versions": None},
    {"questionnaire_versions": []},
    {"questionnaire_versions": "V1"},
    {"questionnaire_versions": {"key": "V1", "effective_from": None}},
    {"questionnaire_versions": ("V1",)},
])
def test_missing_or_non_list_or_empty_is_rejected(config):
    with pytest.raises(ValueError, match="non-empty list"):
        parse_versions(config)


@pytest.mark.parametrize("entry", [
    "V1",
    None,
    42,
    ["V1", None],
    {"effective_from": None},
    {"key": None, "effective_from": None},
    {"key": "", "effective_from": None},
    {"key": "   ", "effective_from": None},
    {"key": 1, "effective_from": None},
])
def test_entry_without_a_usable_key_is_rejected(entry):
    with pytest.raises(ValueError, match="non-empty 'key'"):
        parse_versions({"questionnaire_versions": [entry]})


def test_second_entry_without_a_key_is_rejected_and_named_by_position():
    with pytest.raises(ValueError, match=r"questionnaire_versions\[1\]"):
        parse_versions({"questionnaire_versions": [{"key": "V1", "effective_from": None}, {"effective_from": V2_AT}]})


def test_duplicate_keys_are_rejected():
    with pytest.raises(ValueError, match="twice"):
        parse_versions(cfg(("V1", None), ("V1", V2_AT)))


def test_first_entry_must_start_from_the_beginning():
    with pytest.raises(ValueError, match=r"\[0\]\.effective_from must be null"):
        parse_versions(cfg(("V1", V2_AT)))


def test_later_entry_without_effective_from_is_rejected():
    with pytest.raises(ValueError, match=r"\[1\]\.effective_from must be an ISO instant"):
        parse_versions(cfg(("V1", None), ("V2", None)))


@pytest.mark.parametrize("raw", [20261103, 1.5, ["2026-11-03T18:23:03+05:30"], True])
def test_non_string_effective_from_is_rejected(raw):
    with pytest.raises(ValueError, match="must be an ISO instant"):
        parse_versions(cfg(("V1", None), ("V2", raw)))


@pytest.mark.parametrize("raw", ["", "yesterday", "03/11/2026", "2026-13-45T00:00:00+05:30", "not-a-dateZ"])
def test_unparsable_effective_from_is_rejected(raw):
    with pytest.raises(ValueError):
        parse_versions(cfg(("V1", None), ("V2", raw)))


@pytest.mark.parametrize("raw", ["2026-11-03T18:23:03", "2026-11-03", "2026-11-03 18:23:03"])
def test_naive_datetime_is_rejected(raw):
    with pytest.raises(ValueError, match="UTC offset|ISO instant"):
        parse_versions(cfg(("V1", None), ("V2", raw)))


def test_naive_datetime_message_asks_for_an_offset():
    with pytest.raises(ValueError, match="needs a UTC offset"):
        parse_versions(cfg(("V1", None), ("V2", "2026-11-03T18:23:03")))


def test_equal_effective_from_is_rejected_as_not_strictly_increasing():
    with pytest.raises(ValueError, match="strictly increasing"):
        parse_versions(cfg(("V1", None), ("V2", V2_AT), ("V3", V2_AT)))


def test_equal_instants_in_different_offsets_are_not_strictly_increasing():
    with pytest.raises(ValueError, match="strictly increasing"):
        parse_versions(cfg(("V1", None), ("V2", "2026-11-03T18:23:03+05:30"), ("V3", "2026-11-03T12:53:03Z")))


def test_decreasing_effective_from_is_rejected():
    with pytest.raises(ValueError, match="strictly increasing"):
        parse_versions(cfg(("V1", None), ("V2", V3_AT), ("V3", V2_AT)))


# ---------------------------------------------------------------------------------- stamp_version

def at(text):
    return datetime.fromisoformat(text)


def test_single_version_is_always_stamped():
    versions = parse_versions(cfg(("V1", None)))
    assert stamp_version(versions, at("1999-01-01T00:00:00+00:00")) == "V1"
    assert stamp_version(versions, at("2099-01-01T00:00:00+00:00")) == "V1"


def test_two_version_boundaries():
    versions = parse_versions(cfg(("V1", None), ("V2", V2_AT)))
    start = at(V2_AT)
    assert stamp_version(versions, start - timedelta(seconds=1)) == "V1"
    assert stamp_version(versions, start) == "V2"
    assert stamp_version(versions, start + timedelta(seconds=1)) == "V2"
    assert stamp_version(versions, start - timedelta(days=400)) == "V1"
    assert stamp_version(versions, start + timedelta(days=400)) == "V2"


def test_three_version_boundaries():
    versions = parse_versions(cfg(("V1", None), ("V2", V2_AT), ("V3", V3_AT)))
    one, two = at(V2_AT), at(V3_AT)
    assert stamp_version(versions, one - timedelta(seconds=1)) == "V1"
    assert stamp_version(versions, one) == "V2"
    assert stamp_version(versions, two - timedelta(seconds=1)) == "V2"
    assert stamp_version(versions, two) == "V3"
    assert stamp_version(versions, two + timedelta(seconds=1)) == "V3"


def test_stamping_compares_instants_across_offsets():
    versions = parse_versions(cfg(("V1", None), ("V2", V2_AT)))
    # 2026-11-03T12:53:03Z is exactly 18:23:03+05:30
    assert stamp_version(versions, at("2026-11-03T12:53:02+00:00")) == "V1"
    assert stamp_version(versions, at("2026-11-03T12:53:03+00:00")) == "V2"
    assert stamp_version(versions, at("2026-11-03T18:23:02+05:30")) == "V1"


def test_microsecond_before_the_boundary_is_the_old_version():
    versions = parse_versions(cfg(("V1", None), ("V2", V2_AT)))
    assert stamp_version(versions, at(V2_AT) - timedelta(microseconds=1)) == "V1"


def test_first_entry_with_no_start_covers_the_distant_past():
    versions = parse_versions(cfg(("OLD", None), ("NEW", V2_AT)))
    assert stamp_version(versions, datetime(1970, 1, 1, tzinfo=timezone.utc)) == "OLD"


@pytest.mark.parametrize("persona", list(PERSONAS))
def test_persona_config_with_an_injected_v2_window(persona):
    # the seeded config is only the baseline; the window is test data injected into a copy of it
    assert [k for k, _ in parse_versions(mitra_config_v1(persona))] == ["V1"]
    versions = parse_versions(config_with_v2(persona))
    start = at(TEST_V2_START[persona])
    assert stamp_version(versions, start - timedelta(seconds=1)) == "V1"
    assert stamp_version(versions, start) == "V2"
    assert stamp_version(versions, start + timedelta(seconds=1)) == "V2"


@pytest.mark.parametrize("persona", list(PERSONAS))
def test_a_third_version_needs_no_code(persona):
    versions = parse_versions(config_with_v2_v3(persona))
    assert [k for k, _ in versions] == ["V1", "V2", "V3"]
    v2, v3 = at(TEST_V2_START[persona]), at(TEST_V3_START[persona])
    one = timedelta(seconds=1)
    assert [stamp_version(versions, t) for t in (v2 - one, v2, v3 - one, v3, v3 + one)] == [
        "V1", "V2", "V2", "V3", "V3"]


def test_config_with_windows_without_entries_is_the_baseline():
    assert config_with_windows("Parent")["questionnaire_versions"] == [{"key": "V1", "effective_from": None}]
    versions = parse_versions(config_with_windows("Parent"))
    assert stamp_version(versions, at("2030-01-01T00:00:00+05:30")) == "V1"


@pytest.mark.parametrize("persona", list(PERSONAS))
def test_the_seeded_baseline_stamps_every_date_v1(persona):
    versions = parse_versions(mitra_config_v1(persona))
    assert [k for k, _ in versions] == ["V1"]
    for stamp in ("2020-01-01T00:00:00+05:30", "2026-11-07T00:00:00+05:30", "2030-01-01T00:00:00+05:30"):
        assert stamp_version(versions, at(stamp)) == "V1"


def test_educator_has_a_single_version():
    versions = parse_versions(mitra_config_v1("Educator"))
    assert [k for k, _ in versions] == ["V1"]
    assert stamp_version(versions, at("2026-11-07T00:00:00+05:30")) == "V1"
    assert stamp_version(versions, at("2030-01-01T00:00:00+05:30")) == "V1"
