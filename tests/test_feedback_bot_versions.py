"""
Pure tests (no database) of the rule "the questionnaire version of a Mitra bot session follows the ORDER OF THE BOT IDS
of its persona": rank_bot_versions, the bot id format, and the boundary checks that make the bot id required for
adapter conversation_rows. The database side is tests/test_feedback_bot_versions_pg.py (opt-in).
"""
import copy
import json
from pathlib import Path

import pytest

from app.services.feedback_versions import (
    BOT_ID_RULE,
    bot_id_problem,
    is_valid_bot_id,
    rank_bot_versions,
)
from app.services.ingestion_validation import validate_ingestion_schema
from seed_support import mitra_config_v1

ROOT = Path(__file__).resolve().parent


# ----------------------------------------------------------------------------------------- rank_bot_versions

def test_the_lowest_bot_id_is_v1_and_the_next_v2():
    assert rank_bot_versions(["89", "103"]) == {"89": "V1", "103": "V2"}


def test_the_order_is_numeric_not_alphabetical():
    # as text "103" < "89" < "9"; as numbers 9 < 89 < 103
    assert rank_bot_versions(["103", "9", "89"]) == {"9": "V1", "89": "V2", "103": "V3"}


def test_the_input_order_does_not_matter():
    assert rank_bot_versions(["103", "89"]) == rank_bot_versions(["89", "103"])
    assert rank_bot_versions(iter(["5", "1", "3"])) == {"1": "V1", "3": "V2", "5": "V3"}


def test_duplicates_are_ignored():
    assert rank_bot_versions(["103", "89", "103", "89", "89"]) == {"89": "V1", "103": "V2"}


def test_a_single_bot_is_v1_and_no_bot_gives_nothing():
    assert rank_bot_versions(["7"]) == {"7": "V1"}
    assert rank_bot_versions([]) == {}


def test_a_new_lower_bot_id_shifts_the_others():
    before = rank_bot_versions(["103"])
    after = rank_bot_versions(["103", "89"])
    assert before == {"103": "V1"} and after == {"89": "V1", "103": "V2"}


def test_labels_go_beyond_v9():
    ranks = rank_bot_versions([str(n) for n in range(1, 13)])
    assert ranks["12"] == "V12" and ranks["10"] == "V10" and len(set(ranks.values())) == 12


@pytest.mark.parametrize("bad", ["abc", "", " ", "0", "-3", "089", "1.5", " 89", "89 ", "８９", None, 89])
def test_an_invalid_bot_id_raises_and_is_not_echoed(bad):
    with pytest.raises(ValueError) as error:
        rank_bot_versions(["103", bad])
    if isinstance(bad, str) and bad.strip() != "":
        assert bad not in str(error.value)


# ---------------------------------------------------------------------------------------------- bot id format

@pytest.mark.parametrize("good", ["1", "9", "10", "89", "103", "999999999999999999"])
def test_valid_bot_ids(good):
    assert is_valid_bot_id(good) and bot_id_problem(good) is None


@pytest.mark.parametrize("bad", ["0", "00", "01", "-1", "+1", "1_0", "1,0", "1.0", "1e2", "0x10", "٣", "a1", "1a",
                                 "", " ", "1 ", " 1", "1234567890123456789"])
def test_invalid_bot_ids(bad):
    assert not is_valid_bot_id(bad) and bot_id_problem(bad) is not None


def test_bot_id_problems_name_the_rule_and_never_the_value():
    assert bot_id_problem(None) == "bot id is missing"
    assert bot_id_problem("  ") == "bot id is blank"
    assert bot_id_problem(5) == "bot id must be a string, got int"
    problem = bot_id_problem("secret-text-1")
    assert BOT_ID_RULE in problem and "secret-text-1" not in problem


def test_only_strings_are_valid():
    assert not is_valid_bot_id(89) and not is_valid_bot_id(None)


# -------------------------------------------------------------------------------- the boundary of an event

SCHEMA = mitra_config_v1("Youth")["ingestion_schema"]


def _event():
    return json.loads((ROOT / "kafka_events" / "create" / "create_feedback.json").read_text(encoding="utf-8"))


def _problems(event):
    return validate_ingestion_schema(event, "feedback", "create", feedback_schema=SCHEMA)


def test_the_fixture_event_passes():
    assert _problems(_event()) == []


def test_a_conversation_rows_event_without_a_bot_id_is_rejected_even_when_the_schema_does_not_list_it():
    # the seeded schema lists data.botId as optional; the rule lives in code so no config can switch it off
    assert "data.botId" not in SCHEMA["create"]["required"]
    event = _event()
    del event["data"]["botId"]
    assert _problems(event) == ["'data.botId' is missing (required for adapter conversation_rows)"]


@pytest.mark.parametrize("bad", ["abc", "0", "089", "-4", "4.0"])
def test_a_conversation_rows_event_with_a_bad_bot_id_is_rejected_without_echoing_it(bad):
    event = _event()
    event["data"]["botId"] = bad
    (problem,) = _problems(event)
    assert BOT_ID_RULE in problem and bad not in problem


def test_a_wide_form_event_needs_no_bot_id():
    event = copy.deepcopy(_event())
    event["data"].pop("botId", None)
    event["data"].pop("botName", None)
    event["data"]["adapter"] = "wide_form"
    event["data"]["sourceRef"] = "ref-1"
    event["data"]["completionStatus"] = "COMPLETED"
    event["submissionId"] = event["sessionId"] = "6f1d2c3a-8b4e-4f7a-9c1d-2e5b7a9f0c11"
    problems = _problems(event)
    assert not any("botId" in p for p in problems)
