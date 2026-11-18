"""
Unit tests for app/api/services/feedback_uploads.py (pure parsing / validation / event building).
All fixtures are synthetic (tests/csv_uploads/feedback_*.csv or inline frames); no real respondent text.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from app.api.services.feedback_uploads import (
    build_feedback_events,
    check_feedback_file,
    parse_feedback_csv,
    report_unexpected_stages,
)
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, mitra_config_v1
from app.services.feedback_versions import parse_versions
from feedback_test_windows import (
    TEST_V2_START,
    config_with_v2,
    config_with_v2_v3,
    config_with_windows,
    v2_instant,
    v3_instant,
)

FIXTURES = Path(__file__).resolve().parent / "csv_uploads"
PARENT = PERSONAS["Parent"]
PROGRAM = {"id": PARENT["program_id"], "name": FEEDBACK_PROGRAM_NAME}
LEADER = {"id": PARENT["leader_category_id"], "name": "Parent"}
PUBLISHED = "2026-10-20T00:00:00.000Z"

PARENT_TYPE = PARENT["session_type"]        # Parent_Voice
COLUMNS = ["bot_id", "bot_name", "session_type", "session_status", "session", "submission_id", "language",
           "sequence", "message", "translated_message", "created_at", "stage", "sender_id"]


@pytest.fixture
def config():
    # the seeded config only has the V1 baseline; the stamping tests need a V2 window, which is test data here
    return config_with_v2("Parent")


def read_fixture(name: str) -> pd.DataFrame:
    return parse_feedback_csv((FIXTURES / name).read_bytes())


def build(df, config, **overrides):
    kwargs = dict(program_info=PROGRAM, leader_info=LEADER, tenant_code="mitra", source="mitra",
                  published_at=PUBLISHED)
    kwargs.update(overrides)
    return build_feedback_events(df, config, **kwargs)


def make_df(rows, columns=COLUMNS):
    """rows: dicts overriding a baseline row; missing keys default to a valid respondent row."""
    base = {"bot_id": "88", "bot_name": "Synthetic Parent Bot", "session_type": PARENT_TYPE, "session_status": "COMPLETED", "session": "s" * 32, "submission_id": "100",
            "language": "hi", "sequence": "1", "message": "m", "translated_message": "t",
            "created_at": "2026-10-15T08:00:00.000000+05:30", "stage": "STATE_IDENTIFICATION",
            "sender_id": "120"}
    data = [{**base, **r} for r in rows]
    return pd.DataFrame(data, columns=columns).astype(str)


def session_rows(items, **common):
    """items: (kind, stage, message, translated) in CSV order starting at sequence 1."""
    rows = []
    for n, (kind, stage, message, translated) in enumerate(items, 1):
        rows.append({"sequence": str(n), "stage": stage, "message": message, "translated_message": translated,
                     "sender_id": "1" if kind == "B" else "120",
                     "created_at": f"2026-10-15T08:00:{n:02d}.000000+05:30", **common})
    return rows


def new_rows(items, **common):
    """
    New-shape session: items are (kind, stage, message, translated) with kind "O" (the opening row: blank
    sender_id / stage / created_at), "B" (bot) or "R" (respondent), numbered from sequence 1 as in the CSV.
    """
    rows = session_rows([(k if k != "O" else "B", st, m, t) for k, st, m, t in items], **common)
    for row, (kind, *_rest) in zip(rows, items):
        if kind == "O":
            row.update({"sender_id": "", "stage": "", "created_at": ""})
    return rows


def at_instant(base, seconds):
    return (base + timedelta(seconds=seconds)).isoformat()


def by_sequence(event):
    return {m["sequence"]: m for m in event["data"]["messages"]}


# ---------------------------------------------------------------------------------- parsing

def test_parse_keeps_na_and_none_answers():
    raw = b"submission_id,message\n1,NA\n2,None\n3,\n"
    df = parse_feedback_csv(raw)
    assert df["message"].tolist() == ["NA", "None", ""]


def test_parse_handles_utf8_bom():
    df = parse_feedback_csv("﻿submission_id,message\n1,x\n".encode("utf-8"))
    assert df.columns.tolist() == ["submission_id", "message"]


@pytest.mark.parametrize("raw", [b"", b"   \n", b"submission_id,message\n", b"\xff\xfe\x00bad"])
def test_parse_rejects_empty_or_unreadable(raw):
    with pytest.raises(ValueError):
        parse_feedback_csv(raw)


def test_parse_fixture_keeps_na_none():
    df = read_fixture("feedback_valid_mitra.csv")
    assert {"NA", "None"} <= set(df["message"])


# ---------------------------------------------------------------------------------- file checks

def test_check_accepts_valid_file(config):
    assert check_feedback_file(read_fixture("feedback_valid_mitra.csv"), config, PARENT_TYPE) == []


def test_check_rejects_file_with_other_persona_session_type(config):
    errors = check_feedback_file(read_fixture("feedback_mixed_bots.csv"), config, PARENT_TYPE)
    # counts only: the cell values of the file are never echoed
    assert len(errors) == 1 and "General_Public_Voice" not in errors[0]
    assert errors[0].startswith("2 row(s) carry a session_type other than Parent_Voice (1 other value(s))")


def test_check_rejects_a_whole_file_of_another_persona(config):
    df = make_df([{"session_type": "Youth_Voice"}])
    errors = check_feedback_file(df, config, PARENT_TYPE)
    assert len(errors) == 1 and "Youth_Voice" not in errors[0] and "Parent_Voice" in errors[0]
    assert check_feedback_file(df, mitra_config_v1("Youth"), "Youth_Voice") == []


def test_check_counts_every_foreign_session_type_without_naming_them(config):
    df = make_df([{"session_type": "Youth_Voice"}, {"session_type": "Educator_Voice", "sequence": "2"},
                  {"sequence": "3"}])
    (error,) = check_feedback_file(df, config, PARENT_TYPE)
    assert "2 row(s)" in error and "(2 other value(s))" in error
    assert "Educator_Voice" not in error and "Youth_Voice" not in error


def test_check_rejects_blank_session_type(config):
    df = make_df([{"session_type": ""}, {"session_type": PARENT_TYPE, "sequence": "2"}])
    errors = check_feedback_file(df, config, PARENT_TYPE)
    assert errors == ["session_type is blank in 1 row(s); every row must carry session_type Parent_Voice"]


def test_check_reports_blank_and_foreign_session_types_together(config):
    df = make_df([{"session_type": ""}, {"session_type": "Youth_Voice", "sequence": "2"},
                  {"session_type": "   ", "sequence": "3"}])
    errors = check_feedback_file(df, config, PARENT_TYPE)
    assert len(errors) == 2
    assert "1 row(s) carry a session_type other than" in errors[0] and "Youth_Voice" not in errors[0]
    assert "blank in 2 row(s)" in errors[1]


def test_check_ignores_whitespace_around_session_type(config):
    df = make_df([{"session_type": "  Parent_Voice "}])
    assert check_feedback_file(df, config, PARENT_TYPE) == []


def test_check_requires_the_session_type_column(config):
    df = make_df([{}]).drop(columns=["session_type"])
    errors = check_feedback_file(df, config, PARENT_TYPE)
    assert errors == ["Missing required column(s): session_type"]


def test_check_requires_the_session_type_column(config):
    df = make_df([{}]).drop(columns=["session_type"])
    assert check_feedback_file(df, config, PARENT_TYPE) == ["Missing required column(s): session_type"]


def test_check_session_type_column_name_is_case_insensitive(config):
    df = make_df([{}]).rename(columns={"session_type": "Session_Type"})
    assert check_feedback_file(df, config, PARENT_TYPE) == []
    wrong = make_df([{"session_type": "Youth_Voice"}]).rename(columns={"session_type": " SESSION_TYPE "})
    (error,) = check_feedback_file(wrong, config, PARENT_TYPE)
    assert "1 row(s) carry a session_type other than Parent_Voice" in error


def test_check_wants_the_bot_id_column_but_not_its_values(config):
    # CHANGED (the questionnaire version of a session is the rank of its bot id): the bot_id column is now required.
    # One persona still runs several bots over time, so any bot ids and blank cells on some rows are fine here (each
    # session is checked by build_feedback_events); a missing column, or a column blank in every row, is not.
    df = make_df([{"bot_id": "88"}, {"bot_id": "999", "sequence": "2"}, {"bot_id": "", "sequence": "3"}])
    assert check_feedback_file(df, config, PARENT_TYPE) == []
    assert check_feedback_file(df.drop(columns=["bot_id", "bot_name"]), config, PARENT_TYPE) == [
        "Missing required column(s): bot_id"]
    blank = make_df([{"bot_id": ""}, {"bot_id": "  ", "sequence": "2"}])
    assert check_feedback_file(blank, config, PARENT_TYPE) == [
        "bot_id is blank in every row; every session needs the bot id that asked its questions"]


def test_the_bot_id_header_follows_the_column_map(config):
    # the Mitra export names the column per config (for example company_bot_id): that header is the one required
    config["column_map"] = {**config.get("column_map", {}), "bot_id": "company_bot_id"}
    df = make_df([{}])
    assert check_feedback_file(df, config, PARENT_TYPE) == ["Missing required column(s): company_bot_id"]
    renamed = df.rename(columns={"bot_id": "Company_Bot_Id"})
    assert check_feedback_file(renamed, config, PARENT_TYPE) == []
    events, skipped = build(renamed, config)
    assert skipped == [] and events[0]["data"]["botId"] == "88"


def test_bot_id_is_an_optional_role_of_the_column_map():
    from app.services.feedback_adapter_config import COLUMN_ROLES_OPTIONAL, COLUMN_ROLES_REQUIRED
    assert "bot_id" not in COLUMN_ROLES_REQUIRED
    assert "bot_id" in COLUMN_ROLES_OPTIONAL


def test_check_reports_missing_required_columns_case_insensitively(config):
    df = make_df([{}])
    df = df.drop(columns=["session_status", "stage"]).rename(columns={"submission_id": "Submission_ID"})
    errors = check_feedback_file(df, config, PARENT_TYPE)
    assert len(errors) == 1
    assert "session_status" in errors[0] and "stage" in errors[0] and "submission_id" not in errors[0]


def test_check_allows_extra_columns(config):
    df = make_df([{}])
    df["audio_file"] = ""
    df["whatever"] = "x"
    assert check_feedback_file(df, config, PARENT_TYPE) == []


# ---------------------------------------------------------------------------------- events

def test_valid_fixture_builds_one_event_per_session(config):
    df = read_fixture("feedback_valid_mitra.csv")
    events, skipped = build(df, config)
    assert skipped == []
    assert [e["submissionId"] for e in events] == ["900001", "900002", "900003", "900004"]
    e = events[0]
    assert e["submissionType"] == "feedback" and e["eventType"] == "create"
    assert e["tenantCode"] == "mitra" and e["sessionId"] == "a" * 32
    assert e["eventPublishedAt"] == PUBLISHED
    assert e["tags"] == {"programId": PARENT["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                         "leaderCategoryId": PARENT["leader_category_id"], "leaderCategoryName": "Parent",
                         "state": None, "district": None, "source": "mitra"}
    assert e["data"]["source"] == "mitra" and e["data"]["language"] == "hi"
    assert e["data"]["sessionType"] == "Parent_Voice"
    assert e["data"]["questionnaireVersion"] == "V1"            # the fixture is dated before the V2 window
    assert e["data"]["botId"] == "88" and e["data"]["botName"] == "Synthetic Parent Bot"


def test_published_at_defaults_to_now(config):
    events, _ = build_feedback_events(make_df(session_rows([("R", "STATE_IDENTIFICATION", "a", "b")])), config,
                                      program_info=PROGRAM, leader_info=LEADER, tenant_code="mitra", source="mitra")
    stamp = events[0]["eventPublishedAt"]
    assert stamp.endswith("Z") and len(stamp) == len("2026-10-20T00:00:00.000Z")


def test_sequence_shift_and_synthesised_first_question(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] ans", "english ans"),
        ("B", "DISTRICT_IDENTIFICATION", "District q", "[hi] district q"),
        ("R", "DISTRICT_IDENTIFICATION", "[hi] d", "d"),
    ]))
    events, skipped = build(df, config)
    assert skipped == []
    msgs = events[0]["data"]["messages"]
    assert [m["sequence"] for m in msgs] == [1, 2, 3, 4]
    first = msgs[0]
    assert first["questionKey"] == "STATE_IDENTIFICATION"
    assert first["message"] == first["translatedMessage"] == config["questions"][0]["text"]
    assert first["isFollowUp"] is False and first["parentSequence"] is None
    # no earlier time is known: the synthesised question carries the first CSV row's time
    assert first["messageAt"] == msgs[1]["messageAt"] == events[0]["data"]["submissionDate"]
    assert msgs[1]["parentSequence"] == 1


def test_tail_only_session_is_skipped_with_clear_message(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "b"), ("B", "X", "q", "q")])
    tail = session_rows([("R", "STATE_IDENTIFICATION", "a", "b"), ("B", "X", "q", "q")],
                        session="t" * 32, submission_id="200")
    for r in tail:
        r["sequence"] = str(int(r["sequence"]) + 10)
    events, skipped = build(make_df(rows + tail), config)
    assert [e["submissionId"] for e in events] == ["100"]
    assert len(skipped) == 1 and skipped[0]["submissionId"] == "200"
    assert "starts at sequence 11" in skipped[0]["problems"][0] and "tail-only" in skipped[0]["problems"][0]


def test_first_stage_not_in_catalog_is_skipped(config):
    df = make_df(session_rows([("R", "SOMETHING_ELSE", "a", "b")]))
    events, skipped = build(df, config)
    assert events == [] and "questions catalog" in skipped[0]["problems"][0]
    assert "SOMETHING_ELSE" not in skipped[0]["problems"][0]   # the cell value is not echoed


def test_bot_bot_and_respondent_respondent_runs(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "a", "a-en"),
        ("B", "Q2", "q2a", "q2a-l"),
        ("B", "Q2", "q2b", "q2b-l"),      # two bot messages in a row
        ("R", "Q2", "r1", "r1-en"),
        ("R", "Q2", "r2", "r2-en"),       # two respondent messages in a row
        ("R", "Q2", "r3", "r3-en"),
        ("B", "THANK_YOU", "bye", "bye-l"),
    ]))
    events, skipped = build(df, config)
    assert skipped == []
    m = by_sequence(events[0])
    assert m[1]["parentSequence"] is None                       # synthesised question
    assert m[2]["parentSequence"] == 1                          # first answer -> synthesised question
    assert m[3]["parentSequence"] is None and m[4]["parentSequence"] is None   # both bot rows are questions
    assert m[5]["parentSequence"] == 4 and m[6]["parentSequence"] == 4 and m[7]["parentSequence"] == 4
    assert m[8]["parentSequence"] is None                       # closing row
    assert m[8]["questionKey"] == "THANK_YOU"


def test_parent_links_ignore_sequence_parity(config):
    # a respondent row sitting on an odd stored sequence still points at the preceding bot row
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "a", "a"),
        ("R", "STATE_IDENTIFICATION", "a2", "a2"),
        ("B", "Q2", "q", "q"),
        ("R", "Q2", "r", "r"),
    ]))
    events, _ = build(df, config)
    m = by_sequence(events[0])
    assert (m[2]["parentSequence"], m[3]["parentSequence"], m[4]["parentSequence"], m[5]["parentSequence"]) == (1, 1, None, 4)


def test_unsorted_rows_are_ordered_by_sequence(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r")])
    events, skipped = build(make_df(list(reversed(rows))), config)
    assert skipped == []
    assert [m["sequence"] for m in events[0]["data"]["messages"]] == [1, 2, 3, 4]
    assert events[0]["data"]["submissionDate"] == events[0]["data"]["messages"][1]["messageAt"]


def test_non_default_respondent_sender_is_a_respondent(config):
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r")])
                 )
    df.loc[df["sequence"] == "3", "sender_id"] = "1598"
    events, _ = build(df, config)
    assert by_sequence(events[0])[4]["parentSequence"] == 3


def test_follow_up_is_bot_row_repeating_previous_bot_stage(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "a", "a"),
        ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r"),
        ("B", "Q2", "probe", "probe"), ("R", "Q2", "r2", "r2"),
        ("B", "Q3", "q3", "q3"), ("R", "Q3", "r3", "r3"),
        ("B", "Q2", "again later", "again later"),      # not consecutive: not a follow-up
        ("B", "THANK_YOU", "bye", "bye"), ("B", "THANK_YOU", "bye2", "bye2"),   # closing never a follow-up
    ]))
    events, _ = build(df, config)
    flags = [(m["sequence"], m["isFollowUp"]) for m in events[0]["data"]["messages"]]
    assert [s for s, f in flags if f] == [5]      # CSV row 4 -> stored sequence 5


def test_follow_up_never_set_on_respondent_rows(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    for e in events:
        for m in e["data"]["messages"]:
            if m["parentSequence"] is not None:
                assert m["isFollowUp"] is False


def test_fixture_session_with_multi_message_answer_probe_and_closing(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    m = by_sequence(events[0])
    assert len(m) == 10                                   # 9 CSV rows + synthesised question
    assert m[4]["parentSequence"] == 3 and m[5]["parentSequence"] == 3   # district answer in two messages
    assert m[6]["questionKey"] == "SCHOOL_TYPE" and m[6]["isFollowUp"] is False
    assert m[8]["questionKey"] == "SCHOOL_TYPE" and m[8]["isFollowUp"] is True    # probe
    assert m[10]["questionKey"] == "THANK_YOU" and m[10]["parentSequence"] is None


def test_na_and_none_answers_survive_as_literal_strings(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    m = by_sequence(events[0])
    assert m[7]["message"] == "NA" and m[7]["translatedMessage"] == "NA"
    assert m[9]["message"] == "None" and m[9]["translatedMessage"] == "None"


# ---------------------------------------------------------------------------------- text invariant

def test_bot_rows_swap_columns_and_respondent_rows_copy(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    m = by_sequence(events[0])
    # bot row: CSV message = English, CSV translated_message = language shown
    assert m[3]["message"] == "[hi] district question" and m[3]["translatedMessage"] == "District question text"
    # respondent row: copied as is
    assert m[2]["message"] == "[hi] state answer" and m[2]["translatedMessage"] == "state answer one"


def test_english_session_holds_same_text_on_both_sides(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    en = [e for e in events if e["data"]["language"] == "en"][0]
    for msg in en["data"]["messages"]:
        assert msg["message"] == msg["translatedMessage"] and msg["message"] != ""
    m = by_sequence(en)
    assert m[3]["message"] == "English district question"     # bot row, translated column was empty
    assert m[2]["message"] == "english state answer"          # respondent row, translated column was empty


def test_empty_conversation_side_falls_back_to_english(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "", "english only"),     # respondent: message empty
        ("B", "Q2", "english q", ""),                          # bot: translated_message (= stored message) empty
    ]))
    events, skipped = build(df, config)
    assert skipped == []
    m = by_sequence(events[0])
    assert m[2]["message"] == "english only" and m[2]["translatedMessage"] == "english only"
    assert m[3]["message"] == "english q" and m[3]["translatedMessage"] == "english q"


def test_missing_english_side_stays_null_for_non_english_session(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] answer", ""),
        ("B", "Q2", "", "[hi] question"),                      # bot row with no English text
    ]))
    events, _ = build(df, config)
    m = by_sequence(events[0])
    assert m[2]["message"] == "[hi] answer" and m[2]["translatedMessage"] is None
    assert m[3]["message"] == "[hi] question" and m[3]["translatedMessage"] is None


def test_translated_message_column_may_be_absent(config):
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "english answer", ""),
                               ("B", "Q2", "english q", "")], language="en"))
    df = df.drop(columns=["translated_message"])
    events, skipped = build(df, config)
    assert skipped == []
    assert by_sequence(events[0])[2]["translatedMessage"] == "english answer"


# ---------------------------------------------------------------------------------- completion, dates

def test_completion_status_taken_from_session_status(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    assert [e["data"]["completionStatus"] for e in events] == ["COMPLETED", "IN_PROGRESS", "COMPLETED", "COMPLETED"]


def test_datetimes_are_utc_with_millis_and_z(config):
    df = make_df([{"created_at": "2026-10-15T08:54:34.169642+05:30"}])
    events, _ = build(df, config)
    assert events[0]["data"]["submissionDate"] == "2026-10-15T03:24:34.169Z"
    assert events[0]["data"]["messages"][1]["messageAt"] == "2026-10-15T03:24:34.169Z"


def test_datetime_without_offset_uses_fallback_timezone(config):
    df = make_df([{"created_at": "2026-10-15 08:54:34"}])
    events, _ = build(df, config)
    assert events[0]["data"]["submissionDate"] == "2026-10-15T03:24:34.000Z"      # Asia/Kolkata
    config["datetime"]["fallback_timezone"] = "UTC"
    events, _ = build(df, config)
    assert events[0]["data"]["submissionDate"] == "2026-10-15T08:54:34.000Z"


def test_unknown_fallback_timezone_is_a_config_error(config):
    config["datetime"]["fallback_timezone"] = "Not/AZone"
    with pytest.raises(ValueError):
        build(make_df([{}]), config)


def test_short_fraction_and_z_suffix_parse(config):
    df = make_df([{"created_at": "2026-10-15T08:54:34.17Z"}])
    events, skipped = build(df, config)
    assert skipped == [] and events[0]["data"]["submissionDate"] == "2026-10-15T08:54:34.170Z"


# ---------------------------------------------------------------------------------- bad sessions

def test_bad_sessions_skipped_good_ones_still_build_with_all_problems(config):
    df = read_fixture("feedback_bad_sessions.csv")
    assert check_feedback_file(df, config, PARENT_TYPE) == []
    events, skipped = build(df, config)
    assert [e["submissionId"] for e in events] == ["900201", "900203"]
    by_id = {s["submissionId"]: s for s in skipped}
    assert set(by_id) == {"900202", "900204", "900205"}

    bad = by_id["900202"]
    assert bad["sessionId"] == "2" * 32
    joined = " | ".join(bad["problems"])
    assert "duplicate sequence" in joined
    assert "session_status differs" in joined and "completion_map" in joined and "WAITING" not in joined
    assert "language differs" in joined
    assert len(bad["problems"]) >= 4                                  # all problems, not just the first

    joined = " | ".join(by_id["900204"]["problems"])
    assert "both empty" in joined and "created_at" in joined

    blank = by_id["900205"]
    assert blank["sessionId"] is None and blank["problems"] == ["session is blank in 1 row(s)"]


def test_blank_ids_are_reported_as_none(config):
    df = make_df([{"submission_id": ""}, {"session": "", "submission_id": "7"}])
    events, skipped = build(df, config)
    assert events == []
    assert {(s["submissionId"], s["sessionId"]) for s in skipped} == {(None, "s" * 32), ("7", None)}


def test_submission_mapped_to_two_sessions_and_vice_versa_are_skipped(config):
    rows = (session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="1", session="A" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="1", session="B" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="2", session="C" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="3", session="C" * 32)
            + session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], submission_id="4", session="D" * 32))
    events, skipped = build(make_df(rows), config)
    assert [e["submissionId"] for e in events] == ["4"]
    assert len(skipped) == 4
    assert all("maps to 2" in s["problems"][0] for s in skipped)


@pytest.mark.parametrize("sequence, expected", [
    ("", "sequence is missing"),
    ("abc", "not an integer"),
    ("1.5", "not an integer"),
    ("0", "1 or greater"),
    ("-2", "not an integer"),
])
def test_bad_sequence_values_skip_session(config, sequence, expected):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["sequence"] = sequence
    events, skipped = build(make_df(rows), config)
    assert events == [] and expected in " | ".join(skipped[0]["problems"])


def test_blank_sender_and_blank_language_skip_session(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["sender_id"] = ""
    rows[0]["language"] = ""
    rows[1]["language"] = ""
    events, skipped = build(make_df(rows), config)
    text = " | ".join(skipped[0]["problems"])
    assert events == [] and "sender_id is blank" in text and "language is blank" in text


def test_bot_id_differing_within_session_skips_it(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["bot_id"] = "89"
    events, skipped = build(make_df(rows), config)
    assert events == [] and "bot_id differs" in skipped[0]["problems"][0]


def test_blank_bot_id_on_some_rows_is_ignored(config):
    # the bot is informational: a blank cell is not a "different" bot, botId is the non-blank value
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[0]["bot_id"] = ""
    rows[1]["bot_id"] = "88"
    events, skipped = build(make_df(rows), config)
    assert skipped == [] and len(events) == 1
    assert events[0]["data"]["botId"] == "88"


def test_blank_bot_id_on_every_row_skips_the_session(config):
    # CHANGED: a session without any bot id can not be given a questionnaire version, so it is skipped (it used to
    # produce an event with botId None)
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    for row in rows:
        row["bot_id"] = ""
    events, skipped = build(make_df(rows), config)
    assert events == [] and "bot_id is blank in every row" in skipped[0]["problems"][0]


@pytest.mark.parametrize("value", ["abc", "0", "-5", "089", "8.5", "1e3", "12345678901234567890", "８９"])
def test_a_bot_id_that_is_not_a_positive_integer_in_digits_skips_the_session(config, value):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    for row in rows:
        row["bot_id"] = value
    events, skipped = build(make_df(rows), config)
    assert events == [] and "positive integer written in digits" in skipped[0]["problems"][0]
    assert value not in " ".join(skipped[0]["problems"])      # the cell value is never echoed


def test_the_bot_id_column_absent_raises(config):
    with pytest.raises(ValueError, match="missing required column.*bot_id"):
        build(make_df([{}]).drop(columns=["bot_id"]), config)


def test_missing_role_column_raises(config):
    with pytest.raises(ValueError):
        build(make_df([{}]).drop(columns=["stage"]), config)


# ---------------------------------------------------------------------------------- location tags

def location_config(config):
    config["location"] = {
        "state": {"stage": "STATE_IDENTIFICATION", "aliases": {"bihar": "Bihar", "in the state of bihar": "Bihar"}},
        "district": {"stage": "DISTRICT_IDENTIFICATION", "aliases": {"sitamarhi": "Sitamarhi"}},
    }
    return config


def test_location_tags_from_english_answers_via_aliases(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] x", "  In the   State of BIHAR "),
        ("B", "DISTRICT_IDENTIFICATION", "q", "q"),
        ("R", "DISTRICT_IDENTIFICATION", "[hi] y", "Sitamarhi"),
    ]))
    events, _ = build(df, location_config(config))
    assert events[0]["tags"]["state"] == "Bihar" and events[0]["tags"]["district"] == "Sitamarhi"


def test_location_none_when_unrecognised_or_unanswered(config):
    df = make_df(session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] x", "Atlantis"),
        ("B", "DISTRICT_IDENTIFICATION", "q", "q"),
    ]))
    events, _ = build(df, location_config(config))
    assert events[0]["tags"]["state"] is None and events[0]["tags"]["district"] is None


def test_location_falls_back_to_message_when_no_english_text(config):
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "bihar", "")], language="en"))
    events, _ = build(df, location_config(config))
    assert events[0]["tags"]["state"] == "Bihar"


def test_location_ignores_bot_question_rows_with_the_stage(config):
    # the synthesised question carries the stage key but is a question, not an answer
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "[hi] x", "Atlantis")]))
    cfg = location_config(config)
    cfg["location"]["state"]["aliases"][cfg["questions"][0]["text"].lower()] = "WRONG"
    events, _ = build(df, cfg)
    assert events[0]["tags"]["state"] is None


def test_location_absent_block_gives_none(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    assert all(e["tags"]["state"] is None and e["tags"]["district"] is None for e in events)


# ---------------------------------------------------------------------------------- serialisation

def test_every_event_round_trips_through_json(config):
    for name in ("feedback_valid_mitra.csv", "feedback_bad_sessions.csv"):
        events, skipped = build(read_fixture(name), location_config(mitra_config_v1("Parent")))
        for payload in events + skipped:
            assert json.loads(json.dumps(payload, allow_nan=False)) == payload


def test_event_values_are_plain_python_types(config):
    events, _ = build(read_fixture("feedback_valid_mitra.csv"), config)
    for e in events:
        for m in e["data"]["messages"]:
            assert type(m["sequence"]) is int and type(m["isFollowUp"]) is bool
            assert type(m["message"]) is str
            assert m["parentSequence"] is None or type(m["parentSequence"]) is int


def test_building_events_scales_linearly_with_the_number_of_sessions(config):
    """93,000 rows (3,000 sessions) are built; the time is judged RELATIVE to a small file, never against a clock."""
    import time
    items = [("R", "STATE_IDENTIFICATION", "a", "b")]
    for n in range(2, 32):
        items.append(("B", f"Q{n}", "q", "q") if n % 2 == 0 else ("R", f"Q{n - 1}", "r", "r"))

    def frame(sessions):
        rows = []
        for s in range(sessions):
            rows += session_rows(items, session=f"{s:032d}", submission_id=str(1000 + s))
        return make_df(rows)

    small, large = frame(375), frame(3000)                    # 8x the sessions: linear costs ~8x, quadratic ~64x
    t0 = time.perf_counter()
    build(small, config)
    t_small = time.perf_counter() - t0
    t0 = time.perf_counter()
    events, skipped = build(large, config)
    t_large = time.perf_counter() - t0
    assert len(events) == 3000 and skipped == []
    assert t_large < 30 * max(t_small, 0.01), (t_small, t_large)


# ================================================================================== new behaviour
# opening row, version stamping, session_type, config validation, Educator

V2_PARENT = v2_instant("Parent")             # test-owned window (see tests/feedback_test_windows.py)


def msgs(event):
    return event["data"]["messages"]


V1_STYLE = [
    ("O", "", "Welcome. Which state do you stay in?", "[hi] welcome which state"),
    ("R", "STATE_IDENTIFICATION", "[hi] state answer", "state answer"),
    ("B", "DISTRICT_IDENTIFICATION", "Which district?", "[hi] which district"),
    ("R", "DISTRICT_IDENTIFICATION", "[hi] district answer", "district answer"),
    ("B", "THANK_YOU", "Thanks", "[hi] thanks"),
]

V2_STYLE = [
    ("O", "", "Welcome. Shall we begin?", "[hi] welcome shall we begin"),
    ("R", "OPENING", "[hi] yes", "yes"),
    ("B", "STATE_IDENTIFICATION", "Which state?", "[hi] which state"),
    ("R", "STATE_IDENTIFICATION", "[hi] state answer", "state answer"),
    ("B", "THANK_YOU", "Thanks", "[hi] thanks"),
]


# ---------------------------------------------------------------------------------- opening row

def test_v1_style_opening_row_keeps_csv_sequences_and_synthesises_nothing(config):
    events, skipped = build(make_df(new_rows(V1_STYLE)), config)
    assert skipped == []
    messages = msgs(events[0])
    assert [m["sequence"] for m in messages] == [1, 2, 3, 4, 5]          # CSV sequences, no offset, 5 rows -> 5 messages
    assert [m["questionKey"] for m in messages] == [
        "STATE_IDENTIFICATION", "STATE_IDENTIFICATION", "DISTRICT_IDENTIFICATION", "DISTRICT_IDENTIFICATION",
        "THANK_YOU"]
    assert not any(m["message"] == config["questions"][0]["text"] for m in messages)


def test_v1_style_opening_message_is_the_first_bot_row(config):
    events, _ = build(make_df(new_rows(V1_STYLE)), config)
    opening = msgs(events[0])[0]
    assert opening["sequence"] == 1
    assert opening["messageAt"] is None and opening["parentSequence"] is None
    assert opening["isFollowUp"] is False
    assert opening["questionKey"] == "STATE_IDENTIFICATION"               # the stage of the next row
    # bot rows swap the CSV columns: stored message = conversation language, translated = English
    assert opening["message"] == "[hi] welcome which state"
    assert opening["translatedMessage"] == "Welcome. Which state do you stay in?"


def test_v1_style_answer_links_to_the_opening_message_and_dates_start_at_the_first_real_message(config):
    rows = new_rows(V1_STYLE)
    events, _ = build(make_df(rows), config)
    m = by_sequence(events[0])
    assert m[2]["parentSequence"] == 1 and m[2]["messageAt"] == "2026-10-15T02:30:02.000Z"
    assert m[3]["parentSequence"] is None                                   # bot question
    assert m[4]["parentSequence"] == 3
    assert m[5]["parentSequence"] is None and m[5]["questionKey"] == "THANK_YOU"
    assert events[0]["data"]["submissionDate"] == m[2]["messageAt"]        # first real message, not the opening row
    assert all(m[n]["messageAt"] is not None for n in (2, 3, 4, 5))


def test_v2_style_opening_row_then_opening_answer_then_first_question(config):
    events, skipped = build(make_df(new_rows(V2_STYLE)), config)
    assert skipped == []
    m = by_sequence(events[0])
    assert sorted(m) == [1, 2, 3, 4, 5]
    assert m[1]["questionKey"] == "OPENING" and m[1]["messageAt"] is None and m[1]["parentSequence"] is None
    assert m[1]["message"] == "[hi] welcome shall we begin" and m[1]["translatedMessage"] == "Welcome. Shall we begin?"
    assert m[2]["questionKey"] == "OPENING" and m[2]["parentSequence"] == 1
    assert m[2]["message"] == "[hi] yes" and m[2]["translatedMessage"] == "yes"
    assert m[3]["questionKey"] == "STATE_IDENTIFICATION" and m[3]["parentSequence"] is None
    assert m[3]["isFollowUp"] is False
    assert m[4]["parentSequence"] == 3
    assert events[0]["data"]["submissionDate"] == m[2]["messageAt"]
    # the real first question is the logged bot row: nothing synthesised in front of it
    assert not any(x["message"] == config["questions"][0]["text"] for x in m.values())


def test_v2_style_stored_sequences_equal_csv_sequences(config):
    rows = new_rows(V2_STYLE)
    events, _ = build(make_df(rows), config)
    assert [m["sequence"] for m in msgs(events[0])] == [int(r["sequence"]) for r in rows]


def test_stored_sequences_follow_the_csv_even_with_a_sequence_gap(config):
    rows = new_rows(V2_STYLE)
    for r in rows[2:]:
        r["sequence"] = str(int(r["sequence"]) + 10)
    events, skipped = build(make_df(rows), config)
    assert skipped == []
    assert [m["sequence"] for m in msgs(events[0])] == [1, 2, 13, 14, 15]
    assert by_sequence(events[0])[14]["parentSequence"] == 13


def test_bot_row_right_after_the_opening_row_is_not_a_follow_up_of_it(config):
    items = [
        ("O", "", "Welcome", "[hi] welcome"),
        ("B", "STATE_IDENTIFICATION", "Which state?", "[hi] which state"),         # directly after the opening row
        ("R", "STATE_IDENTIFICATION", "[hi] a", "a"),
        ("B", "STATE_IDENTIFICATION", "Again?", "[hi] again"),                       # a real probe
    ]
    events, skipped = build(make_df(new_rows(items)), config)
    assert skipped == []
    m = by_sequence(events[0])
    assert m[1]["isFollowUp"] is False and m[2]["isFollowUp"] is False
    assert m[2]["parentSequence"] is None and m[1]["questionKey"] == "STATE_IDENTIFICATION"
    assert m[4]["isFollowUp"] is True


def test_follow_up_flags_after_a_respondent_row_one_follow_the_usual_rule(config):
    items = [
        ("O", "", "Welcome", "[hi] welcome"),
        ("R", "STATE_IDENTIFICATION", "[hi] a", "a"),
        ("B", "STATE_IDENTIFICATION", "Probe", "[hi] probe"),     # repeats the opening's stage: a probe of the answer
        ("R", "STATE_IDENTIFICATION", "[hi] b", "b"),
    ]
    events, _ = build(make_df(new_rows(items)), config)
    assert [m["isFollowUp"] for m in msgs(events[0])] == [False, False, True, False]


def test_opening_row_text_columns_for_a_non_english_session_are_swapped(config):
    events, _ = build(make_df(new_rows(V2_STYLE)), config)
    opening = msgs(events[0])[0]
    assert opening["message"].startswith("[hi]") and not opening["translatedMessage"].startswith("[hi]")


def test_english_opening_row_with_empty_translated_column_holds_the_text_on_both_sides(config):
    items = [("O", "", "Welcome, how are you", ""), ("R", "OPENING", "fine", ""), ("B", "STATE_IDENTIFICATION", "q", "")]
    events, skipped = build(make_df(new_rows(items, language="en")), config)
    assert skipped == []
    opening = msgs(events[0])[0]
    assert opening["message"] == opening["translatedMessage"] == "Welcome, how are you"


def test_opening_row_with_no_text_at_all_is_a_text_problem(config):
    items = [("O", "", "", ""), ("R", "OPENING", "a", "a")]
    events, skipped = build(make_df(new_rows(items)), config)
    assert events == [] and "both empty" in " | ".join(skipped[0]["problems"])


def test_old_shape_sessions_still_work_next_to_new_shape_ones(config):
    old = session_rows([
        ("R", "STATE_IDENTIFICATION", "[hi] a", "a"),
        ("B", "DISTRICT_IDENTIFICATION", "q", "[hi] q"),
        ("R", "DISTRICT_IDENTIFICATION", "[hi] d", "d"),
    ], session="o" * 32, submission_id="1")
    new = new_rows(V1_STYLE, session="n" * 32, submission_id="2")
    events, skipped = build(make_df(old + new), config)
    assert skipped == [] and [e["submissionId"] for e in events] == ["1", "2"]
    old_event, new_event = events
    assert [m["sequence"] for m in msgs(old_event)] == [1, 2, 3, 4]                 # synthesised question + 3 rows
    assert msgs(old_event)[0]["message"] == config["questions"][0]["text"]
    assert msgs(old_event)[0]["messageAt"] == old_event["data"]["submissionDate"]
    assert [m["sequence"] for m in msgs(new_event)] == [1, 2, 3, 4, 5]
    assert msgs(new_event)[0]["messageAt"] is None


def test_mixed_fixture_file(config):
    df = read_fixture("feedback_opening_mixed.csv")
    assert check_feedback_file(df, config, PARENT_TYPE) == []
    events, skipped = build(df, config)
    assert [e["submissionId"] for e in events] == ["910031", "910032", "910033"]
    assert [(s["submissionId"], s["problems"]) for s in skipped] == [
        ("910034", ["the session has only the opening row (no real message)"])]
    old, v1, v2 = events
    assert len(msgs(old)) == 5 and msgs(old)[0]["questionKey"] == "STATE_IDENTIFICATION"   # 4 CSV rows + synthesised
    assert len(msgs(v1)) == 5 and msgs(v1)[0]["messageAt"] is None
    assert len(msgs(v2)) == 7 and msgs(v2)[0]["questionKey"] == "OPENING"
    assert [e["data"]["questionnaireVersion"] for e in events] == ["V1", "V1", "V2"]
    assert [e["data"]["botId"] for e in events] == ["88", "88", "99"]
    assert v2["data"]["messages"][4]["isFollowUp"] is True                                 # repeated stage = probe


def test_opening_v1_fixture(config):
    df = read_fixture("feedback_opening_v1.csv")
    assert check_feedback_file(df, config, PARENT_TYPE) == []
    events, skipped = build(df, config)
    assert skipped == [] and len(events) == 2
    hi, en = events
    assert [m["sequence"] for m in msgs(hi)] == [1, 2, 3, 4, 5]
    assert msgs(hi)[0]["message"] == "[hi] hello welcome which state"
    assert msgs(hi)[0]["translatedMessage"] == "Hello, welcome. Which state do you stay in?"
    assert en["data"]["completionStatus"] == "IN_PROGRESS" and len(msgs(en)) == 4
    for m in msgs(en):
        assert m["message"] == m["translatedMessage"] and m["message"] != ""
    assert hi["data"]["questionnaireVersion"] == en["data"]["questionnaireVersion"] == "V1"


def test_opening_v2_fixture(config):
    events, skipped = build(read_fixture("feedback_opening_v2.csv"), config)
    assert skipped == [] and len(events) == 2
    hi, en = events
    assert [m["questionKey"] for m in msgs(hi)][:3] == ["OPENING", "OPENING", "STATE_IDENTIFICATION"]
    assert [m["isFollowUp"] for m in msgs(hi)] == [False, False, False, False, True, False, False]
    assert hi["data"]["questionnaireVersion"] == en["data"]["questionnaireVersion"] == "V2"
    assert hi["data"]["botId"] == "99" and hi["data"]["botName"] == "Synthetic Parent Bot V2"


# ---------------------------------------------------------------------------------- skipped / problem cases

def test_session_with_only_the_opening_row_is_skipped_with_the_documented_message(config):
    only = new_rows([("O", "", "Welcome", "[hi] welcome")], session="x" * 32, submission_id="9")
    good = new_rows(V1_STYLE)
    events, skipped = build(make_df(good + only), config)
    assert [e["submissionId"] for e in events] == ["100"]
    assert skipped == [{"submissionId": "9", "sessionId": "x" * 32,
                        "problems": ["the session has only the opening row (no real message)"]}]


def test_mode_none_treats_the_blank_first_row_as_the_usual_problems(config):
    config["opening_row"] = {"mode": "none"}
    events, skipped = build(make_df(new_rows(V1_STYLE)), config)
    assert events == []
    text = " | ".join(skipped[0]["problems"])
    assert "sender_id is blank in 1 row(s)" in text
    assert "created_at is missing or unparsable in 1 row(s)" in text


def test_a_config_without_opening_row_behaves_like_mode_none(config):
    del config["opening_row"]
    events, skipped = build(make_df(new_rows(V1_STYLE)), config)
    assert events == [] and "sender_id is blank" in " | ".join(skipped[0]["problems"])


def test_mode_none_still_builds_old_shape_files(config):
    config["opening_row"] = {"mode": "none"}
    events, skipped = build(read_fixture("feedback_valid_mitra.csv"), config)
    assert skipped == [] and len(events) == 4


def test_blank_sender_on_a_non_opening_row_is_still_a_problem(config):
    rows = new_rows(V1_STYLE)
    rows[2]["sender_id"] = ""
    events, skipped = build(make_df(rows), config)
    assert events == [] and "sender_id is blank in 1 row(s)" in skipped[0]["problems"]


def test_blank_created_at_on_a_non_opening_row_is_still_a_problem(config):
    rows = new_rows(V1_STYLE)
    rows[3]["created_at"] = ""
    events, skipped = build(make_df(rows), config)
    assert events == []
    assert any("created_at is missing or unparsable in 1 row(s)" in p for p in skipped[0]["problems"])


def test_unparsable_created_at_on_a_non_opening_row_is_a_problem(config):
    rows = new_rows(V2_STYLE)
    rows[1]["created_at"] = "not a date"
    events, skipped = build(make_df(rows), config)
    problem = " | ".join(skipped[0]["problems"])
    assert events == [] and "created_at is missing or unparsable in 1 row(s) (data row(s) 2)" in problem
    assert "not a date" not in problem


def test_blank_row_with_a_stage_is_not_an_opening_row(config):
    rows = new_rows(V1_STYLE)
    rows[0]["stage"] = "STATE_IDENTIFICATION"            # the opening row must have blank sender, stage AND time
    events, skipped = build(make_df(rows), config)
    assert events == []
    text = " | ".join(skipped[0]["problems"])
    assert "sender_id is blank" in text and "created_at is missing" in text


def test_blank_row_with_a_time_is_not_an_opening_row(config):
    rows = new_rows(V1_STYLE)
    rows[0]["created_at"] = "2026-10-15T08:00:00.000000+05:30"
    events, skipped = build(make_df(rows), config)
    assert events == [] and "sender_id is blank in 1 row(s)" in skipped[0]["problems"]


def test_blank_row_that_is_not_at_sequence_one_is_not_an_opening_row(config):
    rows = new_rows(V1_STYLE)
    for r in rows:
        r["sequence"] = str(int(r["sequence"]) + 1)      # CSV starts at 2 (tail-only) with a blank first row
    events, skipped = build(make_df(rows), config)
    assert events == [] and "sender_id is blank" in " | ".join(skipped[0]["problems"])


def test_two_blank_rows_only_the_first_is_the_opening_row(config):
    rows = new_rows(V1_STYLE)
    rows[2].update({"sender_id": "", "stage": "", "created_at": ""})
    events, skipped = build(make_df(rows), config)
    assert events == [] and "sender_id is blank in 1 row(s)" in skipped[0]["problems"]


# ---------------------------------------------------------------------------------- version stamping

def persona_boundary(persona):
    return v2_instant(persona)


def one_session_df(persona, stamp, shape, cfg=None):
    cfg = cfg if cfg is not None else config_with_v2(persona)
    kwargs = dict(session_type=PERSONAS[persona]["session_type"], bot_id=PERSONAS[persona]["bot_id"])
    if shape == "old":
        rows = session_rows([("R", PERSONAS[persona]["first_question_key"], "a", "a"), ("B", "Q2", "q", "q")], **kwargs)
        rows[0]["created_at"] = stamp.isoformat()
        rows[1]["created_at"] = (stamp + timedelta(seconds=5)).isoformat()
    else:
        rows = new_rows([("O", "", "hello", "hello"), ("R", "OPENING", "a", "a"), ("B", "Q2", "q", "q")], **kwargs)
        rows[1]["created_at"] = stamp.isoformat()
        rows[2]["created_at"] = (stamp + timedelta(seconds=5)).isoformat()
    return cfg, make_df(rows)


@pytest.mark.parametrize("shape", ["old", "new"])
@pytest.mark.parametrize("persona, offset, expected", [
    ("Youth", -1, "V1"), ("Youth", 0, "V2"), ("Youth", 1, "V2"),
    ("General Public", -1, "V1"), ("General Public", 0, "V2"), ("General Public", 1, "V2"),
    ("Parent", -1, "V1"), ("Parent", 0, "V2"), ("Parent", 1, "V2"),
])
def test_persona_windows_stamp_the_version(persona, offset, expected, shape):
    cfg, df = one_session_df(persona, persona_boundary(persona) + timedelta(seconds=offset), shape)
    events, skipped = build(df, cfg)
    assert skipped == []
    assert events[0]["data"]["questionnaireVersion"] == expected
    assert events[0]["data"]["sessionType"] == PERSONAS[persona]["session_type"]


def test_seeded_windows_carry_no_dates_and_test_windows_are_injected_data():
    # code and seeds only know the baseline; the V2 instants used above are owned by the tests
    for persona in PERSONAS:
        assert mitra_config_v1(persona)["questionnaire_versions"] == [{"key": "V1", "effective_from": None}]
    assert config_with_v2("Youth")["questionnaire_versions"] == [
        {"key": "V1", "effective_from": None}, {"key": "V2", "effective_from": TEST_V2_START["Youth"]}]
    assert mitra_config_v1("Youth")["questionnaire_versions"] == [{"key": "V1", "effective_from": None}]


@pytest.mark.parametrize("persona", ["Youth", "General Public", "Parent"])
def test_long_before_and_long_after_the_window(persona):
    for days, expected in ((-90, "V1"), (+90, "V2")):
        cfg, df = one_session_df(persona, persona_boundary(persona) + timedelta(days=days), "new")
        assert build(df, cfg)[0][0]["data"]["questionnaireVersion"] == expected


def three_version_config():
    return config_with_windows("Parent", ("V2", "2026-10-10T00:00:00+00:00"), ("V3", "2026-10-20T00:00:00Z"))


@pytest.mark.parametrize("stamp, expected", [
    ("2026-10-09T23:59:59+00:00", "V1"),
    ("2026-10-10T00:00:00+00:00", "V2"),
    ("2026-10-10T00:00:01+00:00", "V2"),
    ("2026-10-19T23:59:59+00:00", "V2"),
    ("2026-10-20T00:00:00+00:00", "V3"),
    ("2026-10-20T00:00:01+00:00", "V3"),
    ("2026-10-20T05:29:59+05:30", "V2"),          # one second before the V3 instant, written in another offset
    ("2026-10-20T05:30:00+05:30", "V3"),          # exactly the V3 instant (2026-10-20T00:00:00Z)
])
def test_custom_three_version_config_boundaries(stamp, expected):
    cfg = three_version_config()
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")])
    rows[0]["created_at"] = stamp
    events, _ = build(make_df(rows), cfg)
    assert events[0]["data"]["questionnaireVersion"] == expected


def test_a_session_takes_its_version_from_the_first_real_message_not_the_last(config):
    boundary = V2_PARENT
    rows = new_rows(V2_STYLE)
    # first real message one second before the V2 window, the rest of the session after it
    rows[1]["created_at"] = at_instant(boundary, -1)
    for n, r in enumerate(rows[2:], 1):
        r["created_at"] = at_instant(boundary, n * 60)
    events, _ = build(make_df(rows), config)
    assert events[0]["data"]["questionnaireVersion"] == "V1"
    assert events[0]["data"]["submissionDate"].endswith("Z")
    # ... and the other way round: first message inside V2 stays V2 however old the later rows look
    rows[1]["created_at"] = at_instant(boundary, 0)
    for n, r in enumerate(rows[2:], 1):
        r["created_at"] = at_instant(boundary, -n * 60)
    events, _ = build(make_df(rows), config)
    assert events[0]["data"]["questionnaireVersion"] == "V2"


def test_old_shape_session_takes_its_version_from_the_first_csv_row(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r")])
    rows[0]["created_at"] = at_instant(V2_PARENT, -1)
    rows[1]["created_at"] = at_instant(V2_PARENT, 10)
    rows[2]["created_at"] = at_instant(V2_PARENT, 20)
    assert build(make_df(rows), config)[0][0]["data"]["questionnaireVersion"] == "V1"


def test_version_uses_the_lowest_sequence_row_even_when_rows_are_unsorted(config):
    rows = new_rows(V2_STYLE)
    rows[1]["created_at"] = at_instant(V2_PARENT, -1)
    for n, r in enumerate(rows[2:], 1):
        r["created_at"] = at_instant(V2_PARENT, n)
    events, _ = build(make_df(list(reversed(rows))), config)
    assert events[0]["data"]["questionnaireVersion"] == "V1"


def test_naive_created_at_is_stamped_in_the_fallback_timezone(config):
    # 2026-11-03 18:52:54 read as Asia/Kolkata is exactly the Parent V2 instant
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")])
    rows[0]["created_at"] = "2026-11-03 18:52:53"
    assert build(make_df(rows), config)[0][0]["data"]["questionnaireVersion"] == "V1"
    rows[0]["created_at"] = "2026-11-03 18:52:54"
    assert build(make_df(rows), config)[0][0]["data"]["questionnaireVersion"] == "V2"


def test_bot_id_is_irrelevant_for_the_version_v1_and_v2_bots_in_one_file(config):
    before = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], session="a" * 32, submission_id="1",
                          bot_id="88")
    after = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], session="b" * 32, submission_id="2",
                         bot_id="777")
    before[0]["created_at"] = at_instant(V2_PARENT, -3600)
    after[0]["created_at"] = at_instant(V2_PARENT, 3600)
    swapped = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], session="c" * 32, submission_id="3",
                           bot_id="777")
    swapped[0]["created_at"] = at_instant(V2_PARENT, -3600)           # the "V2 bot id" on a V1 date stays V1
    df = make_df(before + after + swapped)
    assert check_feedback_file(df, config, PARENT_TYPE) == []
    events, skipped = build(df, config)
    assert skipped == []
    assert [(e["data"]["botId"], e["data"]["questionnaireVersion"]) for e in events] == [
        ("88", "V1"), ("777", "V2"), ("777", "V1")]


@pytest.mark.parametrize("shape", ["old", "new"])
@pytest.mark.parametrize("persona", list(PERSONAS))
def test_v3_needs_no_code_a_third_window_stamps_around_both_boundaries(persona, shape):
    cfg = config_with_v2_v3(persona)
    v2, v3 = v2_instant(persona), v3_instant(persona)
    assert v2 < v3
    cases = [(v2 - timedelta(seconds=1), "V1"), (v2, "V2"), (v2 + timedelta(seconds=1), "V2"),
             (v3 - timedelta(seconds=1), "V2"), (v3, "V3"), (v3 + timedelta(seconds=1), "V3"),
             (v3 + timedelta(days=400), "V3")]
    for stamp, expected in cases:
        _, df = one_session_df(persona, stamp, shape, cfg=cfg)
        events, skipped = build(df, cfg)
        assert skipped == []
        assert events[0]["data"]["questionnaireVersion"] == expected, stamp


def test_v3_boundary_in_another_utc_offset_and_first_message_decides():
    cfg = config_with_v2_v3("Parent")
    v3 = v3_instant("Parent")
    rows = new_rows(V2_STYLE)
    # first real message one second before V3 written in UTC; the rest of the session is after V3
    rows[1]["created_at"] = (v3 - timedelta(seconds=1)).astimezone(timezone.utc).isoformat()
    for n, r in enumerate(rows[2:], 1):
        r["created_at"] = at_instant(v3, n * 60)
    assert build(make_df(rows), cfg)[0][0]["data"]["questionnaireVersion"] == "V2"
    rows[1]["created_at"] = at_instant(v3, 0)
    assert build(make_df(rows), cfg)[0][0]["data"]["questionnaireVersion"] == "V3"


def test_the_seeded_baseline_stamps_everything_v1_whatever_the_date():
    # Without a recorded later version every session is V1, a session in 2030 included. Later versions are
    # recorded in the database with tools/questionnaire_versions.py (set --session-type ... --key V2 ...), not in code.
    for persona in PERSONAS:
        cfg = mitra_config_v1(persona)
        for stamp in ("2020-01-01T00:00:00+05:30", "2026-11-03T18:23:03+05:30", "2026-11-08T12:00:00+05:30",
                      "2030-01-01T00:00:00+05:30"):
            for shape in ("old", "new"):
                _, df = one_session_df(persona, datetime.fromisoformat(stamp), shape, cfg=cfg)
                events, skipped = build(df, cfg)
                assert skipped == []
                assert events[0]["data"]["questionnaireVersion"] == "V1"


def test_educator_has_a_single_version_whatever_the_date():
    cfg = mitra_config_v1("Educator")
    assert [v["key"] for v in cfg["questionnaire_versions"]] == ["V1"]
    for stamp in ("2026-11-06T00:00:00+05:30", "2026-11-08T12:00:00+05:30", "2030-01-01T00:00:00+05:30"):
        _, df = one_session_df("Educator", datetime.fromisoformat(stamp), "new")
        events, skipped = build(df, cfg)
        assert skipped == [] and events[0]["data"]["questionnaireVersion"] == "V1"


def test_versions_in_the_config_parse(config):
    assert [k for k, _ in parse_versions(config)] == ["V1", "V2"]
    assert [k for k, _ in parse_versions(mitra_config_v1("Parent"))] == ["V1"]


# ---------------------------------------------------------------------------------- session_type at session level

def test_session_of_another_session_type_is_skipped_others_still_build(config):
    good = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], session="g" * 32, submission_id="1")
    foreign = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], session="f" * 32, submission_id="2",
                           session_type="Youth_Voice")
    events, skipped = build(make_df(good + foreign), config)
    assert [e["submissionId"] for e in events] == ["1"]
    assert skipped == [{"submissionId": "2", "sessionId": "f" * 32,
                        "problems": ["session_type does not match the config's 'Parent_Voice'"]}]


def test_blank_session_type_in_a_session_skips_it(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")], session_type="")
    events, skipped = build(make_df(rows), config)
    assert events == [] and skipped[0]["problems"] == ["session_type is blank in 2 row(s)"]


def test_session_type_blank_on_one_row_skips_the_session(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["session_type"] = ""
    events, skipped = build(make_df(rows), config)
    assert events == [] and "session_type is blank in 1 row(s)" in skipped[0]["problems"]


def test_session_type_differing_within_a_session_skips_it(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])
    rows[1]["session_type"] = "Youth_Voice"
    events, skipped = build(make_df(rows), config)
    assert events == []
    assert any("session_type differs within the session (2 distinct values)" in p and "Youth_Voice" not in p
               for p in skipped[0]["problems"])


def test_session_type_is_compared_with_the_config_not_the_file_majority(config):
    config["session_type"] = "Youth_Voice"
    events, skipped = build(make_df(session_rows([("R", "STATE_IDENTIFICATION", "a", "a")])), config)
    assert events == [] and "does not match the config's 'Youth_Voice'" in skipped[0]["problems"][0]


def test_session_type_column_is_a_required_role_column(config):
    with pytest.raises(ValueError, match="session_type"):
        build(make_df([{}]).drop(columns=["session_type"]), config)


def test_event_session_type_is_the_config_value(config):
    events, _ = build(make_df([{"session_type": " Parent_Voice "}]), config)
    assert events[0]["data"]["sessionType"] == "Parent_Voice"


# ---------------------------------------------------------------------------------- bot id (required) / bot name (informational)

def test_bot_name_column_absent_is_fine_and_gives_none(config):
    # CHANGED: the bot_id column is required now (see test_check_wants_the_bot_id_column_but_not_its_values); only the
    # bot_name column stays optional
    df = make_df(session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q")])).drop(
        columns=["bot_name"])
    assert check_feedback_file(df, config, PARENT_TYPE) == []
    events, skipped = build(df, config)
    assert skipped == []
    assert events[0]["data"]["botId"] == "88" and events[0]["data"]["botName"] is None


def test_bot_id_and_name_are_filled_from_the_session(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], bot_id="4242", bot_name="Some Bot")
    events, _ = build(make_df(rows), config)
    assert events[0]["data"]["botId"] == "4242" and events[0]["data"]["botName"] == "Some Bot"


def test_bot_id_filled_but_bot_name_column_blank_gives_none_name(config):
    events, _ = build(make_df(session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], bot_name="")), config)
    assert events[0]["data"]["botId"] == "88" and events[0]["data"]["botName"] is None


def test_bot_name_is_the_first_non_blank_value_in_the_session(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "Q2", "q", "q"), ("R", "Q2", "r", "r")])
    rows[0]["bot_name"] = ""
    rows[1]["bot_name"] = "Second"
    rows[2]["bot_name"] = "Third"
    events, _ = build(make_df(rows), config)
    assert events[0]["data"]["botName"] == "Second"


def test_each_session_records_its_own_bot(config):
    a = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], session="a" * 32, submission_id="1",
                     bot_id="88", bot_name="One")
    b = session_rows([("R", "STATE_IDENTIFICATION", "a", "a")], session="b" * 32, submission_id="2",
                     bot_id="99", bot_name="Two")
    events, _ = build(make_df(a + b), config)
    assert [(e["data"]["botId"], e["data"]["botName"]) for e in events] == [("88", "One"), ("99", "Two")]


def test_new_shape_session_records_bot_from_the_opening_row_too(config):
    events, _ = build(make_df(new_rows(V1_STYLE, bot_id="55", bot_name="Opening Bot")), config)
    assert events[0]["data"]["botId"] == "55" and events[0]["data"]["botName"] == "Opening Bot"


def test_bot_id_differing_within_a_new_shape_session_still_skips_it(config):
    rows = new_rows(V2_STYLE)
    rows[3]["bot_id"] = "89"
    events, skipped = build(make_df(rows), config)
    assert events == [] and any("bot_id differs" in p for p in skipped[0]["problems"])


# ---------------------------------------------------------------------------------- config validation

def test_config_without_session_type_is_rejected(config):
    del config["session_type"]
    with pytest.raises(ValueError, match="session_type"):
        build(make_df([{}]), config)


@pytest.mark.parametrize("value", ["", "   ", None, 5])
def test_config_with_a_bad_session_type_is_rejected(config, value):
    config["session_type"] = value
    with pytest.raises(ValueError, match="session_type"):
        build(make_df([{}]), config)


def test_config_without_questionnaire_versions_is_rejected(config):
    del config["questionnaire_versions"]
    with pytest.raises(ValueError, match="questionnaire_versions"):
        build(make_df([{}]), config)


@pytest.mark.parametrize("versions", [
    [],
    None,
    [{"key": "V1", "effective_from": "2026-01-01T00:00:00+05:30"}],
    [{"key": "V1", "effective_from": None}, {"key": "V1", "effective_from": "2026-11-03T00:00:00+05:30"}],
    [{"key": "V1", "effective_from": None}, {"key": "V2", "effective_from": "2026-11-03T00:00:00"}],
    [{"key": "V1", "effective_from": None}, {"key": "V2", "effective_from": "2026-11-03T00:00:00+05:30"},
     {"key": "V3", "effective_from": "2026-11-02T00:00:00+05:30"}],
])
def test_config_with_invalid_questionnaire_versions_is_rejected(config, versions):
    config["questionnaire_versions"] = versions
    with pytest.raises(ValueError, match="questionnaire_versions"):
        build(make_df([{}]), config)


@pytest.mark.parametrize("mode", ["sometimes", "", None, "AUTO", True])
def test_bad_opening_row_mode_is_rejected(config, mode):
    config["opening_row"] = {"mode": mode}
    with pytest.raises(ValueError, match="opening_row.mode"):
        build(make_df([{}]), config)


@pytest.mark.parametrize("mode", ["auto", "none"])
def test_valid_opening_row_modes_are_accepted(config, mode):
    config["opening_row"] = {"mode": mode}
    events, _ = build(make_df([{}]), config)
    assert len(events) == 1


def test_config_validation_runs_before_the_csv_is_read(config):
    del config["session_type"]
    with pytest.raises(ValueError, match="session_type"):
        build(make_df([{}]).drop(columns=["stage"]), config)      # the config problem is reported first


def test_the_real_configs_of_all_personas_validate():
    for persona in PERSONAS:
        cfg = mitra_config_v1(persona)
        assert cfg["session_type"] == PERSONAS[persona]["session_type"]
        assert cfg["opening_row"] == {"mode": "auto"}
        df = make_df(session_rows([("R", PERSONAS[persona]["first_question_key"], "a", "a")],
                                  session_type=cfg["session_type"]))
        events, skipped = build(df, cfg, program_info={"id": "p", "name": FEEDBACK_PROGRAM_NAME},
                                leader_info={"id": "l", "name": persona})
        assert skipped == [] and len(events) == 1


# ---------------------------------------------------------------------------------- Educator

EDUCATOR = PERSONAS["Educator"]
EDUCATOR_ITEMS = [
    ("O", "", "Welcome teachers", ""),
    ("R", "OPENING", "yes", ""),
    ("B", "STATE_IDENTIFICATION", "Which state do you teach in?", ""),
    ("R", "STATE_IDENTIFICATION", "a state", ""),
    ("B", "DISTRICT_IDENTIFICATION", "Which district?", ""),
    ("R", "DISTRICT_IDENTIFICATION", "a district", ""),
    ("B", "GENDER_IDENTIFICATION", "Gender?", ""),
    ("R", "GENDER_IDENTIFICATION", "an answer", ""),
    ("B", "INSTITUTION_TYPE", "Which type of institution?", ""),
    ("R", "INSTITUTION_TYPE", "an institution", ""),
    ("B", "TEACHING_LEVEL", "Which level do you teach?", ""),
    ("R", "TEACHING_LEVEL", "a level", ""),
]


def build_educator(df, cfg=None):
    return build_feedback_events(
        df, cfg if cfg is not None else mitra_config_v1("Educator"),
        program_info={"id": EDUCATOR["program_id"], "name": FEEDBACK_PROGRAM_NAME},
        leader_info={"id": EDUCATOR["leader_category_id"], "name": "Educator"},
        tenant_code="mitra", source="mitra", published_at=PUBLISHED)


def test_synthetic_new_shape_educator_file_builds_educator_events():
    rows = new_rows(EDUCATOR_ITEMS, language="en", session_type="Educator_Voice", bot_id="110",
                    bot_name="Synthetic Educator Bot")
    for n, r in enumerate(rows):
        if r["sender_id"] != "":
            r["created_at"] = f"2026-11-08T10:00:{n:02d}.000000+05:30"
    df = make_df(rows)
    cfg = mitra_config_v1("Educator")
    assert check_feedback_file(df, cfg, "Educator_Voice") == []
    events, skipped = build_educator(df)
    assert skipped == [] and len(events) == 1
    data = events[0]["data"]
    assert data["sessionType"] == "Educator_Voice" and data["questionnaireVersion"] == "V1"
    assert data["botId"] == "110" and data["botName"] == "Synthetic Educator Bot"
    assert events[0]["tags"]["programId"] == EDUCATOR["program_id"]
    assert [m["sequence"] for m in data["messages"]] == list(range(1, 13))
    assert [m["questionKey"] for m in data["messages"]][::2] == [
        "OPENING", "STATE_IDENTIFICATION", "DISTRICT_IDENTIFICATION", "GENDER_IDENTIFICATION", "INSTITUTION_TYPE",
        "TEACHING_LEVEL"]
    assert data["messages"][0]["messageAt"] is None and data["messages"][0]["parentSequence"] is None
    assert data["submissionDate"] == "2026-11-08T04:30:01.000Z"


def test_educator_fixture_file():
    df = read_fixture("feedback_educator.csv")
    assert check_feedback_file(df, mitra_config_v1("Educator"), "Educator_Voice") == []
    events, skipped = build_educator(df)
    assert skipped == [] and len(events) == 2
    assert {e["data"]["sessionType"] for e in events} == {"Educator_Voice"}
    assert {e["data"]["questionnaireVersion"] for e in events} == {"V1"}
    assert [len(msgs(e)) for e in events] == [13, 6]
    assert events[1]["data"]["completionStatus"] == "IN_PROGRESS"


def test_an_educator_file_is_rejected_for_a_parent_upload_and_vice_versa(config):
    df = read_fixture("feedback_educator.csv")
    (error,) = check_feedback_file(df, config, PARENT_TYPE)
    assert "Educator_Voice" not in error and "Parent_Voice" in error and "other than" in error
    parent_df = read_fixture("feedback_valid_mitra.csv")
    (error,) = check_feedback_file(parent_df, mitra_config_v1("Educator"), "Educator_Voice")
    assert "Parent_Voice" not in error and "Educator_Voice" in error


def test_educator_sessions_in_a_parent_config_are_skipped_per_session(config):
    df = read_fixture("feedback_educator.csv")
    events, skipped = build(df, config)
    assert events == [] and len(skipped) == 2
    assert all("does not match the config's 'Parent_Voice'" in s["problems"][0] for s in skipped)


# ---------------------------------------------------------------------------------- per-bot settings in the config

@pytest.mark.parametrize("key", ["max_csv_upload_bytes", "ingestion_schema", "process_steps"])
def test_builder_rejects_a_config_missing_a_per_bot_setting(config, key):
    del config[key]
    df = make_df(session_rows([("B", "STATE_IDENTIFICATION", "q", "q"), ("R", "STATE_IDENTIFICATION", "a", "a")]))
    with pytest.raises(ValueError, match=f"missing '{key}'"):
        build(df, config)


@pytest.mark.parametrize("key,value", [("max_csv_upload_bytes", "100"), ("process_steps", {"name": "x"}),
                                       ("ingestion_schema", []), ("expect_stages", "AGE")])
def test_builder_rejects_a_wrongly_typed_per_bot_setting(config, key, value):
    config[key] = value
    df = make_df(session_rows([("B", "STATE_IDENTIFICATION", "q", "q"), ("R", "STATE_IDENTIFICATION", "a", "a")]))
    with pytest.raises(ValueError, match=key):
        build(df, config)


# ---------------------------------------------------------------------------------- expect_stages (report only)

def _stage_df():
    s1 = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q"),
                   ("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "THANK_YOU", "bye", "bye")],
                  submission_id="100", session="a" * 32)
    s2 = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q"),
                   ("R", "STATE_IDENTIFICATION", "a", "a"), ("B", "SCHOOL_TYPE", "q2", "q2"),
                   ("R", "SCHOOL_TYPE", "a2", "a2"), ("B", "SCHOOL_TYPE", "probe", "probe"),
                   ("B", "MYSTERY", "q3", "q3"), ("B", "THANK_YOU", "bye", "bye")],
                  submission_id="101", session="b" * 32)
    s3 = new_rows([("O", "", "hello", "hello"), ("B", "SCHOOL_TYPE", "q2", "q2"), ("R", "SCHOOL_TYPE", "a2", "a2"),
                   ("B", "CLOSING", "bye", "bye")], submission_id="102", session="c" * 32)
    return make_df(s1 + s2 + s3)


def test_report_is_none_without_expect_stages(config):
    assert "expect_stages" not in config
    assert report_unexpected_stages(_stage_df(), config) is None


def test_report_counts_unexpected_stages_and_their_sessions_without_naming_them(config):
    config["expect_stages"] = ["STATE_IDENTIFICATION"]
    report = report_unexpected_stages(_stage_df(), config)
    # THANK_YOU / CLOSING are closing keys; SCHOOL_TYPE is in 2 sessions (probes do not count twice) and MYSTERY in 1:
    # two distinct unexpected values, two sessions. Counts only: no stage value from the file is in the report.
    assert report == {"sessions": 3, "sessions_with_unexpected_stages": 2, "unexpected_stage_count": 2}
    assert "SCHOOL_TYPE" not in json.dumps(report) and "MYSTERY" not in json.dumps(report)


def test_report_with_everything_expected_is_empty(config):
    config["expect_stages"] = ["STATE_IDENTIFICATION", "SCHOOL_TYPE", "MYSTERY"]
    report = report_unexpected_stages(_stage_df(), config)
    assert report["unexpected_stage_count"] == 0 and report["sessions_with_unexpected_stages"] == 0


def test_report_is_aggregates_only(config):
    config["expect_stages"] = []
    flat = json.dumps(report_unexpected_stages(_stage_df(), config))
    for text in ("hello", "probe", "q3", "a2"):
        assert text not in flat  # no message content, only counts


def test_report_never_changes_what_the_builder_does(config):
    """Stage names legitimately change between versions: an unexpected stage never skips a session."""
    df = _stage_df()
    baseline_events, baseline_skipped = build(df, config)
    config["expect_stages"] = ["NOTHING_MATCHES"]
    assert report_unexpected_stages(df, config)["unexpected_stage_count"] == 3
    events, skipped = build(df, config)
    assert events == baseline_events and skipped == baseline_skipped


def test_report_counts_any_number_of_distinct_stages_and_lists_none(config):
    config["expect_stages"] = ["X"]
    rows = []
    for n in range(205):
        rows += new_rows([("O", "", "h", "h"), ("B", f"STAGE_{n:03d}", "q", "q"), ("R", f"STAGE_{n:03d}", "a", "a")],
                         submission_id=str(500 + n), session=f"{n:032d}")
    report = report_unexpected_stages(make_df(rows), config)
    assert report == {"sessions": 205, "sessions_with_unexpected_stages": 205, "unexpected_stage_count": 205}


def test_report_without_a_stage_column_says_so(config):
    config["expect_stages"] = ["X"]
    df = _stage_df().drop(columns=["stage"])
    report = report_unexpected_stages(df, config)
    assert report["unexpected_stage_count"] == 0 and "missing" in report["note"]


# stage report privacy -------------------------------------------------------------------------------------------

def test_stage_report_never_contains_a_value_from_the_stage_column():
    """Inline #10: a misaligned stage cell can hold a name (`John_Smith` looks exactly like a stage key), text or a
    number. Whatever the shape, only counts are reported: the report holds no free text from the CSV."""
    config = {**mitra_config_v1("Youth"), "expect_stages": ["STATE_IDENTIFICATION"]}
    leaked = "My name is Asha and I live near the old mill"
    values = [leaked, "yes, 12 years", "John_Smith", "MYSTERY_STAGE", "9876543210", "A" * 64, "A" * 65, "HAS-DASH"]
    rows = new_rows([("O", "", "hello", "hello"), ("B", "STATE_IDENTIFICATION", "q", "q")]
                    + [("R", value, "a", "a") for value in values], submission_id="100", session="a" * 32)
    report = report_unexpected_stages(make_df(rows), config)
    flat = json.dumps(report)
    for value in values + ["Asha", "mill", "Smith", "12 years"]:
        assert value not in flat
    assert report == {"sessions": 1, "sessions_with_unexpected_stages": 1, "unexpected_stage_count": len(values)}


def test_stage_report_keys_are_only_the_counters():
    config = {**mitra_config_v1("Youth"), "expect_stages": ["X"]}
    rows = new_rows([("O", "", "h", "h"), ("B", "KEY_ONE", "q", "q")], submission_id="2", session="b" * 32)
    assert set(report_unexpected_stages(make_df(rows), config)) == {
        "sessions", "sessions_with_unexpected_stages", "unexpected_stage_count"}
