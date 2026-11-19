"""
Unit tests for app/api/services/feedback_gforms.py (Google Forms adapter: pure functions).
All fixtures are synthetic (tests/csv_uploads/feedback_gforms_*.csv or inline frames); no real respondent text.
"""
import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from app.api.services.feedback_gforms import (
    build_gforms_events,
    check_gforms_file,
    match_gforms_form,
    normalise_header,
    parse_gforms_csv,
    prepare_gforms_responses,
)
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS

FIXTURES = Path(__file__).resolve().parent / "csv_uploads"
YOUTH = PERSONAS["Youth"]
PROGRAM = {"id": YOUTH["program_id"], "name": FEEDBACK_PROGRAM_NAME}
LEADER = {"id": YOUTH["leader_category_id"], "name": "Youth"}
PUBLISHED = "2026-10-20T00:00:00.000Z"


def q(key, text, headers, kind="open"):
    return {"headers": headers, "question_key": key, "text": text, "kind": kind}


AGE = q("AGE_IDENTIFICATION", "How old are you?", ["How old are you?"], "metadata")
GENDER = q("GENDER_IDENTIFICATION", "What is your gender?", ["What is your gender?"], "metadata")
STATE = q("STATE_IDENTIFICATION", "Which state do you stay in?", ["Which state do you stay in?"], "metadata")
HOPE = q("HOPE", "What is your one biggest hope for young people around you?",
         ["What is your one biggest hope for young people around you?",
          "What is your biggest hope for young people?"])
WORRY = q("WORRY", "What is your one biggest worry for young people around you?",
          ["What is your one biggest worry for young people around you?"])
CLASS = q("CLASS_COURSE", "If you are studying, what class/ course are you studying?",
          ["If you are studying, what class/ course are you studying?"], "metadata")
INCOME = q("INCOME", "What is your average annual household income?",
           ["What is your average annual household income?"], "metadata")
FUTURE = q("FUTURE", "When you think about your future, what do you want your life to look like?",
           ["When you think about your future, what do you want your life to look like?"])


def make_config():
    return {
        "session_type": "Youth_Voice",
        "adapter": "wide_form",
        "timestamp_column": "Timestamp",
        "datetime": {"formats": ["%m/%d/%Y %H:%M:%S"], "fallback_timezone": "Asia/Kolkata"},
        "default_completion": "COMPLETED",
        "ignored_columns": ["Email Address"],
        "ignored_column_patterns": ["^Column \\d+$"],
        "language": {"default": "en", "script_detect": {"kn": "[\\u0C80-\\u0CFF]"}},
        "forms": {
            "general": {"name": "General Public", "questionnaire_version": "V1",
                        "questions": [AGE, GENDER, STATE, HOPE, WORRY]},
            "youth": {"name": "Youth", "questionnaire_version": "V2",
                      "questions": [AGE, GENDER, STATE, CLASS, INCOME, FUTURE]},
        },
    }


@pytest.fixture
def config():
    return make_config()


def read_fixture(name):
    return parse_gforms_csv((FIXTURES / name).read_bytes())


def frame(rows, columns):
    return pd.DataFrame(rows, columns=columns).astype(str)


GENERAL_COLS = ["Timestamp", "Email Address", "How old are you?", "What is your gender? ",
                "Which state do you stay in? ", "What is your one biggest hope for young people around you?",
                "What is your one biggest worry for young people around you?", "Column 8"]


def general_frame(rows):
    """rows: lists of (timestamp, age, gender, state, hope, worry); email and the stray column are filled."""
    return frame([[r[0], "x@example.com", r[1], r[2], r[3], r[4], r[5], ""] for r in rows], GENERAL_COLS)


def prepare(df, cfg):
    return prepare_gforms_responses(df, cfg)


def build(responses, cfg=None, **overrides):
    kwargs = dict(known_refs=set(), program_info=PROGRAM, leader_info=LEADER, tenant_code="mitra",
                  source="google_forms", published_at=PUBLISHED, config=cfg)
    kwargs.update(overrides)
    return build_gforms_events(responses, **kwargs)


def counter_factory():
    state = {"n": 0}

    def factory():
        state["n"] += 1
        return f"id-{state['n']}"
    return factory


# ---------------------------------------------------------------------------------------------
# parse_gforms_csv
# ---------------------------------------------------------------------------------------------

class TestParse:
    def test_reads_everything_as_text_and_keeps_na_and_none(self):
        df = read_fixture("feedback_gforms_general.csv")
        assert df.shape == (7, 8)
        row = df[df["Timestamp"] == "9/26/2026 9:58:05"].iloc[0]
        assert row["How old are you?"] == "NA"
        assert row["What is your gender? "] == "None"
        assert row["What is your one biggest hope for young people around you?"] == ""

    def test_keeps_quotes_commas_and_newlines_in_answers(self):
        df = read_fixture("feedback_gforms_general.csv")
        assert df.iloc[0]["What is your one biggest hope for young people around you?"] == "Good jobs, safe schools"
        assert df.iloc[0]["What is your one biggest worry for young people around you?"] == 'He said "no"\nand then left'

    def test_drops_bom_and_keeps_exported_headers(self):
        df = read_fixture("feedback_gforms_kannada.csv")
        assert df.columns[0] == "Timestamp"
        assert "\n" in df.columns[5]

    @pytest.mark.parametrize("payload", [b"", None])
    def test_empty_file_is_rejected(self, payload):
        with pytest.raises(ValueError, match="empty"):
            parse_gforms_csv(payload)

    def test_header_only_file_is_rejected(self):
        with pytest.raises(ValueError, match="no data rows"):
            parse_gforms_csv((FIXTURES / "feedback_gforms_headers_only.csv").read_bytes())

    def test_unparsable_file_is_rejected(self):
        with pytest.raises(ValueError, match="could not be parsed"):
            parse_gforms_csv(b"a,b\n1,2,3,4\n1\n\"unterminated,2,3\n1,2,3,4,5,6,7")

    def test_non_utf8_is_rejected(self):
        with pytest.raises(ValueError, match="UTF-8"):
            parse_gforms_csv(b"Timestamp,a\n1,\xff\xfe\xfa\n")


# ---------------------------------------------------------------------------------------------
# normalise_header
# ---------------------------------------------------------------------------------------------

class TestNormaliseHeader:
    def test_income_example_from_the_contract(self):
        header = "What is your average annual household income?\n/ನಿಮ್ಮ ಕುಟುಂಬದ ಸರಾಸರಿ ವಾರ್ಷಿಕ ಆದಾಯ ಎಷ್ಟು?”"
        assert normalise_header(header) == "What is your average annual household income?"

    def test_class_course_example_from_the_contract(self):
        header = "If you are studying, what class/ course are you studying?ನೀವು ಓದುತ್ತಿದ್ದರೆ, ಯಾವ ತರಗತಿ?"
        assert normalise_header(header) == "If you are studying, what class/ course are you studying?"

    @pytest.mark.parametrize("header,expected", [
        ("Which State do you stay in? ", "Which State do you stay in?"),
        ("\nWhat are 2-3 things?", "What are 2-3 things?"),
        ("Did you get\n a chance?\t If yes,  which class?", "Did you get a chance? If yes, which class?"),
        (" Where do you think they can get these skills?", "Where do you think they can get these skills?"),
        ("What is your gender? ಲಿಂಗ ", "What is your gender?"),
        ("How old are you? ನಿಮ್ಮ ವಯಸ್ಸು ಎಷ್ಟು?", "How old are you?"),
        ('Is it hard? /"', "Is it hard?"),
        ("Is it hard? “” / ", "Is it hard?"),
        ("Timestamp", "Timestamp"),
        ("", ""),
        ("ಕನ್ನಡ ಮಾತ್ರ", ""),
    ])
    def test_cases(self, header, expected):
        assert normalise_header(header) == expected

    def test_is_idempotent(self):
        header = "What is your average annual household income?\n/ನಿಮ್ಮ ಆದಾಯ?”"
        once = normalise_header(header)
        assert normalise_header(once) == once


# ---------------------------------------------------------------------------------------------
# Form matching
# ---------------------------------------------------------------------------------------------

class TestMatchForm:
    def test_general_fixture_matches_general_form(self, config):
        df = read_fixture("feedback_gforms_general.csv")
        assert match_gforms_form(df, config) == ("general", [])
        assert check_gforms_file(df, config) == []

    def test_bilingual_fixture_matches_youth_form(self, config):
        df = read_fixture("feedback_gforms_kannada.csv")
        assert match_gforms_form(df, config) == ("youth", [])

    def test_ignored_columns_and_patterns_never_count_as_unmapped(self, config):
        df = general_frame([("10/2/2026 10:09:20", "22", "Female", "Goa", "a", "b")])
        df["Column 99"] = ""
        df["Email Address"] = "someone@example.com"
        assert match_gforms_form(df, config) == ("general", [])

    def test_header_variant_matches(self, config):
        cols = ["Timestamp", "How old are you?", "What is your biggest hope for young people?"]
        df = frame([["10/2/2026 10:09:20", "22", "hope"]], cols)
        assert match_gforms_form(df, config) == ("general", [])

    def test_unmapped_headers_are_listed(self, config):
        df = general_frame([("10/2/2026 10:09:20", "22", "Female", "Goa", "a", "b")])
        df["Something new?"] = "x"
        df["Another one"] = "y"
        form_key, errors = match_gforms_form(df, config)
        assert form_key is None
        assert any("Something new?" in e and "Another one" in e for e in errors)
        assert check_gforms_file(df, config) == errors

    def test_missing_timestamp_is_reported(self, config):
        df = general_frame([("10/2/2026 10:09:20", "22", "Female", "Goa", "a", "b")]).drop(columns=["Timestamp"])
        form_key, errors = match_gforms_form(df, config)
        assert form_key is None
        assert any("Timestamp" in e for e in errors)

    def test_missing_timestamp_and_unmapped_are_both_reported(self, config):
        df = frame([["22", "x"]], ["How old are you?", "Totally unknown"])
        form_key, errors = match_gforms_form(df, config)
        assert form_key is None
        assert any("Timestamp" in e for e in errors)
        assert any("Totally unknown" in e for e in errors)

    def test_file_with_only_ignored_columns_has_no_questions(self, config):
        df = frame([["10/2/2026 10:09:20", "a@example.com", ""]], ["Timestamp", "Email Address", "Column 3"])
        form_key, errors = match_gforms_form(df, config)
        assert form_key is None
        assert any("no question columns" in e for e in errors)

    def test_two_columns_for_the_same_question_are_rejected(self, config):
        cols = ["Timestamp", "What is your one biggest hope for young people around you?",
                "What is your biggest hope for young people?"]
        df = frame([["10/2/2026 10:09:20", "a", "b"]], cols)
        form_key, errors = match_gforms_form(df, config)
        assert form_key is None
        assert any("HOPE" in e for e in errors)

    def test_form_that_maps_every_column_wins(self):
        cfg = make_config()
        cfg["forms"] = {"small": {"name": "small", "questionnaire_version": "V1", "questions": [AGE, GENDER]},
                        "big": {"name": "big", "questionnaire_version": "V1", "questions": [AGE, GENDER, STATE]}}
        df = frame([["10/2/2026 10:09:20", "22", "F", "Goa"]], ["Timestamp", "How old are you?",
                                                                 "What is your gender?", "Which state do you stay in?"])
        # 'small' leaves the state column unmapped, only 'big' maps every column
        assert match_gforms_form(df, cfg) == ("big", [])

    def test_tighter_fit_wins_when_matched_counts_are_equal(self):
        cfg = make_config()
        cfg["forms"] = {"small": {"name": "small", "questionnaire_version": "V1", "questions": [AGE, GENDER]},
                        "big": {"name": "big", "questionnaire_version": "V1", "questions": [AGE, GENDER, STATE]}}
        df = frame([["10/2/2026 10:09:20", "22", "F"]], ["Timestamp", "How old are you?", "What is your gender?"])
        assert match_gforms_form(df, cfg) == ("small", [])

    def test_exact_tie_is_ambiguous(self):
        cfg = make_config()
        cfg["forms"] = {"a": {"name": "a", "questionnaire_version": "V1", "questions": [AGE, GENDER]},
                        "b": {"name": "b", "questionnaire_version": "V1", "questions": [AGE, GENDER]}}
        df = frame([["10/2/2026 10:09:20", "22", "F"]], ["Timestamp", "How old are you?", "What is your gender?"])
        form_key, errors = match_gforms_form(df, cfg)
        assert form_key is None
        assert len(errors) == 1 and "'a'" in errors[0] and "'b'" in errors[0]

    def test_invalid_config_raises(self):
        df = frame([["10/2/2026 10:09:20", "22"]], ["Timestamp", "How old are you?"])
        with pytest.raises(ValueError, match="forms"):
            match_gforms_form(df, {"adapter": "wide_form", "timestamp_column": "Timestamp",
                                   "datetime": {"formats": ["%m/%d/%Y"]}})
        with pytest.raises(ValueError, match="adapter"):  # a config without an adapter is never guessed
            match_gforms_form(df, {"timestamp_column": "Timestamp", "datetime": {"formats": ["%m/%d/%Y"]}})


# ---------------------------------------------------------------------------------------------
# Responses: timestamps, answers, skipping, language, collapse
# ---------------------------------------------------------------------------------------------

class TestPrepare:
    def test_fixture_counts(self, config):
        df = read_fixture("feedback_gforms_general.csv")
        responses, skipped, stats = prepare(df, config)
        # 7 rows: 1 identical pair collapses, 1 blank-only row and 1 bad timestamp are skipped
        assert stats == {"rows_read": 7, "collapsed_duplicates": 1}
        assert len(responses) == 4
        assert [s["row"] for s in skipped] == [6, 7]
        assert len({r["source_ref"] for r in responses}) == 4

    def test_month_first_timestamps_and_utc_conversion(self, config):
        df = general_frame([("9/26/2026 9:58:05", "22", "F", "Goa", "a", "b"),
                            ("10/2/2026 10:09:20", "23", "M", "Goa", "c", "d")])
        responses, skipped, _ = prepare(df, config)
        assert skipped == []
        assert responses[0]["timestamp_iso"] == "2026-09-26T09:58:05"
        assert responses[0]["submission_date"] == "2026-09-26T04:28:05.000Z"      # 09:58 IST = 04:28 UTC
        assert responses[1]["timestamp_iso"] == "2026-10-02T10:09:20"
        assert responses[1]["submission_date"] == "2026-10-02T04:39:20.000Z"
        assert responses[1]["source_ref"].startswith("2026-10-02T10:09:20:")

    def test_other_fallback_timezone(self, config):
        config["datetime"]["fallback_timezone"] = "UTC"
        df = general_frame([("10/2/2026 10:09:20", "22", "F", "Goa", "a", "b")])
        responses, _, _ = prepare(df, config)
        assert responses[0]["submission_date"] == "2026-10-02T10:09:20.000Z"

    def test_unknown_timezone_raises(self, config):
        config["datetime"]["fallback_timezone"] = "Nowhere/Land"
        df = general_frame([("10/2/2026 10:09:20", "22", "F", "Goa", "a", "b")])
        with pytest.raises(ValueError, match="timezone"):
            prepare(df, config)

    def test_day_first_value_is_not_silently_accepted(self, config):
        # month 26 does not exist: a day-first export would be unparsable, never misread
        df = general_frame([("26/9/2026 9:58:05", "22", "F", "Goa", "a", "b")])
        responses, skipped, _ = prepare(df, config)
        assert responses == []
        assert skipped[0]["problems"][0].startswith("Timestamp does not match any of the accepted formats")
        assert "26/9/2026" not in skipped[0]["problems"][0]  # the cell value is never echoed

    def test_answers_follow_form_order_and_skip_blanks(self, config):
        # columns in a different order than the form; the worry cell is blank
        cols = ["Timestamp", "What is your one biggest worry for young people around you?", "Which state do you stay in?",
                "How old are you?", "What is your one biggest hope for young people around you?"]
        df = frame([["10/2/2026 10:09:20", "", "Goa", "22", "jobs"]], cols)
        responses, _, _ = prepare(df, config)
        assert [a["question_key"] for a in responses[0]["answers"]] == ["AGE_IDENTIFICATION", "STATE_IDENTIFICATION", "HOPE"]
        assert responses[0]["answers"][0] == {"question_key": "AGE_IDENTIFICATION", "text": "How old are you?", "answer": "22"}

    def test_answer_text_keeps_commas_quotes_newlines_and_na_none(self, config):
        df = read_fixture("feedback_gforms_general.csv")
        responses, _, _ = prepare(df, config)
        by_age = {r["answers"][0]["answer"]: r for r in responses}
        assert by_age["22"]["answers"][-1]["answer"] == 'He said "no"\nand then left'
        assert by_age["22"]["answers"][-2]["answer"] == "Good jobs, safe schools"
        assert [a["answer"] for a in by_age["NA"]["answers"][:2]] == ["NA", "None"]

    def test_bilingual_file_end_to_end(self, config):
        df = read_fixture("feedback_gforms_kannada.csv")
        responses, skipped, stats = prepare(df, config)
        assert skipped == [] and stats == {"rows_read": 3, "collapsed_duplicates": 0}
        assert [r["language"] for r in responses] == ["kn", "en", "en"]
        assert len({r["source_ref"] for r in responses}) == 3       # same second, different answers
        assert responses[1]["timestamp_iso"] == responses[2]["timestamp_iso"] == "2026-09-26T10:29:57"
        # the optional class/course question was left blank in rows 2 and 3
        assert "CLASS_COURSE" in [a["question_key"] for a in responses[0]["answers"]]
        assert "CLASS_COURSE" not in [a["question_key"] for a in responses[1]["answers"]]
        # question text is the English text, whatever the exported header looked like
        income = [a for a in responses[0]["answers"] if a["question_key"] == "INCOME"][0]
        assert income["text"] == "What is your average annual household income?"

    def test_one_kannada_answer_makes_the_whole_response_kannada(self, config):
        df = general_frame([("10/2/2026 10:09:20", "22", "F", "Goa", "plain english", "ಕಷ್ಟ")])
        responses, _, _ = prepare(df, config)
        assert responses[0]["language"] == "kn"

    def test_detect_kinds_limits_language_detection_to_open_answers(self, config):
        # bilingual choice answers (metadata) carry Kannada script even when the free text is English
        config["forms"]["general"]["questions"][0] = q("AGE_IDENTIFICATION", "How old are you?",
                                                       ["How old are you?"], "metadata")
        config["language"]["detect_kinds"] = ["open"]
        df = general_frame([("10/2/2026 10:09:20", "17 / ಹದಿನೇಳು", "F", "Goa", "plain english", "also english"),
                            ("10/2/2026 10:09:21", "17 / ಹದಿನೇಳು", "F", "Goa", "ಕಲಿಕೆ", "english")])
        responses, _, _ = prepare(df, config)
        assert [r["language"] for r in responses] == ["en", "kn"]
        del config["language"]["detect_kinds"]
        responses, _, _ = prepare(df, config)
        assert [r["language"] for r in responses] == ["kn", "kn"]

    def test_blank_only_and_bad_timestamp_are_skipped_with_problems(self, config):
        df = general_frame([("10/2/2026 10:09:20", "", "", "", "", ""),       # nothing answered
                            ("", "22", "F", "Goa", "a", "b"),                   # blank timestamp
                            ("not a date", "22", "F", "Goa", "a", "b"),         # bad timestamp
                            ("", "", "", "", "", ""),                           # both problems
                            ("10/2/2026 10:09:20", "22", "F", "Goa", "a", "b")])
        responses, skipped, stats = prepare(df, config)
        assert len(responses) == 1 and stats["rows_read"] == 5
        assert [s["row"] for s in skipped] == [1, 2, 3, 4]
        assert all(s["submissionId"] is None and s["sessionId"] is None for s in skipped)
        assert skipped[0]["problems"] == ["The response has no non-blank answer"]
        assert skipped[1]["problems"] == ["Timestamp is blank"]
        assert "does not match any of the accepted formats" in skipped[2]["problems"][0]
        assert len(skipped[3]["problems"]) == 2

    def test_whitespace_only_cells_count_as_blank(self, config):
        df = general_frame([("10/2/2026 10:09:20", " ", "\n", " ", "  ", "")])
        responses, skipped, _ = prepare(df, config)
        assert responses == [] and skipped[0]["problems"] == ["The response has no non-blank answer"]

    def test_file_that_does_not_match_raises(self, config):
        df = frame([["10/2/2026 10:09:20", "x"]], ["Timestamp", "Unknown question"])
        with pytest.raises(ValueError, match="Unknown question"):
            prepare(df, config)


# ---------------------------------------------------------------------------------------------
# source_ref: stability, uniqueness, collapse
# ---------------------------------------------------------------------------------------------

ROW = ("10/2/2026 10:09:20", "22", "Female", "Karnataka", "Good jobs", "Prices")


def ref_of(df, cfg):
    responses, _, _ = prepare(df, cfg)
    assert len(responses) == 1
    return responses[0]["source_ref"]


class TestSourceRef:
    def test_format_is_timestamp_then_sha256(self, config):
        ref = ref_of(general_frame([ROW]), config)
        iso, _, digest = ref.rpartition(":")
        assert iso == "2026-10-02T10:09:20"
        assert len(digest) == 64 and int(digest, 16) >= 0

    def test_same_input_gives_same_ref(self, config):
        assert ref_of(general_frame([ROW]), config) == ref_of(general_frame([ROW]), config)

    def test_stable_when_a_blank_question_column_is_added(self, config):
        before = ref_of(general_frame([ROW]), config)
        cfg2 = make_config()
        extra = q("NEW_Q", "A brand new question?", ["A brand new question?"])
        cfg2["forms"]["general"]["questions"].append(extra)
        df2 = general_frame([ROW])
        df2["A brand new question?"] = ""
        assert ref_of(df2, cfg2) == before

    def test_stable_when_a_header_is_reworded(self, config):
        before = ref_of(general_frame([ROW]), config)
        df2 = general_frame([ROW]).rename(columns={
            "What is your one biggest hope for young people around you?": "What is your biggest hope for young people?"})
        assert ref_of(df2, config) == before

    def test_stable_when_the_column_order_changes(self, config):
        df = general_frame([ROW])
        shuffled = df[["Column 8", "What is your one biggest worry for young people around you?", "Timestamp",
                       "Which state do you stay in? ", "How old are you?", "Email Address",
                       "What is your one biggest hope for young people around you?", "What is your gender? "]]
        assert ref_of(shuffled, config) == ref_of(df, config)

    def test_stable_when_whitespace_differs(self, config):
        spaced = ("10/2/2026 10:09:20", " 22 ", "Female", "Karnataka", "Good   jobs\n", "Prices")
        assert ref_of(general_frame([spaced]), config) == ref_of(general_frame([ROW[:4] + ("Good jobs", "Prices")]), config)

    def test_stable_across_the_email_column(self, config):
        df = general_frame([ROW])
        df["Email Address"] = "somebody.else@example.com"
        assert ref_of(df, config) == ref_of(general_frame([ROW]), config)

    def test_changes_when_an_answer_changes_or_the_second_changes(self, config):
        base = ref_of(general_frame([ROW]), config)
        assert ref_of(general_frame([ROW[:5] + ("Taxes",)]), config) != base
        assert ref_of(general_frame([("10/2/2026 10:09:21",) + ROW[1:]]), config) != base

    def test_same_answer_under_a_different_question_differs(self, config):
        a = ref_of(general_frame([("10/2/2026 10:09:20", "22", "", "", "x", "")]), config)
        b = ref_of(general_frame([("10/2/2026 10:09:20", "22", "", "", "", "x")]), config)
        assert a != b

    def test_same_second_different_answers_get_different_refs(self, config):
        df = general_frame([("10/2/2026 10:09:20", "22", "Female", "Goa", "a", "b"),
                            ("10/2/2026 10:09:20", "22", "Female", "Goa", "a", "c")])
        responses, _, stats = prepare(df, config)
        assert len(responses) == 2 and stats["collapsed_duplicates"] == 0
        assert responses[0]["source_ref"] != responses[1]["source_ref"]
        assert responses[0]["source_ref"].split(":")[0:3] == responses[1]["source_ref"].split(":")[0:3]

    def test_identical_rows_collapse_and_are_counted(self, config):
        df = general_frame([ROW, ROW, ROW, ("10/2/2026 10:09:20",) + ROW[1:5] + ("Different",)])
        responses, skipped, stats = prepare(df, config)
        assert len(responses) == 2 and skipped == []
        assert stats == {"rows_read": 4, "collapsed_duplicates": 2}

    def test_unicode_answers_hash_deterministically(self, config):
        row = ("10/2/2026 10:09:20", "22", "F", "Goa", "ಕಲಿಕೆ ಮತ್ತು ಕೆಲಸ", "ಬೆಲೆ")
        assert ref_of(general_frame([row]), config) == ref_of(general_frame([row]), config)


# ---------------------------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------------------------

class TestEvents:
    def responses(self, config, rows=None):
        rows = rows if rows is not None else [ROW]
        return prepare(general_frame(rows), config)[0]

    def test_event_shape(self, config):
        responses = self.responses(config)
        events, stats = build(responses, config, id_factory=lambda: "uuid-1")
        assert stats == {"new": 1, "already_stored": 0}
        event = events[0]
        assert event["submissionId"] == event["sessionId"] == "uuid-1"
        assert event["submissionType"] == "feedback"
        assert event["tenantCode"] == "mitra"
        assert event["eventType"] == "create"
        assert event["eventPublishedAt"] == PUBLISHED
        assert event["tags"] == {
            "programId": PROGRAM["id"], "programName": FEEDBACK_PROGRAM_NAME,
            "leaderCategoryId": LEADER["id"], "leaderCategoryName": "Youth",
            "state": None, "district": None, "source": "google_forms",
        }
        data = event["data"]
        assert data["source"] == "google_forms"
        assert data["language"] == "en"
        assert data["completionStatus"] == "COMPLETED"
        assert data["submissionDate"] == "2026-10-02T04:39:20.000Z"
        assert data["sourceRef"] == responses[0]["source_ref"]

    def test_message_numbering_parents_and_texts(self, config):
        events, _ = build(self.responses(config), config)
        messages = events[0]["data"]["messages"]
        assert len(messages) == 2 * 5                                   # five answers: age, gender, state, hope, worry
        assert [m["sequence"] for m in messages] == list(range(1, 11))
        for n, m in enumerate(messages):
            assert m["isFollowUp"] is False
            assert m["messageAt"] == "2026-10-02T04:39:20.000Z"
            if n % 2 == 0:                                              # question row
                assert m["parentSequence"] is None
                assert m["message"] != "" and m["translatedMessage"] is None       # CHANGED: no copy of the question text
            else:                                                       # answer row
                assert m["parentSequence"] == messages[n - 1]["sequence"]
                assert m["questionKey"] == messages[n - 1]["questionKey"]
        assert messages[0]["message"] == "How old are you?"
        assert messages[0]["questionKey"] == "AGE_IDENTIFICATION"
        assert messages[1]["message"] == "22"
        assert messages[-1]["message"] == "Prices"

    # CHANGED: an English answer used to be copied into translatedMessage; a form carries no translation, so it is None
    def test_english_answers_have_no_translated_message(self, config):
        events, _ = build(self.responses(config), config)
        answers = [m for m in events[0]["data"]["messages"] if m["parentSequence"] is not None]
        assert len(answers) > 0 and all(m["translatedMessage"] is None and m["message"] != "" for m in answers)

    def test_kannada_response_has_no_translated_message_on_answers(self, config):
        responses = self.responses(config, [("10/2/2026 10:09:20", "22", "Female", "Goa", "ಕಲಿಕೆ", "Prices")])
        events, _ = build(responses, config)
        data = events[0]["data"]
        assert data["language"] == "kn"
        answers = [m for m in data["messages"] if m["parentSequence"] is not None]
        questions = [m for m in data["messages"] if m["parentSequence"] is None]
        assert all(m["translatedMessage"] is None for m in answers)
        assert all(m["translatedMessage"] is None and m["message"] != "" for m in questions)   # CHANGED: English question text, no copy
        assert any(m["message"] == "ಕಲಿಕೆ" for m in answers)

    # CHANGED: the default language no longer decides translatedMessage (it is None for every answer)
    def test_default_language_does_not_fill_translated_message(self, config):
        config["language"]["default"] = "hi"
        responses = prepare(general_frame([ROW]), config)[0]
        assert responses[0]["language"] == "hi"
        events, _ = build(responses, config)
        answers = [m for m in events[0]["data"]["messages"] if m["parentSequence"] is not None]
        assert len(answers) > 0 and all(m["translatedMessage"] is None for m in answers)

    def test_known_refs_are_skipped_and_counted(self, config):
        responses = self.responses(config, [ROW, ROW[:5] + ("Taxes",), ROW[:5] + ("Rent",)])
        known = {responses[0]["source_ref"], responses[2]["source_ref"], "unrelated-ref"}
        calls = []

        def factory():
            calls.append(1)
            return f"id-{len(calls)}"
        events, stats = build(responses, config, known_refs=known, id_factory=factory)
        assert stats == {"new": 1, "already_stored": 2}
        assert events[0]["data"]["sourceRef"] == responses[1]["source_ref"]
        assert len(calls) == 1                                          # ids are drawn only for new responses

    def test_everything_known_gives_no_events(self, config):
        responses = self.responses(config)
        events, stats = build(responses, config, known_refs={r["source_ref"] for r in responses})
        assert events == [] and stats == {"new": 0, "already_stored": 1}

    def test_ids_come_from_the_factory_one_per_new_response(self, config):
        responses = self.responses(config, [ROW, ROW[:5] + ("Taxes",)])
        events, _ = build(responses, config, id_factory=counter_factory())
        assert [e["submissionId"] for e in events] == ["id-1", "id-2"]
        assert all(e["submissionId"] == e["sessionId"] for e in events)

    def test_default_id_factory_gives_distinct_uuid4s(self, config):
        import uuid
        responses = self.responses(config, [ROW, ROW[:5] + ("Taxes",)])
        events, _ = build(responses, config)
        ids = [e["submissionId"] for e in events]
        assert len(set(ids)) == 2
        assert all(uuid.UUID(i).version == 4 for i in ids)

    def test_published_at_defaults_to_now_utc_milliseconds(self, config):
        events, _ = build(self.responses(config), config, published_at=None)
        stamp = events[0]["eventPublishedAt"]
        assert stamp.endswith("Z") and len(stamp) == len("2026-10-20T00:00:00.000Z")

    def test_default_completion_must_be_completed_for_a_form_response(self, config):
        config["default_completion"] = "COMPLETED"
        events, _ = build(self.responses(config), config)
        assert events[0]["data"]["completionStatus"] == "COMPLETED"
        config["default_completion"] = "IN_PROGRESS"   # a response is immutable: the config is refused, loudly
        with pytest.raises(ValueError, match="default_completion"):
            build(self.responses(config), config)

    def test_without_config_the_defaults_apply(self, config):
        events, _ = build(self.responses(config), None)
        assert events[0]["data"]["completionStatus"] == "COMPLETED"
        assert events[0]["tags"]["state"] is None

    def test_events_carry_session_type_and_the_forms_questionnaire_version(self, config):
        events, _ = build(self.responses(config), config)       # the general form is V1
        assert events[0]["data"]["sessionType"] == "Youth_Voice"
        assert events[0]["data"]["questionnaireVersion"] == "V1"

    def test_questionnaire_version_follows_the_matched_form(self, config):
        youth = prepare(read_fixture("feedback_gforms_kannada.csv"), config)[0]
        assert youth[0]["form_key"] == "youth"
        events, _ = build(youth, config)
        assert {e["data"]["questionnaireVersion"] for e in events} == {"V2"}
        assert {e["data"]["sessionType"] for e in events} == {"Youth_Voice"}

    def test_session_type_comes_from_the_config(self, config):
        config["session_type"] = "General_Public_Voice"
        events, _ = build(self.responses(config), config)
        assert events[0]["data"]["sessionType"] == "General_Public_Voice"

    def test_session_type_and_version_are_none_without_the_config_keyword(self, config):
        events, _ = build(self.responses(config), None)
        assert events[0]["data"]["sessionType"] is None
        assert events[0]["data"]["questionnaireVersion"] is None

    def test_prepared_responses_carry_the_form_key(self, config):
        responses = self.responses(config)
        assert {r["form_key"] for r in responses} == {"general"}

    def test_real_gforms_config_stamps_each_launch_form_as_v1(self):
        from seed_support import gforms_config_v1
        real = gforms_config_v1("General Public")
        responses = prepare_gforms_responses(general_frame([ROW]).rename(
            columns={"What is your gender? ": "What is your gender?"}), real)[0]
        events, _ = build(responses, real)
        assert events[0]["data"]["sessionType"] == "General_Public_Voice"
        assert events[0]["data"]["questionnaireVersion"] == "V1"

    def test_location_tags_when_configured(self, config):
        config["location"] = {"state": {"stage": "STATE_IDENTIFICATION",
                                        "aliases": {"karnataka": "Karnataka", "ka": "Karnataka"}}}
        events, _ = build(self.responses(config), config)
        assert events[0]["tags"]["state"] == "Karnataka"
        assert events[0]["tags"]["district"] is None
        other = self.responses(config, [("10/2/2026 10:09:20", "22", "F", "Atlantis", "a", "b")])
        events, _ = build(other, config)
        assert events[0]["tags"]["state"] is None

    def test_events_survive_a_strict_json_round_trip(self, config):
        df = read_fixture("feedback_gforms_general.csv")
        responses, _, _ = prepare(df, config)
        events, _ = build(responses, config)
        assert len(events) == 4
        assert json.loads(json.dumps(events, allow_nan=False)) == events
        kn_events, _ = build(prepare(read_fixture("feedback_gforms_kannada.csv"), config)[0], config)
        assert json.loads(json.dumps(kn_events, allow_nan=False, ensure_ascii=False)) == kn_events

    def test_responses_are_not_mutated(self, config):
        responses = self.responses(config)
        before = copy.deepcopy(responses)
        build(responses, config)
        assert responses == before


# ---------------------------------------------------------------------------------------------
# Re-upload (delta) behaviour across the whole adapter
# ---------------------------------------------------------------------------------------------

class TestDelta:
    def test_reupload_and_superset_insert_only_new_responses(self, config):
        first = general_frame([("10/2/2026 10:09:20", "22", "F", "Goa", "a", "b"),
                               ("10/2/2026 10:09:20", "30", "M", "Goa", "c", "d")])
        responses, _, _ = prepare(first, config)
        events, stats = build(responses, config)
        assert stats["new"] == 2
        stored = {e["data"]["sourceRef"] for e in events}

        again, _, _ = prepare(first, config)
        _, stats = build(again, config, known_refs=stored)
        assert stats == {"new": 0, "already_stored": 2}

        grown = general_frame([("10/2/2026 10:09:20", "22", "F", "Goa", "a", "b"),
                               ("10/2/2026 10:09:20", "30", "M", "Goa", "c", "d"),
                               ("10/3/2026 11:00:00", "41", "F", "Goa", "e", "f")])
        events, stats = build(prepare(grown, config)[0], config, known_refs=stored)
        assert stats == {"new": 1, "already_stored": 2}
        assert events[0]["data"]["submissionDate"] == "2026-10-03T05:30:00.000Z"
