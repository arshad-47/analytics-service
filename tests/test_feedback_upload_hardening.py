"""
Upload hardening of the Mitra CSV (adapter conversation_rows), found with the General Public failure-scenario file:

  1. a row with a blank submission_id skips its WHOLE session (no answer disappears from an accepted session);
  2. NUL bytes are removed before parsing (pandas would cut the cell at the NUL), counted, never logged with text;
  3. C0 control characters (except TAB / LF / CR) and DEL are replaced by a space in message and translated_message;
  4. one bad row does not drop a session: an empty respondent row keeps a placeholder, day-first dates are read,
     a row without a valid time gets messageAt null, session_status is matched ignoring case;
  5. csv_uploads.status is 'partial' when at least one session was skipped, 'success' otherwise;
  6. events are ordered by bot id ascending (numeric) and published in that order, under one Kafka key.

Unit tests, no database: inline frames and the in-memory harness of tests/test_feedback_upload_api.py.
All text is synthetic. New optional config keys (each with a default in code): `empty_message_placeholder` and
`datetime.formats` (extends the built-in day-first formats).
"""
import json
import logging
from datetime import datetime, timezone

import pytest

from app.api.services import feedback_uploads as fu
from app.api.services.feedback_uploads import (
    DEFAULT_DAY_FIRST_FORMATS,
    NUL_BYTES_ATTR,
    check_feedback_file,
    parse_feedback_csv,
)
from feedback_test_windows import config_with_v2
from tests.test_feedback_upload_api import BgHarness, _csv, _session
from tests.test_feedback_uploads import (
    COLUMNS,
    PARENT_TYPE,
    V1_STYLE,
    V2_STYLE,
    build,
    by_sequence,
    make_df,
    new_rows,
    session_rows,
)

SESS_A = "a" * 32
SESS_B = "b" * 32
SESS_C = "c" * 32


@pytest.fixture
def config():
    return config_with_v2("Parent")


def csv_bytes(rows, columns=COLUMNS):
    """A CSV file (bytes) of the baseline row of make_df overridden by `rows`; cells are written as they are."""
    frame = make_df(rows, columns)
    lines = [",".join(columns)]
    for _, row in frame.iterrows():
        lines.append(",".join('"' + str(row[c]).replace('"', '""') + '"' for c in columns))
    return ("\n".join(lines) + "\n").encode("utf-8")


# ======================================================================================= 1. blank submission_id

def two_sessions_with_a_blank_submission_id_row(blank_sequence=4):
    rows = (new_rows(V1_STYLE, submission_id="100", session=SESS_A)
            + new_rows(V1_STYLE, submission_id="200", session=SESS_B))
    rows[blank_sequence - 1]["submission_id"] = ""          # one row of the first session loses its submission_id
    return rows


def test_a_blank_submission_id_row_skips_the_whole_session(config):
    events, skipped = build(make_df(two_sessions_with_a_blank_submission_id_row()), config)
    assert [e["submissionId"] for e in events] == ["200"]            # the other session is untouched
    # ONE entry for the session, identified by the ids found on its other rows (not an anonymous second entry)
    assert skipped == [{"submissionId": "100", "sessionId": SESS_A,
                        "problems": ["submission_id is blank in 1 row(s)"]}]


def test_the_blank_submission_id_rows_are_counted_and_other_problems_are_kept(config):
    rows = new_rows(V1_STYLE, submission_id="100", session=SESS_A)
    rows[2]["submission_id"] = ""
    rows[3]["submission_id"] = ""
    rows[1]["language"] = "en"                                       # a second problem in the same session
    events, skipped = build(make_df(rows), config)
    assert events == [] and len(skipped) == 1
    assert "submission_id is blank in 2 row(s)" in skipped[0]["problems"]
    assert any(p.startswith("language differs") for p in skipped[0]["problems"])


def test_a_blank_session_row_skips_the_whole_submission(config):
    rows = new_rows(V1_STYLE, submission_id="100", session=SESS_A)
    rows[3]["session"] = ""
    events, skipped = build(make_df(rows), config)
    assert events == []
    assert skipped == [{"submissionId": "100", "sessionId": SESS_A, "problems": ["session is blank in 1 row(s)"]}]


def test_rows_without_any_id_or_with_one_id_only_are_still_reported_alone(config):
    # no sibling rows carry the other id, so nothing can be attributed: reported as before, never accepted
    df = make_df([{"submission_id": ""}, {"session": "", "submission_id": "7"}])
    events, skipped = build(df, config)
    assert events == []
    assert {(s["submissionId"], s["sessionId"]) for s in skipped} == {(None, "s" * 32), ("7", None)}


def test_no_row_is_dropped_silently_a_session_is_complete_or_skipped(config):
    rows = (new_rows(V1_STYLE, submission_id="1", session=SESS_A)                       # good
            + new_rows(V1_STYLE, submission_id="2", session=SESS_B)                     # one blank submission_id row
            + new_rows(V2_STYLE, submission_id="3", session=SESS_C)                     # one blank session row
            + new_rows(V1_STYLE, submission_id="4", session="d" * 32)                   # one unparsable time
            + new_rows(V1_STYLE, submission_id="5", session="e" * 32))                  # one empty respondent row
    rows[len(V1_STYLE) + 2]["submission_id"] = ""
    rows[len(V1_STYLE) * 2 + 3]["session"] = ""
    rows[len(V1_STYLE) * 2 + len(V2_STYLE) + 1]["created_at"] = "garbage"
    rows[len(V1_STYLE) * 3 + len(V2_STYLE) + 1].update({"message": "", "translated_message": ""})
    df = make_df(rows)
    events, skipped = build(df, config)

    accepted_rows = sum(len(e["data"]["messages"]) for e in events)                     # opening rows are kept
    skipped_rows = 0
    for entry in skipped:
        skipped_rows += int(((df["submission_id"] == (entry["submissionId"] or ""))
                             | (df["session"] == (entry["sessionId"] or ""))).sum())
    assert {e["submissionId"] for e in events} == {"1", "4", "5"}
    assert {s["submissionId"] for s in skipped} == {"2", "3"}
    assert accepted_rows + skipped_rows == len(df)


# ======================================================================================= 2. NUL bytes

NUL_FILE_ROWS = new_rows(V1_STYLE, submission_id="100", session=SESS_A)


def nul_file():
    rows = [dict(r) for r in NUL_FILE_ROWS]
    rows[1]["message"] = "options\x00 today"
    rows[1]["translated_message"] = "options\x00 today"
    return csv_bytes(rows)


def test_a_nul_byte_is_removed_before_parsing_so_the_cell_is_not_cut(config):
    df = parse_feedback_csv(nul_file())
    assert df["message"].tolist()[1] == "options today"
    assert df["translated_message"].tolist()[1] == "options today"
    assert df.attrs[NUL_BYTES_ATTR] == 2
    events, skipped = build(df, config)
    assert skipped == []
    assert by_sequence(events[0])[2]["message"] == "options today"
    assert by_sequence(events[0])[2]["translatedMessage"] == "options today"


def test_a_file_without_nul_bytes_reports_zero(caplog):
    with caplog.at_level(logging.WARNING):
        df = parse_feedback_csv(csv_bytes(NUL_FILE_ROWS))
    assert df.attrs[NUL_BYTES_ATTR] == 0
    assert not [r for r in caplog.records if "NUL" in r.getMessage()]


def test_the_nul_log_line_carries_the_count_and_no_text(caplog):
    with caplog.at_level(logging.WARNING, logger="analytics_service.api.services.feedback_uploads"):
        parse_feedback_csv(nul_file())
    lines = [r.getMessage() for r in caplog.records if "NUL" in r.getMessage()]
    assert lines == ["Removed 2 NUL byte(s) from the uploaded feedback CSV before parsing"]
    assert "options" not in lines[0] and "today" not in lines[0]


def test_a_file_of_only_nul_bytes_is_an_empty_file():
    with pytest.raises(ValueError):
        parse_feedback_csv(b"\x00\x00\x00")


def test_the_upload_report_carries_nul_bytes_removed(monkeypatch):
    h = BgHarness(monkeypatch)
    raw = _csv(_session(SESS_A, "1001", answer="options\x00 today") + _session(SESS_B, "1002"))
    h.run(file_bytes=raw)
    status, meta = h.final
    assert status == "success" and meta["nul_bytes_removed"] == 1
    event = json.loads(h.push.call_args.args[0][0][0])
    assert any(m["message"] == "options today" for m in event["data"]["messages"])


# ======================================================================================= 3. control characters

def one_answer(config, text, translated="t", **overrides):
    rows = new_rows(V1_STYLE, submission_id="100", session=SESS_A)
    rows[1]["message"] = text
    rows[1]["translated_message"] = translated
    report = {}
    events, skipped = build(make_df(rows), config, report=report, **overrides)
    assert skipped == []
    return by_sequence(events[0])[2], report


def test_c0_controls_and_del_become_one_space_each(config):
    text = "a\x01b\x0bc\x0cd\x1fe\x7ff\x08g"
    message, report = one_answer(config, text, translated="x\x02y")
    assert message["message"] == "a b c d e f g"
    assert message["translatedMessage"] == "x y"
    assert report["cells_with_control_characters"] == 2


def test_tab_lf_cr_and_every_printable_character_are_kept(config):
    keep = "tab\there\nnew line\r\nzero​width rtl‏ mark ‮ emoji \U0001F600 कन्नड"
    message, report = one_answer(config, keep, translated=keep)
    assert message["message"] == keep and message["translatedMessage"] == keep
    assert report["cells_with_control_characters"] == 0


def test_a_cell_of_only_control_characters_is_an_empty_cell(config):
    # a respondent row whose only text was control characters is an empty respondent row: kept with the placeholder
    message, report = one_answer(config, "\x01\x02", translated="\x7f")
    assert message["message"] == "[no text]" and report["respondent_rows_without_text"] == 1


# ======================================================================================= 4a. empty respondent row

def session_with_an_empty_row(kind_index, **cells):
    rows = new_rows(V1_STYLE, submission_id="100", session=SESS_A)
    rows[kind_index].update({"message": "", "translated_message": "", **cells})
    return rows


def test_an_empty_respondent_row_is_kept_with_the_placeholder(config):
    report = {}
    events, skipped = build(make_df(session_with_an_empty_row(1)), config, report=report)
    assert skipped == []
    kept = by_sequence(events[0])[2]
    assert kept["message"] == "[no text]" and kept["translatedMessage"] is None      # CHANGED: no copy of the placeholder
    assert kept["parentSequence"] == 1 and kept["messageAt"] is not None
    assert len(events[0]["data"]["messages"]) == len(V1_STYLE)
    assert report["respondent_rows_without_text"] == 1


def test_the_placeholder_can_be_set_in_the_config(config):
    config["empty_message_placeholder"] = "[voice note]"
    events, skipped = build(make_df(session_with_an_empty_row(1)), config)
    assert skipped == [] and by_sequence(events[0])[2]["message"] == "[voice note]"


@pytest.mark.parametrize("bad", ["", "   ", 5, None, "a\x01b"])
def test_a_malformed_placeholder_is_refused(config, bad):
    config["empty_message_placeholder"] = bad
    with pytest.raises(ValueError, match="empty_message_placeholder"):
        build(make_df(new_rows(V1_STYLE)), config)


def test_an_empty_bot_row_is_still_a_session_problem(config):
    events, skipped = build(make_df(session_with_an_empty_row(2)), config)      # row 3 is a bot row
    assert events == []
    assert any("both empty in 1 row(s)" in p for p in skipped[0]["problems"])


def test_an_empty_opening_row_is_still_a_session_problem(config):
    events, skipped = build(make_df(session_with_an_empty_row(0)), config)      # the opening row
    assert events == [] and any("both empty" in p for p in skipped[0]["problems"])


def test_a_row_with_one_empty_text_column_is_unchanged(config):
    events, skipped = build(make_df(session_with_an_empty_row(1, translated_message="english only")), config)
    assert skipped == [] and by_sequence(events[0])[2]["message"] != "[no text]"


# ======================================================================================= 4b. created_at

def created_at_session(stamps, shape=V1_STYLE):
    rows = new_rows(shape, submission_id="100", session=SESS_A)
    for row, stamp in zip(rows[1:], stamps):
        row["created_at"] = stamp
    return rows


@pytest.mark.parametrize("stamp", ["15/10/2026 08:54:43", "15/10/2026 08:54", "15-10-2026 08:54:43",
                                   "15-10-2026 08:54", "15/10/2026"])
def test_day_first_formats_are_read(config, stamp):
    assert len(DEFAULT_DAY_FIRST_FORMATS) == 5
    rows = created_at_session([stamp, "", "", ""])
    events, skipped = build(make_df(rows), config)
    assert skipped == []
    day = by_sequence(events[0])[2]["messageAt"]
    assert day.startswith("2026-10-15T") or day.startswith("2026-10-14T")           # the fallback zone shifts hours
    assert events[0]["data"]["submissionDate"] == day


@pytest.mark.parametrize("stamp, expected", [
    ("November 16, 2026, 9:09:20.296 AM ", "2026-11-16T09:09:20.296Z"),       # the Metabase export, with its trailing space
    ("November 16, 2026, 9:09:20.296 PM", "2026-11-16T21:09:20.296Z"),
    ("November 16, 2026, 9:09:20 AM", "2026-11-16T09:09:20.000Z"),
    ("November 16, 2026, 12:05 AM", "2026-11-16T00:05:00.000Z"),
    ("November 16, 2026, 12:05 PM", "2026-11-16T12:05:00.000Z"),
    ("Nov 6, 2026, 9:09:20.296 AM", "2026-11-06T09:09:20.296Z"),             # abbreviated month, one-digit day
    ("November 16, 2026 9:09:20.296 am", "2026-11-16T09:09:20.296Z"),        # no comma after the year, lower case
    ("November 16, 2026, 21:09:20", "2026-11-16T21:09:20.000Z"),             # 24-hour clock
    ("November 16, 2026", "2026-11-16T00:00:00.000Z"),                       # the date alone
])
def test_dates_with_the_month_written_out_are_read(config, stamp, expected):
    config["datetime"] = {"fallback_timezone": "UTC"}
    events, skipped = build(make_df(created_at_session([stamp, "", "", ""])), config)
    assert skipped == []
    assert by_sequence(events[0])[2]["messageAt"] == expected
    assert events[0]["data"]["submissionDate"] == expected


def test_a_written_out_month_stamp_is_read_in_the_fallback_timezone(config):
    config["datetime"] = {"fallback_timezone": "Asia/Kolkata"}
    events, _ = build(make_df(created_at_session(["November 16, 2026, 9:09:20.296 AM", "", "", ""])), config)
    assert by_sequence(events[0])[2]["messageAt"] == "2026-11-16T03:39:20.296Z"


@pytest.mark.parametrize("stamp", ["Novembre 16, 2026", "November 31, 2026, 9:09 AM", "November 16 2026 9:09 AM", "16 November 2026"])
def test_an_impossible_or_unknown_written_stamp_stays_unparsable(config, stamp):
    events, skipped = build(make_df(created_at_session([stamp, "2026-10-15T08:00:09+05:30", "", ""])), config)
    assert skipped == [] and by_sequence(events[0])[2]["messageAt"] is None      # a null time, never a wrong date


def test_a_day_first_stamp_is_read_in_the_fallback_timezone(config):
    config["datetime"] = {"fallback_timezone": "Asia/Kolkata"}
    events, _ = build(make_df(created_at_session(["15/10/2026 08:54:43", "", "", ""])), config)
    assert by_sequence(events[0])[2]["messageAt"] == "2026-10-15T03:24:43.000Z"


def test_iso_stamps_are_still_read_first(config):
    events, skipped = build(make_df(created_at_session(["2026-10-15T08:54:43+05:30"] * 4)), config)
    assert skipped == [] and by_sequence(events[0])[2]["messageAt"] == "2026-10-15T03:24:43.000Z"


def test_a_month_first_stamp_is_not_guessed(config):
    # 10/15/2026 can not be day-first: it stays unparsable (a null time), never a wrong date
    events, skipped = build(make_df(created_at_session(["10/15/2026 08:54:43", "2026-10-15T08:00:09+05:30", "", ""])), config)
    assert skipped == [] and by_sequence(events[0])[2]["messageAt"] is None


def test_datetime_formats_of_the_config_extend_the_defaults(config):
    config["datetime"] = {"fallback_timezone": "UTC", "formats": ["%Y.%m.%d %H:%M"]}
    events, skipped = build(make_df(created_at_session(["2026.10.15 08:54", "15/10/2026 09:00", "", ""])), config)
    assert skipped == []
    messages = by_sequence(events[0])
    assert messages[2]["messageAt"] == "2026-10-15T08:54:00.000Z"
    assert messages[3]["messageAt"] == "2026-10-15T09:00:00.000Z"                   # the built-in formats still work


@pytest.mark.parametrize("formats", ["%Y", [], [""], [5], ["%Y%"], ["%Y %Q"]])
def test_malformed_datetime_formats_are_refused(config, formats):
    config["datetime"] = {"fallback_timezone": "UTC", "formats": formats}
    if formats == []:
        build(make_df(new_rows(V1_STYLE)), config)                                   # an empty list only adds nothing
        return
    with pytest.raises(ValueError, match="datetime.formats"):
        build(make_df(new_rows(V1_STYLE)), config)


def test_a_row_without_a_valid_time_gets_a_null_message_time_and_is_counted(config):
    rows = created_at_session(["", "garbage", "2026-10-15T08:00:09.000000+05:30", ""])
    report = {}
    events, skipped = build(make_df(rows), config, report=report)
    assert skipped == [] and len(events) == 1
    messages = by_sequence(events[0])
    assert (messages[2]["messageAt"], messages[3]["messageAt"], messages[5]["messageAt"]) == (None, None, None)
    assert messages[4]["messageAt"] is not None and messages[1]["messageAt"] is None      # the opening row, by design
    assert report["rows_without_message_time"] == 3
    # submissionDate is the first row that HAS a time
    assert events[0]["data"]["submissionDate"] == messages[4]["messageAt"]


def test_the_first_row_without_a_time_in_an_old_shape_session_still_builds(config):
    rows = session_rows([("R", "STATE_IDENTIFICATION", "m", "t"), ("B", "DISTRICT_IDENTIFICATION", "q", "q"),
                         ("R", "DISTRICT_IDENTIFICATION", "m", "t")], submission_id="100", session=SESS_A)
    rows[0]["created_at"] = ""
    events, skipped = build(make_df(rows), config)
    assert skipped == []
    messages = events[0]["data"]["messages"]
    assert messages[0]["messageAt"] is None and messages[1]["messageAt"] is None      # synthesised question, row 1
    assert events[0]["data"]["submissionDate"] == messages[2]["messageAt"]


def test_a_session_without_any_valid_time_is_skipped_with_the_old_problem(config):
    rows = created_at_session(["", "garbage", "32/13/2026", ""])
    events, skipped = build(make_df(rows), config)
    assert events == []
    assert skipped[0]["problems"] == ["created_at is missing or unparsable in 4 row(s) (data row(s) 2, 3, 4 ...)"]
    assert "garbage" not in json.dumps(skipped)


# ======================================================================================= 4c. session_status

@pytest.mark.parametrize("word,expected", [("completed", "COMPLETED"), ("Completed", "COMPLETED"),
                                           ("in_progress", "IN_PROGRESS"), ("COMPLETED", "COMPLETED")])
def test_session_status_is_matched_ignoring_case(config, word, expected):
    events, skipped = build(make_df(new_rows(V1_STYLE, session_status=word)), config)
    assert skipped == [] and events[0]["data"]["completionStatus"] == expected


def test_session_status_is_trimmed(config):
    events, skipped = build(make_df(new_rows(V1_STYLE, session_status="  completed ")), config)
    assert skipped == [] and events[0]["data"]["completionStatus"] == "COMPLETED"


def test_the_same_status_in_two_cases_inside_a_session_is_not_a_difference(config):
    rows = new_rows(V1_STYLE)
    rows[2]["session_status"] = "completed"
    events, skipped = build(make_df(rows), config)
    assert skipped == [] and events[0]["data"]["completionStatus"] == "COMPLETED"


def test_a_different_status_inside_a_session_is_still_a_problem(config):
    rows = new_rows(V1_STYLE)
    rows[2]["session_status"] = "in_progress"
    events, skipped = build(make_df(rows), config)
    assert events == [] and any("session_status differs" in p for p in skipped[0]["problems"])


def test_an_unknown_status_word_still_skips_the_session(config):
    events, skipped = build(make_df(new_rows(V1_STYLE, session_status="FINISHED")), config)
    assert events == [] and "FINISHED" not in json.dumps(skipped)
    assert any("not in the config's completion_map" in p for p in skipped[0]["problems"])


def test_a_custom_completion_map_is_read_ignoring_case(config):
    config["completion_map"] = {"DONE": "COMPLETED", "OPEN": "IN_PROGRESS"}
    events, skipped = build(make_df(new_rows(V1_STYLE, session_status="done")), config)
    assert skipped == [] and events[0]["data"]["completionStatus"] == "COMPLETED"


def test_two_completion_map_keys_that_differ_only_by_case_with_different_targets_are_refused(config):
    config["completion_map"] = {"done": "COMPLETED", "DONE": "IN_PROGRESS"}
    with pytest.raises(ValueError, match="differ only by case"):
        build(make_df(new_rows(V1_STYLE)), config)


# ======================================================================================= 5. upload status

def test_status_is_partial_when_a_session_was_skipped(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session(SESS_B, "1002", status="DONE")))
    status, meta = h.final
    assert status == "partial"
    assert (meta["rows_pushed"], meta["sessions_skipped"], meta["sessions_dead_lettered"], meta["sessions_new"]) \
        == (1, 1, 1, 1)
    assert len(meta["schema_validation_errors"]) == 1


def test_status_is_success_when_nothing_was_skipped_and_the_counts_are_present(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session(SESS_B, "1002")))
    status, meta = h.final
    assert status == "success"
    assert (meta["sessions_skipped"], meta["sessions_dead_lettered"], meta["sessions_new"]) == (0, 0, 2)
    assert meta["warnings"] == {"rows_without_message_time": 0, "respondent_rows_without_text": 0,
                                "cells_with_control_characters": 0}
    assert meta["nul_bytes_removed"] == 0


def test_warnings_alone_do_not_make_an_upload_partial(monkeypatch):
    h = BgHarness(monkeypatch)
    rows = _session(SESS_A, "1001")
    rows[1] = rows[1].replace("2026-10-15 08:54:43+05:30", "garbage", 1)              # one unparsable time
    h.run(file_bytes=_csv(rows))
    status, meta = h.final
    assert status == "success" and meta["warnings"]["rows_without_message_time"] >= 1


def test_a_failed_publish_is_still_on_hold_not_partial(monkeypatch):
    h = BgHarness(monkeypatch)
    h.push.side_effect = RuntimeError("kafka down")
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session(SESS_B, "1002", status="DONE")))
    assert h.final[0] == "on_hold"


def test_partial_is_a_finished_status_for_the_claim_and_the_duplicate_check():
    # nothing in the upload code branches on 'success': a retry claim only refuses 'in_progress', the duplicate check
    # only ignores 'failed', and the push endpoint only takes 'pending'. 'partial' therefore behaves like 'success'.
    import inspect
    import app.database.operations as operations

    assert "status != 'in_progress'" in inspect.getsource(operations.try_claim_for_processing)
    assert "status != 'failed'" in inspect.getsource(operations.check_duplicate_file)
    assert fu.NUL_BYTES_ATTR == "nul_bytes_removed"


# ======================================================================================= 6. event order

def bot_session(bot, submission, session):
    return new_rows(V1_STYLE, submission_id=submission, session=session, bot_id=bot)


def test_events_are_ordered_by_bot_id_ascending_numerically(config):
    rows = (bot_session("103", "1", SESS_A) + bot_session("9", "2", SESS_B) + bot_session("101", "3", SESS_C)
            + bot_session("10", "4", "d" * 32))
    events, skipped = build(make_df(rows), config)
    assert skipped == []
    # numeric, not alphabetical (alphabetical would give 10, 101, 103, 9)
    assert [e["data"]["botId"] for e in events] == ["9", "10", "101", "103"]


def test_first_appearance_order_is_kept_inside_one_bot_id(config):
    rows = (bot_session("103", "5", SESS_A) + bot_session("101", "6", SESS_B) + bot_session("103", "1", SESS_C)
            + bot_session("101", "2", "d" * 32) + bot_session("103", "3", "e" * 32))
    events, _ = build(make_df(rows), config)
    assert [(e["data"]["botId"], e["submissionId"]) for e in events] == [
        ("101", "6"), ("101", "2"), ("103", "5"), ("103", "1"), ("103", "3")]


def test_a_late_lower_bot_id_comes_first(config):
    rows = bot_session("103", "1", SESS_A) + bot_session("103", "2", SESS_B) + bot_session("101", "3", SESS_C)
    events, _ = build(make_df(rows), config)
    assert events[0]["submissionId"] == "3" and events[0]["data"]["botId"] == "101"


def test_events_are_published_in_that_order_under_one_key(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session(SESS_A, "1001", bot="91") + _session(SESS_B, "1002", bot="90")
                          + _session(SESS_C, "1003", bot="91") + _session("d" * 32, "1004", bot="9")))
    payloads = h.push.call_args.args[0]
    assert [json.loads(value)["submissionId"] for value, _ in payloads] == ["1004", "1002", "1001", "1003"]
    # one key = one partition: the order of publishing is the order of consuming, whatever the partition count
    assert {key for _, key in payloads} == {"7"}


# ======================================================================================= the file-level check

def test_the_file_check_still_accepts_a_file_with_the_new_tolerances(config):
    df = make_df(created_at_session(["15/10/2026", "", "garbage", ""]) )
    assert check_feedback_file(df, config, PARENT_TYPE) == []


def test_the_report_is_optional(config):
    events, skipped = build(make_df(new_rows(V1_STYLE)), config)
    assert len(events) == 1 and skipped == []


def test_utc_helper_is_unchanged():
    assert fu._format_utc(datetime(2026, 10, 15, 8, 54, 43, tzinfo=timezone.utc)) == "2026-10-15T08:54:43.000Z"
