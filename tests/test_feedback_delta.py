"""
Tests for the delta check of `feedback` uploads (slice 2d): only what is not stored yet is published.

  - Mitra: a session not stored is `new`; a stored session the file adds nothing to is `unchanged`
    (not published, not an error); a grown session or a status flip publishes the FULL session event
    (`updated`); a stored session under another session_id is a per-session error.
  - Google Forms: responses whose source_ref is already stored are skipped; collapsed duplicates and
    rows read are reported; adapter-skipped rows go to the DLQ with their row numbers.
  - A failed lookup of the stored state puts the upload on_hold instead of publishing blindly.

In-memory fakes (the same BgHarness as tests/test_feedback_upload_api.py) except the last section: the
opt-in real-Postgres tests for get_known_source_refs / get_mitra_session_state, which need
FEEDBACK_TEST_DATABASE_URL naming a Postgres server (tests/pg_support.py gives this file a scratch database of its own).
"""
import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
import pg_support

import app.api.services.uploads as uploads_service_module
import app.database.feedback_operations as feedback_ops
from app.api.services import feedback_gforms
from seed_support import PERSONAS
from tests.test_feedback_upload_api import (
    GFORMS_ACTIVE_CONFIG,
    BgHarness,
    _csv,
    _gforms_csv,
    _gforms_headers,
    _record,
    _row,
    _session,
    _session_new_shape,
    ensure_schema,
)

SESS_A, SESS_B = "a" * 32, "b" * 32
# _session(): synthesised first question + 4 CSV rows -> 5 stored messages, stored sequences 1..5
# _session_new_shape(): the opening row (sequence 1) + 4 real rows -> also 5 stored messages, sequences 1..5
FULL_COUNT, FULL_MAX_SEQ = 5, 5


def _stored(submission_id: str, session_id: str, count: int = FULL_COUNT, max_seq: int = FULL_MAX_SEQ,
            status: str = "COMPLETED") -> Dict[str, Dict[str, Any]]:
    return {submission_id: {"session_id": session_id, "max_sequence": max_seq, "message_count": count,
                            "completion_status": status}}


def _published(h: BgHarness) -> List[dict]:
    if not h.push.called:
        return []
    return [json.loads(value) for value, _key in h.push.call_args.args[0]]


# --------------------------------------------------------------------------------------------------
# MITRA delta
# --------------------------------------------------------------------------------------------------
def test_mitra_nothing_stored_every_session_is_new_and_report_is_in_meta(monkeypatch):
    h = BgHarness(monkeypatch)
    file_bytes = _csv(_session(SESS_A, "1001") + _session(SESS_B, "1002"))
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 2 and meta["sessions_skipped"] == 0
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (2, 0, 0)
    assert meta["rows_read"] == 8
    h.state.assert_awaited_once_with("mitra", ["1001", "1002"])
    h.refs.assert_not_called()  # Google Forms lookup is not used for Mitra


def test_mitra_unchanged_session_is_not_published_and_is_not_an_error(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A)  # same messages, COMPLETED both sides
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session(SESS_B, "1002")))
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 0
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (1, 0, 1)
    assert [e["submissionId"] for e in _published(h)] == ["1002"]
    h.dlq.assert_not_called()
    assert "schema_validation_errors" not in meta


def test_mitra_reupload_of_the_same_file_publishes_nothing(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = {**_stored("1001", SESS_A), **_stored("1002", SESS_B)}
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session(SESS_B, "1002")))
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 0
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 0, 2)
    h.push.assert_not_called()
    h.dlq.assert_not_called()


def test_mitra_grown_session_publishes_the_full_event_not_a_trimmed_one(monkeypatch):
    h = BgHarness(monkeypatch)
    # stored: the first question + 2 answered rows (3 messages); the file now holds the whole session (5)
    h.state.return_value = _stored("1001", SESS_A, count=3, max_seq=3, status="IN_PROGRESS")
    h.run(file_bytes=_csv(_session(SESS_A, "1001")))
    status, meta = h.final
    assert status == "success"
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 1, 0)
    assert meta["rows_pushed"] == 1
    (event,) = _published(h)
    messages = event["data"]["messages"]
    assert [m["sequence"] for m in messages] == [1, 2, 3, 4, 5]  # all messages, none trimmed
    sequences = {m["sequence"] for m in messages}
    assert all(m["parentSequence"] in sequences for m in messages if m["parentSequence"] is not None)
    assert event["data"]["completionStatus"] == "COMPLETED"


def test_mitra_status_flip_in_progress_to_completed_publishes(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A, status="IN_PROGRESS")  # same messages, status moved on
    h.run(file_bytes=_csv(_session(SESS_A, "1001", status="COMPLETED")))
    _, meta = h.final
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 1, 0)
    assert len(_published(h)) == 1


def test_mitra_same_status_and_stored_completed_with_older_in_progress_file_are_unchanged(monkeypatch):
    # stored IN_PROGRESS + file IN_PROGRESS: nothing new
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A, status="IN_PROGRESS")
    h.run(file_bytes=_csv(_session(SESS_A, "1001", status="IN_PROGRESS")))
    assert h.final[1]["sessions_unchanged"] == 1
    h.push.assert_not_called()

    # stored COMPLETED + an older file that still says IN_PROGRESS: never moves the status back
    h2 = BgHarness(monkeypatch)
    h2.state.return_value = _stored("1001", SESS_A, status="COMPLETED")
    h2.run(file_bytes=_csv(_session(SESS_A, "1001", status="IN_PROGRESS")))
    _, meta = h2.final
    assert (meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 1)
    h2.push.assert_not_called()


def test_mitra_older_shorter_file_than_what_is_stored_is_unchanged(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A, count=9, max_seq=9, status="COMPLETED")  # stored is ahead
    h.run(file_bytes=_csv(_session(SESS_A, "1001")))
    _, meta = h.final
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 0, 1)
    h.push.assert_not_called()


def test_mitra_stored_session_without_rows_is_republished(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A, count=0, max_seq=0, status="IN_PROGRESS")
    h.run(file_bytes=_csv(_session(SESS_A, "1001")))
    assert h.final[1]["sessions_updated"] == 1


def _classify(sequences, stored_count, stored_max, status="COMPLETED", stored_status="COMPLETED"):
    """_classify_mitra_events for one session whose event holds messages with these sequences."""
    event = {"submissionId": "1001", "sessionId": SESS_A,
             "data": {"messages": [{"sequence": n} for n in sequences], "completionStatus": status}}
    state = _stored("1001", SESS_A, count=stored_count, max_seq=stored_max, status=stored_status)
    to_publish, unchanged, conflicts = uploads_service_module._classify_mitra_events([event], state)
    assert conflicts == []
    return [kind for _, kind in to_publish], unchanged


def test_growth_is_judged_by_the_message_count_and_by_the_highest_sequence_each_on_its_own():
    # same count, a gap was filled and a later row added: only the highest sequence moved (5 -> 7)
    assert _classify([1, 2, 3, 4, 7], stored_count=5, stored_max=5) == (["updated"], 0)
    # same highest sequence, one more message (a row that used to be missing): only the count moved
    assert _classify([1, 2, 3, 4, 5], stored_count=4, stored_max=5) == (["updated"], 0)
    # neither moved: unchanged, and a lower highest sequence with the same count is not growth either
    assert _classify([1, 2, 3, 4, 5], stored_count=5, stored_max=5) == ([], 1)
    assert _classify([1, 2, 3, 4, 5], stored_count=5, stored_max=6) == ([], 1)
    assert _classify([1, 2, 3, 4], stored_count=5, stored_max=5) == ([], 1)


def test_the_status_flip_alone_publishes_and_never_moves_a_completed_session_back():
    assert _classify([1, 2, 3, 4, 5], 5, 5, status="COMPLETED", stored_status="IN_PROGRESS") == (["updated"], 0)
    assert _classify([1, 2, 3, 4, 5], 5, 5, status="IN_PROGRESS", stored_status="COMPLETED") == ([], 1)


def test_mitra_session_id_conflict_is_a_session_error_dead_lettered_others_still_published(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", "f" * 32)  # 1001 is stored under another session token
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session(SESS_B, "1002")))
    status, meta = h.final
    assert status == "partial"   # CHANGED: an upload that skipped sessions ends 'partial'
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (1, 0, 0)
    (entry,) = meta["schema_validation_errors"]
    assert entry["submissionId"] == "1001" and entry["sessionId"] == SESS_A
    assert "different session_id" in entry["problems"][0]
    assert "f" * 32 not in json.dumps(meta)  # the stored token is not echoed back
    assert [e["submissionId"] for e in _published(h)] == ["1002"]
    (messages,) = h.dlq.call_args.args
    assert [headers["submissionId"] for _, headers in messages] == ["1001"]


def test_mitra_state_lookup_failure_puts_upload_on_hold_nothing_published(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.side_effect = RuntimeError("db connection lost")
    h.run(file_bytes=_csv(_session(SESS_A, "1001")))
    status, meta = h.final
    assert status == "on_hold"
    assert meta["stage"] == "Feedback Delta Lookup" and "db connection lost" in meta["exception"]
    h.push.assert_not_called()
    h.dlq.assert_not_called()


def test_mitra_builder_skipped_sessions_are_not_looked_up_and_stay_in_the_report(monkeypatch):
    h = BgHarness(monkeypatch)
    bad = _session("c" * 32, "1003", status="DONE")  # builder skips it
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + bad))
    status, meta = h.final
    assert status == "partial"   # CHANGED: an upload that skipped sessions ends 'partial'
    h.state.assert_awaited_once_with("mitra", ["1001"])
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1 and meta["sessions_new"] == 1


def test_mitra_schema_failed_new_session_is_skipped_not_counted_as_new(monkeypatch):
    h = BgHarness(monkeypatch)
    real = uploads_service_module.validate_ingestion_schema
    monkeypatch.setattr(
        uploads_service_module, "validate_ingestion_schema",
        lambda event, rt, et, feedback_schema: (
            ["bad event"] if event["submissionId"] == "1002" else real(event, rt, et, feedback_schema=feedback_schema)))
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session(SESS_B, "1002")))
    _, meta = h.final
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1
    assert meta["sessions_new"] == 1
    assert meta["schema_validation_errors"] == [{"submissionId": "1002", "sessionId": SESS_B, "problems": ["bad event"]}]


def test_mitra_all_sessions_unchanged_after_dlq_failure_still_success(monkeypatch):
    # an unchanged-only upload never touches Kafka at all
    h = BgHarness(monkeypatch)
    h.push.side_effect = RuntimeError("broker down")
    h.state.return_value = _stored("1001", SESS_A)
    h.run(file_bytes=_csv(_session(SESS_A, "1001")))
    assert h.final[0] == "success"


# --- new-shape sessions (opening row at sequence 1 with blank sender_id / stage / created_at) -----------
def test_new_shape_event_stats_count_the_opening_row_and_use_csv_sequences(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001") + _session_new_shape(SESS_B, "1002", extra_rows=2)))
    status, meta = h.final
    assert status == "success" and (meta["sessions_new"], meta["rows_pushed"]) == (2, 2)
    plain, longer = _published(h)
    assert uploads_service_module._session_stats(plain) == (FULL_COUNT, FULL_MAX_SEQ, "COMPLETED")
    # 5 + 2 pairs: 9 messages, the highest CSV sequence is 9 (no +1 shift, nothing synthesised)
    assert uploads_service_module._session_stats(longer) == (9, 9, "COMPLETED")
    assert plain["data"]["messages"][0]["messageAt"] is None  # the opening row is message 1 of the count


def test_new_shape_nothing_stored_is_new(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001") + _session_new_shape(SESS_B, "1002")))
    status, meta = h.final
    assert status == "success"
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (2, 0, 0)
    assert meta["rows_read"] == 10  # 2 sessions x (opening row + 4 rows)
    h.state.assert_awaited_once_with("mitra", ["1001", "1002"])


def test_new_shape_reupload_of_an_unchanged_session_is_unchanged(monkeypatch):
    h = BgHarness(monkeypatch)
    # stored as the consumer wrote it from the first upload: opening row counted, max sequence 5
    h.state.return_value = {**_stored("1001", SESS_A), **_stored("1002", SESS_B)}
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001") + _session_new_shape(SESS_B, "1002")))
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 0 and meta["sessions_skipped"] == 0
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 0, 2)
    h.push.assert_not_called()
    h.dlq.assert_not_called()


def test_new_shape_one_unchanged_one_new_session(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A)
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001") + _session_new_shape(SESS_B, "1002")))
    _, meta = h.final
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (1, 0, 1)
    assert [e["submissionId"] for e in _published(h)] == ["1002"]


def test_new_shape_session_that_gained_messages_is_updated_and_published_in_full(monkeypatch):
    h = BgHarness(monkeypatch)
    # stored while the chat was running: the opening row + 2 real messages (3 messages, max sequence 3)
    h.state.return_value = _stored("1001", SESS_A, count=3, max_seq=3, status="IN_PROGRESS")
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001")))
    status, meta = h.final
    assert status == "success"
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 1, 0)
    (event,) = _published(h)
    messages = event["data"]["messages"]
    assert [m["sequence"] for m in messages] == [1, 2, 3, 4, 5]  # the opening row and everything after it
    assert messages[0]["messageAt"] is None and messages[0]["parentSequence"] is None
    sequences = {m["sequence"] for m in messages}
    assert all(m["parentSequence"] in sequences for m in messages if m["parentSequence"] is not None)
    assert event["data"]["completionStatus"] == "COMPLETED"


def test_new_shape_session_that_grew_after_it_was_complete_is_updated(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A)  # 5 messages stored
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001", extra_rows=1)))  # the file has 7
    _, meta = h.final
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 1, 0)
    (event,) = _published(h)
    assert len(event["data"]["messages"]) == 7


def test_new_shape_status_flip_publishes_and_stale_shorter_file_is_unchanged(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", SESS_A, status="IN_PROGRESS")
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001", status="COMPLETED")))
    assert h.final[1]["sessions_updated"] == 1 and len(_published(h)) == 1

    # an older, shorter export of a session whose longer state is stored: nothing to add
    h2 = BgHarness(monkeypatch)
    h2.state.return_value = _stored("1001", SESS_A, count=7, max_seq=7, status="COMPLETED")
    h2.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001", status="IN_PROGRESS")))
    _, meta = h2.final
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 0, 1)
    h2.push.assert_not_called()


def test_new_shape_session_id_conflict_is_a_session_error(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = _stored("1001", "f" * 32)
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001") + _session_new_shape(SESS_B, "1002")))
    _, meta = h.final
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1
    (entry,) = meta["schema_validation_errors"]
    assert entry["submissionId"] == "1001" and "different session_id" in entry["problems"][0]


def test_new_shape_session_of_only_the_opening_row_is_skipped_and_not_looked_up(monkeypatch):
    h = BgHarness(monkeypatch)
    only_opening = _session_new_shape("c" * 32, "1003")[:1]
    h.run(file_bytes=_csv(_session_new_shape(SESS_A, "1001") + only_opening))
    status, meta = h.final
    assert status == "partial"   # CHANGED: an upload that skipped sessions ends 'partial'
    h.state.assert_awaited_once_with("mitra", ["1001"])  # the opening-row-only session never reaches the lookup
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1 and meta["sessions_new"] == 1
    (entry,) = meta["schema_validation_errors"]
    assert entry["problems"] == ["the session has only the opening row (no real message)"]


def test_old_and_new_shape_sessions_of_one_upload_are_classified_side_by_side(monkeypatch):
    h = BgHarness(monkeypatch)
    h.state.return_value = {**_stored("1001", SESS_A), **_stored("1002", SESS_B, count=3, max_seq=3)}
    h.run(file_bytes=_csv(_session(SESS_A, "1001") + _session_new_shape(SESS_B, "1002")))
    _, meta = h.final
    assert (meta["sessions_new"], meta["sessions_updated"], meta["sessions_unchanged"]) == (0, 1, 1)
    assert [e["submissionId"] for e in _published(h)] == ["1002"]


def test_classify_mitra_events_pure_function():
    def event(sub, sess, n, status):
        return {"submissionId": sub, "sessionId": sess,
                "data": {"completionStatus": status, "messages": [{"sequence": i + 1} for i in range(n)]}}

    state = {
        "same": {"session_id": "s", "max_sequence": 3, "message_count": 3, "completion_status": "IN_PROGRESS"},
        "grown": {"session_id": "s", "max_sequence": 2, "message_count": 2, "completion_status": "IN_PROGRESS"},
        "flip": {"session_id": "s", "max_sequence": 3, "message_count": 3, "completion_status": "IN_PROGRESS"},
        "clash": {"session_id": "other", "max_sequence": 3, "message_count": 3, "completion_status": "IN_PROGRESS"},
    }
    events = [event("new", "s", 3, "IN_PROGRESS"), event("same", "s", 3, "IN_PROGRESS"),
              event("grown", "s", 3, "IN_PROGRESS"), event("flip", "s", 3, "COMPLETED"),
              event("clash", "s", 3, "IN_PROGRESS")]
    to_publish, unchanged, conflicts = uploads_service_module._classify_mitra_events(events, state)
    assert [(e["submissionId"], kind) for e, kind in to_publish] == [("new", "new"), ("grown", "updated"), ("flip", "updated")]
    assert unchanged == 1
    assert [c["submissionId"] for c in conflicts] == ["clash"]


# --------------------------------------------------------------------------------------------------
# GOOGLE FORMS delta (real adapter, real event schema check; storage lookups faked)
# --------------------------------------------------------------------------------------------------
def _gforms_harness(monkeypatch) -> BgHarness:
    h = BgHarness(monkeypatch, record=_record({"tenant_code": "mitra", "source": "google_forms"}))
    h.config.return_value = GFORMS_ACTIVE_CONFIG
    return h


def _refs_of(file_bytes: bytes) -> List[str]:
    df = feedback_gforms.parse_gforms_csv(file_bytes)
    responses, _, _ = feedback_gforms.prepare_gforms_responses(df, GFORMS_ACTIVE_CONFIG["config"])
    return [r["source_ref"] for r in responses]


def test_gforms_all_new_responses_published_with_report(monkeypatch):
    h = _gforms_harness(monkeypatch)
    file_bytes = _gforms_csv(responses=3)
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "success", meta
    assert meta["rows_pushed"] == 3 and meta["sessions_skipped"] == 0
    assert (meta["responses_new"], meta["responses_already_stored"]) == (3, 0)
    assert (meta["rows_read"], meta["collapsed_duplicates"]) == (3, 0)
    assert meta["config_version_id"] == GFORMS_ACTIVE_CONFIG["config_version_id"]
    assert "sessions_new" not in meta  # the Mitra report keys are not used for Google Forms

    h.config.assert_awaited_once_with(PERSONAS["Youth"]["program_id"], "google_forms", tenant_code="mitra")
    h.state.assert_not_called()
    (call,) = h.refs.await_args_list
    assert call.args[0] == "mitra" and sorted(call.args[1]) == sorted(_refs_of(file_bytes))

    events = _published(h)
    assert len(events) == 3
    refs = set()
    for event in events:
        assert event["submissionType"] == "feedback" and event["data"]["source"] == "google_forms"
        assert event["submissionId"] == event["sessionId"]
        uuid.UUID(event["submissionId"])
        refs.add(event["data"]["sourceRef"])
        assert event["data"]["sessionType"] == "Youth_Voice" and event["data"]["questionnaireVersion"] == "V1"
    assert refs == set(_refs_of(file_bytes))
    assert len({e["submissionId"] for e in events}) == 3  # a fresh id per response
    h.dlq.assert_not_called()


def test_gforms_known_refs_are_skipped_only_new_events_pushed(monkeypatch):
    h = _gforms_harness(monkeypatch)
    file_bytes = _gforms_csv(responses=3)
    refs = _refs_of(file_bytes)
    h.refs.return_value = {refs[0], refs[2]}
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1
    assert (meta["responses_new"], meta["responses_already_stored"]) == (1, 2)
    assert [e["data"]["sourceRef"] for e in _published(h)] == [refs[1]]


def test_gforms_reupload_of_the_same_file_publishes_nothing(monkeypatch):
    h = _gforms_harness(monkeypatch)
    file_bytes = _gforms_csv(responses=2)
    h.refs.return_value = set(_refs_of(file_bytes))
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 0 and (meta["responses_new"], meta["responses_already_stored"]) == (0, 2)
    h.push.assert_not_called()
    h.dlq.assert_not_called()


def test_gforms_cumulative_superset_adds_only_the_new_response(monkeypatch):
    h = _gforms_harness(monkeypatch)
    older = _gforms_csv(responses=2)
    h.refs.return_value = set(_refs_of(older))
    h.run(file_bytes=_gforms_csv(responses=3))  # the same sheet, one response longer
    _, meta = h.final
    assert (meta["rows_read"], meta["responses_new"], meta["responses_already_stored"]) == (3, 1, 2)
    assert len(_published(h)) == 1


def test_gforms_collapsed_duplicates_are_reported(monkeypatch):
    h = _gforms_harness(monkeypatch)
    lines = _gforms_csv(responses=2).decode().splitlines()
    double_submit = "\n".join(lines + [lines[2]]) + "\n"  # response 2 submitted twice in the same second
    h.run(file_bytes=double_submit.encode())
    status, meta = h.final
    assert status == "success"
    assert (meta["rows_read"], meta["collapsed_duplicates"], meta["responses_new"]) == (3, 1, 2)
    assert meta["rows_pushed"] == 2


def test_gforms_skipped_responses_go_to_dlq_with_row_numbers_status_still_success(monkeypatch):
    h = _gforms_harness(monkeypatch)
    headers = _gforms_headers("Youth")
    good = ["10/2/2026 10:00:01"] + ["fine"] * len(headers)
    blank_timestamp = [""] + ["x"] * len(headers)
    no_answers = ["10/2/2026 10:00:03"] + [""] * len(headers)
    lines = [",".join(["Timestamp"] + [f'"{c}"' for c in headers])]
    lines += [",".join(f'"{c}"' for c in r) for r in (good, blank_timestamp, no_answers)]
    h.run(file_bytes=("\n".join(lines) + "\n").encode())

    status, meta = h.final
    assert status == "partial", meta   # CHANGED: an upload that skipped sessions ends 'partial'
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 2 and meta["sessions_dead_lettered"] == 2
    assert meta["responses_new"] == 1 and meta["rows_read"] == 3
    errors = {e["row"]: e for e in meta["schema_validation_errors"]}
    assert set(errors) == {2, 3}  # source data rows 2 and 3 (1-based)
    assert all(e["problems"] and e["submissionId"] is None and e["sessionId"] is None for e in errors.values())

    (messages,) = h.dlq.call_args.args
    assert len(messages) == 2
    by_row = {json.loads(value)["row"]: (json.loads(value), headers_) for value, headers_ in messages}
    assert set(by_row) == {2, 3}
    value, dlq_headers = by_row[2]
    assert value["submissionType"] == "feedback" and value["source"] == "google_forms" and value["csvUploadId"] == 7
    assert value["submissionId"] is None and value["sessionId"] is None
    assert "(row 2)" in dlq_headers["reason"] and dlq_headers["tenantCode"] == "mitra"
    assert "fine" not in json.dumps(value)  # no answer text in the DLQ stub
    assert len(_published(h)) == 1


def test_gforms_schema_failed_event_is_skipped_dead_lettered_with_source_ref(monkeypatch):
    h = _gforms_harness(monkeypatch)
    file_bytes = _gforms_csv(responses=2)
    refs = _refs_of(file_bytes)
    real = uploads_service_module.validate_ingestion_schema

    def fail_first(event, report_type, event_type, feedback_schema):
        assert (report_type, event_type) == ("feedback", "create")
        return (["bad event"] if event["data"]["sourceRef"] == refs[0]
                else real(event, report_type, event_type, feedback_schema=feedback_schema))

    monkeypatch.setattr(uploads_service_module, "validate_ingestion_schema", fail_first)
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "partial"   # CHANGED: an upload that skipped sessions ends 'partial'
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1 and meta["responses_new"] == 1
    (entry,) = meta["schema_validation_errors"]
    assert entry["sourceRef"] == refs[0] and entry["problems"] == ["bad event"]
    uuid.UUID(entry["submissionId"])
    (messages,) = h.dlq.call_args.args
    assert json.loads(messages[0][0])["sourceRef"] == refs[0]


def test_gforms_known_refs_lookup_failure_puts_upload_on_hold_nothing_published(monkeypatch):
    h = _gforms_harness(monkeypatch)
    h.refs.side_effect = RuntimeError("db connection lost")
    h.run(file_bytes=_gforms_csv())
    status, meta = h.final
    assert status == "on_hold"
    assert meta["stage"] == "Feedback Delta Lookup" and "db connection lost" in meta["exception"]
    h.push.assert_not_called()
    h.dlq.assert_not_called()


def test_gforms_kafka_failure_marks_on_hold_and_skips_dlq(monkeypatch):
    h = _gforms_harness(monkeypatch)
    h.push.side_effect = RuntimeError("broker down")
    h.run(file_bytes=_gforms_csv())
    status, meta = h.final
    assert status == "on_hold" and meta["stage"] == "Kafka Publishing"
    h.dlq.assert_not_called()


def test_gforms_background_rechecks_the_stored_file_against_the_persona_config(monkeypatch):
    h = _gforms_harness(monkeypatch)
    h.fetch.return_value = _gforms_csv("Parent")  # a Parents form stored under the Youth program
    h.run()
    status, meta = h.final
    assert status == "on_hold" and meta["stage"] == "Feedback Config Resolution"
    assert any("Unmapped" in e for e in meta["validation_errors"])
    h.push.assert_not_called()
    h.refs.assert_not_called()


def test_gforms_background_loads_wide_file_from_storage_without_losing_na_answers(monkeypatch):
    h = _gforms_harness(monkeypatch)
    headers = _gforms_headers("Youth")
    row = ["10/2/2026 10:00:01", "NA", "None"] + ["fine"] * (len(headers) - 2)
    file_bytes = (",".join(["Timestamp"] + [f'"{c}"' for c in headers]) + "\n" + ",".join(row) + "\n").encode()
    h.fetch.return_value = file_bytes
    h.run()
    assert h.final[0] == "success" and h.final[1]["rows_pushed"] == 1
    (event,) = _published(h)
    answers = [m["message"] for m in event["data"]["messages"]]
    assert "NA" in answers and "None" in answers  # not turned into NaN


def test_story_and_discussion_never_touch_the_delta_lookups(monkeypatch):
    from tests.conftest import install_fake_db
    import app.database.operations as operations_module
    state, refs = AsyncMock(), AsyncMock()
    monkeypatch.setattr(feedback_ops, "get_mitra_session_state", state)
    monkeypatch.setattr(feedback_ops, "get_known_source_refs", refs)
    conn = install_fake_db(monkeypatch, uploads_service_module)
    conn.fetchrow.return_value = None
    record = {"id": 1, "report_type": "story", "cloud_storage_path": "p.csv", "leader_category": "L",
              "program_name": "P", "meta_data": {"tenant_code": "mitra"}}
    monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
    monkeypatch.setattr(operations_module, "update_status", AsyncMock())
    import pandas as pd
    monkeypatch.setattr(uploads_service_module, "fetch_csv", MagicMock(return_value=b"raw"))
    monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=pd.DataFrame([{"id": "1"}])))
    monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(False, ["x"])))
    asyncio.run(uploads_service_module.process_csv_inline(1))
    state.assert_not_called()
    refs.assert_not_called()


# --------------------------------------------------------------------------------------------------
# get_known_source_refs / get_mitra_session_state: query shape with a fake connection
# --------------------------------------------------------------------------------------------------
def test_known_refs_and_session_state_fake_connection_chunking(monkeypatch):
    from tests.conftest import FakeConn, FakePool
    from app.database.db import db

    conn = FakeConn()

    async def run():
        # db.pool is per event loop, so the fake is installed from inside the running loop
        monkeypatch.setattr(db, "pool", FakePool(conn), raising=False)
        monkeypatch.setattr(feedback_ops, "_LOOKUP_CHUNK_SIZE", 2)
        conn.fetch.side_effect = [[{"source_ref": "r1"}], [{"source_ref": "r3"}]]
        got = await feedback_ops.get_known_source_refs("mitra", ["r3", "r1", "r2", "r1"])
        assert got == {"r1", "r3"}
        assert conn.fetch.await_count == 2  # 3 distinct refs, chunks of 2
        assert conn.fetch.await_args_list[0].args[1:] == ("mitra", ["r1", "r2"])
        assert "= ANY($2::text[])" in conn.fetch.await_args_list[0].args[0]

        assert await feedback_ops.get_known_source_refs("mitra", []) == set()
        assert await feedback_ops.get_mitra_session_state("mitra", []) == {}
        assert conn.fetch.await_count == 2  # empty input never queries

        conn.fetch.reset_mock()
        conn.fetch.side_effect = [[{"submission_id": "1", "session_id": "s", "completion_status": "COMPLETED",
                                    "message_count": 5, "max_sequence": 5}]]
        assert await feedback_ops.get_mitra_session_state("mitra", ["1", "2"]) == {
            "1": {"session_id": "s", "max_sequence": 5, "message_count": 5, "completion_status": "COMPLETED"}}

    asyncio.run(run())


# --------------------------------------------------------------------------------------------------
# REAL POSTGRES (opt-in): get_known_source_refs / get_mitra_session_state
# --------------------------------------------------------------------------------------------------
DB_URL = pg_support.SERVER_URL


@pytest.mark.skipif(DB_URL is None, reason="FEEDBACK_TEST_DATABASE_URL not set (empty scratch DB with schema.sql applied)")
def test_real_db_known_source_refs_and_mitra_session_state(monkeypatch):
    import asyncpg
    from app.database.db import db

    program_id = PERSONAS["Youth"]["program_id"]

    async def insert_session(conn, tenant, sub, sess, status, source, source_ref, sequences):
        await conn.execute(
            "INSERT INTO submissions (session_id, submission_id, tenant_code, submission_type, status) "
            "VALUES ($1, $2, $3, 'feedback', 'success')", sess, sub, tenant)
        await conn.execute(
            "INSERT INTO feedback_submissions (submission_id, tenant_code, source, session_type, questionnaire_version, "
            "completion_status, source_ref) VALUES ($1, $2, $3, 'Youth_Voice', 'V1', $4, $5)",
            sub, tenant, source, status, source_ref)
        for seq in sequences:
            await conn.execute(
                "INSERT INTO feedbacks (submission_id, tenant_code, session_id, source, sequence, message) "
                "VALUES ($1, $2, $3, $4, $5, 'synthetic')", sub, tenant, sess, source, seq)

    async def run():
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
        monkeypatch.setattr(db, "pool", pool, raising=False)
        created = []
        try:
            async with pool.acquire() as conn:
                await ensure_schema(conn)
                await conn.execute(
                    "INSERT INTO tenant(name, code) VALUES ('Mitra', 'mitra'), ('Other', 'other') ON CONFLICT DO NOTHING")
                # Mitra sessions: 7001 complete with 5 rows, 7002 in progress with 3 rows, 7003 stored without rows
                await insert_session(conn, "mitra", "7001", "s" * 31 + "1", "COMPLETED", "mitra", None, [1, 2, 3, 4, 5])
                await insert_session(conn, "mitra", "7002", "s" * 31 + "2", "IN_PROGRESS", "mitra", None, [1, 2, 3])
                await insert_session(conn, "mitra", "7003", "s" * 31 + "3", "IN_PROGRESS", "mitra", None, [])
                # the same chat-session id under another tenant must never be returned for 'mitra'
                await insert_session(conn, "other", "7001", "o" * 31 + "1", "IN_PROGRESS", "mitra", None, [1])
                # Google Forms responses
                gf1, gf2 = str(uuid.uuid4()), str(uuid.uuid4())
                await insert_session(conn, "mitra", gf1, gf1, "COMPLETED", "google_forms", "2026-10-02T04:39:20Z:aaa", [1, 2])
                await insert_session(conn, "mitra", gf2, gf2, "COMPLETED", "google_forms", "2026-10-02T04:39:21Z:bbb", [1, 2])
                await insert_session(conn, "other", gf1 + "x", gf1 + "x", "COMPLETED", "google_forms", "2026-10-02T04:39:20Z:ccc", [1, 2])
                created = [("mitra", "7001"), ("mitra", "7002"), ("mitra", "7003"), ("other", "7001"),
                           ("mitra", gf1), ("mitra", gf2), ("other", gf1 + "x")]

            state = await feedback_ops.get_mitra_session_state("mitra", ["7001", "7002", "7003", "7004", "7001"])
            assert state == {
                "7001": {"session_id": "s" * 31 + "1", "max_sequence": 5, "message_count": 5, "completion_status": "COMPLETED"},
                "7002": {"session_id": "s" * 31 + "2", "max_sequence": 3, "message_count": 3, "completion_status": "IN_PROGRESS"},
                "7003": {"session_id": "s" * 31 + "3", "max_sequence": 0, "message_count": 0, "completion_status": "IN_PROGRESS"},
            }  # 7004 is not stored: absent
            assert isinstance(state["7001"]["message_count"], int) and isinstance(state["7001"]["max_sequence"], int)
            other = await feedback_ops.get_mitra_session_state("other", ["7001", "7002"])
            assert list(other) == ["7001"] and other["7001"]["session_id"] == "o" * 31 + "1" and other["7001"]["message_count"] == 1
            assert await feedback_ops.get_mitra_session_state("mitra", []) == {}

            refs = ["2026-10-02T04:39:20Z:aaa", "2026-10-02T04:39:21Z:bbb", "2026-10-02T04:39:22Z:zzz", "2026-10-02T04:39:20Z:ccc"]
            assert await feedback_ops.get_known_source_refs("mitra", refs) == {refs[0], refs[1]}  # ccc is tenant 'other'
            assert await feedback_ops.get_known_source_refs("other", refs) == {refs[3]}
            assert await feedback_ops.get_known_source_refs("mitra", []) == set()

            # chunking: more refs than one chunk still finds the stored ones
            monkeypatch.setattr(feedback_ops, "_LOOKUP_CHUNK_SIZE", 2)
            many = [f"missing-{i}" for i in range(7)] + refs[:2]
            assert await feedback_ops.get_known_source_refs("mitra", many) == {refs[0], refs[1]}
            many_ids = [f"none-{i}" for i in range(5)] + ["7001", "7002"]
            assert set(await feedback_ops.get_mitra_session_state("mitra", many_ids)) == {"7001", "7002"}
        finally:
            async with pool.acquire() as conn:
                for tenant, sub in created:  # feedback_submissions / feedbacks cascade from submissions
                    await conn.execute("DELETE FROM submissions WHERE tenant_code = $1 AND submission_id = $2", tenant, sub)
                await conn.execute("DELETE FROM tenant WHERE code = 'other'")
            await pool.close()

    asyncio.run(run())
