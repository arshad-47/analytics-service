"""
Platform tests: `feedback` is a GENERIC report type. A source is a free label; the upload config's `adapter` (not the
source name) decides how a file is read; the CSV column names (`column_map`) and the status vocabulary
(`completion_map`) come from the config. A second program that is NOT 1 Million Voices is proven with a fictitious
program 'ptm': tenant 'abc', source 'ptm_app', persona session type 'Teacher_Voice', its own headers. Synthetic data
only; nothing here touches a database, Kafka or the network.
"""
import copy
import csv
import io
import json
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from app.api.exceptions import InvalidFeedbackFile
from app.api.services import feedback_gforms, uploads as uploads_service_module
from app.api.services.feedback_gforms import build_gforms_events, normalise_header, prepare_gforms_responses
from app.api.services.feedback_uploads import (
    build_feedback_events,
    check_feedback_file,
    parse_feedback_csv,
    report_unexpected_stages,
)
from app.database import feedback_operations as feedback_ops
from app.services.feedback_adapter_config import (
    COLUMN_ROLES,
    COLUMN_ROLES_OPTIONAL,
    COLUMN_ROLES_REQUIRED,
    DEFAULT_COLUMN_MAP,
    is_safe_code,
    is_safe_id,
    is_safe_source_label,
    resolve_column_map,
    resolve_completion_map,
    shown,
    validate_adapter,
    validate_adapter_config,
    validate_column_map,
    validate_completion_map,
)
from app.services.feedback_constants import (
    ADAPTER_CONVERSATION_ROWS,
    ADAPTER_WIDE_FORM,
    ADAPTERS,
    FEEDBACK_REPORT_TYPE,
    REQUIRED_FEEDBACK_CONFIG_KEYS,
)
from app.services.ingestion_validation import validate_ingestion_schema
from seed_support import PERSONAS, gforms_config_v1, mitra_config_v1
from tests.test_feedback_upload_api import (
    _assert_nothing_stored,
    _client,
    _files,
    _form,
    _headers,
    _patch_upload_pipeline,
)

PTM_PROGRAM = {"id": "11111111-2222-4333-8444-555555555555", "name": "PTM"}
PTM_LEADER = {"id": "66666666-7777-4888-8999-000000000000", "name": "Teacher"}
PTM_TENANT = "abc"
PTM_SOURCE = "ptm_app"
PTM_TYPE = "Teacher_Voice"
PUBLISHED = "2026-10-20T00:00:00.000Z"

# the PTM export has NONE of the Mitra column names
PTM_HEADERS = {
    "session_type": "persona", "session_status": "state", "session": "meeting_session", "submission_id": "meeting_id",
    "language": "lang", "sequence": "turn_no", "message": "utterance", "created_at": "spoken_at", "stage": "topic",
    "sender_id": "speaker_role", "bot_id": "bot_no",    # the bot id is required: a session's version is its rank
}
PTM_COLUMN_MAP = dict(PTM_HEADERS)
PTM_COLUMN_MAP["translated_message"] = "utterance_en"


def ptm_config(**overrides) -> dict:
    """An upload config of the fictitious PTM program: built from the seeded conversation_rows config, then given its
    own persona, column names, status words, bot sender and question catalog (all data, no code)."""
    config = copy.deepcopy(mitra_config_v1("Youth"))
    config.update({
        "session_type": PTM_TYPE,
        "column_map": dict(PTM_COLUMN_MAP),
        "completion_map": {"DONE": "COMPLETED", "OPEN": "IN_PROGRESS"},
        "bot_sender_ids": ["assistant"],
        "opening_row": {"mode": "none"},
        "sequence_offset": 1,
        "closing_keys": ["WRAP_UP"],
        "questions": [{"key": "ATTENDEE", "text": "Who is attending the meeting?"}],
        "message_map": {"bot": {"message": "translated_message", "translated_message": "message"},
                        "respondent": {"message": "message", "translated_message": "translated_message"}},
    })
    config.update(overrides)
    return config


PTM_COLUMNS = ["meeting_id", "meeting_session", "persona", "state", "lang", "turn_no", "utterance", "utterance_en",
               "spoken_at", "topic", "speaker_role", "venue", "bot_no"]


def ptm_rows(meeting="m1", session="s1", status="DONE", persona=PTM_TYPE, count=3):
    rows = []
    for n in range(1, count + 1):
        bot = n % 2 == 0
        rows.append({
            "meeting_id": meeting, "meeting_session": session, "persona": persona, "state": status, "lang": "en",
            "turn_no": str(n), "utterance": "question text" if bot else "answer text", "utterance_en": "",
            "spoken_at": f"2026-10-15T08:00:{n:02d}+05:30", "topic": "ATTENDEE" if n == 1 else "NEXT_Q",
            "speaker_role": "assistant" if bot else "teacher", "venue": "hall", "bot_no": "7",
        })
    return rows


def ptm_df(rows):
    return pd.DataFrame(rows, columns=PTM_COLUMNS).astype(str)


def ptm_csv(rows) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=PTM_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def build(df, config, **kw):
    kwargs = dict(program_info=PTM_PROGRAM, leader_info=PTM_LEADER, tenant_code=PTM_TENANT, source=PTM_SOURCE,
                  published_at=PUBLISHED)
    kwargs.update(kw)
    return build_feedback_events(df, config, **kwargs)


# =============================================================================================
# A6: a second program's CSV with different column names works with NO code change
# =============================================================================================

def test_ptm_file_with_its_own_headers_passes_the_file_check_and_builds_valid_events():
    config = ptm_config()
    df = parse_feedback_csv(ptm_csv(ptm_rows()))
    assert check_feedback_file(df, config, PTM_TYPE) == []
    events, skipped = build(df, config)
    assert skipped == [] and len(events) == 1
    event = events[0]
    assert (event["tenantCode"], event["submissionId"], event["sessionId"]) == ("abc", "m1", "s1")
    assert event["tags"]["source"] == event["data"]["source"] == "ptm_app"
    assert event["data"]["adapter"] == "conversation_rows" and event["data"]["sessionType"] == "Teacher_Voice"
    assert event["data"]["completionStatus"] == "COMPLETED"            # DONE -> COMPLETED through completion_map
    first = event["data"]["messages"][0]                                # the synthesised first question (catalog text)
    assert (first["sequence"], first["questionKey"], first["message"]) == (1, "ATTENDEE", "Who is attending the meeting?")
    assert [m["parentSequence"] for m in event["data"]["messages"]] == [None, 1, None, 3]
    # the event passes the platform's own validation under the PTM config's ingestion schema
    assert validate_ingestion_schema(event, FEEDBACK_REPORT_TYPE, "create", feedback_schema=config["ingestion_schema"]) == []


def test_ptm_headers_are_rejected_when_the_config_has_no_column_map_naming_them():
    config = ptm_config()
    del config["column_map"]                                           # the default map: the Mitra column names
    df = parse_feedback_csv(ptm_csv(ptm_rows()))
    (error,) = check_feedback_file(df, config, PTM_TYPE)
    assert error.startswith("Missing required column(s): session_type, session_status")
    with pytest.raises(ValueError, match="missing required column"):
        build(df, config)


def test_the_column_headers_are_matched_case_insensitively_and_stripped():
    config = ptm_config(column_map={**PTM_COLUMN_MAP, "message": "  UTTERANCE "})
    df = parse_feedback_csv(ptm_csv(ptm_rows()))
    assert check_feedback_file(df, config, PTM_TYPE) == []


def test_session_type_check_uses_the_mapped_column_and_counts_only():
    config = ptm_config()
    df = ptm_df(ptm_rows(persona="Parent_Voice"))
    (error,) = check_feedback_file(df, config, PTM_TYPE)
    assert "3 row(s) carry a session_type other than Teacher_Voice" in error and "Parent_Voice" not in error


def test_stage_report_uses_the_mapped_columns():
    config = ptm_config(expect_stages=["ATTENDEE"])
    report = report_unexpected_stages(ptm_df(ptm_rows()), config)
    assert report == {"sessions": 1, "sessions_with_unexpected_stages": 1, "unexpected_stage_count": 1}


def test_bot_id_column_can_be_mapped():
    config = ptm_config(column_map={**PTM_COLUMN_MAP, "bot_id": "venue"})
    rows = ptm_rows()
    for row in rows:
        row["venue"] = "12"   # a bot id is a positive integer written in digits
    events, _ = build(ptm_df(rows), config)
    assert events[0]["data"]["botId"] == "12"
    assert uploads_service_module._distinct_bot_ids(ptm_df(rows), config) == ["12"]


def test_default_column_map_is_the_role_names():
    assert DEFAULT_COLUMN_MAP == {role: role for role in COLUMN_ROLES}
    assert set(COLUMN_ROLES_REQUIRED) == {"session_type", "session_status", "session", "submission_id", "language",
                                          "sequence", "message", "created_at", "stage", "sender_id"}
    assert set(COLUMN_ROLES_OPTIONAL) == {"bot_id", "bot_name", "translated_message", "conversation_row_id",
                                          "receiver_id", "updated_at"}
    assert resolve_column_map({}) == DEFAULT_COLUMN_MAP


@pytest.mark.parametrize("bad, match", [
    ("x", "must be an object"),
    ({"sesion": "a"}, "unknown role"),
    ({"message": ""}, "non-empty"),
    ({"message": 5}, "non-empty"),
    ({"message": "stage"}, "resolve to the same column"),         # two roles on one header
    ({"message": "STAGE "}, "resolve to the same column"),
])
def test_column_map_is_validated(bad, match):
    with pytest.raises(ValueError, match=match):
        validate_column_map(bad)


# =============================================================================================
# A5: completion vocabulary
# =============================================================================================

def test_completion_map_translates_the_sources_own_words():
    config = ptm_config()
    events, skipped = build(ptm_df(ptm_rows(status="OPEN")), config)
    assert skipped == [] and events[0]["data"]["completionStatus"] == "IN_PROGRESS"
    events, _ = build(ptm_df(ptm_rows(status="DONE")), config)
    assert events[0]["data"]["completionStatus"] == "COMPLETED"


def test_a_status_word_outside_the_map_is_a_session_problem_without_echo():
    events, skipped = build(ptm_df(ptm_rows(status="WEIRDWORD")), ptm_config())
    assert events == []
    (problem,) = [p for p in skipped[0]["problems"] if "completion_map" in p]
    assert "WEIRDWORD" not in problem and "accepted: DONE, OPEN" in problem


def test_events_built_with_a_completion_map_are_accepted_by_ingestion_validation():
    """The reproduced defect: completion_values ['DONE','PARTIAL'] produced events that validation then rejected."""
    config = ptm_config(completion_map={"DONE": "COMPLETED", "PARTIAL": "IN_PROGRESS"})
    for word, expected in (("DONE", "COMPLETED"), ("PARTIAL", "IN_PROGRESS")):
        events, skipped = build(ptm_df(ptm_rows(status=word)), config)
        assert skipped == [] and events[0]["data"]["completionStatus"] == expected
        assert validate_ingestion_schema(events[0], FEEDBACK_REPORT_TYPE, "create",
                                         feedback_schema=config["ingestion_schema"]) == []


@pytest.mark.parametrize("bad", [{}, [], {"DONE": "FINISHED"}, {"": "COMPLETED"}, {" DONE": "COMPLETED"}, "COMPLETED"])
def test_completion_map_is_validated(bad):
    with pytest.raises(ValueError, match="completion_map"):
        validate_completion_map(bad)


def test_default_and_legacy_completion_vocabulary():
    assert resolve_completion_map({}) == {"COMPLETED": "COMPLETED", "IN_PROGRESS": "IN_PROGRESS"}
    # a config that predates completion_map keeps working: completion_values is read as an identity map
    assert resolve_completion_map({"completion_values": ["COMPLETED"]}) == {"COMPLETED": "COMPLETED"}
    # ... but a vocabulary the platform rejects is refused at config time, pointing at completion_map
    with pytest.raises(ValueError, match="completion_map"):
        resolve_completion_map({"completion_values": ["DONE", "PARTIAL"]})


def test_the_seed_carries_an_explicit_completion_map_and_no_completion_values():
    cfg = mitra_config_v1("Youth")
    assert cfg["completion_map"] == {"COMPLETED": "COMPLETED", "IN_PROGRESS": "IN_PROGRESS"}
    assert "completion_values" not in cfg


# =============================================================================================
# A3: the adapter is data; unknown or missing fails loudly
# =============================================================================================

def test_adapter_constants_and_required_key():
    assert ADAPTERS == ("conversation_rows", "wide_form")
    assert "adapter" in REQUIRED_FEEDBACK_CONFIG_KEYS


@pytest.mark.parametrize("value", [None, "", "mitra", "csv", 5, ["wide_form"], "WIDE_FORM"])
def test_unknown_adapter_is_rejected(value):
    with pytest.raises(ValueError, match="adapter"):
        validate_adapter(value)


def test_missing_adapter_is_rejected_with_a_hint():
    config = ptm_config()
    del config["adapter"]
    with pytest.raises(ValueError, match="missing 'adapter'"):
        validate_adapter_config(config)
    with pytest.raises(ValueError, match="adapter"):
        build(ptm_df(ptm_rows()), config)


def test_the_builders_refuse_a_config_of_the_other_adapter():
    with pytest.raises(ValueError, match="adapter must be 'conversation_rows'"):
        build(ptm_df(ptm_rows()), gforms_config_v1("Youth"))
    with pytest.raises(ValueError, match="adapter must be 'wide_form'"):
        feedback_gforms.match_gforms_form(pd.DataFrame({"Timestamp": ["x"]}), mitra_config_v1("Youth"))


def test_keys_of_the_other_adapter_are_rejected():
    rows = ptm_config()
    rows["forms"] = {}
    with pytest.raises(ValueError, match="only adapter wide_form reads: forms"):
        validate_adapter_config(rows)
    form = copy.deepcopy(gforms_config_v1("Youth"))
    form["column_map"] = {}
    with pytest.raises(ValueError, match="only adapter conversation_rows reads: column_map"):
        validate_adapter_config(form)


def test_wide_form_default_completion_can_only_be_completed():
    form = copy.deepcopy(gforms_config_v1("Youth"))
    assert validate_adapter_config(form) == "wide_form"
    form["default_completion"] = "IN_PROGRESS"
    with pytest.raises(ValueError, match="default_completion"):
        validate_adapter_config(form)


def test_the_seeded_blocks_name_their_adapter_and_carry_no_dead_keys():
    from seed_support import seed_config_blocks
    blocks = [b for b in seed_config_blocks() if b["config_type"] == "feedback_upload"]
    assert len(blocks) == 7
    for block in blocks:
        cfg = block["config"]
        expected = {"mitra": "conversation_rows", "google_forms": "wide_form"}[block["config_source"]]
        assert cfg["adapter"] == expected and validate_adapter_config(cfg) == expected
        for dead in ("id_strategy", "layout", "ignore_extra_columns", "columns"):
            assert dead not in cfg, (block["config_source"], dead)
        if expected == "conversation_rows":
            assert cfg["column_map"] == DEFAULT_COLUMN_MAP          # the explicit map of the 1MV export
            assert all("kind" not in q for q in cfg["questions"])


def test_source_names_do_not_select_the_adapter(monkeypatch):
    """A config that says conversation_rows under the source label 'google_forms' is read as conversation rows, and
    a wide_form config under the label 'mitra' as a form."""
    async def resolved(*a, **k):
        return {"program": PTM_PROGRAM, "leader": PTM_LEADER, "tenant_code": PTM_TENANT}

    def active(config):
        async def fn(program_id, source, **k):
            return {"config_id": "c", "config_version_id": "v", "version": 1, "session_type": config["session_type"],
                    "config": config}
        return fn

    monkeypatch.setattr(feedback_ops, "get_program_and_leader", resolved)
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config", active(ptm_config()))
    import asyncio
    target = asyncio.run(uploads_service_module.load_feedback_target("PTM", "Teacher", PTM_TENANT, "google_forms"))
    assert target["adapter"] == "conversation_rows"
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config", active(gforms_config_v1("Youth")))
    target = asyncio.run(uploads_service_module.load_feedback_target("PTM", "Teacher", PTM_TENANT, "mitra"))
    assert target["adapter"] == "wide_form"


@pytest.mark.parametrize("mutate, text", [
    (lambda c: c.pop("adapter"), "missing 'adapter'"),
    (lambda c: c.update(adapter="excel"), "adapter"),
])
def test_load_feedback_target_rejects_a_config_without_a_valid_adapter(monkeypatch, mutate, text):
    import asyncio
    config = ptm_config()
    mutate(config)

    async def resolved(*a, **k):
        return {"program": PTM_PROGRAM, "leader": PTM_LEADER, "tenant_code": PTM_TENANT}

    async def active(program_id, source, **k):
        return {"config_id": "c", "config_version_id": "v", "version": 1, "session_type": PTM_TYPE, "config": config}

    monkeypatch.setattr(feedback_ops, "get_program_and_leader", resolved)
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config", active)
    with pytest.raises(InvalidFeedbackFile) as raised:
        asyncio.run(uploads_service_module.load_feedback_target("PTM", "Teacher", PTM_TENANT, PTM_SOURCE))
    assert text in raised.value.errors[0]


def test_parse_dispatches_by_adapter_and_refuses_an_unknown_one():
    rows_bytes = ptm_csv(ptm_rows())
    assert list(uploads_service_module._parse_feedback_file(rows_bytes, "conversation_rows").columns) == PTM_COLUMNS
    with pytest.raises(ValueError, match="No CSV parser for adapter"):
        uploads_service_module._parse_feedback_file(rows_bytes, "excel")


# ---- end to end through the upload route: the whole PTM upload, no code change ----

def test_ptm_upload_goes_through_the_route_with_source_ptm_app_and_tenant_abc(monkeypatch):
    config = ptm_config()
    active = {"config_id": "c", "config_version_id": "11111111-1111-1111-1111-111111111111", "version": 1,
              "session_type": PTM_TYPE, "config": config}
    resolved = {"program": PTM_PROGRAM, "leader": PTM_LEADER, "tenant_code": PTM_TENANT}
    stores = _patch_upload_pipeline(monkeypatch, resolved=resolved, active=active)
    client = _client()
    resp = client.post("/v1/upload/", headers=_headers(),
                       data=_form(program_name="PTM", leader_category="Teacher", tenant_code="abc", source="ptm_app"),
                       files=_files(ptm_csv(ptm_rows()), "ptm.csv"))
    assert resp.status_code == 200, resp.text
    stores.lookup.assert_awaited_once_with("PTM", "Teacher", "abc")
    stores.config.assert_awaited_once_with(PTM_PROGRAM["id"], "ptm_app", tenant_code="abc")
    meta = stores.insert.await_args.kwargs["meta_data"]
    assert (meta["tenant_code"], meta["source"], meta["adapter"], meta["session_type"]) == (
        "abc", "ptm_app", "conversation_rows", "Teacher_Voice")


def test_a_wide_form_config_under_any_source_label_reads_a_form_export(monkeypatch):
    from tests.test_feedback_upload_api import GFORMS_ACTIVE_CONFIG, _gforms_csv
    resolved = {"program": PTM_PROGRAM, "leader": PTM_LEADER, "tenant_code": PTM_TENANT}
    stores = _patch_upload_pipeline(monkeypatch, resolved=resolved, active=GFORMS_ACTIVE_CONFIG)
    resp = _client().post("/v1/upload/", headers=_headers(),
                          data=_form(tenant_code="abc", source="ptm_form"), files=_files(_gforms_csv(), "form.csv"))
    assert resp.status_code == 200, resp.text
    meta = stores.insert.await_args.kwargs["meta_data"]
    assert (meta["source"], meta["adapter"]) == ("ptm_form", "wide_form")


# =============================================================================================
# A2: source is a free label
# =============================================================================================

@pytest.mark.parametrize("value, ok", [
    ("mitra", True), ("ptm_app", True), ("a", True), ("x" * 40, True), ("a1_b2", True),
    ("", False), ("x" * 41, False), ("Mitra", False), ("ptm-app", False), ("ptm app", False), ("any", False),
    ("a\nb", False), ("é", False), (None, False), (5, False),
])
def test_source_label_rule(value, ok):
    assert is_safe_source_label(value) is ok


def test_a_wide_form_event_of_a_new_source_gets_the_natural_key_rules_and_a_conversation_event_does_not():
    form = copy.deepcopy(gforms_config_v1("Youth"))
    form["session_type"] = PTM_TYPE
    df = pd.DataFrame({"Timestamp": ["10/2/2026 10:09:20"], "How old are you?": ["22"]})
    form["forms"] = {"f": {"name": "F", "questionnaire_version": "V1", "questions": [
        {"headers": ["How old are you?"], "question_key": "AGE", "text": "How old are you?", "kind": "metadata"}]}}
    responses, skipped, _ = prepare_gforms_responses(df, form)
    assert skipped == []
    events, _ = build_gforms_events(responses, known_refs=set(), program_info=PTM_PROGRAM, leader_info=PTM_LEADER,
                                    tenant_code=PTM_TENANT, source=PTM_SOURCE, published_at=PUBLISHED, config=form)
    event = events[0]
    assert event["data"]["source"] == event["tags"]["source"] == "ptm_app" and event["data"]["adapter"] == "wide_form"
    schema = form["ingestion_schema"]
    assert validate_ingestion_schema(event, FEEDBACK_REPORT_TYPE, "create", feedback_schema=schema) == []
    del event["data"]["sourceRef"]
    assert validate_ingestion_schema(event, FEEDBACK_REPORT_TYPE, "create", feedback_schema=schema) == [
        "'data.sourceRef' is missing"]


# =============================================================================================
# A15: header normalisation
# =============================================================================================

@pytest.mark.parametrize("header, expected", [
    ("Child’s name", "Child’s name"),
    ("Child’s class", "Child’s class"),
    ("Café rating", "Café rating"),
    ("Rating – overall", "Rating – overall"),
    ("How old are you? / ನಿಮ್ಮ ವಯಸ್ಸು ಎಷ್ಟು?", "How old are you?"),
    ("How old are you?\nನಿಮ್ಮ ವಯಸ್ಸು", "How old are you?"),
    ("बच्चे की कक्षा / Child class", "Child class"),
    ("  What is your   gender?  ", "What is your gender?"),
    ('Which state do you stay in? /"', "Which state do you stay in?"),
    ("", ""),
])
def test_normalise_header(header, expected):
    assert normalise_header(header) == expected


def test_childs_name_and_childs_class_no_longer_collapse():
    assert normalise_header("Child’s name") != normalise_header("Child’s class")


def test_every_seeded_and_fixture_header_normalises_exactly_as_before():
    """The old rule (cut at the first non-ASCII character) and the new one agree on every header of the seeds."""
    import re
    ws = re.compile(r"\s+")

    def old(h):
        text = ws.sub(" ", h).strip()
        for pos, ch in enumerate(text):
            if ord(ch) > 127:
                text = text[:pos]
                break
        return ws.sub(" ", text.rstrip(' /"”“')).strip()

    from seed_support import seed_config_blocks
    headers = set()
    for block in seed_config_blocks():
        for form in block["config"].get("forms", {}).values():
            for question in form["questions"]:
                headers.update(question["headers"])
        headers.update(block["config"].get("ignored_columns", []))
    assert len(headers) > 30
    assert [h for h in headers if old(h) != normalise_header(h)] == []


# =============================================================================================
# B8: no cell value in a problem
# =============================================================================================

HOSTILE = "my phone is 9876543210 and I live near Govt School"


def test_hostile_cell_values_never_appear_in_problems():
    config = ptm_config()
    rows = ptm_rows(count=4)
    rows[0]["spoken_at"] = HOSTILE                    # created_at
    rows[1]["turn_no"] = HOSTILE                      # sequence
    rows[2]["lang"] = HOSTILE                         # language (differs within the session)
    rows[3]["state"] = HOSTILE                        # status
    rows[3]["venue"] = HOSTILE
    events, skipped = build(ptm_df(rows), config)
    assert events == [] and len(skipped) == 1
    flat = json.dumps(skipped)
    assert "9876543210" not in flat and "Govt School" not in flat and "my phone" not in flat
    joined = " | ".join(skipped[0]["problems"])
    # CHANGED: a bad created_at on one row is a warning (messageAt null) and no longer a session problem
    assert "created_at" not in joined
    assert "sequence is not an integer in 1 row(s) (data row(s) 2)" in joined


def test_a_hostile_stage_and_ids_are_not_echoed():
    config = ptm_config()
    rows = ptm_rows()
    rows[0]["topic"] = HOSTILE
    events, skipped = build(ptm_df(rows), config)
    assert events == [] and HOSTILE not in json.dumps(skipped) and "9876543210" not in json.dumps(skipped)
    rows = ptm_rows(meeting=HOSTILE)
    events, skipped = build(ptm_df(rows), config)
    assert events == [] and skipped[0]["submissionId"] is None and skipped[0]["sessionId"] == "s1"
    assert HOSTILE not in json.dumps(skipped)


def test_the_dlq_messages_of_a_hostile_file_are_free_of_cell_values():
    rows = ptm_rows() + ptm_rows(meeting="m2", session="s2")
    rows[0]["spoken_at"] = HOSTILE
    rows[3]["meeting_id"] = HOSTILE
    _, skipped = build(ptm_df(rows), ptm_config())
    messages = uploads_service_module._build_dlq_messages(skipped, 7, PTM_TENANT, PTM_SOURCE)
    assert len(messages) >= 1
    for value, headers in messages:
        assert "9876543210" not in value and "Govt School" not in value
        assert all("9876543210" not in str(v) for v in headers.values())


def test_the_file_check_never_echoes_the_session_type_cells():
    df = ptm_df(ptm_rows(persona=HOSTILE))
    (error,) = check_feedback_file(df, ptm_config(), PTM_TYPE)
    assert "9876543210" not in error and "Govt" not in error


def test_the_stage_report_never_names_a_stage():
    rows = ptm_rows(count=3)
    rows[0]["topic"] = HOSTILE
    report = report_unexpected_stages(ptm_df(rows), ptm_config(expect_stages=["ATTENDEE"]))
    assert HOSTILE not in json.dumps(report) and "unexpected_stages" not in report
    assert report["unexpected_stage_count"] >= 1


# =============================================================================================
# B11: identifiers
# =============================================================================================

@pytest.mark.parametrize("value", ["900001\n2026-11-13 [ERROR] FORGED", "s\r\nFORGED", "has space", "x" * 129, "", "\x00a",
                                   "a b"])
def test_unsafe_ids_are_refused_by_the_builder_without_being_echoed(value):
    for field in ("meeting_id", "meeting_session"):
        rows = ptm_rows()
        for row in rows:
            row[field] = value
        events, skipped = build(ptm_df(rows), ptm_config())
        assert events == []
        flat = json.dumps(skipped)
        assert "FORGED" not in flat
        if value != "":
            assert skipped[0]["submissionId"] is None or skipped[0]["sessionId"] is None


@pytest.mark.parametrize("value, ok", [
    ("184884", True), ("a" * 32, True), ("0e9c8a77-1d2b-4c3d-8e4f-5a6b7c8d9e0f", True), ("a:b.c_d-e", True),
    ("x" * 128, True), ("x" * 129, False), ("a b", False), ("a\n", False), ("", False), (5, False),
])
def test_safe_id_rule(value, ok):
    assert is_safe_id(value) is ok


@pytest.mark.parametrize("value, ok", [("mitra", True), ("abc", True), ("Youth_Voice", True), ("a.b-c_d", True),
                                       ("x" * 64, True), ("x" * 65, False), ("a b", False), ("a\nb", False)])
def test_safe_code_rule(value, ok):
    assert is_safe_code(value) is ok


def test_shown_only_quotes_short_safe_tokens():
    assert shown("COMPLETED") == "'COMPLETED'" and shown(3) == "3" and shown(None) == "None"
    for hostile in (HOSTILE, "a\nb", "x" * 41, 2.5, {"a": 1}):
        assert shown(hostile) == "<value not shown>"


def test_step_retry_request_tenant_code_is_constrained():
    from pydantic import ValidationError
    from app.api.models.submissions import BulkRetryRequest, StepRetryRequest
    assert StepRetryRequest(session_id="s", submission_id="1", tenant_code="mitra").tenant_code == "mitra"
    assert BulkRetryRequest().tenant_code is None and BulkRetryRequest(tenant_code="abc").tenant_code == "abc"
    for bad in ("mitra\nFORGED", "", "a b", "x" * 65):
        with pytest.raises(ValidationError):
            StepRetryRequest(session_id="s", submission_id="1", tenant_code=bad)
        with pytest.raises(ValidationError):
            BulkRetryRequest(tenant_code=bad)


# =============================================================================================
# C7: dates and sequences at the boundary
# =============================================================================================

@pytest.mark.parametrize("stamp", ["0001-01-01T00:00:00+05:30", "9999-12-31T23:59:59-05:30", "0001-01-01 00:00:00"])
def test_an_out_of_range_created_at_skips_only_that_session(stamp):
    config = ptm_config()
    good = ptm_rows(meeting="good", session="sg")
    bad = ptm_rows(meeting="bad", session="sb")
    # CHANGED: every row of the bad session is out of range (one bad row alone only gets messageAt null now)
    for row in bad:
        row["spoken_at"] = stamp
    events, skipped = build(ptm_df(good + bad), config)       # must not raise OverflowError
    assert [e["submissionId"] for e in events] == ["good"]
    assert [s["submissionId"] for s in skipped] == ["bad"]
    assert "created_at is missing or unparsable in 3 row(s)" in skipped[0]["problems"][0]


def test_an_out_of_range_form_timestamp_skips_only_that_response():
    form = copy.deepcopy(gforms_config_v1("Youth"))
    form["datetime"] = {"formats": ["%Y-%m-%d %H:%M:%S"], "fallback_timezone": "Asia/Kolkata"}
    form["forms"] = {"f": {"name": "F", "questionnaire_version": "V1", "questions": [
        {"headers": ["How old are you?"], "question_key": "AGE", "text": "How old are you?", "kind": "metadata"}]}}
    df = pd.DataFrame({"Timestamp": ["0001-01-01 00:00:00", "2026-10-02 10:09:20"],
                       "How old are you?": ["22", "23"]})
    responses, skipped, _ = prepare_gforms_responses(df, form)
    assert len(responses) == 1 and len(skipped) == 1 and skipped[0]["row"] == 1


@pytest.mark.parametrize("sequence", ["2147483648", "5000000000", "9" * 30, "9" * 5000])
def test_a_sequence_above_int32_is_a_session_problem(sequence):
    rows = ptm_rows()
    rows[2]["turn_no"] = sequence
    events, skipped = build(ptm_df(rows), ptm_config())
    assert events == []
    assert any("is above 2147483647" in p for p in skipped[0]["problems"])
    assert sequence not in json.dumps(skipped)


def test_the_largest_sequence_is_fine_but_the_offset_cannot_push_it_over():
    rows = ptm_rows(count=1)
    rows[0]["turn_no"] = "1"
    assert build(ptm_df(rows), ptm_config())[1] == []
    rows = ptm_rows(count=2)
    rows[1]["turn_no"] = "2147483647"                          # + sequence_offset 1 would overflow
    events, skipped = build(ptm_df(rows), ptm_config())
    assert events == [] and any("sequence_offset" in p for p in skipped[0]["problems"])


# =============================================================================================
# A18: story / discussion keep their defaults
# =============================================================================================

def test_story_upload_without_tenant_code_still_defaults_to_mitra(monkeypatch):
    seen = {}

    async def fake_handle_upload(**kwargs):
        seen.update(kwargs)
        return {"message": "ok", "id": 1, "status": "pending"}

    monkeypatch.setattr(uploads_service_module, "handle_upload", fake_handle_upload)
    resp = _client().post("/v1/upload/", headers=_headers(),
                          data={"report_type": "story", "program_name": "P", "leader_category": "L"},
                          files={"file": ("s.csv", b"a,b\n1,2\n", "text/csv")})
    assert resp.status_code == 200, resp.text
    assert seen["tenant_code"] == "mitra" and seen["source"] is None


# =============================================================================================
# B7: auth and size are checked before the multipart body is read
# =============================================================================================

def test_unauthenticated_upload_is_rejected_before_the_body_is_parsed(monkeypatch):
    from starlette.requests import Request

    async def boom(self, *a, **k):
        raise AssertionError("the multipart body was parsed before auth")

    monkeypatch.setattr(Request, "form", boom)
    client = _client()
    big = b"a,b\n" + b"x" * 200_000
    for headers in ({}, {"Authorization": "Bearer wrong-token"}, {"Authorization": "Basic abc"}):
        resp = client.post("/v1/upload/", headers=headers, data=_form(), files=_files(big))
        assert resp.status_code in (401, 403), (headers, resp.status_code)


def test_an_oversize_content_length_is_413_before_the_body_is_parsed(monkeypatch):
    from starlette.requests import Request
    from app.api.routes import uploads as routes_module

    async def boom(self, *a, **k):
        raise AssertionError("the multipart body was parsed before the size ceiling")

    monkeypatch.setattr(routes_module, "MAX_UPLOAD_REQUEST_BYTES", 1000)
    monkeypatch.setattr(Request, "form", boom)
    resp = _client().post("/v1/upload/", headers=_headers(), data=_form(), files=_files(b"a,b\n" + b"x" * 5000))
    assert resp.status_code == 413


def test_a_body_that_outgrows_the_ceiling_without_content_length_is_cut_off(monkeypatch):
    from app.api.routes import uploads as routes_module
    monkeypatch.setattr(routes_module, "MAX_UPLOAD_REQUEST_BYTES", 1000)

    def chunks():
        for _ in range(20):
            yield b"x" * 500

    # a generator body is sent chunked (no Content-Length)
    resp = _client().post("/v1/upload/", headers={**_headers(), "Content-Type": "multipart/form-data; boundary=zz"},
                          content=chunks())
    assert resp.status_code == 413


def test_a_valid_upload_still_passes_the_preflight(monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    from tests.test_feedback_upload_api import GOOD_FILE
    resp = _client().post("/v1/upload/", headers=_headers(), data=_form(), files=_files(GOOD_FILE))
    assert resp.status_code == 200, resp.text
    stores.insert.assert_awaited_once()
