"""
Boundary validation and settings for the `feedback` report type (Phase 1, slice 1c).
See docs/feedback-upload-plan.md. Pure in-memory: no Postgres / Kafka / Temporal.
"""
import copy
import json
from pathlib import Path

import pytest

from app.config import Settings, settings
from app.services.feedback_constants import (
    COMPLETION_STATUSES,
    DEFAULT_INGESTION_SCHEMA,
)
from seed_support import mitra_config_v1
from app.services.ingestion_validation import validate_ingestion_schema

FIXTURE_ROOT = Path(__file__).resolve().parent / "kafka_events"


def _load(*parts):
    return json.loads(FIXTURE_ROOT.joinpath(*parts).read_text(encoding="utf-8"))


@pytest.fixture
def event():
    return _load("create", "create_feedback.json")


# The feedback schema is the persona config's `ingestion_schema` (passed in by the caller, no DB read here).
SCHEMA = mitra_config_v1("Youth")["ingestion_schema"]


def _problems(ev, event_type="create"):
    return validate_ingestion_schema(ev, "feedback", event_type, feedback_schema=SCHEMA)


def _msg(ev, index):
    return ev["data"]["messages"][index]


# ---------------------------------------------------------------- valid events

def test_valid_fixture_passes(event):
    assert _problems(event) == []


def test_in_progress_fixture_passes():
    assert _problems(_load("create", "create_feedback_in_progress.json")) == []


def test_delete_fixture_passes():
    assert _problems(_load("delete", "delete_feedback.json"), "delete") == []


def test_delete_missing_required_path_fails():
    ev = _load("delete", "delete_feedback.json")
    del ev["sessionId"]
    assert _problems(ev, "delete") == ["'sessionId' is missing"]


def test_state_and_district_are_optional(event):
    event["tags"].pop("state")
    event["tags"].pop("district")
    assert _problems(event) == []


def test_translated_message_and_message_at_optional(event):
    for row in event["data"]["messages"]:
        row["translatedMessage"] = None
        row["messageAt"] = None
    del _msg(event, 1)["translatedMessage"]
    del _msg(event, 1)["messageAt"]
    assert _problems(event) == []


def test_messages_need_not_be_sorted(event):
    event["data"]["messages"].reverse()
    assert _problems(event) == []


def test_is_follow_up_true_is_valid(event):
    _msg(event, 2)["isFollowUp"] = True
    assert _problems(event) == []


def test_is_follow_up_absent_is_valid(event):
    del _msg(event, 0)["isFollowUp"]
    assert _problems(event) == []


# ------------------------------------------------------------ envelope / paths

@pytest.mark.parametrize("path", [
    "submissionId", "sessionId", "tenantCode", "eventType", "eventPublishedAt",
    "tags.programId", "tags.leaderCategoryId",
    "data.source", "data.sessionType", "data.questionnaireVersion",
    "data.language", "data.completionStatus", "data.submissionDate", "data.messages",
])
def test_required_path_missing(event, path):
    parent = event
    *head, leaf = path.split(".")
    for segment in head:
        parent = parent[segment]
    del parent[leaf]
    assert f"'{path}' is missing" in _problems(event)


def test_required_path_null_and_empty(event):
    event["tags"]["programId"] = None
    event["data"]["language"] = ""
    problems = _problems(event)
    assert "'tags.programId' is null" in problems
    assert "'data.language' is empty" in problems


def test_empty_messages_reported_once(event):
    event["data"]["messages"] = []
    assert _problems(event) == ["'data.messages' is empty"]


def test_null_messages_reported_once(event):
    event["data"]["messages"] = None
    assert _problems(event) == ["'data.messages' is null"]


def test_messages_not_a_list(event):
    event["data"]["messages"] = "hello"
    assert _problems(event) == ["'data.messages' must be a list, got str"]


def test_submission_type_must_be_exactly_feedback(event):
    event["submissionType"] = "Feedback"
    assert _problems(event) == ["'submissionType' must be 'feedback', got 'Feedback'"]


# ---------------------------------------------------------- source / completion

@pytest.mark.parametrize("value", list(COMPLETION_STATUSES))
def test_completion_status_allowed(event, value):
    event["data"]["completionStatus"] = value
    assert _problems(event) == []


def test_completion_status_rejected(event):
    event["data"]["completionStatus"] = "DONE"
    assert _problems(event) == ["'data.completionStatus' must be one of COMPLETED, IN_PROGRESS, got 'DONE'"]


@pytest.mark.parametrize("label", ["mitra", "google_forms", "ptm_app", "a", "x" * 40])
def test_any_safe_source_label_is_allowed(event, label):
    # a source is a free label: whether it is usable is decided by its upload config (the consumer resolves that)
    event["data"]["source"] = label
    assert _problems(event) == []


@pytest.mark.parametrize("label", ["Mitra", "ptm app", "ptm-app", "x" * 41, "any", "mitra\nFORGED", "m\u00e9tra"])
def test_unsafe_source_label_is_rejected_without_echo(event, label):
    event["data"]["source"] = label
    (problem,) = _problems(event)
    assert problem.startswith("'data.source' is invalid: ")
    assert "FORGED" not in problem and (label == "any" or label not in problem)  # 'any' is named: it is reserved


# --------------------------------------------------------------- message rows

def test_message_row_not_a_dict(event):
    event["data"]["messages"][3] = "oops"
    assert "'data.messages[3]' must be an object, got str" in _problems(event)


def test_sequence_missing(event):
    del _msg(event, 0)["sequence"]
    assert "'data.messages[0].sequence' is missing" in _problems(event)


@pytest.mark.parametrize("bad", ["2", 2.0, True, None])
def test_sequence_must_be_int(event, bad):
    _msg(event, 5)["sequence"] = bad
    assert f"'data.messages[5].sequence' must be an integer, got {type(bad).__name__}" in _problems(event)


@pytest.mark.parametrize("bad", [0, -3])
def test_sequence_must_be_at_least_one(event, bad):
    _msg(event, 5)["sequence"] = bad
    assert f"'data.messages[5].sequence' must be >= 1, got {bad!r}" in _problems(event)


def test_sequence_above_int32_is_rejected(event):
    _msg(event, 5)["sequence"] = 2147483648
    assert "'data.messages[5].sequence' must be <= 2147483647 (a 32-bit integer)" in _problems(event)
    _msg(event, 5)["sequence"] = 2147483647
    assert not any("must be <=" in p for p in _problems(event))


def test_duplicate_sequence(event):
    _msg(event, 5)["sequence"] = 4
    assert "'data.messages' has duplicate sequence 4 (2 rows)" in _problems(event)


def test_message_missing_null_empty_and_non_string(event):
    del _msg(event, 0)["message"]
    _msg(event, 1)["message"] = None
    _msg(event, 2)["message"] = ""
    _msg(event, 3)["message"] = 42
    problems = _problems(event)
    assert "'data.messages[0].message' is missing" in problems
    assert "'data.messages[1].message' is null" in problems
    assert "'data.messages[2].message' is empty" in problems
    assert "'data.messages[3].message' must be a string, got int" in problems


def test_is_follow_up_must_be_bool(event):
    _msg(event, 2)["isFollowUp"] = "false"
    assert "'data.messages[2].isFollowUp' must be a boolean, got str" in _problems(event)


def test_parent_sequence_must_be_null_or_int(event):
    _msg(event, 1)["parentSequence"] = "1"
    assert "'data.messages[1].parentSequence' must be null or an integer, got str" in _problems(event)


def test_parent_sequence_bool_rejected(event):
    _msg(event, 1)["parentSequence"] = True
    assert "'data.messages[1].parentSequence' must be null or an integer, got bool" in _problems(event)


def test_optional_text_fields_wrong_type(event):
    _msg(event, 1)["translatedMessage"] = 5
    _msg(event, 1)["messageAt"] = 123
    problems = _problems(event)
    assert "'data.messages[1].translatedMessage' must be null or a string, got int" in problems
    assert "'data.messages[1].messageAt' must be null or a string, got int" in problems


# ------------------------------------------------------------- parent linkage

def test_parent_sequence_not_in_event(event):
    _msg(event, 1)["parentSequence"] = 99
    assert ("'data.messages[1].parentSequence' 99 does not match any message sequence in the event"
            in _problems(event))


def test_parent_sequence_must_be_lower(event):
    _msg(event, 1)["parentSequence"] = 3
    assert ("'data.messages[1].parentSequence' 3 must be lower than the row's own sequence 2"
            in _problems(event))


def test_parent_sequence_equal_to_own_is_rejected(event):
    _msg(event, 1)["parentSequence"] = 2
    assert ("'data.messages[1].parentSequence' 2 must be lower than the row's own sequence 2"
            in _problems(event))


def test_parent_check_independent_of_order(event):
    # a valid event stays valid when the rows are shuffled
    event["data"]["messages"] = [_msg(event, i) for i in (4, 1, 5, 0, 3, 2)]
    assert _problems(event) == []


def test_no_answer_in_event(event):
    for row in event["data"]["messages"]:
        row["parentSequence"] = None
    assert _problems(event) == [
        "'data.messages' has no message with a parentSequence (at least one answer is required)"
    ]


def test_tail_only_event_with_unknown_parent_is_rejected(event):
    # a delta event must carry the parent question too (the consumer sets parent_id from this event)
    event["data"]["messages"] = [_msg(event, 3)]
    assert ("'data.messages[0].parentSequence' 3 does not match any message sequence in the event"
            in _problems(event))


def test_all_problems_reported_not_just_first(event):
    event["data"]["completionStatus"] = "DONE"
    event["data"]["source"] = "BAD SOURCE"
    _msg(event, 1)["message"] = ""
    _msg(event, 4)["parentSequence"] = 99
    del event["tags"]["programId"]
    problems = _problems(event)
    assert len(problems) == 5, problems


# ------------------------------------------- session type / questionnaire version / bot

SESSION_FIELDS = ["sessionType", "questionnaireVersion"]


def test_fixtures_carry_session_type_and_version(event):
    assert event["data"]["sessionType"] == "Youth_Voice"
    assert event["data"]["questionnaireVersion"] == "V2"
    in_progress = _load("create", "create_feedback_in_progress.json")
    assert in_progress["data"]["sessionType"] == "Parent_Voice"
    assert in_progress["data"]["questionnaireVersion"] == "V1"


@pytest.mark.parametrize("field", SESSION_FIELDS)
def test_session_fields_missing(event, field):
    del event["data"][field]
    assert _problems(event) == [f"'data.{field}' is missing"]


@pytest.mark.parametrize("field", SESSION_FIELDS)
def test_session_fields_null(event, field):
    event["data"][field] = None
    assert _problems(event) == [f"'data.{field}' is null"]


@pytest.mark.parametrize("field", SESSION_FIELDS)
@pytest.mark.parametrize("blank", ["", "   "])
def test_session_fields_empty_or_blank(event, field, blank):
    event["data"][field] = blank
    assert _problems(event) == [f"'data.{field}' is empty"]


@pytest.mark.parametrize("field", SESSION_FIELDS)
@pytest.mark.parametrize("bad, type_name", [(2, "int"), (1.5, "float"), (True, "bool"), (["V1"], "list"),
                                            ({"k": "V1"}, "dict")])
def test_session_fields_wrong_type(event, field, bad, type_name):
    event["data"][field] = bad
    assert _problems(event) == [f"'data.{field}' must be a string, got {type_name}"]


def test_both_session_fields_missing_reported_together(event):
    del event["data"]["sessionType"]
    del event["data"]["questionnaireVersion"]
    assert _problems(event) == ["'data.sessionType' is missing", "'data.questionnaireVersion' is missing"]


@pytest.mark.parametrize("session_type, version", [
    ("Youth_Voice", "V1"), ("Parent_Voice", "V2"), ("General_Public_Voice", "V1"), ("Educator_Voice", "V1"),
])
def test_any_non_empty_strings_are_accepted(event, session_type, version):
    # the schema only checks shape; which values are legitimate is the upload config's business
    event["data"]["sessionType"] = session_type
    event["data"]["questionnaireVersion"] = version
    assert _problems(event) == []


@pytest.mark.parametrize("field", ["botId", "botName"])
def test_bot_fields_are_optional(event, field):
    del event["data"][field]
    assert _problems(event) == []


def test_bot_fields_absent_together(event):
    del event["data"]["botId"]
    del event["data"]["botName"]
    assert _problems(event) == []


@pytest.mark.parametrize("field", ["botId", "botName"])
@pytest.mark.parametrize("ok", [None, "101", "Mitra Youth Test Bot", ""])
def test_bot_fields_accept_string_or_null(event, field, ok):
    event["data"][field] = ok
    assert _problems(event) == []


@pytest.mark.parametrize("field", ["botId", "botName"])
@pytest.mark.parametrize("bad, type_name", [(101, "int"), (True, "bool"), (["a"], "list"), ({}, "dict")])
def test_bot_fields_wrong_type(event, field, bad, type_name):
    event["data"][field] = bad
    assert _problems(event) == [f"'data.{field}' must be a string or null, got {type_name}"]


def test_in_progress_fixture_has_null_bot_name_and_passes():
    ev = _load("create", "create_feedback_in_progress.json")
    assert ev["data"]["botName"] is None and ev["data"]["botId"] == "102"
    assert _problems(ev) == []


def test_session_fields_checked_for_both_sources(event, gforms):
    for ev in (event, gforms):
        ev["data"]["questionnaireVersion"] = 7
        assert _problems(ev) == ["'data.questionnaireVersion' must be a string, got int"]


def test_delete_event_needs_no_session_fields():
    ev = _load("delete", "delete_feedback.json")
    assert "data" not in ev
    assert _problems(ev, "delete") == []


# -------------------------------------------------------------- update / types

def test_feedback_update_not_supported(event):
    ev = copy.deepcopy(event)
    ev["eventType"] = "update"
    assert _problems(ev, "update") == ["feedback does not support update events in this release"]


def test_unknown_type_message_unchanged():
    assert validate_ingestion_schema({}, "survey", "create") == [
        "Unrecognized submissionType 'survey'; no ingestion schema to validate against"
    ]
    assert validate_ingestion_schema({}, "survey", "update") == [
        "Unrecognized submissionType 'survey'; no ingestion schema to validate against"
    ]


def test_non_string_type_is_unrecognized():
    assert validate_ingestion_schema({}, None, "create") == [
        "Unrecognized submissionType None; no ingestion schema to validate against"
    ]


@pytest.mark.parametrize("folder,name", [
    ("create", "create_story.json"),
    ("create", "create_discussion.json"),
    ("delete", "delete_story.json"),
    ("delete", "delete_discussion.json"),
])
def test_story_and_discussion_still_validate(folder, name):
    ev = _load(folder, name)
    assert validate_ingestion_schema(ev, ev["submissionType"], ev["eventType"]) == []


def test_story_still_reports_missing_field():
    ev = _load("create", "create_story.json")
    del ev["tags"]["state"]
    assert validate_ingestion_schema(ev, "story", "create") == ["'tags.state' is missing"]


def test_story_event_is_not_subject_to_feedback_checks():
    # feedback-only rules must not leak into story: no messages / completionStatus needed
    ev = _load("create", "create_story.json")
    assert "messages" not in ev["data"]
    assert validate_ingestion_schema(ev, "story", "create") == []


def test_story_update_still_supported():
    ev = _load("update", "update_story.json")
    assert validate_ingestion_schema(ev, "story", "update") == []


# -------------------------------------------------------------------- settings

def test_seeded_ingestion_schema_required_fields():
    schema = mitra_config_v1("Youth")["ingestion_schema"]
    assert schema == DEFAULT_INGESTION_SCHEMA
    assert schema["create"]["required"] == [
        "submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt",
        "tags.programId", "tags.leaderCategoryId",
        "data.source", "data.sessionType", "data.questionnaireVersion",
        "data.language", "data.completionStatus", "data.submissionDate", "data.messages",
    ]
    assert schema["create"]["optional"] == [
        "tags.state", "tags.district", "tags.programName", "tags.leaderCategoryName", "tags.source",
        "data.botId", "data.botName",
    ]
    assert "data.botId" not in schema["create"]["required"]
    assert "data.botName" not in schema["create"]["required"]
    assert "tags.state" not in schema["create"]["required"]
    assert "tags.district" not in schema["create"]["required"]
    assert schema["delete"]["required"] == [
        "submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt",
    ]


def test_seeded_schema_is_a_copy_per_config():
    first, second = mitra_config_v1("Youth"), mitra_config_v1("Parent")
    first["ingestion_schema"]["create"]["required"].append("data.extra")
    assert "data.extra" not in second["ingestion_schema"]["create"]["required"]
    assert "data.extra" not in DEFAULT_INGESTION_SCHEMA["create"]["required"]


def test_story_and_discussion_schemas_unchanged():
    assert "tags.state" in settings.get_kafka_ingestion_schema("story")["create"]["required"]
    assert "data.participantsData" in settings.get_kafka_ingestion_schema("discussion")["create"]["required"]


def test_unknown_schema_type_still_raises():
    with pytest.raises(ValueError, match="No Kafka ingestion schema defined for submission type 'survey'"):
        settings.get_kafka_ingestion_schema("survey")


def test_feedback_settings_are_not_in_env_settings():
    """The three feedback settings moved to the config tables: no Settings field, no env override."""
    for field in ("FEEDBACK_MAX_CSV_UPLOAD_BYTES", "FEEDBACK_KAFKA_SCHEMA", "PROCESS_CONFIG_FEEDBACK"):
        assert field not in Settings.model_fields
    assert Settings.model_fields["MAX_CSV_UPLOAD_BYTES"].default == 10485760  # story / discussion cap untouched


@pytest.mark.parametrize("call", [
    lambda: settings.get_kafka_ingestion_schema("feedback"),
    lambda: settings.get_process_config("feedback"),
])
def test_feedback_settings_lookup_raises_pointing_at_the_config_tables(call):
    with pytest.raises(ValueError, match="live in the config tables"):
        call()


def test_unknown_process_config_still_empty_and_story_unchanged():
    assert settings.get_process_config("survey") == []
    assert settings.get_process_config("story") != []
    assert settings.get_process_config("discussion") != []


def test_feedback_create_without_a_schema_is_a_problem_never_a_default(event):
    problems = validate_ingestion_schema(event, "feedback", "create")
    assert len(problems) == 1 and "No feedback ingestion schema was supplied" in problems[0]


def test_feedback_delete_without_a_schema_checks_the_envelope_only():
    ev = _load("delete", "delete_feedback.json")
    assert validate_ingestion_schema(ev, "feedback", "delete") == []
    del ev["tenantCode"]
    assert validate_ingestion_schema(ev, "feedback", "delete") == ["'tenantCode' is missing"]


def test_the_validator_uses_the_schema_it_is_given(event):
    """A persona may require more (or other) paths: the passed schema decides, not a global one."""
    stricter = copy.deepcopy(SCHEMA)
    stricter["create"]["required"].append("data.botId")
    assert validate_ingestion_schema(event, "feedback", "create", feedback_schema=stricter) == []
    event["data"]["botId"] = None
    assert validate_ingestion_schema(event, "feedback", "create", feedback_schema=stricter) == ["'data.botId' is null"]
    assert _problems(event) == []


# ---------------------------------------------------------------- Google Forms (Phase 2, slice 2c)

@pytest.fixture
def gforms():
    return _load("create", "create_feedback_gforms.json")


def test_gforms_valid_fixture_passes(gforms):
    assert _problems(gforms) == []
    assert gforms["submissionId"] == gforms["sessionId"]
    assert len(gforms["data"]["messages"]) == 6


def test_gforms_kannada_language_and_null_translation_pass(gforms):
    gforms["data"]["language"] = "kn"
    for row in gforms["data"]["messages"]:
        row["translatedMessage"] = None
    assert _problems(gforms) == []


def test_gforms_source_ref_missing(gforms):
    del gforms["data"]["sourceRef"]
    assert _problems(gforms) == ["'data.sourceRef' is missing"]


def test_gforms_source_ref_null_and_empty(gforms):
    gforms["data"]["sourceRef"] = None
    assert _problems(gforms) == ["'data.sourceRef' is null"]
    gforms["data"]["sourceRef"] = ""
    assert _problems(gforms) == ["'data.sourceRef' is empty"]


def test_gforms_source_ref_blank_and_wrong_type(gforms):
    # a whitespace-only string counts as empty since the shared _is_empty treats it so
    gforms["data"]["sourceRef"] = "   "
    assert _problems(gforms) == ["'data.sourceRef' is empty"]
    gforms["data"]["sourceRef"] = 12345
    assert _problems(gforms) == ["'data.sourceRef' must be a string, got int"]


@pytest.mark.parametrize("bad", ["184884", "not-a-uuid", 184884])
def test_gforms_submission_id_must_be_a_uuid(gforms, bad):
    gforms["submissionId"] = bad
    problems = _problems(gforms)
    assert any("'submissionId' must be a UUID" in p for p in problems)


def test_gforms_session_id_must_be_a_uuid(gforms):
    gforms["sessionId"] = "a" * 32 + "z"
    assert any("'sessionId' must be a UUID" in p for p in _problems(gforms))


def test_gforms_session_id_must_equal_submission_id(gforms):
    gforms["sessionId"] = "0e9c8a77-1d2b-4c3d-8e4f-5a6b7c8d9e0f"
    problems = _problems(gforms)
    assert len(problems) == 1 and "'sessionId' must equal 'submissionId'" in problems[0]


def test_gforms_completion_status_must_be_completed(gforms):
    gforms["data"]["completionStatus"] = "IN_PROGRESS"
    problems = _problems(gforms)
    assert len(problems) == 1 and "must be COMPLETED for adapter wide_form" in problems[0]


def test_gforms_invalid_completion_value_reported_once(gforms):
    gforms["data"]["completionStatus"] = "DONE"
    problems = _problems(gforms)
    assert len(problems) == 1 and "must be one of COMPLETED, IN_PROGRESS" in problems[0]


def test_gforms_all_problems_reported_together(gforms):
    del gforms["data"]["sourceRef"]
    gforms["data"]["completionStatus"] = "IN_PROGRESS"
    gforms["sessionId"] = "0e9c8a77-1d2b-4c3d-8e4f-5a6b7c8d9e0f"
    gforms["data"]["messages"][1]["parentSequence"] = 5
    problems = _problems(gforms)
    assert len(problems) == 4
    assert any("sourceRef" in p for p in problems)
    assert any("COMPLETED" in p for p in problems)
    assert any("'sessionId' must equal" in p for p in problems)
    assert any("parentSequence" in p for p in problems)


def test_gforms_fixture_carries_session_type_and_form_version(gforms):
    assert gforms["data"]["sessionType"] == "Youth_Voice"
    assert gforms["data"]["questionnaireVersion"] == "V1"
    assert "botId" not in gforms["data"] and "botName" not in gforms["data"]


@pytest.mark.parametrize("field", SESSION_FIELDS)
def test_gforms_session_fields_are_required(gforms, field):
    del gforms["data"][field]
    assert _problems(gforms) == [f"'data.{field}' is missing"]
    gforms["data"][field] = None
    assert _problems(gforms) == [f"'data.{field}' is null"]
    gforms["data"][field] = ""
    assert _problems(gforms) == [f"'data.{field}' is empty"]


def test_gforms_bot_fields_are_optional_but_typed(gforms):
    gforms["data"]["botId"] = None
    gforms["data"]["botName"] = "x"
    assert _problems(gforms) == []
    gforms["data"]["botId"] = 5
    assert _problems(gforms) == ["'data.botId' must be a string or null, got int"]


def test_mitra_source_ref_absent_or_null_is_valid(event):
    assert "sourceRef" not in event["data"]
    assert _problems(event) == []
    event["data"]["sourceRef"] = None
    assert _problems(event) == []


def test_mitra_source_ref_set_is_rejected(event):
    event["data"]["sourceRef"] = "2026-10-02T10:09:20:" + "a" * 64
    problems = _problems(event)
    assert len(problems) == 1 and "'data.sourceRef' must be absent or null for adapter conversation_rows" in problems[0]


def test_mitra_ids_are_not_required_to_be_uuids(event):
    assert event["submissionId"] == "900001" and event["sessionId"] != event["submissionId"]
    assert _problems(event) == []


def test_identity_rules_follow_the_adapter_not_the_source_name(gforms, event):
    # a wide_form event under ANY source label gets the natural-key rules; a conversation_rows event under the label
    # "google_forms" does not
    gforms["data"]["source"] = "ptm_app"
    gforms["sessionId"] = "0e9c8a77-1d2b-4c3d-8e4f-5a6b7c8d9e0f"
    del gforms["data"]["sourceRef"]
    problems = _problems(gforms)
    assert sorted(p.split(" ")[0] for p in problems) == ["'data.sourceRef'", "'sessionId'"]
    event["data"]["source"] = "google_forms"
    assert _problems(event) == []
    event["data"]["sourceRef"] = "x"
    assert _problems(event) == ["'data.sourceRef' must be absent or null for adapter conversation_rows"]


def test_adapter_is_required_and_must_be_known(event):
    del event["data"]["adapter"]
    assert _problems(event) == ["'data.adapter' is missing"]
    event["data"]["adapter"] = "excel"
    assert _problems(event) == ["'data.adapter' must be one of conversation_rows, wide_form, got 'excel'"]
    event["data"]["adapter"] = "x\nFORGED"
    (problem,) = _problems(event)
    assert "FORGED" not in problem


@pytest.mark.parametrize("path, value", [
    ("submissionId", "900001\n2026-11-13 [ERROR] FORGED LINE"),
    ("sessionId", "s\r\nFORGED"),
    ("tenantCode", "mitra\nFORGED"),
    ("submissionId", "a" * 129),
    ("submissionId", "has space"),
])
def test_identifiers_with_control_characters_or_bad_length_are_rejected_without_echo(event, path, value):
    event[path] = value
    problems = _problems(event)
    assert len(problems) == 1 and path in problems[0] and "FORGED" not in problems[0]


def test_session_type_with_control_characters_is_rejected(event):
    event["data"]["sessionType"] = "Youth_Voice\nFORGED"
    (problem,) = _problems(event)
    assert "data.sessionType" in problem and "FORGED" not in problem


def test_delete_events_also_check_identifier_safety():
    ev = _load("delete", "delete_feedback.json")
    ev["submissionId"] = "1\nFORGED"
    (problem,) = _problems(ev, "delete")
    assert "submissionId" in problem and "FORGED" not in problem


# tags.programId must be a UUID; an unsupported eventType names the missing section -------------------

@pytest.mark.parametrize("schema", [SCHEMA, DEFAULT_INGESTION_SCHEMA])
def test_validator_rejects_a_non_uuid_program_id_even_with_a_schema(event, schema):
    original = event["tags"]["programId"]
    event["tags"]["programId"] = "12345"
    problems = validate_ingestion_schema(event, "feedback", "create", feedback_schema=schema)
    assert any("'tags.programId' must be a UUID" in p for p in problems)
    event["tags"]["programId"] = original
    assert not any("tags.programId" in p for p in
                   validate_ingestion_schema(event, "feedback", "create", feedback_schema=schema))


@pytest.mark.parametrize("schema", [None, DEFAULT_INGESTION_SCHEMA])
def test_unsupported_feedback_event_type_names_the_missing_section(event, schema):
    event["eventType"] = "foo"
    problems = validate_ingestion_schema(event, "feedback", "foo", feedback_schema=schema)
    assert problems == ["No ingestion schema section defined for eventType 'foo'"]
