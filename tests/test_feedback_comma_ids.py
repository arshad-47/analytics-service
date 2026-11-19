"""
Submission and session ids can contain commas (Mitra ids such as '710001,710002'). The identifier rule accepts them
end to end (the rule itself, the event validation and the CSV builder), still refuses whitespace, control characters,
quotes, semicolons and overlong ids, and the deprecated datetime.utcnow() is not used anywhere in the code.
Pure tests; the database side is in tests/test_feedback_comma_ids_pg.py. Synthetic data only.
"""
import ast
import json
from pathlib import Path

import pytest

from app.services.feedback_adapter_config import is_safe_id
from app.services.ingestion_validation import validate_ingestion_schema
from seed_support import mitra_config_v1
import test_feedback_platform as platform

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = mitra_config_v1("Youth")["ingestion_schema"]


@pytest.mark.parametrize("value, ok", [
    ("710001,710002", True), ("1,2,3", True), ("a,b", True), (",", True), ("710001,", True), ("x," * 64, True),
    ("a:b.c_d-e,f", True), ("x" * 128, True), ("," * 128, True),
    ("," * 129, False), ("a, b", False), ("a ,b", False), ("a\n,b", False), ("a,b\r", False), ("a,b\t", False),
    ("a;b", False), ("a'b", False), ('a"b', False), ("a/b", False), ("a,b\x00", False), ("", False), (None, False),
    (710001, False),
])
def test_the_id_rule_accepts_commas_and_still_refuses_everything_unsafe(value, ok):
    assert is_safe_id(value) is ok


def _create_event():
    return json.loads((ROOT / "tests" / "kafka_events" / "create" / "create_feedback.json").read_text(encoding="utf-8"))


def _problems(event, event_type="create"):
    return validate_ingestion_schema(event, "feedback", event_type, feedback_schema=SCHEMA)


def test_event_validation_accepts_comma_ids():
    event = _create_event()
    event["submissionId"], event["sessionId"] = "710001,710002", "s,1,2"
    assert _problems(event) == []


@pytest.mark.parametrize("path, value", [("submissionId", "710001, 710002"), ("sessionId", "s,1\nFORGED"),
                                         ("submissionId", "1;2")])
def test_event_validation_still_refuses_unsafe_ids_with_commas_and_does_not_echo_them(path, value):
    event = _create_event()
    event[path] = value
    problems = _problems(event)
    assert len(problems) == 1 and path in problems[0] and "FORGED" not in problems[0]


def test_delete_events_accept_comma_ids():
    event = {"submissionId": "710001,710002", "submissionType": "feedback", "sessionId": "s,1", "tenantCode": "mitra",
             "eventType": "delete", "eventPublishedAt": "2026-11-10T00:00:00.000Z"}
    assert _problems(event, "delete") == []


def test_the_csv_builder_keeps_comma_ids_unchanged_and_does_not_skip_the_session():
    rows = platform.ptm_rows(meeting="710001,710002", session="s,1,2")
    events, skipped = platform.build(platform.ptm_df(rows), platform.ptm_config())
    assert skipped == []
    assert [(e["submissionId"], e["sessionId"]) for e in events] == [("710001,710002", "s,1,2")]


def test_the_csv_builder_still_skips_a_session_with_an_unsafe_id():
    rows = platform.ptm_rows(meeting="710001, 710002")        # a space after the comma
    events, skipped = platform.build(platform.ptm_df(rows), platform.ptm_config())
    assert events == [] and len(skipped) == 1


# --------------------------------------------------------------------------- deprecated datetime APIs

def _deprecated_datetime_calls():
    files = [p for folder in ("app", "tools") for p in (ROOT / folder).rglob("*.py")] + [ROOT / "main.py"]
    found = []
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in ("utcnow", "utcfromtimestamp"):
                found.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    return found


def test_no_deprecated_naive_utc_datetime_calls_remain():
    """datetime.utcnow() and utcfromtimestamp() are deprecated (Python 3.12): use datetime.now(timezone.utc)."""
    assert _deprecated_datetime_calls() == []
