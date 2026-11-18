"""
Pure tests (no database, no network) of the review fixes around app/database/feedback_extraction_operations.py, the
seed SQL, the schema text and the documented queries: sanitised LLM errors, the content fingerprint, the seed prompt
that is never overwritten, no copied session_type / questionnaire_version columns. SYNTHETIC data only.
"""
import traceback
import uuid
from pathlib import Path

import pytest

from app.database import feedback_extraction_operations as ops
from app.services import feedback_extraction as fx

ROOT = Path(__file__).resolve().parent.parent
PHONE = "5550100123"   # synthetic


# ------------------------------------------------------------------------------------------------ sanitised errors

@pytest.mark.parametrize("message, expected", [
    (f"OpenRouter request failed with HTTP 429: {{'error': 'x {PHONE}'}}", "LLM call failed (RuntimeError, HTTP 429)"),
    ("OpenRouter request failed with HTTP 503: upstream", "LLM call failed (RuntimeError, HTTP 503)"),
    (f"Unexpected OpenRouter response shape: {{'a': '{PHONE}'}}", "LLM call failed (RuntimeError)"),
    (f"Failed to connect to OpenRouter: <urlopen error {PHONE}>", "LLM call failed (RuntimeError)"),
    (f"OpenRouter response did not include content. {PHONE}", "LLM call failed (RuntimeError)"),
    # an HTTP code that appears only after the first colon (inside provider text) is not read
    (f"Something else: HTTP 418 {PHONE}", "LLM call failed (RuntimeError)"),
    ("", "LLM call failed (RuntimeError)"),
])
def test_sanitize_keeps_only_the_class_name_and_a_status_from_the_prefix(message, expected):
    error = ops.sanitize_llm_error(RuntimeError(message))
    assert isinstance(error, ops.LlmCallError) and str(error) == expected
    assert PHONE not in str(error) and PHONE not in repr(error)


def test_sanitize_names_the_original_class_for_any_exception():
    assert str(ops.sanitize_llm_error(TimeoutError(f"timed out {PHONE}"))) == "LLM call failed (TimeoutError)"
    assert str(ops.sanitize_llm_error(KeyError(PHONE))) == "LLM call failed (KeyError)"


def test_only_our_own_errors_are_stored_with_their_message_everything_else_by_class_name():
    assert ops._safe_error_text(RuntimeError(f"provider echoed {PHONE}")) == "RuntimeError"
    assert ops._safe_error_text(ValueError(PHONE)) == "ValueError"
    assert ops._safe_error_text(ops.LlmCallError("LLM call failed (RuntimeError, HTTP 429)")) == \
        "LlmCallError: LLM call failed (RuntimeError, HTTP 429)"
    assert ops._safe_error_text(fx.ExtractionOutputError("model answer is not valid JSON")).startswith("ExtractionOutputError: ")
    assert ops._safe_error_text(ops.StaleExtractionError("changed")).startswith("StaleExtractionError: ")
    try:
        raise RuntimeError(PHONE)
    except RuntimeError as exc:
        assert PHONE not in "".join(traceback.format_exception(type(ops.sanitize_llm_error(exc)), ops.sanitize_llm_error(exc), None))


# ------------------------------------------------------------------------------------------------ fingerprint

def _row(seq, message, parent=None, translated=None, key="Q"):
    return {"sequence": seq, "parent_id": parent, "question_key": key, "message": message, "translated_message": translated}


def _rows():
    parent = uuid.uuid4()
    return [_row(1, "Which state?"), _row(2, "Kerala", parent=parent), _row(3, "Gender?")]


def test_fingerprint_is_stable_and_independent_of_row_order():
    rows = _rows()
    assert ops.rows_fingerprint(rows) == ops.rows_fingerprint(list(reversed(rows)))
    assert ops.rows_fingerprint(rows) == ops.rows_fingerprint([dict(r) for r in rows])


def _changed(mutate):
    rows = _rows()
    before = ops.rows_fingerprint(rows)
    mutate(rows)
    return ops.rows_fingerprint(rows) != before


@pytest.mark.parametrize("mutate", [
    lambda r: r.append(_row(4, "more")),                                  # a session that grew
    lambda r: r.pop(),                                                    # a message removed
    lambda r: r[1].update(message="Karnataka"),                           # an answer corrected
    lambda r: r[1].update(translated_message="Kerala"),                   # a translation added
    lambda r: r[0].update(message="Which state do you live in?"),
    lambda r: r[2].update(parent_id=uuid.uuid4()),                        # parent link only: a bot row became an answer
    lambda r: r[1].update(parent_id=None),
    lambda r: r[0].update(question_key="CLOSING"),                        # the closing-key skip depends on it
    lambda r: r[0].update(sequence=7),
])
def test_fingerprint_changes_when_the_content_changes(mutate):
    assert _changed(mutate)


def test_fingerprint_does_not_let_a_value_slide_into_its_neighbour():
    a = [_row(1, "ab", translated="c")]
    b = [_row(1, "a", translated="bc")]
    c = [_row(1, "ab", translated=None)]
    d = [_row(1, "ab", translated="")]
    assert len({ops.rows_fingerprint(x) for x in (a, b, c, d)}) == 4


# ------------------------------------------------------------------------------------------------ seed, schema, docs

def test_extraction_prompt_seed_is_do_nothing_and_says_how_to_change_it():
    sql = (ROOT / "seed_prompts.sql").read_text(encoding="utf-8")
    block = sql[sql.index("-- Feedback Metadata Extraction prompt"):]
    assert "ON CONFLICT (prompt_id, version) DO NOTHING;" in block
    assert "DO UPDATE" not in block
    assert "NEW prompt_version" in block


def test_schema_has_no_copied_session_type_or_questionnaire_version_on_the_extracted_data_table():
    schema = (ROOT / "schema.sql").read_text(encoding="utf-8")
    table = schema[schema.index("CREATE TABLE feedback_extracted_data ("):]
    table = table[:table.index(");")]
    code_lines = [line.split("--")[0] for line in table.splitlines()]
    assert not any("session_type" in line or "questionnaire_version" in line for line in code_lines)
    assert "idx_feedback_extracted_session_field_value" not in schema
    assert "ON feedback_extracted_data (field_name, value)" in schema

