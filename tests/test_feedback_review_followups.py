"""
Follow-ups of a code review of the extraction engine (pure tests, fake LLM, SYNTHETIC data only):

  * a JSON integer of hundreds of digits for a banded field (the age) is 'unmapped', never an OverflowError;
  * the engine's own labels (N/A, Not answered) can not be band labels;
  * a multi-value answer given as ONE string is matched as a whole category before it is split at ; and ,;
  * the special labels are compared without regard to case ('unmapped' in a config is still the Unmapped sentinel), so a
    junk value is never copied to submissions.state;
  * a state-only copy_to_submission mapping clears the district when the state is cleared;
  * a prompt that fails to render after a successful masking call still reports the billed masking usage.
"""
import json

import pytest

from app.services import feedback_extraction as fx
from app.services import feedback_pii_masking as pm

BASE = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "p"}
AGE = {"name": "age", "kind": "integer", "description": "Age.", "min": 0, "max": 120,
       "bands": [{"label": "< 16", "min": None, "max": 16}, {"label": "≥ 16", "min": 16, "max": None}],
       "categories": ["Invalid response"]}


def _parse(fields, answers, **extra):
    config = fx.parse_extraction_config({**BASE, "fields": fields, **extra})
    text = json.dumps({"fields": answers, "pii": {"detected": False, "types": []}})
    return {f.field_name: f for f in fx.parse_model_response(text, config).fields}


# ------------------------------------------------------------------------------------------------ huge integers

@pytest.mark.parametrize("number", [10 ** 400, -10 ** 400, 10 ** 6, 10 ** 30], ids=["400 digits", "minus 400 digits", "1e6", "1e30"])
def test_a_huge_integer_for_a_banded_field_is_unmapped_with_the_words_kept(number):
    field = _parse([AGE], {"age": {"value": number, "raw": None}})["age"]
    assert field.status == "unmapped" and field.value == "Invalid response"      # never empty: the field's fallback label
    assert field.raw_value is not None and len(field.raw_value) > 0


def test_ordinary_numbers_still_select_their_band():
    for value, band in ((15, "< 16"), (15.5, "< 16"), (16, "≥ 16"), (30, "≥ 16"), ("17", "≥ 16")):
        assert _parse([AGE], {"age": {"value": value, "raw": None}})["age"].value == band


# ------------------------------------------------------------------------------------------------ reserved band labels

@pytest.mark.parametrize("label", ["N/A", "n/a", "Not answered", "NOT ANSWERED"])
def test_an_engine_label_is_refused_as_a_band_label(label):
    field = {**AGE, "bands": [{"label": label, "min": None, "max": 16}, {"label": "≥ 16", "min": 16, "max": None}]}
    with pytest.raises(fx.ExtractionConfigError, match="engine writes those"):
        fx.parse_extraction_config({**BASE, "fields": [field]})


# ------------------------------------------------------------------------------------------------ one string, several categories

FIELD_WITH_SEMICOLON = {"name": "area", "kind": "category", "multi": True, "description": "Area.",
                        "categories": ["Research; development", "Teaching", "Others"]}


def test_a_multi_answer_given_as_one_string_is_matched_whole_before_it_is_split():
    whole = _parse([FIELD_WITH_SEMICOLON], {"area": {"value": "Research; development", "raw": None}})["area"]
    assert (whole.value, whole.status) == ("Research; development", "extracted")
    as_list = _parse([FIELD_WITH_SEMICOLON], {"area": {"value": ["Research; development"], "raw": None}})["area"]
    assert (as_list.value, as_list.status) == ("Research; development", "extracted")
    split = _parse([FIELD_WITH_SEMICOLON], {"area": {"value": "Teaching, Research; development", "raw": None}})["area"]
    assert split.value_list is not None and "Teaching" in split.value_list      # a real list is still split


def test_a_multi_answer_that_lists_categories_is_still_split():
    field = {"name": "status", "kind": "category", "multi": True, "description": "S.", "categories": ["Studying", "Working", "Others"]}
    assert _parse([field], {"status": {"value": "Studying, Working", "raw": None}})["status"].value == "Studying, Working"


# ------------------------------------------------------------------------------------------------ case of the special labels

STATE = {"name": "state", "kind": "category", "description": "State.", "categories": ["Karnataka", "unmapped", "Others"]}


def test_a_special_label_spelled_in_lower_case_is_still_the_special_label():
    state = _parse([STATE], {"state": {"value": "Unmapped", "raw": None}})["state"]
    assert state.value == "unmapped" and state.status == "unmapped"          # the configured spelling, the sentinel's status


def test_a_lower_case_special_label_is_never_copied_to_submissions():
    config = fx.parse_extraction_config({**BASE, "fields": [STATE], "copy_to_submission": {"state": "state"}})
    result = fx.ExtractionResult((fx.FieldResult("state", "unmapped", "x", "extracted", 1.0),), False, ())
    assert fx.submission_geography(config, result) == {"state": None, "district": None}
    good = fx.ExtractionResult((fx.FieldResult("state", "Karnataka", "x", "extracted", 1.0),), False, ())
    assert fx.submission_geography(config, good) == {"state": "Karnataka"}


def test_in_a_multi_field_a_lower_case_exclusive_label_stands_alone():
    field = {"name": "kind", "kind": "category", "multi": True, "description": "K.",
             "categories": ["Physical", "prefer not to say", "Others"]}
    assert _parse([field], {"kind": {"value": ["Physical", "Prefer not to say"], "raw": None}})["kind"].value == "Physical"


# ------------------------------------------------------------------------------------------------ the district follows its state

def test_a_state_only_mapping_clears_the_district_when_the_state_is_cleared():
    district = {"name": "district", "kind": "free_text", "description": "D."}
    config = fx.parse_extraction_config({**BASE, "fields": [STATE, district], "copy_to_submission": {"state": "state"}})
    result = fx.ExtractionResult((fx.FieldResult("state", "Not answered", None, "not_provided", None),
                                  fx.FieldResult("district", "Mysuru", "x", "extracted", 1.0)), False, ())
    assert fx.submission_geography(config, result) == {"state": None, "district": None}


# ------------------------------------------------------------------------------------------------ billed masking usage

def test_a_prompt_that_fails_to_render_after_masking_still_reports_the_masking_usage():
    config = fx.parse_extraction_config({**BASE, "fields": [{"name": "district", "kind": "free_text", "description": "D."}],
                                         "pii": {"masking_prompt_name": "Mask"}})
    rows = [{"sequence": 1, "parent_id": None, "question_key": "Q", "message": "District?", "translated_message": None},
            {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "Mysuru", "translated_message": None}]

    def masking_call(prompt):
        pairs = json.loads(prompt.split("Analyse the following answers:\n", 1)[1].strip())
        return json.dumps({k: {"masked_text": v["answer"], "pii_found": [], "abusive_language": False, "abusive_spans": []}
                           for k, v in pairs.items()}), {"prompt_tokens": 7, "completion_tokens": 3}

    step = pm.MaskingStep("keys {columns}", "Analyse the following answers:\n{{text}}", masking_call)
    with pytest.raises(fx.PromptRenderError) as raised:
        fx.extract(rows, config, "{{no_such_variable}}", "{{conversation}}", lambda p: pytest.fail("no call"), (), step)
    assert raised.value.prior_usage == {"prompt_tokens": 7, "completion_tokens": 3}
