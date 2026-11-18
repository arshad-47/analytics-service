"""
The value of an extracted field is NEVER empty, and it is always one of a small normalised set of labels or a category
of the field (app/services/feedback_extraction.py, constants in feedback_extraction_constants.py). Pure tests, fake
LLM, SYNTHETIC data only:

  * labels: not_provided is 'Not answered', an unmappable answer is the field's fallback (Unmapped / Others / Invalid
    response), a field that does not apply is 'N/A' with the status not_applicable;
  * aliases: every spelling of Delhi (and the other states) is mapped by the code, not only hinted to the model;
  * banded numbers: the age is stored as the label of its band, decimals are valid, the number stays in raw_value;
  * multi-value fields: distinct labels in the config's order, joined by ', ', each also in value_list; stand-alone
    labels give way to real categories;
  * N/A by questionnaire version and by an earlier answer (`versions`, `applies_when`), decided by the code;
  * the config validator refuses a bad list of aliases, bands, versions, applies_when or multi labels.
"""
import copy
import json

import pytest

from app.services import feedback_extraction as fx

PNTS, INV, OTH, UNMAP = "Prefer not to say", "Invalid response", "Others", "Unmapped"


def _field(name, kind="category", **extra):
    base = {"name": name, "kind": kind, "description": f"The {name}."}
    base.update(extra)
    return base


def _raw(*fields, **top):
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "p", "fields": list(fields)}
    raw.update(top)
    return raw


def _config(*fields, **top):
    return fx.parse_extraction_config(_raw(*fields, **top))


def _answer(config, values=None, pii=False):
    entries = {f.name: {"value": None, "raw": None} for f in config.fields}
    for name, value in (values or {}).items():
        entries[name] = value if isinstance(value, dict) else {"value": value, "raw": None}
    return json.dumps({"fields": entries, "pii": {"detected": pii, "types": ["name"] if pii else []}})


def _parse(config, values=None):
    return {f.field_name: f for f in fx.parse_model_response(_answer(config, values), config).fields}


ROWS = [
    {"sequence": 1, "parent_id": None, "question_key": "Q", "message": "Q?", "translated_message": None},
    {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "an answer", "translated_message": None},
]


def _extract(config, values=None, version=None, rows=ROWS):
    seen = []

    def llm(prompt):
        seen.append(prompt)
        return _answer(config.__class__(**{**config.__dict__, "fields": config.fields_for(version)}), values), {}
    run = fx.extract(rows, config, "{{persona}} {{fields}} {{output_format}}", "{{conversation}}", llm,
                     questionnaire_version=version)
    return run, seen


# ------------------------------------------------------------------------------------------------ never empty

GENDER = _field("gender", categories=["Male", "Female", OTH, PNTS, INV])


def test_a_field_nobody_answered_is_not_answered_and_the_status_says_why():
    field = _parse(_config(GENDER))["gender"]
    assert (field.value, field.value_list, field.status, field.raw_value) == ("Not answered", ("Not answered",), "not_provided", None)


def test_the_model_saying_not_answered_or_na_for_a_category_means_no_answer():
    config = _config(GENDER)
    for text in ("Not answered", "not answered", "N/A", "n/a", "Not applicable"):
        field = _parse(config, {"gender": text})["gender"]
        assert (field.value, field.status) == ("Not answered", "not_provided"), text


def test_an_answer_outside_the_list_is_never_taken_as_given_and_never_empty():
    field = _parse(_config(GENDER), {"gender": {"value": "Martian", "raw": "i am a martian"}})["gender"]
    assert (field.status, field.value, field.raw_value) == ("unmapped", "Others", "i am a martian")
    no_others = _config(_field("kind", categories=["Yes", "No", INV]))
    assert _parse(no_others, {"kind": "Maybe"})["kind"].value == "Invalid response"
    place = _config(_field("state", categories=["Goa", UNMAP, INV]))
    assert _parse(place, {"state": "Narnia"})["state"].value == "Unmapped"          # the field's own Unmapped comes first


def test_a_label_that_needs_a_persons_look_has_the_status_unmapped_and_the_others_are_extracted():
    config = _config(_field("state", categories=["Goa", PNTS, UNMAP, INV, "Outside India"]))
    for label, status in (("Goa", "extracted"), (PNTS, "extracted"), ("Outside India", "extracted"), (UNMAP, "unmapped"), (INV, "unmapped")):
        field = _parse(config, {"state": label})["state"]
        assert (field.value, field.status) == (label, status), label


def test_a_free_text_field_can_answer_with_a_special_label_and_otherwise_keeps_the_text():
    district = _field("district", "free_text", categories=[PNTS, UNMAP, INV])
    config = _config(district)
    assert _parse(config, {"district": "prefer NOT to say"})["district"].value == PNTS
    assert _parse(config, {"district": "Unmapped"})["district"].status == "unmapped"
    assert _parse(config, {"district": "Mysuru"})["district"].value == "Mysuru"
    assert _parse(config, {"district": None})["district"].value == "Not answered"
    assert _parse(config, {"district": "x" * 300})["district"].value == "Unmapped"        # too long to be a place: the fallback


def test_the_engine_labels_are_refused_in_a_category_list_because_the_code_writes_them():
    for label in ("Not answered", "N/A", "not ANSWERED"):
        with pytest.raises(fx.ExtractionConfigError, match="engine writes"):
            _config(_field("gender", categories=["Male", label]))


def test_empty_and_oversized_sessions_have_no_empty_value_either():
    config = _config(GENDER, _field("district", "free_text"))
    empty = fx.empty_result(config)
    assert [(f.value, f.status) for f in empty.fields] == [("Not answered", "not_provided")] * 2
    big = fx.oversized_result(config)
    # nothing was extracted, so no real category may be claimed: 'Not answered', status unmapped (kept on the review list)
    assert [(f.value, f.status) for f in big.fields] == [("Not answered", "unmapped")] * 2
    assert all("size limit" in f.justification for f in big.fields)


# ------------------------------------------------------------------------------------------------ aliases

STATE = _field("state", categories=["Delhi", "Odisha", "Uttar Pradesh", UNMAP, INV],
               aliases={"Delhi": ["New Delhi", "NCT of Delhi", "Delhi/NCR", "NCR"], "Odisha": ["Orissa"]})


@pytest.mark.parametrize("spelling", ["Delhi", "delhi", "  NEW   DELHI ", "NCT of Delhi", "Delhi/NCR", "ncr"])
def test_every_spelling_of_delhi_becomes_the_one_delhi_value(spelling):
    field = _parse(_config(STATE), {"state": {"value": spelling, "raw": spelling}})["state"]
    assert (field.value, field.status) == ("Delhi", "extracted")


def test_an_alias_is_exact_after_case_and_spacing_and_other_spellings_are_not_repaired():
    config = _config(STATE)
    assert _parse(config, {"state": "Orissa"})["state"].value == "Odisha"
    assert _parse(config, {"state": "Delhi NCT region"})["state"].status == "unmapped"
    assert _parse(config, {"state": "Delhi NCT region"})["state"].value == "Unmapped"


def test_the_config_validator_refuses_an_alias_that_is_not_a_category_or_already_means_another_value():
    with pytest.raises(fx.ExtractionConfigError, match="not a category"):
        _config(_field("state", categories=["Goa"], aliases={"Delhi": ["NCR"]}))
    with pytest.raises(fx.ExtractionConfigError, match="already means"):
        _config(_field("state", categories=["Goa", "Delhi"], aliases={"Delhi": ["goa"]}))
    with pytest.raises(fx.ExtractionConfigError, match="non-empty"):
        _config(_field("state", categories=["Goa"], aliases={"Goa": []}))


# ------------------------------------------------------------------------------------------------ banded numbers

AGE_BANDS = [{"label": "< 12", "min": None, "max": 12}, {"label": "≥ 12 and < 16", "min": 12, "max": 16},
             {"label": "≥ 16 and < 22", "min": 16, "max": 22}, {"label": "≥ 22", "min": 22, "max": None}]
AGE = _field("age", "integer", min=0, max=120, bands=AGE_BANDS, categories=[PNTS, "Unclear", INV])


@pytest.mark.parametrize("stated, band", [
    (11, "< 12"), (11.99, "< 12"), (12, "≥ 12 and < 16"), (15.5, "≥ 12 and < 16"), ("15.5", "≥ 12 and < 16"),
    (16, "≥ 16 and < 22"), (21, "≥ 16 and < 22"), ("22", "≥ 22"), (60, "≥ 22"), (0, "< 12"), (120, "≥ 22"),
])
def test_the_age_is_stored_as_the_label_of_its_band_and_the_number_stays_in_raw(stated, band):
    field = _parse(_config(AGE), {"age": {"value": stated, "raw": None}})["age"]
    assert (field.value, field.status, field.value_list) == (band, "extracted", (band,))
    assert field.raw_value == (stated if isinstance(stated, str) else repr(stated) if isinstance(stated, float) else str(stated))


def test_the_respondents_own_words_are_kept_when_the_model_gives_them():
    field = _parse(_config(AGE), {"age": {"value": 15.5, "raw": "fifteen and a half"}})["age"]
    assert (field.value, field.raw_value) == ("≥ 12 and < 16", "fifteen and a half")


@pytest.mark.parametrize("label, status", [(PNTS, "extracted"), ("Unclear", "unmapped"), (INV, "unmapped")])
def test_the_special_answers_of_a_banded_number(label, status):
    field = _parse(_config(AGE), {"age": label})["age"]
    assert (field.value, field.status) == (label, status)


@pytest.mark.parametrize("stated", [-1, 121, 10 ** 9, "seventeen", "17 years", "1e1", True])
def test_a_number_outside_the_range_or_text_that_is_no_number_is_invalid_with_the_words_kept(stated):
    field = _parse(_config(AGE), {"age": {"value": stated, "raw": None}})["age"]
    assert field.value == "Invalid response" and field.status == "unmapped" and field.raw_value is not None


def test_no_answer_for_a_banded_number_is_not_answered():
    assert _parse(_config(AGE), {"age": None})["age"].value == "Not answered"
    assert _parse(_config(AGE), {"age": "N/A"})["age"].value == "Not answered"


@pytest.mark.parametrize("bands, message", [
    ([{"label": "a", "min": None, "max": 5}], "at least two"),
    ([{"label": "a", "min": 0, "max": 5}, {"label": "b", "min": 5, "max": None}], "open below"),
    ([{"label": "a", "min": None, "max": 5}, {"label": "b", "min": 6, "max": None}], "no gap, no overlap"),
    ([{"label": "a", "min": None, "max": 5}, {"label": "b", "min": 5, "max": 9}], "open above"),
    ([{"label": "a", "min": None, "max": 5}, {"label": "a", "min": 5, "max": None}], "unique"),
    ([{"label": "a", "min": None, "max": 5}, {"label": "b", "min": 5, "max": None, "x": 1}], "label, min, max"),
])
def test_the_validator_refuses_bands_that_are_not_contiguous_and_open_at_both_ends(bands, message):
    with pytest.raises(fx.ExtractionConfigError, match=message):
        _config(_field("age", "integer", min=0, max=99, bands=bands))


def test_bands_belong_to_integer_fields_only():
    with pytest.raises(fx.ExtractionConfigError, match="only an integer field has bands"):
        _config(_field("age", categories=["a"], bands=AGE_BANDS))


# ------------------------------------------------------------------------------------------------ multi-value

STATUS = _field("status", categories=["Studying", "Working", "Self-employed", OTH, PNTS, INV], multi=True)


def test_several_categories_are_stored_in_the_config_order_joined_and_listed():
    for answer in (["Working", "Studying"], ["working", "STUDYING", "Studying"], "Working, Studying", "Studying; Working"):
        field = _parse(_config(STATUS), {"status": answer})["status"]
        assert (field.value, field.value_list, field.status) == ("Studying, Working", ("Studying", "Working"), "extracted"), answer


def test_a_single_category_is_a_one_item_list_and_the_same_combination_is_always_the_same_text():
    field = _parse(_config(STATUS), {"status": "Working"})["status"]
    assert (field.value, field.value_list) == ("Working", ("Working",))


def test_stand_alone_labels_give_way_to_a_real_category_but_others_combines():
    config = _config(STATUS)
    assert _parse(config, {"status": [PNTS, "Studying"]})["status"].value == "Studying"
    assert _parse(config, {"status": [INV, PNTS]})["status"].value == PNTS               # only stand-alone labels: the first in config order
    assert _parse(config, {"status": ["Others", "Working"]})["status"].value == "Working, Others"


def test_an_item_outside_the_list_becomes_others_and_the_status_asks_for_a_look():
    field = _parse(_config(STATUS), {"status": {"value": ["Studying", "Astronaut"], "raw": "student astronaut"}})["status"]
    assert (field.value, field.status, field.raw_value) == ("Studying, Others", "unmapped", "student astronaut")
    no_others = _config(_field("kind", categories=["Yes", "No"], multi=True))
    assert _parse(no_others, {"kind": ["Yes", "Maybe"]})["kind"].value == "Yes"           # nothing to stand in: dropped, still unmapped
    assert _parse(no_others, {"kind": ["Yes", "Maybe"]})["kind"].status == "unmapped"
    assert _parse(no_others, {"kind": ["Maybe"]})["kind"].value == "Invalid response"     # nothing matched at all: the fallback


def test_an_empty_list_and_null_are_no_answer_and_a_list_of_non_strings_is_a_bad_answer():
    config = _config(STATUS)
    for empty in ([], None, "", "  "):
        assert _parse(config, {"status": empty})["status"].value == "Not answered", empty
    with pytest.raises(fx.ExtractionOutputError, match="short list of strings"):
        _parse(config, {"status": [1, 2]})
    with pytest.raises(fx.ExtractionOutputError):
        _parse(config, {"status": {"a": 1}})


def test_a_list_for_a_single_value_field_is_still_refused():
    with pytest.raises(fx.ExtractionOutputError, match="must be a string or null"):
        _parse(_config(GENDER), {"gender": ["Male", "Female"]})


def test_the_validator_refuses_multi_on_other_kinds_and_a_comma_in_a_multi_label():
    with pytest.raises(fx.ExtractionConfigError, match="only a category field can be multi"):
        _config(_field("d", "free_text", multi=True))
    with pytest.raises(fx.ExtractionConfigError, match="comma"):
        _config(_field("status", categories=["Studying, working"], multi=True))
    with pytest.raises(fx.ExtractionConfigError, match="multi must be a boolean"):
        _config(_field("status", categories=["a"], multi=1))


def test_the_prompt_tells_the_model_how_to_answer_a_multi_value_and_a_banded_field():
    config = _config(STATUS, AGE)
    prompt = fx.render_prompt("{{fields}}\n{{output_format}}", "x", config, fx.build_conversation(ROWS))
    assert "choose one or more allowed values and return them as a JSON list" in prompt
    assert "decimals allowed" in prompt and "Special answers: Prefer not to say; Unclear; Invalid response" in prompt
    assert '"value": []' in prompt                                       # the example of the multi field


# ------------------------------------------------------------------------------------------------ N/A by version and by an earlier answer

MONTHLY = _field("monthly", categories=["Low", "High"], versions=["V2"])
ANNUAL = _field("annual", categories=["Low", "High"], versions=["V1"])
DISABILITY = _field("disability", categories=["Yes", "No"])
KIND = _field("kind", categories=["Physical", "Other"], multi=True, applies_when={"field": "disability", "in": ["Yes"]})


def test_a_field_that_the_questionnaire_version_does_not_ask_is_not_sent_and_comes_back_na():
    config = _config(GENDER, MONTHLY, ANNUAL)
    run, seen = _extract(config, {"gender": "Male", "annual": "Low"}, version="V1")
    by_name = {f.field_name: f for f in run.result.fields}
    assert [f.field_name for f in run.result.fields] == ["gender", "monthly", "annual"]       # config order, every field
    assert (by_name["monthly"].value, by_name["monthly"].status, by_name["monthly"].value_list) == ("N/A", "not_applicable", ("N/A",))
    assert "questionnaire version V1" in by_name["monthly"].justification
    assert by_name["annual"].value == "Low" and by_name["gender"].value == "Male"
    assert "monthly" not in seen[0] and "annual" in seen[0]                                   # the model is never asked it
    run2, _ = _extract(config, {"gender": "Male", "monthly": "High"}, version="V2")
    assert {f.field_name: f.value for f in run2.result.fields} == {"gender": "Male", "monthly": "High", "annual": "N/A"}


def test_an_unknown_version_asks_every_field():
    config = _config(GENDER, MONTHLY, ANNUAL)
    run, _ = _extract(config, {"gender": "Male", "monthly": "Low", "annual": "High"}, version=None)
    assert [f.status for f in run.result.fields] == ["extracted"] * 3


def test_a_field_whose_earlier_field_has_another_value_is_na_whatever_the_model_wrote():
    config = _config(DISABILITY, KIND)
    yes, _ = _extract(config, {"disability": "Yes", "kind": ["Physical"]})
    assert {f.field_name: f.value for f in yes.result.fields} == {"disability": "Yes", "kind": "Physical"}
    no, _ = _extract(config, {"disability": "No", "kind": ["Physical"]})                    # the model invented a kind
    kind = {f.field_name: f for f in no.result.fields}["kind"]
    assert (kind.value, kind.status, kind.value_list) == ("N/A", "not_applicable", ("N/A",))
    assert "disability" in kind.justification
    skipped, _ = _extract(config, {"disability": None})
    assert {f.field_name: f.value for f in skipped.result.fields} == {"disability": "Not answered", "kind": "N/A"}


def test_applies_when_follows_a_multi_value_field_and_chains_through_na():
    status = _field("status", categories=["Studying", "Working"], multi=True)
    level = _field("level", categories=["School", "College"], applies_when={"field": "status", "in": ["Studying"]})
    course = _field("course", categories=["Science", "Arts"], applies_when={"field": "level", "in": ["College"]})
    config = _config(status, level, course)
    both, _ = _extract(config, {"status": ["Working", "Studying"], "level": "College", "course": "Arts"})
    assert {f.field_name: f.value for f in both.result.fields} == {"status": "Studying, Working", "level": "College", "course": "Arts"}
    working, _ = _extract(config, {"status": ["Working"], "level": "College", "course": "Arts"})
    assert {f.field_name: f.value for f in working.result.fields} == {"status": "Working", "level": "N/A", "course": "N/A"}


def test_a_field_after_a_field_the_version_removes_is_na_too():
    config = _config(_field("a", categories=["x"], versions=["V2"]), _field("b", categories=["y"], applies_when={"field": "a", "in": ["x"]}))
    run, _ = _extract(config, {"b": "y"}, version="V1")
    assert [(f.value, f.status) for f in run.result.fields] == [("N/A", "not_applicable")] * 2


def test_a_session_without_an_answer_still_has_na_for_the_fields_of_the_other_version():
    config = _config(GENDER, MONTHLY, ANNUAL)
    run, _ = _extract(config, version="V2", rows=[ROWS[0]])
    assert run.llm_called is False
    assert [(f.field_name, f.value) for f in run.result.fields] == [("gender", "Not answered"), ("monthly", "Not answered"), ("annual", "N/A")]


def test_the_validator_refuses_bad_versions_and_bad_applies_when():
    with pytest.raises(fx.ExtractionConfigError, match="versions"):
        _config(_field("a", categories=["x"], versions=[]))
    with pytest.raises(fx.ExtractionConfigError, match="versions"):
        _config(_field("a", categories=["x"], versions=["V1", "V1"]))
    with pytest.raises(fx.ExtractionConfigError, match="BEFORE"):
        _config(_field("a", categories=["x"], applies_when={"field": "b", "in": ["y"]}), _field("b", categories=["y"]))
    with pytest.raises(fx.ExtractionConfigError, match="BEFORE"):
        _config(_field("a", categories=["x"], applies_when={"field": "a", "in": ["x"]}))
    with pytest.raises(fx.ExtractionConfigError, match="values of 'a'"):
        _config(_field("a", categories=["x"]), _field("b", categories=["y"], applies_when={"field": "a", "in": ["nope"]}))
    with pytest.raises(fx.ExtractionConfigError, match="applies_when must be"):
        _config(_field("a", categories=["x"]), _field("b", categories=["y"], applies_when={"field": "a"}))


# ------------------------------------------------------------------------------------------------ a flagged session keeps everything

def test_a_pii_flagged_session_keeps_its_values_and_words_even_for_free_text():
    config = _config(_field("district", "free_text"), GENDER)
    rows = [ROWS[0], {**ROWS[1], "message": "Mysuru, call me on 9876543210"}]
    answer = _answer(config, {"district": {"value": "Mysuru", "raw": "Mysuru, call me"}, "gender": {"value": "Male", "raw": "boy"}})
    run = fx.extract(rows, config, "{{fields}} {{output_format}}", "{{conversation}}", lambda prompt: (answer, {}))
    assert run.result.pii_detected is True
    fields = {f.field_name: f for f in run.result.fields}
    assert (fields["district"].value, fields["district"].raw_value) == ("Mysuru", "Mysuru, call me")
    assert (fields["gender"].value, fields["gender"].raw_value) == ("Male", "boy")
    assert not hasattr(fx, "minimise_flagged")


def test_the_config_dict_is_not_changed_by_parsing():
    raw = _raw(STATE, AGE, STATUS, DISABILITY, KIND, MONTHLY)
    before = copy.deepcopy(raw)
    fx.parse_extraction_config(raw)
    assert raw == before
