"""
`copy_to_submission` (extraction config): the validated state / district of a session are copied to submissions.state /
district. Pure tests of the config check and of `submission_geography` (what is written, what is cleared). SYNTHETIC data.
"""
import json

import pytest

from app.services import feedback_extraction as fx

STATES = ["Karnataka", "Delhi", "Uttar Pradesh", "Prefer not to say", "Unmapped", "Invalid response", "Outside India"]


def _raw(copy=None, extra_fields=()):
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "p",
           "fields": [{"name": "state", "kind": "category", "description": "State.", "categories": STATES},
                      {"name": "district", "kind": "free_text", "description": "District.",
                       "categories": ["Prefer not to say", "Unmapped", "Invalid response", "Outside India"]},
                      {"name": "tags", "kind": "category", "multi": True, "description": "t", "categories": ["A", "B"]},
                      {"name": "age", "kind": "integer", "description": "a", "min": 1, "max": 99},
                      *extra_fields]}
    if copy is not None:
        raw["copy_to_submission"] = copy
    return raw


def _config(copy=None):
    return fx.parse_extraction_config(_raw(copy))


def _result(state=("Karnataka", "extracted"), district=("Mysuru", "extracted")):
    def field(name, pair):
        return fx.FieldResult(name, pair[0], pair[0], pair[1], 0.9)
    return fx.ExtractionResult(tuple(field(n, p) for n, p in (("state", state), ("district", district))), False, ())


def test_the_key_is_optional_and_a_config_without_it_copies_nothing():
    config = _config()
    assert dict(config.submission_columns) == {}
    assert fx.submission_geography(config, _result()) == {}


def test_a_valid_state_and_district_are_copied():
    config = _config({"state": "state", "district": "district"})
    assert dict(config.submission_columns) == {"state": "state", "district": "district"}
    assert fx.submission_geography(config, _result()) == {"state": "Karnataka", "district": "Mysuru"}


@pytest.mark.parametrize("label", ["Prefer not to say", "Unmapped", "Invalid response", "Outside India", "Others", "Not answered", "N/A"])
def test_a_special_label_leaves_the_column_null(label):
    config = _config({"state": "state", "district": "district"})
    assert fx.submission_geography(config, _result(state=(label, "extracted"))) == {"state": None, "district": None}
    assert fx.submission_geography(config, _result(district=(label, "extracted"))) == {"state": "Karnataka", "district": None}


@pytest.mark.parametrize("status", ["unmapped", "not_provided", "not_applicable"])
def test_only_an_extracted_value_is_copied(status):
    config = _config({"state": "state", "district": "district"})
    assert fx.submission_geography(config, _result(state=("Karnataka", status))) == {"state": None, "district": None}
    assert fx.submission_geography(config, _result(district=("Mysuru", status))) == {"state": "Karnataka", "district": None}


def test_a_district_is_never_copied_without_a_valid_state():
    config = _config({"state": "state", "district": "district"})
    assert fx.submission_geography(config, _result(state=("Unmapped", "extracted"))) == {"state": None, "district": None}


def test_a_state_derived_from_a_district_is_still_a_state():
    config = _config({"state": "state", "district": "district"})
    result = _result(state=("Uttar Pradesh", "extracted"), district=("Gautam Buddha Nagar", "extracted"))
    assert fx.submission_geography(config, result) == {"state": "Uttar Pradesh", "district": "Gautam Buddha Nagar"}


def test_a_special_answer_of_the_district_field_itself_is_never_a_district():
    config = fx.parse_extraction_config(_raw({"state": "state", "district": "district"}, ()))
    spec = {f.name: f for f in config.fields}["district"]
    assert "Outside India" in spec.categories
    assert fx.submission_geography(config, _result(district=("Outside India", "extracted")))["district"] is None


@pytest.mark.parametrize("copy, message", [
    ({}, "non-empty"), ([], "non-empty"), ("state", "non-empty"),
    ({"city": "state"}, "not one of"),
    ({"state": "nope"}, "must name a field"), ({"state": 3}, "must name a field"),
    ({"state": "tags"}, "single-value"),                     # multi-value
    ({"state": "age"}, "single-value"),                      # integer
    ({"district": "district"}, "together with a state"),
])
def test_a_bad_mapping_is_refused_when_the_config_is_parsed(copy, message):
    with pytest.raises(fx.ExtractionConfigError, match=message):
        fx.parse_extraction_config(_raw(copy))


def test_the_seeded_extraction_configs_all_copy_state_and_district():
    import re
    from pathlib import Path
    text = (Path(__file__).resolve().parent.parent / "seed_config.sql").read_text(encoding="utf-8")
    blocks = re.findall(r"\$cfg\$\n(.*?)\n\$cfg\$::jsonb", text, re.DOTALL)
    extraction = [json.loads(b) for b in blocks if '"fields"' in b and '"session_type"' in b]
    assert len(extraction) == 4
    for raw in extraction:
        assert raw["copy_to_submission"] == {"state": "state", "district": "district"}
        assert fx.parse_extraction_config(raw).submission_columns == {"state": "state", "district": "district"}
