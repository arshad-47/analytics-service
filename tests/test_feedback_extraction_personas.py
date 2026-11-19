"""
Pure tests of the per-persona extraction content: the DOTS metadata columns, bands and flags of each persona's seeded
config (the `-- CONFIG feedback_extraction any ...` blocks of seed_config.sql, read through tests/seed_support.py),
the persona prompts of seed_prompts.sql, and the contract between them (`prompt_name`). The real-Postgres parts
(the seed never overwrites an edited config) are in tests/test_feedback_extraction_personas_pg.py. No database, no
network, no LLM. SYNTHETIC conversations only.
"""
import json
import dataclasses
import re
from pathlib import Path

import pytest

from app.services import feedback_extraction as fx
from app.services.feedback_extraction_constants import ANALYSIS_TYPE, PII_TYPES

PROMPT_NAME = "Feedback Metadata Extraction"   # the program-neutral template prompt of seed_prompts.sql
from seed_support import PERSONAS, extraction_config_v1

ROOT = Path(__file__).resolve().parent.parent
SEED_PROMPTS = ROOT / "seed_prompts.sql"

# The DOTS export columns (the headers of the user's DOTS samples), in export order.
DOTS_FIELDS = {
    "Youth": ["gender", "age", "state", "district", "current_activity_status", "education_level", "course_details",
              "institution_type", "monthly_household_income", "annual_household_income", "disability", "disability_type"],
    "General Public": ["gender", "age", "state", "district", "occupation"],
    "Parent": ["state", "district", "child_gender", "disability", "disability_type", "respondent_relationship",
               "school_type", "child_grade", "parent_education", "parent_education_level"],
    "Educator": ["state", "district", "gender", "institution_type", "teaching_level"],
}
SENSITIVE = {
    "Youth": {"monthly_household_income", "annual_household_income", "disability", "disability_type"},
    "General Public": set(),
    "Parent": set(),
    "Educator": set(),
}
KNOWN_VARIABLES = {"persona", "fields", "field_names", "pii_types", "output_format", "conversation_id", "conversation"}
PERSONA_NAMES = list(DOTS_FIELDS)
# The persona prompts of seed_prompts.sql: the generic name, " - ", the persona name.
PERSONA_PROMPT_NAMES = {persona: f"{PROMPT_NAME} - {persona}" for persona in PERSONA_NAMES}


def _config(persona):
    return fx.parse_extraction_config(extraction_config_v1(persona))


# ------------------------------------------------------------------------------------------------ prompts from the SQL

def _unescape(text):
    return text.replace("''", "'")


def _seeded_prompt(sql, name):
    """(analysis_type, system, user, statements) of the prompt `name` as seeded in seed_prompts.sql (text parsing)."""
    head = re.search(
        r"INSERT INTO prompts \(name, analysis_type, created_at, updated_at\)\s*VALUES \('" + re.escape(name)
        + r"', '([a-z_]+)', now\(\), now\(\)\)\s*ON CONFLICT \(name\) DO NOTHING;", sql)
    assert head is not None, f"prompt {name!r} is not seeded (with ON CONFLICT (name) DO NOTHING)"
    start = sql.index("INSERT INTO prompt_version", head.end())
    end = sql.index("ON CONFLICT (prompt_id, version) DO NOTHING;", start)
    block = sql[start:end]
    assert f"WHERE p.name = '{name}'" in block
    strings = re.findall(r"E'((?:[^']|'')*)'", block)
    assert len(strings) == 2, "a persona prompt has exactly a system and a user text"
    assert re.search(r"\b1,\s*E'", block) and "TRUE," in block       # version 1, active
    return head.group(1), _unescape(strings[0]), _unescape(strings[1])


@pytest.fixture(scope="module")
def sql():
    return SEED_PROMPTS.read_text(encoding="utf-8")


# ------------------------------------------------------------------------------------------------ fields

@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_field_names_equal_the_dots_columns_exactly_and_in_order(persona):
    assert list(_config(persona).field_names()) == DOTS_FIELDS[persona]


@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_seed_config_parses_with_the_engine_parser_and_names_its_own_prompt(persona):
    raw = extraction_config_v1(persona)
    config = fx.parse_extraction_config(raw)
    assert raw["prompt_name"] == PERSONA_PROMPT_NAMES[persona] == config.prompt_name   # names its own prompt
    assert raw["prompt_name"].startswith(PROMPT_NAME + " - ") and raw["prompt_name"] == raw["prompt_name"].strip()
    assert config.session_type == PERSONAS[persona]["session_type"] and config.persona == persona
    assert config.pii_types == PII_TYPES
    assert len({extraction_config_v1(p)["prompt_name"] for p in PERSONA_NAMES}) == 4


@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_sensitive_flags_are_set_on_exactly_the_sensitive_fields(persona):
    flagged = {f.name for f in _config(persona).fields if f.sensitive}
    assert flagged == SENSITIVE[persona]


@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_no_field_is_provisional_because_the_category_lists_are_the_data_teams_finalised_ones(persona):
    assert not any(f.provisional for f in _config(persona).fields)


SPECIAL_PLACE_ANSWERS = ("Prefer not to say", "Unmapped", "Invalid response", "Outside India")
YOUTH_AGE_BANDS = ["< 12", "≥ 12 and < 16", "≥ 16 and < 22", "≥ 22 and < 26", "≥ 26 and < 30", "≥ 30 and < 35",
                   "≥ 35 and < 40", "≥ 40 and < 45", "≥ 45 and < 50", "≥ 50 and < 55", "≥ 55 and < 60", "≥ 60"]
GENERAL_PUBLIC_AGE_BANDS = ["< 12", "≥ 12 and < 26", "≥ 26 and < 36", "≥ 36 and < 46", "≥ 46 and < 60", "≥ 60"]
GENDERS = ("Male", "Female", "Others", "Prefer not to say", "Invalid response", "Unsure / don't know")


def test_state_is_the_36_states_and_uts_plus_the_special_answers_and_district_is_free_text_in_every_persona():
    youth_state = {f.name: f for f in _config("Youth").fields}["state"]
    states = youth_state.categories[:36]
    assert len(states) == 36 and len(set(states)) == 36
    assert youth_state.categories[36:] == SPECIAL_PLACE_ANSWERS
    for name in ("Dadra and Nagar Haveli and Daman and Diu", "Jammu and Kashmir", "Ladakh", "Delhi", "Telangana"):
        assert name in states
    assert "Delhi (NCT)" not in states
    for persona in PERSONA_NAMES:
        by_name = {f.name: f for f in _config(persona).fields}
        assert by_name["state"].categories == youth_state.categories
        assert by_name["district"].kind == "free_text" and by_name["district"].categories == SPECIAL_PLACE_ANSWERS


def test_every_variant_of_delhi_is_an_alias_of_the_one_delhi_value_and_a_region_is_decided_with_the_district():
    state = {f.name: f for f in _config("Youth").fields}["state"]
    assert set(state.aliases["Delhi"]) >= {"New Delhi", "NCT", "NCT of Delhi", "Delhi NCR", "Delhi/NCR", "NCR"}
    assert "it was derived" in state.description and "'Delhi/NCR'" in state.description
    assert "Noida" in state.description and "Uttar Pradesh" in state.description and "Gurugram" in state.description and "Haryana" in state.description
    assert "outside india" in state.description.lower()          # a place outside India makes the district 'Outside India' too
    for persona in PERSONA_NAMES:
        assert {f.name: f for f in _config(persona).fields}["state"].aliases == state.aliases


def test_age_is_a_banded_number_for_youth_and_general_public_only():
    for persona, bands in (("Youth", YOUTH_AGE_BANDS), ("General Public", GENERAL_PUBLIC_AGE_BANDS)):
        age = {f.name: f for f in _config(persona).fields}["age"]
        assert age.kind == "integer" and (age.min_value, age.max_value) == (0, 120)
        assert [b.label for b in age.bands] == bands
        assert age.categories == ("Prefer not to say", "Unclear", "Invalid response")
    for persona in ("Parent", "Educator"):
        assert "age" not in _config(persona).field_names()


def test_gender_lists_are_the_same_in_every_persona_and_a_parent_may_name_several_children():
    for persona, name in (("Youth", "gender"), ("General Public", "gender"), ("Educator", "gender"), ("Parent", "child_gender")):
        assert {f.name: f for f in _config(persona).fields}[name].categories == GENDERS
    assert {f.name: f for f in _config("Parent").fields}["child_gender"].multi


def test_youth_status_education_and_course_lists_are_the_finalised_ones_and_depend_on_each_other():
    by_name = {f.name: f for f in _config("Youth").fields}
    assert by_name["current_activity_status"].categories == (
        "Studying", "Working", "Self-employed", "Looking for work/unemployed", "Not currently studying", "Others",
        "Prefer not to say", "Invalid response")
    assert by_name["current_activity_status"].multi
    assert by_name["education_level"].categories == (
        "Grade 6 to 10", "Grade 11 to 12", "Skill programs", "Pursuing undergraduate and postgraduate", "Others",
        "Prefer not to say", "Invalid response")
    assert by_name["education_level"].applies_when == ("current_activity_status", ("Studying",))
    assert by_name["course_details"].applies_when == (
        "education_level", ("Grade 11 to 12", "Skill programs", "Pursuing undergraduate and postgraduate"))
    assert by_name["course_details"].categories == (
        "Science", "Commerce", "Electrical or electronics", "Mechanical or manufacturing", "Automobile or transport",
        "Civil or construction", "Computer or IT", "Health", "Beauty", "Hospitality", "Arts (BA or MA)",
        "Science (B.Sc or M.Sc)", "Commerce (B.Com or M.Com)", "Technology (B.Tech or M.Tech)",
        "Business administration (BBA or MBA)", "Computer applications (BCA or MCA)", "Medicine (MBBS or MD)",
        "Law (LLB or LLM)", "Others", "Prefer not to say", "Invalid response")
    assert by_name["institution_type"].categories == (
        "Government", "Private", "Government-aided", "Others", "Not sure", "Prefer not to say", "Invalid response")


def test_youth_income_has_a_monthly_band_list_for_v2_and_an_annual_one_for_v1_with_the_symbols_the_data_team_chose():
    by_name = {f.name: f for f in _config("Youth").fields}
    monthly, annual = by_name["monthly_household_income"], by_name["annual_household_income"]
    assert monthly.categories == (
        "Zero income / no income", "< ₹25,000", "≥ ₹25,000 and < ₹66,000", "≥ ₹66,000 and < ₹83,000",
        "≥ ₹83,000", "Don't know", "Prefer not to say", "Invalid response", "Unclear")
    assert annual.categories == (
        "Zero income / no income", "< ₹3 lakh", "≥ ₹3 lakh and < ₹8 lakh", "≥ ₹8 lakh and < ₹10 lakh",
        "≥ ₹10 lakh", "Don't know", "Prefer not to say", "Invalid response", "Unclear")
    assert monthly.versions == ("V2",) and annual.versions == ("V1",) and monthly.sensitive and annual.sensitive
    assert "divide the amount by 12" in monthly.description and "MONTH" in monthly.description
    assert "multiply the amount by 12" in annual.description and "YEAR" in annual.description
    assert "'around 25,000' belongs here" in monthly.guidance["≥ ₹25,000 and < ₹66,000"]


def test_disability_fields_are_sensitive_and_disability_type_is_a_multi_value_category_that_needs_a_yes():
    for persona in ("Youth", "Parent"):
        by_name = {f.name: f for f in _config(persona).fields}
        assert by_name["disability"].categories == ("Yes", "No", "Prefer not to say", "Invalid response")
        kind = by_name["disability_type"]
        assert kind.kind == "category" and kind.multi
        if persona == "Youth":
            assert kind.sensitive and by_name["disability"].sensitive
        else:
            assert not kind.sensitive and not by_name["disability"].sensitive
        assert kind.categories == ("Physical disability", "Intellectual disability", "Others", "Prefer not to say", "Invalid response")
        assert kind.applies_when == ("disability", ("Yes",))
    assert "CHILD" in {f.name: f for f in _config("Parent").fields}["disability"].description


def test_parent_child_fields_are_about_the_child_and_parent_fields_about_the_parent():
    by_name = {f.name: f for f in _config("Parent").fields}
    assert "CHILD" in by_name["child_gender"].description and "not of the parent" in by_name["child_gender"].description
    grade = by_name["child_grade"]
    assert grade.categories == (
        "Pre-primary", "Primary (classes 1-5)", "Middle (classes 6-8)", "High school (classes 9-12)",
        "Undergraduate/postgraduate", "Skill programs", "Others", "Prefer not to say", "Invalid response",
        "Unsure / don't know", "Out of school / not currently enrolled")
    assert grade.multi and grade.versions == ("V1",)
    assert by_name["respondent_relationship"].categories == (
        "Mother", "Father", "Guardian", "Other relative", "Unmapped", "Prefer not to say", "Invalid response")
    assert by_name["school_type"].categories == ("Government", "Private", "Others", "Not sure", "Prefer not to say", "Invalid response")
    for name in ("parent_education", "parent_education_level"):
        assert "RESPONDENT" in by_name[name].description and "not the child" in by_name[name].description
    assert by_name["parent_education"].categories == ("Yes", "No", "Prefer not to say", "Invalid response")
    assert by_name["parent_education_level"].categories == grade.categories[:-1] + ("Did not go to school",)


def test_educator_lists_are_the_finalised_ones_and_a_teacher_of_several_levels_gets_each_once():
    by_name = {f.name: f for f in _config("Educator").fields}
    level = by_name["teaching_level"]
    assert level.categories == ("Pre-primary", "Primary (classes 1-5)", "Middle (classes 6-8)", "High school (classes 9-12)",
                                "Undergraduate", "Postgraduate", "Others", "Prefer not to say", "Invalid response")
    assert level.multi and "Postgraduate only when" in level.description
    assert by_name["institution_type"].categories == ("Government", "Private", "Others", "Not sure", "Prefer not to say", "Invalid response")


def test_general_public_occupation_is_the_finalised_sector_list():
    occupation = {f.name: f for f in _config("General Public").fields}["occupation"]
    assert occupation.categories == (
        "Education", "Health", "Government and public services", "IT and services", "Manufacturing", "Agriculture",
        "Retail and trade", "NGO or social sector", "Others", "Not currently working", "Homemaker", "Student",
        "Prefer not to say", "Unclear", "Invalid response")


def test_labels_use_one_spelling_across_personas_and_fields():
    """The normalised set: sentence case, 'Others', 'Invalid response', 'Prefer not to say', 'Not sure' ... everywhere."""
    labels = set()
    for persona in PERSONA_NAMES:
        for f in _config(persona).fields:
            labels.update(f.categories)
    for wrong in ("Other", "Invalid", "Invalid responses", "Invalid Response", "Prefer Not to Say", "Not Answered", "Not answered",
                  "N/A", "Not Sure", "Unsure / Don't Know", "Zero Income / No Income"):
        assert wrong not in labels, wrong
    assert {"Others", "Invalid response", "Prefer not to say", "Not sure", "Unsure / don't know"} <= labels
    for persona in PERSONA_NAMES:                       # no comma in the label of a multi-value field
        for f in _config(persona).fields:
            if f.multi:
                assert all("," not in c for c in f.categories), f.name


# ------------------------------------------------------------------------------------------------ prompts

@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_every_prompt_name_resolves_to_a_seeded_active_version_1_of_the_same_analysis_type(sql, persona):
    name = extraction_config_v1(persona)["prompt_name"]
    analysis_type, system, user = _seeded_prompt(sql, name)
    assert analysis_type == ANALYSIS_TYPE == "feedback_metadata_extraction"
    assert len(system) > 500 and len(user) > 100


def test_the_generic_prompt_is_still_seeded_as_the_fallback_and_has_its_own_name(sql):
    analysis_type, system, user = _seeded_prompt(sql, PROMPT_NAME)
    assert analysis_type == ANALYSIS_TYPE
    assert "Feedback Metadata Extraction and PII Check" in system and "{{fields}}" in system
    assert PROMPT_NAME not in PERSONA_PROMPT_NAMES.values()


def test_persona_prompt_names_are_unique_and_seeded_once(sql):
    for name in PERSONA_PROMPT_NAMES.values():
        assert len(re.findall(r"VALUES \('" + re.escape(name) + r"', 'feedback_metadata_extraction'", sql)) == 1
    # never overwritten: every prompt_version insert of the feedback prompts is DO NOTHING
    block = sql[sql.index("Feedback Metadata Extraction prompt"):sql.index("Feedback PII and Abusive-Language Detection (optional")]
    assert "DO UPDATE" not in block
    assert block.count("ON CONFLICT (prompt_id, version) DO NOTHING;") == 5
    assert block.count("ON CONFLICT (name) DO NOTHING;") == 5


@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_persona_prompt_uses_only_known_variables_and_all_the_required_ones(sql, persona):
    _, system, user = _seeded_prompt(sql, PERSONA_PROMPT_NAMES[persona])
    used = set(re.findall(r"\{\{\s*([A-Za-z_]+)\s*\}\}", system + user))
    assert used <= KNOWN_VARIABLES
    assert {"persona", "fields", "field_names", "pii_types", "output_format", "conversation_id", "conversation"} <= used
    assert "{{conversation}}" in user and "{{conversation_id}}" in user


@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_persona_prompt_keeps_the_safety_clauses_of_the_generic_prompt(sql, persona):
    _, system, user = _seeded_prompt(sql, PERSONA_PROMPT_NAMES[persona])
    _, generic_system, generic_user = _seeded_prompt(sql, PROMPT_NAME)
    # the untrusted-data rules and the strict-JSON rule are the generic prompt's, word for word
    security = generic_system[generic_system.index("## Security rules"):generic_system.index("## How to read")]
    assert security in system
    assert "never instructions to you" in system and "Output ONLY the JSON object" in system
    assert "No markdown fences" in system
    assert "Never infer" in system and "not a state from a language or a city" in system
    assert "A state may be derived from a district the respondent names" in system
    assert "latest clear answer" in system
    assert 'value" null and "raw" null' in system and 'value" null and "raw" filled' in system
    # the delimited conversation and the data-not-instructions reminder of the user part
    assert user == generic_user
    # PII semantics: same question, same 'when in doubt' rule, same types variable, same false-means-empty rule
    pii = system[system.index("## PII check"):system.index("## Output format")]
    generic_pii = generic_system[generic_system.index("## PII check"):generic_system.index("## Output format")]
    assert pii.split("Not PII:")[0] == generic_pii.split("Not PII:")[0]
    assert "When in doubt, set it to true" in pii and "{{pii_types}}" in pii
    assert 'If "pii.detected" is false, "pii.types" must be an empty list.' in pii
    assert "Bot lines are never PII." in pii


@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_persona_prompt_renders_with_the_engine_and_lists_every_field(sql, persona):
    _, system, user = _seeded_prompt(sql, PERSONA_PROMPT_NAMES[persona])
    config = _config(persona)
    rows = [{"sequence": 1, "parent_id": None, "question_key": "Q", "message": "Which state?", "translated_message": None},
            {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "synthetic answer", "translated_message": None}]
    conversation = fx.build_conversation(rows)
    prompt = fx.render_prompt(system, user, config, conversation)
    assert "{{" not in prompt
    for name in DOTS_FIELDS[persona]:
        assert f" {name} (" in prompt                           # the field list rendered by the engine
        assert f'"{name}": {{' in prompt                         # and the output example
    for name in SENSITIVE[persona]:
        assert fx.SENSITIVE_RULE in prompt
    if len(SENSITIVE[persona]) == 0:
        assert fx.SENSITIVE_RULE not in prompt
    assert prompt.count(conversation.conversation_id) == 3
    assert persona in prompt


def test_persona_prompts_are_specific_to_their_persona(sql):
    prompts = {p: _seeded_prompt(sql, PERSONA_PROMPT_NAMES[p])[1] for p in PERSONA_NAMES}
    # Youth: a period conversion for income, several statuses, a dropout rule
    assert "MONTHLY or YEARLY" in prompts["Youth"] and "convert first" in prompts["Youth"]
    assert "Not currently studying" in prompts["Youth"] and "both Studying and Working" in prompts["Youth"]
    assert "disability_type: the kinds they name" in prompts["Youth"]
    # General Public: sector of work
    assert "sector" in prompts["General Public"] and "Others" in prompts["General Public"] and "Unclear" in prompts["General Public"]
    # Parent: child vs parent, several children
    assert "CHILD" in prompts["Parent"] and "parent_education" in prompts["Parent"] and "Other relative" in prompts["Parent"]
    assert "every gender that applies" in prompts["Parent"]
    # Educator: the levels and several of them
    assert "High school (classes 9-12)" in prompts["Educator"] and "every band that applies" in prompts["Educator"]
    # every persona prompt tells the model how a multi-value answer is written
    for text in prompts.values():
        assert 'a field marked "one or more", a JSON list' in text
    # each persona prompt is more specific than the generic one but stays short (one call per session)
    generic = _seeded_prompt(sql, PROMPT_NAME)[1]
    for text in prompts.values():
        assert len(text) > len(generic) and len(text) < 2 * len(generic) + 2500


def test_prompts_contain_no_real_data_markers_and_no_hard_coded_dates_or_bot_ids(sql):
    for persona in PERSONA_NAMES:
        _, system, user = _seeded_prompt(sql, PERSONA_PROMPT_NAMES[persona])
        assert re.search(r"\b20\d\d-\d\d-\d\d\b|\bbot\s*\d+\b|\bV[12]\b", system + user, re.I) is None


# ------------------------------------------------------------------------------------------------ engine round trip

def _answer(config, values, pii=(False, [])):
    entries = {n: {"value": None, "raw": None} for n in config.field_names()}
    for name, (value, raw) in values.items():
        entries[name] = {"value": value, "raw": raw}
    return json.dumps({"fields": entries, "pii": {"detected": pii[0], "types": pii[1]}})


def test_a_synthetic_youth_answer_maps_every_kind_and_sensitive_fields_stay_optional():
    config = _config("Youth")
    answer = _answer(config, {
        "age": (14.5, "fourteen and a half"), "gender": ("Female", "girl"), "state": ("Karnataka", "karnataka"),
        "district": ("Mysuru", "mysore"), "current_activity_status": (["Studying", "Working"], "in school, part time job"),
        "education_level": ("Grade 6 to 10", "class 9"), "institution_type": ("Government", "govt"),
        "monthly_household_income": ("< \u20b925,000", "3 lakh a year"),
    })
    fields = {f.field_name: f for f in fx.parse_model_response(answer, config).fields}
    assert (fields["age"].value, fields["age"].raw_value, fields["age"].status) == ("\u2265 12 and < 16", "fourteen and a half", "extracted")
    assert fields["current_activity_status"].value == "Studying, Working" and fields["current_activity_status"].value_list == ("Studying", "Working")
    assert fields["monthly_household_income"].value == "< \u20b925,000"
    assert fields["disability"].status == "not_provided" and fields["course_details"].status == "not_provided"


def test_out_of_range_age_and_a_stated_value_that_fits_no_band_are_unmapped_with_the_words_kept():
    config = _config("Youth")
    answer = _answer(config, {"age": ("244", "244"), "monthly_household_income": (None, "between 60 and 90 thousand")})
    fields = {f.field_name: f for f in fx.parse_model_response(answer, config).fields}
    assert (fields["age"].status, fields["age"].value, fields["age"].raw_value) == ("unmapped", "Invalid response", "244")
    # monthly_household_income is a sensitive field: its words are not stored (raw_value NULL) unless store_raw is set
    assert (fields["monthly_household_income"].status, fields["monthly_household_income"].raw_value) == ("unmapped", None)


def test_each_persona_accepts_an_answer_with_every_configured_field_and_rejects_a_missing_or_extra_one():
    for persona in PERSONA_NAMES:
        config = _config(persona)
        run = fx.extract([{"sequence": 1, "parent_id": None, "question_key": "Q", "message": "Q?", "translated_message": None},
                          {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "x", "translated_message": None}],
                         config, "{{persona}} {{fields}} {{output_format}}", "{{conversation}}",
                         lambda prompt: (_answer(config, {}), {}))
        assert [f.field_name for f in run.result.fields] == DOTS_FIELDS[persona]
        broken = json.loads(_answer(config, {}))
        del broken["fields"][DOTS_FIELDS[persona][0]]
        with pytest.raises(fx.ExtractionOutputError):
            fx.parse_model_response(json.dumps(broken), config)
        broken = json.loads(_answer(config, {}))
        broken["fields"]["school_name"] = {"value": None, "raw": None}
        with pytest.raises(fx.ExtractionOutputError):
            fx.parse_model_response(json.dumps(broken), config)


def test_parent_child_grade_and_educator_level_bands_map_and_others_do_not_coerce():
    parent = _config("Parent")
    fields = {f.field_name: f for f in fx.parse_model_response(
        _answer(parent, {"child_grade": ("Middle (classes 6-8)", "6th"), "parent_education_level": ("Did not go to school", "never"),
                         "respondent_relationship": ("Aunt", "aunt")}), parent).fields}
    assert fields["child_grade"].value == "Middle (classes 6-8)"
    assert fields["parent_education_level"].value == "Did not go to school"
    # "Aunt" is no value of the list: it is not coerced, the field's own Unmapped stands in and the words stay
    assert (fields["respondent_relationship"].status, fields["respondent_relationship"].value,
            fields["respondent_relationship"].raw_value) == ("unmapped", "Unmapped", "aunt")
    educator = _config("Educator")
    level = {f.field_name: f for f in fx.parse_model_response(
        _answer(educator, {"teaching_level": ("High school", "high school")}), educator).fields}["teaching_level"]
    assert level.status == "unmapped" and level.raw_value == "high school"      # a bare 'high school' is not a band
    assert level.value == "Others"                                              # the field's fallback, never an empty value


# ------------------------------------------------------------------------------------------------ generic, config-driven seeds

@pytest.mark.parametrize("persona", PERSONA_NAMES)
def test_every_seeded_config_states_its_limits_pii_policy_and_placeholders_explicitly(persona):
    raw = extraction_config_v1(persona)
    config = fx.parse_extraction_config(raw)
    assert raw["limits"] == {"max_respondent_message_chars": 20000, "max_bot_message_chars": 4000,
                             "max_conversation_chars": 200000, "max_value_chars": 120, "max_raw_chars": 500}
    assert config.limits == fx.Limits(20000, 4000, 200000, 120, 500)                       # the documented defaults
    assert raw["pii"]["min_digit_run"] == 8 and raw["pii"]["min_digit_run_short_groups"] == 10
    assert tuple(raw["pii"]["url_tlds"]) == fx.DEFAULT_PII_URL_TLDS
    # the defaults plus the model-based masking prompt that every persona switches on
    assert raw["pii"]["masking_prompt_name"] == "Feedback PII and Abusive-Language Detection"
    assert config.pii_policy == dataclasses.replace(
        fx.DEFAULT_PII_POLICY, masking_prompt_name="Feedback PII and Abusive-Language Detection")
    assert all(isinstance(t, dict) and set(t) == {"name", "description"} for t in raw["pii"]["types"])
    assert config.pii_types == PII_TYPES and set(config.pii_type_descriptions) == set(PII_TYPES)
    assert "id_number" in config.pii_type_descriptions and "Aadhaar" in config.pii_type_descriptions["id_number"]   # program data
    assert "n/a" in config.placeholders and "नहीं पता" in config.placeholders                 # a non-English word is data too


def test_the_generic_prompt_is_program_neutral_and_only_a_template():
    sql = SEED_PROMPTS.read_text(encoding="utf-8")
    _, system, user = _seeded_prompt(sql, PROMPT_NAME)
    text = system + user
    for word in ("One Million", "India", "Indian", "Youth", "Parent", "state, district", "government, private"):
        assert word not in text, word
    config = fx.parse_extraction_config(extraction_config_v1("Youth"))
    prompt = fx.render_prompt(system, user, config, fx.build_conversation([]))        # renders with the engine's own variables
    assert "{{" not in prompt
