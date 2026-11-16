"""
Pure tests of the per-persona extraction content: the DOTS metadata columns, bands and flags of each persona's seeded
config (the `-- CONFIG feedback_extraction any ...` blocks of seed_config.sql, read through tests/seed_support.py),
the persona prompts of seed_prompts.sql, and the contract between them (`prompt_name`). The real-Postgres parts
(the seed never overwrites an edited config) are in tests/test_feedback_extraction_personas_pg.py. No database, no
network, no LLM. SYNTHETIC conversations only.
"""
import json
import re
from pathlib import Path

import pytest

from app.services import feedback_extraction as fx
from app.services.feedback_extraction_constants import ANALYSIS_TYPE, PII_TYPES

PROMPT_NAME = "Feedback Metadata Extraction"   # the program-neutral template prompt of seed_prompts.sql
from seed_support import PERSONAS, extraction_config_v1

ROOT = Path(__file__).resolve().parent.parent
SEED_PROMPTS = ROOT / "seed_prompts.sql"
BANDS_DOC = ROOT / "docs" / "feedback-extraction-bands.md"

# The DOTS export columns (the headers of the user's DOTS samples), in export order.
DOTS_FIELDS = {
    "Youth": ["age", "gender", "state", "district", "current_activity_status", "education_level", "course_details",
              "institution_type", "monthly_household_income", "disability", "disability_type"],
    "General Public": ["age", "gender", "state", "district", "occupation"],
    "Parent": ["state", "district", "child_gender", "disability", "disability_type", "respondent_relationship",
               "school_type", "child_grade", "parent_education", "parent_education_level"],
    "Educator": ["state", "district", "gender", "institution_type", "teaching_level"],
}
SENSITIVE = {
    "Youth": {"monthly_household_income", "disability", "disability_type"},
    "General Public": set(),
    "Parent": {"disability", "disability_type"},
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
def test_every_field_is_provisional_except_state_and_district(persona):
    for f in _config(persona).fields:
        assert f.provisional is (f.name not in ("state", "district")), f.name


def test_state_is_the_36_states_and_uts_district_is_free_text_in_every_persona():
    youth_states = {f.name: f for f in _config("Youth").fields}["state"].categories
    assert len(youth_states) == 36 and len(set(youth_states)) == 36
    for name in ("Dadra and Nagar Haveli and Daman and Diu", "Jammu and Kashmir", "Ladakh", "Delhi (NCT)", "Telangana"):
        assert name in youth_states
    for persona in PERSONA_NAMES:
        by_name = {f.name: f for f in _config(persona).fields}
        assert by_name["state"].categories == youth_states
        assert by_name["district"].kind == "free_text"


def test_age_is_an_integer_field_with_bounds_for_youth_and_general_public_only():
    for persona in ("Youth", "General Public"):
        age = {f.name: f for f in _config(persona).fields}["age"]
        assert age.kind == "integer" and (age.min_value, age.max_value) == (5, 100)
    for persona in ("Parent", "Educator"):
        assert "age" not in _config(persona).field_names()


def test_youth_income_is_a_monthly_band_list_that_tells_the_model_to_convert_annual_to_monthly():
    income = {f.name: f for f in _config("Youth").fields}["monthly_household_income"]
    assert income.kind == "category" and income.categories == (
        "Less than ₹25,000", "₹25,000 - ₹66,000", "₹67,000 - ₹83,000", "Above ₹83,000", "Don't know")
    assert "divide the amount by 12" in income.description and "MONTH" in income.description
    # the story's gap (66,001 to 66,999) is closed in the guidance
    assert "between 66,000 and 67,000" in income.guidance["₹25,000 - ₹66,000"]


def test_disability_fields_are_sensitive_and_disability_type_is_free_text_in_youth_and_parent():
    for persona in ("Youth", "Parent"):
        by_name = {f.name: f for f in _config(persona).fields}
        assert by_name["disability"].categories == ("Yes", "No", "Prefer not to say", "Invalid")
        assert by_name["disability_type"].kind == "free_text" and by_name["disability_type"].sensitive
    assert "CHILD" in {f.name: f for f in _config("Parent").fields}["disability"].description


def test_parent_child_fields_are_about_the_child_and_parent_fields_about_the_parent():
    by_name = {f.name: f for f in _config("Parent").fields}
    assert "CHILD" in by_name["child_gender"].description and "not of the parent" in by_name["child_gender"].description
    assert by_name["child_grade"].categories == (
        "Primary (Classes 1-5)",
        "Middle (Classes 6-8)",
        "Secondary (Classes 9-10)",
        "Higher Secondary (Classes 11-12)",
        "College/University",
        "Other",
    )
    assert by_name["respondent_relationship"].categories == ("Mother", "Father", "Guardian")
    assert by_name["school_type"].categories == ("Government", "Private", "Others", "Invalid")
    for name in ("parent_education", "parent_education_level"):
        assert "RESPONDENT" in by_name[name].description and "not the child" in by_name[name].description
    assert "Did Not Go to School" in by_name["parent_education_level"].categories


def test_educator_teaching_level_keeps_the_story_bands_and_says_when_they_cannot_be_derived():
    level = {f.name: f for f in _config("Educator").fields}["teaching_level"]
    assert level.categories == ("Primary", "Middle", "Secondary", "Higher Secondary", "Undergraduate",
                                "Postgraduate", "Others", "Invalid")
    assert "only when the respondent names" in level.description
    assert level.provisional


def test_youth_keeps_the_existing_names_and_has_no_school_type_but_institution_type():
    names = _config("Youth").field_names()
    assert "institution_type" in names and "school_type" not in names
    assert {f.name: f for f in _config("Youth").fields}["institution_type"].categories == (
        "Government", "Government-aided", "Private", "Others", "Invalid")


def test_general_public_occupation_is_the_story_sector_list():
    occupation = {f.name: f for f in _config("General Public").fields}["occupation"]
    assert occupation.categories == (
        "Education", "Health", "Government and public services", "IT and services", "Manufacturing", "Agriculture",
        "Retail and trade", "NGO or social sector", "Others", "Invalid")


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
    block = sql[sql.index("Feedback Metadata Extraction prompt"):]
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
    assert "Never infer" in system and "not a state from a language, a city or a district" in system
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
    # Youth: annual to monthly before banding
    assert "always MONTHLY" in prompts["Youth"] and "divide by 12" in prompts["Youth"]
    assert "annual" in prompts["Youth"] and "per month" in prompts["Youth"]
    assert "disability_type: only when disability is Yes" in prompts["Youth"]
    # General Public: sector of work
    assert "sector" in prompts["General Public"] and "Others" in prompts["General Public"]
    # Parent: child vs parent
    assert "CHILD" in prompts["Parent"] and "parent_education" in prompts["Parent"] and "Guardian" in prompts["Parent"]
    # Educator: the teaching level cannot always be derived
    assert "ONLY when the respondent names the classes" in prompts["Educator"]
    assert "value null" in prompts["Educator"]
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
        "age": ("14", "fourteen"), "gender": ("Female", "girl"), "state": ("Karnataka", "karnataka"),
        "district": ("Mysuru", "mysore"), "current_activity_status": ("Studying", "in school"),
        "education_level": ("9", "class 9"), "institution_type": ("Government", "govt"),
        "monthly_household_income": ("Less than ₹25,000", "3 lakh a year"),
    })
    fields = {f.field_name: f for f in fx.parse_model_response(answer, config).fields}
    assert (fields["age"].value, fields["age"].status) == ("14", "extracted")
    assert fields["monthly_household_income"].value == "Less than ₹25,000"
    assert fields["disability"].status == "not_provided" and fields["course_details"].status == "not_provided"


def test_out_of_range_age_and_a_stated_value_that_fits_no_band_are_unmapped_with_the_words_kept():
    config = _config("Youth")
    answer = _answer(config, {"age": ("244", "244"), "monthly_household_income": (None, "between 60 and 90 thousand")})
    fields = {f.field_name: f for f in fx.parse_model_response(answer, config).fields}
    assert (fields["age"].status, fields["age"].value, fields["age"].raw_value) == ("unmapped", None, "244")
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
        _answer(parent, {"child_grade": ("Middle (Classes 6-8)", "6th"), "parent_education_level": ("Did Not Go to School", "never"),
                         "respondent_relationship": ("Aunt", "aunt")}), parent).fields}
    assert fields["child_grade"].value == "Middle (Classes 6-8)"
    assert fields["parent_education_level"].value == "Did Not Go to School"
    assert (fields["respondent_relationship"].status, fields["respondent_relationship"].raw_value) == ("unmapped", "aunt")
    educator = _config("Educator")
    level = {f.field_name: f for f in fx.parse_model_response(
        _answer(educator, {"teaching_level": ("High school", "high school")}), educator).fields}["teaching_level"]
    assert level.status == "unmapped" and level.raw_value == "high school"      # a bare 'high school' is not a band


# ------------------------------------------------------------------------------------------------ doc

def test_bands_doc_lists_every_field_of_every_persona_and_where_the_bands_live():
    text = BANDS_DOC.read_text(encoding="utf-8")
    for persona, names in DOTS_FIELDS.items():
        assert persona in text
        for name in names:
            assert f"`{name}`" in text, (persona, name)
    assert "seed_config.sql" in text
    assert "python tools/run_feedback_extraction.py upgrade-config" not in text      # the command is gone
    assert "seed_feedback_extraction" not in text
    for prompt_name in PERSONA_PROMPT_NAMES.values():
        assert prompt_name in text
    assert "Changing a band or a field" in text and "How an existing database picks this up" in text


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
    assert config.pii_policy == fx.DEFAULT_PII_POLICY
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
