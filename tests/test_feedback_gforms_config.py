"""
Tests for the Google Forms upload config (docs/feedback-upload-plan.md, Phase 2, slice 2a):
`gforms_config_v1(persona)` of tests/seed_support.py (the seeded blocks of seed_config.sql).

Everything here is synthetic or a tracked fixture: the launch-form header rows are BUILT from the config the way a
form export prints them (question wording, a script-suffixed bilingual variant, a stray unnamed column, the e-mail
column), never read from a file outside the repository.
"""
import csv
import json
import re
from pathlib import Path

import pytest

from app.api.services.feedback_gforms import normalise_header
from seed_support import GFORMS_PERSONAS, PERSONAS, SESSION_TYPE_TO_PERSONA, gforms_config_v1, mitra_config_v1

FIXTURES = Path(__file__).resolve().parent / "csv_uploads"
# a Kannada script suffix as the bilingual launch forms print it after the English question
_BILINGUAL_SUFFIX = " / \u0c95\u0ca8\u0ccd\u0ca8\u0ca1"


def _questions(persona):
    for form in gforms_config_v1(persona)["forms"].values():
        yield from form["questions"]


def _header_map(persona):
    """normalised header -> question_key, across every form of the persona."""
    return {h: q["question_key"] for q in _questions(persona) for h in q["headers"]}


def _resolves(persona, header_row):
    """Mirrors the adapter's column rule: ignored by name / pattern, else the normalised header is configured."""
    cfg = gforms_config_v1(persona)
    known = _header_map(persona)
    unresolved = []
    for col in header_row:
        if col == cfg["timestamp_column"] or col in cfg["ignored_columns"]:
            continue
        if any(re.search(pattern, col) for pattern in cfg["ignored_column_patterns"]):
            continue
        if normalise_header(col) not in known:
            unresolved.append(col)
    return unresolved


def _launch_header(persona, wording=0, bilingual=False, stray_column=None):
    """
    A header row as the persona's launch-form export prints it: Timestamp, one column per configured question (the
    `wording`-th header variant where the config lists several), optionally with a script-suffixed bilingual form of
    the question, and optionally a stray unnamed trailing column. Questions only, no answer.
    """
    header = ["Timestamp"]
    for q in _questions(persona):
        text = q["headers"][min(wording, len(q["headers"]) - 1)]
        header.append(text + _BILINGUAL_SUFFIX if bilingual else text)
    if stray_column is not None:
        header.append(stray_column)
    return header


def _tracked_header(name):
    with (FIXTURES / name).open(encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f))


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_config_is_json_serialisable_and_round_trips(persona):
    cfg = gforms_config_v1(persona)
    assert json.loads(json.dumps(cfg)) == cfg


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_config_top_level_contract(persona):
    cfg = gforms_config_v1(persona)
    assert cfg["adapter"] == "wide_form"
    # keys that no code reads were removed from the seed (they looked like settings and did nothing)
    assert "layout" not in cfg and "id_strategy" not in cfg
    assert cfg["timestamp_column"] == "Timestamp"
    assert cfg["default_completion"] == "COMPLETED"
    assert "%m/%d/%Y %H:%M:%S" in cfg["datetime"]["formats"]
    assert cfg["datetime"]["fallback_timezone"] == "Asia/Kolkata"
    assert cfg["ignored_columns"] == ["Email Address"]
    assert cfg["ignored_column_patterns"] == ["^Column \\d+$"]
    # script detection looks at open-question answers only: bilingual forms print choice answers in both languages
    assert cfg["language"]["default"] == "en"
    assert cfg["language"]["script_detect"] == {"kn": "[\\u0C80-\\u0CFF]"}
    assert cfg["language"]["detect_kinds"] == ["open"]
    assert cfg["language"].get("english_only") is True
    assert cfg["forms"], "at least one form"
    for pattern in cfg["ignored_column_patterns"] + list(cfg["language"]["script_detect"].values()):
        re.compile(pattern)
    # month-first, no timezone: the sample from the export notes parses with the configured format
    from datetime import datetime
    assert datetime.strptime("9/26/2026 9:58:05", cfg["datetime"]["formats"][0]) == datetime(2026, 9, 26, 9, 58, 5)


def test_each_call_returns_an_independent_copy():
    first = gforms_config_v1("Youth")
    first["forms"].clear()
    assert gforms_config_v1("Youth")["forms"]


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_every_question_is_complete(persona):
    for form_key, form in gforms_config_v1(persona)["forms"].items():
        assert isinstance(form_key, str) and form_key
        assert isinstance(form["name"], str) and form["name"]
        assert form["questions"]
        for q in form["questions"]:
            assert set(q) == {"headers", "question_key", "text", "kind"}
            assert isinstance(q["headers"], list) and q["headers"]
            assert all(isinstance(h, str) and h for h in q["headers"])
            assert isinstance(q["question_key"], str) and q["question_key"]
            assert re.fullmatch(r"[A-Z][A-Z0-9_]*", q["question_key"]), q["question_key"]
            assert isinstance(q["text"], str) and q["text"]
            assert q["kind"] in ("metadata", "open")


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_headers_are_already_normalised(persona):
    for q in _questions(persona):
        for h in q["headers"]:
            assert normalise_header(h) == h, h
        assert normalise_header(q["text"]) == q["text"]


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_no_header_maps_to_two_question_keys_within_a_form(persona):
    for form_key, form in gforms_config_v1(persona)["forms"].items():
        seen = {}
        for q in form["questions"]:
            for h in q["headers"]:
                assert seen.setdefault(h, q["question_key"]) == q["question_key"], (form_key, h)
        # and no header is listed twice for the same key either: one column, one question
        flat = [h for q in form["questions"] for h in q["headers"]]
        assert len(flat) == len(set(flat)), form_key


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_each_form_has_metadata_and_open_questions(persona):
    for form in gforms_config_v1(persona)["forms"].values():
        kinds = {q["kind"] for q in form["questions"]}
        assert kinds == {"metadata", "open"}


def test_each_persona_has_the_expected_launch_forms():
    expected_forms = {
        "Youth": ["youth_launch"],
        "Parent": ["parents_english_launch", "parents_survey_v2"],
        "General Public": ["general_public_english_launch"],
    }
    assert {p: list(gforms_config_v1(p)["forms"]) for p in GFORMS_PERSONAS} == expected_forms
    assert {p: len(list(_questions(p))) for p in GFORMS_PERSONAS} == {"Youth": 18, "Parent": 31, "General Public": 11}


def test_demographic_questions_are_metadata():
    for persona in GFORMS_PERSONAS:
        for q in _questions(persona):
            if q["question_key"] in ("AGE_IDENTIFICATION", "GENDER_IDENTIFICATION",
                                     "STATE_IDENTIFICATION", "DISTRICT_IDENTIFICATION"):
                assert q["kind"] == "metadata", (persona, q["question_key"])


def test_persona_first_question_matches_the_mitra_catalog():
    # the Mitra config's synthesised first question must exist in the Google Forms form of the same persona,
    # with the same canonical key (one key = one question across sources)
    for persona, p in ((n, PERSONAS[n]) for n in GFORMS_PERSONAS):
        assert p["first_question_key"] in {q["question_key"] for q in _questions(persona)}


def test_youth_form_has_both_header_wordings():
    (form,) = gforms_config_v1("Youth")["forms"].values()
    (chance,) = [q for q in form["questions"]
                 if q["question_key"] == "CHANCE_TO_LEARN_NOW" and len(q["headers"]) > 1]
    assert chance["headers"] == [
        "For the things you just mentioned, are you getting the chance to learn them right now?",
        "Out of the things you just mentioned, are you getting the chance to learn them right now?",
    ]


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
@pytest.mark.parametrize("bilingual", [False, True])
def test_launch_form_headers_resolve_for_the_matching_persona(persona, bilingual):
    header = _launch_header(persona, bilingual=bilingual)
    assert header[0] == "Timestamp"
    assert _resolves(persona, header) == []


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_launch_form_headers_do_not_resolve_for_other_personas(persona):
    header = _launch_header(persona)
    for other in [p for p in GFORMS_PERSONAS if p != persona]:
        assert _resolves(other, header) != [], other


def test_a_form_that_lists_two_wordings_of_a_question_resolves_both_to_one_key():
    # derived from the config: every question with more than one header variant (the launch form was reworded once)
    for persona in GFORMS_PERSONAS:
        known = _header_map(persona)
        for q in _questions(persona):
            for variant in q["headers"]:
                assert known[normalise_header(variant)] == q["question_key"]
        reworded = [q for q in _questions(persona) if len(q["headers"]) > 1]
        if len(reworded) > 0:
            first, second = _launch_header(persona, wording=0), _launch_header(persona, wording=1)
            assert first != second
            assert _resolves(persona, first) == [] and _resolves(persona, second) == []
            assert [known[normalise_header(c)] for c in first[1:]] == [known[normalise_header(c)] for c in second[1:]]


def test_the_tracked_launch_form_fixtures_resolve_for_their_persona_or_name_the_missing_columns():
    # headers of the tracked fixtures that the tests of the adapter use: a column that is not configured is REPORTED
    # (listed by its raw name), never silently dropped, and the configured ones resolve
    for name, persona in (("feedback_gforms_general.csv", "General Public"), ("feedback_gforms_kannada.csv", "Youth")):
        header = _tracked_header(name)
        unresolved = _resolves(persona, header)
        assert all(col in header for col in unresolved)
        assert len(unresolved) < len(header) - 1                       # most of the columns are configured questions


def test_a_stray_unnamed_column_and_email_address_are_ignored_not_unmapped():
    for persona in GFORMS_PERSONAS:
        header = _launch_header(persona, stray_column="Column 16")
        assert _resolves(persona, header) == []
        assert _resolves(persona, header + ["Email Address"]) == []
        # a genuinely unknown column is still reported
        assert _resolves(persona, header + ["Favourite colour"]) == ["Favourite colour"]


# ---------------------------------------------------------------------------------------------
# session_type and questionnaire_version
# ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_config_carries_the_personas_session_type(persona):
    cfg = gforms_config_v1(persona)
    assert cfg["session_type"] == PERSONAS[persona]["session_type"]
    assert SESSION_TYPE_TO_PERSONA[cfg["session_type"]] == persona


def test_session_types_of_the_three_google_form_personas():
    assert {p: gforms_config_v1(p)["session_type"] for p in GFORMS_PERSONAS} == {
        "Youth": "Youth_Voice", "Parent": "Parent_Voice", "General Public": "General_Public_Voice",
    }


@pytest.mark.parametrize("persona", list(GFORMS_PERSONAS))
def test_every_form_has_a_questionnaire_version(persona):
    for form_key, form in gforms_config_v1(persona)["forms"].items():
        assert form["questionnaire_version"] in ("V1", "V2"), form_key
        assert isinstance(form["questionnaire_version"], str) and form["questionnaire_version"].strip()


def test_launch_form_versions_are_known_mitra_version_keys():
    # a form's version label uses the same vocabulary as the persona's Mitra questionnaire_versions
    for persona in GFORMS_PERSONAS:
        mitra_keys = {v["key"] for v in mitra_config_v1(persona)["questionnaire_versions"]} | {"V2"}
        for form in gforms_config_v1(persona)["forms"].values():
            assert form["questionnaire_version"] in mitra_keys


def test_educator_has_no_google_form_definition():
    assert "Educator" in PERSONAS and "Educator" not in GFORMS_PERSONAS
    assert PERSONAS["Educator"]["gforms_config_id"] is None
    with pytest.raises(KeyError):
        gforms_config_v1("Educator")


def test_unknown_persona_raises_key_error():
    with pytest.raises(KeyError):
        gforms_config_v1("Nobody")


def test_google_form_personas_have_a_config_id_and_the_others_do_not():
    for name, persona in PERSONAS.items():
        assert (persona["gforms_config_id"] is not None) == (name in GFORMS_PERSONAS), name
