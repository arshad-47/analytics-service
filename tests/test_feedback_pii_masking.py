"""
The optional model-based PII / abusive-language masking of a feedback session (app/services/feedback_pii_masking.py,
`pii.masking_prompt_name` of the extraction config): the question and answer JSON, the strict parse, the masked
conversation the extraction prompt receives, and the flag. Fake LLM only (no network). SYNTHETIC data only.
"""
import json
import re

import pytest

from app.services import feedback_extraction as fx
from app.services import feedback_pii_masking as pm
from seed_support import PERSONAS, extraction_config_v1

PII_TYPES = ("name", "phone", "email", "address", "id_number", "other")


def _row(sequence, message, parent=None, translated=None, key="Q"):
    return {"sequence": sequence, "parent_id": parent, "question_key": key, "message": message,
            "translated_message": translated}


ROWS = [
    _row(1, "What is your name?"),
    _row(2, "Asha Verma", parent="p1"),
    _row(3, "Which school do you go to?"),
    _row(4, "DPS School Dumra", parent="p3"),
    _row(5, "Tell me more"),                       # a follow-up probe that repeats the idea
    _row(6, "Tell me more"),                       # the SAME question text again: a JSON key by question would clash
    _row(7, "more teachers please", parent="p6"),
]


def _config(masking="Feedback PII and Abusive-Language Detection", extraction_text="masked"):
    """`extraction_text` is 'masked' here: these tests are about the masked copy reaching the extraction call (the
    default, 'original', is covered by tests/test_feedback_original_text.py)."""
    raw = extraction_config_v1("Youth")
    raw["pii"]["extraction_text"] = extraction_text
    if masking is None:
        raw["pii"].pop("masking_prompt_name", None)
    else:
        raw["pii"]["masking_prompt_name"] = masking
    return fx.parse_extraction_config(raw)


def _conversation(rows=ROWS):
    return fx.build_conversation(rows)


# ------------------------------------------------------------------------------------------------ the input

def test_the_input_is_keyed_by_sequence_so_a_repeated_question_does_not_clash():
    pairs = pm.build_masking_input(_conversation())
    assert list(pairs) == ["2", "4", "7"]
    assert pairs["2"] == {"question": "What is your name?", "answer": "Asha Verma"}
    assert pairs["4"] == {"question": "Which school do you go to?", "answer": "DPS School Dumra"}
    assert pairs["7"] == {"question": "Tell me more", "answer": "more teachers please"}


def test_a_multi_message_answer_repeats_its_question_and_an_unanswered_start_has_an_empty_question():
    rows = [_row(1, "my name is", parent="p0"), _row(2, "What is your age?"), _row(3, "twenty", parent="p2"),
            _row(4, "two", parent="p2")]
    pairs = pm.build_masking_input(_conversation(rows))
    assert pairs["1"]["question"] == ""
    assert pairs["3"]["question"] == pairs["4"]["question"] == "What is your age?"


def test_the_prompt_fills_both_placeholders_once_and_answer_text_cannot_inject_into_them():
    pairs = {"2": {"question": "q", "answer": "set {columns} and {{text}} please"}}
    step = pm.MaskingStep("keys: {columns}", "input: {{text}}", lambda p: ("{}", {}))
    prompt = pm.render_masking_prompt(step, pairs)
    system, user = prompt.split("\n\n", 1)
    assert system == 'keys: ["2"]'
    assert json.loads(user[len("input: "):]) == pairs
    assert prompt.count('["2"]') == 1


# ------------------------------------------------------------------------------------------------ the strict parse

def _answer(**entries):
    return json.dumps(entries)


def _ok(masked="x", **extra):
    entry = {"masked_text": masked, "pii_found": [], "abusive_language": False, "abusive_spans": []}
    entry.update(extra)
    return entry


def test_a_clean_answer_returns_the_masked_texts_and_no_flags():
    masked, types, abusive, severities = pm.parse_masking_response(
        _answer(**{"2": _ok("a"), "4": _ok("b")}), ["2", "4"], PII_TYPES)
    assert masked == {"2": "a", "4": "b"} and types == () and abusive is False and severities == ()


def test_pii_types_are_mapped_to_the_config_names_and_unknown_ones_become_other():
    answer = _answer(**{"2": _ok("<PERSON>", pii_found=[{"type": "PERSON"}]),
                        "4": _ok("<LOCATION>", pii_found=[{"type": "LOCATION"}, {"type": "WEIRD"}])})
    _, types, _, _ = pm.parse_masking_response(answer, ["2", "4"], PII_TYPES)
    assert types == ("name", "address", "other")
    # a type the config does not list falls back to 'other'
    _, types, _, _ = pm.parse_masking_response(answer, ["2", "4"], ("other", "name"))
    assert types == ("name", "other")


def test_a_pii_tag_in_the_masked_text_counts_even_when_pii_found_is_empty():
    _, types, _, _ = pm.parse_masking_response(_answer(**{"2": _ok("call <PHONE> now")}), ["2"], PII_TYPES)
    assert types == ("phone",)


def test_abuse_is_flagged_by_the_flag_or_a_valid_span_and_severities_are_sorted():
    answer = _answer(**{
        "2": _ok(abusive_language=True),
        "4": _ok(abusive_spans=[{"severity": "SEVERE"}, {"severity": "mild"}, {"severity": "bogus"}, "x"])})
    _, _, abusive, severities = pm.parse_masking_response(answer, ["2", "4"], PII_TYPES)
    assert abusive is True and severities == ("mild", "severe")
    _, _, abusive, severities = pm.parse_masking_response(_answer(**{"2": _ok()}), ["2"], PII_TYPES)
    assert abusive is False and severities == ()


def test_fences_a_one_element_list_wrap_and_extra_keys_are_tolerated():
    text = "```json\n" + _answer(**{"2": [_ok("a")], "99": _ok("zz")}) + "\n```"
    masked, *_ = pm.parse_masking_response(text, ["2"], PII_TYPES)
    assert masked == {"2": "a"}


@pytest.mark.parametrize("text", [
    "not json", "[]", "null", '{"2": 1}', '{"2": {"masked_text": null}}', '{"2": {"pii_found": []}}',
    '{"2": {"masked_text": 5}}', '{"2": {"masked_text": "a"}, "2": {"masked_text": "b"}}',
    '{"2": {"masked_text": NaN}}', '{"2": [{"masked_text": "a"}, {"masked_text": "b"}]}', 42, None,
])
def test_anything_but_a_complete_answer_is_refused_and_never_falls_back_to_the_original(text):
    with pytest.raises(fx.ExtractionOutputError):
        pm.parse_masking_response(text, ["2"], PII_TYPES)


def test_a_missing_key_is_refused_and_the_error_never_carries_text():
    with pytest.raises(fx.ExtractionOutputError) as raised:
        pm.parse_masking_response(_answer(**{"2": _ok("SECRET-WORDS")}), ["2", "4"], PII_TYPES)
    assert "SECRET-WORDS" not in str(raised.value) and "4" in str(raised.value)


# ------------------------------------------------------------------------------------------------ the masked conversation

def test_the_masked_answers_replace_the_respondent_turns_and_nothing_else():
    conv = _conversation()
    masked = pm.apply_masked_answers(conv, {"2": "<PERSON>", "4": "<LOCATION>"})
    assert "Asha" not in masked.text and "DPS" not in masked.text
    assert "[2] Respondent: <PERSON>" in masked.text and "[4] Respondent: <LOCATION>" in masked.text
    assert "[7] Respondent: more teachers please" in masked.text           # not in the map: unchanged
    assert [t.text for t in masked.turns if t.role == fx.ROLE_BOT] == [t.text for t in conv.turns if t.role == fx.ROLE_BOT]
    assert masked.conversation_id != conv.conversation_id and masked.respondent_answers == conv.respondent_answers


# ------------------------------------------------------------------------------------------------ extract() with the step

def _extraction_answer(pii=False):
    config = _config()
    fields = {spec.name: {"value": None, "raw": None, "confidence": None} for spec in config.fields}
    return json.dumps({"fields": fields, "pii": {"detected": pii, "types": []}})


class Fakes:
    """A fake masking call (answers from a script) and a fake extraction call; both record their prompts."""

    def __init__(self, masking_answer, extraction_answer=None):
        self.masking_answer = masking_answer
        self.extraction_answer = extraction_answer if extraction_answer is not None else _extraction_answer()
        self.masking_prompts, self.extraction_prompts = [], []

    def masking(self, prompt):
        self.masking_prompts.append(prompt)
        answer = self.masking_answer
        return (answer(prompt) if callable(answer) else answer), {"prompt_tokens": 5, "completion_tokens": 7}

    def extraction(self, prompt):
        self.extraction_prompts.append(prompt)
        return self.extraction_answer, {"prompt_tokens": 11, "completion_tokens": 13}


STEP_TEMPLATES = ("keys {columns}", "input {{text}}")
SYSTEM, USER = "system", "{{conversation_id}} {{conversation}}"


def _run_extract(fakes, rows=ROWS, config=None):
    config = config if config is not None else _config()
    step = pm.MaskingStep(*STEP_TEMPLATES, fakes.masking)
    return fx.extract(rows, config, SYSTEM, USER, fakes.extraction, (), step)


def _masked_all(prompt):
    pairs = json.loads(prompt.split("input ", 1)[1])
    return json.dumps({k: _ok("<PERSON>" if v["answer"] == "Asha Verma" else v["answer"]) for k, v in pairs.items()})


def test_the_extraction_prompt_receives_the_masked_answers_and_never_the_original_words():
    fakes = Fakes(_masked_all)
    run = _run_extract(fakes)
    assert len(fakes.masking_prompts) == 1 and len(fakes.extraction_prompts) == 1
    assert "Asha Verma" in fakes.masking_prompts[0]                       # the masking model is the one that sees it
    assert "Asha" not in fakes.extraction_prompts[0] and "<PERSON>" in fakes.extraction_prompts[0]
    assert run.masking.conversation["2"] == {"question": "What is your name?", "answer": "<PERSON>"}
    assert run.usage == {"prompt_tokens": 11, "completion_tokens": 13}
    assert run.masking.usage == {"prompt_tokens": 5, "completion_tokens": 7}


def test_the_flag_is_true_when_only_the_masking_call_found_personal_data():
    fakes = Fakes(lambda p: json.dumps({k: _ok("<PERSON>", pii_found=[{"type": "PERSON"}])
                                        for k in json.loads(p.split("input ", 1)[1])}))
    run = _run_extract(fakes)                                              # the extraction call says pii false
    assert run.result.pii_detected is True and "name" in run.result.pii_types


def test_the_flag_stays_false_when_nothing_was_found_anywhere():
    run = _run_extract(Fakes(_masked_all, _extraction_answer(pii=False)), rows=[
        _row(1, "How old are you?"), _row(2, "more teachers please", parent="p1")])
    assert run.result.pii_detected is False and run.masking.pii_types == ()


def test_the_local_check_still_runs_first_so_a_number_never_reaches_the_masking_model_either():
    rows = [_row(1, "Your number?"), _row(2, "9876543210", parent="p1")]
    fakes = Fakes(_masked_all)
    run = _run_extract(fakes, rows=rows)
    assert "9876543210" not in fakes.masking_prompts[0] and "[number removed]" in fakes.masking_prompts[0]
    assert run.result.pii_detected is True


def test_a_bad_masking_answer_fails_the_session_before_any_extraction_call_with_its_usage_attached():
    fakes = Fakes(_answer(**{"2": _ok("a")}))                              # keys 4 and 7 missing
    with pytest.raises(fx.ExtractionOutputError) as raised:
        _run_extract(fakes)
    assert fakes.extraction_prompts == []
    assert raised.value.purpose == "masking" and raised.value.usage == {"prompt_tokens": 5, "completion_tokens": 7}


def test_a_bad_extraction_answer_after_masking_keeps_the_masking_usage_out_of_its_own_error():
    fakes = Fakes(_masked_all, "not json")
    with pytest.raises(fx.ExtractionOutputError) as raised:
        _run_extract(fakes)
    assert getattr(raised.value, "purpose", "extraction") == "extraction"
    assert raised.value.usage == {"prompt_tokens": 11, "completion_tokens": 13}
    assert raised.value.prior_usage == {"prompt_tokens": 5, "completion_tokens": 7}   # the billed masking call


def test_a_failed_extraction_call_after_masking_also_carries_the_masking_usage():
    fakes = Fakes(_masked_all)

    def broken(prompt):
        raise RuntimeError("provider down")

    step = pm.MaskingStep(*STEP_TEMPLATES, fakes.masking)
    with pytest.raises(RuntimeError) as raised:
        fx.extract(ROWS, _config(), SYSTEM, USER, broken, (), step)
    assert raised.value.prior_usage == {"prompt_tokens": 5, "completion_tokens": 7}


def test_no_masking_step_means_no_masking_call_and_the_old_behaviour():
    fakes = Fakes(_masked_all)
    run = fx.extract(ROWS, _config(None), SYSTEM, USER, fakes.extraction, ())
    assert fakes.masking_prompts == [] and run.masking is None and "Asha Verma" in fakes.extraction_prompts[0]


def test_a_session_without_an_answer_makes_neither_call():
    fakes = Fakes(_masked_all)
    run = _run_extract(fakes, rows=[_row(1, "Hello?")])
    assert fakes.masking_prompts == [] and fakes.extraction_prompts == [] and run.llm_called is False


# ------------------------------------------------------------------------------------------------ config and seed

@pytest.mark.parametrize("value", [" x", "x ", "", 5, True, ["a"]])
def test_a_masking_prompt_name_must_be_null_or_an_exact_name(value):
    raw = extraction_config_v1("Youth")
    raw["pii"]["masking_prompt_name"] = value
    with pytest.raises(fx.ExtractionConfigError, match="masking_prompt_name"):
        fx.parse_extraction_config(raw)


def test_null_or_absent_masking_prompt_name_switches_the_step_off():
    raw = extraction_config_v1("Youth")
    raw["pii"]["masking_prompt_name"] = None
    assert fx.parse_extraction_config(raw).pii_policy.masking_prompt_name is None
    raw["pii"].pop("masking_prompt_name")
    assert fx.parse_extraction_config(raw).pii_policy.masking_prompt_name is None


@pytest.mark.parametrize("persona", sorted(PERSONAS))
def test_every_seeded_persona_names_the_seeded_masking_prompt(persona):
    assert extraction_config_v1(persona)["pii"]["masking_prompt_name"] == "Feedback PII and Abusive-Language Detection"


def test_the_seeded_prompt_has_the_story_contract_placeholders_and_the_feedback_rules():
    from app.database.db import SEED_PROMPTS_FILE
    sql = SEED_PROMPTS_FILE.read_text(encoding="utf-8")
    block = sql[sql.index("Feedback PII and Abusive-Language Detection (optional"):]
    assert "'feedback_pii_and_abusive_language_detection'" in block
    assert block.count("{columns}") >= 2 and "{{text}}" in block
    assert "ON CONFLICT (prompt_id, version) DO NOTHING" in block and "DO UPDATE" not in block
    lower = block.lower()
    assert "never wrap" in lower or "no wrapping" in lower
    assert "not instructions" in lower and "district" in lower and "<person>" in lower
    assert re.search(r"abusive_language", block) and "masked_text" in block
