"""
Tests of the masking that runs BEFORE the LLM call (app/services/feedback_extraction.py, `mask_respondent_pii` and
`extract`): what the deterministic PII check finds in respondent text (numbers, e-mail addresses, handles, links) is
replaced by a placeholder in the prompt, so it never reaches the AI provider, while pii_detected is still forced TRUE.
The fake LLM records the prompt it receives. SYNTHETIC data only; no database, no network.
"""
import json
import time

import pytest

from app.services import feedback_extraction as fx
from app.services import feedback_extraction_constants as constants

SYSTEM = "{{persona}} | {{field_names}}\n{{fields}}\n{{pii_types}}\n{{output_format}}"
USER = "BEGIN {{conversation_id}}\n{{conversation}}\nEND"

PHONE_TEXT = "call me on 98765 43210, mail asha@gmail.com, insta @asha_k, site www.x.in/asha"
LEAKS = ("98765", "43210", "asha@gmail.com", "@asha_k", "www.x.in/asha", "asha_k")


def _config(**pii):
    """These tests are about what the MODEL receives once the deterministic check has run, so they read the masked
    text: `pii.extraction_text` is 'masked' here (the default, 'original', is covered by tests/test_feedback_original_text.py)."""
    raw = {"schema_version": 1, "persona": "Parent", "session_type": "Parent_Voice", "prompt_name": "Some Prompt",
           "fields": [{"name": "district", "kind": "free_text", "description": "The district."}],
           "pii": {"extraction_text": "masked", **pii}}
    return fx.parse_extraction_config(raw)


def _row(seq, text, answer=True, english=None, **extra):
    row = {"sequence": seq, "parent_id": 1 if answer else None, "question_key": None, "message": text,
           "translated_message": english}
    row.update(extra)
    return row


def _answer(pii=(False, [])):
    return json.dumps({"fields": {"district": {"value": "Mysuru", "raw": "Mysuru"}},
                       "pii": {"detected": pii[0], "types": pii[1]}})


def _run(rows, config=None, answer=None):
    """extract() with a fake LLM that records the prompts; returns (run, prompts)."""
    prompts = []

    def llm(prompt):
        prompts.append(prompt)
        return (answer if answer is not None else _answer()), {"prompt_tokens": 1, "completion_tokens": 1}

    return fx.extract(rows, config if config is not None else _config(), SYSTEM, USER, llm), prompts


# ------------------------------------------------------------------------------------------------ what the model receives

def test_the_prompt_holds_none_of_the_numbers_emails_handles_or_links_but_the_placeholders():
    rows = [_row(1, "Your contact details?", answer=False), _row(2, PHONE_TEXT)]
    run, prompts = _run(rows)
    assert len(prompts) == 1
    for leak in LEAKS:
        assert leak not in prompts[0], leak
    for placeholder in (constants.MASK_NUMBER, constants.MASK_EMAIL, constants.MASK_HANDLE, constants.MASK_LINK):
        assert placeholder in prompts[0], placeholder
    assert "call me on" in prompts[0] and "Your contact details?" in prompts[0]      # the rest of the text is kept
    assert run.llm_called is True


def test_the_flag_is_forced_true_with_the_types_even_when_the_model_says_false():
    run, _ = _run([_row(1, "Q", answer=False), _row(2, PHONE_TEXT)], answer=_answer(pii=(False, [])))
    assert run.result.pii_detected is True
    assert run.result.pii_types == ("phone", "email", "other")


def test_the_model_flag_still_counts_when_the_check_found_nothing_and_the_fields_are_extracted():
    rows = [_row(1, "Q", answer=False), _row(2, "I live in Mysuru and my teacher is Mrs Rao")]
    run, prompts = _run(rows, answer=_answer(pii=(True, ["name"])))
    assert run.result.pii_detected is True and run.result.pii_types == ("name",)
    assert "Mrs Rao" in prompts[0]


def test_the_model_flag_and_the_check_are_combined():
    run, _ = _run([_row(1, "Q", answer=False), _row(2, "my number is 9876543210")], answer=_answer(pii=(True, ["name"])))
    assert run.result.pii_types == ("name", "phone")


def test_types_outside_the_configured_list_are_recorded_as_other():
    run, _ = _run([_row(1, "Q", answer=False), _row(2, "9876543210 asha@gmail.com")],
                  config=_config(types=["name", "other"]))
    assert run.result.pii_types == ("other",)


def test_a_flagged_session_keeps_what_the_model_extracted():
    run, _ = _run([_row(1, "Q", answer=False), _row(2, PHONE_TEXT)])
    district = {f.field_name: f for f in run.result.fields}["district"]
    assert run.result.pii_detected is True
    assert district.value == "Mysuru" and district.raw_value is not None       # nothing is wiped any more


def test_the_translated_column_is_masked_too():
    rows = [_row(1, "Q", answer=False), _row(2, "mera number hai", english="my number is 98765 43210 or asha@gmail.com")]
    run, prompts = _run(rows)
    for leak in ("98765", "43210", "asha@gmail.com"):
        assert leak not in prompts[0]
    assert constants.MASK_NUMBER in prompts[0] and constants.MASK_EMAIL in prompts[0]
    assert run.result.pii_detected is True


def test_both_columns_with_a_hit_each_are_masked():
    masked, kinds = fx.mask_respondent_pii([_row(1, "phone 9876543210", english="mail asha@gmail.com")])
    assert masked[0]["message"] == f"phone {constants.MASK_NUMBER}"
    assert masked[0]["translated_message"] == f"mail {constants.MASK_EMAIL}"
    assert kinds == ("phone", "email")


@pytest.mark.parametrize("digits", [
    "९८७६५४३२१०",          # Devanagari
    "９８７６５４３２１０",      # fullwidth
    "⑨⑧⑦⑥⑤④③②①⓪",          # circled
    "⁹⁸⁷⁶⁵⁴³²¹⁰",          # superscript
    "98765​43210",         # zero-width space inside
    "98765­43210",         # soft hyphen inside
])
def test_digits_of_other_scripts_and_hidden_characters_are_masked_before_the_call(digits):
    run, prompts = _run([_row(1, "Q", answer=False), _row(2, f"call {digits} now")])
    assert constants.MASK_NUMBER in prompts[0]
    assert "call" in prompts[0] and "now" in prompts[0]
    assert not any(ch.isdecimal() or ch in "⑨⑧⑦⑥⑤④③②①⓪⁹⁸⁷⁶⁵⁴³²¹⁰" for ch in prompts[0].split("Respondent:")[1])
    assert run.result.pii_detected is True


def test_a_fullwidth_at_sign_email_is_masked():
    masked, kinds = fx.mask_respondent_pii([_row(1, "write to asha＠example.org please")])
    assert masked[0]["message"] == f"write to {constants.MASK_EMAIL} please" and kinds == ("email",)


# ------------------------------------------------------------------------------------------------ chained numbers

def test_a_number_answered_in_parts_is_masked_in_every_message_of_the_chain():
    rows = [_row(1, "What is your number?", answer=False), _row(2, "98765"), _row(3, "43210")]
    run, prompts = _run(rows)
    assert "98765" not in prompts[0] and "43210" not in prompts[0]
    assert prompts[0].count(constants.MASK_NUMBER) == 2
    assert run.result.pii_detected is True and run.result.pii_types == ("phone",)


def test_a_chain_of_three_and_with_a_bot_message_in_between_is_masked_in_all_parts():
    rows = [_row(1, "Number?", answer=False), _row(2, "987"), _row(3, "Go on", answer=False), _row(4, "654"),
            _row(5, "More", answer=False), _row(6, "3210")]
    masked, kinds = fx.mask_respondent_pii(rows)
    assert [r["message"] for r in masked if r["parent_id"] is not None] == [constants.MASK_NUMBER] * 3
    assert [r["message"] for r in masked if r["parent_id"] is None] == ["Number?", "Go on", "More"]
    assert kinds == ("phone",)


def test_parts_that_do_not_form_a_number_are_left_alone():
    rows = [_row(1, "98765 is my pin"), _row(2, "43210"), _row(3, "I was born in"), _row(4, "2018"), _row(5, "2022")]
    masked, kinds = fx.mask_respondent_pii(rows)
    assert kinds == ()
    assert [r["message"] for r in masked] == ["98765 is my pin", "43210", "I was born in", "2018", "2022"]


# ------------------------------------------------------------------------------------------------ what stays untouched

def test_bot_rows_are_never_masked_even_when_they_look_like_pii():
    bot = "Call us on 98765 43210 or mail help@school.org, see www.school.in"
    rows = [_row(1, bot, answer=False), _row(2, "ok i will")]
    masked, kinds = fx.mask_respondent_pii(rows)
    assert kinds == () and masked[0]["message"] == bot
    run, prompts = _run(rows)
    assert bot in prompts[0] and run.result.pii_detected is False


def test_a_bot_row_with_a_hit_next_to_a_respondent_hit_stays_as_it_was():
    bot = "Reach help@school.org"
    masked, _ = fx.mask_respondent_pii([_row(1, bot, answer=False), _row(2, "my number 9876543210")])
    assert masked[0]["message"] == bot and masked[1]["message"] == f"my number {constants.MASK_NUMBER}"


def test_text_without_a_hit_is_byte_for_byte_the_original():
    # not NFKC-normalised, not whitespace-collapsed, same objects: only rows with a hit change
    original = "Ｉ am ｆrom   Mysuru​,  age ①②"
    rows = [_row(1, "Q", answer=False), _row(2, original, english="plain  text  here")]
    masked, kinds = fx.mask_respondent_pii(rows)
    assert kinds == ()
    assert masked[1]["message"] is original and masked[1]["translated_message"] == "plain  text  here"
    assert masked[1] is rows[1]
    run, prompts = _run(rows)
    assert run.result.pii_detected is False


def test_a_column_without_a_hit_keeps_its_original_text_next_to_a_masked_one():
    original = "Ｉ am ｆrom   Mysuru"
    masked, _ = fx.mask_respondent_pii([_row(1, original, english="call 9876543210")])
    assert masked[0]["message"] is original
    assert masked[0]["translated_message"] == f"call {constants.MASK_NUMBER}"


def test_the_input_rows_are_never_modified():
    rows = [_row(1, "Q", answer=False), _row(2, PHONE_TEXT, english=PHONE_TEXT)]
    snapshot = [dict(r) for r in rows]
    fx.mask_respondent_pii(rows)
    _run(rows)
    assert rows == snapshot


def test_a_masked_text_has_no_hit_left():
    masked, kinds = fx.mask_respondent_pii([_row(1, PHONE_TEXT, english=PHONE_TEXT)])
    assert len(kinds) == 3
    assert fx.mask_respondent_pii(masked)[1] == ()
    assert fx.pii_precheck_kinds(masked) == ()


def test_ordinary_numbers_amounts_and_years_are_not_masked():
    text = "I am 15, class 9, born 2008, Fees 2,50,000-3,00,000 and Students 120, 135, 150, 160, studied 2018-2022"
    masked, kinds = fx.mask_respondent_pii([_row(1, text)])
    assert kinds == () and masked[0]["message"] == text


# ------------------------------------------------------------------------------------------------ limits are about the original

def test_the_size_checks_look_at_the_original_text_and_make_no_call():
    config = _config()
    huge = "9" * (config.limits.max_respondent_message_chars + 1)
    run, prompts = _run([_row(1, "Q", answer=False), _row(2, huge)], config)
    assert prompts == [] and run.llm_called is False and run.skip_reason == fx.SKIP_OVERSIZED
    run, prompts = _run([_row(1, "Q", answer=False), _row(2, "")], config)
    assert prompts == [] and run.skip_reason == fx.SKIP_NO_ANSWER


def test_the_logs_name_only_the_kinds(caplog):
    caplog.set_level("INFO", logger="analytics_service.feedback_extraction")
    _run([_row(1, "Q", answer=False), _row(2, PHONE_TEXT)])
    for leak in LEAKS:
        assert leak not in caplog.text


# ------------------------------------------------------------------------------------------------ mask_before_call = false

def test_mask_before_call_is_true_by_default_and_a_boolean():
    assert _config().pii_policy.mask_before_call is True
    assert _config(mask_before_call=True).pii_policy.mask_before_call is True
    assert _config(mask_before_call=False).pii_policy.mask_before_call is False
    for bad in ("false", 0, 1, None, []):
        with pytest.raises(fx.ExtractionConfigError, match="mask_before_call"):
            _config(mask_before_call=bad)


def test_with_mask_before_call_false_the_text_goes_unmasked_and_the_check_runs_after_the_call():
    rows = [_row(1, "Q", answer=False), _row(2, PHONE_TEXT)]
    run, prompts = _run(rows, _config(mask_before_call=False))
    assert "98765 43210" in prompts[0] and "asha@gmail.com" in prompts[0] and "@asha_k" in prompts[0]
    assert constants.MASK_NUMBER not in prompts[0]
    assert run.result.pii_detected is True and run.result.pii_types == ("phone", "email", "other")     # still flagged


def test_with_mask_before_call_false_a_clean_session_is_unchanged():
    run, prompts = _run([_row(1, "Q", answer=False), _row(2, "I live in Mysuru")], _config(mask_before_call=False))
    assert "I live in Mysuru" in prompts[0] and run.result.pii_detected is False


# ------------------------------------------------------------------------------------------------ speed

@pytest.mark.parametrize("text", [
    "-" * 20000, "1 " * 10000, "a@" * 10000, "9876543210 " * 1800, "www." * 5000, "http://" * 2800,
    "x.com/" * 3300, "a@b.com " * 2500, ("a" * 60 + ".") * 300 + "com", "1-a@2." * 3000, "9" * 20000,
])
def test_a_20000_character_message_is_masked_fast(text):
    rows = [_row(1, "Q", answer=False), _row(2, text, english=text)]
    started = time.perf_counter()
    masked, _ = fx.mask_respondent_pii(rows)
    assert time.perf_counter() - started < 5.0
    assert fx.pii_precheck_kinds(masked) == ()      # whatever was found is gone


def test_masking_scales_linearly_with_the_length():
    def best(text):
        times = []
        for _ in range(3):
            started = time.perf_counter()
            fx.mask_respondent_pii([_row(1, text, english=text)])
            times.append(time.perf_counter() - started)
        return min(times)

    unit = "call 98765 43210 mail a@b.in www.x.in/a "
    small, large = best(unit * 40), best(unit * 400)
    assert large < 40 * max(small, 0.0005)
