"""
Pure tests of the deterministic PII backstop (app/services/feedback_extraction.py, `pii_precheck_kinds`): its policy
(digits across separators and Unicode forms, years and lists of small numbers, split numbers, e-mail / handle / URL
shapes), its linear running time on pathological input, and its configuration (pii.min_digit_run, pii.url_tlds, ...).
SYNTHETIC data only; no database, no network.
"""
import time
import unicodedata

import pytest

from app.services import feedback_extraction as fx

MAX_SECONDS_HANG = 5.0   # a hang guard only (one pre-check over a 20,000-character message; the old code needed 9-11 s)


def _row(seq, text, answer=True, english=None):
    return {"sequence": seq, "parent_id": 1 if answer else None, "question_key": None, "message": text,
            "translated_message": english}


def _kinds(text, policy=fx.DEFAULT_PII_POLICY, english=None):
    return fx.pii_precheck_kinds([_row(1, text, english=english)], policy)


# ------------------------------------------------------------------------------------------------ B1: linear time

# name -> (repeated unit, repetitions, tail): a 20,000-character message at full size, a tenth of it at 1/10 of the repetitions
PATHOLOGICAL = {
    "hyphens": ("-", 20000, ""),
    "word-hyphen": ("a-", 10000, ""),
    "letters": ("a", 20000, ""),
    "digits": ("1", 20000, ""),
    "digit-space": ("1 ", 10000, ""),
    "at-signs": ("@", 20000, ""),
    "word-at": ("a@", 10000, ""),
    "dots": (".", 20000, ""),
    "label-dots": ("a.", 10000, ""),
    "hyphen-dots": ("-.", 10000, ""),
    "www": ("www.", 5000, ""),
    "http": ("http://", 2800, ""),
    "tld-tail": (("a" * 60) + ".", 300, "com"),
    "email-like": ("a", 19990, "@b"),
    "mixed": ("1-a@2.", 3000, ""),
}


def _shape(name, tenths=10):
    unit, count, tail = PATHOLOGICAL[name]
    return unit * (count * tenths // 10) + tail


def _best_of(rows, repeats=3):
    best = None
    for _ in range(repeats):
        started = time.perf_counter()
        fx.pii_precheck_kinds(rows)
        elapsed = time.perf_counter() - started
        best = elapsed if best is None or elapsed < best else best
    return best


@pytest.mark.parametrize("name", sorted(PATHOLOGICAL))
def test_the_check_scales_linearly_with_the_length_of_the_message(name):
    """
    Ten times the text must cost about ten times the time (a quadratic regex costs a hundred times). The bound is
    RELATIVE (a machine that is slow or busy slows both measurements), with room for noise: 40x for a 10x input.
    """
    small, large = _shape(name, 1), _shape(name)        # 10x apart (the tail that makes a shape dangerous is kept)
    t_small = _best_of([_row(1, small, english=small)])
    t_large = _best_of([_row(1, large, english=large)])      # both columns are scanned
    assert t_large < 40 * max(t_small, 0.0005), (name, t_small, t_large)
    assert t_large < MAX_SECONDS_HANG, name                # never a hang (the old code needed 9-11 s on one message)


def test_the_session_with_many_pathological_rows_does_not_blow_up_with_the_number_of_rows():
    one = [_row(1, "-" * 20000, english="-" * 20000)]
    many = [_row(i, "-" * 20000, english="-" * 20000) for i in range(1, 36)]     # the reviewer's 35 rows
    t_one, t_many = _best_of(one, 2), _best_of(many, 2)
    assert t_many < 100 * max(t_one, 0.001)         # 35x the rows: linear, with room for noise; the old code needed minutes
    assert t_many < MAX_SECONDS_HANG * 10


# ------------------------------------------------------------------------------------------------ B2: bypasses closed

@pytest.mark.parametrize("text", [
    "call 98765\u200b43210",           # zero-width space between the digits
    "call 98765\u00ad43210",           # soft hyphen
    "call 98765\u200d43210",           # zero-width joiner
    "call 98765\ufeff43210",           # BOM
    "call 98765–43210",            # en dash
    "call 98765—43210",            # em dash
    "call 98765·43210",            # middle dot
    "call 98765,43210",
    "call 98765/43210",
    "call 98765_43210",
    "call ph:98765;43210",
    "call 98765 – 43210",
    "call \u2079\u2078\u2077\u2076\u2075\u2074\u00b3\u00b2\u00b9\u2070",   # superscripts, all ASSIGNED (see below)
    "call ⑨⑧⑦⑥⑤④③②①⓪",        # circled digits
    "call ９８７６５４３２１０",        # fullwidth digits
    "call ९८७६५४३२१०",        # Devanagari digits
    "+91 98765 43210",
    "(022) 2345 6789",
    "98 76 54 32 10",                    # ten digits in pairs
    "9 8 7 6 5 4 3 2 1 0",
    "98765.43210",
    "12345678",
])
def test_phone_shaped_digit_runs_are_flagged_whatever_the_separator_or_script(text):
    assert fx.PII_KIND_NUMBER in _kinds(text), text


def test_the_superscript_case_really_exercises_superscript_digits():
    """Every character of that case is an assigned digit-like code point that NFKC turns into 0-9 (U+2072 and U+2073
    are unassigned, so a string holding them would only test that two unknown characters split the run)."""
    superscripts = "\u2079\u2078\u2077\u2076\u2075\u2074\u00b3\u00b2\u00b9\u2070"
    assert [unicodedata.category(ch) for ch in superscripts] == ["No"] * 10
    assert unicodedata.normalize("NFKC", superscripts) == "9876543210"
    assert all(unicodedata.name(ch).startswith("SUPERSCRIPT") for ch in superscripts)
    assert fx.PII_KIND_NUMBER in _kinds("call " + superscripts)
    # the invisible characters of the cases above are format characters, removed before matching
    for invisible in ("\u200b", "\u00ad", "\u200d", "\ufeff"):
        assert unicodedata.category(invisible) == "Cf"


def test_a_number_answered_in_parts_is_flagged_when_the_parts_are_nothing_but_numbers():
    rows = [_row(1, "What is your number?", answer=False), _row(2, "98765"), _row(3, "That seems short", answer=False),
            _row(4, "43210")]
    assert fx.pii_precheck_kinds(rows) == (fx.PII_KIND_NUMBER,)
    assert fx.pii_precheck_kinds([_row(1, "98765"), _row(2, "43210")]) == (fx.PII_KIND_NUMBER,)


def test_parts_with_words_or_small_numbers_are_not_joined():
    assert fx.pii_precheck_kinds([_row(1, "98765 is my pin"), _row(2, "43210")]) == ()
    assert fx.pii_precheck_kinds([_row(1, "15"), _row(2, "9"), _row(3, "2008"), _row(4, "12")]) == ()   # age, class, year, age
    assert fx.pii_precheck_kinds([_row(1, "2018"), _row(2, "2022")]) == ()                                # two years


def test_a_fullwidth_at_sign_is_an_email():
    assert fx.PII_KIND_EMAIL in _kinds("write to asha＠example.org")


# ------------------------------------------------------------------------------------------------ C4: false positives fixed

@pytest.mark.parametrize("text", [
    "I studied 2018-2022 in a govt school",
    "2019 - 2023",
    "from 2018 to 2022", "2018/2019/2020", "2018–2022", "2018, 2019, 2020",
    "marks 85 90 78 92",
    "my kids are 10 12 14 16",
    "1 2 3 4 5 6 7 8",
    "learn.in the village", "school.in future", "it was fun.in my class", "go home.me and my friends",
    "rate@5%", "5pm@6", "x@", "@5",
    "I am 17, class 9, born 2008", "25,000 per month", "Rs 1,20,000 a year", "10.5 lakh", "1234567",
    "I am 15 and my sister is 12 in class 7", "i scored 95.5 percent", "five 4 three",
])
def test_ordinary_numbers_and_sentences_are_not_flagged(text):
    assert _kinds(text) == (), text


@pytest.mark.parametrize("text, kind", [
    ("find me on insta @asha_rao", fx.PII_KIND_EMAIL),
    ("asha@gmail", fx.PII_KIND_EMAIL),
    ("asha.rao@example.org", fx.PII_KIND_EMAIL),
    ("see www.example.org/page", fx.PII_KIND_URL),
    ("https://example.org", fx.PII_KIND_URL),
    ("mail me at gmail.com", fx.PII_KIND_URL),
    ("my site is asha-school.com", fx.PII_KIND_URL),
    ("mysite.in", fx.PII_KIND_URL),
    ("visit mysite.in/page", fx.PII_KIND_URL),
    ("visit mysite.in.", fx.PII_KIND_URL),
    ("my page mysite.co 2 times", fx.PII_KIND_URL),
])
def test_emails_handles_and_urls_are_still_flagged(text, kind):
    assert kind in _kinds(text), text


def test_a_run_of_years_is_not_a_number_but_a_year_next_to_other_digits_is_counted():
    assert _kinds("2018-2022") == ()
    assert fx.PII_KIND_NUMBER in _kinds("2018-2022-98765")       # not all groups are years
    assert fx.PII_KIND_NUMBER in _kinds("20182022")              # one 8-digit group is an id, not two years


def test_a_full_date_written_in_digits_counts_as_a_number():
    # documented: 8 digits across '/' are indistinguishable from an id; a date of birth is identifying anyway
    assert fx.PII_KIND_NUMBER in _kinds("12/03/2010")


def test_letters_between_digit_groups_end_the_run_number_words_are_left_to_the_model():
    assert _kinds("9876 five 43210") == ()
    assert _kinds("98765x43210") == ()


def test_a_gap_of_more_than_three_separator_characters_ends_the_run():
    assert fx.PII_KIND_NUMBER in _kinds("98765 - 43210")        # three characters (space, dash, space)
    assert fx.PII_KIND_NUMBER in _kinds("98765-.-43210")
    assert _kinds("98765-.-.43210") == ()                       # four


# ------------------------------------------------------------------------------------------------ commas: amounts and lists

@pytest.mark.parametrize("text", [
    "Fees 2,50,000-3,00,000",
    "School got 1,50,00,000 grant",
    "Students 120, 135, 150, 160",
    "Rs 2,50,000 to 3,00,000 per year", "Rs 12,34,56,789 crore", "1,23,45,678", "12,345,678", "1,234,567,890",
    "marks 120,135,150,160", "classes 6, 7, 8, 9, 10, 11, 12", "ages 10, 12, 14, 16, 18, 20",
    "२,५०,०००-३,००,०००",                    # Devanagari digits with Indian grouping
    "१,५०,००,०००",
    "Fees 2,​50,​000-3,​00,​000",          # zero-width space after the commas
    "Fees ２，５０，０００－３，００，０００",      # fullwidth digits and commas
    "Students １２０，１３５，１５０，１６０",
    "Students 120 ,135 ,150 ,160",           # a comma next to a space
    "2500, 120, 3000, 150",                  # a short group on either side of the comma
])
def test_a_comma_next_to_a_group_of_one_to_three_digits_ends_the_run(text):
    assert _kinds(text) == (), text


@pytest.mark.parametrize("text", [
    "98765,43210",
    "call 98765,43210",
    "98765, 43210",
    "9876,54321",
    "98765 , 43210",
    "９８７６５，４３２１０",                   # fullwidth digits and comma
    "९८७६५,४३२१०",                        # Devanagari digits
    "98765,​43210",                        # zero-width space after the comma
    "9876543210,9876543211",
])
def test_a_comma_between_two_groups_of_four_or_more_digits_still_joins_them(text):
    assert fx.PII_KIND_NUMBER in _kinds(text), text


@pytest.mark.parametrize("text", [
    "98765 43210", "+91-98765-43210", "98 76 54 32 10", "987 654 3210", "080 2345 6789", "9876543210",
    "98765.43210", "98765/43210", "98765–43210", "98765—43210", "98765·43210", "(022) 2345 6789", "12/03/2010",
    "९८७६५ ४३२१०", "９８７６５ ４３２１０", "98765​43210", "9 8 7 6 5 4 3 2 1 0",
])
def test_every_other_separator_still_joins_digit_groups(text):
    assert fx.PII_KIND_NUMBER in _kinds(text), text


def test_a_run_ended_by_a_comma_does_not_hide_a_real_number_next_to_it():
    assert fx.PII_KIND_NUMBER in _kinds("Fees 2,50,000 and my phone 98765 43210")
    assert fx.PII_KIND_NUMBER in _kinds("120, 135, 9876543210")
    assert _kinds("2018-2022, 85 90 78 92") == ()


def test_the_digit_runs_of_an_amount_and_of_a_number_with_a_comma():
    assert fx._digit_runs("2,50,000-3,00,000") == [["2"], ["50"], ["000", "3"], ["00"], ["000"]]
    assert fx._digit_runs("120, 135, 150") == [["120"], ["135"], ["150"]]
    assert fx._digit_runs("98765,43210") == [["98765", "43210"]]
    assert fx._digit_runs("98765, 43210 x 12") == [["98765", "43210"], ["12"]]
    assert fx._digit_runs("1234,12") == [["1234"], ["12"]]


def test_a_date_of_birth_is_still_flagged_as_a_number():
    assert fx.PII_KIND_NUMBER in _kinds("12/03/2010")
    assert fx.PII_KIND_NUMBER in _kinds("12-03-2010")


# ------------------------------------------------------------------------------------------------ A12: config policy

def _config_with(**extra):
    raw = {"schema_version": 1, "persona": "P", "session_type": "S", "prompt_name": "X",
           "fields": [{"name": "district", "kind": "free_text", "description": "d"}]}
    raw.update(extra)
    return fx.parse_extraction_config(raw)


def test_the_default_policy_equals_the_documented_values_when_the_config_has_no_block():
    config = _config_with()
    assert config.pii_policy == fx.PiiPolicy(8, 10, ("com", "in", "org", "net", "co", "io", "me", "edu", "gov", "app",
                                                      "link", "ly", "tv", "info"))
    assert config.limits == fx.Limits(20000, 4000, 200000, 120, 500)
    assert "n/a" in config.placeholders and "dont know" in config.placeholders


def test_min_digit_run_is_config():
    assert _kinds("Student id 1234567") == ()
    strict = _config_with(pii={"min_digit_run": 7}).pii_policy
    assert fx.PII_KIND_NUMBER in _kinds("Student id 1234567", strict)
    lax = _config_with(pii={"min_digit_run": 12}).pii_policy
    assert _kinds("call 9876543210", lax) == ()
    assert lax.min_digit_run_short_groups == 12        # the short-group threshold never drops below the plain one


def test_min_digit_run_short_groups_is_config():
    policy = _config_with(pii={"min_digit_run": 8, "min_digit_run_short_groups": 8}).pii_policy
    assert fx.PII_KIND_NUMBER in _kinds("marks 85 90 78 92", policy)      # the text has letters: runs are '85 90 78 92'


def test_url_tlds_are_config_and_an_empty_list_leaves_only_scheme_and_www_urls():
    assert _kinds("see asha.school") == ()
    custom = _config_with(pii={"url_tlds": ["school"]}).pii_policy
    assert fx.PII_KIND_URL in _kinds("see asha.school", custom)
    assert _kinds("see gmail.com", custom) == ()
    none = _config_with(pii={"url_tlds": []}).pii_policy
    assert _kinds("see gmail.com", none) == ()
    assert fx.PII_KIND_URL in _kinds("see www.example.org", none)


@pytest.mark.parametrize("pii", [
    {"min_digit_run": 3}, {"min_digit_run": 31}, {"min_digit_run": True}, {"min_digit_run": "8"},
    {"min_digit_run": 12, "min_digit_run_short_groups": 10},
    {"url_tlds": "com"}, {"url_tlds": ["COM"]}, {"url_tlds": ["c.m"]}, {"url_tlds": [1]}, {"url_tlds": ["x"]},
    {"url_tlds": ["com"] * 201}, {"unknown_key": 1}, {"types": []}, {"types": ["name"]}, "types", [],
])
def test_a_bad_pii_block_is_refused(pii):
    with pytest.raises(fx.ExtractionConfigError):
        _config_with(pii=pii)


@pytest.mark.parametrize("limits", [
    {"max_respondent_message_chars": 99}, {"max_respondent_message_chars": 1000001}, {"max_bot_message_chars": 0},
    {"max_value_chars": True}, {"max_raw_chars": "500"}, {"max_conversation_chars": 999}, {"unknown": 1}, [],
    {"max_value_chars": 600, "max_raw_chars": 500},
    {"max_respondent_message_chars": 50000, "max_conversation_chars": 40000},
])
def test_a_bad_limits_block_is_refused(limits):
    with pytest.raises(fx.ExtractionConfigError):
        _config_with(limits=limits)


def test_limits_are_applied():
    config = _config_with(limits={"max_respondent_message_chars": 100, "max_bot_message_chars": 100})
    rows = [_row(1, "B" * 150, answer=False), _row(2, "z" * 101)]
    conversation = fx.build_conversation(rows, (), config.limits)
    assert conversation.oversized_respondent_message is True
    assert conversation.turns[0].text == "B" * 100 + " [truncated]"
    assert fx.build_conversation([_row(1, "z" * 100)], (), config.limits).oversized_respondent_message is False
