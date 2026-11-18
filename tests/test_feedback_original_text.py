"""
The extraction call reads the ORIGINAL conversation (config `pii.extraction_text`, default 'original') so the extracted
words (raw_value, justification) are the respondent's own and can be analysed first; the masking stays exactly as it was
and its masked copy is the only text that may be shared. Also (pure tests, fake LLM, SYNTHETIC data only):

  * the masking call still reads the locally masked text, the masked copy never holds the original words;
  * 'masked' keeps the old behaviour, anything else is refused;
  * a spreadsheet formula typed as an answer is not a handle or a link (its digits are still checked);
  * 'skip', 'idk', 'none' ... returned by the model for a category field mean 'Not answered';
  * a justification of a masking finding that repeats a word the masking took out is dropped;
  * a session skipped for size is 'Not answered' everywhere, never a real category.
"""
import json

import pytest

from app.services import feedback_extraction as fx
from app.services import feedback_pii_masking as pm

NAME, PHONE, EMAIL = "Asha Verma", "98765 43210", "asha.verma@gmail.com"
MARK = "Analyse the following answers:\n"
PII_TYPES = ("name", "phone", "email", "address", "id_number", "other")


def _config(extraction_text=None, masking=True):
    pii = {}
    if extraction_text is not None:
        pii["extraction_text"] = extraction_text
    if masking:
        pii["masking_prompt_name"] = "Mask"
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "p",
           "fields": [{"name": "district", "kind": "free_text", "description": "District."},
                      {"name": "gender", "kind": "category", "description": "Gender.", "categories": ["Male", "Female", "Others"]}]}
    if len(pii) > 0:
        raw["pii"] = pii
    return fx.parse_extraction_config(raw)


ROWS = [
    {"sequence": 1, "parent_id": None, "question_key": "Q", "message": "Your name and contact?", "translated_message": None},
    {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": f"I am {NAME}, call {PHONE} or mail {EMAIL}", "translated_message": None},
    {"sequence": 3, "parent_id": None, "question_key": "G", "message": "Your gender?", "translated_message": None},
    {"sequence": 4, "parent_id": "p", "question_key": "G", "message": "Female", "translated_message": None},
]


def _masking_model(prompt):
    pairs = json.loads(prompt.split(MARK, 1)[1].strip())
    return json.dumps({k: {"masked_text": v["answer"].replace(NAME, "<PERSON>"),
                           "pii_found": [{"type": "PERSON", "justification": "a name"}] if NAME in v["answer"] else [],
                           "abusive_language": False, "abusive_spans": []} for k, v in pairs.items()})


EXTRACTION_ANSWER = json.dumps({
    "fields": {"district": {"value": "Mysuru", "raw": f"I am {NAME}, call {PHONE}", "confidence": 1, "justification": "named"},
               "gender": {"value": "Female", "raw": "Female", "confidence": 1, "justification": "stated"}},
    "pii": {"detected": False, "types": []}})


class Calls:
    def __init__(self):
        self.masking, self.extraction = [], []

    def masking_call(self, prompt):
        self.masking.append(prompt)
        return _masking_model(prompt), {"prompt_tokens": 1, "completion_tokens": 1}

    def extraction_call(self, prompt):
        self.extraction.append(prompt)
        return EXTRACTION_ANSWER, {"prompt_tokens": 2, "completion_tokens": 2}


def _run(config, calls=None):
    calls = calls or Calls()
    step = pm.MaskingStep("keys {columns}", "Analyse the following answers:\n{{text}}", calls.masking_call) if config.pii_policy.masking_prompt_name else None
    run = fx.extract(ROWS, config, "{{fields}}", "{{conversation}}", calls.extraction_call, (), step)
    return run, calls


# ------------------------------------------------------------------------------------------------ what each call reads

def test_the_extraction_call_reads_the_original_text_by_default_and_raw_value_is_the_original():
    run, calls = _run(_config())
    assert NAME in calls.extraction[0] and "98765 43210" in calls.extraction[0] and EMAIL in calls.extraction[0]
    assert "<PERSON>" not in calls.extraction[0] and "[number removed]" not in calls.extraction[0]
    district = {f.field_name: f for f in run.result.fields}["district"]
    assert district.raw_value == f"I am {NAME}, call {PHONE}"                    # the respondent's own words
    assert fx.PiiPolicy().extraction_text == "original"


def test_the_masking_call_still_reads_the_locally_masked_text_and_never_the_numbers():
    run, calls = _run(_config())
    assert PHONE not in calls.masking[0] and EMAIL not in calls.masking[0] and "[number removed]" in calls.masking[0]
    assert NAME in calls.masking[0]                                                # the masking model is the one that masks names


def test_the_masked_copy_that_may_be_shared_holds_none_of_the_original_words():
    run, _ = _run(_config())
    shared = json.dumps(run.masking.conversation)
    assert "<PERSON>" in shared and "[number removed]" in shared and "[email removed]" in shared
    for leak in (NAME, "Asha", "98765", EMAIL):
        assert leak not in shared
    assert run.result.pii_detected is True                                         # the flag is unchanged: local check + masking


def test_masked_keeps_the_old_behaviour_the_extraction_reads_the_masked_copy():
    run, calls = _run(_config("masked"))
    for leak in (NAME, "98765", EMAIL):
        assert leak not in calls.extraction[0]
    assert "<PERSON>" in calls.extraction[0] and "[number removed]" in calls.extraction[0]


def test_without_the_masking_step_the_extraction_still_reads_the_original_and_the_flag_comes_from_the_local_check():
    run, calls = _run(_config(masking=False))
    assert NAME in calls.extraction[0] and PHONE in calls.extraction[0]
    assert run.masking is None and run.result.pii_detected is True and "phone" in run.result.pii_types


def test_an_unknown_extraction_text_is_refused():
    for bad in ("both", "", None, True, "ORIGINAL"):
        with pytest.raises(fx.ExtractionConfigError, match="extraction_text"):
            fx.parse_extraction_config({"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "p",
                                        "fields": [{"name": "district", "kind": "free_text", "description": "d"}],
                                        "pii": {"extraction_text": bad}})


# ------------------------------------------------------------------------------------------------ a formula is not a handle

@pytest.mark.parametrize("text", ['=HYPERLINK("http://example.org","click")', "@SUM(1+1)", "+cmd(1)", "-SUM(A1:A2)", '=IMPORTXML("www.x.com","//a")'])
def test_a_formula_typed_as_an_answer_is_not_a_handle_or_a_link(text):
    masked, kinds = fx.mask_respondent_pii([{"sequence": 2, "parent_id": "p", "question_key": None, "message": text, "translated_message": None}])
    assert kinds == () and masked[0]["message"] == text


@pytest.mark.parametrize("text, kind", [("@asha_v", "email"), ("see www.ashablog.in", "other"), ("=SUM(9876543210)", "phone"),
                                         ("mail asha@gmail.com", "email")])
def test_real_handles_links_and_digits_are_still_found(text, kind):
    _, kinds = fx.mask_respondent_pii([{"sequence": 2, "parent_id": "p", "question_key": None, "message": text, "translated_message": None}])
    assert kind in kinds


# ------------------------------------------------------------------------------------------------ "skip" is not a category

GENDER = {"name": "gender", "kind": "category", "description": "g", "categories": ["Male", "Female", "Others", "Unsure / don't know"]}
STATUS = {"name": "status", "kind": "category", "description": "s", "multi": True, "categories": ["Studying", "Working", "Others"]}


def _parse(field, value):
    config = fx.parse_extraction_config({"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "p", "fields": [field]})
    entries = {field["name"]: {"value": value, "raw": None}}
    return {f.field_name: f for f in fx.parse_model_response(json.dumps({"fields": entries, "pii": {"detected": False, "types": []}}), config).fields}[field["name"]]


@pytest.mark.parametrize("word", ["skip", "Skipped", "idk", "none", "nothing", "NA", "unknown", "nil"])
def test_a_placeholder_word_for_a_category_field_is_not_answered_not_others(word):
    field = _parse(GENDER, word)
    assert (field.value, field.status) == ("Not answered", "not_provided")


def test_the_fields_own_categories_win_over_the_placeholder_words():
    assert _parse(GENDER, "Unsure / don't know").value == "Unsure / don't know"
    assert _parse(GENDER, "Martian").value == "Others"                              # a real off-list value is still the fallback


def test_a_multi_field_made_only_of_placeholder_words_is_not_answered_and_real_items_still_count():
    assert _parse(STATUS, ["skip", "idk"]).value == "Not answered"
    assert _parse(STATUS, ["Studying", "skip"]).value == "Studying"
    assert _parse(STATUS, "none").status == "not_provided"


# ------------------------------------------------------------------------------------------------ justifications that quote

def _finding(answer, justification):
    return {"answer": answer, "kind": "pii", "type": "person", "justification": justification}


def test_a_justification_that_repeats_a_masked_out_word_is_dropped_and_the_finding_stays():
    pairs = {"2": {"question": "q", "answer": "my name is Asha Verma and I teach"}}
    masked = {"2": "my name is <PERSON> and I teach"}
    findings = [_finding("2", "Asha Verma is a personal name"), _finding("2", "A person's name identifies the respondent"),
                _finding("2", "ASHA is the respondent")]
    out = pm.scrub_findings(findings, pairs, masked)
    assert [("justification" in f) for f in out] == [False, True, False]
    assert all(f["type"] == "person" and f["answer"] == "2" for f in out)
    assert out[1]["justification"] == "A person's name identifies the respondent"      # 'name' stays in the masked text: kept


def test_scrubbing_works_in_other_scripts_and_never_touches_findings_without_a_justification():
    pairs = {"2": {"question": "q", "answer": "ನನ್ನ ಹೆಸರು ಆಶಾ ವರ್ಮಾ"}}
    masked = {"2": "ನನ್ನ ಹೆಸರು <PERSON>"}
    out = pm.scrub_findings([_finding("2", "ಆಶಾ is a name"), {"answer": "2", "kind": "pii", "type": "person"}, _finding("9", "Asha")], pairs, masked)
    assert "justification" not in out[0] and "justification" not in out[1]
    assert out[2]["justification"] == "Asha"                                           # an unknown answer key: nothing to compare


def test_the_run_stores_findings_with_the_scrub_applied():
    def quoting(prompt):
        pairs = json.loads(prompt.split(MARK, 1)[1].strip())
        return json.dumps({k: {"masked_text": v["answer"].replace(NAME, "<PERSON>"),
                               "pii_found": [{"type": "PERSON", "justification": f"{NAME} is a name"}] if NAME in v["answer"] else [],
                               "abusive_language": False, "abusive_spans": []} for k, v in pairs.items()})
    calls = Calls()
    calls.masking_call = lambda prompt: (quoting(prompt), {"prompt_tokens": 1, "completion_tokens": 1})
    run, _ = _run(_config(), calls)
    assert run.masking.findings and all("justification" not in f for f in run.masking.findings)
    assert NAME not in json.dumps(pm.masking_meta(run.masking, "v"))


# ------------------------------------------------------------------------------------------------ skipped for size

def test_a_session_skipped_for_size_says_not_answered_everywhere_and_stays_on_the_review_list():
    rows = [ROWS[0], {**ROWS[1], "message": "x" * 25000}]
    run = fx.extract(rows, _config(), "{{fields}}", "{{conversation}}", lambda p: pytest.fail("no call"), ())
    assert run.llm_called is False and run.result.pii_detected is True
    assert [(f.value, f.status) for f in run.result.fields] == [("Not answered", "unmapped")] * 2
    assert all("size limit" in f.justification and f.raw_value is None for f in run.result.fields)
