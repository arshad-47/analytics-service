"""
The model's justification of what it extracted or flagged, the complete responses of the calls, and the stage-by-stage
view of the ONE Temporal activity (no database, no network, fake LLM). SYNTHETIC data only.

  * extraction: an optional `justification` per field is kept (one line, cut, neutralised); missing or malformed is
    NULL and never fails the session; the prompt's output format asks for it;
  * masking: every pii_found / abusive_spans item becomes a finding with its justification and confidence, never text;
    the older `reason` key is read when `justification` is absent;
  * extract(): reports each stage to `progress`, keeps the raw answers, attaches the answer of an unusable call;
  * StageTracker: timings, tokens, skipped stages, thread-safe relay to the event loop's thread;
  * the activity: `stages` in its result, heartbeats, the failed stage in a non-retryable error;
  * the workflow copies the stages into the step's entry of process_status.
"""
import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import install_fake_workflow_context

import app.database.feedback_extraction_operations as ops
import app.temporal.feedback_extraction_activity as activity_module
import app.temporal.workflows as workflows_module
from app.services import feedback_extraction as fx
from app.services import feedback_pii_masking as pm
from app.services.feedback_extraction_constants import (
    FAILED_RESPONSE_MAX_CHARS, MAX_JUSTIFICATION_CHARS, STAGE_METADATA_EXTRACTION, STAGE_PII_MASKING, STEP_NAME)
from seed_support import extraction_config_v1

ROOT = Path(__file__).resolve().parent.parent
PII_TYPES = ("name", "phone", "email", "address", "id_number", "other")


def _config(masking=True):
    raw = extraction_config_v1("Youth")
    if masking:
        raw["pii"]["masking_prompt_name"] = "Feedback PII and Abusive-Language Detection"
    else:
        raw["pii"].pop("masking_prompt_name", None)
    return fx.parse_extraction_config(raw)


def _answer(config, justification="__absent__", **overrides):
    fields = {}
    for name in config.field_names():
        entry = {"value": None, "raw": None, "confidence": None}
        if justification != "__absent__":
            entry["justification"] = justification
        fields[name] = entry
    fields.update(overrides)
    return json.dumps({"fields": fields, "pii": {"detected": False, "types": []}})


# ------------------------------------------------------------------------------------------------ extraction

def test_a_justification_is_kept_as_one_clean_line():
    config = _config()
    result = fx.parse_model_response(_answer(config, "  The respondent  said\nthey live in Delhi.  "), config)
    assert {f.justification for f in result.fields} == {"The respondent said they live in Delhi."}


def test_a_missing_blank_or_malformed_justification_is_none_and_never_fails_the_session():
    config = _config()
    for given in ("__absent__", None, "", "   ", 42, ["a"], {"a": 1}, True):
        result = fx.parse_model_response(_answer(config, given), config)
        assert all(f.justification is None for f in result.fields), given


def test_a_long_justification_is_cut_and_one_that_starts_like_a_formula_is_neutralised():
    config = _config()
    long = fx.parse_model_response(_answer(config, "x" * (MAX_JUSTIFICATION_CHARS + 50)), config)
    assert all(len(f.justification) == MAX_JUSTIFICATION_CHARS for f in long.fields)
    formula = fx.parse_model_response(_answer(config, "=HYPERLINK(1)"), config)
    assert all(f.justification == "'=HYPERLINK(1)" for f in formula.fields)


def test_an_unknown_key_in_a_field_still_fails_and_justification_is_the_only_new_one():
    config = _config()
    name = config.field_names()[0]
    with pytest.raises(fx.ExtractionOutputError, match="unexpected key"):
        fx.parse_model_response(_answer(config, **{name: {"value": None, "raw": None, "why": "x"}}), config)


def test_the_justification_belongs_to_unmapped_and_not_provided_fields_too():
    config = _config()
    name = next(s.name for s in config.fields if s.kind == "category")
    entry = {"value": None, "raw": "Delhi/NCR", "confidence": 0.5, "justification": "NCR is a region, not a state."}
    result = fx.parse_model_response(_answer(config, **{name: entry}), config)
    field = next(f for f in result.fields if f.field_name == name)
    assert field.status == "unmapped" and field.justification == "NCR is a region, not a state."


def test_the_output_format_of_the_prompt_asks_for_a_short_justification_that_does_not_quote():
    config = _config()
    text = fx._render_output_format(config)
    assert text.count('"justification": null') == len(config.field_names())
    assert "at most 20 words" in text and "do not quote" in text


# ------------------------------------------------------------------------------------------------ masking findings

def _masking_answer(**entries):
    return json.dumps(entries)


def test_findings_carry_type_justification_and_confidence_and_never_text():
    answer = _masking_answer(**{
        "2": {"masked_text": "<PERSON>", "abusive_language": True,
              "pii_found": [{"type": "PERSON", "confidence": 0.97, "justification": "A person name."}],
              "abusive_spans": [{"severity": "Moderate", "confidence": 2, "justification": "An insult at a teacher."}]},
        "4": {"masked_text": "fine", "pii_found": [], "abusive_language": False, "abusive_spans": []},
    })
    masked, types, abusive, severities, findings = pm.parse_masking_details(answer, ["2", "4"], PII_TYPES)
    assert masked == {"2": "<PERSON>", "4": "fine"} and types == ("name",) and abusive and severities == ("moderate",)
    assert findings == (
        {"answer": "2", "kind": "pii", "type": "person", "confidence": 0.97, "justification": "A person name."},
        {"answer": "2", "kind": "abusive_language", "severity": "moderate",       # confidence 2 is out of range: dropped
         "justification": "An insult at a teacher."},
    )


def test_the_older_reason_key_is_read_when_justification_is_absent_and_justification_wins_when_both_exist():
    answer = _masking_answer(**{
        "2": {"masked_text": "<PHONE>", "abusive_language": False, "abusive_spans": [],
              "pii_found": [{"type": "PHONE", "reason": "A phone number."},
                            {"type": "ID", "reason": "old", "justification": "new"}]}})
    findings = pm.parse_masking_details(answer, ["2"], PII_TYPES)[4]
    assert [f["justification"] for f in findings] == ["A phone number.", "new"]


def test_a_finding_without_justification_still_lists_its_type_and_a_string_item_does_not_crash():
    answer = _masking_answer(**{"2": {"masked_text": "x", "pii_found": [{"type": "EMAIL"}, "PERSON", None],
                                      "abusive_language": False, "abusive_spans": []}})
    findings = pm.parse_masking_details(answer, ["2"], PII_TYPES)[4]
    assert findings[0] == {"answer": "2", "kind": "pii", "type": "email"}
    assert len(findings) == 3


def test_the_four_value_parse_is_unchanged_and_the_stored_masking_object_lists_the_findings():
    answer = _masking_answer(**{"2": {"masked_text": "<PERSON>", "abusive_language": False, "abusive_spans": [],
                                      "pii_found": [{"type": "PERSON", "justification": "A name."}]}})
    assert pm.parse_masking_response(answer, ["2"], PII_TYPES) == pm.parse_masking_details(answer, ["2"], PII_TYPES)[:4]
    masked, types, abusive, severities, findings = pm.parse_masking_details(answer, ["2"], PII_TYPES)
    result = pm.MaskingResult({"2": {"question": "q", "answer": masked["2"]}}, types, abusive, severities, {}, findings,
                              "RAW")
    meta = pm.masking_meta(result, "v1")
    assert meta["findings"] == [{"answer": "2", "kind": "pii", "type": "person", "justification": "A name."}]
    assert "RAW" not in json.dumps(meta)       # the raw answer is stored elsewhere (extraction.responses), if at all


def test_the_seeded_masking_prompt_asks_for_justification_and_forbids_quoting():
    seed = (ROOT / "seed_prompts.sql").read_text(encoding="utf-8")
    block = seed[seed.index("Feedback PII and Abusive-Language Detection (optional step"):]
    assert '"justification": "max 12 words, why this is personal data"' in block
    assert '"justification": "max 12 words, why this is abusive"' in block
    assert "never quote it" in block


# ------------------------------------------------------------------------------------------------ extract(): progress

ROWS = [
    {"sequence": 1, "parent_id": None, "question_key": "Q", "message": "Which state?", "translated_message": None},
    {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "Delhi/NCR", "translated_message": None},
]
USAGE = {"prompt_tokens": 7, "completion_tokens": 3}


def _masking_step(answer=None, fail=False):
    def call(prompt):
        if fail:
            raise ops.LlmCallError("LLM call failed (TimeoutError)")
        return (answer if answer is not None else _masking_answer(**{
            "2": {"masked_text": "Delhi/NCR", "pii_found": [], "abusive_language": False, "abusive_spans": []}}), USAGE)
    return pm.MaskingStep("keys {columns}", "in {{text}}", call)


def _extract(config, call, masking=None, progress=None):
    system = user = "{{fields}} {{conversation}}"
    return fx.extract(ROWS, config, system, user, call, (), masking, progress)


def test_progress_reports_each_stage_in_order_and_the_run_keeps_both_raw_answers():
    config = _config()
    seen = []
    answer = _answer(config, "why")
    run = _extract(config, lambda prompt: (answer, USAGE), _masking_step(),
                   lambda stage, status, usage: seen.append((stage, status, usage)))
    assert [(s, st) for s, st, _ in seen] == [
        (STAGE_PII_MASKING, "running"), (STAGE_PII_MASKING, "success"),
        (STAGE_METADATA_EXTRACTION, "running"), (STAGE_METADATA_EXTRACTION, "success")]
    assert seen[1][2] == USAGE and seen[3][2] == USAGE and seen[0][2] is None
    assert run.response == answer and run.masking.response is not None and "Delhi/NCR" in run.masking.response


def test_without_a_masking_step_only_the_extraction_stage_is_reported():
    config = _config(masking=False)
    seen = []
    _extract(config, lambda prompt: (_answer(config), USAGE), None, lambda *a: seen.append(a[:2]))
    assert seen == [(STAGE_METADATA_EXTRACTION, "running"), (STAGE_METADATA_EXTRACTION, "success")]


def test_a_failing_masking_call_is_reported_failed_and_the_extraction_stage_never_starts():
    config = _config()
    seen = []
    with pytest.raises(ops.LlmCallError):
        _extract(config, lambda prompt: pytest.fail("no extraction call"), _masking_step(fail=True),
                 lambda *a: seen.append(a[:2]))
    assert seen == [(STAGE_PII_MASKING, "running"), (STAGE_PII_MASKING, "failed")]


def test_an_unusable_masking_answer_is_attached_to_the_error_with_its_usage_and_the_stage_is_failed():
    config = _config()
    seen = []
    with pytest.raises(fx.ExtractionOutputError) as caught:
        _extract(config, lambda prompt: pytest.fail("no extraction call"), _masking_step("not json"),
                 lambda *a: seen.append(a))
    assert caught.value.response == "not json" and caught.value.purpose == "masking" and caught.value.usage == USAGE
    assert seen[-1] == (STAGE_PII_MASKING, "failed", USAGE)


def test_an_unusable_extraction_answer_is_attached_to_the_error_and_the_masking_stage_stays_a_success():
    config = _config()
    seen = []
    with pytest.raises(fx.ExtractionOutputError) as caught:
        _extract(config, lambda prompt: ("{broken", USAGE), _masking_step(), lambda *a: seen.append(a[:2]))
    assert caught.value.response == "{broken"
    assert seen == [(STAGE_PII_MASKING, "running"), (STAGE_PII_MASKING, "success"),
                    (STAGE_METADATA_EXTRACTION, "running"), (STAGE_METADATA_EXTRACTION, "failed")]


def test_an_error_inside_the_progress_function_never_disturbs_the_extraction():
    config = _config()

    def broken(stage, status, usage):
        raise RuntimeError("the reporter is down")
    run = _extract(config, lambda prompt: (_answer(config), USAGE), _masking_step(), broken)
    assert run.llm_called is True and run.result.fields


# ------------------------------------------------------------------------------------------------ StageTracker

def test_the_tracker_records_status_seconds_tokens_and_a_reason_for_a_skipped_stage(monkeypatch):
    clock = iter([10.0, 11.5, 11.5, 14.0, 14.0])
    monkeypatch.setattr(ops, "_stage_clock", lambda: next(clock))
    seen = []
    tracker = ops.StageTracker("s1", seen.append)
    tracker.update(STAGE_PII_MASKING, "running")
    tracker.update(STAGE_PII_MASKING, "success", USAGE)
    tracker.update(STAGE_METADATA_EXTRACTION, "running")
    tracker.update(STAGE_METADATA_EXTRACTION, "failed")
    tracker.update("other", "skipped", reason="no_respondent_answer")
    assert tracker.stages == [
        {"name": STAGE_PII_MASKING, "status": "success", "seconds": 1.5, "prompt_tokens": 7, "completion_tokens": 3},
        {"name": STAGE_METADATA_EXTRACTION, "status": "failed", "seconds": 2.5},
        {"name": "other", "status": "skipped", "reason": "no_respondent_answer"},
    ]
    assert len(seen) == 5 and seen[0] == [{"name": STAGE_PII_MASKING, "status": "running"}]


def test_the_stage_clock_is_not_the_budget_clock(monkeypatch):
    """Reporting a stage must not consume the readings the activity-budget arithmetic takes from `_monotonic`."""
    readings = []
    monkeypatch.setattr(ops, "_monotonic", lambda: readings.append(1) or 0.0)
    tracker = ops.StageTracker("s1")
    tracker.update(STAGE_PII_MASKING, "running")
    tracker.update(STAGE_PII_MASKING, "success", USAGE)
    assert readings == []


def test_the_tracker_never_holds_text_and_a_failing_listener_is_ignored():
    def broken(stages):
        raise RuntimeError("listener down")
    tracker = ops.StageTracker("s1", broken)
    tracker.update(STAGE_PII_MASKING, "running")
    tracker.update(STAGE_PII_MASKING, "success", {**USAGE, "choices": [{"message": "private words"}]})
    assert "private" not in json.dumps(tracker.stages)


def test_a_worker_thread_reaches_the_listener_on_the_event_loops_thread():
    """The Temporal heartbeat must run on the loop's thread; the LLM calls run on a worker thread."""
    async def main():
        threads = []
        tracker = ops.StageTracker("s1", lambda stages: threads.append(threading.get_ident()), asyncio.get_running_loop())
        loop_thread = threading.get_ident()
        await asyncio.to_thread(tracker.update, STAGE_PII_MASKING, "running")
        await asyncio.sleep(0)               # let the call_soon_threadsafe callback run
        tracker.update(STAGE_PII_MASKING, "success", USAGE)   # on the loop thread itself: direct
        return loop_thread, threads
    loop_thread, threads = asyncio.run(main())
    assert threads == [loop_thread, loop_thread]


def test_the_failed_response_copy_is_cut_and_marked_and_absent_when_there_is_none():
    assert ops.failed_response_meta(None) == {} and ops.failed_response_meta("") == {} and ops.failed_response_meta(5) == {}
    assert ops.failed_response_meta("abc") == {"response": "abc"}
    long = "x" * (FAILED_RESPONSE_MAX_CHARS + 1)
    assert ops.failed_response_meta(long) == {"response": "x" * FAILED_RESPONSE_MAX_CHARS, "response_truncated": True}


# ------------------------------------------------------------------------------------------------ the activity

STAGES = [{"name": STAGE_PII_MASKING, "status": "success", "seconds": 1.0},
          {"name": STAGE_METADATA_EXTRACTION, "status": "failed", "seconds": 2.0}]


def test_the_result_lists_the_stages_and_the_heartbeat_carries_them(monkeypatch):
    from temporalio.testing import ActivityEnvironment
    config = _config()
    monkeypatch.setattr(activity_module, "db", SimpleNamespace(pool="POOL"))
    beats = []

    async def fake(pool, submission_id, tenant_code, **kwargs):
        kwargs["on_stage"]([{"name": STAGE_PII_MASKING, "status": "running"}])
        return SimpleNamespace(run=SimpleNamespace(result=fx.empty_result(config), llm_called=True),
                               stages=[{"name": STAGE_PII_MASKING, "status": "success"}])

    monkeypatch.setattr(activity_module, "extract_submission", fake)
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: beats.append(details)
    out = asyncio.run(env.run(activity_module.feedback_metadata_extraction_activity,
                              {"submission_id": "s1", "tenant_code": "mitra", "analysis_type": STEP_NAME}))
    assert out["stages"] == [{"name": STAGE_PII_MASKING, "status": "success"}]
    assert beats == [({"stages": [{"name": STAGE_PII_MASKING, "status": "running"}]},)]


def test_a_worker_thread_heartbeat_reaches_temporal_through_the_event_loop():
    """
    The LLM calls run on a worker thread (asyncio.to_thread) but the SDK's heartbeat may only run on the event loop's
    thread: the tracker hands the stage list over, and the activity that is current in the worker is the right one.
    """
    from temporalio.testing import ActivityEnvironment
    beats = []

    async def inside_an_activity():
        tracker = ops.StageTracker("s1", activity_module._report_stages, asyncio.get_running_loop())
        await asyncio.to_thread(tracker.update, STAGE_PII_MASKING, "running")
        await asyncio.to_thread(tracker.update, STAGE_PII_MASKING, "success", USAGE)
        await asyncio.sleep(0)

    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: beats.append(details)
    asyncio.run(env.run(inside_an_activity))
    assert [d[0]["stages"][0]["status"] for d in beats] == ["running", "success"]


def test_reporting_stages_outside_a_temporal_activity_does_nothing():
    activity_module._report_stages([{"name": "x", "status": "running"}])     # no RuntimeError


def test_a_non_retryable_failure_names_the_failed_stage_and_carries_the_stage_list(monkeypatch):
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment
    monkeypatch.setattr(activity_module, "db", SimpleNamespace(pool="POOL"))

    async def bad(*args, **kwargs):
        error = ops.LlmCallError("LLM call failed (RuntimeError, HTTP 401)", 401)
        error.stages = STAGES
        raise error

    monkeypatch.setattr(activity_module, "extract_submission", bad)
    with pytest.raises(ApplicationError) as caught:
        asyncio.run(ActivityEnvironment().run(activity_module.feedback_metadata_extraction_activity,
                                              {"submission_id": "s1", "tenant_code": "mitra"}))
    assert caught.value.non_retryable is True and caught.value.type == "LlmCallError"
    assert caught.value.message.endswith("(stage: metadata_extraction)") and "HTTP 401" in caught.value.message
    assert caught.value.details == ({"stages": STAGES},)


def test_a_failure_before_any_stage_has_no_stage_in_its_text(monkeypatch):
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment
    monkeypatch.setattr(activity_module, "db", SimpleNamespace(pool="POOL"))

    async def no_config(*args, **kwargs):
        raise ops.ExtractionSetupError("No extraction config")

    monkeypatch.setattr(activity_module, "extract_submission", no_config)
    with pytest.raises(ApplicationError) as caught:
        asyncio.run(ActivityEnvironment().run(activity_module.feedback_metadata_extraction_activity,
                                              {"submission_id": "s1", "tenant_code": "mitra"}))
    assert "stage" not in caught.value.message and caught.value.non_retryable is True


# ------------------------------------------------------------------------------------------------ the workflow

def _payload(steps):
    return {"submission_id": "s1", "tenant_code": "mitra", "process_steps": steps}


def test_the_workflow_copies_the_stages_into_the_step_entry_of_process_status(monkeypatch):
    async def run():
        result_stub = {"status": "success", "stages": [{"name": STAGE_PII_MASKING, "status": "success", "seconds": 1.2},
                                                      {"name": STAGE_METADATA_EXTRACTION, "status": "success"}]}
        exec_mock, _, _ = install_fake_workflow_context(
            monkeypatch, activity_results={workflows_module.feedback_metadata_extraction_activity: result_stub})
        result = await workflows_module.ConfigDrivenProcessingWorkflow().run(_payload([{"name": STEP_NAME}]))
        assert result["steps"][0]["stages"] == result_stub["stages"]
        last = exec_mock.call_args_list[-1].args[1]
        assert last["process_status"]["steps"][0]["stages"] == result_stub["stages"]
    asyncio.run(run())


def test_a_result_without_stages_leaves_the_step_entry_as_before(monkeypatch):
    async def run():
        install_fake_workflow_context(
            monkeypatch, activity_results={workflows_module.feedback_metadata_extraction_activity: {"status": "success"}})
        result = await workflows_module.ConfigDrivenProcessingWorkflow().run(_payload([{"name": STEP_NAME}]))
        assert "stages" not in result["steps"][0]
    asyncio.run(run())
