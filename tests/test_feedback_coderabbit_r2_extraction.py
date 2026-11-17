"""
CodeRabbit round 2 (PR #36), extraction side: the LLM timeout clamp subtracts the preparation time (a slow database
stall is RETRYABLE, not a failure), URLs that hold an '@' are masked and flagged, the numbers-only chain is
incremental, and every configured timeout still runs under the activity budget.
SYNTHETIC data only; no database, no network (a fake pool, a fake clock and a fake LLM).
"""
import asyncio
import json
import logging
import random
import time
from types import SimpleNamespace

import pytest

import app.database.feedback_extraction_operations as ops
from app.services import feedback_extraction as fx
from app.services import feedback_extraction_constants as constants
from app.temporal import feedback_extraction_activity as activity_module

SYSTEM = "{{persona}} | {{field_names}}\n{{fields}}\n{{pii_types}}\n{{output_format}}"
USER = "BEGIN {{conversation_id}}\n{{conversation}}\nEND"
MARGIN = constants.ACTIVITY_MARGIN_SECONDS


def _config(**extra):
    raw = {"schema_version": 1, "persona": "Parent", "session_type": "Parent_Voice", "prompt_name": "Some Prompt",
           "fields": [{"name": "district", "kind": "free_text", "description": "The district."}]}
    raw.update(extra)
    return fx.parse_extraction_config(raw)


def _row(seq, text, answer=True, english=None):
    return {"sequence": seq, "parent_id": 1 if answer else None, "question_key": None, "message": text,
            "translated_message": english}


def _answer():
    return json.dumps({"fields": {"district": {"value": "Mysuru", "raw": "Mysuru"}},
                       "pii": {"detected": False, "types": []}})


# ------------------------------------------------------------------------------------------------ inline #13: clamp

def test_the_clamp_subtracts_the_preparation_time_before_the_margin():
    assert ops.clamp_llm_timeout(180, 300, 0) == 180                  # nothing spent: as before
    assert ops.clamp_llm_timeout(100, 300, 50) == 100                 # still fits: 50 + 100 + 120 <= 300
    assert ops.clamp_llm_timeout(180, 300, 50) == 130                 # 300 - 50 - 120
    assert ops.clamp_llm_timeout(1000, 720, 100) == 500               # a long configured timeout: both reasons


def test_a_fraction_of_a_second_of_preparation_changes_nothing():
    """The margin covers it; otherwise the default 180 s call would be cut to 179 s on every single submission."""
    assert ops.clamp_llm_timeout(180, 300, 0.9) == 180
    assert ops.clamp_llm_timeout(180, 300, 1.0) == 179


def test_a_reduction_caused_only_by_preparation_is_info_a_configured_overrun_is_a_warning(caplog):
    caplog.set_level(logging.INFO, logger="analytics_service.feedback_extraction")
    ops.clamp_llm_timeout(180, 300, 50)
    assert [r.levelname for r in caplog.records] == ["INFO"]
    assert "130" in caplog.records[0].getMessage() and "50" in caplog.records[0].getMessage()
    caplog.clear()
    ops.clamp_llm_timeout(200, 300, 0)
    assert [r.levelname for r in caplog.records] == ["WARNING"]


def test_the_call_never_ends_after_the_margin_whatever_the_preparation_took():
    """Property: start + elapsed + effective timeout <= start + budget - margin (to whole seconds), or the call is refused."""
    rnd = random.Random(13)
    for _ in range(3000):
        budget = rnd.randint(MARGIN + 1, 3600)
        timeout = rnd.randint(1, 3480)
        elapsed = rnd.uniform(0, budget * 1.2)
        try:
            effective = ops.clamp_llm_timeout(timeout, budget, elapsed)
        except ops.ExtractionBudgetExhaustedError:
            continue
        assert 1 <= effective <= timeout
        assert int(elapsed) + effective + MARGIN <= budget


def test_a_budget_with_nothing_left_after_the_margin_stays_a_non_retryable_setup_error():
    for budget in (MARGIN, MARGIN - 1, 0):
        with pytest.raises(ops.ExtractionSetupError):
            ops.clamp_llm_timeout(60, budget, 0)
    assert activity_module.classify_failure(ops.ExtractionSetupError("budget"), 1) is not None


def test_preparation_that_used_up_the_budget_is_a_retryable_error_not_a_setup_error():
    """The reviewer's note: failing the step on a database stall would be worse than retrying it."""
    with pytest.raises(ops.ExtractionBudgetExhaustedError) as caught:
        ops.clamp_llm_timeout(180, 300, 175)          # 5 s left: no useful call
    assert not isinstance(caught.value, ops.ExtractionSetupError)
    for attempt in (1, 2, 3):
        assert activity_module.classify_failure(caught.value, attempt) is None      # None = Temporal may retry it
    with pytest.raises(ops.ExtractionBudgetExhaustedError):
        ops.clamp_llm_timeout(180, 300, 400)          # the preparation alone overran the whole budget
    # the floor of a call is min(timeout, MIN_LLM_CALL_SECONDS)
    assert ops.clamp_llm_timeout(180, 300, 150) == 30                                # exactly the floor still runs
    with pytest.raises(ops.ExtractionBudgetExhaustedError):
        ops.clamp_llm_timeout(180, 300, 151)                                         # one second below it


class _Pool:
    """Just enough of a pool for a dry-run extract_submission."""

    def acquire(self):
        class _Ctx:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *exc):
                return False
        return _Ctx()


def _world(monkeypatch, clock, preparation_seconds, config=None):
    """extract_submission whose input and prompt loading advance a fake clock by `preparation_seconds`."""
    config = config if config is not None else _config()

    async def fake_inputs(conn, submission_id, tenant_code):
        clock["now"] += preparation_seconds / 2
        return {"session_type": "Parent_Voice", "questionnaire_version": "V1", "identity": ("Parent_Voice", "t", None),
                "rows": [_row(1, "Q", answer=False), _row(2, "I live in Mysuru")], "fingerprint": "f",
                "loaded_at": None, "config_row": {"id": "cv", "config": config}, "closing_keys": []}

    async def fake_prompt(conn, config):
        clock["now"] += preparation_seconds / 2
        return {"id": "pv", "name": "Some Prompt", "system_prompt": SYSTEM, "user_prompt": USER}

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    monkeypatch.setattr(ops, "load_prompt_for_config", fake_prompt)
    monkeypatch.setattr(ops, "_monotonic", lambda: clock["now"])


def _extract(budget, llm, **kwargs):
    return asyncio.run(ops.extract_submission(_Pool(), "s1", "t", llm_function=llm, write=False,
                                              activity_budget_seconds=budget, **kwargs))


def test_extract_submission_subtracts_what_its_own_preparation_took(monkeypatch):
    clock = {"now": 1000.0}
    _world(monkeypatch, clock, 100, _config(llm={"timeout_seconds": 180}))
    seen = []

    def llm(prompt, **kwargs):
        seen.append(kwargs)
        return _answer(), {}

    _extract(300, llm)                     # configured 180 s: 300 - 100 - 120 = 80 s left for the call
    assert seen[0]["timeout"] == 80


def test_extract_submission_without_slow_preparation_keeps_the_full_timeout(monkeypatch):
    clock = {"now": 1000.0}
    _world(monkeypatch, clock, 0.2, _config(llm={"timeout_seconds": 150}))
    seen = []
    _extract(300, lambda prompt, **kw: (seen.append(kw), (_answer(), {}))[1])
    assert seen[0]["timeout"] == 150


def test_a_slow_database_stall_makes_no_call_and_raises_the_retryable_error(monkeypatch):
    clock = {"now": 1000.0}
    _world(monkeypatch, clock, 160)        # 300 - 160 - 120 = 20 s < the 30 s floor
    calls = []
    with pytest.raises(ops.ExtractionBudgetExhaustedError):
        _extract(300, lambda *a, **k: calls.append(1))
    assert calls == []


def test_the_slow_stall_does_not_write_a_failed_llm_log_it_costs_nothing(monkeypatch):
    """write=True: nothing was called, so no 'failed' llm log either (and no connection for it)."""
    clock = {"now": 1000.0}
    _world(monkeypatch, clock, 200)
    logged = []

    async def spy(*args, **kwargs):
        logged.append(1)

    monkeypatch.setattr(ops, "_log_failed_call", spy)
    with pytest.raises(ops.ExtractionBudgetExhaustedError):
        asyncio.run(ops.extract_submission(_Pool(), "s1", "t", llm_function=lambda *a, **k: None, write=True,
                                           activity_budget_seconds=300))
    assert logged == []


def test_the_cli_without_a_budget_is_untouched_by_preparation_time(monkeypatch):
    clock = {"now": 1000.0}
    _world(monkeypatch, clock, 5000, _config(llm={"timeout_seconds": 1000}))
    seen = []
    asyncio.run(ops.extract_submission(_Pool(), "s1", "t", write=False,
                                       llm_function=lambda prompt, **kw: (seen.append(kw), (_answer(), {}))[1]))
    assert seen[0]["timeout"] == 1000


def test_the_activity_lets_the_exhausted_budget_error_through_as_retryable(monkeypatch):
    """Through Temporal's ActivityEnvironment: a plain exception (the retry policy retries it), not an ApplicationError."""
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import ActivityEnvironment

    monkeypatch.setattr(activity_module, "db", SimpleNamespace(pool="POOL"))

    async def stalled(*args, **kwargs):
        raise ops.ExtractionBudgetExhaustedError("preparation used the budget")

    monkeypatch.setattr(activity_module, "extract_submission", stalled)
    params = {"submission_id": "s1", "tenant_code": "mitra", "analysis_type": constants.ANALYSIS_TYPE}
    with pytest.raises(ops.ExtractionBudgetExhaustedError) as caught:
        asyncio.run(ActivityEnvironment().run(activity_module.feedback_metadata_extraction_activity, params))
    assert not isinstance(caught.value, ApplicationError)


# ------------------------------------------------------------------------------------------------ inline #21

def test_every_configured_timeout_runs_under_the_default_budget_it_is_clamped_never_refused():
    """The config validator accepts 1..MAX_LLM_TIMEOUT_SECONDS; the workflow sizes the budget from the step override or
    the .env setting only. With no preparation time every one of those timeouts must still produce a call."""
    for budget in (constants.extraction_activity_timeout_seconds(None),
                   constants.extraction_activity_timeout_seconds(60),
                   constants.extraction_activity_timeout_seconds(constants.MAX_LLM_TIMEOUT_SECONDS)):
        available = budget - MARGIN
        for timeout in list(range(1, constants.MAX_LLM_TIMEOUT_SECONDS + 1, 29)) + [constants.MAX_LLM_TIMEOUT_SECONDS]:
            effective = ops.clamp_llm_timeout(timeout, budget, 0)
            assert effective == min(timeout, available) and effective >= 1


def test_a_config_timeout_is_clamped_through_the_whole_path_and_the_step_override_raises_the_budget(monkeypatch, caplog):
    caplog.set_level(logging.WARNING, logger="analytics_service.feedback_extraction")
    clock = {"now": 5.0}
    _world(monkeypatch, clock, 0, _config(llm={"timeout_seconds": 600}))
    seen = []

    def llm(prompt, **kwargs):
        seen.append(kwargs["timeout"])
        return _answer(), {}

    _extract(constants.extraction_activity_timeout_seconds(None), llm)                       # no step override
    assert seen == [180] and len([r for r in caplog.records if r.levelname == "WARNING"]) == 1
    seen.clear()
    caplog.clear()
    # the documented way to run the long call: llm_timeout_seconds on the workflow step sizes the budget AND is the timeout
    _extract(constants.extraction_activity_timeout_seconds(600), llm, llm_overrides={"llm_timeout_seconds": 600})
    assert seen == [600] and [r for r in caplog.records if r.levelname == "WARNING"] == []


# ------------------------------------------------------------------------------------------------ inline #19

def _kinds(*texts):
    return fx.pii_precheck_kinds([_row(i + 1, t) for i, t in enumerate(texts)])


def _masked(text):
    rows = [_row(1, text)]
    masked, kinds = fx.mask_respondent_pii(rows)
    return masked[0]["message"], kinds


URL_WITH_AT = [
    ("see https://example.org/@asha_k now", ("asha_k", "example.org")),          # profile URL: '/' precedes the '@'
    ("https://example.org/@asha_k", ("asha_k", "example.org")),
    ("medium.com/@asha_k", ("asha_k", "medium")),                                # bare domain + path
    ("www.example.org/@asha_k", ("asha_k", "example.org")),
    ("example.org/@asha_k/posts", ("asha_k", "example.org")),
    ("https://t.me/@asha_k", ("asha_k", "t.me")),
    ("(https://example.org/@asha_k)", ("asha_k", "example.org")),
    ("ftp://example.org/@asha_k", ("asha_k", "example.org")),
    ("http://example.org/users/@asha_k/photos?x=1", ("asha_k", "example.org")),
    ("https://example.org/a/b@c", ("example.org",)),                             # an '@' inside a path
]


@pytest.mark.parametrize("text,leaks", URL_WITH_AT)
def test_a_url_with_an_at_sign_is_flagged_and_masked_not_sent_to_the_provider(text, leaks):
    masked, kinds = _masked(text)
    assert fx.PII_KIND_URL in kinds or fx.PII_KIND_EMAIL in kinds, text
    for leak in leaks:
        assert leak not in masked, (text, masked)
    assert fx.pii_precheck_kinds([_row(1, text)]) == kinds                          # the detector and the masker agree
    assert fx.pii_precheck_kinds([_row(1, masked)]) == ()                           # nothing left that re-checks dirty


@pytest.mark.parametrize("text", [
    "https://user:pass@host.example.org/p",         # credentials in the URL
    "user:pass@host",
    "mailto:asha@example.org",                      # mailto-like
    "ftp://asha:secret@files.example.org",
])
def test_credentials_and_mailto_shapes_stay_masked(text):
    masked, kinds = _masked(text)
    assert len(kinds) > 0
    for leak in ("asha", "pass", "secret", "example.org", "host"):
        assert leak not in masked, (text, masked)
    assert fx.pii_precheck_kinds([_row(1, masked)]) == ()


def test_the_prompt_the_model_receives_holds_neither_the_profile_url_nor_the_handle():
    prompts = []

    def llm(prompt):
        prompts.append(prompt)
        return _answer(), {"prompt_tokens": 1, "completion_tokens": 1}

    rows = [_row(1, "Where are you from?", answer=False), _row(2, "Mysuru, my page is https://example.org/@asha_k ok")]
    run = fx.extract(rows, _config(), SYSTEM, USER, llm)
    assert len(prompts) == 1
    assert "asha_k" not in prompts[0] and "example.org" not in prompts[0] and constants.MASK_LINK in prompts[0]
    assert "Mysuru" in prompts[0]
    assert run.result.pii_detected is True and "other" in run.result.pii_types


def test_the_translated_column_is_covered_too():
    rows = [_row(1, "no link here", english="my page medium.com/@asha_k")]
    masked, kinds = fx.mask_respondent_pii(rows)
    assert kinds == (fx.PII_KIND_URL,) and "asha_k" not in masked[0]["translated_message"]
    assert masked[0]["message"] == "no link here"


def test_tokens_with_an_at_sign_that_are_neither_address_nor_link_stay_unflagged():
    for text in ("rate@5% only", "x@", "meet @ 5 pm", "a @ b"):
        assert _masked(text) == (text, ()), text


def test_addresses_and_handles_behave_as_before():
    assert _masked("mail asha@gmail.com please") == ("mail [email removed] please", (fx.PII_KIND_EMAIL,))
    assert _masked("insta @asha_k ok") == ("insta [handle removed] ok", (fx.PII_KIND_EMAIL,))
    assert _masked("john.com@example.org") == ("[email removed]", (fx.PII_KIND_EMAIL,))   # the domain-looking name is not a link


def test_url_with_at_shapes_stay_linear_in_the_length_of_the_text():
    shapes = {
        "slash-at": "https://a.org/@" * 1333,
        "bare-slash-at": "a.org/@x " * 2222,
        "at-slash": "@/" * 10000,
        "mixed": "a@b/" * 5000,
        "creds": "https://u:p@h.org/@" * 1052,
    }
    for name, big in shapes.items():
        small = big[:max(len(big) // 8, 40)]
        small_rows, big_rows = [_row(1, small, english=small)], [_row(1, big, english=big)]
        t_small = _best_of(small_rows)
        t_big = _best_of(big_rows)
        assert t_big < 5.0, (name, t_big)
        assert t_big < 60 * max(t_small, 0.0005), (name, t_small, t_big)


def _best_of(rows, runs=3):
    best = None
    for _ in range(runs):
        start = time.perf_counter()
        fx.mask_respondent_pii(rows)
        elapsed = time.perf_counter() - start
        best = elapsed if best is None else min(best, elapsed)
    return best


# ------------------------------------------------------------------------------------------------ review body: chain

def _reference_is_number(groups, policy):
    """The original, non-incremental rule, written out again here."""
    if all(len(g) == 4 and constants.YEAR_MIN <= int(g) <= constants.YEAR_MAX for g in groups):
        return False
    short = len(groups) > 1 and all(len(g) <= 2 for g in groups)
    needed = policy.min_digit_run_short_groups if short else policy.min_digit_run
    return sum(len(g) for g in groups) >= needed


def test_the_incremental_run_statistics_equal_the_full_rescan_for_every_prefix():
    rnd = random.Random(657)
    pool = ["2020", "1999", "2100", "123", "98765", "43210", "12", "7", "045", "1900", "2099", "12345678"]
    policy = fx.DEFAULT_PII_POLICY
    for _ in range(3000):
        groups = [rnd.choice(pool) for _ in range(rnd.randint(0, 9))]
        stats = fx._RunStats()
        for end in range(len(groups) + 1):
            if end > 0:
                stats.add([groups[end - 1]])
            assert stats.is_number(policy) == _reference_is_number(groups[:end], policy), groups[:end]
        assert fx._run_is_number(groups, policy) == _reference_is_number(groups, policy)


def test_many_consecutive_year_messages_are_checked_in_linear_time():
    """n messages of '2020': the old chain check rescanned the whole accumulated chain on every message (n(n+1)/2
    year checks: 9 s for 8,000 messages). Never a number (all years), so nothing is flagged or masked."""
    def rows(n):
        return [_row(i, "2020") for i in range(1, n + 1)]

    small, big = rows(2500), rows(20000)
    t_small, t_big = _best_of(small, 2), _best_of(big, 2)
    assert fx.pii_precheck_kinds(big) == ()
    masked, kinds = fx.mask_respondent_pii(big)
    assert kinds == () and all(m is r for m, r in zip(masked, big))
    assert t_big < 3.0, t_big
    assert t_big < 24 * max(t_small, 0.001), (t_small, t_big)       # 8x the messages: linear, with room (quadratic is 64x)


def test_a_long_chain_of_years_that_ends_in_a_real_number_is_still_masked_in_every_message():
    rows = [_row(i, "2020") for i in range(1, 6)] + [_row(6, "98765"), _row(7, "43210")]
    assert fx.pii_precheck_kinds(rows) == (fx.PII_KIND_NUMBER,)
    masked, _ = fx.mask_respondent_pii(rows)
    assert [m["message"] for m in masked] == ["[number removed]"] * 7


def test_the_chain_resets_on_a_message_with_words_and_short_groups_never_join_it():
    rows = [_row(1, "98765"), _row(2, "ok"), _row(3, "43210")]
    assert fx.pii_precheck_kinds(rows) == ()
    assert fx.pii_precheck_kinds([_row(i, "123") for i in range(1, 5)]) == (fx.PII_KIND_NUMBER,)      # 12 digits in parts
    assert fx.pii_precheck_kinds([_row(i, "123") for i in range(1, 3)]) == ()                         # 6 digits: below the run size
