"""
Real-Postgres tests of the model-based masking step inside `extract_submission` (opt-in like the other *_pg files:
FEEDBACK_TEST_DATABASE_URL names a Postgres server; tests/pg_support.py gives this file its own scratch database).
Fake LLM only (no network). SYNTHETIC data only.

  * the masking call runs first on the question and answer JSON of the stored session, the extraction call receives
    the masked answers only, and both calls get an llm_logs row of their own analysis type;
  * the masked answers, the PII types and the abuse flag are kept in feedback_submissions.meta_data.extraction.masking
    (masked text only; no schema change) and pii_detected follows the masking call;
  * a bad masking answer, or a failure of the extraction call after a good masking call, leaves the session
    unwritten (pii_detected NULL), logs what was really called and never uses the original text as a fallback;
  * a config without the key makes no masking call and a re-run drops an older masking record;
  * a prompt name that does not exist or is not a masking prompt fails before any call.
"""
import asyncio
import json
import uuid

import asyncpg
import pytest

import pg_support
from app.database.db import SEED_PROMPTS_FILE, db
from app.database.feedback_extraction_operations import ExtractionSetupError, extract_submission
from app.database.operations import insert_or_update_submission
from app.services.feedback_extraction import ExtractionOutputError
from seed_support import PERSONAS, extraction_config_id, seed_config
from test_feedback_extraction_tenant_pg import _event

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

MASKING_NAME = "Feedback PII and Abusive-Language Detection"
NAME_WORDS = "zzq-asha-verma"
SCHOOL_WORDS = "zzq-dps-dumra"
MASKING_MARK = "Analyse the following answers:"


def _run(body):
    async def main():
        conn = await asyncpg.connect(pg_support.current_dsn())
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await conn.execute(SEED_PROMPTS_FILE.read_text(encoding="utf-8"))
            await seed_config(conn)
            pool = await asyncpg.create_pool(pg_support.current_dsn(), min_size=2, max_size=6)
            db.pool = pool
            return await body(conn, pool)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions")
            await conn.close()
    return asyncio.run(main())


async def _session(conn, masking=MASKING_NAME, sid=None):
    """A stored Youth session with a name answer and a school answer, and a one-field extraction config."""
    sid = sid if sid is not None else f"m-{uuid.uuid4().hex[:8]}"
    event = _event("mitra", sid, "x")
    event["data"]["messages"] = [
        {"sequence": 1, "questionKey": "NAME", "isFollowUp": False, "message": "What is your name?",
         "translatedMessage": None, "messageAt": None, "parentSequence": None},
        {"sequence": 2, "questionKey": "NAME", "isFollowUp": False, "message": NAME_WORDS,
         "translatedMessage": None, "messageAt": None, "parentSequence": 1},
        {"sequence": 3, "questionKey": "SCHOOL", "isFollowUp": False, "message": "Which school do you attend?",
         "translatedMessage": None, "messageAt": None, "parentSequence": None},
        {"sequence": 4, "questionKey": "SCHOOL", "isFollowUp": False, "message": SCHOOL_WORDS,
         "translatedMessage": None, "messageAt": None, "parentSequence": 3},
    ]
    await insert_or_update_submission(conn, event)
    await _set_config(conn, masking)
    return sid


async def _set_config(conn, masking):
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "Feedback Metadata Extraction",
           "fields": [{"name": "district", "kind": "free_text", "description": "District."}]}
    if masking is not None:
        raw["pii"] = {"masking_prompt_name": masking}
    await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                       extraction_config_id("Youth_Voice"), json.dumps(raw))


class Fake:
    """Answers the masking prompt (recognised by its text) and the extraction prompt; records every prompt."""

    def __init__(self, masking=None, extraction=None, fail_extraction=False):
        self.masking, self.extraction, self.fail_extraction = masking, extraction, fail_extraction
        self.prompts = []

    def __call__(self, prompt, model=None, max_tokens=None, timeout=None):
        self.prompts.append(prompt)
        if MASKING_MARK in prompt:
            pairs = json.loads(prompt.split(MASKING_MARK + "\n", 1)[1].strip())
            answer = self.masking(pairs) if self.masking is not None else _mask_names(pairs)
            return answer, {"prompt_tokens": 3, "completion_tokens": 4}
        if self.fail_extraction:
            return "not json", {"prompt_tokens": 6, "completion_tokens": 1}
        answer = self.extraction if self.extraction is not None else json.dumps(
            {"fields": {"district": {"value": "Mysuru", "raw": "mysore", "confidence": 0.9}},
             "pii": {"detected": False, "types": []}})
        return answer, {"prompt_tokens": 8, "completion_tokens": 2}


def _mask_names(pairs):
    out = {}
    for key, pair in pairs.items():
        if pair["answer"] == NAME_WORDS:
            out[key] = {"masked_text": "<PERSON>", "pii_found": [{"type": "PERSON"}], "abusive_language": False,
                        "abusive_spans": []}
        else:
            out[key] = {"masked_text": pair["answer"], "pii_found": [], "abusive_language": False, "abusive_spans": []}
    return json.dumps(out)


async def _meta(conn, sid):
    row = await conn.fetchrow("SELECT pii_detected, meta_data FROM feedback_submissions WHERE submission_id = $1", sid)
    meta = row["meta_data"]
    return row["pii_detected"], (json.loads(meta) if isinstance(meta, str) else meta)


async def _logs(conn, sid):
    rows = await conn.fetch(
        "SELECT analysis_type, status, prompt_tokens, completion_tokens, error_message FROM llm_logs "
        "WHERE submission_id = $1 ORDER BY analysis_type", sid)
    return [dict(r) for r in rows]


def test_the_masking_call_runs_first_and_the_extraction_prompt_holds_only_the_masked_answers():
    async def body(conn, pool):
        sid = await _session(conn)
        fake = Fake()
        outcome = await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert len(fake.prompts) == 2 and MASKING_MARK in fake.prompts[0] and MASKING_MARK not in fake.prompts[1]
        assert NAME_WORDS in fake.prompts[0]
        assert NAME_WORDS not in fake.prompts[1] and "<PERSON>" in fake.prompts[1] and SCHOOL_WORDS in fake.prompts[1]
        pairs = json.loads(fake.prompts[0].split(MASKING_MARK + "\n", 1)[1].strip())
        assert pairs == {"2": {"question": "What is your name?", "answer": NAME_WORDS},
                         "4": {"question": "Which school do you attend?", "answer": SCHOOL_WORDS}}
        assert outcome.run.masking is not None
    _run(body)


def test_the_masked_answers_and_the_flags_are_stored_in_meta_data_and_both_calls_are_logged():
    async def body(conn, pool):
        sid = await _session(conn)
        await extract_submission(pool, sid, "mitra", llm_function=Fake())
        pii, meta = await _meta(conn, sid)
        assert pii is True                                                   # only the masking call found PII
        masking = meta["extraction"]["masking"]
        assert masking["conversation"] == {
            "2": {"question": "What is your name?", "answer": "<PERSON>"},
            "4": {"question": "Which school do you attend?", "answer": SCHOOL_WORDS}}
        assert masking["pii_types"] == ["name"] and masking["abusive_language"] is False
        assert masking["abusive_severities"] == [] and uuid.UUID(masking["prompt_version_id"])
        assert NAME_WORDS not in json.dumps(meta)                            # the original words are nowhere in meta_data
        logs = await _logs(conn, sid)
        assert [(l["analysis_type"], l["status"], l["prompt_tokens"]) for l in logs] == [
            ("feedback_metadata_extraction", "success", 8), ("feedback_pii_and_abusive_language_detection", "success", 3)]
        assert "pii_detected" not in meta["extraction"]
    _run(body)


def test_abuse_is_recorded_without_flagging_pii():
    async def body(conn, pool):
        sid = await _session(conn)

        def masking(pairs):
            return json.dumps({k: {"masked_text": "<INSULT>" if v["answer"] == SCHOOL_WORDS else v["answer"],
                                   "pii_found": [], "abusive_language": v["answer"] == SCHOOL_WORDS,
                                   "abusive_spans": [{"severity": "moderate"}] if v["answer"] == SCHOOL_WORDS else []}
                               for k, v in pairs.items()})

        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=masking))
        pii, meta = await _meta(conn, sid)
        masking_meta = meta["extraction"]["masking"]
        assert pii is False and masking_meta["pii_types"] == []
        assert masking_meta["abusive_language"] is True and masking_meta["abusive_severities"] == ["moderate"]
    _run(body)


def test_a_bad_masking_answer_makes_no_extraction_call_writes_nothing_and_logs_one_failed_call():
    async def body(conn, pool):
        sid = await _session(conn)
        fake = Fake(masking=lambda pairs: json.dumps({"2": {"masked_text": "<PERSON>"}}))        # answer 4 missing
        with pytest.raises(ExtractionOutputError):
            await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert len(fake.prompts) == 1
        pii, meta = await _meta(conn, sid)
        assert pii is None and (meta is None or "extraction" not in meta)
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE submission_id = $1", sid) == 0
        logs = await _logs(conn, sid)
        assert [(l["analysis_type"], l["status"]) for l in logs] == [("feedback_pii_and_abusive_language_detection", "failed")]
        assert "ExtractionOutputError" in logs[0]["error_message"] and NAME_WORDS not in logs[0]["error_message"]
    _run(body)


def test_a_failed_extraction_after_a_good_masking_call_logs_both_and_writes_nothing():
    async def body(conn, pool):
        sid = await _session(conn)
        with pytest.raises(ExtractionOutputError):
            await extract_submission(pool, sid, "mitra", llm_function=Fake(fail_extraction=True))
        assert (await _meta(conn, sid))[0] is None
        logs = await _logs(conn, sid)
        assert [(l["analysis_type"], l["status"]) for l in logs] == [
            ("feedback_metadata_extraction", "failed"), ("feedback_pii_and_abusive_language_detection", "success")]
    _run(body)


def test_a_dry_run_calls_both_and_writes_and_logs_nothing():
    async def body(conn, pool):
        sid = await _session(conn)
        fake = Fake()
        await extract_submission(pool, sid, "mitra", llm_function=fake, write=False)
        assert len(fake.prompts) == 2
        assert (await _meta(conn, sid))[0] is None and await _logs(conn, sid) == []
    _run(body)


def test_without_the_key_there_is_no_masking_call_and_a_rerun_drops_the_old_masking_record():
    async def body(conn, pool):
        sid = await _session(conn)
        await extract_submission(pool, sid, "mitra", llm_function=Fake())
        assert "masking" in (await _meta(conn, sid))[1]["extraction"]
        await _set_config(conn, None)
        fake = Fake()
        await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert len(fake.prompts) == 1 and MASKING_MARK not in fake.prompts[0] and NAME_WORDS in fake.prompts[0]
        pii, meta = await _meta(conn, sid)
        assert "masking" not in meta["extraction"] and pii is False
    _run(body)


@pytest.mark.parametrize("name, message", [
    ("No Such Prompt", "No prompt named"),
    ("Theme Classification", "analysis_type"),
    ("Feedback Metadata Extraction", "analysis_type"),            # an extraction prompt is not a masking prompt
])
def test_an_unknown_or_wrong_kind_of_masking_prompt_fails_before_any_call(name, message):
    async def body(conn, pool):
        sid = await _session(conn, masking=name)
        fake = Fake()
        with pytest.raises(ExtractionSetupError, match=message):
            await extract_submission(pool, sid, "mitra", llm_function=fake)
        assert fake.prompts == [] and await _logs(conn, sid) == []
    _run(body)


def test_a_masking_prompt_with_no_active_version_fails_loudly_and_never_falls_back():
    async def body(conn, pool):
        sid = await _session(conn)
        await conn.execute(
            "UPDATE prompt_version SET is_active = FALSE WHERE prompt_id = (SELECT id FROM prompts WHERE name = $1)",
            MASKING_NAME)
        fake = Fake()
        try:
            with pytest.raises(ExtractionSetupError, match="no active prompt version"):
                await extract_submission(pool, sid, "mitra", llm_function=fake)
            assert fake.prompts == []
        finally:
            await conn.execute(
                "UPDATE prompt_version SET is_active = TRUE WHERE prompt_id = (SELECT id FROM prompts WHERE name = $1)",
                MASKING_NAME)
    _run(body)


def test_the_stale_write_check_still_protects_a_masked_run_and_logs_both_calls():
    async def body(conn, pool):
        sid = await _session(conn)
        from app.database.feedback_extraction_operations import StaleExtractionError
        loop = asyncio.get_running_loop()
        fake = Fake()

        async def grow():     # the session grows while the extraction call is running
            await conn.execute(
                "UPDATE feedbacks SET message = message || ' more' WHERE submission_id = $1 AND sequence = 4", sid)

        def calling(prompt, model=None, max_tokens=None, timeout=None):
            result = fake(prompt, model=model, max_tokens=max_tokens, timeout=timeout)
            if MASKING_MARK not in prompt:
                asyncio.run_coroutine_threadsafe(grow(), loop).result(10)
            return result

        with pytest.raises(StaleExtractionError):
            await extract_submission(pool, sid, "mitra", llm_function=calling)
        assert (await _meta(conn, sid))[0] is None
        logs = await _logs(conn, sid)
        assert [(l["analysis_type"], l["status"]) for l in logs] == [
            ("feedback_metadata_extraction", "failed"), ("feedback_pii_and_abusive_language_detection", "success")]
    _run(body)


def _clock(monkeypatch, values):
    """Replaces the activity-budget clock by a script: started, the pre-call clamp, then one reading per call."""
    from app.database import feedback_extraction_operations as ops
    readings = iter(values)
    monkeypatch.setattr(ops, "_monotonic", lambda: next(readings))


def test_the_two_calls_share_one_activity_budget_and_each_gets_what_is_left(monkeypatch):
    async def body(conn, pool):
        sid = await _session(conn)
        _clock(monkeypatch, [0, 0, 0, 50])                  # the extraction call starts 50 s into the budget
        seen = []
        fake = Fake()

        def spy(prompt, model=None, max_tokens=None, timeout=None):
            seen.append(timeout)
            return fake(prompt, model=model, max_tokens=max_tokens, timeout=timeout)

        await extract_submission(pool, sid, "mitra", llm_function=spy, activity_budget_seconds=200,
                                 llm_overrides={"llm_timeout_seconds": 60})
        assert seen == [60, 30]                              # 200 s budget - 120 s margin = 80 s; 80 - 50 = 30
    _run(body)


def test_a_masking_call_that_uses_up_the_budget_makes_the_step_retryable_and_the_good_call_is_still_logged(monkeypatch):
    async def body(conn, pool):
        from app.database.feedback_extraction_operations import ExtractionBudgetExhaustedError
        sid = await _session(conn)
        _clock(monkeypatch, [0, 0, 0, 75])                  # only 5 s are left for the extraction call
        fake = Fake()
        with pytest.raises(ExtractionBudgetExhaustedError):
            await extract_submission(pool, sid, "mitra", llm_function=fake, activity_budget_seconds=200,
                                     llm_overrides={"llm_timeout_seconds": 60})
        assert len(fake.prompts) == 1                        # no extraction call was made
        assert (await _meta(conn, sid))[0] is None
        assert [(l["analysis_type"], l["status"]) for l in await _logs(conn, sid)] == [
            ("feedback_pii_and_abusive_language_detection", "success")]
    _run(body)
