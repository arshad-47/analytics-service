"""
Real-Postgres tests (opt-in like the other *_pg files: FEEDBACK_TEST_DATABASE_URL names a Postgres server) of what is
stored of the two LLM calls of a session: the model's justification per field and per finding, the complete responses,
the copy of an unusable answer in the failed llm log, and the stage list of the one activity. Fake LLM only (no
network). SYNTHETIC data only.

  * feedback_extracted_data.justification is written (and rewritten by a re-run); older databases get the column from
    schema.sql at startup;
  * meta_data.extraction.masking.findings holds type / severity, confidence and justification, never the matched text;
  * meta_data.extraction.responses holds both complete answers of every session, flagged ones included (the data stays in
    our system), and is replaced by every re-run;
  * a call whose answer cannot be used keeps a truncated copy of it in its failed llm_logs row (a good call does not);
  * the outcome carries the stage list (masking, then extraction) and tells a listener after every change.
"""
import json

import pytest

import pg_support
from app.database.feedback_extraction_operations import extract_submission
from app.services.feedback_extraction import ExtractionOutputError
from test_feedback_extraction_tenant_pg import _event
from test_feedback_pii_masking_pg import (
    MASKING_MARK, NAME_WORDS, SCHOOL_WORDS, Fake, _logs, _meta, _run, _session, _set_config)

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(DSN is None or DSN == "", reason=pg_support.SKIP_REASON)


def _clean_masking(pairs):
    """Nothing to mask in any answer: the session is not flagged."""
    return json.dumps({k: {"masked_text": v["answer"], "pii_found": [], "abusive_language": False, "abusive_spans": []}
                       for k, v in pairs.items()})


def _name_with_justification(pairs):
    out = json.loads(_clean_masking(pairs))
    for key, pair in pairs.items():
        if pair["answer"] == NAME_WORDS:
            out[key] = {"masked_text": "<PERSON>", "abusive_language": True,
                        "pii_found": [{"type": "PERSON", "confidence": 0.98, "justification": "A person's full name."}],
                        "abusive_spans": [{"severity": "mild", "confidence": 0.6, "justification": "Casual venting."}]}
    return json.dumps(out)


def _extraction(district_value="Mysuru", justification="They named the district.", pii=False):
    return json.dumps({
        "fields": {"district": {"value": district_value, "raw": "mysore", "confidence": 0.9,
                                "justification": justification}},
        "pii": {"detected": pii, "types": ["name"] if pii else []}})


async def _justifications(conn, sid):
    rows = await conn.fetch(
        "SELECT field_name, value, status, justification FROM feedback_extracted_data WHERE submission_id = $1", sid)
    return {r["field_name"]: (r["value"], r["status"], r["justification"]) for r in rows}


async def _log_meta(conn, sid):
    rows = await conn.fetch(
        "SELECT analysis_type, status, meta_data FROM llm_logs WHERE submission_id = $1 ORDER BY analysis_type", sid)
    return {r["analysis_type"]: (r["status"], json.loads(r["meta_data"]) if isinstance(r["meta_data"], str) else r["meta_data"])
            for r in rows}


def test_the_justification_of_a_field_is_stored_and_a_rerun_rewrites_it():
    async def body(conn, pool):
        sid = await _session(conn)
        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=_clean_masking, extraction=_extraction()))
        assert await _justifications(conn, sid) == {"district": ("Mysuru", "extracted", "They named the district.")}
        await extract_submission(pool, sid, "mitra", llm_function=Fake(
            masking=_clean_masking, extraction=_extraction(None, "The answer is a region, not a district.")))
        assert await _justifications(conn, sid) == {
            "district": ("Invalid response", "unmapped", "The answer is a region, not a district.")}
    _run(body)


def test_an_answer_without_justification_is_stored_as_null():
    async def body(conn, pool):
        sid = await _session(conn)
        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=_clean_masking))   # the default answer has none
        assert (await _justifications(conn, sid))["district"][2] is None
    _run(body)


def test_findings_with_justification_are_stored_without_the_matched_text():
    async def body(conn, pool):
        sid = await _session(conn)
        await extract_submission(pool, sid, "mitra", llm_function=Fake(
            masking=_name_with_justification, extraction=_extraction()))
        pii, meta = await _meta(conn, sid)
        assert pii is True
        masking = meta["extraction"]["masking"]
        assert masking["findings"] == [
            {"answer": "2", "kind": "pii", "type": "person", "confidence": 0.98, "justification": "A person's full name."},
            {"answer": "2", "kind": "abusive_language", "severity": "mild", "confidence": 0.6,
             "justification": "Casual venting."}]
        assert NAME_WORDS not in json.dumps(meta)
    _run(body)


def test_both_complete_responses_are_stored_for_a_session_that_is_not_flagged_and_replaced_by_a_rerun():
    async def body(conn, pool):
        sid = await _session(conn)
        first = _extraction("Mysuru", "First run.")
        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=_clean_masking, extraction=first))
        pii, meta = await _meta(conn, sid)
        assert pii is False
        responses = meta["extraction"]["responses"]
        assert responses["extraction"] == first
        assert json.loads(responses["masking"]) == json.loads(_clean_masking(
            {"2": {"question": "", "answer": NAME_WORDS}, "4": {"question": "", "answer": SCHOOL_WORDS}}))
        second = _extraction("Mysuru", "Second run.")
        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=_clean_masking, extraction=second))
        assert (await _meta(conn, sid))[1]["extraction"]["responses"]["extraction"] == second
    _run(body)


def test_a_flagged_session_keeps_its_responses_and_its_justifications():
    async def body(conn, pool):
        sid = await _session(conn)
        text = _extraction(pii=True)
        await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=text))
        pii, meta = await _meta(conn, sid)
        assert pii is True and meta["extraction"]["responses"]["extraction"] == text      # the data stays in our system
        assert (await _justifications(conn, sid))["district"][2] == "They named the district."
    _run(body)


def test_a_rerun_replaces_the_responses_of_an_earlier_run_flagged_or_not():
    async def body(conn, pool):
        sid = await _session(conn)
        clean = _extraction(justification="clean run")
        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=_clean_masking, extraction=clean))
        assert (await _meta(conn, sid))[1]["extraction"]["responses"]["extraction"] == clean
        flagged = _extraction(justification="flagged run", pii=True)
        await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=flagged))        # names the person
        pii, meta = await _meta(conn, sid)
        assert pii is True and meta["extraction"]["responses"]["extraction"] == flagged
    _run(body)


def test_without_the_masking_step_only_the_extraction_response_is_stored():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        text = _extraction()
        await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=text))
        assert (await _meta(conn, sid))[1]["extraction"]["responses"] == {"extraction": text}
    _run(body)


def test_an_unusable_extraction_answer_is_kept_in_its_failed_log_and_the_good_masking_call_keeps_none():
    async def body(conn, pool):
        sid = await _session(conn)
        with pytest.raises(ExtractionOutputError):
            await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=_clean_masking, fail_extraction=True))
        logs = await _log_meta(conn, sid)
        status, meta = logs["feedback_metadata_extraction"]
        assert status == "failed" and meta["response"] == "not json" and "response_truncated" not in meta
        status, meta = logs["feedback_pii_and_abusive_language_detection"]
        assert status == "success" and "response" not in (meta or {})
        assert (await _meta(conn, sid))[0] is None                                  # nothing else was written
    _run(body)


def test_an_unusable_masking_answer_is_kept_in_its_failed_log_and_a_long_one_is_cut():
    async def body(conn, pool):
        sid = await _session(conn)
        long_answer = "x" * 5000
        with pytest.raises(ExtractionOutputError):
            await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=lambda pairs: long_answer))
        status, meta = (await _log_meta(conn, sid))["feedback_pii_and_abusive_language_detection"]
        assert status == "failed" and meta["response"] == "x" * 4000 and meta["response_truncated"] is True
        assert "feedback_metadata_extraction" not in await _log_meta(conn, sid)
    _run(body)


def test_a_failed_call_with_no_answer_keeps_none():
    async def body(conn, pool):
        sid = await _session(conn)

        def down(prompt, model=None, max_tokens=None, timeout=None):
            raise TimeoutError("timed out")
        with pytest.raises(Exception):
            await extract_submission(pool, sid, "mitra", llm_function=down)
        status, meta = (await _log_meta(conn, sid))["feedback_pii_and_abusive_language_detection"]
        assert status == "failed" and "response" not in (meta or {})
    _run(body)


def test_the_outcome_lists_the_stages_and_a_listener_hears_every_change():
    async def body(conn, pool):
        sid = await _session(conn)
        heard = []
        outcome = await extract_submission(
            pool, sid, "mitra", llm_function=Fake(masking=_clean_masking, extraction=_extraction()),
            on_stage=heard.append)
        names = [(s["name"], s["status"]) for s in outcome.stages]
        assert names == [("pii_masking", "success"), ("metadata_extraction", "success")]
        assert all(s["seconds"] >= 0 and s["prompt_tokens"] > 0 for s in outcome.stages)
        assert outcome.stages[0]["prompt_tokens"] == 3 and outcome.stages[1]["prompt_tokens"] == 8
        assert [[(s["name"], s["status"]) for s in snap] for snap in heard] == [
            [("pii_masking", "running")],
            [("pii_masking", "success")],
            [("pii_masking", "success"), ("metadata_extraction", "running")],
            [("pii_masking", "success"), ("metadata_extraction", "success")]]
        assert "answer" not in json.dumps(heard) and NAME_WORDS not in json.dumps(heard)
    _run(body)


def test_a_failure_carries_the_stage_list_naming_the_stage_that_failed():
    async def body(conn, pool):
        sid = await _session(conn)
        with pytest.raises(ExtractionOutputError) as caught:
            await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=_clean_masking, fail_extraction=True))
        assert [(s["name"], s["status"]) for s in caught.value.stages] == [
            ("pii_masking", "success"), ("metadata_extraction", "failed")]
        with pytest.raises(ExtractionOutputError) as masking_failed:
            await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=lambda pairs: "{"))
        assert [(s["name"], s["status"]) for s in masking_failed.value.stages] == [("pii_masking", "failed")]
    _run(body)


def test_a_session_without_an_answer_makes_no_call_and_both_stages_are_skipped_with_the_reason():
    async def body(conn, pool):
        from app.database.operations import insert_or_update_submission
        event = _event("mitra", "m-noanswer", "x")
        event["data"]["messages"] = [
            {"sequence": 1, "questionKey": "NAME", "isFollowUp": False, "message": "What is your name?",
             "translatedMessage": None, "messageAt": None, "parentSequence": None}]
        await insert_or_update_submission(conn, event)
        await _set_config(conn, "Feedback PII and Abusive-Language Detection")
        fake = Fake()
        outcome = await extract_submission(pool, "m-noanswer", "mitra", llm_function=fake)
        assert fake.prompts == []
        assert outcome.stages == [{"name": "pii_masking", "status": "skipped", "reason": "no_respondent_answer"},
                                  {"name": "metadata_extraction", "status": "skipped", "reason": "no_respondent_answer"}]
        assert "responses" not in (await _meta(conn, "m-noanswer"))[1]["extraction"]
    _run(body)


def test_a_database_created_before_the_justification_column_gets_it_from_schema_sql():
    async def body(conn, pool):
        await conn.execute("ALTER TABLE feedback_extracted_data DROP COLUMN justification")
        await conn.execute(pg_support.schema_sql())
        assert await conn.fetchval(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name = 'feedback_extracted_data' AND column_name = 'justification'") == 1
    _run(body)


def test_the_masking_mark_used_by_the_fake_is_the_prompt_text_of_the_seed():
    """Guards the helper the file shares with the masking tests: the fake recognises the seeded user prompt."""
    assert MASKING_MARK == "Analyse the following answers:"
