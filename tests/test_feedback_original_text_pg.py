"""
Real-Postgres tests (opt-in like the other *_pg files: FEEDBACK_TEST_DATABASE_URL names a Postgres server) of what is
stored when the extraction reads the ORIGINAL text (`pii.extraction_text`, the default): raw_value is the respondent's own
words, while the masked copy in meta_data (the only part that may be shared) holds none of them; the abusive_language
column; a justification that quotes is scrubbed before it is stored; a session skipped for size. Fake LLM only.
SYNTHETIC data only.
"""
import json

import pytest

import pg_support
from app.database.feedback_extraction_operations import extract_submission
from seed_support import extraction_config_id
from test_feedback_pii_masking_pg import (
    MASKING_MARK, MASKING_NAME, NAME_WORDS, SCHOOL_WORDS, Fake, _mask_names, _meta, _run, _session)

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(DSN is None or DSN == "", reason=pg_support.SKIP_REASON)


async def _config(conn, masking=MASKING_NAME, extraction_text=None):
    """One district field; `extraction_text` None keeps the default ('original')."""
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "Feedback Metadata Extraction",
           "fields": [{"name": "district", "kind": "free_text", "description": "District."}], "pii": {}}
    if masking is not None:
        raw["pii"]["masking_prompt_name"] = masking
    if extraction_text is not None:
        raw["pii"]["extraction_text"] = extraction_text
    await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                       extraction_config_id("Youth_Voice"), json.dumps(raw))


def _extraction(raw=NAME_WORDS, why="named"):
    return json.dumps({"fields": {"district": {"value": "Mysuru", "raw": raw, "confidence": 1, "justification": why}},
                       "pii": {"detected": False, "types": []}})


async def _rows(conn, sid):
    rows = await conn.fetch("SELECT * FROM feedback_extracted_data WHERE submission_id = $1", sid)
    return {r["field_name"]: r for r in rows}


def test_raw_value_is_the_original_and_the_shared_copy_holds_none_of_the_original_words():
    async def body(conn, pool):
        sid = await _session(conn)
        await _config(conn)
        fake = Fake(masking=_mask_names, extraction=_extraction())
        await extract_submission(pool, sid, "mitra", llm_function=fake)
        extraction_prompt = [p for p in fake.prompts if MASKING_MARK not in p][0]
        masking_prompt = [p for p in fake.prompts if MASKING_MARK in p][0]
        assert NAME_WORDS in extraction_prompt and SCHOOL_WORDS in extraction_prompt          # the extraction reads the original
        assert NAME_WORDS in masking_prompt                                                   # and the masking model, as before
        assert (await _rows(conn, sid))["district"]["raw_value"] == NAME_WORDS                # the respondent's own words
        pii, meta = await _meta(conn, sid)
        assert pii is True                                                                    # the masking call flagged the name
        shared = json.dumps(meta["extraction"]["masking"], ensure_ascii=False)
        assert "<PERSON>" in shared and NAME_WORDS not in shared                              # the copy that may be shared is masked
        assert meta["extraction"]["responses"]["extraction"] == _extraction()                 # internal: the raw answer of the call
        logs = json.dumps([dict(r) for r in await conn.fetch("SELECT error_message, meta_data FROM llm_logs WHERE submission_id = $1", sid)], default=str)
        assert NAME_WORDS not in logs
    _run(body)


def test_extraction_text_masked_keeps_the_old_behaviour_end_to_end():
    async def body(conn, pool):
        sid = await _session(conn)
        await _config(conn, extraction_text="masked")
        fake = Fake(masking=_mask_names, extraction=_extraction(raw="<PERSON>"))
        await extract_submission(pool, sid, "mitra", llm_function=fake)
        extraction_prompt = [p for p in fake.prompts if MASKING_MARK not in p][0]
        assert NAME_WORDS not in extraction_prompt and "<PERSON>" in extraction_prompt
        assert (await _rows(conn, sid))["district"]["raw_value"] == "<PERSON>"
    _run(body)


def test_the_abusive_language_column_follows_the_masking_call_and_is_null_without_it():
    async def body(conn, pool):
        sid = await _session(conn)
        await _config(conn)

        def abusive(pairs):
            return json.dumps({k: {"masked_text": v["answer"], "pii_found": [], "abusive_language": True,
                                   "abusive_spans": [{"severity": "mild", "justification": "casual profanity"}]}
                               for k, v in pairs.items()})
        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=abusive, extraction=_extraction()))
        assert await conn.fetchval("SELECT abusive_language FROM feedback_submissions WHERE submission_id = $1", sid) is True
        assert (await _meta(conn, sid))[0] is False                                           # abuse is not a PII flag
        await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=_extraction()))   # clean answers
        assert await conn.fetchval("SELECT abusive_language FROM feedback_submissions WHERE submission_id = $1", sid) is False
        await _config(conn, masking=None)                                                     # no masking step: nothing known
        await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=_extraction()))
        assert await conn.fetchval("SELECT abusive_language FROM feedback_submissions WHERE submission_id = $1", sid) is None
    _run(body)


def test_an_older_database_gets_the_abusive_language_column_at_startup():
    async def body(conn, pool):
        await conn.execute("ALTER TABLE feedback_submissions DROP COLUMN abusive_language")
        for _ in range(2):                                      # runs at every start: a second run changes nothing
            await conn.execute(pg_support.schema_sql())
        assert await conn.fetchval("SELECT count(*) FROM information_schema.columns "
                                   "WHERE table_name = 'feedback_submissions' AND column_name = 'abusive_language'") == 1
    _run(body)


def test_a_justification_that_quotes_the_masked_out_words_is_not_stored_in_the_shared_copy():
    async def body(conn, pool):
        sid = await _session(conn)
        await _config(conn)

        def quoting(pairs):
            out = json.loads(_mask_names(pairs))
            for key, pair in pairs.items():
                if pair["answer"] == NAME_WORDS:
                    out[key]["pii_found"] = [{"type": "PERSON", "confidence": 0.9, "justification": f"{NAME_WORDS} is a name"}]
            return json.dumps(out)
        await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=quoting, extraction=_extraction()))
        _, meta = await _meta(conn, sid)
        findings = meta["extraction"]["masking"]["findings"]
        assert len(findings) == 1 and findings[0]["type"] == "person" and "justification" not in findings[0]
        assert NAME_WORDS not in json.dumps(meta["extraction"]["masking"])
    _run(body)


def test_a_session_skipped_for_size_is_not_answered_everywhere_and_makes_no_call():
    async def body(conn, pool):
        from app.database.operations import insert_or_update_submission
        from test_feedback_extraction_tenant_pg import _event
        event = _event("mitra", "m-big", "x")
        event["data"]["messages"] = [
            {"sequence": 1, "questionKey": "Q", "isFollowUp": False, "message": "Your district?", "translatedMessage": None,
             "messageAt": None, "parentSequence": None},
            {"sequence": 2, "questionKey": "Q", "isFollowUp": False, "message": "word " * 5000, "translatedMessage": None,
             "messageAt": None, "parentSequence": 1}]
        await insert_or_update_submission(conn, event)
        await _config(conn)
        fake = Fake()
        outcome = await extract_submission(pool, "m-big", "mitra", llm_function=fake)
        assert fake.prompts == [] and outcome.run.llm_called is False
        row = (await _rows(conn, "m-big"))["district"]
        assert (row["value"], row["status"], list(row["value_list"])) == ("Not answered", "unmapped", ["Not answered"])
        assert "size limit" in row["justification"]
        assert (await _meta(conn, "m-big"))[0] is True
    _run(body)
