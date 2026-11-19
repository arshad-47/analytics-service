"""
Real-Postgres tests (opt-in like the other *_pg files: FEEDBACK_TEST_DATABASE_URL names a Postgres server) of how the
never-empty values are stored: `value` always set, `value_list` with the individual labels (counting categories of a
multi-value field), the `not_applicable` status, the CHECK on status that an older database gets replaced at startup, the
index on value_list, and the stale check that a relabelled questionnaire version triggers. Fake LLM only (no network).
SYNTHETIC data only.
"""
import json

import asyncpg
import pytest

import pg_support
from app.database.feedback_extraction_operations import (
    StaleExtractionError, asked_fields, extract_submission, load_submission_inputs, persona_identity, save_extraction)
from app.services import feedback_extraction as fx
from seed_support import extraction_config_id
from test_feedback_pii_masking_pg import Fake, _meta, _run, _session

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(DSN is None or DSN == "", reason=pg_support.SKIP_REASON)

FIELDS = [
    {"name": "gender", "kind": "category", "description": "Gender.", "categories": ["Male", "Female", "Others", "Prefer not to say"]},
    {"name": "status", "kind": "category", "description": "Status.", "multi": True,
     "categories": ["Studying", "Working", "Others", "Prefer not to say"]},
    {"name": "monthly", "kind": "category", "description": "Monthly.", "categories": ["Low", "High"], "versions": ["V2"]},
    {"name": "annual", "kind": "category", "description": "Annual.", "categories": ["Low", "High"], "versions": ["V1"]},
    {"name": "disability", "kind": "category", "description": "Disability.", "categories": ["Yes", "No"]},
    {"name": "kind", "kind": "category", "description": "Kind.", "multi": True, "categories": ["Physical", "Others"],
     "applies_when": {"field": "disability", "in": ["Yes"]}},
    {"name": "age", "kind": "integer", "description": "Age.", "min": 0, "max": 120,
     "bands": [{"label": "< 16", "min": None, "max": 16}, {"label": "≥ 16", "min": 16, "max": None}],
     "categories": ["Invalid response"]},
]


async def _use_config(conn, sid, version):
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice",
           "prompt_name": "Feedback Metadata Extraction", "fields": FIELDS}
    await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                       extraction_config_id("Youth_Voice"), json.dumps(raw))
    await conn.execute("UPDATE feedback_submissions SET questionnaire_version = $2 WHERE submission_id = $1", sid, version)


def _answer(version, **values):
    names = [f["name"] for f in FIELDS if "versions" not in f or version in f["versions"]]
    entries = {n: {"value": None, "raw": None} for n in names}
    for name, value in values.items():
        entries[name] = value if isinstance(value, dict) else {"value": value, "raw": None}
    return json.dumps({"fields": entries, "pii": {"detected": False, "types": []}})


async def _rows(conn, sid):
    rows = await conn.fetch("SELECT * FROM feedback_extracted_data WHERE submission_id = $1", sid)
    return {r["field_name"]: r for r in rows}


def test_every_row_has_a_value_and_a_value_list_and_a_multi_field_stores_all_its_labels():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _use_config(conn, sid, "V2")
        answer = _answer("V2", gender="Female", status=["Working", "Studying"], monthly="High", disability="Yes",
                         kind=["Physical", "Others"], age={"value": 15.5, "raw": "fifteen and a half"})
        await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=answer))
        rows = await _rows(conn, sid)
        assert set(rows) == {f["name"] for f in FIELDS}
        assert all(r["value"] is not None and len(r["value_list"]) > 0 for r in rows.values())
        assert (rows["status"]["value"], list(rows["status"]["value_list"])) == ("Studying, Working", ["Studying", "Working"])
        assert (rows["kind"]["value"], list(rows["kind"]["value_list"])) == ("Physical, Others", ["Physical", "Others"])
        assert (rows["age"]["value"], rows["age"]["raw_value"], rows["age"]["status"]) == ("< 16", "fifteen and a half", "extracted")
        assert (rows["gender"]["value"], list(rows["gender"]["value_list"])) == ("Female", ["Female"])
    _run(body)


def test_a_field_the_version_does_not_ask_and_one_an_earlier_answer_rules_out_are_na_rows_in_the_table():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _use_config(conn, sid, "V1")
        fake = Fake(extraction=_answer("V1", gender="Male", disability="No", kind=["Physical"], annual="Low"))
        await extract_submission(pool, sid, "mitra", llm_function=fake)
        rows = await _rows(conn, sid)
        assert (rows["monthly"]["value"], rows["monthly"]["status"], list(rows["monthly"]["value_list"])) == ("N/A", "not_applicable", ["N/A"])
        assert " monthly (" not in fake.prompts[0] and " annual (" in fake.prompts[0]      # the model is only asked what V1 asks
        assert (rows["kind"]["value"], rows["kind"]["status"]) == ("N/A", "not_applicable")      # the model invented a kind
        assert (rows["annual"]["value"], rows["annual"]["status"]) == ("Low", "extracted")
        assert (rows["age"]["value"], rows["age"]["status"]) == ("Not answered", "not_provided")
        assert rows["kind"]["justification"] is not None and "disability" in rows["kind"]["justification"]
    _run(body)


def test_categories_of_a_multi_field_are_counted_with_value_list():
    async def body(conn, pool):
        for combination in (["Studying"], ["Studying", "Working"], ["Working"]):
            sid = await _session(conn, masking=None)
            # V1 is the rank label of the persona's only bot: a hand-made V2 would be repaired (the session sent back
            # through extraction) by the next ingest of the persona
            await _use_config(conn, sid, "V1")
            await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=_answer("V1", status=combination)))
        counted = {r["label"]: r["sessions"] for r in await conn.fetch(
            "SELECT label, count(*) AS sessions FROM feedback_extracted_data, unnest(value_list) AS label "
            "WHERE field_name = 'status' GROUP BY label")}
        assert counted == {"Studying": 2, "Working": 2}
        assert await conn.fetchval(
            "SELECT count(*) FROM feedback_extracted_data WHERE field_name = 'status' AND 'Working' = ANY(value_list)") == 2
        assert await conn.fetchval(
            "SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_feedback_extracted_value_list'") == 1
    _run(body)


def test_the_database_refuses_an_unknown_status_and_accepts_not_applicable():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _use_config(conn, sid, "V2")
        await extract_submission(pool, sid, "mitra", llm_function=Fake(extraction=_answer("V2")))
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute("UPDATE feedback_extracted_data SET status = 'whatever' WHERE submission_id = $1", sid)
        await conn.execute("UPDATE feedback_extracted_data SET status = 'not_applicable' WHERE submission_id = $1 AND field_name = 'gender'", sid)
    _run(body)


def test_an_older_database_gets_the_new_status_and_the_value_list_column_at_startup():
    async def body(conn, pool):
        await conn.execute("DELETE FROM feedback_extracted_data")
        await conn.execute("ALTER TABLE feedback_extracted_data DROP COLUMN value_list")
        await conn.execute("DROP INDEX IF EXISTS idx_feedback_extracted_value_list")
        await conn.execute("ALTER TABLE feedback_extracted_data DROP CONSTRAINT feedback_extracted_data_status_check")
        await conn.execute("ALTER TABLE feedback_extracted_data ADD CONSTRAINT feedback_extracted_data_status_check "
                           "CHECK (status IN ('extracted', 'not_provided', 'unmapped'))")
        for _ in range(2):                                   # the upgrade runs at every start: a second run changes nothing
            await conn.execute(pg_support.schema_sql())
        assert await conn.fetchval(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'feedback_extracted_data' AND column_name = 'value_list'") == 1
        definition = await conn.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'feedback_extracted_data'::regclass "
            "AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%not_provided%'")
        assert "not_applicable" in definition
        assert await conn.fetchval("SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_feedback_extracted_value_list'") == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM pg_constraint WHERE conrelid = 'feedback_extracted_data'::regclass AND contype = 'c' "
            "AND pg_get_constraintdef(oid) LIKE '%not_provided%'") == 1                       # one CHECK on status, not two
    _run(body)


def test_a_relabelled_questionnaire_version_during_the_call_makes_the_write_stale_and_writes_nothing():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _use_config(conn, sid, "V1")
        async with pool.acquire() as c:
            inputs = await load_submission_inputs(c, sid, "mitra")
        assert inputs["identity"][3] == asked_fields(inputs["config_row"]["config"], "V1")     # the fields V1 asks
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V2' WHERE submission_id = $1", sid)
        config = inputs["config_row"]["config"]
        async with pool.acquire() as c:
            with pytest.raises(StaleExtractionError, match="questionnaire version"):
                await save_extraction(
                    c, submission_id=sid, tenant_code="mitra", input_fingerprint=inputs["fingerprint"],
                    result=fx.empty_result(config), config_version_id=inputs["config_row"]["id"],
                    prompt_version_id=inputs["config_row"]["id"], run_at=inputs["loaded_at"],
                    input_identity=inputs["identity"], config=config)
        assert await _rows(conn, sid) == {}
        assert (await _meta(conn, sid))[0] is None
    _run(body)


def test_a_relabel_that_asks_the_same_fields_does_not_make_the_write_stale():
    """General Public asks the same five fields in every version: a relabel (a lower bot id arrived) must not discard a finished call."""
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "Feedback Metadata Extraction",
               "fields": [f for f in FIELDS if "versions" not in f and "applies_when" not in f]}
        await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                           extraction_config_id("Youth_Voice"), json.dumps(raw))
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V1' WHERE submission_id = $1", sid)
        async with pool.acquire() as c:
            inputs = await load_submission_inputs(c, sid, "mitra")
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V2' WHERE submission_id = $1", sid)
        config = inputs["config_row"]["config"]
        async with pool.acquire() as c:
            await save_extraction(
                c, submission_id=sid, tenant_code="mitra", input_fingerprint=inputs["fingerprint"],
                result=fx.empty_result(config), config_version_id=inputs["config_row"]["id"],
                prompt_version_id=await conn.fetchval("SELECT id FROM prompt_version LIMIT 1"), run_at=inputs["loaded_at"],
                input_identity=inputs["identity"], config=config)
        assert len(await _rows(conn, sid)) == len(config.fields)                  # written, not discarded
    _run(body)


def test_the_identity_of_a_session_includes_the_fields_its_questionnaire_version_asks():
    assert persona_identity("Youth_Voice", "mitra", "P1", ("a", "b")) != persona_identity("Youth_Voice", "mitra", "P1", ("a",))
    assert persona_identity("Youth_Voice", "mitra", "P1", ("a",)) == persona_identity("Youth_Voice", "mitra", "P1", ("a",))
    assert persona_identity("Youth_Voice", "mitra", "P1") == ("Youth_Voice", "mitra", "P1", ())


def test_a_config_version_activated_while_the_call_ran_makes_the_write_stale_and_writes_nothing():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _use_config(conn, sid, "V1")
        async with pool.acquire() as c:
            inputs = await load_submission_inputs(c, sid, "mitra")
        config = inputs["config_row"]["config"]
        cid = extraction_config_id("Youth_Voice")
        # an operator activates the next version of the config (the same fields) while the LLM call is running
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", cid)
        await conn.execute(
            "INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
            "SELECT config_id, 2, TRUE, config, 'ops', 'next' FROM config_version WHERE config_id = $1::uuid AND version = 1", cid)
        async with pool.acquire() as c:
            with pytest.raises(StaleExtractionError, match="config in force changed"):
                await save_extraction(
                    c, submission_id=sid, tenant_code="mitra", input_fingerprint=inputs["fingerprint"],
                    result=fx.empty_result(config), config_version_id=inputs["config_row"]["id"],
                    prompt_version_id=inputs["config_row"]["id"], run_at=inputs["loaded_at"],
                    input_identity=inputs["identity"], config=config)
        assert await _rows(conn, sid) == {}
        assert (await _meta(conn, sid))[0] is None
    _run(body)
