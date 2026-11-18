"""
Real-Postgres tests (opt-in like the other *_pg files) of `copy_to_submission`: the validated state / district of the
extraction are written to submissions.state / district in the SAME transaction as the extraction rows, cleared when a
later extraction is not valid, and cleared when the session is reset (persona change, questionnaire relabel).
A config without the key leaves the columns alone. Fake LLM only. SYNTHETIC data only.
"""
import json

import pytest

import pg_support
from app.database.feedback_extraction_operations import extract_submission
from seed_support import extraction_config_id
from test_feedback_pii_masking_pg import MASKING_NAME, Fake, _mask_names, _run, _session

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(DSN is None or DSN == "", reason=pg_support.SKIP_REASON)

SPECIAL = ["Prefer not to say", "Unmapped", "Invalid response", "Outside India"]


async def _config(conn, copy=True, masking=False):
    raw = {"schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "Feedback Metadata Extraction",
           "fields": [{"name": "state", "kind": "category", "description": "State.",
                       "categories": ["Karnataka", "Delhi", *SPECIAL]},
                      {"name": "district", "kind": "free_text", "description": "District.", "categories": SPECIAL}],
           "pii": {"masking_prompt_name": MASKING_NAME} if masking else {}}
    if copy:
        raw["copy_to_submission"] = {"state": "state", "district": "district"}
    await conn.execute("UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid",
                       extraction_config_id("Youth_Voice"), json.dumps(raw))


def _answer(state, district):
    return json.dumps({"fields": {"state": {"value": state, "raw": state, "confidence": 1, "justification": "said"},
                                  "district": {"value": district, "raw": district, "confidence": 1, "justification": "said"}},
                       "pii": {"detected": False, "types": []}})


async def _geo(conn, sid):
    row = await conn.fetchrow("SELECT state, district FROM submissions WHERE submission_id = $1", sid)
    return row["state"], row["district"]


async def _extract(pool, sid, state, district, masking=None):
    return await extract_submission(pool, sid, "mitra", llm_function=Fake(masking=masking, extraction=_answer(state, district)))


def test_a_valid_state_and_district_are_copied_with_the_extraction():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _config(conn)
        assert await _geo(conn, sid) == (None, None)
        await _extract(pool, sid, "Karnataka", "Mysuru")
        assert await _geo(conn, sid) == ("Karnataka", "Mysuru")
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE submission_id = $1", sid) == 2
    _run(body)


@pytest.mark.parametrize("state, district, expected", [
    ("Prefer not to say", "Mysuru", (None, None)),            # no valid state: no district either
    ("Karnataka", "Unmapped", ("Karnataka", None)),
    ("Karnataka", "Outside India", ("Karnataka", None)),
    ("Outside India", "Outside India", (None, None)),
    ("Karnataka", "Prefer not to say", ("Karnataka", None)),
])
def test_only_a_valid_place_is_written_and_special_answers_leave_null(state, district, expected):
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _config(conn)
        await _extract(pool, sid, state, district)
        assert await _geo(conn, sid) == expected
    _run(body)


def test_a_later_extraction_that_is_not_valid_clears_what_an_earlier_one_wrote():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await _config(conn)
        await _extract(pool, sid, "Karnataka", "Mysuru")
        await _extract(pool, sid, "Delhi", "Unmapped")
        assert await _geo(conn, sid) == ("Delhi", None)
        await _extract(pool, sid, "Unmapped", "Mysuru")
        assert await _geo(conn, sid) == (None, None)
    _run(body)


def test_a_session_flagged_for_pii_keeps_its_state_and_district():
    async def body(conn, pool):
        sid = await _session(conn)                                         # the answers hold a name: flagged
        await _config(conn, masking=True)
        await _extract(pool, sid, "Karnataka", "Mysuru", masking=_mask_names)
        assert await conn.fetchval("SELECT pii_detected FROM feedback_submissions WHERE submission_id = $1", sid) is True
        assert await _geo(conn, sid) == ("Karnataka", "Mysuru")
    _run(body)


def test_a_config_without_the_key_leaves_the_columns_untouched():
    async def body(conn, pool):
        sid = await _session(conn, masking=None)
        await conn.execute("UPDATE submissions SET state = 'Kerala', district = 'Kochi' WHERE submission_id = $1", sid)
        await _config(conn, copy=False)
        await _extract(pool, sid, "Karnataka", "Mysuru")
        assert await _geo(conn, sid) == ("Kerala", "Kochi")
    _run(body)


def _relabel_helpers():
    import test_feedback_relabel_pg as r
    return r


def test_a_relabel_that_resets_the_session_clears_the_copied_place():
    r = _relabel_helpers()

    async def body(conn, pool):
        program = r._seeded("Youth")
        y1, _ = await r._ingest(conn, program, "103", r.YOUTH)
        y2, _ = await r._ingest(conn, program, "103", r.YOUTH)
        for sid in (y1, y2):
            await r._pretend_extracted(conn, sid)
            await conn.execute("UPDATE submissions SET state = 'Karnataka', district = 'Mysuru' WHERE submission_id = $1", sid)
        new, res = await r._ingest(conn, program, "89", r.YOUTH)         # the asked fields change: both are reset
        assert sorted(res["reset_submission_ids"]) == sorted([y1, y2])
        for sid in (y1, y2):
            assert await _geo(conn, sid) == (None, None)
    r._run(body)


def test_a_label_only_relabel_keeps_the_copied_place_with_the_kept_extraction():
    r = _relabel_helpers()

    async def body(conn, pool):
        program = r._seeded("General Public")
        g1, _ = await r._ingest(conn, program, "103", r.GENERAL)
        await r._pretend_extracted(conn, g1)
        await conn.execute("UPDATE submissions SET state = 'Karnataka', district = 'Mysuru' WHERE submission_id = $1", g1)
        _, res = await r._ingest(conn, program, "89", r.GENERAL)         # V1 -> V2 but the same fields: nothing to redo
        assert res["reset_submission_ids"] == []
        assert await _geo(conn, g1) == ("Karnataka", "Mysuru")
    r._run(body)


def test_a_persona_change_of_an_existing_session_clears_the_copied_place():
    r = _relabel_helpers()
    import factories

    async def body(conn, pool):
        from app.database.operations import insert_or_update_submission
        program = r._seeded("Youth")
        event = factories.conversation_event(program, r.YOUTH, "mitra", "V1", bot_id="103")
        sid = event["submissionId"]
        await insert_or_update_submission(conn, event)
        await r._pretend_extracted(conn, sid)
        await conn.execute("UPDATE submissions SET state = 'Karnataka', district = 'Mysuru' WHERE submission_id = $1", sid)
        moved = json.loads(json.dumps(event))
        moved["data"]["sessionType"] = "Parent_Voice"                    # redelivered as another persona
        await insert_or_update_submission(conn, moved)
        assert await conn.fetchval("SELECT count(*) FROM feedback_extracted_data WHERE submission_id = $1", sid) == 0
        assert await _geo(conn, sid) == (None, None)
    r._run(body)
