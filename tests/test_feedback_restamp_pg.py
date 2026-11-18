"""
Real-Postgres tests for restamp_questionnaire_versions (app/database/feedback_operations.py).

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards) (the restamp touches every stored Mitra session, and the tests wipe the feedback rows and the
configs before each case). The file applies schema.sql itself (the way app/database/db.py does it), creates the
tenant 'mitra' and runs the real seed. The seed holds only the V1 baseline for every persona (the V2 windows are
data, not code), so each case first records the windows it needs with set_questionnaire_version (restamp off), the
way an operator does: Youth and Parent get a V2, Educator and General Public stay V1 only.
Sessions are stored through the real ingest path (insert_or_update_submission) with a deliberately chosen stored
version, so every case starts from "the stored label disagrees with the session's date" or "agrees with it".
Synthetic data only.
"""
import asyncio
import json
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest
import factories
import pg_support

from app.database.db import db
from app.database.feedback_operations import restamp_questionnaire_versions, set_questionnaire_version
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, mitra_config_v1, seed_config

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH, PARENT, EDUCATOR = PERSONAS["Youth"], PERSONAS["Parent"], PERSONAS["Educator"]

# The V2 windows recorded by _run, with the +05:30 offset an operator would type, and the same instants in UTC.
YOUTH_V2_TEXT = "2026-11-03T18:23:03+05:30"
PARENT_V2_TEXT = "2026-11-03T18:52:54+05:30"
YOUTH_V2 = datetime(2026, 11, 3, 12, 53, 3, tzinfo=timezone.utc)
PARENT_V2 = datetime(2026, 11, 3, 13, 22, 54, tzinfo=timezone.utc)


def _run(body, windows=True):
    """
    Runs `body(conn)` on a scratch database with the schema applied, tenant 'mitra' present and the seed run on a
    clean slate (no sessions, no configs left over from an earlier test), with db.pool pointing at that database
    so the operations under test use it. With `windows` (the default) the Youth and Parent V2 windows are
    recorded first (restamp off, so no session exists yet to be touched). Wipes the sessions again afterwards.
    """
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await _wipe(conn)
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await seed_config(conn)
            pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
            db.pool = pool
            if windows:
                await _record_windows()
            return await body(conn)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await _wipe(conn)
            await conn.close()
    return asyncio.run(main())


async def _record_windows():
    for session_type, text in (("Youth_Voice", YOUTH_V2_TEXT), ("Parent_Voice", PARENT_V2_TEXT)):
        res = await set_questionnaire_version(session_type, "V2", text, created_by="test", restamp=False)
        assert res["changed"] is True and res["config_version"] == 2


async def _wipe(conn):
    await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")  # cascades to the feedback tables


def _messages():
    return [
        {"sequence": 1, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "How old are you?",
         "translatedMessage": None, "messageAt": None, "parentSequence": None},
        {"sequence": 2, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "17",
         "translatedMessage": None, "messageAt": None, "parentSequence": 1},
    ]


async def _add(conn, persona, session_type, stored_version, started_at, source="mitra"):
    """Stores one session through the real ingest path; returns its submission id."""
    sid = f"r{uuid.uuid4().hex[:14]}"
    event = {
        "submissionId": sid if source == "mitra" else str(uuid.uuid4()), "submissionType": "feedback",
        "tenantCode": "mitra", "eventType": "create", "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": PERSONAS[persona]["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": PERSONAS[persona]["leader_category_id"], "leaderCategoryName": persona,
                 "state": None, "district": None, "source": source},
        "data": {"source": source, "adapter": "wide_form" if source == "google_forms" else "conversation_rows", "sessionType": session_type, "questionnaireVersion": stored_version,
                 "language": "en", "completionStatus": "COMPLETED",
                 "submissionDate": started_at.isoformat(), "messages": _messages()},
    }
    event["sessionId"] = event["submissionId"] if source == "google_forms" else f"sess-{uuid.uuid4().hex}"
    if source == "google_forms":
        event["data"]["sourceRef"] = f"{started_at:%Y-%m-%dT%H:%M:%S}:{uuid.uuid4().hex}{uuid.uuid4().hex}"
    else:
        event["data"]["botId"] = PERSONAS[persona]["bot_id"]
    await insert_or_update_submission(conn, event)
    if source != "google_forms":
        # these tests are about the date-window labels, which only apply to sessions without a bot id
        await factories.as_window_stamped_session(conn, sid, stored_version)
    return event["submissionId"]


async def _versions(conn):
    rows = await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")
    return {r["submission_id"]: r["questionnaire_version"] for r in rows}


async def _stamps(conn):
    rows = await conn.fetch("SELECT submission_id, questionnaire_version, updated_at FROM feedback_submissions")
    return {r["submission_id"]: (r["questionnaire_version"], r["updated_at"]) for r in rows}


def _counts(entry):
    """The counts of one restamp result entry (the entry also names its tenant / program / session_type / source)."""
    assert entry["tenant_code"] == "mitra" and entry["source"] == "mitra" and entry["program_id"] is not None
    return {k: entry[k] for k in ("sessions", "changed", "versions")}


def _at(*args):
    return datetime(*args, tzinfo=timezone.utc)


async def _mixed(conn):
    """A mix of right and wrong stored versions around the V2 dates; returns {name: submission id}."""
    ids = {
        # Youth: V2 starts at YOUTH_V2 (inclusive)
        "y_before_ok": await _add(conn, "Youth", "Youth_Voice", "V1", _at(2026, 11, 3, 12, 53, 2)),
        "y_boundary_wrong": await _add(conn, "Youth", "Youth_Voice", "V1", YOUTH_V2),
        "y_after_ok": await _add(conn, "Youth", "Youth_Voice", "V2", _at(2026, 11, 5, 9, 0, 0)),
        "y_early_wrong": await _add(conn, "Youth", "Youth_Voice", "V2", _at(2026, 10, 20, 9, 0, 0)),
        # Parent: V2 starts at PARENT_V2
        "p_before_wrong": await _add(conn, "Parent", "Parent_Voice", "V2", _at(2026, 11, 3, 13, 22, 53)),
        "p_after_wrong": await _add(conn, "Parent", "Parent_Voice", "V1", _at(2026, 11, 4, 8, 0, 0)),
        "p_after_ok": await _add(conn, "Parent", "Parent_Voice", "V2", PARENT_V2),
        # Educator: a single questionnaire, so everything is V1
        "e_ok": await _add(conn, "Educator", "Educator_Voice", "V1", _at(2026, 11, 8, 8, 0, 0)),
        "e_wrong": await _add(conn, "Educator", "Educator_Voice", "V2", _at(2026, 11, 9, 8, 0, 0)),
        # Google Forms: the version comes from the form, never from the date
        "g_youth": await _add(conn, "Youth", "Youth_Voice", "V1", _at(2026, 11, 10, 8, 0, 0), source="google_forms"),
        "g_parent": await _add(conn, "Parent", "Parent_Voice", "V1", _at(2026, 11, 10, 9, 0, 0), source="google_forms"),
    }
    return ids


EXPECTED_AFTER = {
    "y_before_ok": "V1", "y_boundary_wrong": "V2", "y_after_ok": "V2", "y_early_wrong": "V1",
    "p_before_wrong": "V1", "p_after_wrong": "V2", "p_after_ok": "V2",
    "e_ok": "V1", "e_wrong": "V1",
    "g_youth": "V1", "g_parent": "V1",
}
WRONG = {"y_boundary_wrong", "y_early_wrong", "p_before_wrong", "p_after_wrong", "e_wrong"}


def test_seed_holds_only_the_baseline_and_the_recorded_windows_match_the_constants():
    async def body(conn):
        # straight after the seed: config_version 1, only V1, for every persona (no dates anywhere)
        rows = await conn.fetch(
            "SELECT c.session_type, v.version, v.config FROM config c JOIN config_version v ON v.config_id = c.id "
            "WHERE c.config_source = 'mitra' AND v.is_active")
        assert len(rows) == 4
        for r in rows:
            assert r["version"] == 1
            assert json.loads(r["config"])["questionnaire_versions"] == [{"key": "V1", "effective_from": None}]
        # what _run records for the other tests parses to the UTC constants used in this file
        from app.services.feedback_versions import parse_versions
        await _record_windows()
        for st, expected in (("Youth_Voice", YOUTH_V2), ("Parent_Voice", PARENT_V2)):
            cfg = json.loads(await conn.fetchval(
                "SELECT v.config FROM config c JOIN config_version v ON v.config_id = c.id AND v.is_active "
                "WHERE c.config_source = 'mitra' AND c.session_type = $1", st))
            assert parse_versions(cfg)[1][1] == expected
    _run(body, windows=False)


def test_dry_run_reports_the_counts_and_changes_nothing():
    async def body(conn):
        ids = await _mixed(conn)
        before = await _stamps(conn)
        res = await restamp_questionnaire_versions(dry_run=True)
        assert res["dry_run"] is True and res["changed"] == len(WRONG)
        assert {k: _counts(v) for k, v in res["session_types"].items()} == {
            "Youth_Voice": {"sessions": 4, "changed": 2, "versions": {"V1": 2, "V2": 2}},
            "Parent_Voice": {"sessions": 3, "changed": 2, "versions": {"V1": 1, "V2": 2}},
            "Educator_Voice": {"sessions": 2, "changed": 1, "versions": {"V1": 2}},
            "General_Public_Voice": {"sessions": 0, "changed": 0, "versions": {}},  # configured, no session
        }
        assert await _stamps(conn) == before  # labels and updated_at untouched
        assert (await _versions(conn))[ids["y_early_wrong"]] == "V2"
    _run(body)


def test_real_run_relabels_exactly_the_sessions_whose_window_differs():
    async def body(conn):
        ids = await _mixed(conn)
        before = await _stamps(conn)
        res = await restamp_questionnaire_versions()
        assert res["dry_run"] is False and res["changed"] == len(WRONG)
        assert {st: v["changed"] for st, v in res["session_types"].items()} == {
            "Youth_Voice": 2, "Parent_Voice": 2, "Educator_Voice": 1, "General_Public_Voice": 0}
        after = await _stamps(conn)
        assert {name: after[sid][0] for name, sid in ids.items()} == EXPECTED_AFTER
        for name, sid in ids.items():
            if name in WRONG:
                assert after[sid][1] > before[sid][1], name  # updated_at moved
            else:
                assert after[sid] == before[sid], name        # correct sessions and Google Forms rows untouched
    _run(body)


def test_google_forms_rows_are_never_touched_even_when_their_date_says_otherwise():
    async def body(conn):
        # the form is V1 although its response is dated after the Youth / Parent V2 start and the Educator has no V2
        ids = await _mixed(conn)
        await conn.execute("UPDATE feedback_submissions SET questionnaire_version = 'V7' WHERE source = 'google_forms'")
        before = await _stamps(conn)
        await restamp_questionnaire_versions()
        after = await _stamps(conn)
        for name in ("g_youth", "g_parent"):
            assert after[ids[name]] == before[ids[name]] and after[ids[name]][0] == "V7"
    _run(body)


def test_session_type_filter_limits_the_run():
    async def body(conn):
        ids = await _mixed(conn)
        res = await restamp_questionnaire_versions(session_type="Parent_Voice")
        assert list(res["session_types"]) == ["Parent_Voice"] and res["changed"] == 2
        versions = await _versions(conn)
        assert versions[ids["p_before_wrong"]] == "V1" and versions[ids["p_after_wrong"]] == "V2"
        assert versions[ids["y_early_wrong"]] == "V2" and versions[ids["y_boundary_wrong"]] == "V1"  # still wrong
        assert versions[ids["e_wrong"]] == "V2"

        res = await restamp_questionnaire_versions(session_type="Youth_Voice", dry_run=True)
        assert list(res["session_types"]) == ["Youth_Voice"] and res["changed"] == 2
        assert (await _versions(conn))[ids["y_early_wrong"]] == "V2"
    _run(body)


def test_unknown_session_type_matches_nothing():
    async def body(conn):
        ids = await _mixed(conn)
        before = await _stamps(conn)
        res = await restamp_questionnaire_versions(session_type="Nobody_Voice")
        assert res == {"session_types": {}, "changed": 0, "dry_run": False}
        assert await _stamps(conn) == before
        assert len(ids) == 11
    _run(body)


def test_second_run_is_idempotent():
    async def body(conn):
        await _mixed(conn)
        first = await restamp_questionnaire_versions()
        assert first["changed"] == len(WRONG)
        stamps = await _stamps(conn)
        second = await restamp_questionnaire_versions()
        assert second["changed"] == 0
        assert all(v["changed"] == 0 for v in second["session_types"].values())
        assert {st: v["sessions"] for st, v in second["session_types"].items()} == {
            st: v["sessions"] for st, v in first["session_types"].items()}
        assert await _stamps(conn) == stamps  # not even updated_at moves
    _run(body)


def test_no_sessions_gives_zero_counts_per_configured_persona():
    async def body(conn):
        res = await restamp_questionnaire_versions()
        assert res["changed"] == 0
        assert set(res["session_types"]) == {"Youth_Voice", "Parent_Voice", "General_Public_Voice", "Educator_Voice"}
        assert all(_counts(v) == {"sessions": 0, "changed": 0, "versions": {}} for v in res["session_types"].values())
    _run(body)


def test_session_without_a_start_date_is_skipped():
    async def body(conn):
        sid = await _add(conn, "Youth", "Youth_Voice", "V2", _at(2026, 10, 1, 0, 0, 0))
        await conn.execute("UPDATE submissions SET submission_date = NULL WHERE submission_id = $1", sid)
        res = await restamp_questionnaire_versions()
        assert _counts(res["session_types"]["Youth_Voice"]) == {"sessions": 0, "changed": 0, "versions": {}}
        assert (await _versions(conn))[sid] == "V2"
    _run(body)


def test_session_type_without_an_active_config_is_left_alone():
    async def body(conn):
        ids = await _mixed(conn)
        await conn.execute(
            "UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", EDUCATOR["config_id"])
        res = await restamp_questionnaire_versions()
        assert "Educator_Voice" not in res["session_types"] and res["changed"] == 4
        assert (await _versions(conn))[ids["e_wrong"]] == "V2"
    _run(body)


def test_a_new_active_config_version_with_moved_dates_drives_the_relabel():
    async def body(conn):
        ids = await _mixed(conn)
        await restamp_questionnaire_versions()
        # version 2 of the Youth config moves the V2 start to 8 Nov: sessions between 3 and 8 Nov become V1
        cfg = _youth_cfg()
        cfg["questionnaire_versions"][1]["effective_from"] = "2026-11-08T00:00:00+05:30"
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH["config_id"])
        await conn.execute(
            "INSERT INTO config_version (config_id, version, is_active, config) "
            "SELECT $1::uuid, max(version) + 1, TRUE, $2::jsonb FROM config_version WHERE config_id = $1::uuid",
            YOUTH["config_id"], json.dumps(cfg))
        res = await restamp_questionnaire_versions(session_type="Youth_Voice")
        assert res["changed"] == 2 and res["session_types"]["Youth_Voice"]["versions"] == {"V1": 4}
        versions = await _versions(conn)
        assert versions[ids["y_boundary_wrong"]] == "V1" and versions[ids["y_after_ok"]] == "V1"
    _run(body)


def _youth_cfg():
    """The Youth config as _run leaves it: the baseline plus the recorded V2."""
    cfg = mitra_config_v1("Youth")
    cfg["questionnaire_versions"].append({"key": "V2", "effective_from": YOUTH_V2_TEXT})
    return cfg


def _bad(mutate):
    cfg = _youth_cfg()
    mutate(cfg)
    return cfg


INVALID_CONFIGS = {
    "missing": lambda c: c.pop("questionnaire_versions"),
    "not_a_list": lambda c: c.update(questionnaire_versions={"V1": None}),
    "empty_list": lambda c: c.update(questionnaire_versions=[]),
    "blank_key": lambda c: c["questionnaire_versions"].__setitem__(0, {"key": " ", "effective_from": None}),
    "duplicate_key": lambda c: c["questionnaire_versions"][1].update(key="V1"),
    "first_has_date": lambda c: c["questionnaire_versions"][0].update(effective_from="2026-01-01T00:00:00+00:00"),
    "naive_datetime": lambda c: c["questionnaire_versions"][1].update(effective_from="2026-11-03T18:23:03"),
    "not_a_date": lambda c: c["questionnaire_versions"][1].update(effective_from="soon"),
    "date_missing": lambda c: c["questionnaire_versions"][1].update(effective_from=None),
    "not_increasing": lambda c: c["questionnaire_versions"].append(
        {"key": "V3", "effective_from": "2026-11-01T00:00:00+05:30"}),
}


@pytest.mark.parametrize("name", list(INVALID_CONFIGS))
def test_invalid_questionnaire_versions_in_an_active_config_raise_and_change_nothing(name):
    async def body(conn):
        ids = await _mixed(conn)
        cfg = _bad(INVALID_CONFIGS[name])
        await conn.execute(
            "UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid AND is_active",
            YOUTH["config_id"], json.dumps(cfg))
        before = await _stamps(conn)
        with pytest.raises(ValueError, match="questionnaire_versions"):
            await restamp_questionnaire_versions()
        # one transaction: the other personas (whatever the order) were not relabelled either
        assert await _stamps(conn) == before
        assert (await _versions(conn))[ids["p_before_wrong"]] == "V2"
        # a dry run reads the same config, so it fails the same way
        with pytest.raises(ValueError, match="questionnaire_versions"):
            await restamp_questionnaire_versions(dry_run=True)
    _run(body)


def test_invalid_config_of_another_session_type_does_not_block_a_filtered_run():
    async def body(conn):
        ids = await _mixed(conn)
        cfg = _bad(INVALID_CONFIGS["empty_list"])
        await conn.execute(
            "UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid AND is_active",
            YOUTH["config_id"], json.dumps(cfg))
        res = await restamp_questionnaire_versions(session_type="Parent_Voice")
        assert res["changed"] == 2
        assert (await _versions(conn))[ids["p_before_wrong"]] == "V1"
        with pytest.raises(ValueError):
            await restamp_questionnaire_versions(session_type="Youth_Voice")
    _run(body)


def test_inactive_invalid_config_version_is_ignored():
    async def body(conn):
        await _mixed(conn)
        # an old, inactive version with garbage next to the active valid one: only the active one is read
        await conn.execute(
            "INSERT INTO config_version (config_id, version, is_active, config) VALUES ($1::uuid, 0, FALSE, $2::jsonb)",
            YOUTH["config_id"], json.dumps({"questionnaire_versions": []}))
        res = await restamp_questionnaire_versions()
        assert res["changed"] == len(WRONG)
    _run(body)


async def _add_other_tenant_session(conn, submission_id, stored_version, started_at):
    """A Youth_Voice Mitra session of ANOTHER tenant (its own fictitious program) that reuses a submission id of 'mitra'."""
    program = await factories.add_program(conn, "other", "Other Programme", "Youth")
    await factories.store_session(
        conn, program, "Youth_Voice", "mitra", stored_version, started_at, sid=submission_id,
        messages=_messages())


def test_the_relabel_is_scoped_by_tenant_and_submission_id():
    async def body(conn):
        # the same submission id in two tenants; the mitra copy is wrong (V1 after the V2 start), the other
        # tenant's copy is labelled V2 already (and is "right" by the mitra windows): neither may be mixed up
        sid = await _add(conn, "Youth", "Youth_Voice", "V1", _at(2026, 11, 5, 9, 0, 0))
        await _add_other_tenant_session(conn, sid, "V2", _at(2026, 11, 5, 9, 0, 0))
        try:
            before = {r["tenant_code"]: (r["questionnaire_version"], r["updated_at"]) for r in await conn.fetch(
                "SELECT tenant_code, questionnaire_version, updated_at FROM feedback_submissions "
                "WHERE submission_id = $1", sid)}
            assert set(before) == {"mitra", "other"}
            res = await restamp_questionnaire_versions()
            assert res["session_types"]["Youth_Voice"]["changed"] == 1
            after = {r["tenant_code"]: (r["questionnaire_version"], r["updated_at"]) for r in await conn.fetch(
                "SELECT tenant_code, questionnaire_version, updated_at FROM feedback_submissions "
                "WHERE submission_id = $1", sid)}
            assert after["mitra"][0] == "V2" and after["mitra"][1] > before["mitra"][1]
            assert after["other"] == before["other"]  # not touched
        finally:
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'other'")
    _run(body)


def test_a_session_of_another_tenant_that_points_at_a_mitra_program_is_never_counted_or_relabelled():
    """
    The program of a session does not isolate tenants on its own (submissions.program_id only has a foreign key to
    programs): a row of tenant 'other' that carries a MITRA program id, with a label the mitra windows WOULD change
    (V1 after the V2 start), must stay out of the count and keep its label. Plain SQL writes it (the ingest path
    refuses it), the way a mis-tagged or hostile row would look.
    """
    async def body(conn):
        mine = await _add(conn, "Youth", "Youth_Voice", "V1", _at(2026, 11, 5, 9, 0, 0))          # wrong: becomes V2
        await pg_support.insert_foreign_session(
            conn, tenant_code="other", submission_id="foreign-1", program_id=YOUTH["program_id"],
            started_at=_at(2026, 11, 5, 9, 0, 0), version="V1", session_type="Youth_Voice")        # would become V2
        before = await conn.fetchrow(
            "SELECT questionnaire_version, updated_at FROM feedback_submissions WHERE tenant_code = 'other'")
        try:
            dry = await restamp_questionnaire_versions("Youth_Voice", dry_run=True)
            assert _counts(dry["session_types"]["Youth_Voice"]) == {"sessions": 1, "changed": 1, "versions": {"V2": 1}}
            res = await restamp_questionnaire_versions("Youth_Voice")
            assert _counts(res["session_types"]["Youth_Voice"]) == {"sessions": 1, "changed": 1, "versions": {"V2": 1}}
            assert res["changed"] == 1
            assert (await _versions(conn))[mine] == "V2"
            after = await conn.fetchrow(
                "SELECT questionnaire_version, updated_at FROM feedback_submissions WHERE tenant_code = 'other'")
            assert (after["questionnaire_version"], after["updated_at"]) == (before["questionnaire_version"], before["updated_at"])
            assert after["questionnaire_version"] == "V1"
        finally:
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'other'")
    _run(body)
