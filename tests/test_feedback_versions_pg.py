"""
Real-Postgres tests for set_questionnaire_version / list_questionnaire_versions
(app/database/feedback_operations.py): the questionnaire version windows of a persona are DATA in its Mitra upload
config (config_version rows), recorded with set_questionnaire_version, no code involved.

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards) (the tests wipe the feedback rows and the configs before each case). The file applies schema.sql
itself (the way app/database/db.py does it), creates the tenant 'mitra' and runs the real seed (config_version 1,
only V1, for every persona), points db.pool at the scratch database and stores sessions through the real ingest
path. Synthetic data only.
"""
import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import factories
import pg_support

from app.database.db import db
from app.database.feedback_operations import (
    list_questionnaire_versions,
    restamp_questionnaire_versions,
    set_questionnaire_version,
)
from app.database.operations import insert_or_update_submission
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, seed_config

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

YOUTH, PARENT, GENERAL, EDUCATOR = (PERSONAS[n] for n in ("Youth", "Parent", "General Public", "Educator"))
ST_ALL = ("Educator_Voice", "General_Public_Voice", "Parent_Voice", "Youth_Voice")

V2_TEXT = "2026-11-03T18:23:03+05:30"      # = 2026-11-03 12:53:03 UTC
V3_TEXT = "2026-12-01T10:30:00+05:30"      # = 2026-12-01 05:00:00 UTC
V2_UTC = datetime(2026, 11, 3, 12, 53, 3, tzinfo=timezone.utc)
V3_UTC = datetime(2026, 12, 1, 5, 0, 0, tzinfo=timezone.utc)


def _run(body):
    """
    Runs `body(conn)` on the scratch database of the running module: schema applied, tenant 'mitra', clean seed,
    db.pool pointing at it. Shared with the modules that import it (their own scratch database, hence current_dsn()).
    """
    dsn = pg_support.current_dsn()

    async def main():
        conn = await asyncpg.connect(dsn)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await seed_config(conn)
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
            db.pool = pool
            return await body(conn)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions WHERE tenant_code = 'mitra'")
            await conn.close()
    return asyncio.run(main())


def _at(*args):
    return datetime(*args, tzinfo=timezone.utc)


def _messages():
    return [
        {"sequence": 1, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "How old are you?",
         "translatedMessage": None, "messageAt": None, "parentSequence": None},
        {"sequence": 2, "questionKey": "AGE_IDENTIFICATION", "isFollowUp": False, "message": "17",
         "translatedMessage": None, "messageAt": None, "parentSequence": 1},
    ]


async def _add(conn, persona, stored_version, started_at, source="mitra"):
    """Stores one session (Mitra or Google Forms) through the real ingest path; returns its submission id."""
    sid = f"v{uuid.uuid4().hex[:14]}" if source == "mitra" else str(uuid.uuid4())
    event = {
        "submissionId": sid, "sessionId": sid if source == "google_forms" else f"sess-{uuid.uuid4().hex}",
        "submissionType": "feedback", "tenantCode": "mitra", "eventType": "create",
        "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": PERSONAS[persona]["program_id"], "programName": FEEDBACK_PROGRAM_NAME,
                 "leaderCategoryId": PERSONAS[persona]["leader_category_id"], "leaderCategoryName": persona,
                 "state": None, "district": None, "source": source},
        "data": {"source": source, "adapter": "wide_form" if source == "google_forms" else "conversation_rows", "sessionType": PERSONAS[persona]["session_type"],
                 "questionnaireVersion": stored_version, "language": "en", "completionStatus": "COMPLETED",
                 "submissionDate": started_at.isoformat(), "messages": _messages()},
    }
    if source == "google_forms":
        event["data"]["sourceRef"] = f"{started_at:%Y-%m-%dT%H:%M:%S}:{uuid.uuid4().hex}{uuid.uuid4().hex}"
    else:
        event["data"]["botId"] = PERSONAS[persona]["bot_id"]
    await insert_or_update_submission(conn, event)
    if source != "google_forms":
        # these tests are about the date-window labels, which only apply to sessions without a bot id
        await factories.as_window_stamped_session(conn, sid, stored_version)
    return sid


async def _labels(conn):
    rows = await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")
    return {r["submission_id"]: r["questionnaire_version"] for r in rows}


async def _versions_of(conn, persona):
    """All config_version rows of the persona's Mitra config, oldest first, with the parsed config."""
    rows = await conn.fetch(
        "SELECT version, is_active, config, created_by, change_note FROM config_version WHERE config_id = $1::uuid "
        "ORDER BY version", persona["config_id"])
    return [{"version": r["version"], "is_active": r["is_active"], "versions": json.loads(r["config"]).get("questionnaire_versions"),
             "config": json.loads(r["config"]), "created_by": r["created_by"], "change_note": r["change_note"]}
            for r in rows]


async def _active_counts(conn):
    rows = await conn.fetch("SELECT config_id, count(*) FILTER (WHERE is_active) AS n FROM config_version GROUP BY 1")
    return {str(r["config_id"]): r["n"] for r in rows}


async def _snapshot(conn):
    """Everything a refused / dry-run call must not touch: configs, config versions, session labels."""
    cfg = [tuple(r.values()) for r in await conn.fetch("SELECT * FROM config ORDER BY id")]
    ver = [tuple(r.values()) for r in await conn.fetch("SELECT * FROM config_version ORDER BY config_id, version")]
    fs = [tuple(r.values()) for r in await conn.fetch(
        "SELECT submission_id, tenant_code, questionnaire_version, session_type, updated_at "
        "FROM feedback_submissions ORDER BY submission_id")]
    return cfg, ver, fs


def _keys(versions):
    return [(e["key"], e["effective_from"]) for e in versions]


BASE = [("V1", None)]


# ---------------------------------------------------------------------------------- append V2 then V3 (history)

def test_appending_v2_then_v3_writes_config_versions_2_and_3_and_keeps_history():
    async def body(conn):
        before = await _versions_of(conn, YOUTH)
        assert [(v["version"], v["is_active"]) for v in before] == [(1, True)]
        assert _keys(before[0]["versions"]) == BASE

        r2 = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, created_by="alice")
        assert r2["changed"] is True and r2["dry_run"] is False and r2["session_type"] == "Youth_Voice"
        assert r2["config_version"] == 2 and _keys(r2["versions"]) == BASE + [("V2", V2_TEXT)]
        rows = await _versions_of(conn, YOUTH)
        assert [(v["version"], v["is_active"]) for v in rows] == [(1, False), (2, True)]
        assert _keys(rows[0]["versions"]) == BASE                      # the old version is intact
        assert _keys(rows[1]["versions"]) == BASE + [("V2", V2_TEXT)]
        assert rows[1]["created_by"] == "alice"
        assert rows[1]["change_note"] == f"questionnaire version V2 effective from {V2_TEXT}"  # the default note
        assert (await _active_counts(conn))[YOUTH["config_id"]] == 1

        r3 = await set_questionnaire_version("Youth_Voice", "V3", V3_TEXT, created_by="bob", change_note="third edit of the bot")
        assert r3["changed"] is True and r3["config_version"] == 3
        assert _keys(r3["versions"]) == BASE + [("V2", V2_TEXT), ("V3", V3_TEXT)]
        rows = await _versions_of(conn, YOUTH)
        assert [(v["version"], v["is_active"]) for v in rows] == [(1, False), (2, False), (3, True)]
        assert _keys(rows[0]["versions"]) == BASE
        assert _keys(rows[1]["versions"]) == BASE + [("V2", V2_TEXT)]  # history stays exactly as it was written
        assert _keys(rows[2]["versions"]) == BASE + [("V2", V2_TEXT), ("V3", V3_TEXT)]
        assert rows[1]["created_by"] == "alice" and rows[2]["created_by"] == "bob"
        assert rows[2]["change_note"] == "third edit of the bot"  # a custom note is stored as given
        assert (await _active_counts(conn))[YOUTH["config_id"]] == 1
        # the rest of the config (columns, opening row ...) is carried over unchanged
        assert {k: v for k, v in rows[2]["config"].items() if k != "questionnaire_versions"} == \
               {k: v for k, v in rows[0]["config"].items() if k != "questionnaire_versions"}
    _run(body)


def test_default_created_by_is_cli():
    async def body(conn):
        await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        assert (await _versions_of(conn, YOUTH))[-1]["created_by"] == "cli"
    _run(body)


def test_exactly_one_active_version_after_every_kind_of_call():
    async def body(conn):
        calls = [
            ("V2", V2_TEXT, {}),                                   # append
            ("V3", V3_TEXT, {}),                                   # append
            ("V3", V3_TEXT, {}),                                   # no change
            ("V2", "2026-11-10T00:00:00+05:30", {}),               # move forward
            ("V2", "2026-11-02T00:00:00+05:30", {}),               # move backward
            ("V4", "2027-01-01T00:00:00+05:30", {"restamp": False}),
            ("V5", "2027-02-01T00:00:00+05:30", {"dry_run": True}),  # rolled back
        ]
        for key, text, extra in calls:
            await set_questionnaire_version("Youth_Voice", key, text, **extra)
            counts = await _active_counts(conn)
            assert counts and all(n == 1 for n in counts.values()), (key, counts)
        rows = await _versions_of(conn, YOUTH)
        assert [v["version"] for v in rows] == [1, 2, 3, 4, 5, 6][:len(rows)]   # contiguous
        assert [v["version"] for v in rows if v["is_active"]] == [rows[-1]["version"]]
        assert _keys(rows[-1]["versions"])[-1][0] == "V4"                       # V5 was only a dry run
    _run(body)


# ---------------------------------------------------------------------------------- relabelling in the same call

def test_stored_sessions_are_relabelled_in_the_same_call_boundary_inclusive_second_precision():
    async def body(conn):
        s = {
            "far_before": await _add(conn, "Youth", "V1", _at(2026, 10, 1, 9, 0, 0)),
            "just_before": await _add(conn, "Youth", "V2", V2_UTC - timedelta(seconds=1)),     # stored wrong
            "sub_second_before": await _add(conn, "Youth", "V2", V2_UTC - timedelta(microseconds=1)),
            "boundary": await _add(conn, "Youth", "V1", V2_UTC),                                # stored wrong
            "just_after": await _add(conn, "Youth", "V1", V2_UTC + timedelta(seconds=1)),
            "later": await _add(conn, "Youth", "V1", _at(2026, 11, 20, 9, 0, 0)),
        }
        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        assert res["restamp"] == {"sessions": 6, "changed": 5, "versions": {"V1": 3, "V2": 3}}
        labels = await _labels(conn)
        assert {n: labels[i] for n, i in s.items()} == {
            "far_before": "V1", "just_before": "V1", "sub_second_before": "V1",
            "boundary": "V2", "just_after": "V2", "later": "V2"}

        # a V3 that starts before a session stamped V2: that session becomes V3, earlier ones keep V2
        res = await set_questionnaire_version("Youth_Voice", "V3", "2026-11-10T00:00:00+00:00")
        assert res["restamp"] == {"sessions": 6, "changed": 1, "versions": {"V1": 3, "V2": 2, "V3": 1}}
        labels = await _labels(conn)
        assert labels[s["later"]] == "V3" and labels[s["just_after"]] == "V2" and labels[s["boundary"]] == "V2"
    _run(body)


def test_restamp_false_leaves_the_labels_and_a_later_restamp_fixes_them():
    async def body(conn):
        sid = await _add(conn, "Youth", "V1", _at(2026, 11, 5, 9, 0, 0))
        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, restamp=False)
        assert res["changed"] is True and res["config_version"] == 2 and res["restamp"] is None
        assert (await _labels(conn))[sid] == "V1"
        listed = await list_questionnaire_versions("Youth_Voice")
        assert listed["Youth_Voice"]["sessions"] == {"V1": 1} and listed["Youth_Voice"]["config_version"] == 2
        fixed = await restamp_questionnaire_versions("Youth_Voice")
        assert fixed["changed"] == 1 and (await _labels(conn))[sid] == "V2"
    _run(body)


def test_dry_run_writes_nothing_but_reports_the_new_config_version_and_the_counts():
    async def body(conn):
        a = await _add(conn, "Youth", "V1", _at(2026, 10, 1, 9, 0, 0))
        b = await _add(conn, "Youth", "V1", _at(2026, 11, 5, 9, 0, 0))
        before = await _snapshot(conn)
        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, dry_run=True)
        assert res["dry_run"] is True and res["changed"] is True and res["config_version"] == 2
        assert _keys(res["versions"]) == BASE + [("V2", V2_TEXT)]
        assert res["restamp"] == {"sessions": 2, "changed": 1, "versions": {"V1": 1, "V2": 1}}
        assert await _snapshot(conn) == before                      # no config_version, no relabel, no updated_at
        assert (await _labels(conn))[b] == "V1" and len(await _versions_of(conn, YOUTH)) == 1

        # the real call afterwards writes exactly what the dry run announced
        real = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        assert real["config_version"] == res["config_version"] and real["restamp"] == res["restamp"]
        assert real["dry_run"] is False and (await _labels(conn))[b] == "V2" and (await _labels(conn))[a] == "V1"

        # a dry run of a date move reports the next number, too, and still writes nothing
        before = await _snapshot(conn)
        moved = await set_questionnaire_version("Youth_Voice", "V2", "2026-11-09T00:00:00+05:30", dry_run=True)
        assert moved["changed"] is True and moved["config_version"] == 3
        assert moved["restamp"] == {"sessions": 2, "changed": 1, "versions": {"V1": 2}}
        assert await _snapshot(conn) == before
    _run(body)


def test_dry_run_with_no_restamp_reports_no_counts():
    async def body(conn):
        before = await _snapshot(conn)
        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, dry_run=True, restamp=False)
        assert res["changed"] is True and res["config_version"] == 2 and res["restamp"] is None
        assert await _snapshot(conn) == before
    _run(body)


# ---------------------------------------------------------------------------------- moving an existing key

def test_moving_the_date_of_an_existing_key_writes_a_new_config_version_and_relabels_both_ways():
    async def body(conn):
        early = await _add(conn, "Youth", "V1", _at(2026, 11, 2, 0, 0, 0))
        mid = await _add(conn, "Youth", "V1", _at(2026, 11, 6, 0, 0, 0))
        late = await _add(conn, "Youth", "V1", _at(2026, 11, 20, 0, 0, 0))
        await set_questionnaire_version("Youth_Voice", "V2", "2026-11-01T00:00:00+00:00")
        await set_questionnaire_version("Youth_Voice", "V3", "2026-11-15T00:00:00+00:00")
        labels = await _labels(conn)
        assert (labels[early], labels[mid], labels[late]) == ("V2", "V2", "V3")

        # forward: V2 now starts after `early` and after `mid`
        res = await set_questionnaire_version("Youth_Voice", "V2", "2026-11-10T00:00:00+00:00")
        assert res["changed"] is True and res["config_version"] == 4
        assert res["restamp"]["changed"] == 2 and res["restamp"]["versions"] == {"V1": 2, "V3": 1}
        assert _keys(res["versions"]) == BASE + [("V2", "2026-11-10T00:00:00+00:00"), ("V3", "2026-11-15T00:00:00+00:00")]
        labels = await _labels(conn)
        assert (labels[early], labels[mid], labels[late]) == ("V1", "V1", "V3")

        # backward: V2 starts before `mid` again (but after `early`)
        res = await set_questionnaire_version("Youth_Voice", "V2", "2026-11-05T00:00:00+00:00")
        assert res["changed"] is True and res["config_version"] == 5 and res["restamp"]["changed"] == 1
        labels = await _labels(conn)
        assert (labels[early], labels[mid], labels[late]) == ("V1", "V2", "V3")

        rows = await _versions_of(conn, YOUTH)
        assert [(v["version"], v["is_active"]) for v in rows] == [(1, False), (2, False), (3, False), (4, False), (5, True)]
        assert _keys(rows[3]["versions"])[1] == ("V2", "2026-11-10T00:00:00+00:00")   # history kept per move
        assert _keys(rows[4]["versions"])[1] == ("V2", "2026-11-05T00:00:00+00:00")
    _run(body)


def test_the_same_value_again_changes_nothing_and_writes_no_config_version():
    async def body(conn):
        await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, created_by="alice")
        sid = await _add(conn, "Youth", "V1", _at(2026, 11, 5, 9, 0, 0))  # stored label is stale
        before_versions = await _versions_of(conn, YOUTH)
        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        assert res["changed"] is False and res["config_version"] == 2 and res["dry_run"] is False
        assert _keys(res["versions"]) == BASE + [("V2", V2_TEXT)]
        assert res["restamp_note"] == "no change to the version list"
        assert await _versions_of(conn, YOUTH) == before_versions           # no new row, nothing deactivated
        # it still relabels (restamp is on), and reports it
        assert res["restamp"] == {"sessions": 1, "changed": 1, "versions": {"V2": 1}}
        assert (await _labels(conn))[sid] == "V2"

        again = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, restamp=False)
        assert again["changed"] is False and again["restamp"] is None and again["config_version"] == 2
        assert len(await _versions_of(conn, YOUTH)) == 2
    _run(body)


def test_the_same_instant_in_another_offset_or_z_is_not_a_change():
    async def body(conn):
        await set_questionnaire_version("Youth_Voice", "V2", "2026-11-03T18:23:03+05:30")
        before = await _versions_of(conn, YOUTH)
        for same_instant in ("2026-11-03T12:53:03+00:00", "2026-11-03T12:53:03Z", "2026-11-03T14:53:03+02:00"):
            res = await set_questionnaire_version("Youth_Voice", "V2", same_instant, restamp=False)
            assert res["changed"] is False and res["config_version"] == 2
        assert await _versions_of(conn, YOUTH) == before    # no new config_version
    _run(body)


def test_same_value_with_dry_run_writes_nothing():
    async def body(conn):
        await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        sid = await _add(conn, "Youth", "V1", _at(2026, 11, 5, 9, 0, 0))
        before = await _snapshot(conn)
        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, dry_run=True)
        assert res["changed"] is False and res["restamp"] == {"sessions": 1, "changed": 1, "versions": {"V2": 1}}
        assert await _snapshot(conn) == before and (await _labels(conn))[sid] == "V1"
    _run(body)


# ---------------------------------------------------------------------------------- refusals (nothing written)

async def _prepare_for_refusals(conn):
    """Youth with V2 and V3, and a stale-labelled session, so a bad call has something it could wrongly change."""
    await set_questionnaire_version("Youth_Voice", "V2", "2026-11-03T00:00:00+00:00")
    await set_questionnaire_version("Youth_Voice", "V3", "2026-12-01T00:00:00+00:00")
    await _add(conn, "Youth", "V1", _at(2026, 11, 10, 9, 0, 0))   # should be V2, stored wrong
    await _add(conn, "Parent", "V2", _at(2026, 11, 10, 9, 0, 0))  # should be V1, stored wrong
    await _add(conn, "Youth", "V1", _at(2026, 11, 11, 9, 0, 0), source="google_forms")


REFUSALS = {
    "first_key": ("Youth_Voice", "V1", "2026-11-01T00:00:00+00:00", "first one"),
    "naive_datetime": ("Youth_Voice", "V4", "2026-12-05T10:00:00", "UTC offset"),
    "naive_date_only": ("Youth_Voice", "V4", "2026-12-05", "UTC offset"),
    "unparsable_text": ("Youth_Voice", "V4", "next tuesday", "not an ISO instant"),
    "empty_effective_from": ("Youth_Voice", "V4", "", "not an ISO instant"),
    "none_effective_from": ("Youth_Voice", "V4", None, "not an ISO instant"),
    "not_after_previous": ("Youth_Voice", "V4", "2026-11-30T23:59:59+00:00", "strictly increasing"),
    "equal_to_previous": ("Youth_Voice", "V4", "2026-12-01T00:00:00+00:00", "strictly increasing"),
    "same_instant_other_offset": ("Youth_Voice", "V4", "2026-12-01T05:30:00+05:30", "strictly increasing"),
    "move_v2_past_v3": ("Youth_Voice", "V2", "2026-12-02T00:00:00+00:00", "strictly increasing"),
    "move_v2_onto_v3": ("Youth_Voice", "V2", "2026-12-01T00:00:00+00:00", "strictly increasing"),
    "move_v3_before_v2": ("Youth_Voice", "V3", "2026-11-02T00:00:00+00:00", "strictly increasing"),
    "empty_key": ("Youth_Voice", "", "2026-12-05T00:00:00+00:00", "non-empty"),
    "blank_key": ("Youth_Voice", "   ", "2026-12-05T00:00:00+00:00", "non-empty"),
    "unknown_persona": ("Nobody_Voice", "V2", "2026-12-05T00:00:00+00:00", "no active upload config with questionnaire versions"),
    "persona_label_instead_of_session_type": ("Youth", "V2", "2026-12-05T00:00:00+00:00", "no active upload config with questionnaire versions"),
}


@pytest.mark.parametrize("name", list(REFUSALS))
def test_refused_calls_raise_value_error_and_write_nothing(name):
    session_type, key, effective_from, match = REFUSALS[name]

    async def body(conn):
        await _prepare_for_refusals(conn)
        before = await _snapshot(conn)
        for extra in ({}, {"dry_run": True}, {"restamp": False}):
            with pytest.raises(ValueError, match=match):
                await set_questionnaire_version(session_type, key, effective_from, **extra)
        assert await _snapshot(conn) == before
        assert all(n == 1 for n in (await _active_counts(conn)).values())
    _run(body)


def test_a_persona_with_only_a_google_forms_config_is_refused():
    async def body(conn):
        # General Public has a Mitra and a Google Forms config; with the Mitra one inactive only the form's remains
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", GENERAL["config_id"])
        gforms_before = await conn.fetch(
            "SELECT v.config FROM config c JOIN config_version v ON v.config_id = c.id "
            "WHERE c.config_source = 'google_forms' AND c.session_type = 'General_Public_Voice'")
        assert len(gforms_before) == 1
        before = await _snapshot(conn)
        with pytest.raises(ValueError, match="no active upload config with questionnaire versions"):
            await set_questionnaire_version("General_Public_Voice", "V2", V2_TEXT)
        assert await _snapshot(conn) == before
    _run(body)


def test_google_forms_configs_and_sessions_are_never_touched():
    async def body(conn):
        g = await _add(conn, "Youth", "V1", _at(2026, 11, 20, 9, 0, 0), source="google_forms")
        gforms_cfg = [tuple(r.values()) for r in await conn.fetch(
            "SELECT v.* FROM config c JOIN config_version v ON v.config_id = c.id WHERE c.config_source = 'google_forms' "
            "ORDER BY v.id")]
        assert len(gforms_cfg) == 3
        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        assert res["restamp"]["sessions"] == 0                       # the form response is not a Mitra session
        assert (await _labels(conn))[g] == "V1"
        assert [tuple(r.values()) for r in await conn.fetch(
            "SELECT v.* FROM config c JOIN config_version v ON v.config_id = c.id WHERE c.config_source = 'google_forms' "
            "ORDER BY v.id")] == gforms_cfg
    _run(body)


@pytest.mark.parametrize("name,stored", [
    ("empty_list", {"questionnaire_versions": []}),
    ("missing", {"adapter": "conversation_rows"}),  # the list is gone but the adapter says it is required
    ("duplicate_key", {"questionnaire_versions": [{"key": "V1", "effective_from": None},
                                                  {"key": "V1", "effective_from": "2026-11-03T00:00:00+00:00"}]}),
    ("naive_date", {"questionnaire_versions": [{"key": "V1", "effective_from": None},
                                               {"key": "V2", "effective_from": "2026-11-03T00:00:00"}]}),
    ("first_has_date", {"questionnaire_versions": [{"key": "V1", "effective_from": "2026-01-01T00:00:00+00:00"}]}),
])
def test_a_stored_invalid_list_makes_set_raise_and_roll_back(name, stored):
    async def body(conn):
        sid = await _add(conn, "Youth", "V1", _at(2026, 11, 10, 9, 0, 0))
        await conn.execute(
            "UPDATE config_version SET config = $2::jsonb WHERE config_id = $1::uuid AND is_active",
            YOUTH["config_id"], json.dumps(stored))
        before = await _snapshot(conn)
        for extra in ({}, {"dry_run": True}):
            with pytest.raises(ValueError, match="questionnaire_versions"):
                await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, **extra)
        assert await _snapshot(conn) == before and (await _labels(conn))[sid] == "V1"
        assert len(await _versions_of(conn, YOUTH)) == 1
    _run(body)


# ---------------------------------------------------------------------------------- accepted date forms

@pytest.mark.parametrize("given,stored", [
    ("2026-12-01T05:00:00Z", "2026-12-01T05:00:00+00:00"),
    ("2026-12-01T05:00:00z", "2026-12-01T05:00:00+00:00"),
    ("2026-12-01T10:30:00+05:30", "2026-12-01T10:30:00+05:30"),
    ("2026-12-01T14:00:00+09:00", "2026-12-01T14:00:00+09:00"),
    ("2026-11-30T20:00:00-09:00", "2026-11-30T20:00:00-09:00"),
    ("2026-12-01T05:00:00.250000+00:00", "2026-12-01T05:00:00.250000+00:00"),
])
def test_utc_suffix_and_other_offsets_are_accepted_and_stored_with_isoformat(given, stored):
    async def body(conn):
        sid = await _add(conn, "Youth", "V1", _at(2026, 12, 1, 5, 0, 1))   # one second after, whatever the offset
        res = await set_questionnaire_version("Youth_Voice", "V2", given)
        assert res["changed"] is True and res["versions"][1] == {"key": "V2", "effective_from": stored}
        assert _keys((await _versions_of(conn, YOUTH))[-1]["versions"])[1] == ("V2", stored)
        assert (await _labels(conn))[sid] == "V2"
    _run(body)


def test_the_key_is_trimmed_and_the_offset_changes_the_instant_not_the_wall_clock():
    async def body(conn):
        early = await _add(conn, "Youth", "V1", _at(2026, 12, 1, 4, 0, 0))   # 09:30 IST, before 10:30 IST
        late = await _add(conn, "Youth", "V1", _at(2026, 12, 1, 5, 30, 0))
        res = await set_questionnaire_version("Youth_Voice", " V2 ", V3_TEXT)
        assert [e["key"] for e in res["versions"]] == ["V1", "V2"]
        labels = await _labels(conn)
        assert (labels[early], labels[late]) == ("V1", "V2")
    _run(body)


# ---------------------------------------------------------------------------------- personas and isolation

def test_educator_works_like_the_others():
    async def body(conn):
        a = await _add(conn, "Educator", "V1", _at(2026, 11, 1, 9, 0, 0))
        b = await _add(conn, "Educator", "V1", _at(2026, 11, 8, 9, 0, 0))
        res = await set_questionnaire_version("Educator_Voice", "V2", "2026-11-05T00:00:00+00:00", created_by="eve")
        assert res["changed"] is True and res["config_version"] == 2
        assert res["restamp"] == {"sessions": 2, "changed": 1, "versions": {"V1": 1, "V2": 1}}
        labels = await _labels(conn)
        assert (labels[a], labels[b]) == ("V1", "V2")
        rows = await _versions_of(conn, EDUCATOR)
        assert [(v["version"], v["is_active"]) for v in rows] == [(1, False), (2, True)]
        listed = await list_questionnaire_versions("Educator_Voice")
        assert listed["Educator_Voice"]["config_version"] == 2
    _run(body)


def test_other_personas_configs_and_sessions_are_untouched():
    async def body(conn):
        await _add(conn, "Youth", "V1", _at(2026, 11, 10, 9, 0, 0))
        # sessions of the other personas, deliberately labelled wrong (V2 with no V2 recorded): a Youth call must not fix them
        others = [await _add(conn, p, "V2", _at(2026, 11, 10, 9, 0, 0)) for p in ("Parent", "General Public", "Educator")]
        g = await _add(conn, "Youth", "V1", _at(2026, 11, 10, 9, 0, 0), source="google_forms")
        other_cfgs = {p["config_id"]: await _versions_of(conn, p) for p in (PARENT, GENERAL, EDUCATOR)}
        stamps = {r["submission_id"]: r["updated_at"] for r in await conn.fetch(
            "SELECT submission_id, updated_at FROM feedback_submissions")}
        await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        for p in (PARENT, GENERAL, EDUCATOR):
            assert await _versions_of(conn, p) == other_cfgs[p["config_id"]]
        labels = await _labels(conn)
        assert [labels[s] for s in others] == ["V2", "V2", "V2"] and labels[g] == "V1"
        for r in await conn.fetch("SELECT submission_id, updated_at FROM feedback_submissions"):
            if r["submission_id"] in others or r["submission_id"] == g:
                assert r["updated_at"] == stamps[r["submission_id"]]
    _run(body)


# ---------------------------------------------------------------------------------- list_questionnaire_versions

def test_list_reports_versions_config_version_and_session_counts_for_all_four_personas():
    async def body(conn):
        await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)
        await set_questionnaire_version("Youth_Voice", "V3", V3_TEXT)
        await set_questionnaire_version("Parent_Voice", "V2", V2_TEXT)
        for persona, when in (("Youth", _at(2026, 10, 1, 9, 0, 0)), ("Youth", _at(2026, 11, 5, 9, 0, 0)),
                              ("Youth", _at(2026, 11, 6, 9, 0, 0)), ("Youth", _at(2026, 12, 5, 9, 0, 0)),
                              ("Parent", _at(2026, 11, 5, 9, 0, 0)), ("Educator", _at(2026, 11, 5, 9, 0, 0))):
            await _add(conn, persona, "V9", when)           # stored label irrelevant: counted as stored
        await _add(conn, "Youth", "V1", _at(2026, 11, 5, 9, 0, 0), source="google_forms")  # forms are not counted

        listed = await list_questionnaire_versions()
        assert list(listed) == list(ST_ALL)                  # sorted by session_type, all four
        assert listed["Youth_Voice"]["config_version"] == 3
        assert _keys(listed["Youth_Voice"]["versions"]) == BASE + [("V2", V2_TEXT), ("V3", V3_TEXT)]
        assert listed["Youth_Voice"]["sessions"] == {"V9": 4}
        assert listed["Parent_Voice"]["config_version"] == 2
        assert _keys(listed["Parent_Voice"]["versions"]) == BASE + [("V2", V2_TEXT)]
        assert listed["Parent_Voice"]["sessions"] == {"V9": 1}
        for st in ("Educator_Voice", "General_Public_Voice"):
            assert listed[st]["config_version"] == 1 and _keys(listed[st]["versions"]) == BASE
        assert listed["Educator_Voice"]["sessions"] == {"V9": 1} and listed["General_Public_Voice"]["sessions"] == {}

        await restamp_questionnaire_versions()
        listed = await list_questionnaire_versions()
        assert listed["Youth_Voice"]["sessions"] == {"V1": 1, "V2": 2, "V3": 1}
        assert listed["Parent_Voice"]["sessions"] == {"V2": 1}
        assert listed["Educator_Voice"]["sessions"] == {"V1": 1}
    _run(body)


def test_list_filters_by_session_type_and_reports_only_the_active_version():
    async def body(conn):
        await set_questionnaire_version("Parent_Voice", "V2", V2_TEXT)
        only = await list_questionnaire_versions("Parent_Voice")
        assert list(only) == ["Parent_Voice"] and only["Parent_Voice"]["config_version"] == 2
        assert await list_questionnaire_versions("Nobody_Voice") == {}
        # a deactivated config (nothing active) drops out of the listing
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", EDUCATOR["config_id"])
        assert "Educator_Voice" not in await list_questionnaire_versions()
    _run(body)


def test_list_is_read_only():
    async def body(conn):
        await _add(conn, "Youth", "V7", _at(2026, 11, 5, 9, 0, 0))
        before = await _snapshot(conn)
        await list_questionnaire_versions()
        assert await _snapshot(conn) == before
    _run(body)


# ---------------------------------------------------------------------------------- the row lock

def test_a_set_waits_for_the_row_lock_and_does_not_lose_a_concurrent_update():
    """
    The holder connection takes the lock of the config row and, meanwhile, records V2 the way a concurrent
    set_questionnaire_version would (deactivate v1, insert v2 as the active one) and commits. A set of V3 that was
    started while the lock was held must wait, then build on the V2 list, not on the stale V1 list it started with.
    """
    async def body(conn):
        holder = await asyncpg.connect(DSN)
        try:
            tx = holder.transaction()
            await tx.start()
            await holder.fetchrow("SELECT id FROM config WHERE id = $1::uuid FOR UPDATE", YOUTH["config_id"])
            task = asyncio.ensure_future(set_questionnaire_version("Youth_Voice", "V3", V3_TEXT))
            await asyncio.sleep(0.7)
            assert not task.done(), "set_questionnaire_version did not wait for the config row lock"
            v2_config = (await _versions_of(conn, YOUTH))[0]["config"]
            v2_config["questionnaire_versions"].append({"key": "V2", "effective_from": V2_TEXT})
            await holder.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH["config_id"])
            await holder.execute(
                "INSERT INTO config_version (config_id, version, is_active, config, created_by) "
                "VALUES ($1::uuid, 2, TRUE, $2::jsonb, 'holder')", YOUTH["config_id"], json.dumps(v2_config))
            await tx.commit()
            res = await asyncio.wait_for(task, 10)
        finally:
            await holder.close()
        assert _keys(res["versions"]) == BASE + [("V2", V2_TEXT), ("V3", V3_TEXT)]   # neither update is lost
        rows = await _versions_of(conn, YOUTH)
        assert [(v["version"], v["is_active"]) for v in rows] == [(1, False), (2, False), (3, True)]
        assert _keys(rows[2]["versions"]) == BASE + [("V2", V2_TEXT), ("V3", V3_TEXT)]
    _run(body)


def test_two_sequential_sets_from_two_connections_both_stick():
    async def body(conn):
        other = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
        try:
            first = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT)         # through db.pool
            db.pool, saved = other, db.pool                                               # a second connection
            try:
                second = await set_questionnaire_version("Youth_Voice", "V3", V3_TEXT)
            finally:
                db.pool = saved
        finally:
            await other.close()
        assert (first["config_version"], second["config_version"]) == (2, 3)
        assert _keys((await _versions_of(conn, YOUTH))[-1]["versions"]) == BASE + [("V2", V2_TEXT), ("V3", V3_TEXT)]
        assert (await _active_counts(conn))[YOUTH["config_id"]] == 1
    _run(body)
