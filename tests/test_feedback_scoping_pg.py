"""
Real-Postgres tests of the SCOPING of the feedback lookups and operator tools (app/database/feedback_operations.py):
everything is keyed on (tenant, program, session_type, source), because a persona key (session_type) is not unique
across the programs of a tenant, a source is a free label and a tenant can reuse a persona name. A lookup that matches
more than one row refuses (AmbiguousFeedbackConfigError, a ValueError) instead of guessing, and nothing silently falls
back to another program's, tenant's or source's config.

Opt-in: skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch database for this file alone and drops it afterwards). The file applies schema.sql, creates the tenant 'mitra', runs the real seed, and builds the fictitious
second program / second tenant / second source of each case with plain INSERTs. Synthetic data only.
"""
import asyncio
import copy
import inspect
import json
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest
import factories
import pg_support

from app.database.db import db
from app.database.feedback_operations import (
    AmbiguousFeedbackConfigError,
    detect_questionnaire_versions,
    get_active_feedback_config,
    get_feedback_process_steps,
    get_program_and_leader,
    list_feedback_config,
    list_questionnaire_versions,
    migrate_feedback_config,
    restamp_questionnaire_versions,
    set_feedback_config_value,
    set_questionnaire_version,
)
from app.database.operations import insert_or_update_submission
from app.services.feedback_constants import FEEDBACK_METADATA_EXTRACTION_STEP
from seed_support import PERSONAS, load_seed_config, mitra_config_v1, seed_config

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

PARENT = PERSONAS["Parent"]
YOUTH = PERSONAS["Youth"]
EDUCATOR = PERSONAS["Educator"]
STEP = [{"name": FEEDBACK_METADATA_EXTRACTION_STEP}]
V2_TEXT = "2026-11-03T00:00:00+00:00"


def _run(body):
    """Runs `body(conn)` on the scratch database: schema, tenant 'mitra', clean seed, db.pool pointing at it."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra') ON CONFLICT (code) DO NOTHING")
            await conn.execute("DELETE FROM submissions")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            # a source is a free label: the CHECK that listed three sources (schema.sql, review finding A1) is dropped
            # here so these tests also run before the schema owner removes it; a no-op afterwards
            await conn.execute("ALTER TABLE config DROP CONSTRAINT IF EXISTS config_config_source_check")
            await conn.execute("DELETE FROM programs")
            await conn.execute("DELETE FROM leader_category")
            await conn.execute("DELETE FROM tenant WHERE code <> 'mitra'")
            await seed_config(conn)
            pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
            db.pool = pool
            return await body(conn)
        finally:
            db.pool = None
            if pool is not None:
                await pool.close()
            await conn.execute("DELETE FROM submissions")
            await conn.close()
    return asyncio.run(main())


# ------------------------------------------------------------------------------------------------ builders

async def _program(conn, tenant, program_name, leader_name, **ids):
    program = await factories.add_program(conn, tenant, program_name, leader_name, **ids)
    return {**program, "program_name": program_name}


_config = factories.upload_config
_add_config = factories.add_config


async def _session(conn, program, session_type, source, stored_version, started_at):
    """Stores one session through the real ingest path (any source label, the adapter of a conversation export)."""
    return await factories.store_session(conn, program, session_type, source, stored_version, started_at,
                                         messages=factories.conversation_messages(answers=("41",), key="AGE_IDENTIFICATION"))


def _at(*args):
    return datetime(*args, tzinfo=timezone.utc)


async def _labels(conn):
    return {r["submission_id"]: r["questionnaire_version"]
            for r in await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")}


async def _snapshot(conn):
    """Everything a refused call must not touch."""
    cfg = [tuple(r.values()) for r in await conn.fetch("SELECT * FROM config ORDER BY id")]
    ver = [tuple(r.values()) for r in await conn.fetch("SELECT * FROM config_version ORDER BY config_id, version")]
    fs = [tuple(r.values()) for r in await conn.fetch(
        "SELECT submission_id, tenant_code, questionnaire_version, updated_at FROM feedback_submissions "
        "ORDER BY submission_id")]
    return cfg, ver, fs


async def _active_versions(conn, config_id):
    row = await conn.fetchrow("SELECT version, config FROM config_version WHERE config_id = $1::uuid AND is_active",
                              config_id)
    return row["version"], json.loads(row["config"])


async def _two_programs_one_persona(conn):
    """Program 'PTM 2026' of tenant mitra with its OWN leader category named 'Parent' (the seeded 1M Voice program
    also has one) and its own mitra upload config for the SAME session_type Parent_Voice (steps: the extraction)."""
    p2 = await _program(conn, "mitra", "PTM 2026", "Parent")
    cfg2 = await _add_config(conn, p2, "mitra", "Parent_Voice", _config(process_steps=STEP))
    p1 = {"program_id": PARENT["program_id"], "leader_id": PARENT["leader_category_id"], "tenant": "mitra",
          "program_name": PARENT["program_name"]}
    return p1, p2, PARENT["config_id"], cfg2


# ------------------------------------------------------------------------------------- A9 / C5: program + persona

def test_each_program_resolves_to_its_own_persona_when_two_leaders_share_a_name():
    async def body(conn):
        p1, p2, _, _ = await _two_programs_one_persona(conn)
        one = await get_program_and_leader(PARENT["program_name"], "Parent", "mitra")
        two = await get_program_and_leader("PTM 2026", "Parent", "mitra")
        assert one == {"program": {"id": p1["program_id"], "name": PARENT["program_name"]},
                       "leader": {"id": p1["leader_id"], "name": "Parent"}, "tenant_code": "mitra"}
        assert two == {"program": {"id": p2["program_id"], "name": "PTM 2026"},
                       "leader": {"id": p2["leader_id"], "name": "Parent"}, "tenant_code": "mitra"}
        assert await get_program_and_leader("PTM 2026", "Youth", "mitra") is None      # not that program's persona
        assert await get_program_and_leader("PTM 2026", "Parent", "other") is None     # not that tenant
        assert await get_program_and_leader("Nobody", "Parent", "mitra") is None
    _run(body)


def test_a_program_whose_leader_category_belongs_to_another_tenant_is_never_resolved():
    """
    programs.leaders_id only has a foreign key to leader_category (no tenant check): a program of 'mitra' that points
    at a leader category of tenant 'other' (plain SQL, the shape a mis-tagged row would have) must not resolve for
    'mitra' by that leader's name, nor for 'other' by the program name.
    """
    async def body(conn):
        foreign = await _program(conn, "other", "Foreign Programme", "Seekers")
        await conn.execute(
            "INSERT INTO programs (id, leaders_id, name, tenant_code) VALUES ($1::uuid, $2::uuid, 'Mis-tagged', 'mitra')",
            factories.uid(), foreign["leader_id"])
        assert await get_program_and_leader("Mis-tagged", "Seekers", "mitra") is None
        assert await get_program_and_leader("Mis-tagged", "Seekers", "other") is None
        assert (await get_program_and_leader("Foreign Programme", "Seekers", "other"))["program"]["id"] == foreign["program_id"]
    _run(body)


@pytest.mark.parametrize("duplicate_created_at", [_at(2000, 1, 1), _at(2100, 1, 1)])
def test_a_preexisting_duplicate_leader_name_never_hides_the_program(duplicate_created_at):
    # the seed adds Educator with fixed ids; a tenant that already had an 'Educator' leader row (no program under
    # it) must not make the persona unresolvable, whichever of the two rows is older
    async def body(conn):
        await conn.execute(
            "INSERT INTO leader_category (id, name, tenant_code, created_at) VALUES ($1::uuid, 'Educator', 'mitra', $2::timestamptz)",
            str(uuid.uuid4()), duplicate_created_at)
        assert await conn.fetchval("SELECT count(*) FROM leader_category WHERE name = 'Educator'") == 2
        found = await get_program_and_leader(EDUCATOR["program_name"], "Educator", "mitra")
        assert found is not None
        assert found["program"]["id"] == EDUCATOR["program_id"] and found["leader"]["id"] == EDUCATOR["leader_category_id"]
    _run(body)


def test_two_programs_of_one_name_under_two_leaders_of_one_name_are_refused_not_guessed():
    async def body(conn):
        await _program(conn, "mitra", "Twin", "Teacher")
        assert (await get_program_and_leader("Twin", "Teacher", "mitra"))["program"]["name"] == "Twin"
        await _program(conn, "mitra", "Twin", "Teacher")
        with pytest.raises(AmbiguousFeedbackConfigError, match="more than one program named 'Twin'"):
            await get_program_and_leader("Twin", "Teacher", "mitra")
        assert isinstance(AmbiguousFeedbackConfigError("x"), ValueError)
    _run(body)


# ------------------------------------------------------------------------------------- A8: get_active_feedback_config

def test_the_source_of_get_active_feedback_config_has_no_default():
    assert inspect.signature(get_active_feedback_config).parameters["config_source"].default is inspect.Parameter.empty


def test_a_program_with_two_session_types_is_ambiguous_until_the_session_type_is_given():
    async def body(conn):
        p2 = await _program(conn, "mitra", "PTM 2026", "Parent")
        await _add_config(conn, p2, "ptm_app", "Parent_PTM")
        await _add_config(conn, p2, "ptm_app", "Teacher_PTM")
        with pytest.raises(AmbiguousFeedbackConfigError, match="Parent_PTM, Teacher_PTM"):
            await get_active_feedback_config(p2["program_id"], "ptm_app")
        assert (await get_active_feedback_config(p2["program_id"], "ptm_app", session_type="Teacher_PTM"))[
            "session_type"] == "Teacher_PTM"
        assert await get_active_feedback_config(p2["program_id"], "mitra") is None      # another source: nothing
        assert await get_active_feedback_config(p2["program_id"], "ptm_app", tenant_code="other") is None
        only = await get_active_feedback_config(PARENT["program_id"], "mitra")           # one config: no ambiguity
        assert only["session_type"] == "Parent_Voice"
    _run(body)


# ------------------------------------------------------------------------------------- A8: process steps, no fallback

def test_process_steps_come_from_the_submissions_own_program_and_never_from_another():
    async def body(conn):
        p1, p2, _, _ = await _two_programs_one_persona(conn)
        s1 = await _session(conn, p1, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        s2 = await _session(conn, p2, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        assert await get_feedback_process_steps(conn, s1, "mitra") == []
        assert await get_feedback_process_steps(conn, s2, "mitra") == STEP

        # a third program of the same tenant with the same persona key but NO config of its own: it used to run the
        # steps of some other program's config; now it is an operator error that names the program
        p3 = await _program(conn, "mitra", "Unconfigured", "Parent")
        s3 = await _session(conn, p3, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        with pytest.raises(ValueError, match="No active mitra feedback_upload config .*" + p3["program_id"]):
            await get_feedback_process_steps(conn, s3, "mitra")

        # the same for a program whose program row was deleted (submissions.program_id SET NULL)
        await conn.execute("UPDATE submissions SET program_id = NULL WHERE submission_id = $1", s3)
        with pytest.raises(ValueError, match="No active mitra feedback_upload config"):
            await get_feedback_process_steps(conn, s3, "mitra")
    _run(body)


def test_the_persona_memo_is_keyed_by_program_too():
    async def body(conn):
        p1, p2, _, _ = await _two_programs_one_persona(conn)
        s1 = await _session(conn, p1, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        s2 = await _session(conn, p2, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        memo = {}
        assert await get_feedback_process_steps(conn, s1, "mitra", memo) == []
        assert await get_feedback_process_steps(conn, s2, "mitra", memo) == STEP
        assert len(memo) == 2
    _run(body)


# ------------------------------------------------------------------------------------- A8: set_feedback_config_value

def test_set_refuses_when_two_programs_share_the_persona_and_edits_only_the_one_named():
    async def body(conn):
        p1, p2, cfg1, cfg2 = await _two_programs_one_persona(conn)
        before = await _snapshot(conn)
        with pytest.raises(AmbiguousFeedbackConfigError, match="2 mitra upload configs for session_type 'Parent_Voice' match"):
            await set_feedback_config_value("Parent_Voice", "mitra", "max_csv_upload_bytes", 5, tenant_code="mitra")
        assert await _snapshot(conn) == before                      # nothing written

        res = await set_feedback_config_value("Parent_Voice", "mitra", "max_csv_upload_bytes", 5_000_000,
                                              tenant_code="mitra", program_id=p2["program_id"])
        assert res["changed"] and res["program_id"] == p2["program_id"] and res["program_name"] == "PTM 2026"
        assert (await _active_versions(conn, cfg2))[0] == 2 and (await _active_versions(conn, cfg1))[0] == 1
        assert (await _active_versions(conn, cfg2))[1]["max_csv_upload_bytes"] == 5_000_000

        res = await set_feedback_config_value("Parent_Voice", "mitra", "max_csv_upload_bytes", 6_000_000,
                                              program_name=PARENT["program_name"])
        assert res["program_id"] == p1["program_id"] and (await _active_versions(conn, cfg1))[1]["max_csv_upload_bytes"] == 6_000_000
        with pytest.raises(ValueError, match="no mitra upload config"):
            await set_feedback_config_value("Parent_Voice", "mitra", "max_csv_upload_bytes", 7, program_name="Nope")
    _run(body)


@pytest.mark.parametrize("kwargs,needle", [
    ({"tenant_code": "  "}, "tenant_code must be a non-empty string"),
    ({"program_name": ""}, "program_name must be a non-empty string"),
    ({"program_id": "not-a-uuid"}, "program_id must be a UUID"),
])
def test_blank_or_malformed_selectors_are_refused_not_read_as_not_given(kwargs, needle):
    async def body(conn):
        before = await _snapshot(conn)
        with pytest.raises(ValueError, match=needle):
            await set_feedback_config_value("Parent_Voice", "mitra", "max_csv_upload_bytes", 5, **kwargs)
        for call in (list_questionnaire_versions, restamp_questionnaire_versions, detect_questionnaire_versions,
                     list_feedback_config):
            with pytest.raises(ValueError, match=needle):
                await call(**kwargs)
        assert await _snapshot(conn) == before
    _run(body)


# ------------------------------------------------------------------------------------- A8 / C7 / C9: versions

def test_set_questionnaire_version_refuses_when_two_programs_share_the_persona():
    async def body(conn):
        p1, p2, cfg1, cfg2 = await _two_programs_one_persona(conn)
        await _session(conn, p1, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        await _session(conn, p2, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        before = await _snapshot(conn)
        for kwargs in ({}, {"dry_run": True}, {"tenant_code": "mitra"}, {"source": "mitra"}):
            with pytest.raises(AmbiguousFeedbackConfigError, match="upload configs with questionnaire versions match"):
                await set_questionnaire_version("Parent_Voice", "V2", V2_TEXT, **kwargs)
        assert await _snapshot(conn) == before
    _run(body)


def test_set_questionnaire_version_changes_and_relabels_only_the_program_named():
    async def body(conn):
        p1, p2, cfg1, cfg2 = await _two_programs_one_persona(conn)
        s1 = await _session(conn, p1, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        s2 = await _session(conn, p2, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        res = await set_questionnaire_version("Parent_Voice", "V2", V2_TEXT, program_id=p2["program_id"])
        assert res["changed"] and res["program_id"] == p2["program_id"] and res["tenant_code"] == "mitra"
        assert res["source"] == "mitra" and res["restamp"]["sessions"] == 1 and res["restamp"]["changed"] == 1
        assert await _labels(conn) == {s1: "V1", s2: "V2"}          # the other program's session is not touched
        assert (await _active_versions(conn, cfg1))[0] == 1          # nor is its config
        assert (await _active_versions(conn, cfg2))[0] == 2
    _run(body)


def test_listing_and_restamping_never_mix_the_sessions_of_two_programs():
    async def body(conn):
        p1, p2, cfg1, cfg2 = await _two_programs_one_persona(conn)
        # P2's config has a V2 window (restamp off), P1's has only V1. Both programs' sessions start after the window.
        await set_questionnaire_version("Parent_Voice", "V2", V2_TEXT, program_id=p2["program_id"], restamp=False)
        s1 = await _session(conn, p1, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        s2 = await _session(conn, p2, "Parent_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))

        listed = await list_questionnaire_versions()
        keys = [k for k in listed if "Parent_Voice" in k]
        assert len(keys) == 2 and all(k.endswith("/Parent_Voice/mitra") for k in keys)   # collision: full labels
        by_program = {v["program_id"]: v for k, v in listed.items() if k in keys}
        assert by_program[p1["program_id"]]["sessions"] == {"V1": 1} and by_program[p1["program_id"]]["config_version"] == 1
        assert by_program[p2["program_id"]]["sessions"] == {"V1": 1} and by_program[p2["program_id"]]["config_version"] == 2
        assert "Youth_Voice" in listed                                                   # unique key stays the bare one

        with pytest.raises(AmbiguousFeedbackConfigError):
            await restamp_questionnaire_versions("Parent_Voice")
        dry = await restamp_questionnaire_versions("Parent_Voice", dry_run=True, program_id=p2["program_id"])
        assert dry["changed"] == 1 and await _labels(conn) == {s1: "V1", s2: "V1"}

        # the sweep relabels each program from ITS OWN windows only (P1 has no V2, so its session stays V1; before the
        # fix each config relabelled the sessions of both programs and the two disagreed)
        res = await restamp_questionnaire_versions()
        assert res["changed"] == 1 and await _labels(conn) == {s1: "V1", s2: "V2"}
        assert (await restamp_questionnaire_versions())["changed"] == 0

        only_p1 = await list_questionnaire_versions("Parent_Voice", program_name=PARENT["program_name"])
        assert list(only_p1) == ["Parent_Voice"] and only_p1["Parent_Voice"]["sessions"] == {"V1": 1}
    _run(body)


def test_detect_names_the_program_in_its_next_step_and_scopes_the_sessions():
    async def body(conn):
        p1, p2, _, _ = await _two_programs_one_persona(conn)
        res = await detect_questionnaire_versions("Parent_Voice")
        assert len(res) == 2 and {v["program_id"] for v in res.values()} == {p1["program_id"], p2["program_id"]}
        one = await detect_questionnaire_versions("Parent_Voice", program_id=p2["program_id"])
        assert list(one) == ["Parent_Voice"] and one["Parent_Voice"]["tenant_code"] == "mitra"
    _run(body)


def test_two_tenants_with_the_same_persona_key_are_never_mixed():
    async def body(conn):
        other = await _program(conn, "other", "1M Voice", "Youth")
        cfg_other = await _add_config(conn, other, "mitra", "Youth_Voice", _config(session_type="Youth_Voice"))
        mitra = {"program_id": YOUTH["program_id"], "leader_id": YOUTH["leader_category_id"], "tenant": "mitra",
                 "program_name": YOUTH["program_name"]}
        s_mitra = await _session(conn, mitra, "Youth_Voice", "mitra", "V1", _at(2026, 11, 5, 9, 0, 0))
        s_other = await _session(conn, other, "Youth_Voice", "mitra", "V1", _at(2026, 11, 6, 9, 0, 0))
        before = await _snapshot(conn)

        # B5 / C9: no tenant given and two tenants have the persona: refuse, write nothing
        with pytest.raises(AmbiguousFeedbackConfigError):
            await set_questionnaire_version("Youth_Voice", "V9", "2027-01-01T00:00:00+05:30", restamp=False)
        assert await _snapshot(conn) == before

        # the read side is scoped too: the counts of one tenant never include the other's sessions
        listed = await list_questionnaire_versions("Youth_Voice", tenant_code="mitra")
        assert listed["Youth_Voice"]["sessions"] == {"V1": 1} and listed["Youth_Voice"]["tenant_code"] == "mitra"
        both = await list_questionnaire_versions("Youth_Voice")
        assert {v["tenant_code"] for v in both.values()} == {"mitra", "other"}
        assert all(v["sessions"] == {"V1": 1} for v in both.values())

        res = await set_questionnaire_version("Youth_Voice", "V2", V2_TEXT, tenant_code="mitra")
        assert res["tenant_code"] == "mitra" and res["restamp"]["sessions"] == 1
        assert await _labels(conn) == {s_mitra: "V2", s_other: "V1"}
        assert (await _active_versions(conn, cfg_other))[0] == 1       # the other tenant's config was not written

        with pytest.raises(AmbiguousFeedbackConfigError):
            await restamp_questionnaire_versions("Youth_Voice")
        assert (await restamp_questionnaire_versions("Youth_Voice", tenant_code="other"))["changed"] == 0
    _run(body)


def test_a_program_with_only_a_custom_source_works_without_naming_it_and_two_sources_must_be_told_apart():
    async def body(conn):
        ptm = await _program(conn, "mitra", "PTM 2026", "Teacher")
        await _add_config(conn, ptm, "ptm_app", "Teacher_PTM", _config(session_type="Teacher_PTM"))
        s = await _session(conn, ptm, "Teacher_PTM", "ptm_app", "V1", _at(2026, 11, 5, 9, 0, 0))
        # no 'mitra' literal anywhere: the only versioned config of the persona is the ptm_app one
        res = await set_questionnaire_version("Teacher_PTM", "V2", V2_TEXT)
        assert res["source"] == "ptm_app" and res["restamp"]["changed"] == 1 and (await _labels(conn))[s] == "V2"

        # a second source for the same program and persona: now it must be named
        await _add_config(conn, ptm, "ptm_web", "Teacher_PTM", _config(session_type="Teacher_PTM"))
        w = await _session(conn, ptm, "Teacher_PTM", "ptm_web", "V1", _at(2026, 11, 5, 10, 0, 0))
        before = await _snapshot(conn)
        with pytest.raises(AmbiguousFeedbackConfigError, match="ptm_app"):
            await set_questionnaire_version("Teacher_PTM", "V3", "2026-12-01T00:00:00+00:00")
        assert await _snapshot(conn) == before
        res = await set_questionnaire_version("Teacher_PTM", "V2", V2_TEXT, source="ptm_web")
        assert res["source"] == "ptm_web"
        assert await _labels(conn) == {s: "V2", w: "V2"}             # each source relabelled by its own config
        listed = await list_questionnaire_versions("Teacher_PTM", source="ptm_web")
        assert list(listed) == ["Teacher_PTM"] and listed["Teacher_PTM"]["sessions"] == {"V2": 1}
        assert len(await list_questionnaire_versions("Teacher_PTM")) == 2
    _run(body)


def test_a_session_of_one_source_is_not_relabelled_by_another_sources_windows():
    async def body(conn):
        ptm = await _program(conn, "mitra", "PTM 2026", "Teacher")
        cfg_a = await _add_config(conn, ptm, "ptm_app", "Teacher_PTM", _config(session_type="Teacher_PTM"))
        await _add_config(conn, ptm, "ptm_web", "Teacher_PTM", _config(session_type="Teacher_PTM"))
        a = await _session(conn, ptm, "Teacher_PTM", "ptm_app", "V1", _at(2026, 11, 5, 9, 0, 0))
        w = await _session(conn, ptm, "Teacher_PTM", "ptm_web", "V1", _at(2026, 11, 5, 9, 0, 0))
        await set_questionnaire_version("Teacher_PTM", "V2", V2_TEXT, source="ptm_app")
        assert await _labels(conn) == {a: "V2", w: "V1"}
        assert (await restamp_questionnaire_versions())["changed"] == 0
        assert (await _active_versions(conn, cfg_a))[0] == 2
    _run(body)


# ------------------------------------------------------------------------------------- A8 / A10: show + migrate

def test_show_is_narrowed_by_the_selectors_and_names_the_program():
    async def body(conn):
        p1, p2, _, _ = await _two_programs_one_persona(conn)
        everything = (await list_feedback_config())["configs"]
        parents = [c for c in everything if c["session_type"] == "Parent_Voice" and c["source"] == "mitra"]
        assert {c["program_id"] for c in parents} == {p1["program_id"], p2["program_id"]}
        assert all(c["tenant_code"] == "mitra" and c["program_name"] is not None for c in everything)
        one = (await list_feedback_config("Parent_Voice", program_id=p2["program_id"], source="mitra"))["configs"]
        assert len(one) == 1 and one[0]["process_steps"] == STEP
        assert (await list_feedback_config(tenant_code="other"))["configs"] == []
        assert len((await list_feedback_config(program_name="PTM 2026"))["configs"]) == 1
    _run(body)


def test_migrate_derives_the_legacy_adapter_but_never_invents_one_for_a_new_source():
    async def body(conn):
        ptm = await _program(conn, "mitra", "PTM 2026", "Teacher")
        legacy = _config(session_type="Teacher_PTM")
        del legacy["adapter"]
        cfg_ptm = await _add_config(conn, ptm, "ptm_app", "Teacher_PTM", legacy)
        await conn.execute(
            "UPDATE config_version SET config = config - 'adapter' WHERE config_id = $1::uuid", PARENT["config_id"])
        res = await migrate_feedback_config()
        mitra_detail = [d for d in res["details"] if d["source"] == "mitra" and d["session_type"] == "Parent_Voice"]
        assert len(mitra_detail) == 1 and mitra_detail[0]["added"] == ["adapter"]
        assert (await _active_versions(conn, PARENT["config_id"]))[1]["adapter"] == "conversation_rows"
        # the new source has no legacy meaning: reported, left alone, no new version
        assert [i for i in res["invalid"] if i["source"] == "ptm_app" and i["key"] == "adapter"]
        assert not [d for d in res["details"] if d["source"] == "ptm_app"]
        version, config = await _active_versions(conn, cfg_ptm)
        assert version == 1 and "adapter" not in config
        assert (await migrate_feedback_config())["migrated"] == 0       # idempotent
    _run(body)
