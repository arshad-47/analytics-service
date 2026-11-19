"""
Real-Postgres tests of ONE config-lookup rule that the upload, the Kafka consumer, the process steps and the extraction
all share (reviewer finding on PR 36: the upload and the extraction used to find a config by different rules).

For a (tenant, source, session_type) the config of the session's OWN program wins; when the program has none, the
tenant's PROGRAM-LESS default config (program_id NULL) for that source and persona is used; a config of ANOTHER program
is never used; when more than one candidate is left at the winning level the lookup raises AmbiguousFeedbackConfigError.

The scenario: tenant 'mitra' has a program Alpha with a config of its own and a program Beta that has only the shared
default config; tenant 'other' has its own default and its own program. Synthetic data only. Opt-in like every PG test:
skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py makes a scratch database).
"""
import asyncio
import json
from datetime import datetime, timezone

import asyncpg
import pytest
import factories
import pg_support

from app.api.services import uploads as upload_service
from app.database.db import db
from app.database.feedback_extraction_operations import load_closing_keys, load_extraction_config
from app.database.feedback_operations import (
    AmbiguousFeedbackConfigError,
    detect_questionnaire_versions,
    get_active_feedback_config,
    get_feedback_process_steps,
    list_questionnaire_versions,
    restamp_questionnaire_versions,
    set_feedback_config_value,
    set_questionnaire_version,
)
from app.kafka.consumer import IngestionConsumer
from app.services.feedback_constants import FEEDBACK_METADATA_EXTRACTION_STEP
from seed_support import PERSONAS, extraction_config_v1

DSN = pg_support.SERVER_URL
pytestmark = pytest.mark.skipif(
    DSN is None or DSN == "", reason="FEEDBACK_TEST_DATABASE_URL not set (real Postgres required)")

SESSION_TYPE = "Parent_Voice"
SOURCE = "mitra"
STEP = [{"name": FEEDBACK_METADATA_EXTRACTION_STEP}]
V2_TEXT = "2026-11-03T00:00:00+00:00"
# a marker per config: the size cap tells which config a lookup returned
OWN_CAP, DEFAULT_CAP, OTHER_TENANT_CAP, OTHER_PROGRAM_CAP = 1_111, 2_222, 3_333, 4_444


def _run(body):
    """Runs `body(conn)` on the scratch database: schema, a clean slate, db.pool pointing at it."""
    async def main():
        conn = await asyncpg.connect(DSN)
        pool = None
        try:
            await conn.execute(pg_support.schema_sql())
            await conn.execute("DELETE FROM submissions")
            await conn.execute("DELETE FROM config_version")
            await conn.execute("DELETE FROM config")
            await conn.execute("DELETE FROM programs")
            await conn.execute("DELETE FROM leader_category")
            await conn.execute("DELETE FROM tenant")
            await conn.execute("INSERT INTO tenant (name, code) VALUES ('Mitra', 'mitra')")
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


def _at(*args):
    return datetime(*args, tzinfo=timezone.utc)


def _config(cap, session_type=SESSION_TYPE, **changes):
    return factories.upload_config(session_type=session_type, max_csv_upload_bytes=cap, **changes)


async def _add_default(conn, tenant, cap, session_type=SESSION_TYPE, source=SOURCE, config=None,
                       config_type="feedback_upload"):
    """A PROGRAM-LESS config (program_id NULL) of `tenant`; returns its id."""
    config_id = factories.uid()
    await conn.execute("INSERT INTO tenant (name, code) VALUES ($1, $1) ON CONFLICT (code) DO NOTHING", tenant)
    await conn.execute(
        "INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, session_type) "
        "VALUES ($1::uuid, $2, 'default', $3, NULL, $4, $5)", config_id, source, config_type, tenant, session_type)
    body = config if config is not None else factories.upload_config(
        session_type=session_type, max_csv_upload_bytes=cap)
    await conn.execute(
        "INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
        "VALUES ($1::uuid, 1, TRUE, $2::jsonb, 'test', 'test')", config_id, json.dumps(body))
    return config_id


async def _world(conn):
    """Alpha (own config, cap OWN_CAP), Beta (nothing of its own: only Alpha's config exists beside the default), the
    mitra default (DEFAULT_CAP); tenant 'other' with its own program Gamma and its own default (OTHER_TENANT_CAP)."""
    alpha = await factories.add_program(conn, "mitra", "Alpha", "Parent")
    beta = await factories.add_program(conn, "mitra", "Beta", "Parent")
    alpha_config = await factories.add_config(conn, alpha, SOURCE, SESSION_TYPE, _config(OWN_CAP))
    default_config = await _add_default(conn, "mitra", DEFAULT_CAP)
    gamma = await factories.add_program(conn, "other", "Gamma", "Parent")
    other_default = await _add_default(conn, "other", OTHER_TENANT_CAP)
    return {"alpha": alpha, "beta": beta, "gamma": gamma, "alpha_config": alpha_config,
            "default_config": default_config, "other_default": other_default}


def _cap(found):
    return None if found is None else found["config"]["max_csv_upload_bytes"]


# ------------------------------------------------------------------------------- get_active_feedback_config

def test_a_program_without_a_config_of_its_own_gets_the_tenants_default_and_the_own_config_wins():
    async def body(conn):
        w = await _world(conn)
        own = await get_active_feedback_config(w["alpha"]["program_id"], SOURCE, tenant_code="mitra")
        assert _cap(own) == OWN_CAP and own["config_id"] == w["alpha_config"]
        for kwargs in ({}, {"tenant_code": "mitra"}, {"tenant_code": "mitra", "session_type": SESSION_TYPE}):
            fallback = await get_active_feedback_config(w["beta"]["program_id"], SOURCE, **kwargs)
            assert _cap(fallback) == DEFAULT_CAP and fallback["config_id"] == w["default_config"], kwargs
            assert fallback["session_type"] == SESSION_TYPE
        # the own config wins whatever the session_type argument says
        assert _cap(await get_active_feedback_config(
            w["alpha"]["program_id"], SOURCE, tenant_code="mitra", session_type=SESSION_TYPE)) == OWN_CAP
    _run(body)


def test_a_config_of_another_program_is_never_picked():
    async def body(conn):
        alpha = await factories.add_program(conn, "mitra", "Alpha", "Parent")
        beta = await factories.add_program(conn, "mitra", "Beta", "Parent")
        await factories.add_config(conn, alpha, SOURCE, SESSION_TYPE, _config(OWN_CAP))
        # Alpha has a config, Beta has none and the tenant has no default: Beta gets nothing, never Alpha's
        assert await get_active_feedback_config(beta["program_id"], SOURCE, tenant_code="mitra") is None
        assert await get_active_feedback_config(beta["program_id"], SOURCE, tenant_code="mitra",
                                                session_type=SESSION_TYPE) is None
        # a default of ANOTHER source does not serve this source either
        await _add_default(conn, "mitra", DEFAULT_CAP, source="google_forms")
        assert await get_active_feedback_config(beta["program_id"], SOURCE, tenant_code="mitra") is None
        assert _cap(await get_active_feedback_config(beta["program_id"], "google_forms", tenant_code="mitra")) == DEFAULT_CAP
        # nor does a default of another persona when the persona is named
        assert await get_active_feedback_config(beta["program_id"], "google_forms", tenant_code="mitra",
                                                session_type="Youth_Voice") is None
    _run(body)


def test_the_default_of_another_tenant_is_never_used_with_or_without_a_tenant_argument():
    async def body(conn):
        w = await _world(conn)
        # Gamma (tenant 'other') gets the default of 'other'; Beta (tenant 'mitra') gets the one of 'mitra'
        assert _cap(await get_active_feedback_config(w["gamma"]["program_id"], SOURCE)) == OTHER_TENANT_CAP
        assert _cap(await get_active_feedback_config(w["gamma"]["program_id"], SOURCE, tenant_code="other")) == OTHER_TENANT_CAP
        assert _cap(await get_active_feedback_config(w["beta"]["program_id"], SOURCE)) == DEFAULT_CAP
        # a tenant that is not the program's tenant gets nothing (neither the program's own default nor its own)
        assert await get_active_feedback_config(w["gamma"]["program_id"], SOURCE, tenant_code="mitra") is None
        assert await get_active_feedback_config(w["beta"]["program_id"], SOURCE, tenant_code="other") is None
        # a tenant whose only config is the default of another tenant has none
        lonely = await factories.add_program(conn, "third", "Delta", "Parent")
        assert await get_active_feedback_config(lonely["program_id"], SOURCE) is None
        assert await get_active_feedback_config(factories.uid(), SOURCE) is None           # unknown program
    _run(body)


def test_ambiguity_is_still_raised_at_the_winning_level():
    async def body(conn):
        w = await _world(conn)
        # (1) the winning level is the default and it holds two personas: the persona must be named
        await _add_default(conn, "mitra", 5_555, session_type="Youth_Voice")
        with pytest.raises(AmbiguousFeedbackConfigError, match="Parent_Voice, Youth_Voice"):
            await get_active_feedback_config(w["beta"]["program_id"], SOURCE, tenant_code="mitra")
        assert _cap(await get_active_feedback_config(
            w["beta"]["program_id"], SOURCE, tenant_code="mitra", session_type="Youth_Voice")) == 5_555
        assert _cap(await get_active_feedback_config(
            w["beta"]["program_id"], SOURCE, tenant_code="mitra", session_type=SESSION_TYPE)) == DEFAULT_CAP
        # (2) the program has several session types of its own: still ambiguous although a default exists
        await factories.add_config(conn, w["alpha"], SOURCE, "Teacher_Voice", _config(6_666, "Teacher_Voice"))
        with pytest.raises(AmbiguousFeedbackConfigError, match="Parent_Voice, Teacher_Voice"):
            await get_active_feedback_config(w["alpha"]["program_id"], SOURCE, tenant_code="mitra")
        assert _cap(await get_active_feedback_config(
            w["alpha"]["program_id"], SOURCE, tenant_code="mitra", session_type="Teacher_Voice")) == 6_666
        # (3) the program's own level is decided by the program: with one own config (Alpha, Youth_Voice named) the
        # default of ANOTHER persona is not mixed in, and an own persona the program lacks falls to the default
        assert _cap(await get_active_feedback_config(
            w["alpha"]["program_id"], SOURCE, tenant_code="mitra", session_type="Youth_Voice")) == 5_555
    _run(body)


def test_a_config_without_an_active_version_does_not_hide_the_default():
    async def body(conn):
        w = await _world(conn)
        await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", w["alpha_config"])
        assert _cap(await get_active_feedback_config(w["alpha"]["program_id"], SOURCE, tenant_code="mitra")) == DEFAULT_CAP
    _run(body)


# ------------------------------------------------------------------------------- the consumer's schema lookup

def test_the_consumers_runtime_lookup_resolves_the_default_for_a_program_without_its_own_config():
    async def body(conn):
        w = await _world(conn)
        load = IngestionConsumer._load_feedback_runtime_config
        beta = await load(None, "mitra", w["beta"]["program_id"], SESSION_TYPE, SOURCE)
        alpha = await load(None, "mitra", w["alpha"]["program_id"], SESSION_TYPE, SOURCE)
        assert beta is not None and alpha is not None
        assert beta["config_version_id"] == str(await conn.fetchval(
            "SELECT id FROM config_version WHERE config_id = $1::uuid", w["default_config"]))
        assert alpha["config_version_id"] == str(await conn.fetchval(
            "SELECT id FROM config_version WHERE config_id = $1::uuid", w["alpha_config"]))
        # another tenant's program and another source: nothing
        assert await load(None, "mitra", w["gamma"]["program_id"], SESSION_TYPE, SOURCE) is None
        assert await load(None, "mitra", w["beta"]["program_id"], SESSION_TYPE, "google_forms") is None
    _run(body)


# ------------------------------------------------------------------------------- the upload target

def test_the_upload_target_resolves_the_default_for_a_program_without_a_config_and_says_so_when_none_exists():
    async def body(conn):
        w = await _world(conn)
        beta = await upload_service.load_feedback_target("Beta", "Parent", "mitra", SOURCE)
        assert beta["active"]["config_id"] == w["default_config"]
        assert beta["resolved"]["program"]["id"] == w["beta"]["program_id"]
        assert beta["max_csv_upload_bytes"] == DEFAULT_CAP
        alpha = await upload_service.load_feedback_target("Alpha", "Parent", "mitra", SOURCE)
        assert alpha["active"]["config_id"] == w["alpha_config"] and alpha["max_csv_upload_bytes"] == OWN_CAP
        # tenant isolation: Gamma of tenant 'other' uses the default of 'other'
        gamma = await upload_service.load_feedback_target("Gamma", "Parent", "other", SOURCE)
        assert gamma["max_csv_upload_bytes"] == OTHER_TENANT_CAP

        # nothing for this source: neither the program's own nor a default (the config of Alpha is not Beta's)
        with pytest.raises(upload_service.InvalidFeedbackFile) as raised:
            await upload_service.load_feedback_target("Beta", "Parent", "mitra", "google_forms")
        assert "no config for this program and no program-less default" in " ".join(raised.value.errors)
    _run(body)


def test_the_upload_target_is_a_409_when_the_winning_level_is_ambiguous():
    async def body(conn):
        w = await _world(conn)
        await _add_default(conn, "mitra", 5_555, session_type="Youth_Voice")        # two defaults, persona not named
        with pytest.raises(upload_service.AmbiguousFeedbackTarget):
            await upload_service.load_feedback_target("Beta", "Parent", "mitra", SOURCE)
        # the program with a config of its own is not affected by the ambiguity of the default
        assert (await upload_service.load_feedback_target("Alpha", "Parent", "mitra", SOURCE))["max_csv_upload_bytes"] == OWN_CAP
    _run(body)


# ------------------------------------------------------------------------------- the process steps

def test_process_steps_resolve_the_default_for_a_program_without_a_config_and_never_another_programs():
    async def body(conn):
        alpha = await factories.add_program(conn, "mitra", "Alpha", "Parent")
        beta = await factories.add_program(conn, "mitra", "Beta", "Parent")
        await factories.add_config(conn, alpha, SOURCE, SESSION_TYPE, _config(OWN_CAP, process_steps=STEP))
        s_alpha = await factories.store_session(conn, alpha, SESSION_TYPE, SOURCE, "V1", _at(2026, 11, 5, 9, 0, 0))
        s_beta = await factories.store_session(conn, beta, SESSION_TYPE, SOURCE, "V1", _at(2026, 11, 5, 9, 0, 0))

        # no default yet: Alpha's steps are never run for Beta, the error names the program and the missing default
        with pytest.raises(ValueError, match="No active mitra feedback_upload config .*" + beta["program_id"]
                           + ".*no program-less default"):
            await get_feedback_process_steps(conn, s_beta, "mitra")

        await _add_default(conn, "mitra", DEFAULT_CAP, config=_config(DEFAULT_CAP, process_steps=[]))
        memo = {}
        assert await get_feedback_process_steps(conn, s_beta, "mitra", memo) == []      # the default's steps
        assert await get_feedback_process_steps(conn, s_alpha, "mitra", memo) == STEP   # its own, not the default's
        assert len(memo) == 2                                                          # keyed by program

        # a session without a program (the program row was deleted) uses the default too
        await conn.execute("UPDATE submissions SET program_id = NULL WHERE submission_id = $1", s_beta)
        assert await get_feedback_process_steps(conn, s_beta, "mitra") == []
    _run(body)


def test_process_steps_of_another_tenants_default_are_never_used():
    async def body(conn):
        beta = await factories.add_program(conn, "mitra", "Beta", "Parent")
        await _add_default(conn, "other", OTHER_TENANT_CAP, config=_config(OTHER_TENANT_CAP, process_steps=STEP))
        s_beta = await factories.store_session(conn, beta, SESSION_TYPE, SOURCE, "V1", _at(2026, 11, 5, 9, 0, 0))
        with pytest.raises(ValueError, match="No active mitra feedback_upload config"):
            await get_feedback_process_steps(conn, s_beta, "mitra")
    _run(body)


# ------------------------------------------------------------------------------- extraction (existing behaviour)

def test_extraction_and_closing_keys_follow_the_same_rule():
    async def body(conn):
        w = await _world(conn)
        raw = extraction_config_v1("Parent")
        raw["session_type"] = SESSION_TYPE
        raw["llm"] = {"model": "default/model"}
        default_ext = await _add_default(conn, "mitra", 0, source="any", config=raw, config_type="feedback_extraction")
        alpha_raw = dict(raw, llm={"model": "alpha/model"})
        alpha_ext = factories.uid()
        await conn.execute(
            "INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, session_type) "
            "VALUES ($1::uuid, 'any', 'alpha', 'feedback_extraction', $2::uuid, 'mitra', $3)",
            alpha_ext, w["alpha"]["program_id"], SESSION_TYPE)
        await conn.execute("INSERT INTO config_version (config_id, version, is_active, config) "
                           "VALUES ($1::uuid, 1, TRUE, $2::jsonb)", alpha_ext, json.dumps(alpha_raw))
        beta_cfg = await load_extraction_config(conn, "mitra", SESSION_TYPE, w["beta"]["program_id"], SOURCE)
        alpha_cfg = await load_extraction_config(conn, "mitra", SESSION_TYPE, w["alpha"]["program_id"], SOURCE)
        assert str(beta_cfg["config_id"]) == default_ext      # no extraction config of its own: the default
        assert str(alpha_cfg["config_id"]) == alpha_ext       # its own wins
        # closing keys: the upload config of the same level (own, else default)
        await conn.execute("UPDATE config_version SET config = config || '{\"closing_keys\": [\"OWN_BYE\"]}'::jsonb "
                           "WHERE config_id = $1::uuid", w["alpha_config"])
        await conn.execute("UPDATE config_version SET config = config || '{\"closing_keys\": [\"DEFAULT_BYE\"]}'::jsonb "
                           "WHERE config_id = $1::uuid", w["default_config"])
        assert await load_closing_keys(conn, "mitra", SESSION_TYPE, w["alpha"]["program_id"], SOURCE) == ["OWN_BYE"]
        assert await load_closing_keys(conn, "mitra", SESSION_TYPE, w["beta"]["program_id"], SOURCE) == ["DEFAULT_BYE"]
        assert await load_closing_keys(conn, "other", SESSION_TYPE, w["beta"]["program_id"], SOURCE) == ["THANK_YOU", "THANKYOU", "CLOSING"]
    _run(body)


# ------------------------------------------------------------------------------- version windows

async def _labels(conn):
    return {r["submission_id"]: r["questionnaire_version"]
            for r in await conn.fetch("SELECT submission_id, questionnaire_version FROM feedback_submissions")}


async def _version_world(conn, alpha_versioned=True):
    """Alpha with its own config, Beta without, ONE default whose windows carry V2 from V2_TEXT; one session of each
    program, started after the window, both stored as V1."""
    alpha = await factories.add_program(conn, "mitra", "Alpha", "Parent")
    beta = await factories.add_program(conn, "mitra", "Beta", "Parent")
    alpha_config = _config(OWN_CAP)
    if not alpha_versioned:
        # no questionnaire windows and not a conversation export (the two things that make a source "versioned")
        del alpha_config["questionnaire_versions"]
        alpha_config["adapter"] = "wide_form"
    await factories.add_config(conn, alpha, SOURCE, SESSION_TYPE, alpha_config)
    default_config = _config(DEFAULT_CAP)
    default_config["questionnaire_versions"] = [{"key": "V1", "effective_from": None},
                                                {"key": "V2", "effective_from": V2_TEXT}]
    await _add_default(conn, "mitra", DEFAULT_CAP, config=default_config)
    s_alpha = await factories.store_session(conn, alpha, SESSION_TYPE, SOURCE, "V1", _at(2026, 11, 5, 9, 0, 0))
    s_beta = await factories.store_session(conn, beta, SESSION_TYPE, SOURCE, "V1", _at(2026, 11, 5, 9, 0, 0))
    return alpha, beta, s_alpha, s_beta


def test_the_defaults_windows_apply_to_the_sessions_of_programs_without_a_config_and_never_to_the_others():
    async def body(conn):
        alpha, beta, s_alpha, s_beta = await _version_world(conn)
        # sweep: each config relabels ITS sessions. Alpha has its own windows (V1 only): its session stays V1 although
        # the default has a V2 window; Beta has no config of its own, so the default's windows give it V2
        dry = await restamp_questionnaire_versions(dry_run=True)
        assert dry["changed"] == 1 and await _labels(conn) == {s_alpha: "V1", s_beta: "V1"}
        res = await restamp_questionnaire_versions()
        assert res["changed"] == 1 and await _labels(conn) == {s_alpha: "V1", s_beta: "V2"}
        assert (await restamp_questionnaire_versions())["changed"] == 0
        by_program = {v["program_id"]: v for v in res["session_types"].values()}
        assert by_program[None]["sessions"] == 1 and by_program[alpha["program_id"]]["sessions"] == 1
    _run(body)


def test_list_and_detect_count_only_the_sessions_each_config_governs():
    async def body(conn):
        alpha, beta, s_alpha, s_beta = await _version_world(conn)
        listed = await list_questionnaire_versions()
        by_program = {v["program_id"]: v for v in listed.values()}
        assert by_program[None]["sessions"] == {"V1": 1}                   # Beta's only, not Alpha's
        assert by_program[alpha["program_id"]]["sessions"] == {"V1": 1}    # Alpha's only
        # a second session of Alpha never shows under the default
        await factories.store_session(conn, alpha, SESSION_TYPE, SOURCE, "V1", _at(2026, 11, 6, 9, 0, 0))
        again = {v["program_id"]: v for v in (await list_questionnaire_versions()).values()}
        assert again[None]["sessions"] == {"V1": 1} and again[alpha["program_id"]]["sessions"] == {"V1": 2}
        found = {v["program_id"]: v for v in (await detect_questionnaire_versions()).values()}
        assert set(found) == {None, alpha["program_id"]}
    _run(body)


def test_set_on_the_defaults_windows_never_relabels_a_program_that_has_its_own_config():
    async def body(conn):
        # Alpha's own config has no questionnaire windows, so the default is the only candidate of `set`: it relabels
        # the sessions of Beta (no config of its own) and NOT the ones of Alpha (own config, even an unversioned one)
        alpha, beta, s_alpha, s_beta = await _version_world(conn, alpha_versioned=False)
        res = await set_questionnaire_version(SESSION_TYPE, "V3", "2026-11-04T00:00:00+00:00", tenant_code="mitra")
        assert res["program_id"] is None and res["changed"] is True
        assert res["restamp"]["sessions"] == 1 and res["restamp"]["changed"] == 1
        assert await _labels(conn) == {s_alpha: "V1", s_beta: "V3"}
        again = await restamp_questionnaire_versions()
        assert await _labels(conn) == {s_alpha: "V1", s_beta: "V3"} and again["changed"] == 0
    _run(body)


def test_set_refuses_to_pick_between_the_programs_own_config_and_the_default():
    async def body(conn):
        alpha, beta, s_alpha, s_beta = await _version_world(conn)
        before = await _labels(conn)
        with pytest.raises(AmbiguousFeedbackConfigError):
            await set_questionnaire_version(SESSION_TYPE, "V3", "2026-11-04T00:00:00+00:00", tenant_code="mitra")
        # a setting is not a fallback either: it acts on the one row it was told (Alpha's), never on the default
        res = await set_feedback_config_value(SESSION_TYPE, SOURCE, "max_csv_upload_bytes", 9_999,
                                              tenant_code="mitra", program_id=alpha["program_id"])
        assert res["program_id"] == alpha["program_id"]
        assert _cap(await get_active_feedback_config(alpha["program_id"], SOURCE, tenant_code="mitra")) == 9_999
        assert _cap(await get_active_feedback_config(beta["program_id"], SOURCE, tenant_code="mitra")) == DEFAULT_CAP
        assert await _labels(conn) == before
        # naming the program that has no config of its own touches nothing (there is no row of Beta to edit)
        with pytest.raises(ValueError, match="no mitra upload config"):
            await set_feedback_config_value(SESSION_TYPE, SOURCE, "max_csv_upload_bytes", 1,
                                            tenant_code="mitra", program_id=beta["program_id"])
    _run(body)


def test_a_default_never_covers_the_sessions_of_another_tenant_or_a_program_with_a_config_of_its_own_source():
    async def body(conn):
        alpha, beta, s_alpha, s_beta = await _version_world(conn)
        # a session of Beta from ANOTHER source has no config here at all: the default of 'mitra'/'mitra' is not its
        other_source = await factories.store_session(conn, beta, SESSION_TYPE, "ptm_app", "V1", _at(2026, 11, 5, 9, 0, 0))
        # a session of tenant 'other' (a program of its own, no config of its own) is not covered by mitra's default
        gamma = await factories.add_program(conn, "other", "Gamma", "Parent")
        s_gamma = await factories.store_session(conn, gamma, SESSION_TYPE, SOURCE, "V1", _at(2026, 11, 5, 9, 0, 0))
        await restamp_questionnaire_versions()
        labels = await _labels(conn)
        assert labels[s_beta] == "V2" and labels[other_source] == "V1" and labels[s_gamma] == "V1"
    _run(body)
