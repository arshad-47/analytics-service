"""
Tests for the `feedback` report type on POST /v1/upload/ (slice 1e + 2d): file-level checks in
handle_upload (Mitra and Google Forms), session-level processing + DLQ in process_csv_inline, size cap,
`source` validation, and (opt-in) the real-Postgres lookups in app/database/feedback_operations.py.
The delta check (new / unchanged / updated sessions, already stored responses) is in
tests/test_feedback_delta.py.

Everything is in-memory fakes (same conventions as tests/unit_testing.py) except the last section,
which is skipped unless FEEDBACK_TEST_DATABASE_URL names a Postgres server (tests/pg_support.py creates a scratch
database for this file; the section applies schema.sql itself).
"""
import asyncio
import csv
import io
import json
import uuid
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
import pg_support
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.exceptions import register_exception_handlers
from app.api.router import api_router
import app.api.services.uploads as uploads_service_module
import app.database.feedback_operations as feedback_ops
import app.database.operations as operations_module
from app.config import settings
from app.services.feedback_adapter_config import COLUMN_ROLES_REQUIRED
from seed_support import FEEDBACK_PROGRAM_NAME, PERSONAS, gforms_config_v1, mitra_config_v1
from tests.conftest import FakeConn, install_fake_db, settings_override
from feedback_test_windows import config_with_v2

YOUTH = PERSONAS["Youth"]
PROGRAM = {"id": YOUTH["program_id"], "name": FEEDBACK_PROGRAM_NAME}
LEADER = {"id": YOUTH["leader_category_id"], "name": "Youth"}
RESOLVED = {"program": PROGRAM, "leader": LEADER, "tenant_code": "mitra"}


def _resolved(persona: str) -> dict:
    p = PERSONAS[persona]
    return {"program": {"id": p["program_id"], "name": FEEDBACK_PROGRAM_NAME},
            "leader": {"id": p["leader_category_id"], "name": persona}, "tenant_code": "mitra"}


def _active(persona: str, version_id: str = "11111111-1111-1111-1111-111111111111", *, baseline: bool = False) -> dict:
    """
    What get_active_feedback_config(program_id, "mitra") returns for a persona. The seeded config only has the V1
    baseline, so by default the stub carries the test-owned V2 window (tests/feedback_test_windows.py) that the
    stamping tests need; `baseline=True` returns the config exactly as seeded (everything is V1).
    """
    p = PERSONAS[persona]
    return {
        "config_id": p["config_id"],
        "config_version_id": version_id,
        "version": 1,
        "session_type": p["session_type"],
        "config": mitra_config_v1(persona) if baseline else config_with_v2(persona),
    }


ACTIVE_CONFIG = _active("Youth")
YOUTH_TYPE = YOUTH["session_type"]  # Youth_Voice

HEADER = ("bot_id,bot_name,session_type,session_status,session,submission_id,language,sequence,conversation_row_id,"
          "message,translated_message,created_at,updated_at,stage,receiver_id,sender_id,audio_file,file_url,other_params")
TS = "2026-10-15 08:54:43+05:30"
# Sessions before / after the test V2 window of the stub config (see _active): they get V1 / V2.
BEFORE_V2 = "2026-10-15 08:54:43+05:30"
AFTER_V2 = "2026-11-10 09:00:00+05:30"


def _row(bot, sess, sid, seq, msg, tr, stage, sender, status="COMPLETED", lang="en", session_type=YOUTH_TYPE,
         ts=TS, bot_name="Youth"):
    return (f"{bot},{bot_name},{session_type},{status},{sess},{sid},{lang},{seq},{seq},{msg},{tr},{ts},{ts},"
            f"{stage},1,{sender},,,")


def _session(sess, sid, bot="90", status="COMPLETED", answer="17", session_type=YOUTH_TYPE, ts=TS):
    """OLD shape (no opening row): 4 CSV rows, stored as 5 messages (first question synthesised)."""
    kw = {"session_type": session_type, "ts": ts}
    return [
        _row(bot, sess, sid, 1, answer, "", "AGE_IDENTIFICATION", 120, status, **kw),
        _row(bot, sess, sid, 2, "Which language do you speak?", "Which language do you speak?", "LANG_Q", 1, status, **kw),
        _row(bot, sess, sid, 3, "English", "", "LANG_Q", 120, status, **kw),
        _row(bot, sess, sid, 4, "Thanks", "Thanks", "THANK_YOU", 1, status, **kw),
    ]


def _session_new_shape(sess, sid, bot="90", status="COMPLETED", answer="17", session_type=YOUTH_TYPE, ts=TS,
                       extra_rows: int = 0):
    """
    NEW shape: the opening row sits at sequence 1 with blank sender_id / stage / created_at; the first real
    message is sequence 2. 5 CSV rows -> 5 stored messages at the CSV sequences 1..5. `extra_rows` appends
    more respondent / bot pairs after the closing row's position (used to make a session "grow").
    """
    kw = {"session_type": session_type, "ts": ts}
    rows = [
        _row(bot, sess, sid, 1, "Welcome to the survey", "Welcome to the survey", "", "", status,
             session_type=session_type, ts=""),
        _row(bot, sess, sid, 2, answer, "", "AGE_IDENTIFICATION", 120, status, **kw),
        _row(bot, sess, sid, 3, "Which language do you speak?", "Which language do you speak?", "LANG_Q", 1, status, **kw),
        _row(bot, sess, sid, 4, "English", "", "LANG_Q", 120, status, **kw),
        _row(bot, sess, sid, 5, "Thanks", "Thanks", "THANK_YOU", 1, status, **kw),
    ]
    seq = 6
    for n in range(extra_rows):
        rows.append(_row(bot, sess, sid, seq, f"Extra question {n}", "", "EXTRA_Q", 1, status, **kw))
        rows.append(_row(bot, sess, sid, seq + 1, f"extra answer {n}", "", "EXTRA_Q", 120, status, **kw))
        seq += 2
    return rows


GFORMS_ACTIVE_CONFIG = {
    "config_id": YOUTH["gforms_config_id"],
    "config_version_id": "22222222-2222-2222-2222-222222222222",
    "version": 3,
    "session_type": YOUTH_TYPE,
    "config": gforms_config_v1("Youth"),
}


def _gforms_headers(persona: str) -> List[str]:
    """The headers of the persona's (single) configured Google Forms form, as exported."""
    form = next(iter(gforms_config_v1(persona)["forms"].values()))
    return [q["headers"][0] for q in form["questions"]]


def _gforms_csv(persona: str = "Youth", responses: int = 2, headers: Optional[List[str]] = None,
                timestamp_header: Optional[str] = "Timestamp", first_second: int = 0) -> bytes:
    """An ORIGINAL-format wide Google Forms CSV: Timestamp + one column per question, synthetic answers."""
    question_headers = headers if headers is not None else _gforms_headers(persona)
    header = ([timestamp_header] if timestamp_header is not None else []) + question_headers
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    for n in range(responses):
        stamp = f"10/2/2026 10:09:{first_second + n:02d}"
        row = [stamp] if timestamp_header is not None else []
        row += [f"answer {n} {i}" for i in range(len(question_headers))]
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def _csv(rows: List[str], header: str = HEADER) -> bytes:
    return ("\n".join([header] + rows) + "\n").encode("utf-8")


GOOD_FILE = _csv(_session("a" * 32, "1001") + _session("b" * 32, "1002"))


def _client() -> TestClient:
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(api_router)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def client():
    return _client()


def _headers():
    return {"Authorization": f"Bearer {settings.AUTH_TOKEN}"}


def _form(**overrides):
    data = {"report_type": "feedback", "program_name": FEEDBACK_PROGRAM_NAME, "leader_category": "Youth", "tenant_code": "mitra",
            "source": "mitra"}
    data.update(overrides)
    return data


def _files(content: bytes = GOOD_FILE, name: str = "youth.csv"):
    return {"file": (name, content, "text/csv")}


class Stores:
    """Handles on every side-effecting call a rejected upload must NOT make."""

    def __init__(self, insert, upload, lookup, config):
        self.insert, self.upload, self.lookup, self.config = insert, upload, lookup, config


def _patch_upload_pipeline(monkeypatch, resolved: Optional[dict] = RESOLVED, active: Optional[dict] = ACTIVE_CONFIG) -> Stores:
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    insert = AsyncMock(return_value=7)
    monkeypatch.setattr(operations_module, "insert_upload_record", insert)
    # claim returns None -> handle_upload leaves the record 'pending' and schedules no background task
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value=None))
    upload = MagicMock(return_value="path/to/youth.csv")
    monkeypatch.setattr(uploads_service_module, "upload_csv", upload)
    lookup = AsyncMock(return_value=resolved)
    config = AsyncMock(return_value=active)
    monkeypatch.setattr(feedback_ops, "get_program_and_leader", lookup)
    monkeypatch.setattr(feedback_ops, "get_active_feedback_config", config)
    return Stores(insert, upload, lookup, config)


def _assert_nothing_stored(stores: Stores):
    stores.upload.assert_not_called()
    stores.insert.assert_not_called()


# FILE LEVEL (handle_upload)
def test_feedback_good_persona_file_accepted(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"message": "Successfully uploaded to cloud", "id": 7, "status": "pending"}

    stores.lookup.assert_awaited_once_with(FEEDBACK_PROGRAM_NAME, "Youth", "mitra")
    stores.config.assert_awaited_once_with(PROGRAM["id"], "mitra", tenant_code="mitra")  # (program_id, config_source): no bot id
    stores.upload.assert_called_once()
    kwargs = stores.insert.await_args.kwargs
    assert kwargs["report_type"] == "feedback"
    meta = kwargs["meta_data"]
    assert meta["source"] == "mitra"
    assert meta["persona"] == "Youth"
    assert meta["session_type"] == "Youth_Voice"
    assert meta["bot_ids"] == ["90"]
    assert "bot_id" not in meta
    assert meta["config_version_id"] == ACTIVE_CONFIG["config_version_id"]
    assert meta["config_version"] == 1
    assert meta["tenant_code"] == "mitra"


def test_feedback_background_task_gets_parsed_string_dataframe(monkeypatch):
    """handle_upload hands the dtype=str frame to the background task ("NA" stays "NA")."""
    _patch_upload_pipeline(monkeypatch)
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value="success"))
    rows = _session("a" * 32, "1001", answer="NA")
    tasks = MagicMock()

    async def run():
        return await uploads_service_module.handle_upload(
            "feedback", FEEDBACK_PROGRAM_NAME, "Youth", "mitra", "na.csv", _csv(rows), tasks, source="mitra")

    out = asyncio.run(run())
    assert out["status"] == "in_progress"
    fn, record_id, file_bytes, df = tasks.add_task.call_args.args
    assert fn is uploads_service_module.process_csv_inline
    assert df.loc[0, "message"] == "NA"


def test_feedback_unknown_program_or_leader_rejected_nothing_stored(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, resolved=None)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(leader_category="Nobody"), files=_files())
    assert resp.status_code == 404
    assert "Nobody" in resp.json()["detail"]
    stores.config.assert_not_called()  # never fabricated, never continued
    _assert_nothing_stored(stores)


def test_feedback_wrong_persona_session_type_rejected_whole(client, monkeypatch):
    # Youth_Voice rows uploaded as Parent: the Parent config only allows Parent_Voice.
    stores = _patch_upload_pipeline(monkeypatch, resolved=_resolved("Parent"), active=_active("Parent"))
    youth_rows = _csv(_session("a" * 32, "1001") + _session("b" * 32, "1002"))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(leader_category="Parent"),
                       files=_files(youth_rows))
    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        "8 row(s) carry a session_type other than Parent_Voice (1 other value(s)); only session_type Parent_Voice is "
        "allowed for this upload"]
    stores.config.assert_awaited_once_with(PERSONAS["Parent"]["program_id"], "mitra", tenant_code="mitra")
    _assert_nothing_stored(stores)


def test_feedback_file_mixing_two_personas_is_rejected_whole(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    mixed = _csv(_session("a" * 32, "1001") + _session("b" * 32, "1002", session_type="Parent_Voice"))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(mixed))
    assert resp.status_code == 400
    (error,) = resp.json()["errors"]
    assert "Parent_Voice" not in error and "only session_type Youth_Voice is allowed" in error
    _assert_nothing_stored(stores)


def test_feedback_file_with_several_bot_ids_is_accepted_and_all_recorded(client, monkeypatch):
    # A persona runs several bots over time: one Mitra file may hold a V1 bot and a V2 bot.
    stores = _patch_upload_pipeline(monkeypatch)
    several = _csv(_session("a" * 32, "1001", bot="90") + _session("b" * 32, "1002", bot="91")
                   + _session("c" * 32, "1003", bot="90"))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(several))
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"message": "Successfully uploaded to cloud", "id": 7, "status": "pending"}
    meta = stores.insert.await_args.kwargs["meta_data"]
    assert meta["session_type"] == "Youth_Voice"
    assert meta["bot_ids"] == ["90", "91"]  # distinct, sorted, informational
    stores.config.assert_awaited_once_with(PROGRAM["id"], "mitra", tenant_code="mitra")  # the bot id never picks the config


def test_feedback_file_without_bot_id_column_is_accepted(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    header = HEADER.replace("bot_id,", "", 1)
    rows = [r.split(",", 1)[1] for r in _session("a" * 32, "1001")]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows, header)))
    assert resp.status_code == 200, resp.text
    meta = stores.insert.await_args.kwargs["meta_data"]
    assert meta["session_type"] == "Youth_Voice"
    assert "bot_ids" not in meta and "bot_id" not in meta  # nothing to record
    stores.upload.assert_called_once()


def test_feedback_blank_bot_id_on_every_row_is_accepted(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    rows = [r.replace("90,Youth,", ",Youth,", 1) for r in _session("a" * 32, "1001")]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows)))
    assert resp.status_code == 200, resp.text
    assert "bot_ids" not in stores.insert.await_args.kwargs["meta_data"]


def test_feedback_file_without_session_type_column_rejected_nothing_stored(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    header = HEADER.replace("session_type,", "", 1)
    rows = [",".join(v for i, v in enumerate(r.split(",")) if i != 2) for r in _session("a" * 32, "1001")]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows, header)))
    assert resp.status_code == 400
    assert resp.json()["errors"] == ["Missing required column(s): session_type"]
    _assert_nothing_stored(stores)


def test_feedback_blank_session_type_on_some_rows_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    rows = _session("a" * 32, "1001") + _session("b" * 32, "1002")
    rows[1] = rows[1].replace(",Youth_Voice,", ",,", 1)
    rows[5] = rows[5].replace(",Youth_Voice,", ",,", 1)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows)))
    assert resp.status_code == 400
    assert resp.json()["errors"] == ["session_type is blank in 2 row(s); every row must carry session_type Youth_Voice"]
    _assert_nothing_stored(stores)


def test_feedback_educator_upload_resolves_its_own_config(client, monkeypatch):
    edu = PERSONAS["Educator"]
    stores = _patch_upload_pipeline(monkeypatch, resolved=_resolved("Educator"), active=_active("Educator"))
    rows = _session("a" * 32, "1001", bot="110", session_type="Educator_Voice")
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(leader_category="Educator"),
                       files=_files(_csv(rows)))
    assert resp.status_code == 200, resp.text
    stores.lookup.assert_awaited_once_with(FEEDBACK_PROGRAM_NAME, "Educator", "mitra")
    stores.config.assert_awaited_once_with(edu["program_id"], "mitra", tenant_code="mitra")
    meta = stores.insert.await_args.kwargs["meta_data"]
    assert meta["persona"] == "Educator"
    assert meta["session_type"] == "Educator_Voice"
    assert meta["bot_ids"] == ["110"]
    assert meta["config_id"] == edu["config_id"]


def test_feedback_educator_rows_uploaded_as_youth_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)  # Youth config
    rows = _session("a" * 32, "1001", bot="110", session_type="Educator_Voice")
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows)))
    assert resp.status_code == 400
    assert len(resp.json()["errors"]) == 1
    assert "Educator_Voice" not in resp.json()["errors"][0] and "only session_type Youth_Voice is allowed" in resp.json()["errors"][0]
    _assert_nothing_stored(stores)


def test_feedback_persona_without_mitra_config_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, resolved=_resolved("Educator"), active=None)
    rows = _session("a" * 32, "1001", bot="110", session_type="Educator_Voice")
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(leader_category="Educator"),
                       files=_files(_csv(rows)))
    assert resp.status_code == 400
    assert resp.json()["errors"] == ["No upload config for source 'mitra' is set up for Educator: no config for this program and no program-less default"]
    _assert_nothing_stored(stores)


def test_feedback_new_shape_file_passes_the_file_check(client, monkeypatch):
    """The opening row (blank sender_id / stage / created_at) is no file-level problem."""
    stores = _patch_upload_pipeline(monkeypatch)
    new_shape = _csv(_session_new_shape("a" * 32, "1001") + _session_new_shape("b" * 32, "1002", bot="91"))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(new_shape))
    assert resp.status_code == 200, resp.text
    assert stores.insert.await_args.kwargs["meta_data"]["bot_ids"] == ["90", "91"]
    stores.upload.assert_called_once()


def test_feedback_missing_required_column_rejected_nothing_stored(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    cols = HEADER.split(",")
    idx = cols.index("session_status")
    header = ",".join(c for c in cols if c != "session_status")
    rows = [",".join(v for i, v in enumerate(r.split(",")) if i != idx) for r in _session("a" * 32, "1001")]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_csv(rows, header)))
    assert resp.status_code == 400
    assert any("session_status" in e for e in resp.json()["errors"])
    _assert_nothing_stored(stores)


def test_feedback_unparseable_file_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(HEADER.encode() + b"\n"))
    assert resp.status_code == 400
    assert resp.json()["errors"][0].startswith("Failed to parse CSV")
    _assert_nothing_stored(stores)


def test_feedback_does_not_use_story_column_validation(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    spy = MagicMock(return_value=(False, ["story schema"]))
    monkeypatch.setattr(uploads_service_module, "validate_columns", spy)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 200
    spy.assert_not_called()


def test_feedback_duplicate_file_still_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=True))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 400
    assert "ALREADY EXISTS" in resp.json()["detail"]
    stores.lookup.assert_awaited_once()  # the route resolves the config first (its size cap is part of it)
    _assert_nothing_stored(stores)


# `source` and size cap
@pytest.mark.parametrize("bad", ["WhatsApp App", "ptm-app", "x" * 41, "any", "a\nb"])
def test_feedback_unsafe_source_label_rejected_before_any_lookup(client, monkeypatch, bad):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source=bad), files=_files())
    assert resp.status_code == 400 and resp.json()["detail"].startswith("Invalid source")
    stores.lookup.assert_not_called()
    _assert_nothing_stored(stores)


def test_feedback_unknown_but_safe_source_is_refused_only_for_lack_of_a_config(client, monkeypatch):
    # "whatsapp" is a perfectly good LABEL: no list in code decides; the missing upload config does
    stores = _patch_upload_pipeline(monkeypatch, active=None)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="whatsapp"), files=_files())
    assert resp.status_code == 400 and resp.json()["errors"] == [
        "No upload config for source 'whatsapp' is set up for Youth: no config for this program and no program-less default"]
    stores.config.assert_awaited_once_with(PROGRAM["id"], "whatsapp", tenant_code="mitra")
    _assert_nothing_stored(stores)


@pytest.mark.parametrize("missing", ["source", "tenant_code"])
def test_feedback_requires_tenant_code_and_source_no_defaults(client, monkeypatch, missing):
    stores = _patch_upload_pipeline(monkeypatch)
    data = _form()
    del data[missing]
    resp = client.post("/v1/upload/", headers=_headers(), data=data, files=_files())
    assert resp.status_code == 422
    assert resp.json()["detail"][0]["loc"] == ["body", missing]
    stores.lookup.assert_not_called()
    _assert_nothing_stored(stores)


@pytest.mark.parametrize("missing", ["source", "tenant_code"])
def test_feedback_blank_tenant_code_or_source_is_missing_too(client, monkeypatch, missing):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(**{missing: "  "}), files=_files())
    assert resp.status_code == 422
    stores.lookup.assert_not_called()


def test_feedback_unsafe_tenant_code_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(tenant_code="mitra\nFORGED"), files=_files())
    assert resp.status_code == 400 and "FORGED" not in resp.text
    stores.lookup.assert_not_called()


def test_feedback_source_defaults_to_mitra_and_is_case_insensitive(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="MITRA"), files=_files())
    assert resp.status_code == 200
    assert stores.insert.await_args.kwargs["meta_data"]["source"] == "mitra"


# GOOGLE FORMS FILE LEVEL (the original wide CSV, checked by the adapter against the persona's config)
def test_gforms_good_persona_file_accepted(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"), files=_files(_gforms_csv()))
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"message": "Successfully uploaded to cloud", "id": 7, "status": "pending"}

    stores.lookup.assert_awaited_once_with(FEEDBACK_PROGRAM_NAME, "Youth", "mitra")
    stores.config.assert_awaited_once_with(PROGRAM["id"], "google_forms", tenant_code="mitra")
    stores.upload.assert_called_once()
    meta = stores.insert.await_args.kwargs["meta_data"]
    assert meta["source"] == "google_forms"
    assert meta["persona"] == "Youth"
    assert meta["config_id"] == YOUTH["gforms_config_id"]
    assert meta["config_version_id"] == GFORMS_ACTIVE_CONFIG["config_version_id"]
    assert meta["config_version"] == 3
    assert meta["tenant_code"] == "mitra"
    assert meta["session_type"] == "Youth_Voice"
    assert "bot_id" not in meta and "bot_ids" not in meta


def test_gforms_background_task_gets_parsed_wide_dataframe(monkeypatch):
    _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value="success"))
    tasks = MagicMock()

    async def run():
        return await uploads_service_module.handle_upload(
            "feedback", FEEDBACK_PROGRAM_NAME, "Youth", "mitra", "forms.csv", _gforms_csv(), tasks, source="google_forms")

    assert asyncio.run(run())["status"] == "in_progress"
    fn, record_id, file_bytes, df = tasks.add_task.call_args.args
    assert fn is uploads_service_module.process_csv_inline
    assert "Timestamp" in df.columns and len(df) == 2


def test_gforms_wrong_persona_form_rejected_listing_unmapped_headers(client, monkeypatch):
    # a Parents form uploaded under Youth: the Youth config does not know its headers
    stores = _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"),
                       files=_files(_gforms_csv("Parent")))
    assert resp.status_code == 400
    errors = " | ".join(resp.json()["errors"])
    assert "Unmapped column(s)" in errors
    assert "Which State do you stay in?" in errors  # a Parents-only header is named
    _assert_nothing_stored(stores)


def test_gforms_unmapped_header_rejects_whole_file(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    headers = _gforms_headers("Youth") + ["Favourite colour"]
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"),
                       files=_files(_gforms_csv(headers=headers)))
    assert resp.status_code == 400
    assert any("Favourite colour" in e for e in resp.json()["errors"])
    _assert_nothing_stored(stores)


def test_gforms_missing_timestamp_column_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"),
                       files=_files(_gforms_csv(timestamp_header=None)))
    assert resp.status_code == 400
    assert any("Timestamp" in e for e in resp.json()["errors"])
    _assert_nothing_stored(stores)


def test_gforms_source_with_mitra_shaped_file_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"), files=_files())  # Mitra CSV
    assert resp.status_code == 400
    errors = " | ".join(resp.json()["errors"])
    assert "Timestamp" in errors and "bot_id" in errors  # the Mitra columns are unmapped
    _assert_nothing_stored(stores)


def test_mitra_source_with_gforms_shaped_file_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="mitra"), files=_files(_gforms_csv()))
    assert resp.status_code == 400
    # a wide Google Forms export has none of the Mitra columns (the active config is looked up first)
    (error,) = resp.json()["errors"]
    assert error == "Missing required column(s): " + ", ".join(COLUMN_ROLES_REQUIRED)
    _assert_nothing_stored(stores)


def test_gforms_no_config_for_persona_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=None)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"), files=_files(_gforms_csv()))
    assert resp.status_code == 400
    assert resp.json()["errors"] == ["No upload config for source 'google_forms' is set up for Youth: no config for this program and no program-less default"]
    _assert_nothing_stored(stores)


def test_gforms_educator_has_no_google_forms_config(client, monkeypatch):
    # The Educator persona has a Mitra config only; the seed creates no Google Forms row for it.
    assert PERSONAS["Educator"]["gforms_config_id"] is None
    stores = _patch_upload_pipeline(monkeypatch, resolved=_resolved("Educator"), active=None)
    resp = client.post("/v1/upload/", headers=_headers(),
                       data=_form(source="google_forms", leader_category="Educator"), files=_files(_gforms_csv()))
    assert resp.status_code == 400
    assert resp.json()["errors"] == ["No upload config for source 'google_forms' is set up for Educator: no config for this program and no program-less default"]
    stores.config.assert_awaited_once_with(PERSONAS["Educator"]["program_id"], "google_forms", tenant_code="mitra")
    _assert_nothing_stored(stores)


def test_gforms_parent_upload_still_works_with_its_own_config(client, monkeypatch):
    parent = PERSONAS["Parent"]
    active = {"config_id": parent["gforms_config_id"], "config_version_id": "33333333-3333-3333-3333-333333333333",
              "version": 1, "session_type": parent["session_type"], "config": gforms_config_v1("Parent")}
    stores = _patch_upload_pipeline(monkeypatch, resolved=_resolved("Parent"), active=active)
    resp = client.post("/v1/upload/", headers=_headers(),
                       data=_form(source="google_forms", leader_category="Parent"), files=_files(_gforms_csv("Parent")))
    assert resp.status_code == 200, resp.text
    stores.config.assert_awaited_once_with(parent["program_id"], "google_forms", tenant_code="mitra")
    meta = stores.insert.await_args.kwargs["meta_data"]
    assert (meta["source"], meta["persona"], meta["session_type"]) == ("google_forms", "Parent", "Parent_Voice")


def test_gforms_unknown_program_or_leader_rejected_never_fabricated(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, resolved=None, active=GFORMS_ACTIVE_CONFIG)
    resp = client.post("/v1/upload/", headers=_headers(),
                       data=_form(source="google_forms", leader_category="Nobody"), files=_files(_gforms_csv()))
    assert resp.status_code == 404
    stores.config.assert_not_called()
    _assert_nothing_stored(stores)


def test_gforms_unparseable_file_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    headers_only = (",".join(["Timestamp"] + _gforms_headers("Youth")) + "\n").encode()
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"), files=_files(headers_only))
    assert resp.status_code == 400
    assert resp.json()["errors"][0].startswith("Failed to parse CSV")
    _assert_nothing_stored(stores)


def test_gforms_duplicate_file_still_rejected(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=GFORMS_ACTIVE_CONFIG)
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=True))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"), files=_files(_gforms_csv()))
    assert resp.status_code == 400 and "ALREADY EXISTS" in resp.json()["detail"]
    stores.lookup.assert_awaited_once()
    _assert_nothing_stored(stores)


def test_source_field_ignored_for_story(client, monkeypatch):
    """story/discussion do not look at `source` (an unknown value must not change their behaviour)."""
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=True))
    resp = client.post(
        "/v1/upload/", headers=_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "source": "whatsapp"},
        files={"file": ("s.csv", b"id,Title\n1,x\n", "text/csv")},
    )
    assert resp.status_code == 400
    assert "ALREADY EXISTS" in resp.json()["detail"]  # got past request validation to the service


def _active_with(**overrides) -> dict:
    """The stub Youth config with some per-bot settings replaced (None removes the key)."""
    active = _active("Youth")
    config = dict(active["config"])
    for key, value in overrides.items():
        if value is None:
            config.pop(key, None)
        else:
            config[key] = value
    return {**active, "config": config}


def test_feedback_size_cap_comes_from_the_config_story_keeps_its_own(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=_active_with(max_csv_upload_bytes=len(GOOD_FILE) + 1))
    settings_override(monkeypatch, settings, MAX_CSV_UPLOAD_BYTES=10)
    # feedback file is far above the 10-byte story cap but under the config's cap -> accepted
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 200, resp.text
    # the config was loaded ONCE for the upload (the route hands it to the service)
    stores.lookup.assert_awaited_once_with(FEEDBACK_PROGRAM_NAME, "Youth", "mitra")
    stores.config.assert_awaited_once_with(PROGRAM["id"], "mitra", tenant_code="mitra")

    # story with the same payload size is rejected by the story cap
    resp = client.post(
        "/v1/upload/", headers=_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L"},
        files={"file": ("s.csv", GOOD_FILE, "text/csv")},
    )
    assert resp.status_code == 413


def test_feedback_file_over_the_config_cap_is_413_and_nothing_stored(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch, active=_active_with(max_csv_upload_bytes=len(GOOD_FILE) - 1))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 413
    assert resp.json() == {"detail": "Uploaded file is too large"}  # same status and message as before
    _assert_nothing_stored(stores)


def test_feedback_cap_is_per_persona_config(client, monkeypatch):
    """Two personas with different caps: the same file passes under one and is 413 under the other."""
    small = _active_with(max_csv_upload_bytes=len(GOOD_FILE) - 1)
    stores = _patch_upload_pipeline(monkeypatch, active=small)
    assert client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files()).status_code == 413
    stores.config.return_value = _active_with(max_csv_upload_bytes=len(GOOD_FILE))
    assert client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files()).status_code == 200


def test_feedback_cap_applies_to_google_forms_too(client, monkeypatch):
    gforms = {**GFORMS_ACTIVE_CONFIG, "config": {**GFORMS_ACTIVE_CONFIG["config"], "max_csv_upload_bytes": 10}}
    stores = _patch_upload_pipeline(monkeypatch, active=gforms)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(source="google_forms"),
                       files=_files(_gforms_csv()))
    assert resp.status_code == 413
    _assert_nothing_stored(stores)


@pytest.mark.parametrize("override,expected", [
    ({"max_csv_upload_bytes": None}, "max_csv_upload_bytes"),
    ({"max_csv_upload_bytes": 0}, "positive integer"),
    ({"max_csv_upload_bytes": "100"}, "positive integer"),
    ({"max_csv_upload_bytes": True}, "positive integer"),
    ({"ingestion_schema": None}, "ingestion_schema"),
    ({"ingestion_schema": {"create": {}}}, "'create', 'update' and 'delete'"),
    ({"process_steps": None}, "process_steps"),
    ({"process_steps": [{"columns": ["x"]}]}, "non-empty string 'name'"),
    ({"expect_stages": ["A", "A"]}, "must not repeat"),
])
def test_feedback_config_missing_or_invalid_setting_is_rejected_nothing_stored(client, monkeypatch, override, expected):
    stores = _patch_upload_pipeline(monkeypatch, active=_active_with(**override))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 400
    assert any(expected in err for err in resp.json()["errors"]), resp.json()
    assert "Youth" in resp.json()["errors"][0]  # names the persona
    _assert_nothing_stored(stores)


def test_feedback_config_missing_key_hint_names_the_migrate_tool(client, monkeypatch):
    _patch_upload_pipeline(monkeypatch, active=_active_with(process_steps=None))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert "tools/feedback_config.py migrate" in resp.json()["errors"][0]


def test_validators_report_type_and_caps(monkeypatch):
    from app.api.exceptions import FileTooLarge
    from app.api.validators.uploads import max_upload_bytes, validate_file_bytes, validate_report_type

    assert validate_report_type(" Feedback ") == "feedback"
    settings_override(monkeypatch, settings, MAX_CSV_UPLOAD_BYTES=5)
    # feedback: the cap is the resolved config's, passed in; there is no global feedback cap
    validate_file_bytes(b"x" * 20, "feedback", 50)
    with pytest.raises(FileTooLarge):
        validate_file_bytes(b"x" * 51, "feedback", 50)
    with pytest.raises(ValueError, match="max_csv_upload_bytes"):
        validate_file_bytes(b"x" * 20, "feedback")
    with pytest.raises(ValueError, match="max_csv_upload_bytes"):
        max_upload_bytes("feedback")
    with pytest.raises(FileTooLarge):
        validate_file_bytes(b"x" * 20, "story")
    with pytest.raises(FileTooLarge):
        validate_file_bytes(b"x" * 20)  # legacy call signature keeps the story cap


# EXPECT_STAGES (report-only)
def _stages_file(extra_stage_rows: bool = True) -> bytes:
    rows = _session("a" * 32, "1001") + _session("b" * 32, "1002")
    if extra_stage_rows:
        rows += [_row("90", "b" * 32, "1002", 5, "Odd question", "", "NEW_STAGE", 1),
                 _row("90", "b" * 32, "1002", 6, "an answer", "", "NEW_STAGE", 120)]
    return _csv(rows)


def test_expect_stages_reports_unexpected_stages_without_skipping_anything(client, monkeypatch):
    # expected = AGE_IDENTIFICATION + LANG_Q; THANK_YOU is a closing key; NEW_STAGE is neither (session 1002 only)
    stores = _patch_upload_pipeline(
        monkeypatch, active=_active_with(expect_stages=["AGE_IDENTIFICATION", "LANG_Q"]))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_stages_file()))
    assert resp.status_code == 200, resp.text
    report = resp.json()["stage_report"]
    assert report == {"sessions": 2, "sessions_with_unexpected_stages": 1, "unexpected_stage_count": 1}
    assert "NEW_STAGE" not in resp.text   # counts only, no stage value from the file
    assert stores.insert.await_args.kwargs["meta_data"]["stage_report"] == report  # also on the upload record
    stores.upload.assert_called_once()  # the file is stored: report-only, never a rejection


def test_stage_report_is_absent_without_expect_stages(client, monkeypatch):
    _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_stages_file()))
    assert resp.status_code == 200
    assert resp.json() == {"message": "Successfully uploaded to cloud", "id": 7, "status": "pending"}


def test_expect_stages_all_known_reports_nothing_unexpected(client, monkeypatch):
    _patch_upload_pipeline(monkeypatch, active=_active_with(expect_stages=["AGE_IDENTIFICATION", "LANG_Q"]))
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files(_stages_file(False)))
    assert resp.json()["stage_report"]["unexpected_stage_count"] == 0
    assert resp.json()["stage_report"]["sessions_with_unexpected_stages"] == 0


# SESSION LEVEL (process_csv_inline)
def _record(meta: Optional[dict] = None) -> dict:
    return {
        "id": 7, "report_type": "feedback", "cloud_storage_path": "path/to/youth.csv",
        "leader_category": "Youth", "program_name": FEEDBACK_PROGRAM_NAME,
        "meta_data": meta if meta is not None else {"tenant_code": "mitra", "source": "mitra"},
    }


class BgHarness:
    def __init__(self, monkeypatch, record: Optional[dict] = None):
        self.record = record or _record()
        monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=self.record))
        self.update_status = AsyncMock()
        monkeypatch.setattr(operations_module, "update_status", self.update_status)
        self.lookup = AsyncMock(return_value=RESOLVED)
        self.config = AsyncMock(return_value=ACTIVE_CONFIG)
        monkeypatch.setattr(feedback_ops, "get_program_and_leader", self.lookup)
        monkeypatch.setattr(feedback_ops, "get_active_feedback_config", self.config)
        # delta lookups: nothing stored yet (tests/test_feedback_delta.py drives these)
        self.state = AsyncMock(return_value={})
        self.refs = AsyncMock(return_value=set())
        monkeypatch.setattr(feedback_ops, "get_mitra_session_state", self.state)
        monkeypatch.setattr(feedback_ops, "get_known_source_refs", self.refs)
        self.push = MagicMock()
        monkeypatch.setattr(uploads_service_module, "_push_rows_sync", self.push)
        self.dlq = MagicMock()
        monkeypatch.setattr(uploads_service_module, "_push_dlq_sync", self.dlq)
        self.fetch = MagicMock(return_value=GOOD_FILE)
        monkeypatch.setattr(uploads_service_module, "fetch_csv", self.fetch)

    def run(self, **kwargs):
        asyncio.run(uploads_service_module.process_csv_inline(7, **kwargs))

    @property
    def final(self):
        call = self.update_status.await_args_list[-1]
        return call.args[1], (call.args[2] if len(call.args) > 2 else None)


def test_background_all_sessions_published_status_success(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 2
    assert meta["sessions_skipped"] == 0
    assert meta["config_version_id"] == ACTIVE_CONFIG["config_version_id"]
    assert "processed_at" in meta and "schema_validation_errors" not in meta and "truncated" not in meta
    h.dlq.assert_not_called()

    payloads = h.push.call_args.args[0]
    assert [key for _, key in payloads] == ["7-0", "7-1"]
    event = json.loads(payloads[0][0])
    assert event["submissionType"] == "feedback" and event["submissionId"] == "1001"
    assert event["tags"]["programId"] == PROGRAM["id"] and event["tags"]["leaderCategoryId"] == LEADER["id"]
    # status was moved to in_progress first
    assert h.update_status.await_args_list[0].args[:2] == (7, "in_progress")


def _published_events(h: "BgHarness") -> List[dict]:
    return [json.loads(value) for value, _key in h.push.call_args.args[0]]


def test_background_new_shape_sessions_build_events_with_the_right_questionnaire_version(monkeypatch):
    """Opening row at sequence 1 (blank sender_id / stage / created_at): the file check passes and the
    background step keeps it as the first bot row; the version comes from the session's first real message."""
    h = BgHarness(monkeypatch)
    file_bytes = _csv(_session_new_shape("a" * 32, "1001", bot="90", ts=BEFORE_V2)
                      + _session_new_shape("b" * 32, "1002", bot="91", ts=AFTER_V2))
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "success", meta
    assert meta["rows_pushed"] == 2 and meta["sessions_skipped"] == 0
    h.dlq.assert_not_called()

    before, after = _published_events(h)
    assert (before["submissionId"], after["submissionId"]) == ("1001", "1002")
    assert before["data"]["questionnaireVersion"] == "V1" and after["data"]["questionnaireVersion"] == "V2"
    for event, bot in ((before, "90"), (after, "91")):
        data = event["data"]
        assert data["sessionType"] == "Youth_Voice" and data["botId"] == bot and data["botName"] == "Youth"
        messages = data["messages"]
        assert [m["sequence"] for m in messages] == [1, 2, 3, 4, 5]  # CSV sequences, no +1 shift, no synthesised row
        opening = messages[0]
        assert opening["message"] == "Welcome to the survey"
        assert opening["messageAt"] is None and opening["parentSequence"] is None
        assert opening["questionKey"] == "AGE_IDENTIFICATION"  # the stage of the next row
        assert messages[1]["parentSequence"] == 1 and messages[1]["messageAt"] is not None
    assert before["data"]["submissionDate"] == "2026-10-15T03:24:43.000Z"  # first real message, not the opening row
    assert after["data"]["submissionDate"] == "2026-11-10T03:30:00.000Z"


def test_background_questionnaire_version_boundary_and_start_of_session(monkeypatch):
    h = BgHarness(monkeypatch)
    v2 = "2026-11-03 18:23:03+05:30"  # the Youth test window start (TEST_V2_START), exactly
    just_before = "2026-11-03 18:23:02+05:30"
    straddling = _session_new_shape("c" * 32, "1003", ts=just_before)
    straddling[-1] = straddling[-1].replace(just_before, AFTER_V2)  # last message long after V2: start decides
    file_bytes = _csv(_session_new_shape("a" * 32, "1001", ts=v2)
                      + _session_new_shape("b" * 32, "1002", ts=just_before) + straddling)
    h.run(file_bytes=file_bytes)
    assert h.final[0] == "success"
    versions = {e["submissionId"]: e["data"]["questionnaireVersion"] for e in _published_events(h)}
    assert versions == {"1001": "V2", "1002": "V1", "1003": "V1"}


def test_background_old_shape_sessions_are_also_stamped(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session("a" * 32, "1001", ts=BEFORE_V2) + _session("b" * 32, "1002", ts=AFTER_V2)))
    assert h.final[0] == "success"
    old, newer = _published_events(h)
    assert (old["data"]["questionnaireVersion"], newer["data"]["questionnaireVersion"]) == ("V1", "V2")
    assert [m["sequence"] for m in old["data"]["messages"]] == [1, 2, 3, 4, 5]  # synthesised first question + shift


def test_background_versions_follow_each_personas_own_date(monkeypatch):
    # 2026-11-03 18:30 IST: after Youth's V2 (18:23:03) but before Parent's (18:52:54)
    stamp = "2026-11-03 18:30:00+05:30"
    parent = PERSONAS["Parent"]
    h = BgHarness(monkeypatch)
    h.lookup.return_value = _resolved("Parent")
    h.config.return_value = _active("Parent")
    h.run(file_bytes=_csv(_session_new_shape("a" * 32, "1001", bot="88", session_type="Parent_Voice", ts=stamp)))
    assert h.final[0] == "success"
    (event,) = _published_events(h)
    assert event["data"]["sessionType"] == "Parent_Voice" and event["data"]["questionnaireVersion"] == "V1"
    assert event["tags"]["programId"] == parent["program_id"]

    h2 = BgHarness(monkeypatch)
    h2.run(file_bytes=_csv(_session_new_shape("a" * 32, "1001", ts=stamp)))
    (event,) = _published_events(h2)
    assert event["data"]["questionnaireVersion"] == "V2"  # Youth


def test_background_educator_sessions_are_always_v1(monkeypatch):
    h = BgHarness(monkeypatch)
    h.lookup.return_value = _resolved("Educator")
    h.config.return_value = _active("Educator", baseline=True)
    h.run(file_bytes=_csv(_session_new_shape("a" * 32, "1001", bot="110", session_type="Educator_Voice", ts=AFTER_V2)))
    assert h.final[0] == "success"
    (event,) = _published_events(h)
    assert event["data"]["questionnaireVersion"] == "V1" and event["data"]["botId"] == "110"


def test_background_seeded_baseline_config_stamps_everything_v1_whatever_the_date(monkeypatch):
    # The config as seeded has only V1: a 2030 session is V1 too. Later versions are recorded in the database
    # (tools/questionnaire_versions.py set ...), never in code, and then show up in the active config.
    h = BgHarness(monkeypatch)
    h.config.return_value = _active("Youth", baseline=True)
    h.run(file_bytes=_csv(_session_new_shape("a" * 32, "1001", ts=BEFORE_V2)
                          + _session_new_shape("b" * 32, "1002", ts=AFTER_V2)
                          + _session_new_shape("c" * 32, "1003", ts="2030-01-01 00:00:00+05:30")))
    assert h.final[0] == "success"
    assert [e["data"]["questionnaireVersion"] for e in _published_events(h)] == ["V1", "V1", "V1"]


def test_background_stored_file_of_another_persona_is_put_on_hold(monkeypatch):
    # the stored file is re-checked against the active config: Parent rows under the Youth config are put on hold
    h = BgHarness(monkeypatch)
    h.fetch.return_value = _csv(_session("a" * 32, "1001", session_type="Parent_Voice"))
    h.run()
    status, meta = h.final
    assert status == "on_hold" and meta["stage"] == "Feedback Config Resolution"
    assert "only session_type Youth_Voice is allowed" in meta["validation_errors"][0]
    h.push.assert_not_called()


def test_background_skipped_sessions_go_to_dlq_listed_in_meta_status_still_success(monkeypatch):
    h = BgHarness(monkeypatch)
    bad = _session("c" * 32, "1003", status="DONE")  # session_status not COMPLETED / IN_PROGRESS
    empty_msg = _session("d" * 32, "1004")
    empty_msg[2] = _row("90", "d" * 32, "1004", 3, "", "", "LANG_Q", 120)  # empty answer
    file_bytes = _csv(_session("a" * 32, "1001") + bad + empty_msg)
    h.run(file_bytes=file_bytes)

    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1
    assert meta["sessions_skipped"] == 2
    assert meta["sessions_dead_lettered"] == 2
    assert "dlq_publish_error" not in meta
    skipped = {e["submissionId"]: e for e in meta["schema_validation_errors"]}
    assert set(skipped) == {"1003", "1004"}
    assert skipped["1003"]["sessionId"] == "c" * 32 and skipped["1003"]["problems"]

    (messages,) = h.dlq.call_args.args
    assert len(messages) == 2
    by_id = {headers["submissionId"]: (json.loads(value), headers) for value, headers in messages}
    value, headers = by_id["1003"]
    assert value["submissionType"] == "feedback" and value["sessionId"] == "c" * 32
    assert value["tenantCode"] == "mitra" and value["csvUploadId"] == 7 and value["problems"]
    assert headers["sessionId"] == "c" * 32 and headers["tenantCode"] == "mitra"
    assert headers["reason"].startswith("Skipped feedback session:")
    assert "answer" not in json.dumps(value).lower().replace("problems", "")  # no conversation text
    # only the good session reached the ingestion topic
    assert len(h.push.call_args.args[0]) == 1


def test_background_schema_failed_event_is_skipped_and_dead_lettered(monkeypatch):
    h = BgHarness(monkeypatch)
    real = uploads_service_module.validate_ingestion_schema

    def fail_second(event, report_type, event_type, feedback_schema):
        assert (report_type, event_type) == ("feedback", "create")
        # the schema is the persona config's, loaded once for the upload (not a global setting)
        assert feedback_schema == ACTIVE_CONFIG["config"]["ingestion_schema"]
        return (["'data.messages' is empty"] if event["submissionId"] == "1002"
                else real(event, report_type, event_type, feedback_schema=feedback_schema))

    monkeypatch.setattr(uploads_service_module, "validate_ingestion_schema", fail_second)
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1
    assert meta["schema_validation_errors"] == [
        {"submissionId": "1002", "sessionId": "b" * 32, "problems": ["'data.messages' is empty"]}
    ]
    (messages,) = h.dlq.call_args.args
    assert [headers["submissionId"] for _, headers in messages] == ["1002"]


def test_background_dlq_failure_is_recorded_but_upload_still_success(monkeypatch):
    h = BgHarness(monkeypatch)
    h.dlq.side_effect = RuntimeError("dlq broker down")
    file_bytes = _csv(_session("a" * 32, "1001") + _session("c" * 32, "1003", status="DONE"))
    h.run(file_bytes=file_bytes)
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 1
    assert "dlq broker down" in meta["dlq_publish_error"]
    assert "sessions_dead_lettered" not in meta
    assert len(meta["schema_validation_errors"]) == 1


def test_background_error_list_capped_at_200_with_truncated_marker(monkeypatch):
    h = BgHarness(monkeypatch)
    rows = _session("a" * 32, "1")
    for i in range(2, 252):  # 250 bad sessions
        rows += _session(f"{i:032d}", str(i), status="DONE")
    h.run(file_bytes=_csv(rows))
    status, meta = h.final
    assert status == "success"
    assert meta["rows_pushed"] == 1 and meta["sessions_skipped"] == 250
    assert len(meta["schema_validation_errors"]) == 200
    assert meta["truncated"] is True
    (messages,) = h.dlq.call_args.args
    assert len(messages) == 250  # the DLQ gets every skipped session, only the meta list is capped
    assert meta["sessions_dead_lettered"] == 250


def test_background_all_sessions_skipped_is_still_success(monkeypatch):
    h = BgHarness(monkeypatch)
    h.run(file_bytes=_csv(_session("c" * 32, "1003", status="DONE")))
    status, meta = h.final
    assert status == "success" and meta["rows_pushed"] == 0 and meta["sessions_skipped"] == 1
    h.push.assert_not_called()
    h.dlq.assert_called_once()


def test_background_kafka_failure_marks_on_hold_and_skips_dlq(monkeypatch):
    h = BgHarness(monkeypatch)
    h.push.side_effect = RuntimeError("broker down")
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold"
    assert meta["stage"] == "Kafka Publishing" and "broker down" in meta["exception"]
    h.dlq.assert_not_called()


def test_background_fetch_failure_marks_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.fetch.side_effect = RuntimeError("bucket unreachable")

    async def no_sleep(_):
        return None

    monkeypatch.setattr(uploads_service_module.asyncio, "sleep", no_sleep)
    h.run()
    status, meta = h.final
    assert status == "on_hold" and meta["stage"] == "CSV Fetching"
    assert h.fetch.call_count == 3
    h.push.assert_not_called()


def test_background_loads_from_storage_with_string_parse_and_rechecks_file(monkeypatch):
    h = BgHarness(monkeypatch)
    h.fetch.return_value = _csv(_session("a" * 32, "1001", answer="None"))
    h.run()
    status, meta = h.final
    assert status == "success" and meta["rows_pushed"] == 1
    event = json.loads(h.push.call_args.args[0][0][0])
    assert "None" in [m["message"] for m in event["data"]["messages"]]  # not turned into NaN

    # a stored file that no longer passes the config's file-level check is put on hold
    h2 = BgHarness(monkeypatch)
    h2.fetch.return_value = _csv(_session("a" * 32, "1001"), HEADER.replace("session_status", "status"))
    h2.run()
    status, meta = h2.final
    assert status == "on_hold" and meta["stage"] == "Feedback Config Resolution"
    assert meta["validation_errors"]


def test_background_unknown_program_never_fabricated_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.lookup.return_value = None
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold"
    assert meta["stage"] == "Feedback Config Resolution" and "does not exist" in meta["error"]
    h.push.assert_not_called()
    h.dlq.assert_not_called()


def test_background_config_missing_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.config.return_value = None
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold"
    assert meta["validation_errors"] == ["No upload config for source 'mitra' is set up for Youth: no config for this program and no program-less default"]


def test_background_db_error_on_hold(monkeypatch):
    h = BgHarness(monkeypatch)
    h.lookup.side_effect = RuntimeError("db down")
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold" and "db down" in meta["error"]


def test_background_missing_tenant_in_record_on_hold(monkeypatch):
    h = BgHarness(monkeypatch, record=_record(meta={}))
    h.run(file_bytes=GOOD_FILE)
    status, meta = h.final
    assert status == "on_hold" and meta["stage"] == "Feedback Config Resolution"
    h.lookup.assert_not_called()


def test_background_meta_data_as_json_string(monkeypatch):
    h = BgHarness(monkeypatch, record={**_record(), "meta_data": json.dumps({"tenant_code": "mitra", "source": "mitra"})})
    h.run(file_bytes=GOOD_FILE)
    assert h.final[0] == "success"


def test_push_dlq_sync_produces_to_dlq_topic_with_headers(monkeypatch):
    from tests.conftest import make_fake_kafka_producer
    producer = make_fake_kafka_producer()
    monkeypatch.setattr(uploads_service_module, "_get_producer", lambda: producer)
    settings_override(monkeypatch, settings, KAFKA_TOPIC_INGESTION_DLQ="test.dlq")
    uploads_service_module._push_dlq_sync([('{"a": 1}', {"submissionId": "1", "sessionId": None, "reason": "r"})])
    args = producer.produce.call_args
    assert args.args[0] == "test.dlq"
    assert args.kwargs["value"] == b'{"a": 1}'
    assert dict(args.kwargs["headers"]) == {"submissionId": b"1", "reason": b"r"}  # None identifiers omitted


def test_push_dlq_sync_raises_on_delivery_error(monkeypatch):
    from confluent_kafka import KafkaException
    from tests.conftest import make_fake_kafka_producer
    producer = make_fake_kafka_producer(delivery_error="boom")
    monkeypatch.setattr(uploads_service_module, "_get_producer", lambda: producer)
    with pytest.raises(KafkaException):
        uploads_service_module._push_dlq_sync([("{}", {"reason": "r"})])


# STORY / DISCUSSION unchanged: the story branch of process_csv_inline is not entered for feedback
# and the feedback branch is not entered for story.
def test_story_record_never_enters_feedback_branch(monkeypatch):
    conn = install_fake_db(monkeypatch, uploads_service_module)
    conn.fetchrow.return_value = None
    record = {"id": 1, "report_type": "story", "cloud_storage_path": "p.csv", "leader_category": "L",
              "program_name": "P", "meta_data": {"tenant_code": "mitra"}}
    monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
    monkeypatch.setattr(operations_module, "update_status", AsyncMock())
    feedback_branch = AsyncMock()
    monkeypatch.setattr(uploads_service_module, "_process_feedback_inline", feedback_branch)
    import pandas as pd
    monkeypatch.setattr(uploads_service_module, "fetch_csv", MagicMock(return_value=b"raw"))
    monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=pd.DataFrame([{"id": "1"}])))
    monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(False, ["x"])))
    asyncio.run(uploads_service_module.process_csv_inline(1))
    feedback_branch.assert_not_called()


# REAL POSTGRES (opt-in): get_program_and_leader / get_active_feedback_config
DB_URL = pg_support.SERVER_URL


async def ensure_schema(conn) -> None:
    """Applies schema.sql when the scratch database is still empty (idempotent otherwise)."""
    await conn.execute(pg_support.schema_sql())


@pytest.mark.skipif(DB_URL is None, reason="FEEDBACK_TEST_DATABASE_URL not set (scratch DB, empty or with schema.sql applied)")
def test_real_db_lookups_by_name_and_tenant(monkeypatch):
    import asyncpg
    from app.database.db import db
    from seed_support import seed_config

    async def run():
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
        monkeypatch.setattr(db, "pool", pool, raising=False)
        try:
            async with pool.acquire() as conn:
                await ensure_schema(conn)
                await conn.execute(
                    "INSERT INTO tenant(name, code) VALUES ('Mitra', 'mitra'), ('Other', 'other') ON CONFLICT DO NOTHING")
                await seed_config(conn)
                # same names under ANOTHER tenant must never be matched for tenant 'mitra'
                other_leader = await conn.fetchval(
                    "INSERT INTO leader_category (name, tenant_code) VALUES ('Youth', 'other') RETURNING id")
                await conn.execute(
                    "INSERT INTO programs (leaders_id, name, tenant_code) VALUES ($1, '1M Voice', 'other')", other_leader)

            for persona, p in PERSONAS.items():
                got = await feedback_ops.get_program_and_leader(FEEDBACK_PROGRAM_NAME, persona, "mitra")
                assert got == {
                    "program": {"id": p["program_id"], "name": FEEDBACK_PROGRAM_NAME},
                    "leader": {"id": p["leader_category_id"], "name": persona},
                    "tenant_code": "mitra",
                }
            other = await feedback_ops.get_program_and_leader(FEEDBACK_PROGRAM_NAME, "Youth", "other")
            assert other["leader"]["id"] == str(other_leader)
            assert await feedback_ops.get_program_and_leader(FEEDBACK_PROGRAM_NAME, "Nobody", "mitra") is None
            assert await feedback_ops.get_program_and_leader("No Program", "Youth", "mitra") is None
            assert await feedback_ops.get_program_and_leader(FEEDBACK_PROGRAM_NAME, "Youth", "missing-tenant") is None

            for persona, p in PERSONAS.items():
                active = await feedback_ops.get_active_feedback_config(p["program_id"], "mitra")
                assert active["config_id"] == p["config_id"] and active["version"] == 1
                assert active["session_type"] == p["session_type"]
                assert active["config"] == mitra_config_v1(persona)  # JSONB came back as a dict
                same = await feedback_ops.get_active_feedback_config(p["program_id"], "mitra", tenant_code="mitra")
                assert same == active
                # the tenant narrows the lookup: another tenant's code finds nothing
                assert await feedback_ops.get_active_feedback_config(p["program_id"], "mitra", tenant_code="other") is None
            # the lookup is by program + source: an unknown program or source finds nothing
            assert await feedback_ops.get_active_feedback_config(str(uuid.uuid4()), "mitra") is None
            assert await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "whatsapp") is None

            # only the ACTIVE version is returned; v2 supersedes v1
            async with pool.acquire() as conn:
                await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH["config_id"])
                await conn.execute(
                    "INSERT INTO config_version (config_id, version, is_active, config) VALUES ($1::uuid, 2, TRUE, $2::jsonb)",
                    YOUTH["config_id"], json.dumps({"marker": "v2"}))
            active = await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "mitra")
            assert active["version"] == 2 and active["config"] == {"marker": "v2"}
            assert active["session_type"] == "Youth_Voice"
            async with pool.acquire() as conn:
                await conn.execute("UPDATE config_version SET is_active = FALSE WHERE config_id = $1::uuid", YOUTH["config_id"])
            assert await feedback_ops.get_active_feedback_config(YOUTH["program_id"], "mitra") is None
        finally:
            # these lookups need committed rows (feedback_ops uses its own connections), so undo what the test added
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM config_version WHERE config_id = $1::uuid AND version = 2", YOUTH["config_id"])
                await conn.execute("UPDATE config_version SET is_active = TRUE WHERE config_id = $1::uuid AND version = 1", YOUTH["config_id"])
                await conn.execute("DELETE FROM programs WHERE tenant_code = 'other'")
                await conn.execute("DELETE FROM leader_category WHERE tenant_code = 'other'")
                await conn.execute("DELETE FROM tenant WHERE code = 'other'")
            await pool.close()

    asyncio.run(run())


@pytest.mark.skipif(DB_URL is None, reason="FEEDBACK_TEST_DATABASE_URL not set (scratch DB, empty or with schema.sql applied)")
def test_real_db_active_config_is_found_per_source(monkeypatch):
    import asyncpg
    from app.database.db import db
    from seed_support import seed_config

    async def run():
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
        monkeypatch.setattr(db, "pool", pool, raising=False)
        try:
            async with pool.acquire() as conn:
                await ensure_schema(conn)
                await conn.execute("INSERT INTO tenant(name, code) VALUES ('Mitra', 'mitra') ON CONFLICT DO NOTHING")
                await seed_config(conn)

            for persona, p in PERSONAS.items():
                mitra = await feedback_ops.get_active_feedback_config(p["program_id"], "mitra")
                assert mitra["config_id"] == p["config_id"] and mitra["session_type"] == p["session_type"]
                gforms = await feedback_ops.get_active_feedback_config(p["program_id"], "google_forms")
                if p["gforms_config_id"] is None:
                    assert persona == "Educator" and gforms is None  # no Google Form for the Educator
                    continue
                assert gforms["config_id"] == p["gforms_config_id"]
                assert gforms["session_type"] == p["session_type"]
                assert gforms["config"] == gforms_config_v1(persona)
            # another persona's program never leaks in
            assert (await feedback_ops.get_active_feedback_config(
                PERSONAS["Parent"]["program_id"], "google_forms"))["config_id"] == PERSONAS["Parent"]["gforms_config_id"]
            assert await feedback_ops.get_active_feedback_config(str(uuid.uuid4()), "google_forms") is None
        finally:
            await pool.close()  # the seed rows stay (the opt-in tests run against an empty database per run)

    asyncio.run(run())


# ---- ambiguous program / config (fixer S: AmbiguousFeedbackConfigError) -> 409 without config content ----

@pytest.mark.parametrize("where", ["program", "config"])
def test_feedback_ambiguous_program_or_config_is_409_without_config_content(client, monkeypatch, where):
    from app.database.feedback_operations import AmbiguousFeedbackConfigError
    stores = _patch_upload_pipeline(monkeypatch)
    leaked = "SECRET-CANDIDATE-LIST id=deadbeef session_type=Youth_Voice,Teacher_Voice"
    boom = AsyncMock(side_effect=AmbiguousFeedbackConfigError(f"2 configs match ({leaked})"))
    monkeypatch.setattr(feedback_ops, "get_program_and_leader" if where == "program" else "get_active_feedback_config", boom)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 409
    assert "SECRET-CANDIDATE" not in resp.text and "deadbeef" not in resp.text and "Teacher_Voice" not in resp.text
    assert "operator" in resp.json()["detail"]
    _assert_nothing_stored(stores)


def test_feedback_config_lookup_is_narrowed_by_tenant(client, monkeypatch):
    stores = _patch_upload_pipeline(monkeypatch)
    resp = client.post("/v1/upload/", headers=_headers(), data=_form(), files=_files())
    assert resp.status_code == 200
    stores.config.assert_awaited_once_with(PROGRAM["id"], "mitra", tenant_code="mitra")
