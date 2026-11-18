"""
Builders of FICTITIOUS programs, tenants, upload configs and sessions for the tests that are about behaviour and not
about the seeded 1MV content (tests/seed_support.py reads that). One place instead of a copy per test file:

    program = await add_program(conn, "abc", "PTM 2026", "Teacher")          # tenant + leader category + program
    await add_config(conn, program, "ptm_app", "Teacher_PTM")               # an active upload config of it
    sid = await store_session(conn, program, "Teacher_PTM", "ptm_app", "V1", started_at)   # through the real ingest path

A program is the dict {"tenant", "program_id", "leader_id", "program_name", "leader_name"}. Nothing here knows a
persona name, a program name or a count of the seed: the base of `upload_config` is the first seeded upload config,
only because a config has to be valid; every test that needs a value overrides it.
"""
import copy
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.database.operations import insert_or_update_submission
from seed_support import PERSONAS, mitra_config_v1


def uid() -> str:
    return str(uuid.uuid4())


def at(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def upload_config(**changes) -> Dict[str, Any]:
    """A valid conversation_rows upload config (a deep copy of the first seeded one) with `changes` applied."""
    config = copy.deepcopy(mitra_config_v1(next(iter(PERSONAS))))
    config.update(changes)
    return config


async def add_program(conn, tenant: str, program_name: str, leader_name: str, *,
                      leader_id: Optional[str] = None, program_id: Optional[str] = None) -> Dict[str, str]:
    """A fictitious leader category + program in `tenant` (the tenant row is created when it does not exist)."""
    leader_id = leader_id if leader_id is not None else uid()
    program_id = program_id if program_id is not None else uid()
    await conn.execute(
        "INSERT INTO tenant (name, code) VALUES ($1, $2) ON CONFLICT (code) DO NOTHING", tenant.upper(), tenant)
    await conn.execute("INSERT INTO leader_category (id, name, tenant_code) VALUES ($1::uuid, $2, $3)",
                       leader_id, leader_name, tenant)
    await conn.execute("INSERT INTO programs (id, leaders_id, name, tenant_code) VALUES ($1::uuid, $2::uuid, $3, $4)",
                       program_id, leader_id, program_name, tenant)
    return {"tenant": tenant, "program_id": program_id, "leader_id": leader_id, "program_name": program_name,
            "leader_name": leader_name}


async def add_config(conn, program: Dict[str, str], source: str, session_type: str,
                     config: Optional[Dict[str, Any]] = None, *, config_type: str = "feedback_upload") -> str:
    """An active version 1 of a config of `program` (default: `upload_config(session_type=...)`); returns the id."""
    config_id = uid()
    await conn.execute(
        "INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, session_type) "
        "VALUES ($1::uuid, $2, $3, $4, $5::uuid, $6, $7)",
        config_id, source, f"{program['program_name']} {session_type} {source}", config_type, program["program_id"],
        program["tenant"], session_type)
    await conn.execute(
        "INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note) "
        "VALUES ($1::uuid, 1, TRUE, $2::jsonb, 'test', 'test')",
        config_id, json.dumps(config if config is not None else upload_config(session_type=session_type)))
    return config_id


def conversation_messages(question: str = "How old are you?", answers=("41",), key: str = "Q1") -> List[Dict[str, Any]]:
    """One bot question (sequence 1) and one answer row per entry of `answers` (parent = the question)."""
    messages = [{"sequence": 1, "questionKey": key, "isFollowUp": False, "message": question,
                 "translatedMessage": None, "messageAt": None, "parentSequence": None}]
    for number, text in enumerate(answers, start=2):
        messages.append({"sequence": number, "questionKey": key, "isFollowUp": False, "message": text,
                         "translatedMessage": None, "messageAt": None, "parentSequence": 1})
    return messages


def conversation_event(program: Dict[str, str], session_type: str, source: str = "mitra", version: str = "V1",
                       started_at: Optional[datetime] = None, *, adapter: str = "conversation_rows",
                       sid: Optional[str] = None, messages: Optional[List[Dict[str, Any]]] = None,
                       source_ref: Optional[str] = None, bot_id: Optional[str] = "101") -> Dict[str, Any]:
    """A create event of one session of `program` as the platform publishes it (any source label, any adapter).
    A conversation_rows event carries `bot_id` (required there: its questionnaire version is the rank of its bot id
    inside the persona); a wide_form event carries none."""
    sid = sid if sid is not None else f"c{uuid.uuid4().hex[:14]}"
    started_at = started_at if started_at is not None else at(2026, 11, 10, 8, 0, 0)
    event = {
        "submissionId": sid, "sessionId": sid if adapter == "wide_form" else f"sess-{uuid.uuid4().hex}",
        "submissionType": "feedback", "tenantCode": program["tenant"], "eventType": "create",
        "eventPublishedAt": "2026-11-10T00:00:00.000Z",
        "tags": {"programId": program["program_id"], "programName": program["program_name"],
                 "leaderCategoryId": program["leader_id"], "leaderCategoryName": program.get("leader_name", "Leader"),
                 "state": None, "district": None, "source": source},
        "data": {"source": source, "adapter": adapter, "sessionType": session_type, "questionnaireVersion": version,
                 "language": "en", "completionStatus": "COMPLETED", "submissionDate": started_at.isoformat(),
                 "messages": messages if messages is not None else conversation_messages()},
    }
    if source_ref is not None:
        event["data"]["sourceRef"] = source_ref
    if adapter != "wide_form" and bot_id is not None:
        event["data"]["botId"] = bot_id
    return event


async def store_session(conn, program: Dict[str, str], session_type: str, source: str, version: str,
                        started_at: datetime, **event_options) -> str:
    """Stores one session through the real ingest path; returns its submission id. The tests that use it are about
    version labels that come from date windows, which only apply to a session WITHOUT a bot id (a bot session's label
    is the rank of its bot id), so a conversation_rows session is stored and then turned into one carrying `version`
    and no bot id (as_window_stamped_session); pass window_stamped=False to keep it a bot session."""
    window_stamped = event_options.pop("window_stamped", True)
    event = conversation_event(program, session_type, source, version, started_at, **event_options)
    await insert_or_update_submission(conn, event)
    if window_stamped and event["data"]["adapter"] != "wide_form":
        await as_window_stamped_session(conn, event["submissionId"], version, program["tenant"])
    return event["submissionId"]


async def as_window_stamped_session(conn, submission_id: str, version: str, tenant: str = "mitra") -> None:
    """
    Turns a stored conversation_rows session into one WITHOUT a bot id that carries `version` (what a session stored
    before the bot id became required looks like). Tests of the date-window machinery (set / restamp / detect, which
    only touch sessions without a bot id; a bot session's version is the rank of its bot id) use it to get sessions
    whose label comes from a window.
    """
    await conn.execute(
        "UPDATE feedback_submissions SET meta_data = COALESCE(meta_data, '{}'::jsonb) - 'bot_id', "
        "questionnaire_version = $3 WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant, version)
