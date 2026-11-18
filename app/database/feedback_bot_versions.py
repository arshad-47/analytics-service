"""
Questionnaire version of a Mitra bot session = the RANK of its bot id inside its persona (the pure rule is
app/services/feedback_versions.rank_bot_versions; this module applies it to the stored sessions).

A persona here is (tenant_code, program_id of the submission, session_type, source). The distinct bot ids stored for
it (feedback_submissions.meta_data ->> 'bot_id') are ranked by number: lowest = V1, next = V2 ... When a new bot id
arrives that is LOWER than stored ones, their ranks shift, so the stored sessions whose label changed are relabelled
in the ingest transaction that brought the new bot (apply_bot_version_ranks). A relabelled session changes meaning
(a field that exists in one questionnaire version only is 'N/A' by version), so its extraction is stale and it goes
back through processing, exactly like a session that moved persona (operations._write_feedback_submission).

Locking (this order everywhere, so no two writers can deadlock):
  1. the persona advisory lock (lock_persona), the FIRST lock of the transaction (before the metadata upserts, any row
     lock and the read of the bot ids), so two concurrent ingests of two new bots of one persona can not each compute
     ranks without the other. A transaction that needs two personas takes them in key order (lock_personas);
  2. the submissions rows, then the feedback_submissions rows (the order of the extraction save,
     feedback_extraction_operations.save_extraction), in ascending submission_id.
The extraction save never takes the advisory lock, so it can only make a restamp wait, never deadlock with it.
"""
import json
import logging
from typing import Any, Dict, List, Optional

from app.services.feedback_versions import BOT_ID_PATTERN, rank_bot_versions

logger = logging.getLogger("analytics_service.database.feedback_bot_versions")

# SQL (POSIX) form of BOT_ID_PATTERN: a stored bot id outside it (data stored before the boundary check existed) is
# not a bot session for ranking purposes and keeps the label it has.
BOT_ID_SQL_PATTERN = f"^{BOT_ID_PATTERN}$"

# SQL condition: a Mitra bot session = a conversation_rows session (no natural key, source_ref IS NULL) that carries a
# rankable bot id (`fs` = feedback_submissions). NULL-safe, so `NOT {BOT_SESSION_SQL}` works for any other session.
BOT_SESSION_SQL = f"(fs.source_ref IS NULL AND COALESCE((fs.meta_data ->> 'bot_id') ~ '{BOT_ID_SQL_PATTERN}', FALSE))"


def _program_text(program_id: Any) -> Optional[str]:
    return None if program_id is None else str(program_id).lower()


def _lock_key(tenant_code: str, program_id: Any, session_type: str, source: str) -> str:
    """The persona as JSON, so that two personas can not share a key."""
    return "feedback_bot_versions:" + json.dumps([tenant_code, _program_text(program_id), session_type, source])


async def lock_persona(conn, tenant_code: str, program_id: Any, session_type: str, source: str) -> None:
    """
    Transaction-level advisory lock of one persona; blocks while another transaction holds it and is released at the
    end of the current transaction. Re-taking it in the same transaction is free. hashtextextended gives the 64-bit
    lock id.
    """
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", _lock_key(tenant_code, program_id, session_type, source)
    )


async def lock_personas(conn, personas: List[tuple]) -> None:
    """
    Lock several personas ((tenant_code, program_id, session_type, source) tuples) in one fixed order (by key), for a
    transaction that touches two of them (a session that moved persona): two such transactions can then not wait on
    each other's lock.
    """
    unique = {_lock_key(*persona): persona for persona in personas}
    for key in sorted(unique):
        await lock_persona(conn, *unique[key])


async def _persona_sessions(conn, tenant_code: str, program_id: Any, session_type: str, source: str):
    """submission_id, bot_id, questionnaire_version of the bot sessions of the persona, by submission_id."""
    return await conn.fetch(
        f"""
        SELECT fs.submission_id, fs.meta_data ->> 'bot_id' AS bot_id, fs.questionnaire_version
        FROM feedback_submissions fs
        JOIN submissions s ON s.submission_id = fs.submission_id AND s.tenant_code = fs.tenant_code
        WHERE fs.tenant_code = $1 AND s.program_id IS NOT DISTINCT FROM $2::uuid
          AND fs.session_type = $3 AND fs.source = $4 AND {BOT_SESSION_SQL}
        ORDER BY fs.submission_id
        """,
        tenant_code, _program_text(program_id), session_type, source,
    )


async def label_for_incoming_bot(
    conn, tenant_code: str, program_id: Any, session_type: str, source: str, submission_id: str, bot_id: str
) -> str:
    """
    The label the session being ingested gets: the rank of `bot_id` among the bot ids stored for the persona by the
    OTHER sessions plus `bot_id` itself. The session's own stored bot id is left out because this event may change it.
    The caller holds the persona lock (lock_persona). The event's row is written with this label, so a re-delivery of
    the same event finds the label unchanged (and is a no-op) instead of flipping it back and forth.
    """
    stored = await _persona_sessions(conn, tenant_code, program_id, session_type, source)
    ids = [row["bot_id"] for row in stored if row["submission_id"] != submission_id]
    return rank_bot_versions([*ids, bot_id])[bot_id]


async def apply_bot_version_ranks(
    conn, tenant_code: str, program_id: Any, session_type: str, source: str
) -> Dict[str, Any]:
    """
    Make the stored label of every bot session of the persona equal to the rank of its bot id, inside the caller's
    transaction. A session whose label changes is sent back through processing: its extracted fields are deleted, the
    PII flag is reset to "not checked" (NULL), meta_data -> 'extraction' is removed and submissions.status is
    'pending'; llm_logs stay (audit trail). Takes the persona lock first (free when the caller already holds it).
    Returns {"sessions": n, "versions": {label: bot id}, "restamped": [submission_id, ...] (ascending)}.
    """
    await lock_persona(conn, tenant_code, program_id, session_type, source)
    rows = await _persona_sessions(conn, tenant_code, program_id, session_type, source)
    ranks = rank_bot_versions(row["bot_id"] for row in rows)
    stale = [row for row in rows if row["questionnaire_version"] != ranks[row["bot_id"]]]
    summary: Dict[str, Any] = {
        "sessions": len(rows),
        "versions": {label: bot_id for bot_id, label in sorted(ranks.items(), key=lambda kv: int(kv[0]))},
        "restamped": [row["submission_id"] for row in stale],
    }
    if len(stale) == 0:
        return summary

    ids: List[str] = [row["submission_id"] for row in stale]
    labels: List[str] = [ranks[row["bot_id"]] for row in stale]
    # Row locks in the order of the extraction save (submissions, then feedback_submissions), so a save that is
    # mid-flight for one of these sessions finishes first and its identity check then sees the new label.
    await conn.fetch(
        "SELECT 1 FROM submissions WHERE tenant_code = $1 AND submission_id = ANY($2::text[]) "
        "ORDER BY submission_id FOR NO KEY UPDATE",
        tenant_code, ids,
    )
    await conn.fetch(
        "SELECT 1 FROM feedback_submissions WHERE tenant_code = $1 AND submission_id = ANY($2::text[]) "
        "ORDER BY submission_id FOR UPDATE",
        tenant_code, ids,
    )
    await conn.execute(
        "DELETE FROM feedback_extracted_data WHERE tenant_code = $1 AND submission_id = ANY($2::text[])",
        tenant_code, ids,
    )
    await conn.execute(
        """
        UPDATE feedback_submissions fs
        SET questionnaire_version = t.version, pii_detected = NULL, meta_data = fs.meta_data - 'extraction',
            updated_at = now()
        FROM unnest($2::text[], $3::text[]) AS t(submission_id, version)
        WHERE fs.tenant_code = $1 AND fs.submission_id = t.submission_id
        """,
        tenant_code, ids, labels,
    )
    await conn.execute(
        "UPDATE submissions SET status = 'pending', updated_at = now() "
        "WHERE tenant_code = $1 AND submission_id = ANY($2::text[]) AND status <> 'pending'",
        tenant_code, ids,
    )
    logger.info(
        f"Questionnaire versions of a persona followed its bot ids: {len(stale)} stored session(s) relabelled and "
        f"sent back through processing (tenant {tenant_code}, session_type {session_type}, source {source})."
    )
    return summary
