"""
Questionnaire version of a Mitra bot session = the RANK of its bot id inside its persona (the pure rule is
app/services/feedback_versions.rank_bot_versions; this module applies it to the stored sessions).

A persona here is (tenant_code, program_id of the submission, session_type, source). The distinct bot ids stored for
it (feedback_submissions.meta_data ->> 'bot_id') are ranked by number: lowest = V1, next = V2 ... When a new bot id
arrives that is LOWER than stored ones, their ranks shift, so the stored sessions whose label changed are relabelled
in the ingest transaction that brought the new bot (apply_bot_version_ranks). A relabelled session changes meaning
(a field that exists in one questionnaire version only is 'N/A' by version). Whether that matters depends on the
persona's ACTIVE extraction config (relabel_needs_reextraction): when the old and the new label ask exactly the same
fields, the stored extraction still describes the session, so only the label is updated and nothing is processed
again (no paid LLM call, status untouched). When some field is asked in one version only, the extraction is stale and
the session goes back through processing (extracted rows deleted, PII flag reset, status 'pending'), exactly like a
session that moved persona (operations._write_feedback_submission); the caller must then dispatch it (the consumer
does so in real-time mode, batch mode picks it up).

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


def relabel_needs_reextraction(config: Any, old_version: Optional[str], new_version: Optional[str]) -> bool:
    """
    Pure decision: must a stored session relabelled `old_version` -> `new_version` be extracted again?
    `config` is the persona's active ExtractionConfig (app.services.feedback_extraction), or None when none could be
    loaded. Only the NAMES of config.fields_for(old) and config.fields_for(new) are compared: when they are equal the
    stored rows hold exactly the fields the new label asks, so nothing about the extraction is stale (applies_when
    chains are deliberately not looked at: they depend on the extracted values, which do not change). With no config
    the answer is True, the safe side: the stored rows can not be shown to still describe the session.
    """
    if config is None:
        return True
    if old_version == new_version:
        return False
    old_names = {spec.name for spec in config.fields_for(old_version)}
    new_names = {spec.name for spec in config.fields_for(new_version)}
    return old_names != new_names


async def _persona_extraction_config(conn, tenant_code: str, program_id: Any, session_type: str, source: str) -> Any:
    """
    The ExtractionConfig of the persona's ACTIVE extraction config, or None when there is none or it is unusable (the
    relabel then takes the safe side, see relabel_needs_reextraction). Imported here because the extraction operations
    import app.database.operations, which imports this module.
    """
    from app.database.feedback_extraction_operations import ExtractionSetupError, load_extraction_config

    try:
        loaded = await load_extraction_config(conn, tenant_code, session_type, _program_text(program_id), source)
    except (ExtractionSetupError, ValueError):
        # no active config, or a malformed one (ExtractionConfigError is a ValueError); a database error is NOT caught
        logger.warning(
            f"No usable active extraction config for a relabelled persona (tenant {tenant_code}, session_type "
            f"{session_type}, source {source}); its relabelled sessions are extracted again."
        )
        return None
    return loaded["config"]


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
    transaction. Takes the persona lock first (free when the caller already holds it). For a session whose label
    changes, relabel_needs_reextraction (on the persona's active extraction config) decides:
      * the same fields are asked: only questionnaire_version and updated_at change; extracted rows, the PII and
        abusive-language flags,
        meta_data -> 'extraction' and the submission status stay exactly as they are ("label only");
      * otherwise the session is "reset": its extracted fields are deleted, the PII and abusive-language flags are
        NULL (not checked),
        meta_data -> 'extraction' is removed and submissions.status is 'pending'.
    llm_logs stay (audit trail). Returns {"sessions": n, "versions": {label: bot id},
    "restamped": [submission_id, ...] (every relabelled session, ascending),
    "reset": [...] (the subset sent back to 'pending'; the caller dispatches them),
    "label_only": [...] (the rest)}.
    """
    await lock_persona(conn, tenant_code, program_id, session_type, source)
    rows = await _persona_sessions(conn, tenant_code, program_id, session_type, source)
    ranks = rank_bot_versions(row["bot_id"] for row in rows)
    stale = [row for row in rows if row["questionnaire_version"] != ranks[row["bot_id"]]]
    summary: Dict[str, Any] = {
        "sessions": len(rows),
        "versions": {label: bot_id for bot_id, label in sorted(ranks.items(), key=lambda kv: int(kv[0]))},
        "restamped": [row["submission_id"] for row in stale],
        "reset": [],
        "label_only": [],
    }
    if len(stale) == 0:
        return summary

    config = await _persona_extraction_config(conn, tenant_code, program_id, session_type, source)
    decisions: Dict[tuple, bool] = {}
    reset_rows: List[Any] = []
    label_only_rows: List[Any] = []
    for row in stale:
        pair = (row["questionnaire_version"], ranks[row["bot_id"]])
        if pair not in decisions:
            decisions[pair] = relabel_needs_reextraction(config, *pair)
        (reset_rows if decisions[pair] else label_only_rows).append(row)
    summary["reset"] = [row["submission_id"] for row in reset_rows]
    summary["label_only"] = [row["submission_id"] for row in label_only_rows]

    ids: List[str] = [row["submission_id"] for row in stale]
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
    if len(label_only_rows) > 0:
        await conn.execute(
            """
            UPDATE feedback_submissions fs
            SET questionnaire_version = t.version, updated_at = now()
            FROM unnest($2::text[], $3::text[]) AS t(submission_id, version)
            WHERE fs.tenant_code = $1 AND fs.submission_id = t.submission_id
            """,
            tenant_code, summary["label_only"], [ranks[row["bot_id"]] for row in label_only_rows],
        )
    if len(reset_rows) > 0:
        reset_ids: List[str] = summary["reset"]
        await conn.execute(
            "DELETE FROM feedback_extracted_data WHERE tenant_code = $1 AND submission_id = ANY($2::text[])",
            tenant_code, reset_ids,
        )
        await conn.execute(
            """
            UPDATE feedback_submissions fs
            SET questionnaire_version = t.version, pii_detected = NULL, abusive_language = NULL,
                meta_data = fs.meta_data - 'extraction',
                updated_at = now()
            FROM unnest($2::text[], $3::text[]) AS t(submission_id, version)
            WHERE fs.tenant_code = $1 AND fs.submission_id = t.submission_id
            """,
            tenant_code, reset_ids, [ranks[row["bot_id"]] for row in reset_rows],
        )
        # The state / district copied from the old extraction go with it; the new extraction writes them again.
        await conn.execute(
            "UPDATE submissions SET state = NULL, district = NULL, updated_at = now() "
            "WHERE tenant_code = $1 AND submission_id = ANY($2::text[])",
            tenant_code, reset_ids,
        )
        await conn.execute(
            "UPDATE submissions SET status = 'pending', updated_at = now() "
            "WHERE tenant_code = $1 AND submission_id = ANY($2::text[]) AND status <> 'pending'",
            tenant_code, reset_ids,
        )
    logger.info(
        f"Questionnaire versions of a persona followed its bot ids: {len(stale)} stored session(s) relabelled, "
        f"{len(reset_rows)} sent back through processing, {len(label_only_rows)} relabelled only (same fields asked) "
        f"(tenant {tenant_code}, session_type {session_type}, source {source})."
    )
    return summary
