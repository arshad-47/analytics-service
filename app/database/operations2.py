import json
import logging
import urllib.parse
from typing import Dict, Any, Optional, List
import asyncpg
from datetime import datetime

logger = logging.getLogger("analytics_service.operations")

def _normalize_string_list(value: Any) -> Optional[str]:
    """Helper to convert list or string to database TEXT."""
    if value is None:
        return None
    if isinstance(value, list):
        return "\n".join(str(item) for item in value)
    return str(value)

def _normalize_statement_list(value: Any) -> Optional[List[str]]:
    """Helper to convert a value into a Postgres TEXT[] value — one array element
    per discrete statement. Used for discussion_submissions.challenges/solutions;
    thematic_activity.py and pii_and_abusive_activity.py (which also reads/writes
    this column) get back a native Python list with no decoding needed, since
    asyncpg handles the TEXT[] binding automatically in both directions.

    A native array is used instead of delimiter-joining a single TEXT value because
    a delimiter character (e.g. "|") occurring naturally inside one statement would
    otherwise corrupt the round-trip by splitting that statement into multiple
    fragments — a Postgres array has no such ambiguity."""
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]

def _normalize_url_list(value: Any) -> Optional[List[str]]:
    """Helper to convert list or single string URL to database TEXT[].
    Returns None when the key was genuinely absent (vs. explicitly empty), so
    partial updates can distinguish "not provided" from "cleared" via COALESCE."""
    if value is None:
        return None
    if isinstance(value, list):
        return [str(url) for url in value]
    if not value:
        return []
    return [str(value)]

def _strip_base_url(url: str) -> str:
    """Strips scheme+host from an absolute URL, keeping only the path (e.g. /bucket/folder/file.pdf)."""
    url_str = str(url).strip()
    parsed = urllib.parse.urlparse(url_str)
    if parsed.scheme and parsed.netloc:
        return parsed.path
    return url_str

def _normalize_media_url_list(value: Any) -> Optional[List[str]]:
    """Like _normalize_url_list, but stores only the path — never the base URL."""
    normalized = _normalize_url_list(value)
    return None if normalized is None else [_strip_base_url(url) for url in normalized]

def _normalize_pdf_urls(value: Any) -> tuple:
    """Handle pdfUrls in new object format {original, masked}.
    Returns (original_urls, masked_urls); each is None when absent so COALESCE
    preserves the existing column on partial updates."""
    if value is None:
        return None, None
    if isinstance(value, dict):
        original = _normalize_media_url_list(value.get("original"))
        masked = _normalize_media_url_list(value.get("masked"))
        return original, masked
    # Legacy fallback: plain string or list
    return _normalize_media_url_list(value), None

async def upsert_metadata(conn: asyncpg.Connection, tags: Dict[str, Any], tenant_code: str) -> tuple:
    """
    Safely upserts parent metadata tables: tenant, leader_category, and programs.
    Reads from the new top-level `tags` dict with flattened keys.
    Returns (program_id, leader_id) as UUIDs.
    """
    # 1. Upsert Tenant
    tenant_name = tenant_code.capitalize()
    await conn.execute(
        """
        INSERT INTO tenant (name, code)
        VALUES ($1, $2)
        ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
        """,
        tenant_name, tenant_code
    )

    leader_id = None
    program_id = None

    # 2. Upsert Leader Category (from flattened tags)
    leader_id = tags.get("leaderCategoryId")
    if leader_id:
        await conn.execute(
            """
            INSERT INTO leader_category (id, name, description, tenant_code)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET 
                name = EXCLUDED.name, 
                description = EXCLUDED.description, 
                updated_at = now()
            """,
            leader_id,
            tags.get("leaderCategoryName", ""),
            None,  # TODO: description can be added once the PM provides it 
            tenant_code
        )

    # 3. Upsert Programs (depends on leader category)
    program_id = tags.get("programId")
    if program_id and leader_id:
        await conn.execute(
            """
            INSERT INTO programs (id, leaders_id, name, description, tenant_code)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE SET 
                leaders_id = EXCLUDED.leaders_id, 
                name = EXCLUDED.name, 
                description = EXCLUDED.description, 
                updated_at = now()
            """,
            program_id,
            leader_id,
            tags.get("programName", ""),
            None,  # TODO: description can be added once the PM provides it 
            tenant_code
        )

    return program_id, leader_id

async def upsert_participant_metrics(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    submission_type: str,
    participants_data: List[Dict[str, Any]],
) -> None:
    """
    Upserts role-based participant counts from Kafka `participantsData` into the
    dynamic KPI metrics tables (metric_definitions + submission_metrics).
    participants_data is always a full snapshot (not a delta), so existing metrics
    for this submission are cleared and replaced rather than merged.
    """
    await conn.execute(
        "DELETE FROM submission_metrics WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant_code
    )

    for entry in participants_data:
        role = entry.get("role")
        count = entry.get("count")
        if not role or count is None:
            continue
        metric_code = str(role).strip()

        await conn.execute(
            """
            INSERT INTO metric_definitions (code, label, value_type, submission_type, is_active)
            VALUES ($1, $2, 'numeric', $3, TRUE)
            ON CONFLICT (code) DO UPDATE SET
                label = EXCLUDED.label,
                value_type = EXCLUDED.value_type,
                submission_type = EXCLUDED.submission_type,
                is_active = EXCLUDED.is_active
            """,
            metric_code, metric_code, submission_type
        )

        await conn.execute(
            """
            INSERT INTO submission_metrics (submission_id, tenant_code, metric_code, numeric_value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (submission_id, tenant_code, metric_code) DO UPDATE SET
                numeric_value = EXCLUDED.numeric_value
            """,
            submission_id, tenant_code, metric_code, int(count)
        )

async def delete_submission(conn: asyncpg.Connection, submission_id: str, tenant_code: str) -> bool:
    """
    Deletes a submission and cascades down to specific table rows.
    """
    result = await conn.execute(
        "DELETE FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant_code
    )
    # result string format: e.g. "DELETE 1"
    deleted = result.startswith("DELETE") and not result.endswith("0")
    if deleted:
        logger.info(f"Deleted submission {submission_id} under tenant {tenant_code}")
    else:
        logger.warning(f"Submission {submission_id} not found under tenant {tenant_code} for deletion")
    return deleted

async def insert_or_update_submission(
    conn: asyncpg.Connection,
    event_payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Performs transactional write for submission master and type-specific tables.
    Handles the new Kafka event format with top-level `tags` and delta-only `newValues`.
    """
    submission_id = str(event_payload["submissionId"])
    tenant_code = event_payload["tenantCode"]
    submission_type = event_payload["submissionType"]
    session_id = event_payload.get("sessionId")
    event_type = event_payload.get("eventType", "create").lower().strip()
    tags = event_payload.get("tags", {})

    # Resolve the data payload based on event type.
    # For updates, newValues is delta-only — do NOT merge in oldValues. Downstream
    # activities (e.g. pii_and_abusive_activity) overwrite these same columns with
    # masked text after ingestion; filling gaps from the producer's raw oldValues
    # would silently revert already-masked fields. Absent fields stay None here and
    # are preserved via COALESCE in the SQL below, leaving them untouched in the DB.
    if event_type == "update":
        data = event_payload.get("newValues", {})
    else:
        data = event_payload.get("data", {})

    # Start database transaction if not already handled
    async with conn.transaction():
        # Upsert parent metadata tables (reads from flattened tags)
        program_id, leader_id = await upsert_metadata(conn, tags, tenant_code)

        # Parse submission date. On a partial update where submissionDate is absent,
        # leave it None (rather than defaulting to now()) so COALESCE below preserves
        # the existing value instead of overwriting it with a "real" default.
        sub_date_str = data.get("submissionDate")
        if sub_date_str:
            submission_date = datetime.fromisoformat(sub_date_str.replace("Z", "+00:00"))
        elif event_type != "update":
            submission_date = datetime.utcnow()
        else:
            submission_date = None

        # Read state/district/organization from tags (new format)
        state = tags.get("state")
        district = tags.get("district")
        organization = tags.get("organization")

        # 1. Upsert Master Submission record
        # Note: status is initialized as 'pending' for new creates
        try:
            submission_uuid_row = await conn.fetchrow(
                """
                INSERT INTO submissions (
                    session_id, submission_id, tenant_code, submission_type, user_id, user_name, role,
                    state, district, organization, submission_date, program_id, leader_id, status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (submission_id, tenant_code) DO UPDATE SET
                    session_id = COALESCE(EXCLUDED.session_id, submissions.session_id),
                    user_id = COALESCE(EXCLUDED.user_id, submissions.user_id),
                    user_name = COALESCE(EXCLUDED.user_name, submissions.user_name),
                    role = COALESCE(EXCLUDED.role, submissions.role),
                    state = COALESCE(EXCLUDED.state, submissions.state),
                    district = COALESCE(EXCLUDED.district, submissions.district),
                    organization = COALESCE(EXCLUDED.organization, submissions.organization),
                    submission_date = COALESCE(EXCLUDED.submission_date, submissions.submission_date),
                    program_id = COALESCE(EXCLUDED.program_id, submissions.program_id),
                    leader_id = COALESCE(EXCLUDED.leader_id, submissions.leader_id),
                    status = EXCLUDED.status,
                    updated_at = now()
                RETURNING id, status;
                """,
                session_id,
                submission_id,
                tenant_code,
                submission_type,
                data.get("userId"),
                data.get("userName"),
                data.get("designation"), # Designation maps to role
                state,
                district,
                organization,
                submission_date,
                program_id,
                leader_id,
                "pending"
            )
        except asyncpg.exceptions.UniqueViolationError as e:
            if e.constraint_name == "submissions_session_id_key":
                raise ValueError(
                    f"session_id {session_id!r} is already associated with a different submission; "
                    f"rejecting submission {submission_id} under tenant {tenant_code}."
                ) from e
            raise

        db_sub_uuid = submission_uuid_row["id"]
        db_sub_status = submission_uuid_row["status"]

        # 2. Upsert specific payload type
        normalized_type = submission_type.lower().strip()
        if "story" in normalized_type:
            # Upsert story submission
            # Update if exists, else insert
            row_exists = await conn.fetchval(
                "SELECT 1 FROM story_submissions WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code
            )
            challenges_joined = _normalize_statement_list(data.get("challenges"))
            action_steps_joined = _normalize_statement_list(data.get("actionSteps"))
            image_urls = _normalize_media_url_list(data.get("imageUrls"))
            pdf_urls, masked_pdf_urls = _normalize_pdf_urls(data.get("pdfUrls"))

            if row_exists:
                await conn.execute(
                    """
                    UPDATE story_submissions SET
                        title = COALESCE($3, title),
                        objective = COALESCE($4, objective),
                        challenge = COALESCE($5, challenge),
                        action_steps = COALESCE($6, action_steps),
                        impact = COALESCE($7, impact),
                        duration = COALESCE($8, duration),
                        blurb = COALESCE($9, blurb),
                        content = COALESCE($10, content),
                        image_urls = COALESCE($11, image_urls),
                        pdf_urls = COALESCE($12, pdf_urls),
                        masked_pdf_urls = COALESCE($13, masked_pdf_urls),
                        transcript_link = COALESCE($14, transcript_link),
                        updated_at = now()
                    WHERE submission_id = $1 AND tenant_code = $2
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    data.get("objective"),
                    challenges_joined,
                    action_steps_joined,
                    data.get("impact"),
                    data.get("duration"),
                    data.get("blurb"),
                    data.get("content"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink")
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO story_submissions (
                        submission_id, tenant_code, title, objective, challenge, action_steps,
                        impact, duration, blurb, content, image_urls, pdf_urls, masked_pdf_urls, transcript_link
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    data.get("objective"),
                    challenges_joined,
                    action_steps_joined,
                    data.get("impact"),
                    data.get("duration"),
                    data.get("blurb"),
                    data.get("content"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink")
                )

        elif "discussion" in normalized_type:
            # Upsert discussion submission
            row_exists = await conn.fetchval(
                "SELECT 1 FROM discussion_submissions WHERE submission_id = $1 AND tenant_code = $2",
                submission_id, tenant_code
            )
            challenges_joined = _normalize_statement_list(data.get("challenges"))
            solutions_joined = _normalize_statement_list(data.get("solutions"))
            image_urls = _normalize_media_url_list(data.get("imageUrls"))
            pdf_urls, masked_pdf_urls = _normalize_pdf_urls(data.get("pdfUrls"))

            if row_exists:
                await conn.execute(
                    """
                    UPDATE discussion_submissions SET
                        title = COALESCE($3, title),
                        challenges = COALESCE($4, challenges),
                        solutions = COALESCE($5, solutions),
                        author = COALESCE($6, author),
                        language = COALESCE($7, language),
                        image_urls = COALESCE($8, image_urls),
                        pdf_urls = COALESCE($9, pdf_urls),
                        masked_pdf_urls = COALESCE($10, masked_pdf_urls),
                        transcript_link = COALESCE($11, transcript_link),
                        updated_at = now()
                    WHERE submission_id = $1 AND tenant_code = $2
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    challenges_joined,
                    solutions_joined,
                    data.get("author"),
                    data.get("language"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink")
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO discussion_submissions (
                        submission_id, tenant_code, title, challenges, solutions,
                        author, language, image_urls, pdf_urls, masked_pdf_urls, transcript_link
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    submission_id, tenant_code,
                    data.get("title"),
                    challenges_joined,
                    solutions_joined,
                    data.get("author"),
                    data.get("language"),
                    image_urls,
                    pdf_urls,
                    masked_pdf_urls,
                    data.get("transcriptLink")
                )

            # Dynamic KPI metrics: participantsData is a full snapshot when present;
            # absent on a partial update means it didn't change, so leave existing
            # submission_metrics rows untouched.
            participants_data = data.get("participantsData")
            if participants_data is not None:
                await upsert_participant_metrics(
                    conn, submission_id, tenant_code, submission_type, participants_data
                )

        logger.info(f"Successfully ingested {submission_type} submission {submission_id} under tenant {tenant_code}")
        return {
            "id": db_sub_uuid,
            "submission_id": submission_id,
            "tenant_code": tenant_code,
            "status": db_sub_status
        }

async def update_submission_status(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    status: str,
    process_status: Optional[Dict[str, Any]] = None
) -> None:
    """
    Updates the execution status and status metadata on the master submissions table.
    """
    if process_status:
        process_status_json = json.dumps(process_status)
        await conn.execute(
            """
            UPDATE submissions 
            SET status = $3, process_status = $4, updated_at = now()
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code, status, process_status_json
        )
    else:
        await conn.execute(
            """
            UPDATE submissions 
            SET status = $3, updated_at = now()
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code, status
        )

async def insert_llm_log(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    model_name: str,
    analysis_type: str,
    prompt_version_id: str,
    prompt_tokens: int,
    completion_tokens: int,
    status: str,
    error_message: Optional[str] = None,
    meta_data: Optional[Dict[str, Any]] = None
) -> None:
    """
    Logs metadata about individual LLM executions to the database for token tracking.
    """
    meta_json = json.dumps(meta_data) if meta_data else None
    await conn.execute(
        """
        INSERT INTO llm_logs (
            submission_id, tenant_code, model_name, analysis_type, prompt_version_id,
            prompt_tokens, completion_tokens, status, error_message, meta_data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        submission_id,
        tenant_code,
        model_name,
        analysis_type,
        prompt_version_id,
        prompt_tokens,
        completion_tokens,
        status,
        error_message,
        meta_json
    )

async def insert_analysis_result(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    theme_id: Optional[str],
    analysis_type: str,
    statements: str,
    statement_type: str,
    confidence_score: Optional[float] = None,
    justification: Optional[str] = None,
    category_type: Optional[str] = None,
    similarity_score: Optional[float] = None,
    multi_theme_mapped: bool = False,
    meta_data: Optional[Dict[str, Any]] = None,
    improvement_environment: Optional[str] = None
) -> None:
    """
    Saves theme/environmental extraction analysis output to database.
    """
    if similarity_score is not None:
        similarity_score = round(similarity_score, 2)
    meta_json = json.dumps(meta_data) if meta_data else None
    await conn.execute(
        """
        INSERT INTO analysis_results (
            submission_id, tenant_code, theme_id, analysis_type, statements,
            statement_type, confidence_score, justification, category_type,
            similarity_score, multi_theme_mapped, meta_data, improvement_environment
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """,
        submission_id,
        tenant_code,
        theme_id,
        analysis_type,
        statements,
        statement_type,
        confidence_score,
        justification,
        category_type,
        similarity_score,
        multi_theme_mapped,
        meta_json,
        improvement_environment
    )


async def insert_ranking_result(
    conn: asyncpg.Connection,
    submission_id: str,
    tenant_code: str,
    criteria_data: Dict[str, Any],
    composite_score: float,
    tier: Optional[str],
    overall_summary: Optional[str],
    meta_data: Optional[Dict[str, Any]] = None
) -> None:
    """
    Saves LLM-generated story rating/ranking output to the ranking table.
    """
    await conn.execute(
        """
        INSERT INTO ranking (
            submission_id, tenant_code, criteria_data, composite_score, tier, overall_summary, meta_data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        submission_id,
        tenant_code,
        json.dumps(criteria_data),
        composite_score,
        tier,
        overall_summary,
        json.dumps(meta_data) if meta_data else None
    )


async def get_submission_type_and_payload(conn: asyncpg.Connection, submission_id: str, tenant_code: str) -> tuple:
    """
    Retrieves the submission type and payload details for story or discussion submissions.
    """
    sub_row = await conn.fetchrow(
        "SELECT submission_type FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant_code
    )
    if not sub_row:
        raise ValueError(f"Submission {submission_id} not found in database.")
    
    sub_type = sub_row["submission_type"].lower().strip()
    
    if "story" in sub_type:
        payload_row = await conn.fetchrow(
            """
            SELECT title, objective, challenge, action_steps, impact, duration, blurb, content, image_urls, pdf_urls, pii_masked_at, abusive_masked_at
            FROM story_submissions
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code
        )
    elif "discussion" in sub_type:
        payload_row = await conn.fetchrow(
            """
            SELECT title, challenges, solutions, author, language, image_urls, pii_masked_at, abusive_masked_at 
            FROM discussion_submissions 
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code
        )
    else:
        raise ValueError(f"Unsupported submission type: {sub_type}")

    if not payload_row:
        raise ValueError(f"Payload details not found for {submission_id} under type {sub_type}.")

    return sub_type, dict(payload_row)

# CSV Upload Tracking (csv_uploads)
async def check_duplicate_file(
    program_name: str,
    leader_category: str,
    report_type: str,
    file_name: str,
    file_size: int,
) -> bool:
    """Return True if a matching file already exists in the tracker."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT 1 FROM csv_uploads
            WHERE program_name = $1
              AND leader_category = $2
              AND report_type = $3
              AND file_name = $4
              AND file_size = $5
            LIMIT 1
            """,
            program_name,
            leader_category,
            report_type,
            file_name,
            file_size,
        )
        return exists is not None


async def insert_upload_record(
    report_type: str,
    program_name: str,
    leader_category: str,
    cloud_storage_path: str,
    file_name: Optional[str] = None,
    file_size: Optional[int] = None,
    meta_data: Optional[Dict[str, Any]] = None,
    status: str = "pending",
) -> int:
    """Insert a new row with the given status. Returns the new row's id."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO csv_uploads
                    (report_type, program_name, leader_category, cloud_storage_path,
                     file_name, file_size, meta_data, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                RETURNING id
                """,
                report_type,
                program_name,
                leader_category,
                cloud_storage_path,
                file_name,
                file_size,
                json.dumps(meta_data or {}),
                status,
            )
            record_id = row["id"]
            logger.info("Inserted csv_uploads record %s (status=%s)", record_id, status)
            return record_id
        except asyncpg.UniqueViolationError:
            from app.api.exceptions import DuplicateFile
            raise DuplicateFile("FILE ALREADY EXISTS")


async def get_record(record_id: int) -> Optional[dict]:
    """Fetch a single tracker record by id."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM csv_uploads WHERE id = $1",
            record_id,
        )
        return dict(row) if row else None


async def update_status(
    record_id: int,
    status: str,
    meta_data: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Update status, optionally merging new keys into meta_data.
    """
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        if meta_data is not None:
            await conn.execute(
                """
                UPDATE csv_uploads
                SET status = $1,
                    meta_data = COALESCE(meta_data, '{}'::jsonb) || $2::jsonb
                WHERE id = $3
                """,
                status,
                json.dumps(meta_data),
                record_id,
            )
        else:
            await conn.execute(
                "UPDATE csv_uploads SET status = $1 WHERE id = $2",
                status,
                record_id,
            )


async def list_by_status(status: str) -> list:
    """List all tracker records with a given status."""
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM csv_uploads WHERE status = $1 ORDER BY created_at",
            status,
        )
        return [dict(r) for r in rows]


async def try_claim_for_processing(record_id: int) -> Optional[str]:
    """
    Atomically set status to 'in_progress' if record exists and its status is not 'in_progress'.
    Returns:
      - 'success' if successfully claimed/updated.
      - 'in_progress' if it is already in progress.
      - None if the record does not exist.
    """
    from app.database.db import db
    if not db.pool:
        await db.connect()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE csv_uploads
            SET status = 'in_progress'
            WHERE id = $1 AND status != 'in_progress'
            RETURNING status
            """,
            record_id,
        )
        if row:
            logger.info("Atomically claimed record %s for processing", record_id)
            return "success"

        # If update did not match any row, determine if it doesn't exist or is in_progress
        exists = await conn.fetchval(
            "SELECT 1 FROM csv_uploads WHERE id = $1 LIMIT 1",
            record_id,
        )
        if not exists:
            return None
        return "in_progress"

