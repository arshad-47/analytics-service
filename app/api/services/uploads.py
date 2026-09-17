from typing import Any, Dict, List, Optional, Union
import asyncio
import io
import json
import logging
import threading
import time
import uuid
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer, KafkaException
from fastapi import BackgroundTasks

from app.config import settings
from app.api.validators.uploads import validate_columns
from app.services.storage import get_object_storage, AccessMode
from app.database import operations
from app.database.db import db
from app.services.ingestion_validation import validate_ingestion_schema
from app.api.exceptions import (
    DuplicateFile,
    InvalidCsvColumns,
    RecordNotFound,
    RecordAlreadyProcessing,
    RecordNotPending,
)

logger = logging.getLogger("analytics_service.api.services.uploads")


# ---------------------------------------------------------------------------
# CSV Processing & Formatting Helpers
# ---------------------------------------------------------------------------

def load_csv(csv_file: Union[io.BytesIO, bytes]) -> pd.DataFrame:
    """Parse an in-memory CSV (BytesIO or raw bytes from GCS) into a DataFrame."""
    if isinstance(csv_file, bytes):
        csv_file = io.BytesIO(csv_file)
    else:
        csv_file.seek(0)
    return pd.read_csv(csv_file)


def split_csv(df: pd.DataFrame) -> List[pd.DataFrame]:
    """
    Split a DataFrame into chunks for processing.
    """
    return [df]


def get_csv_value(row_dict: Dict[str, Any], expected_cols: List[str], target_col: str, default: Any = None) -> Any:
    """
    Finds target_col in expected_cols case-insensitively,
    and returns the corresponding value from row_dict case-insensitively.
    Falls back to alias matching for common CSV column name variations.
    """
    _ALIASES = {
        "district": ["matched_district"],
        "organization": ["detected_organization"],
        "transcript link": ["transcript_link"],
        "image urls": ["image_urls"],
        "pdf urls": ["pdf_urls"],
        "session id": ["session"],
        "user location": ["user_location"],
        "report created at": ["created_at"],
        "date of discussion": ["date_of_discussion"],
    }

    target_lower = target_col.lower()
    matched_col = None
    for col in expected_cols:
        if col.lower() == target_lower:
            matched_col = col
            break

    if not matched_col:
        matched_col = target_col

    actual_lower = matched_col.lower()
    for k, v in row_dict.items():
        if k.lower() == actual_lower:
            return v

    aliases = _ALIASES.get(target_lower, [])
    for alias in aliases:
        for k, v in row_dict.items():
            if k.lower() == alias:
                return v

    return default


def parse_csv_list(val) -> List[str]:
    if pd.isna(val) or val is None or not str(val).strip():
        return []
    s = str(val).strip()
    if (s.startswith("[") and s.endswith("]")) or \
       (s.startswith("(") and s.endswith(")")) or \
       (s.startswith("{") and s.endswith("}")):
        s = s[1:-1].strip()

    if "|" in s:
        raw_items = s.split("|")
    else:
        raw_items = s.split(",")

    cleaned = []
    for x in raw_items:
        x_clean = x.strip().strip("'\"").strip()
        if x_clean:
            cleaned.append(x_clean)
    return cleaned


def get_url_field(val):
    urls = parse_csv_list(val)
    if not urls:
        return None
    if len(urls) == 1:
        return urls[0]
    return urls


def clean_segment(s: str) -> str:
    import re
    s = s.strip()
    pattern_num = r'^\s*\d+[\.\)]\s*'
    s = re.sub(pattern_num, '', s).strip()
    pattern_bullet = r'^\s*[\-\*•]\s*'
    s = re.sub(pattern_bullet, '', s).strip()
    return s


def parse_segments(val, delimiter="|") -> List[str]:
    import re
    if pd.isna(val) or val is None or not str(val).strip():
        return []
    s = str(val).strip()
    if delimiter in s:
        raw_segments = s.split(delimiter)
    elif "\n" in s:
        raw_segments = s.split("\n")
    else:
        numbered_pattern = r'(?:^|\s)\d+(?:\.(?!\d)|\))\s*\S'
        has_numbering = len(re.findall(numbered_pattern, s)) >= 2
        if has_numbering:
            raw_segments = re.split(r'(?:^|\s+)\d+(?:\.(?!\d)|\))\s*', s)
        else:
            raw_segments = [s]

    segments = []
    for x in raw_segments:
        x_clean = clean_segment(x)
        if x_clean:
            segments.append(x_clean)
    return segments


def format_datetime(val, with_ms=True, fallback_to_now=True) -> Optional[str]:
    if pd.isna(val) or val is None:
        if not fallback_to_now:
            return None
        val = datetime.utcnow()
    if isinstance(val, str):
        try:
            val = pd.to_datetime(val)
        except Exception:
            return val
    if hasattr(val, "to_pydatetime"):
        val = val.to_pydatetime()
    if isinstance(val, datetime):
        if with_ms:
            return val.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            return val.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(val)


def _is_row_complete(row_dict: Dict[str, Any], report_type: str) -> Any:
    return True, []


def row_to_json(
    row: pd.Series,
    report_type: str,
    event_type: str = "create",
    metadata: Optional[dict] = None,
    expected_cols: Optional[List[str]] = None,
) -> str:
    row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}

    normalized_type = report_type.lower().strip()
    if expected_cols is None:
        raw_cols = settings.STORY_CSV_COLUMN if normalized_type == "story" else settings.DISCUSSION_CSV_COLUMN
        try:
            expected_cols = json.loads(raw_cols)
        except Exception:
            expected_cols = []

    try:
        submission_id = int(get_csv_value(row_dict, expected_cols, "id"))
    except Exception:
        submission_id = get_csv_value(row_dict, expected_cols, "id")

    # No fallback generation — a missing Session ID is left null so the
    # pre-publish schema check (STORY_KAFKA_SCHEMA/DISCUSSION_KAFKA_SCHEMA
    # both require sessionId) catches and reports it, instead of silently
    # inventing an identifier for a row that never had one.
    session_id = get_csv_value(row_dict, expected_cols, "Session ID")

    program_info = None
    leader_info = None
    tenant_code = "mitra"

    if metadata:
        program_info = metadata.get("programInfo")
        leader_info = metadata.get("LeaderCategoryInfo")
        tenant_code = metadata.get("tenantCode", "mitra")

    designation = None
    if normalized_type == "story":
        designation = get_csv_value(row_dict, expected_cols, "Designation")
    if not designation and leader_info and leader_info.get("name"):
        designation = leader_info["name"].split("(")[0].strip()

    published_at_raw = get_csv_value(row_dict, expected_cols, "Report Created At")
    event_published_at = format_datetime(published_at_raw, with_ms=True)

    user_name = get_csv_value(row_dict, expected_cols, "User name")
    organization = get_csv_value(row_dict, expected_cols, "Organization")
    district = get_csv_value(row_dict, expected_cols, "District")

    state = None
    if normalized_type == "discussion":
        state_raw = get_csv_value(row_dict, expected_cols, "User Location")
    else:
        state_raw = get_csv_value(row_dict, expected_cols, "Location")

    if state_raw:
        state_str = str(state_raw)
        if "," in state_str:
            state_str = state_str.split(",")[-1].strip()
        state = state_str.title()

    user_id = None
    author_val = get_csv_value(row_dict, expected_cols, "Author")
    if normalized_type == "discussion":
        user_id = str(author_val) if author_val is not None else str(submission_id)
    else:
        user_id_val = get_csv_value(row_dict, expected_cols, "userId")
        user_id = str(author_val) if author_val is not None else (
            str(user_id_val) if user_id_val is not None else str(submission_id)
        )

    # submission_date (report created at) always comes from "Report Created At" for
    # both types — same source, same field in submissions table.
    submission_date = format_datetime(published_at_raw, with_ms=False)

    # discussion_date (when the discussion took place) is discussion-only and maps
    # to data.discussionDate → discussion_submissions.discussion_date.
    discussion_date = None
    if normalized_type == "discussion":
        discussion_date_raw = get_csv_value(row_dict, expected_cols, "Date of Discussion")
        discussion_date = format_datetime(discussion_date_raw, with_ms=False, fallback_to_now=False)

    pdf_col = "Pdf" if normalized_type == "story" else "PDF Urls"
    original_pdf = get_url_field(get_csv_value(row_dict, expected_cols, pdf_col))
    pdf_urls = None
    if original_pdf:
        pdf_urls = {"original": original_pdf}

    tags = {
        "state": state,
        "district": district,
        "organization": organization,
        "programId": program_info.get("id") if program_info else None,
        "programName": program_info.get("name") if program_info else None,
        "leaderCategoryId": leader_info.get("id") if leader_info else None,
        "leaderCategoryName": leader_info.get("name") if leader_info else None,
    }

    if normalized_type == "discussion":
        participants_data = []
        role_cols = settings.get_discussion_participants_map()

        total_role = None
        for role, col_name in role_cols.items():
            if role.lower() == "participant count" or (col_name and col_name.lower() == "participant count"):
                total_role = role
                break

        total_count = None
        if total_role:
            col_name = role_cols[total_role]
            if col_name:
                val = get_csv_value(row_dict, expected_cols, col_name)
                if val is not None:
                    try:
                        total_count = int(val)
                    except Exception:
                        pass

        if total_count is not None and total_count > 0:
            participants_data.append({"role": total_role, "count": total_count})

            for role, col_name in role_cols.items():
                if role == total_role or not col_name:
                    continue
                val = get_csv_value(row_dict, expected_cols, col_name)
                if val is not None:
                    try:
                        count = int(val)
                        if count > 0:
                            participants_data.append({"role": role, "count": count})
                    except Exception:
                        pass

        data = {
            "title": get_csv_value(row_dict, expected_cols, "Title"),
            "userId": user_id,
            "userName": user_name,
            "designation": designation,
            "submissionDate": submission_date,    # report created at → submissions.submission_date
            "discussionDate": discussion_date,     # date of discussion → discussion_submissions.discussion_date
            "imageUrls": parse_csv_list(get_csv_value(row_dict, expected_cols, "Image Urls")),
            "pdfUrls": pdf_urls,
            "transcriptLink": get_csv_value(row_dict, expected_cols, "Transcript Link") or None,
            "challenges": parse_segments(get_csv_value(row_dict, expected_cols, "Challenges")),
            "solutions": parse_segments(get_csv_value(row_dict, expected_cols, "Solutions")),
            "participantsData": participants_data,
            "author": user_id,
            "language": get_csv_value(row_dict, expected_cols, "Language") or "en",
        }
    else:  # story
        data = {
            "title": get_csv_value(row_dict, expected_cols, "Title"),
            "userId": user_id,
            "userName": user_name,
            "designation": designation,
            "submissionDate": submission_date,
            "imageUrls": parse_csv_list(get_csv_value(row_dict, expected_cols, "Images")),
            "pdfUrls": pdf_urls,
            "transcriptLink": get_csv_value(row_dict, expected_cols, "Transcript Link") or None,
            "objective": get_csv_value(row_dict, expected_cols, "Objective"),
            "challenges": parse_segments(get_csv_value(row_dict, expected_cols, "Challenges")),
            "actionSteps": parse_segments(get_csv_value(row_dict, expected_cols, "Action Steps")),
            "impact": get_csv_value(row_dict, expected_cols, "Impact"),
            "duration": get_csv_value(row_dict, expected_cols, "Duration"),
            "blurb": get_csv_value(row_dict, expected_cols, "Blurb"),
            "content": get_csv_value(row_dict, expected_cols, "Content"),
        }

    payload = {
        "submissionId": submission_id,
        "submissionType": report_type,
        "sessionId": session_id,
        "tenantCode": tenant_code,
        "eventType": event_type,
        "eventPublishedAt": event_published_at,
        "tags": tags,
        "data": data,
    }
    return json.dumps(payload, default=str)


def rows_to_json(
    df: pd.DataFrame,
    report_type: str,
    event_type: str = "create",
    metadata: Optional[dict] = None,
):
    normalized_type = report_type.lower().strip()
    raw_cols = settings.STORY_CSV_COLUMN if normalized_type == "story" else settings.DISCUSSION_CSV_COLUMN
    try:
        expected_cols = json.loads(raw_cols)
    except Exception:
        expected_cols = []

    for _, row in df.iterrows():
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        is_complete, missing_fields = _is_row_complete(row_dict, report_type)
        if not is_complete:
            logger.warning(
                "Skipping CSV row due to missing required data: %s",
                missing_fields,
            )
            continue
        yield row_to_json(row, report_type, event_type, metadata, expected_cols=expected_cols)


# ---------------------------------------------------------------------------
# Kafka Producer (singleton, thread-safe)
# ---------------------------------------------------------------------------

_producer: Optional[Producer] = None
_producer_lock = threading.Lock()


def _get_producer() -> Producer:
    """Return a singleton confluent-kafka Producer, creating it on first call."""
    global _producer
    if _producer is not None:
        return _producer
    with _producer_lock:
        if _producer is None:
            _producer = Producer({
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
                "enable.idempotence": True,
            })
    return _producer


def _push_rows_sync(payloads: List[Any]) -> None:
    """
    Runs in a worker thread (via asyncio.to_thread) — produce()/poll() are
    blocking calls. Tracks delivery callbacks specifically for this batch,
    preventing process-wide flush() interference between concurrent CSV uploads.
    """
    if not payloads:
        return

    producer = _get_producer()
    pending_count = len(payloads)
    delivery_error = None
    done_event = threading.Event()
    lock = threading.Lock()

    def _on_delivery(err, _msg):
        nonlocal pending_count, delivery_error
        with lock:
            if err is not None and delivery_error is None:
                delivery_error = err
            pending_count -= 1
            if pending_count <= 0:
                done_event.set()

    for payload, key in payloads:
        producer.produce(
            settings.KAFKA_TOPIC_INGESTION,
            value=payload.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            callback=_on_delivery,
        )
        producer.poll(0)
        with lock:
            if delivery_error is not None:
                raise KafkaException(delivery_error)

    # Poll network events until all delivery callbacks for THIS batch complete
    timeout_seconds = 30.0
    start_time = time.time()
    while not done_event.is_set():
        producer.poll(0.1)
        with lock:
            if delivery_error is not None:
                raise KafkaException(delivery_error)
        if time.time() - start_time > timeout_seconds:
            break

    with lock:
        if delivery_error is not None:
            raise KafkaException(delivery_error)
        if pending_count > 0:
            raise TimeoutError(
                f"Timed out waiting for batch Kafka delivery ({pending_count} of {len(payloads)} remaining)"
            )


# ---------------------------------------------------------------------------
# Inline CSV Processing (replaces Temporal activities)
# ---------------------------------------------------------------------------

async def process_csv_inline(
    record_id: int,
    file_bytes: Optional[bytes] = None,
    df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Processes a single csv_upload record end-to-end:
      1. Use in-memory CSV bytes (or fetch from cloud storage if file_bytes is None)
      2. Validate columns
      3. Look up program/leader metadata from DB
      4. Build Kafka payloads, schema-validate each row
      5. Publish valid rows to Kafka
      6. Update DB status to 'success' (or 'on_hold' on failure)

    Runs as a FastAPI BackgroundTask — any exception is caught, logged, and
    recorded in the csv_uploads row so the caller's 200 response is unaffected.
    """
    record = await operations.get_record(record_id)
    if not record:
        logger.error("process_csv_inline: record %s not found", record_id)
        return

    cloud_storage_path = record["cloud_storage_path"]
    report_type = record["report_type"]

    # --- 1. Fetch/Parse CSV (use pre-parsed df/file_bytes if available, else fetch from storage) ---
    try:
        if df is None:
            if file_bytes is None:
                last_exc: Exception = RuntimeError("unreachable")
                for attempt in range(3):
                    try:
                        storage = get_object_storage()
                        csv_file = await asyncio.to_thread(
                            storage.download_bytes, cloud_storage_path, AccessMode.PRIVATE
                        )
                        break
                    except Exception as exc:
                        last_exc = exc
                        if attempt < 2:
                            wait = 2 ** attempt  # 1s, 2s — then give up
                            logger.warning(
                                "Storage fetch attempt %d/3 failed for record %s (%s); retrying in %ds",
                                attempt + 1, record_id, exc, wait,
                            )
                            await asyncio.sleep(wait)
                else:
                    raise last_exc
            else:
                csv_file = file_bytes
            df = await asyncio.to_thread(load_csv, csv_file)
    except Exception as exc:
        logger.exception("Failed to fetch/load CSV for record %s", record_id)
        error_meta = {
            "stage": "CSV Fetching",
            "error": "Failed to fetch/load CSV",
            "exception": str(exc),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        await operations.update_status(record_id, "on_hold", error_meta)
        return

    # --- 2. Validate columns (only if loaded from GCS; handle_upload already validated file_bytes) ---
    if file_bytes is None:
        is_valid, errors = await asyncio.to_thread(validate_columns, df, report_type)
        if not is_valid:
            logger.warning("Validation failed for record %s: %s", record_id, errors)
            error_meta = {
                "stage": "CSV Column Validation",
                "error": "Invalid CSV schema",
                "validation_errors": errors,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            await operations.update_status(record_id, "on_hold", error_meta)
            return

    await operations.update_status(record_id, "in_progress")

    # --- 3. Look up program / leader category metadata from Postgres ---
    program_info = None
    leader_info = None
    record_meta = record.get("meta_data") or {}
    if isinstance(record_meta, str):
        try:
            record_meta = json.loads(record_meta)
        except json.JSONDecodeError:
            record_meta = {}
    if not isinstance(record_meta, dict):
        record_meta = {}

    tenant_code = record_meta.get("tenant_code")

    try:
        async with db.pool.acquire() as conn:
            leader_row = await conn.fetchrow(
                "SELECT id, name, description, tenant_code FROM leader_category WHERE name = $1 LIMIT 1",
                record.get("leader_category"),
            )
            if leader_row:
                leader_info = {
                    "id": str(leader_row["id"]),
                    "name": leader_row["name"],
                    "description": leader_row["description"],
                }
                tenant_code = leader_row["tenant_code"]

            if leader_row:
                program_row = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code, leaders_id FROM programs WHERE name = $1 AND leaders_id = $2 LIMIT 1",
                    record.get("program_name"),
                    leader_row["id"],
                )
            else:
                program_row = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code, leaders_id FROM programs WHERE name = $1 LIMIT 1",
                    record.get("program_name"),
                )

            if program_row:
                program_info = {
                    "id": str(program_row["id"]),
                    "name": program_row["name"],
                    "description": program_row["description"],
                }
                tenant_code = program_row.get("tenant_code", tenant_code)

            if program_row and not leader_info:
                leader_row_from_program = await conn.fetchrow(
                    "SELECT id, name, description, tenant_code FROM leader_category WHERE id = $1 LIMIT 1",
                    program_row["leaders_id"],
                )
                if leader_row_from_program:
                    leader_info = {
                        "id": str(leader_row_from_program["id"]),
                        "name": leader_row_from_program["name"],
                        "description": leader_row_from_program["description"],
                    }
                    tenant_code = leader_row_from_program.get("tenant_code", tenant_code)
    except Exception as db_exc:
        logger.warning("Failed to query program/leader category metadata from DB: %s", db_exc)

    # Use record's values if DB query didn't find matching rows (do not fabricate placeholders)
    if not leader_info and record.get("leader_category"):
        leader_info = {
            "id": str(uuid.uuid4()),
            "name": record.get("leader_category"),
            "description": f"Leader category: {record.get('leader_category')}",
        }
    if not program_info and record.get("program_name"):
        program_info = {
            "id": str(uuid.uuid4()),
            "name": record.get("program_name"),
            "description": f"Program: {record.get('program_name')}",
        }

    metadata = {
        "programInfo": program_info,
        "LeaderCategoryInfo": leader_info,
        "tenantCode": tenant_code,
    }

    # --- 4. Build Kafka payloads and schema-validate each row ---
    # Run entirely in a thread: for large CSVs (tens of thousands of rows) the
    # json.loads + validate_ingestion_schema loop can take several seconds.  Keeping
    # it on the event loop would freeze every concurrent request (health checks, other
    # uploads) for that duration — a regression vs. the old Temporal activity which
    # ran in an isolated worker process.
    def _build_payloads_sync() -> tuple[list, list, int]:
        chunks = split_csv(df)
        _payloads: list = []
        _schema_errors: list = []
        _row_number = 0

        for chunk in chunks:
            for payload_str in rows_to_json(chunk, report_type, metadata=metadata):
                _row_number += 1
                try:
                    payload_dict = json.loads(payload_str)
                except json.JSONDecodeError as exc:
                    _schema_errors.append({"row": _row_number, "problems": [f"Failed to parse generated payload: {exc}"]})
                    continue

                problems = validate_ingestion_schema(payload_dict, report_type, "create")
                if problems:
                    _schema_errors.append({
                        "row": _row_number,
                        "submissionId": payload_dict.get("submissionId"),
                        "sessionId": payload_dict.get("sessionId"),
                        "problems": problems,
                    })
                    continue

                _payloads.append((payload_str, f"{record_id}-{len(_payloads)}"))

        return _payloads, _schema_errors, _row_number

    payloads, schema_errors, row_number = await asyncio.to_thread(_build_payloads_sync)

    if schema_errors:
        logger.warning(
            "record %s: %d of %d row(s) failed pre-publish schema validation and were skipped: %s",
            record_id, len(schema_errors), row_number, schema_errors,
        )

    # --- 5. Publish to Kafka ---
    if payloads:
        try:
            await asyncio.to_thread(_push_rows_sync, payloads)
        except Exception as exc:
            logger.exception("Kafka push failed for record %s", record_id)
            error_meta = {
                "stage": "Kafka Publishing",
                "error": "Failed to publish record",
                "exception": str(exc),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            # Use "on_hold" — NOT "pending" — so that handle_push (which only
            # accepts status="pending") will reject any retry attempt.  Setting
            # "pending" here would let a manual POST /v1/process/csv/{id} replay
            # the entire file from scratch, republishing rows that already landed
            # in Kafka (partial-batch duplicates).  "on_hold" requires explicit
            # operator action to re-queue, mirroring the original Temporal
            # activity's maximum_attempts=1 policy that existed for exactly
            # this reason.
            await operations.update_status(record_id, "on_hold", error_meta)
            return

    # --- 6. Update status to success ---
    final_meta = {"rows_pushed": len(payloads), "processed_at": datetime.utcnow().isoformat() + "Z"}
    if schema_errors:
        final_meta["schema_validation_errors"] = schema_errors

    await operations.update_status(record_id, "success", final_meta)
    logger.info("CSV record %s processed successfully: %d rows pushed to Kafka", record_id, len(payloads))


# ---------------------------------------------------------------------------
# Service Orchestration Logic
# ---------------------------------------------------------------------------

async def handle_upload(
    report_type: str,
    program_name: str,
    leader_category: str,
    tenant_code: str,
    file_name: str,
    file_bytes: bytes,
    background_tasks: BackgroundTasks,
) -> dict:
    normalized_type = report_type.lower().strip()
    file_size = len(file_bytes)

    # Duplicate check
    is_duplicate = await operations.check_duplicate_file(
        program_name=program_name,
        leader_category=leader_category,
        report_type=normalized_type,
        file_name=file_name,
        file_size=file_size,
    )
    if is_duplicate:
        raise DuplicateFile("FILE ALREADY EXISTS")

    # Validate columns FIRST — reject before touching GCS or the DB, so a
    # malformed CSV never leaves cloud-storage or tracking-table clutter behind.
    try:
        df = await asyncio.to_thread(pd.read_csv, io.BytesIO(file_bytes))
    except Exception as exc:
        raise InvalidCsvColumns([f"Failed to parse CSV: {exc}"])

    is_valid, errors = await asyncio.to_thread(validate_columns, df, normalized_type)
    if not is_valid:
        raise InvalidCsvColumns(errors)

    # Upload to Cloud Storage
    try:
        storage = get_object_storage()
        prefix = settings.STORAGE_CSV_PREFIX.strip("/")
        object_key = f"{prefix}/{uuid.uuid4()}_{file_name}"

        stored_object = await asyncio.to_thread(
            storage.upload_bytes,
            data=file_bytes,
            object_key=object_key,
            content_type="text/csv",
            access_mode=AccessMode.PRIVATE,
        )
        cloud_storage_path = stored_object.key
    except Exception as exc:
        logger.error("Cloud Storage Upload failed: %s", exc)
        raise RuntimeError(f"Cloud Storage Upload failed: {exc}. Please verify storage settings.")

    meta_data = {
        "original_filename": file_name,
        "program_name": program_name,
        "leader_category": leader_category,
        "report_type": normalized_type,
        "tenant_code": tenant_code,
    }

    record_id = await operations.insert_upload_record(
        report_type=normalized_type,
        program_name=program_name,
        leader_category=leader_category,
        cloud_storage_path=cloud_storage_path,
        file_name=file_name,
        file_size=file_size,
        meta_data=meta_data,
        status="pending",
    )

    logger.info(
        "CSV uploaded: id=%s, report_type=%s, status=pending, cloud_storage_path=%s",
        record_id, normalized_type, cloud_storage_path,
    )

    claim_status = await operations.try_claim_for_processing(record_id)
    if claim_status == "success":
        background_tasks.add_task(process_csv_inline, record_id, file_bytes, df)
        logger.info("Scheduled inline CSV processing for upload ID %s", record_id)

    return {
        "message": "Successfully uploaded to cloud",
        "id": record_id,
        "status": "in_progress" if claim_status == "success" else "pending",
    }


async def handle_push(record_id: int, background_tasks: BackgroundTasks) -> dict:
    record = await operations.get_record(record_id)
    if not record:
        raise RecordNotFound("Record not found")

    status = record.get("status")
    if status == "in_progress":
        raise RecordAlreadyProcessing("Record is already being processed")
    if status != "pending":
        raise RecordNotPending("Only pending records can be processed")

    claim_status = await operations.try_claim_for_processing(record_id)
    if claim_status is None:
        raise RecordNotFound("Record not found")
    if claim_status == "in_progress":
        raise RecordAlreadyProcessing("Record is already being processed")

    # Schedule inline processing as a background task (no Temporal)
    background_tasks.add_task(process_csv_inline, record_id)
    return {"status": "success", "message": "CSV processing started"}

