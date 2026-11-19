from typing import Any, Dict, List, Optional, Tuple, Union
import asyncio
import io
import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
import pandas as pd
from confluent_kafka import Producer, KafkaException
from fastapi import BackgroundTasks

from app.config import settings
from app.api.validators.uploads import validate_columns
from app.services.storage.factory import get_object_storage
from app.services.storage.models import AccessMode
from app.database import operations
from app.database import feedback_operations
from app.database.feedback_operations import AmbiguousFeedbackConfigError
from app.database.db import db
from app.services.datetime_parsing import parse_datetime_text
from app.services.ingestion_validation import validate_ingestion_schema
from app.services.feedback_adapter_config import (
    is_safe_code,
    resolve_column_map,
    source_label_problem,
    validate_adapter_config,
)
from app.services.feedback_config_validation import validate_feedback_config_keys
from app.services.feedback_constants import (
    ADAPTER_CONVERSATION_ROWS,
    ADAPTER_WIDE_FORM,
    COMPLETION_COMPLETED,
    CONFIG_KEY_INGESTION_SCHEMA,
    CONFIG_KEY_MAX_CSV_UPLOAD_BYTES,
    FEEDBACK_REPORT_TYPE,
)
from app.api.services import feedback_gforms, feedback_uploads
from app.api.exceptions import (
    AmbiguousFeedbackTarget,
    DuplicateFile,
    FeedbackProgramNotFound,
    InvalidCsvColumns,
    InvalidFeedbackFile,
    RecordNotFound,
    RecordAlreadyProcessing,
    RecordNotPending,
)

logger = logging.getLogger("analytics_service.api.services.uploads")

def upload_csv(file_bytes: bytes, report_type: str, original_filename: str) -> str:
    """Upload raw CSV bytes to the configured storage bucket and return the object key."""
    if not settings.STORAGE_PRIVATE_BUCKET:
        raise ValueError("STORAGE_PRIVATE_BUCKET is not configured in settings.")

    prefix = (settings.STORAGE_CSV_PREFIX or settings.CSV_BLOB_UPLOADS).strip("/")
    safe_filename = re.sub(r"[^A-Za-z0-9._-]", "_", os.path.basename(original_filename))
    filename = f"{uuid.uuid4()}_{safe_filename}"
    object_key = f"{prefix}/{filename}" if prefix else filename

    storage = get_object_storage()
    obj = storage.upload_bytes(file_bytes, object_key, content_type="text/csv", access_mode=AccessMode.PRIVATE)
    
    logger.info(f"Uploaded CSV to object storage at {object_key}")
    return obj.key

def fetch_csv(bucket_path: str) -> bytes:
    """Download CSV bytes from the configured storage bucket."""
    storage = get_object_storage()
    data = storage.download_bytes(bucket_path, access_mode=AccessMode.PRIVATE)
    logger.info(f"Fetched CSV from object storage at {bucket_path} ({len(data)} bytes)")
    return data


# CSV Processing & Formatting Helpers
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
    """
    The event text ('2026-11-16T09:09:20.296Z', UTC) of a story / discussion date cell. The text is read by
    parse_datetime_text, the SAME reader as the feedback upload (ISO 8601, day-first numeric, month written out such as
    'November 16, 2026, 9:09:20.296 AM'; numeric month-first is not guessed; an offset is converted to UTC, a value
    without one is read as UTC). An empty cell becomes the current time unless `fallback_to_now` is False (then None);
    text that cannot be read is returned unchanged, so the ingestion schema check rejects it.
    """
    if val is None or (not isinstance(val, str) and pd.isna(val)) or (isinstance(val, str) and val.strip() == ""):
        if not fallback_to_now:
            return None
        val = datetime.now(timezone.utc)
    if isinstance(val, str):
        parsed = parse_datetime_text(val)
        if parsed is None:
            return val
        val = parsed
    if hasattr(val, "to_pydatetime"):
        val = val.to_pydatetime()
    if isinstance(val, datetime):
        if val.tzinfo is not None:
            val = val.astimezone(timezone.utc)
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
            "pri_member_information": {
                "name": get_csv_value(row_dict, expected_cols, "PRI Member Name") or None,
                "designation": get_csv_value(row_dict, expected_cols, "PRI Member Designation") or None,
            },
            "school_representative_information": {
                "name": get_csv_value(row_dict, expected_cols, "School Representative Name") or None,
                "designation": get_csv_value(row_dict, expected_cols, "School Representative Designation") or None,
            },
            "priMemberName":                   get_csv_value(row_dict, expected_cols, "PRI Member Name") or None,
            "priMemberDesignation":            get_csv_value(row_dict, expected_cols, "PRI Member Designation") or None,
            "schoolRepresentativeName":        get_csv_value(row_dict, expected_cols, "School Representative Name") or None,
            "schoolRepresentativeDesignation": get_csv_value(row_dict, expected_cols, "School Representative Designation") or None,
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


# Kafka Producer (singleton, thread-safe)
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


# Inline CSV Processing (replaces Temporal activities)
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

    if report_type == FEEDBACK_REPORT_TYPE:
        await _process_feedback_inline(record_id, record, file_bytes, df)
        return

    # --- 1. Fetch/Parse CSV (use pre-parsed df/file_bytes if available, else fetch from storage) ---
    try:
        if df is None:
            if file_bytes is None:
                last_exc: Exception = RuntimeError("unreachable")
                for attempt in range(3):
                    try:
                        csv_file = await asyncio.to_thread(fetch_csv, cloud_storage_path)
                        break
                    except Exception as exc:
                        last_exc = exc
                        if attempt < 2:
                            wait = 2 ** attempt  # 1s, 2s — then give up
                            logger.warning(
                                "GCS fetch attempt %d/3 failed for record %s (%s); retrying in %ds",
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
            "timestamp": datetime.now(timezone.utc).isoformat(),
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
                "timestamp": datetime.now(timezone.utc).isoformat(),
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
                "timestamp": datetime.now(timezone.utc).isoformat(),
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
    final_meta = {"rows_pushed": len(payloads), "processed_at": datetime.now(timezone.utc).isoformat()}
    if schema_errors:
        final_meta["schema_validation_errors"] = schema_errors

    await operations.update_status(record_id, "success", final_meta)
    logger.info("CSV record %s processed successfully: %d rows pushed to Kafka", record_id, len(payloads))

# Feedback (generic sources: the config's adapter reads the file) — file-level and session-level processing
FEEDBACK_MAX_REPORTED_ERRORS = 200


def _record_meta(record: dict) -> dict:
    """csv_uploads.meta_data as a dict (asyncpg may hand JSONB back as str)."""
    record_meta = record.get("meta_data")
    if isinstance(record_meta, str):
        try:
            record_meta = json.loads(record_meta)
        except json.JSONDecodeError:
            record_meta = None
    if not isinstance(record_meta, dict):
        return {}
    return record_meta


def _distinct_bot_ids(df: pd.DataFrame, config: Dict[str, Any]) -> List[str]:
    """Distinct non-blank bot_id values of a conversation_rows file (the column of the `bot_id` role, matched
    case-insensitively). Informational only: they are recorded on the upload, never matched. A file without the
    column gives []."""
    header = resolve_column_map(config)["bot_id"].lower()
    bot_col = next((c for c in df.columns if str(c).strip().lower() == header), None)
    if bot_col is None:
        return []
    values = df[bot_col].astype(str).str.strip()
    return sorted(set(values[values != ""]))


# The CSV parser of each adapter. The adapter comes from the upload config, never from the source name.
_ADAPTER_PARSERS = {
    ADAPTER_CONVERSATION_ROWS: feedback_uploads.parse_feedback_csv,
    ADAPTER_WIDE_FORM: feedback_gforms.parse_gforms_csv,
}


def _parse_feedback_file(file_bytes: bytes, adapter: str) -> pd.DataFrame:
    """Parse an uploaded feedback CSV with the parser of its config's adapter (every cell a string). Raises
    ValueError (also for an adapter without a parser)."""
    parser = _ADAPTER_PARSERS.get(adapter)
    if parser is None:
        raise ValueError(f"No CSV parser for adapter {adapter!r}")
    return parser(file_bytes)


async def load_feedback_target(
    program_name: str,
    leader_category: str,
    tenant_code: str,
    source: str,
) -> Dict[str, Any]:
    """
    Resolves what a feedback upload targets, ONCE per upload: the program + persona (by name and tenant) and the
    persona's ACTIVE upload config for `source` (a free label: valid when this config exists), whose settings are
    validated here (adapter, max_csv_upload_bytes, ingestion_schema, process_steps, optional expect_stages and the
    adapter's own keys: a config that lacks one or has a wrong type is rejected with a clear message, never replaced
    by a default). Never fabricates a program/leader.
    The config is the program's own; when the program has none, the tenant's program-less default for the source
    (the one rule of feedback_operations.get_active_feedback_config; never another program's).
    Returns {"resolved", "active", "adapter", "max_csv_upload_bytes"}; raises FeedbackProgramNotFound / InvalidFeedbackFile.
    """
    problem = source_label_problem(source)
    if problem is not None:
        raise InvalidFeedbackFile([f"Invalid source: {problem}"])
    if not is_safe_code(tenant_code):
        raise InvalidFeedbackFile(["Invalid tenant_code: 1 to 64 characters of letters, digits, '.', '_' and '-'"])
    try:
        resolved = await feedback_operations.get_program_and_leader(program_name, leader_category, tenant_code)
    except AmbiguousFeedbackConfigError:
        # the exception text lists candidate rows: it is logged by the caller's handler, never returned
        logger.warning("Ambiguous feedback program for tenant %s: more than one program matches the given names",
                       tenant_code)
        raise AmbiguousFeedbackTarget(
            "More than one program matches this program name and leader category for the tenant; "
            "an operator must remove the duplicate before this upload can be accepted."
        )
    if resolved is None:
        raise FeedbackProgramNotFound(
            f"Program {program_name!r} with leader category {leader_category!r} does not exist for tenant {tenant_code!r}."
        )
    try:
        active = await feedback_operations.get_active_feedback_config(
            resolved["program"]["id"], source, tenant_code=tenant_code
        )
    except AmbiguousFeedbackConfigError:
        logger.warning("Ambiguous feedback upload config for tenant %s, source %s: more than one config matches",
                       tenant_code, source)
        raise AmbiguousFeedbackTarget(
            f"More than one upload config of source {source!r} matches this program (it has several session types, "
            "or no config of its own and several program-less defaults); "
            "an operator must disambiguate the configuration before this upload can be accepted."
        )
    if active is None:
        raise InvalidFeedbackFile(
            [f"No upload config for source {source!r} is set up for {resolved['leader']['name']}: "
             "no config for this program and no program-less default"]
        )
    try:
        validate_feedback_config_keys(active["config"])
        adapter = validate_adapter_config(active["config"])
    except ValueError as exc:
        raise InvalidFeedbackFile([f"The {source} upload config of {resolved['leader']['name']} is invalid: {exc}"])
    # The file check reads the config ROW's session_type, the event builder reads the config JSON's: when they
    # disagree (a manual edit) every session would be accepted by the check and then skipped to the DLQ. Refuse the
    # upload here instead (no value is echoed).
    if active["config"].get("session_type") != active["session_type"]:
        raise InvalidFeedbackFile([
            f"The {source} upload config of {resolved['leader']['name']} is invalid: its 'session_type' setting is "
            "missing or differs from the session_type the config is registered under"
        ])
    return {
        "resolved": resolved,
        "active": active,
        "adapter": adapter,
        "max_csv_upload_bytes": active["config"][CONFIG_KEY_MAX_CSV_UPLOAD_BYTES],
    }


async def _resolve_feedback_context(
    df: pd.DataFrame,
    program_name: str,
    leader_category: str,
    tenant_code: str,
    source: str,
    check_file: bool,
    target: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Resolves everything a feedback file needs and runs the file-level checks:
      program + persona (by name and tenant), the active config (load_feedback_target, or the `target` the
      route already loaded) and, when check_file, the file check of the config's ADAPTER:
        conversation_rows: the persona's program picks the config row (its session_type) and the file is checked by
          check_feedback_file: every column of the config's column_map is present and every row carries that
          session_type. The bot ids in the file are recorded, never matched (a persona runs several bots over time;
          one file may hold V1 and V2 sessions).
        wide_form: the file is checked by check_gforms_file (the form must match the persona's config).
    Never fabricates a program/leader. Raises FeedbackProgramNotFound / InvalidFeedbackFile.
    """
    if target is None:
        target = await load_feedback_target(program_name, leader_category, tenant_code, source)
    resolved = target["resolved"]
    active = target["active"]
    adapter = target["adapter"]

    bot_ids: List[str] = []
    if adapter == ADAPTER_WIDE_FORM:
        if check_file:
            file_errors = await asyncio.to_thread(feedback_gforms.check_gforms_file, df, active["config"])
            if file_errors:
                raise InvalidFeedbackFile(list(file_errors))
    elif adapter == ADAPTER_CONVERSATION_ROWS:
        bot_ids = _distinct_bot_ids(df, active["config"])

        if check_file:
            file_errors = await asyncio.to_thread(
                feedback_uploads.check_feedback_file, df, active["config"], active["session_type"]
            )
            if file_errors:
                raise InvalidFeedbackFile(list(file_errors))
    else:
        raise InvalidFeedbackFile([f"The upload config names an adapter without a file check: {adapter!r}"])

    return {
        "program_info": resolved["program"],
        "leader_info": resolved["leader"],
        "session_type": active["session_type"],
        "adapter": adapter,
        "bot_ids": bot_ids,
        "config": active["config"],
        "ingestion_schema": active["config"][CONFIG_KEY_INGESTION_SCHEMA],
        "config_id": active["config_id"],
        "config_version_id": active["config_version_id"],
        "config_version": active["version"],
    }


def _push_dlq_sync(messages: List[Any]) -> None:
    """
    Runs in a worker thread. Publishes skipped feedback sessions to the DLQ topic using the same
    delivery-callback tracking as _push_rows_sync. `messages` = [(value_json_str, headers_dict)].
    Raises if any delivery fails or is not confirmed in time.
    """
    if not messages:
        return

    producer = _get_producer()
    pending_count = len(messages)
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

    for value, headers in messages:
        producer.produce(
            settings.KAFKA_TOPIC_INGESTION_DLQ,
            value=value.encode("utf-8"),
            headers=[(k, str(v).encode("utf-8")) for k, v in headers.items() if v is not None],
            callback=_on_delivery,
        )
        producer.poll(0)
        with lock:
            if delivery_error is not None:
                raise KafkaException(delivery_error)

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
                f"Timed out waiting for DLQ delivery ({pending_count} of {len(messages)} remaining)"
            )


def _build_dlq_messages(skipped: List[dict], record_id: int, tenant_code: str, source: str) -> List[Any]:
    """
    One DLQ message per skipped session / response. Identifiers go in headers (as the consumer's DLQ does);
    the value is a small JSON envelope with the problems and NO conversation text (a session that
    could not be built has no event, and answers may come from children). The original file is in
    the bucket (csv_uploads.cloud_storage_path) for replay after the fix.
    """
    skipped_at = datetime.now(timezone.utc).isoformat()
    messages = []
    for entry in skipped:
        problems = entry["problems"]
        value = {
            "submissionType": FEEDBACK_REPORT_TYPE,
            "eventType": "create",
            "submissionId": entry.get("submissionId"),
            "sessionId": entry.get("sessionId"),
            "tenantCode": tenant_code,
            "source": source,
            "csvUploadId": record_id,
            "problems": problems,
            "skippedAt": skipped_at,
        }
        # A response the Google Forms adapter could not build has no ids: the source row number
        # (and, for a response whose event failed the schema check, its natural key) identify it.
        if entry.get("row") is not None:
            value["row"] = entry["row"]
        if entry.get("sourceRef") is not None:
            value["sourceRef"] = entry["sourceRef"]
        where = f" (row {entry['row']})" if entry.get("row") is not None else ""
        headers = {
            "reason": f"Skipped feedback session{where}: {'; '.join(problems)}"[:1000],
            "submissionId": entry.get("submissionId"),
            "tenantCode": tenant_code,
            "sessionId": entry.get("sessionId"),
        }
        messages.append((json.dumps(value, default=str), headers))
    return messages


class _FeedbackStageFailure(Exception):
    """A step of the feedback background processing failed; the record is put on_hold with this stage."""

    def __init__(self, stage: str, error: str, cause: Exception):
        super().__init__(error)
        self.stage = stage
        self.error = error
        self.cause = cause


def _session_stats(event: dict) -> Tuple[int, int, str]:
    """(message_count, max_sequence, completion_status) of a feedback event."""
    messages = event["data"]["messages"]
    return len(messages), max(m["sequence"] for m in messages), event["data"]["completionStatus"]


def _classify_mitra_events(
    events: List[dict],
    state: Dict[str, Dict[str, Any]],
) -> Tuple[List[Tuple[dict, str]], int, List[dict]]:
    """
    Mitra delta check (plan: Incremental uploads). Compares each session event with the stored state
    from get_mitra_session_state and returns (to_publish, unchanged_count, conflicts):
      - session not stored                                   -> publish, kind "new";
      - stored with a different session_id                   -> conflict (skipped, reported, dead-lettered);
      - stored, event has more messages or a higher sequence -> publish the FULL event, kind "updated"
        (the consumer's row upserts are idempotent and every parentSequence must resolve inside the event);
      - stored, event COMPLETED but stored is not            -> publish, kind "updated" (status flip);
      - otherwise (nothing the file adds)                    -> unchanged: not published, not an error.
    """
    to_publish: List[Tuple[dict, str]] = []
    unchanged = 0
    conflicts: List[dict] = []
    for event in events:
        submission_id = str(event["submissionId"])
        stored = state.get(submission_id)
        if stored is None:
            to_publish.append((event, "new"))
            continue
        if stored["session_id"] != event["sessionId"]:
            conflicts.append({
                "submissionId": event["submissionId"],
                "sessionId": event["sessionId"],
                "problems": [
                    f"submission_id {submission_id} is already stored under a different session_id; "
                    "the same chat session cannot carry two session tokens"
                ],
            })
            continue
        message_count, max_sequence, completion_status = _session_stats(event)
        grown = message_count > stored["message_count"] or max_sequence > stored["max_sequence"]
        status_flip = (
            completion_status == COMPLETION_COMPLETED and stored["completion_status"] != COMPLETION_COMPLETED
        )
        if grown or status_flip:
            to_publish.append((event, "updated"))
        else:
            unchanged += 1
    return to_publish, unchanged, conflicts


def _validate_for_publish(
    candidates: List[Tuple[dict, str]],
    record_id: int,
    ingestion_schema: Dict[str, Any],
) -> Tuple[List[Any], List[dict], List[str]]:
    """
    Schema-validates each (event, kind) with validate_ingestion_schema against `ingestion_schema`, the persona
    config's schema loaded once for the upload. Returns (payloads, skipped,
    kinds_published): the Kafka payloads of the valid events, an entry per event that failed (with
    submissionId / sessionId, plus the response's sourceRef for Google Forms) and the kind of each
    published event.
    """
    payloads: List[Any] = []
    skipped: List[dict] = []
    kinds: List[str] = []
    for event, kind in candidates:
        problems = validate_ingestion_schema(event, FEEDBACK_REPORT_TYPE, "create", feedback_schema=ingestion_schema)
        if problems:
            entry = {
                "submissionId": event.get("submissionId"),
                "sessionId": event.get("sessionId"),
                "problems": problems,
            }
            source_ref = (event.get("data") or {}).get("sourceRef")
            if source_ref is not None:
                entry["sourceRef"] = source_ref
            skipped.append(entry)
            continue
        # ONE key for the whole upload: a Kafka key picks the partition and order is only kept inside a partition, so
        # distinct keys per event would let a topic with several partitions deliver the sessions in another order
        # than build_feedback_events (bot id ascending) put them in. The consumer ranks the bot ids of a persona as
        # the sessions arrive.
        payloads.append((json.dumps(event, default=str), f"{record_id}"))
        kinds.append(kind)
    return payloads, skipped, kinds


async def _prepare_conversation_rows_payloads(
    record_id: int,
    df: pd.DataFrame,
    ctx: Dict[str, Any],
    tenant_code: str,
    source: str,
) -> Tuple[List[Any], List[dict], Dict[str, Any]]:
    """
    conversation_rows session level: one event per session (feedback_uploads.build_feedback_events), the delta check
    against the stored sessions, then schema validation of the sessions to publish. Returns
    (payloads, skipped, report). Raises _FeedbackStageFailure.
    """
    warnings: Dict[str, int] = {}

    def _build_sync() -> tuple:
        return feedback_uploads.build_feedback_events(
            df,
            ctx["config"],
            program_info=ctx["program_info"],
            leader_info=ctx["leader_info"],
            tenant_code=tenant_code,
            source=source,
            report=warnings,
        )

    try:
        events, builder_skipped = await asyncio.to_thread(_build_sync)
    except Exception as exc:
        raise _FeedbackStageFailure("Feedback Event Building", "Failed to build feedback events", exc)

    skipped: List[dict] = [dict(entry) for entry in builder_skipped]

    # A failed state lookup must not turn into blind publishing of the whole file.
    try:
        state = await feedback_operations.get_mitra_session_state(
            tenant_code, [str(event["submissionId"]) for event in events]
        )
    except Exception as exc:
        raise _FeedbackStageFailure(
            "Feedback Delta Lookup", "Failed to read the stored state of the sessions in the file", exc
        )

    def _classify_and_validate_sync() -> tuple:
        to_publish, unchanged, conflicts = _classify_mitra_events(events, state)
        payloads, schema_skipped, kinds = _validate_for_publish(to_publish, record_id, ctx["ingestion_schema"])
        return payloads, conflicts + schema_skipped, kinds, unchanged

    try:
        payloads, more_skipped, kinds, unchanged = await asyncio.to_thread(_classify_and_validate_sync)
    except Exception as exc:
        raise _FeedbackStageFailure("Feedback Event Building", "Failed to validate feedback events", exc)
    skipped.extend(more_skipped)

    report = {
        "rows_read": len(df),
        "sessions_new": kinds.count("new"),
        "sessions_updated": kinds.count("updated"),
        "sessions_unchanged": unchanged,
        # counts only (never text): rows kept with a null message time or a placeholder text, cells whose control
        # characters were replaced; a warning never skips a session
        "warnings": dict(warnings),
        "nul_bytes_removed": int(df.attrs.get(feedback_uploads.NUL_BYTES_ATTR, 0)),
    }
    return payloads, skipped, report


async def _prepare_wide_form_payloads(
    record_id: int,
    df: pd.DataFrame,
    ctx: Dict[str, Any],
    tenant_code: str,
    source: str,
) -> Tuple[List[Any], List[dict], Dict[str, Any]]:
    """
    wide_form response level: the adapter reshapes each wide row into a response, the responses
    already stored (by source_ref) are looked up, events are built only for the new ones and
    schema-validated. Returns (payloads, skipped, report). Raises _FeedbackStageFailure.
    """
    try:
        responses, adapter_skipped, stats = await asyncio.to_thread(
            feedback_gforms.prepare_gforms_responses, df, ctx["config"]
        )
    except Exception as exc:
        raise _FeedbackStageFailure("Feedback Event Building", "Failed to convert the form rows", exc)

    # A failed lookup must not turn into blind publishing of the whole file.
    try:
        known_refs = await feedback_operations.get_known_source_refs(
            tenant_code, [response["source_ref"] for response in responses]
        )
    except Exception as exc:
        raise _FeedbackStageFailure(
            "Feedback Delta Lookup", "Failed to look up the responses already stored", exc
        )

    def _build_and_validate_sync() -> tuple:
        events, build_stats = feedback_gforms.build_gforms_events(
            responses,
            known_refs=known_refs,
            program_info=ctx["program_info"],
            leader_info=ctx["leader_info"],
            tenant_code=tenant_code,
            source=source,
            config=ctx["config"],
        )
        payloads, schema_skipped, kinds = _validate_for_publish([(event, "new") for event in events], record_id, ctx["ingestion_schema"])
        return payloads, schema_skipped, kinds, build_stats

    try:
        payloads, schema_skipped, kinds, build_stats = await asyncio.to_thread(_build_and_validate_sync)
    except Exception as exc:
        raise _FeedbackStageFailure("Feedback Event Building", "Failed to build feedback events", exc)

    skipped = [dict(entry) for entry in adapter_skipped] + schema_skipped
    report = {
        "rows_read": stats["rows_read"],
        "responses_new": len(kinds),
        "responses_already_stored": build_stats["already_stored"],
        "collapsed_duplicates": stats["collapsed_duplicates"],
    }
    return payloads, skipped, report


# The payload builder of each adapter (same signature); chosen by the config's adapter.
_ADAPTER_PREPARERS = {
    ADAPTER_CONVERSATION_ROWS: _prepare_conversation_rows_payloads,
    ADAPTER_WIDE_FORM: _prepare_wide_form_payloads,
}


# csv_uploads.status of a finished feedback upload (VARCHAR(20)). Others: pending, in_progress, on_hold, failed.
UPLOAD_STATUS_SUCCESS = "success"
UPLOAD_STATUS_PARTIAL = "partial"


async def _process_feedback_inline(
    record_id: int,
    record: dict,
    file_bytes: Optional[bytes] = None,
    df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Session-level processing of a `feedback` csv_uploads record (called by process_csv_inline):
    re-resolve program/leader/config, build one event per session (conversation_rows) or per new response
    (wide_form), apply the delta check, schema-validate each event, publish the valid ones to
    Kafka and send every skipped session / response to the DLQ. File-level problems were rejected in
    handle_upload; a bad session is skipped (not fatal): the record still finishes with its report, as 'success' when
    nothing was skipped and as 'partial' when at least one session or response was (UPLOAD_STATUS_PARTIAL).
    Fetch / config-resolution / delta-lookup / Kafka-publish failures end 'on_hold', exactly as
    story/discussion.
    """
    cloud_storage_path = record["cloud_storage_path"]
    record_meta = _record_meta(record)
    loaded_here = df is None
    source = record_meta.get("source")
    tenant_code = record_meta.get("tenant_code")

    # --- 1. Re-resolve program / leader / active config (never fabricated). The config's adapter decides how the
    # stored file is read, so this comes before the file is parsed. ---
    try:
        if tenant_code is None:
            raise InvalidFeedbackFile(["tenant_code is missing from the upload record"])
        if source is None:
            raise InvalidFeedbackFile(["source is missing from the upload record"])
        target = await load_feedback_target(
            record.get("program_name"), record.get("leader_category"), tenant_code, source
        )
    except InvalidFeedbackFile as exc:
        logger.warning("Feedback record %s failed config resolution/file checks: %s", record_id, exc.errors)
        await operations.update_status(record_id, "on_hold", {
            "stage": "Feedback Config Resolution",
            "error": str(exc),
            "validation_errors": exc.errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return
    except Exception as exc:
        logger.exception("Failed to resolve feedback program/leader/config for record %s", record_id)
        await operations.update_status(record_id, "on_hold", {
            "stage": "Feedback Config Resolution",
            "error": str(exc),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return

    # --- 2. Fetch/Parse CSV (pre-parsed df from handle_upload if available, else fetch from storage) ---
    try:
        if df is None:
            if file_bytes is None:
                last_exc: Exception = RuntimeError("unreachable")
                for attempt in range(3):
                    try:
                        file_bytes = await asyncio.to_thread(fetch_csv, cloud_storage_path)
                        break
                    except Exception as exc:
                        last_exc = exc
                        if attempt < 2:
                            wait = 2 ** attempt  # 1s, 2s — then give up
                            logger.warning(
                                "GCS fetch attempt %d/3 failed for record %s (%s); retrying in %ds",
                                attempt + 1, record_id, exc, wait,
                            )
                            await asyncio.sleep(wait)
                else:
                    raise last_exc
            # every cell a string: "NA" / "None" answers must survive
            df = await asyncio.to_thread(_parse_feedback_file, file_bytes, target["adapter"])
    except Exception as exc:
        logger.exception("Failed to fetch/load feedback CSV for record %s", record_id)
        error_meta = {
            "stage": "CSV Fetching",
            "error": "Failed to fetch/load CSV",
            "exception": str(exc),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await operations.update_status(record_id, "on_hold", error_meta)
        return

    await operations.update_status(record_id, "in_progress")

    try:
        ctx = await _resolve_feedback_context(
            df, record.get("program_name"), record.get("leader_category"), tenant_code, source,
            check_file=loaded_here,  # handle_upload already checked a freshly uploaded file
            target=target,
        )
    except InvalidFeedbackFile as exc:
        logger.warning("Feedback record %s failed config resolution/file checks: %s", record_id, exc.errors)
        await operations.update_status(record_id, "on_hold", {
            "stage": "Feedback Config Resolution",
            "error": str(exc),
            "validation_errors": exc.errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return
    except Exception as exc:
        logger.exception("Failed to check the feedback file of record %s", record_id)
        await operations.update_status(record_id, "on_hold", {
            "stage": "Feedback Config Resolution",
            "error": str(exc),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return

    # --- 3. Build events, delta check against what is stored, schema-validate each ---
    try:
        payloads, skipped, report = await _ADAPTER_PREPARERS[ctx["adapter"]](record_id, df, ctx, tenant_code, source)
    except _FeedbackStageFailure as failure:
        logger.error(
            "Feedback record %s failed at %r: %s", record_id, failure.stage, failure.cause, exc_info=failure.cause
        )
        await operations.update_status(record_id, "on_hold", {
            "stage": failure.stage,
            "error": failure.error,
            "exception": str(failure.cause),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        return

    if skipped:
        logger.warning(
            "record %s: %d session(s)/response(s) were skipped (sent to the DLQ): %s",
            record_id, len(skipped), skipped[:20],
        )

    # --- 4. Publish valid sessions to Kafka ---
    if payloads:
        try:
            await asyncio.to_thread(_push_rows_sync, payloads)
        except Exception as exc:
            logger.exception("Kafka push failed for feedback record %s", record_id)
            # "on_hold", not "pending": same reasoning as the story/discussion branch (no blind replay).
            await operations.update_status(record_id, "on_hold", {
                "stage": "Kafka Publishing",
                "error": "Failed to publish record",
                "exception": str(exc),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            return

    # --- 5. Skipped sessions -> DLQ (a DLQ failure is recorded but never fails the upload) ---
    final_meta: Dict[str, Any] = {
        "rows_pushed": len(payloads),
        "sessions_skipped": len(skipped),
        "sessions_dead_lettered": 0,
        **report,
        "config_version_id": ctx["config_version_id"],
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    if skipped:
        try:
            dlq_messages = _build_dlq_messages(skipped, record_id, tenant_code, source)
            await asyncio.to_thread(_push_dlq_sync, dlq_messages)
            final_meta["sessions_dead_lettered"] = len(dlq_messages)
        except Exception as exc:
            logger.exception("DLQ publish failed for feedback record %s (%d skipped)", record_id, len(skipped))
            final_meta["dlq_publish_error"] = str(exc)
        final_meta["schema_validation_errors"] = skipped[:FEEDBACK_MAX_REPORTED_ERRORS]
        if len(skipped) > FEEDBACK_MAX_REPORTED_ERRORS:
            final_meta["truncated"] = True

    # --- 6. Finished (bad sessions are skipped, not fatal). 'success' says that every session of the file was taken;
    # 'partial' that some were skipped (their reasons are in meta_data and on the DLQ): an operator who only reads the
    # status must not see 'success' for a file that lost sessions. Both are finished uploads with a report. ---
    final_status = UPLOAD_STATUS_PARTIAL if len(skipped) > 0 else UPLOAD_STATUS_SUCCESS
    await operations.update_status(record_id, final_status, final_meta)
    logger.info(
        "Feedback record %s finished as %s: %d event(s) pushed to Kafka, %d skipped, report %s",
        record_id, final_status, len(payloads), len(skipped), report,
    )


# Service Orchestration Logic
async def handle_upload(
    report_type: str,
    program_name: str,
    leader_category: str,
    tenant_code: str,
    file_name: str,
    file_bytes: bytes,
    background_tasks: BackgroundTasks,
    source: Optional[str] = None,
    feedback_target: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Orchestrates one upload. For feedback, `feedback_target` is what the route already loaded with
    load_feedback_target (program, persona and config are then resolved once per upload, and the size cap has
    been applied before this point); without it the target is loaded here.
    """
    normalized_type = report_type.lower().strip()
    stage_report: Optional[Dict[str, Any]] = None
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

    meta_data = {
        "original_filename": file_name,
        "program_name": program_name,
        "leader_category": leader_category,
        "report_type": normalized_type,
        "tenant_code": tenant_code,
    }

    if normalized_type == FEEDBACK_REPORT_TYPE:
        # File-level checks (nothing is stored if any fails). The config's adapter decides the parser and the
        # checks. conversation_rows: the message-level CSV is parsed with dtype=str / keep_default_na=False so
        # "NA" / "None" answers survive and every column of the config's column_map must be present. wide_form:
        # the original wide CSV is parsed by the adapter and its form must match the persona's config.
        if source is None:
            raise InvalidFeedbackFile(["source is required for a feedback upload"])
        target = feedback_target
        if target is None:
            target = await load_feedback_target(program_name, leader_category, tenant_code, source)
        try:
            df = await asyncio.to_thread(_parse_feedback_file, file_bytes, target["adapter"])
        except ValueError as exc:
            raise InvalidFeedbackFile([f"Failed to parse CSV: {exc}"])
        ctx = await _resolve_feedback_context(
            df, program_name, leader_category, tenant_code, source, check_file=True, target=target,
        )
        # Optional expect_stages (conversation_rows): report the stages that are neither expected nor closing keys.
        # Report only: it never skips or rejects a session (stage names legitimately change between versions).
        if ctx["adapter"] == ADAPTER_CONVERSATION_ROWS:
            stage_report = await asyncio.to_thread(feedback_uploads.report_unexpected_stages, df, ctx["config"])
            if stage_report is not None:
                meta_data["stage_report"] = stage_report
        meta_data.update({
            "source": source,
            "adapter": ctx["adapter"],
            "persona": ctx["leader_info"]["name"],
            "config_id": ctx["config_id"],
            "config_version_id": ctx["config_version_id"],
            "config_version": ctx["config_version"],
        })
        meta_data["session_type"] = ctx["session_type"]
        nul_bytes = int(df.attrs.get(feedback_uploads.NUL_BYTES_ATTR, 0)) if ctx["adapter"] == ADAPTER_CONVERSATION_ROWS else 0
        if nul_bytes > 0:
            meta_data["nul_bytes_removed"] = nul_bytes
        if len(ctx["bot_ids"]) > 0:
            meta_data["bot_ids"] = ctx["bot_ids"]
    else:
        # Validate CSV content and structure
        parse_errors = []
        df = None
        try:
            df = await asyncio.to_thread(pd.read_csv, io.BytesIO(file_bytes))
        except Exception as exc:
            parse_errors = [f"Failed to parse CSV: {exc}"]

        validation_errors = []
        if df is not None:
            is_valid, errors = await asyncio.to_thread(validate_columns, df, normalized_type)
            if not is_valid:
                validation_errors = errors

        all_errors = parse_errors or validation_errors

        if all_errors:
            # Reject before touching GCS or the DB — no side effects for invalid uploads.
            raise InvalidCsvColumns(all_errors)


    # Upload valid file to GCS
    try:
        cloud_storage_path = await asyncio.to_thread(upload_csv, file_bytes, normalized_type, file_name)
    except Exception as exc:
        logger.error("GCS Upload failed: %s", exc)
        meta_data["error"] = f"GCS Upload failed: {exc}"
        try:
            await operations.insert_upload_record(
                report_type=normalized_type,
                program_name=program_name,
                leader_category=leader_category,
                cloud_storage_path="gcs_upload_failed",
                file_name=file_name,
                file_size=file_size,
                meta_data=meta_data,
                status="failed",
            )
        except Exception as db_exc:
            logger.warning("Failed to record upload failure in DB: %s", db_exc)
        raise RuntimeError(f"GCS Upload failed: {exc}. Please verify GCS settings.")

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

    response = {
        "message": "Successfully uploaded to cloud",
        "id": record_id,
        "status": "in_progress" if claim_status == "success" else "pending",
    }
    if stage_report is not None:
        response["stage_report"] = stage_report
    return response


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

