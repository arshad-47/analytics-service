import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from temporalio.client import Client

from app.config import settings
from app.database import feedback_operations
from app.database.db import db
from app.database.operations import (
    insert_or_update_submission,
    delete_submission,
    update_submission_status,
    is_feedback_type,
    mark_pending_submission_config_failed,
)
from app.services.feedback_config_cache import FeedbackConfigCache, InvalidCacheKeyError
from app.services.feedback_config_validation import validate_ingestion_schema_config, validate_process_steps
from app.services.feedback_adapter_config import validate_adapter_config
from app.services.feedback_constants import CONFIG_KEY_INGESTION_SCHEMA, CONFIG_KEY_PROCESS_STEPS
from app.services.ingestion_validation import feedback_identifier_problems, validate_ingestion_schema
from app.temporal.workflows import ConfigDrivenProcessingWorkflow

logger = logging.getLogger("analytics_service.kafka.consumer")

# Appended to the DLQ reason when a feedback event cannot be validated because its persona's upload config is missing
# or invalid: that is an operator error (not a producer bug), the event itself may be fine.
FEEDBACK_CONFIG_REPLAY_HINT = (
    "operator error, not a bad event: fix the persona's feedback_upload config (for a config that predates the "
    "per-bot settings run `python tools/feedback_config.py migrate`, or migrate-legacy for a database created before session_type existed), then replay this message from the DLQ"
)

# How long the consumer trusts a persona's feedback settings (ingestion schema, process steps) before it reads the
# config again; a new config version is picked up within this time and replaces the cached one.
FEEDBACK_CONFIG_CACHE_TTL_SECONDS = 60.0


def _payload_fingerprint(raw_payload: str) -> str:
    """A log-safe stand-in for a raw Kafka payload: a short hash plus byte length,
    enough to correlate a log line with the full message already preserved on the
    DLQ topic, without duplicating submission text / user identifiers into logs."""
    digest = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()[:12]
    return f"sha256:{digest} ({len(raw_payload)} bytes)"


def _extract_identifiers(event: dict) -> dict:
    """Pulls correlation identifiers (submissionId/tenantCode/sessionId — not
    submission content) out of a parsed event, so an invalid message can be found
    directly by these fields instead of by hashing payloads."""
    return {
        "submissionId": event.get("submissionId"),
        "tenantCode": event.get("tenantCode"),
        "sessionId": event.get("sessionId"),
    }


def _event_text(event: dict, *path: str):
    """A non-blank string at the nested path of an event, else None."""
    current = event
    for segment in path:
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    if isinstance(current, str) and len(current.strip()) > 0:
        return current.strip()
    return None


_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]")
_MAX_LOGGED_IDENTIFIER = 128


def _log_safe(value) -> str:
    """An identifier as it may appear in a log line: control characters (CR, LF, ...) become '?' and the length is
    capped, so an event can never forge or flood a log line (an event that failed validation still has its raw ids)."""
    text = _CONTROL_CHARS.sub("?", str(value))
    if len(text) > _MAX_LOGGED_IDENTIFIER:
        text = text[:_MAX_LOGGED_IDENTIFIER] + "..."
    return text


def _format_identifiers(identifiers: dict) -> str:
    present = [f"{k}={_log_safe(v)}" for k, v in identifiers.items() if v is not None]
    return ", ".join(present) if present else "no identifiers found"


class IngestionConsumer:
    def __init__(self):
        self.consumer_conf = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            # Manual commit (see start()) — commit only after process_message()
            # has actually completed, so a crash mid-processing leaves the offset
            # uncommitted and the message is redelivered rather than silently lost.
            "enable.auto.commit": False,
        }
        self.dlq_topic = settings.KAFKA_TOPIC_INGESTION_DLQ
        self.consumer = None
        self.dlq_producer = None
        self.temporal_client = None
        self.running = False
        # The persona's ingestion schema / process steps live in its upload config (config tables, not .env):
        # cached per (tenant, program, session_type, source) so a burst of events does not query per event.
        self.feedback_config_cache = FeedbackConfigCache(
            self._load_feedback_runtime_config, ttl_seconds=FEEDBACK_CONFIG_CACHE_TTL_SECONDS
        )

    async def _load_feedback_runtime_config(self, tenant_code: str, program_id: str, session_type: str, source: str):
        """Loader of the cache: the active config's validated ingestion_schema + process_steps, or None when the
        persona has no active config. Raises ValueError for a config that lacks / has invalid settings."""
        active = await feedback_operations.get_active_feedback_config(
            program_id, source, tenant_code=tenant_code, session_type=session_type
        )
        if active is None:
            return None
        config = active["config"]
        for key in (CONFIG_KEY_INGESTION_SCHEMA, CONFIG_KEY_PROCESS_STEPS):
            if key not in config:
                raise ValueError(
                    f"Feedback config of session_type {session_type!r} ({source}) is missing '{key}' "
                    "(add it with: python tools/feedback_config.py migrate; or migrate-legacy for a database created before session_type existed)"
                )
        # the adapter says how the source's identity rules work; the event must carry the same one
        try:
            adapter = validate_adapter_config(config)
        except ValueError as exc:
            raise ValueError(f"{exc} (session_type {session_type!r}, source {source!r})")
        return {
            "config_version_id": active["config_version_id"],
            "adapter": adapter,
            "ingestion_schema": validate_ingestion_schema_config(config[CONFIG_KEY_INGESTION_SCHEMA]),
            "process_steps": validate_process_steps(config[CONFIG_KEY_PROCESS_STEPS]),
        }

    async def _feedback_schema_for_event(self, event: dict, event_type: str):
        """
        (schema, problems) for a feedback event: the ingestion schema of the persona the event names
        (tenantCode, tags.programId, data.sessionType, data.source), from the cached upload config. A create that
        cannot be tied to an active, valid config is a problem (the caller sends it to the DLQ with identifiers). A
        delete event carries no persona, so when its config cannot be resolved it falls back to the envelope check
        (schema None) instead of being dead-lettered.
        """
        lookup = {
            "tenantCode": _event_text(event, "tenantCode"),
            "tags.programId": _event_text(event, "tags", "programId"),
            "data.sessionType": _event_text(event, "data", "sessionType"),
            "data.source": _event_text(event, "data", "source"),
        }
        missing = [name for name, value in lookup.items() if value is None]
        if len(missing) > 0:
            if event_type == "delete":
                return None, []
            return None, [
                "Cannot select the feedback ingestion schema (it is the persona's upload config): "
                + ", ".join(f"'{name}' is missing or not a non-empty string" for name in missing)
            ]
        # The identifiers become cache keys and SQL parameters: validate them BEFORE they are used (a hostile event
        # could otherwise pin large keys in the cache or carry control characters). The values are never echoed.
        bad = feedback_identifier_problems(event, create=True)
        if len(bad) > 0:
            if event_type == "delete":
                return None, []
            return None, bad
        try:
            runtime = await self.feedback_config_cache.get(
                lookup["tenantCode"], lookup["tags.programId"], lookup["data.sessionType"], lookup["data.source"]
            )
        except InvalidCacheKeyError as exc:
            # an identifier the cache refuses as a key (defence in depth: validated above): name the failed field, the
            # message never carries its value. A producer bug, not an operator error, so no replay hint.
            if event_type == "delete":
                return None, []
            return None, [f"Invalid feedback config key, the event cannot be tied to a persona config: {exc}"]
        except ValueError as exc:
            if event_type == "delete":
                return None, []
            return None, [f"Feedback upload config is invalid: {exc} ({FEEDBACK_CONFIG_REPLAY_HINT})"]
        if runtime is None:
            if event_type == "delete":
                return None, []
            return None, [
                f"No active {lookup['data.source']} feedback_upload config for session_type "
                f"{lookup['data.sessionType']!r}, program {lookup['tags.programId']!r}, tenant {lookup['tenantCode']!r} "
                f"({FEEDBACK_CONFIG_REPLAY_HINT})"
            ]
        if event_type == "create":
            event_adapter = _event_text(event, "data", "adapter")
            if event_adapter != runtime["adapter"]:
                return None, [
                    "'data.adapter' does not match the adapter of the persona's upload config "
                    f"(the config's adapter is {runtime['adapter']!r}; {FEEDBACK_CONFIG_REPLAY_HINT})"
                ]
        return runtime["ingestion_schema"], []

    async def initialize(self):
        """
        Connects database pool, caches the Temporal client, and sets up the DLQ producer.
        """
        await db.connect()
        self.dlq_producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        try:
            logger.info(f"Initializing Temporal client on {settings.TEMPORAL_HOST}...")
            self.temporal_client = await Client.connect(settings.TEMPORAL_HOST, namespace=settings.TEMPORAL_NAMESPACE)
        except Exception as e:
            logger.error(f"Failed to connect to Temporal: {e}. Worker will not be able to trigger real-time flows.")

    async def _trigger_realtime_workflow(self, submission_id: str, tenant_code: str, submission_type: str):
        """
        Starts a Temporal workflow execution in real-time.

        A feedback submission's steps come from its persona's upload config. When that config is missing or invalid
        (get_feedback_process_steps raises ValueError, an operator error) the row is already stored, so this logs
        the problem with the identifiers and marks the submission 'failed' with the reason in its process_status
        (the bulk retry picks it up once the config is fixed) and returns normally: the message is NOT retried or
        dead-lettered (retrying cannot succeed and would only produce an identifier-less DLQ entry for a stored
        row). A database error is never caught here: it propagates, so process_message is retried as before.
        """
        # A feedback submission whose type has no process steps is complete once ingested: mark it
        # 'success' here (batch mode does the same through the workflow) instead of leaving it 'pending'
        # forever. Story/discussion keep their existing behaviour (checked below, after the Temporal client).
        # The feedback steps come from the persona's upload config (config tables), story/discussion from settings.
        is_feedback = is_feedback_type(submission_type)
        feedback_steps = None
        if is_feedback:
            async with db.pool.acquire() as conn:
                try:
                    feedback_steps = await feedback_operations.get_feedback_process_steps(conn, submission_id, tenant_code)
                except ValueError as exc:
                    logger.error(
                        f"Cannot resolve the process steps of feedback submission {submission_id} (tenant: {tenant_code}); "
                        f"marking it 'failed' (fix the persona config, then retry it): {exc}"
                    )
                    # only while the row is still 'pending' (a workflow may have taken it meanwhile)
                    await mark_pending_submission_config_failed(conn, submission_id, tenant_code, str(exc))
                    return
        if is_feedback and len(feedback_steps) == 0:
            async with db.pool.acquire() as conn:
                await update_submission_status(
                    conn, submission_id, tenant_code, "success",
                    {"status": "success", "steps": [], "timestamp": datetime.now(timezone.utc).isoformat()},
                )
            logger.info(f"No process steps defined for submission type '{submission_type}'. Marked submission {submission_id} as 'success' at ingestion.")
            return

        if not self.temporal_client:
            try:
                logger.info(f"Temporal client not connected. Attempting to reconnect to {settings.TEMPORAL_HOST}...")
                self.temporal_client = await Client.connect(settings.TEMPORAL_HOST, namespace=settings.TEMPORAL_NAMESPACE)
            except Exception as e:
                logger.error(f"Failed to connect to Temporal: {e}. Leaving submission {submission_id} as 'pending'.")
                return

        process_steps = feedback_steps if is_feedback else settings.get_process_config(submission_type)
        if len(process_steps) == 0:
            logger.warning(f"No process steps defined for submission type '{submission_type}'. Skipping orchestration.")
            return

        workflow_id = f"realtime-{submission_id}-{tenant_code}"
        payload = {
            "submission_id": submission_id,
            "tenant_code": tenant_code,
            "process_steps": process_steps
        }

        try:
            logger.info(f"Starting Temporal workflow {workflow_id} for submission {submission_id}...")
            
            # Start workflow asynchronously without waiting for it to finish
            await self.temporal_client.start_workflow(
                ConfigDrivenProcessingWorkflow.run,
                payload,
                id=workflow_id,
                task_queue=settings.TEMPORAL_QUEUE
            )
            
            # Update status in PostgreSQL to 'processing' only if still 'pending'
            async with db.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE submissions
                    SET status = 'processing', updated_at = now()
                    WHERE submission_id = $1 AND tenant_code = $2 AND status = 'pending'
                    """,
                    submission_id,
                    tenant_code,
                )
                
            logger.info(f"Workflow {workflow_id} triggered successfully.")
        except Exception as e:
            logger.error(f"Failed to trigger workflow {workflow_id}: {e}. Leaving submission {submission_id} as 'pending'.")
            # Reset client on connection / gRPC issues to trigger reconnect next time
            err_str = str(e).lower()
            if any(term in err_str for term in ["connect", "rpc", "connection", "unavailable"]):
                logger.info("Detected connection issue with Temporal. Resetting client to trigger reconnect on next message.")
                self.temporal_client = None

    async def _send_to_dlq(self, raw_payload: str, reason: str, identifiers: dict = None) -> None:
        """
        Publishes a malformed/invalid message to the dead-letter topic instead of
        silently dropping it, so it can be inspected or replayed later after the
        producer-side bug that caused it is fixed. When available, submissionId/
        tenantCode/sessionId are attached as extra headers (and logged) so the
        message can be found directly, without hashing payloads.

        Raises if the producer isn't initialized, or if delivery can't be confirmed
        within the flush timeout — callers (and start()'s offset-commit logic) must
        treat this message as NOT durably handled unless this returns without
        raising, otherwise a failed/unconfirmed DLQ publish plus a committed offset
        would lose the message with no trace of it anywhere.
        """
        identifiers = identifiers or {}
        id_str = _format_identifiers(identifiers)

        if not self.dlq_producer:
            raise RuntimeError(
                f"DLQ producer not initialized; cannot durably route invalid message. "
                f"Reason: {reason}. Identifiers: {id_str}"
            )

        def _produce():
            delivery_error = {}

            def _on_delivery(err, _msg):
                if err is not None:
                    delivery_error["error"] = err

            headers = [("reason", reason.encode("utf-8"))]
            for key, value in identifiers.items():
                if value is not None:
                    headers.append((key, str(value).encode("utf-8")))
            self.dlq_producer.produce(
                self.dlq_topic,
                value=raw_payload.encode("utf-8"),
                headers=headers,
                callback=_on_delivery,
            )
            # flush() polls until the delivery callback fires (or the timeout
            # elapses) — only this guarantees we know the outcome before returning,
            # unlike poll(0) which just services already-completed callbacks.
            remaining = self.dlq_producer.flush(10)
            if remaining > 0:
                raise TimeoutError(
                    f"Timed out waiting for DLQ delivery confirmation ({remaining} message(s) still in-flight)"
                )
            if "error" in delivery_error:
                raise RuntimeError(f"DLQ delivery failed: {delivery_error['error']}")

        await asyncio.to_thread(_produce)
        logger.warning(f"Sent invalid message to DLQ topic '{self.dlq_topic}'. Reason: {reason}. Identifiers: {id_str}. Payload: {_payload_fingerprint(raw_payload)}")

    async def process_message(self, raw_payload: str) -> None:
        """
        Processes a single deserialized Kafka message, executing DB changes and routing.
        Invalid messages (bad JSON, wrong shape, missing/null required keys, unsupported
        eventType) are routed to the DLQ topic instead of being silently dropped.
        """
        try:
            event = json.loads(raw_payload)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Kafka message JSON: {_payload_fingerprint(raw_payload)}. Error: {e}")
            await self._send_to_dlq(raw_payload, f"Invalid JSON: {e}")
            return

        if not isinstance(event, dict):
            reason = f"Expected a JSON object, got {type(event).__name__}"
            logger.error(f"Invalid Kafka message shape: expected an object but received {type(event).__name__}: {_payload_fingerprint(raw_payload)}")
            await self._send_to_dlq(raw_payload, reason)
            return

        identifiers = _extract_identifiers(event)
        event_type_raw = event.get("eventType", "create")
        if not isinstance(event_type_raw, str):
            reason = f"Invalid eventType: expected a string, got {type(event_type_raw).__name__} ({event_type_raw!r})"
            logger.error(f"{reason}. Identifiers: {_format_identifiers(identifiers)}")
            await self._send_to_dlq(raw_payload, reason, identifiers)
            return
        event_type = event_type_raw.lower().strip()
        submission_type = event.get("submissionType")

        feedback_schema = None
        schema_problems = []
        if is_feedback_type(submission_type) and event_type in ("create", "delete"):
            feedback_schema, schema_problems = await self._feedback_schema_for_event(event, event_type)
        if len(schema_problems) > 0:
            problems = schema_problems
        else:
            problems = validate_ingestion_schema(event, submission_type, event_type, feedback_schema=feedback_schema)
        if problems:
            reason = f"Failed ingestion validation for submissionType {submission_type!r}, eventType {event_type!r}: {'; '.join(problems)}"
            logger.error(f"{reason}. Identifiers: {_format_identifiers(identifiers)}")
            await self._send_to_dlq(raw_payload, reason, identifiers)
            return

        # Keep None as None here (rather than str(None) == "None", which is truthy)
        # — the validator above already guarantees submissionId is present/non-empty,
        # this just normalizes it to a string for downstream DB calls.
        submission_id_raw = event.get("submissionId")
        submission_id = str(submission_id_raw) if submission_id_raw is not None else None
        tenant_code = event.get("tenantCode")

        logger.info(f"Processing event '{event_type}' for submission {submission_id} (tenant: {tenant_code})")

        async with db.pool.acquire() as conn:
            if event_type == "create":
                is_feedback = is_feedback_type(submission_type)
                # Check for duplicate entry. Feedback never skips: a create on an existing session
                # reconciles (new messages are added, completion status can advance) inside
                # insert_or_update_submission, so a re-uploaded, grown session is not lost.
                exists = None
                if hasattr(conn, "fetchval") and not is_feedback:
                    exists = await conn.fetchval(
                        "SELECT 1 FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
                        submission_id, tenant_code
                    )
                if exists:
                    logger.warning(f"Duplicate entry: Submission {submission_id} under tenant {tenant_code} already exists. Skipping ingestion.")
                    return

                res = await insert_or_update_submission(conn, event)

                if is_feedback:
                    # A Google Forms response that is already stored resolves to the STORED submission,
                    # whose id differs from the (freshly generated) one in the event.
                    stored_submission_id = res.get("submission_id")
                    if isinstance(stored_submission_id, str):
                        submission_id = stored_submission_id

                if is_feedback and res.get("changed") is False and res.get("status") != "pending":
                    # Identical re-delivery of an already-ingested session (or an already-stored Google Forms
                    # response): nothing was written, so there is nothing to (re)process. A still-'pending'
                    # row falls through and is processed.
                    logger.info(f"Feedback submission {submission_id} under tenant {tenant_code} is unchanged. Nothing to process.")
                    return
                
                # Check orchestration mode
                mode = settings.PROCESSING_MODE.lower().strip()
                if mode == "real-time":
                    # Trigger the processing immediately
                    await self._trigger_realtime_workflow(submission_id, tenant_code, submission_type)
                else:
                    logger.info(f"Batch mode enabled. Queued submission {submission_id} in 'pending' status.")

            elif event_type == "update":
                if is_feedback_type(submission_type):
                    # Not supported in this release: send to the DLQ with identifiers right away
                    # (retrying a deterministic rejection would only delay it).
                    reason = f"eventType 'update' is not supported for submissionType {submission_type!r} in this release (only create and delete)"
                    logger.error(f"{reason}. Identifiers: {_format_identifiers(identifiers)}")
                    await self._send_to_dlq(raw_payload, reason, identifiers)
                    return
                res = await insert_or_update_submission(conn, event)
                
                # Check orchestration mode
                mode = settings.PROCESSING_MODE.lower().strip()
                if mode == "real-time":
                    # Trigger the processing immediately
                    await self._trigger_realtime_workflow(submission_id, tenant_code, submission_type)
                else:
                    logger.info(f"Batch mode enabled. Queued submission {submission_id} in 'pending' status.")

            elif event_type == "delete":
                # Execute deletion
                await delete_submission(conn, submission_id, tenant_code)
            else:
                logger.error(f"Unsupported Kafka eventType: {event_type}. Identifiers: {_format_identifiers(identifiers)}")
                await self._send_to_dlq(raw_payload, f"Unsupported eventType: {event_type}", identifiers)

    async def _ensure_topics_exist(self, topics: list) -> None:
        """Create the given Kafka topics if they do not already exist."""
        admin_client = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

        try:
            futures = await asyncio.to_thread(
                admin_client.create_topics,
                [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics],
            )
            for name, future in futures.items():
                try:
                    future.result(timeout=30)
                    logger.info(f"Kafka topic '{name}' is ready.")
                except Exception as exc:
                    message = str(exc).lower()
                    if "already exists" in message:
                        logger.info(f"Kafka topic '{name}' already exists.")
                    else:
                        logger.warning(f"Could not create Kafka topic '{name}': {exc}")
        except Exception as exc:
            logger.warning(f"Kafka admin client failed while ensuring topics {topics}: {exc}")

    async def start(self) -> None:
        """
        Starts the polling loop using confluent-kafka inside an asyncio executor thread.
        """
        await self.initialize()
        await self._ensure_topics_exist([settings.KAFKA_TOPIC_INGESTION, self.dlq_topic])

        self.consumer = Consumer(self.consumer_conf)
        self.consumer.subscribe([settings.KAFKA_TOPIC_INGESTION])
        self.running = True
        
        logger.info(f"Started Kafka consumer listening to '{settings.KAFKA_TOPIC_INGESTION}'...")
        
        try:
            while self.running:
                # Poll message inside thread to avoid blocking loop
                msg = await asyncio.to_thread(self.consumer.poll, timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    code = msg.error().code()
                    if code == KafkaError._PARTITION_EOF:
                        continue
                    if code in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.UNKNOWN_PARTITION):
                        logger.warning(f"Kafka topic not ready yet: {msg.error()}")
                        await asyncio.sleep(1)
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
                
                raw_payload = msg.value().decode("utf-8")

                # Retry THIS message in-place (with backoff) before ever polling
                # again — leaving an offset uncommitted is not sufficient on its own:
                # Kafka's commit(message=msg) sets the partition's committed pointer
                # to msg.offset()+1 (an absolute position, not a per-message ledger),
                # so if we moved on and a LATER message on this same partition got
                # committed, that would silently advance the committed pointer past
                # this still-unresolved one, permanently skipping it on any future
                # restart. Resolving it (success or DLQ) before the next poll()
                # guarantees no later commit can ever jump past an unresolved message.
                processed_ok = False
                last_error = None
                max_attempts = 3
                for attempt in range(1, max_attempts + 1):
                    try:
                        await self.process_message(raw_payload)
                        processed_ok = True
                        break
                    except Exception as e:
                        last_error = e
                        logger.error(
                            f"Error processing Kafka message (attempt {attempt}/{max_attempts}): {e}. "
                            f"Payload: {_payload_fingerprint(raw_payload)}",
                            exc_info=True,
                        )
                        if attempt < max_attempts:
                            await asyncio.sleep(2 ** attempt)

                if not processed_ok:
                    # Retries exhausted — route to the DLQ rather than leave this
                    # message to be silently skipped by a later commit on this
                    # partition. Only once it's durably captured there is it safe
                    # to let the offset advance past it.
                    try:
                        await self._send_to_dlq(
                            raw_payload,
                            f"Processing failed after {max_attempts} attempts: {last_error}",
                        )
                        processed_ok = True
                    except Exception as dlq_err:
                        logger.error(
                            f"Failed to route unprocessable message to DLQ — leaving offset "
                            f"uncommitted so it is retried. Payload: {_payload_fingerprint(raw_payload)}. "
                            f"Error: {dlq_err}",
                            exc_info=True,
                        )

                # Commit only once the message has been durably handled — either
                # process_message() succeeded, or (after exhausting retries) it was
                # confirmed delivered to the DLQ. If neither happened, the offset
                # stays uncommitted and the message is redelivered on the next
                # poll/restart instead of being silently dropped or skipped.
                if processed_ok:
                    try:
                        await asyncio.to_thread(self.consumer.commit, msg, asynchronous=False)
                    except Exception as commit_err:
                        logger.error(f"Failed to commit Kafka offset: {commit_err}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("Kafka consumer loop cancelled.")
        finally:
            self.running = False
            if self.consumer:
                self.consumer.close()
            if self.dlq_producer:
                await asyncio.to_thread(self.dlq_producer.flush, 10)
            await db.disconnect()
            logger.info("Kafka consumer stopped.")

    def stop(self) -> None:
        self.running = False
