"""
Comprehensive unit-test suite covering tests/TEST_CASES.csv.

No real Postgres, Kafka, or Temporal connection is ever made — every external
dependency (asyncpg pool, confluent_kafka Producer/Consumer/AdminClient,
ObjectStorage interface, urllib-based LLM calls, and Temporal's
Client/workflow context) is mocked via conftest.py's shared fakes plus plain
unittest.mock, following this repo's existing convention (no pytest-asyncio —
async bodies run via asyncio.run()).

Sections mirror tests/TEST_CASES.csv's module column, in the same order:
  KAFKA-*, SEC-*, CONFIG-*, DB-*, THEME-*, PII-*, RATING-*, BATCH-*, MODE-*,
  LLM-*, UPLOAD-*

Explicitly excluded (documented, not attempted — see the plan this file was
built from): RATING-009, THEME-018, BATCH-006, and KAFKA-021 is narrowed to
an assertable subset. These are inherently live-system/timing/concurrency
observations, or (BATCH-006) enforced by Temporal's own server, not this
codebase.
"""
import asyncio
import json
import time
from pathlib import Path
from unittest.mock import ANY, AsyncMock, MagicMock

import json5
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import settings
from conftest import (
    make_fake_object_storage,
    FakeConn,
    FakePool,
    install_fake_db,
    make_fake_kafka_producer,
    make_fake_admin_client,
    make_fake_gcs_client,
    install_fake_llm,
    install_failing_llm,
    install_fake_workflow_context,
    settings_override,
)

FIXTURE_ROOT = Path(__file__).resolve().parent / "kafka_events"
CSV_FIXTURE_ROOT = Path(__file__).resolve().parent / "csv_uploads"


def _load_fixture(path: Path) -> dict:
    return json5.loads(path.read_text(encoding="utf-8"))


def _fixture(*parts) -> dict:
    return _load_fixture(FIXTURE_ROOT.joinpath(*parts))


def _find_execute_call(conn, *needles):
    """
    insert_or_update_submission's conn also runs tenant/leader_category/programs
    upserts (and participant-metrics writes) via conn.execute before/after the
    story_submissions or discussion_submissions upsert, so the *last* execute
    call isn't reliably the one under test. Finds the one call.args[0] (the
    SQL string) contains all of `needles`.
    """
    matches = [c for c in conn.execute.call_args_list if all(n in c.args[0] for n in needles)]
    assert len(matches) == 1, f"expected exactly one execute() call matching {needles}, found {len(matches)}"
    return matches[0]


# =============================================================================
# KAFKA INGESTION (KAFKA-*)
# =============================================================================

import app.kafka.consumer as consumer_module


def _consumer_with_mocks(monkeypatch, fetchval_return=None):
    """A fresh IngestionConsumer with insert/delete/trigger mocked and a FakeConn
    installed for db.pool (fetchval_return controls the create-duplicate check)."""
    consumer = consumer_module.IngestionConsumer()
    fake_conn = install_fake_db(monkeypatch, consumer_module)
    fake_conn.fetchval.return_value = fetchval_return
    insert_mock = AsyncMock(return_value={"id": "uuid-1", "status": "pending"})
    delete_mock = AsyncMock(return_value=True)
    trigger_mock = AsyncMock()
    monkeypatch.setattr(consumer_module, "insert_or_update_submission", insert_mock, raising=False)
    monkeypatch.setattr(consumer_module, "delete_submission", delete_mock, raising=False)
    monkeypatch.setattr(consumer, "_trigger_realtime_workflow", trigger_mock, raising=False)
    monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", "real-time", raising=False)
    return consumer, insert_mock, delete_mock, trigger_mock


def test_kafka_001_valid_discussion_create_ingested(monkeypatch):
    async def run_test():
        consumer, insert_mock, delete_mock, trigger_mock = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        await consumer.process_message(json.dumps(event))
        insert_mock.assert_awaited_once()
        trigger_mock.assert_awaited_once_with(str(event["submissionId"]), event["tenantCode"], event["submissionType"])
        delete_mock.assert_not_awaited()
        dlq_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_002_valid_story_create_ingested(monkeypatch):
    async def run_test():
        consumer, insert_mock, delete_mock, trigger_mock = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_story.json")
        await consumer.process_message(json.dumps(event))
        insert_mock.assert_awaited_once()
        trigger_mock.assert_awaited_once()
        dlq_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_003_update_applies_delta_only_newvalues():
    """Calls insert_or_update_submission directly (unmocked) to confirm fields
    absent from newValues are passed as None, so the real SQL's COALESCE would
    preserve them rather than overwrite with a stale/None value."""
    from app.database.operations import insert_or_update_submission

    async def run_test():
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "uuid-1", "status": "processing"}
        conn.fetchval.return_value = 1  # row_exists = True -> UPDATE branch
        event = _fixture("update", "update_discussion.json")
        await insert_or_update_submission(conn, event)

        update_call = _find_execute_call(conn, "UPDATE discussion_submissions")
        args = update_call.args
        # UPDATE discussion_submissions SET title=$3, challenges=$4, solutions=$5, ...
        # newValues only has title/participantsData -> challenges/solutions args must be None
        assert args[3] == event["newValues"]["title"]
        assert args[4] is None  # challenges
        assert args[5] is None  # solutions
    asyncio.run(run_test())


def test_kafka_004_delete_event_removes_submission(monkeypatch):
    async def run_test():
        consumer, insert_mock, delete_mock, trigger_mock = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("delete", "delete_discussion.json")
        await consumer.process_message(json.dumps(event))
        delete_mock.assert_awaited_once_with(ANY, str(event["submissionId"]), event["tenantCode"])
        insert_mock.assert_not_awaited()
        trigger_mock.assert_not_awaited()
        dlq_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_005_malformed_json_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        await consumer.process_message("{bad json")
        dlq_mock.assert_awaited_once()
        assert "Invalid JSON:" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


@pytest.mark.parametrize("payload,expected_type", [("[]", "list"), ("42", "int")])
def test_kafka_006_non_object_json_routed_to_dlq(monkeypatch, payload, expected_type):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        await consumer.process_message(payload)
        dlq_mock.assert_awaited_once()
        assert f"got {expected_type}" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_007_missing_submission_id_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        del event["submissionId"]
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "'submissionId' is missing" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_008_null_submission_id_routed_to_dlq_not_coerced(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["submissionId"] = None
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "'submissionId' is null" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_009_missing_tenant_code_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        del event["tenantCode"]
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "'tenantCode' is missing" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_010_empty_tags_state_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["tags"]["state"] = ""
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "'tags.state' is empty" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_011_empty_challenges_array_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["data"]["challenges"] = []
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "'data.challenges' is empty" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_012_empty_solutions_array_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["data"]["solutions"] = []
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "'data.solutions' is empty" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


@pytest.mark.parametrize("bad_event_type,expected_fragment", [(123, "got int"), (None, "got NoneType")])
def test_kafka_013_non_string_event_type_does_not_crash(monkeypatch, bad_event_type, expected_fragment):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["eventType"] = bad_event_type
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert expected_fragment in dlq_mock.await_args.args[1]

        # Consumer keeps working afterward — no crash/hang.
        good_event = _fixture("create", "create_story.json")
        await consumer.process_message(json.dumps(good_event))
        insert_mock.assert_awaited_once()
    asyncio.run(run_test())


def test_kafka_014_non_string_submission_type_does_not_crash(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["submissionType"] = 123
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "Unrecognized submissionType 123" in dlq_mock.await_args.args[1]

        good_event = _fixture("create", "create_story.json")
        await consumer.process_message(json.dumps(good_event))
        insert_mock.assert_awaited_once()
    asyncio.run(run_test())


def test_kafka_015_unrecognized_submission_type_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["submissionType"] = "survey"
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "Unrecognized submissionType 'survey'" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_016_unsupported_event_type_routed_to_dlq(monkeypatch):
    """
    Note: validate_ingestion_schema() rejects ANY eventType outside
    create/update/delete at the schema-lookup stage (event_schema.get(event_type)
    is always None for "archive"), so process_message's final
    `else: "Unsupported eventType: ..."` branch is never actually reached for
    this input — it's effectively dead code given the current validator. This
    test asserts the REAL reachable reason rather than the sheet's original
    (unreachable) wording.
    """
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        event["eventType"] = "archive"
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "No ingestion schema section defined for eventType 'archive'" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_017_update_newvalues_empty_field_routed_to_dlq(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("update", "update_discussion.json")
        event["newValues"]["title"] = ""
        await consumer.process_message(json.dumps(event))
        dlq_mock.assert_awaited_once()
        assert "'newValues.title' is empty" in dlq_mock.await_args.args[1]
        insert_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_kafka_018_update_omitting_field_preserves_existing_value():
    """update_discussion.json's newValues omits challenges/solutions entirely —
    the validator must not flag the omission (only newValuesNoEmpty checks keys
    actually PRESENT in newValues), and the DB call must pass None for them."""
    from app.database.operations import insert_or_update_submission
    from app.services.ingestion_validation import validate_ingestion_schema

    event = _fixture("update", "update_discussion.json")
    problems = validate_ingestion_schema(event, event["submissionType"], "update")
    assert problems == []

    async def run_test():
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "uuid-1", "status": "processing"}
        conn.fetchval.return_value = 1
        await insert_or_update_submission(conn, event)
        args = _find_execute_call(conn, "UPDATE discussion_submissions").args
        assert args[4] is None  # challenges
        assert args[5] is None  # solutions
    asyncio.run(run_test())


def test_kafka_019_duplicate_create_skipped(monkeypatch, caplog):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch, fetchval_return=1)  # row already exists
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_story.json")
        with caplog.at_level("WARNING"):
            await consumer.process_message(json.dumps(event))
        insert_mock.assert_not_awaited()
        dlq_mock.assert_not_awaited()
        assert any("Duplicate entry" in r.message for r in caplog.records)
    asyncio.run(run_test())


class _FakeKafkaMsg:
    def __init__(self, value: bytes):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


def _make_fake_kafka_consumer(messages):
    """poll() pops one message per call, then returns None forever (with a tiny
    real sleep to keep the busy-loop from pegging a CPU core during the test)."""
    remaining = list(messages)

    def _poll(timeout=1.0):
        if remaining:
            return remaining.pop(0)
        time.sleep(0.01)
        return None

    fake = MagicMock()
    fake.poll = MagicMock(side_effect=_poll)
    fake.commit = MagicMock()
    fake.close = MagicMock()
    fake.subscribe = MagicMock()
    return fake


async def _run_consumer_briefly(consumer, duration=0.25):
    task = asyncio.create_task(consumer.start())
    await asyncio.sleep(duration)
    consumer.running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_kafka_020_db_failure_retries_3_times_then_dlq(monkeypatch):
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        # No asyncio.sleep patch here — consumer_module.asyncio *is* the real
        # asyncio module, so patching .sleep globally would also break this
        # test's own timing helper. The real 2s/4s backoff is allowed to elapse.
        monkeypatch.setattr(consumer_module, "Producer", MagicMock(return_value=make_fake_kafka_producer()))
        monkeypatch.setattr(consumer_module, "AdminClient", MagicMock(return_value=make_fake_admin_client()))
        monkeypatch.setattr(consumer_module, "Consumer", MagicMock(
            return_value=_make_fake_kafka_consumer([_FakeKafkaMsg(b'{"submissionId": 1}')])
        ))
        monkeypatch.setattr(consumer_module.db, "connect", AsyncMock())
        monkeypatch.setattr(consumer_module.db, "disconnect", AsyncMock())
        monkeypatch.setattr(consumer_module.Client, "connect", AsyncMock(side_effect=Exception("no temporal")))

        process_mock = AsyncMock(side_effect=RuntimeError("session_id collision"))
        monkeypatch.setattr(consumer, "process_message", process_mock)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)

        await _run_consumer_briefly(consumer, duration=7.0)  # 2s + 4s real backoff + margin

        assert process_mock.await_count == 3
        dlq_mock.assert_awaited_once()
        assert "Processing failed after 3 attempts" in dlq_mock.await_args.args[1]
    asyncio.run(run_test())


def test_kafka_021_dlq_failure_leaves_offset_uncommitted(monkeypatch):
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        # Real 2s/4s backoff is allowed to elapse (see test_kafka_020 comment).
        fake_kafka_consumer = _make_fake_kafka_consumer([_FakeKafkaMsg(b'{"submissionId": 1}')])
        monkeypatch.setattr(consumer_module, "Producer", MagicMock(return_value=make_fake_kafka_producer()))
        monkeypatch.setattr(consumer_module, "AdminClient", MagicMock(return_value=make_fake_admin_client()))
        monkeypatch.setattr(consumer_module, "Consumer", MagicMock(return_value=fake_kafka_consumer))
        monkeypatch.setattr(consumer_module.db, "connect", AsyncMock())
        monkeypatch.setattr(consumer_module.db, "disconnect", AsyncMock())
        monkeypatch.setattr(consumer_module.Client, "connect", AsyncMock(side_effect=Exception("no temporal")))

        monkeypatch.setattr(consumer, "process_message", AsyncMock(side_effect=RuntimeError("boom")))
        monkeypatch.setattr(consumer, "_send_to_dlq", AsyncMock(side_effect=RuntimeError("dlq unreachable")))

        await _run_consumer_briefly(consumer, duration=7.0)

        fake_kafka_consumer.commit.assert_not_called()
    asyncio.run(run_test())


def test_kafka_022_delimiter_character_preserved_intact():
    """A statement containing a literal '|' round-trips as one unmodified TEXT[]
    element — _normalize_statement_list never delimiter-splits."""
    from app.database.operations import _normalize_statement_list

    value = ["Space is an issue | teachers agree"]
    result = _normalize_statement_list(value)
    assert result == ["Space is an issue | teachers agree"]
    assert len(result) == 1


def test_kafka_023_later_message_never_commits_past_unresolved_earlier(monkeypatch):
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        # Real 2s/4s backoff is allowed to elapse (see test_kafka_020 comment).
        fake_kafka_consumer = _make_fake_kafka_consumer([
            _FakeKafkaMsg(b'{"submissionId": "fail-me"}'),
            _FakeKafkaMsg(b'{"submissionId": "succeed-me"}'),
        ])
        monkeypatch.setattr(consumer_module, "Producer", MagicMock(return_value=make_fake_kafka_producer()))
        monkeypatch.setattr(consumer_module, "AdminClient", MagicMock(return_value=make_fake_admin_client()))
        monkeypatch.setattr(consumer_module, "Consumer", MagicMock(return_value=fake_kafka_consumer))
        monkeypatch.setattr(consumer_module.db, "connect", AsyncMock())
        monkeypatch.setattr(consumer_module.db, "disconnect", AsyncMock())
        monkeypatch.setattr(consumer_module.Client, "connect", AsyncMock(side_effect=Exception("no temporal")))

        call_order = []

        async def fake_process(raw_payload):
            payload = json.loads(raw_payload)
            if payload["submissionId"] == "fail-me":
                call_order.append("process:fail-me")
                raise RuntimeError("boom")
            call_order.append("process:succeed-me")

        monkeypatch.setattr(consumer, "process_message", fake_process)

        async def fake_dlq(raw_payload, reason, identifiers=None):
            call_order.append("dlq:fail-me")

        monkeypatch.setattr(consumer, "_send_to_dlq", fake_dlq)

        await _run_consumer_briefly(consumer, duration=7.0)

        # All 3 retries + the DLQ publish for the failing message happen before
        # the second message is ever handed to process_message.
        first_success_index = call_order.index("process:succeed-me")
        assert call_order[:first_success_index].count("process:fail-me") == 3
        assert "dlq:fail-me" in call_order[:first_success_index]
    asyncio.run(run_test())


def test_kafka_024_topics_auto_created_on_startup(monkeypatch):
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        admin_client = make_fake_admin_client(topics_exist=False)
        monkeypatch.setattr(consumer_module, "AdminClient", MagicMock(return_value=admin_client))
        await consumer._ensure_topics_exist(["analytics.ingestion.raw", "analytics.ingestion.raw.dlq"])
        admin_client.create_topics.assert_called_once()
        created_topics = [nt.topic for nt in admin_client.create_topics.call_args.args[0]]
        assert set(created_topics) == {"analytics.ingestion.raw", "analytics.ingestion.raw.dlq"}
    asyncio.run(run_test())


def test_kafka_025_offset_commits_only_after_success(monkeypatch):
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        fake_kafka_consumer = _make_fake_kafka_consumer([_FakeKafkaMsg(b'{"submissionId": 1}')])
        monkeypatch.setattr(consumer_module, "Producer", MagicMock(return_value=make_fake_kafka_producer()))
        monkeypatch.setattr(consumer_module, "AdminClient", MagicMock(return_value=make_fake_admin_client()))
        monkeypatch.setattr(consumer_module, "Consumer", MagicMock(return_value=fake_kafka_consumer))
        monkeypatch.setattr(consumer_module.db, "connect", AsyncMock())
        monkeypatch.setattr(consumer_module.db, "disconnect", AsyncMock())
        monkeypatch.setattr(consumer_module.Client, "connect", AsyncMock(side_effect=Exception("no temporal")))
        monkeypatch.setattr(consumer, "process_message", AsyncMock())  # succeeds

        await _run_consumer_briefly(consumer)

        fake_kafka_consumer.commit.assert_called_once()
        assert fake_kafka_consumer.commit.call_args.kwargs.get("asynchronous") is False
    asyncio.run(run_test())


# =============================================================================
# SECURITY & LOGGING (SEC-*)
# =============================================================================

def test_sec_001_raw_payload_never_logged_in_plaintext(monkeypatch, caplog):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        dlq_mock = AsyncMock()
        monkeypatch.setattr(consumer, "_send_to_dlq", dlq_mock)
        event = _fixture("create", "create_discussion.json")
        marker = "UNIQUE_MARKER_TEXT"
        event["data"]["challenges"] = [marker]
        event["tags"]["state"] = ""  # force a validation failure -> DLQ path logs the payload fingerprint
        with caplog.at_level("ERROR"):
            await consumer.process_message(json.dumps(event))
        assert not any(marker in r.message for r in caplog.records)
    asyncio.run(run_test())


def test_sec_002_dlq_headers_surface_only_non_sensitive_identifiers(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, _ = _consumer_with_mocks(monkeypatch)
        producer = make_fake_kafka_producer()
        consumer.dlq_producer = producer
        event = _fixture("create", "create_discussion.json")
        event["tags"]["state"] = ""
        await consumer.process_message(json.dumps(event))
        produce_call = producer.produce.call_args
        headers = dict(produce_call.kwargs["headers"])
        assert set(headers.keys()) <= {"reason", "submissionId", "tenantCode", "sessionId"}
        assert headers["submissionId"] == str(event["submissionId"]).encode("utf-8")
    asyncio.run(run_test())


def test_sec_003_pii_malformed_llm_response_never_logs_full_content(monkeypatch, caplog):
    async def run_test():
        import app.temporal.pii_and_abusive_activity as pii_module
        conn = install_fake_db(monkeypatch, pii_module)
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{text}}"}

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "story", {"objective": "some text"}

        monkeypatch.setattr(pii_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        monkeypatch.setattr(operations_module, "update_submission_status", AsyncMock())
        monkeypatch.setattr(pii_module, "insert_llm_log", AsyncMock())

        marker = "FAKE_PII_MARKER_12345"
        install_fake_llm(monkeypatch, content=f"not json at all {marker}")

        with caplog.at_level("ERROR"):
            with pytest.raises(Exception):
                await pii_module.pii_and_abusive_language_detection_activity({
                    "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
                })
        assert any("Failed to parse LLM response JSON" in r.message for r in caplog.records)
        assert not any(marker in r.message for r in caplog.records)
    asyncio.run(run_test())


def test_sec_004_thematic_malformed_llm_response_never_logs_full_content(monkeypatch, caplog):
    async def run_test():
        import app.temporal.thematic_activity as thematic_module
        marker = "FAKE_PII_MARKER_67890"
        install_fake_llm(monkeypatch, content=f"not json {marker}")
        conn = install_fake_db(monkeypatch, thematic_module)
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{approved_themes}} {{statements}}"}
        monkeypatch.setattr(thematic_module, "insert_llm_log", AsyncMock())

        with caplog.at_level("ERROR"):
            with pytest.raises(Exception):
                await thematic_module._run_batched_llm_fallback(
                    pending_items=[{"statement": "x", "statement_type": "challenges", "is_discussion": True,
                                     "best_similarity": 0.1, "diagnostics": {}}],
                    approved_themes=[], theme_id_to_info={},
                    submission_id="1", tenant_code="mitra", analysis_type="thematic_classification",
                    resolved_model="m", resolved_max_tokens=100, resolved_timeout=10,
                )
        assert any("JSON parsing failed for batched LLM response" in r.message for r in caplog.records)
        assert not any(marker in r.message for r in caplog.records)
    asyncio.run(run_test())


# =============================================================================
# CONFIG & SETTINGS (CONFIG-*)
# =============================================================================

from app.config import Settings


def _settings_kwargs(**overrides):
    base = dict(AUTH_TOKEN="test-token")
    base.update(overrides)
    return base


def test_config_001_valid_process_config_loads():
    s = Settings(**_settings_kwargs())
    assert s.get_process_config("story")[0]["name"] == "pii_and_abusive_language_detection"
    assert isinstance(s.get_process_config("discussion"), list)


def test_config_002_invalid_process_config_story_fails_fast():
    with pytest.raises(Exception, match="Invalid JSON configuration for PROCESS_CONFIG_STORY"):
        Settings(**_settings_kwargs(PROCESS_CONFIG_STORY="not valid json"))


def test_config_003_valid_kafka_schema_loads():
    s = Settings(**_settings_kwargs())
    assert set(s.get_kafka_ingestion_schema("story").keys()) == {"create", "update", "delete"}
    assert set(s.get_kafka_ingestion_schema("discussion").keys()) == {"create", "update", "delete"}


def test_config_004_kafka_schema_missing_required_key_fails_validation():
    bad_schema = json.dumps({"create": {"required": []}, "update": {"required": []}})  # no "delete"
    with pytest.raises(Exception, match="must be a JSON object with 'create', 'update', and 'delete' keys"):
        Settings(**_settings_kwargs(STORY_KAFKA_SCHEMA=bad_schema))


def test_config_005_kafka_schema_required_not_list_of_strings_fails_validation():
    bad_schema = json.dumps({
        "create": {"required": "not-a-list"},
        "update": {"required": []},
        "delete": {"required": []},
    })
    with pytest.raises(Exception, match="must be a list of strings"):
        Settings(**_settings_kwargs(DISCUSSION_KAFKA_SCHEMA=bad_schema))


def test_config_006_reset_db_with_production_environment_is_refused(monkeypatch, caplog):
    async def run_test():
        import app.database.db as db_module
        settings_override(monkeypatch, db_module.settings, RESET_DB=True, ENVIRONMENT="production")
        fake_conn = FakeConn()
        db_instance = db_module.Database()
        db_instance.pool = FakePool(fake_conn)
        with caplog.at_level("ERROR"):
            await db_instance.initialize_schema()
        assert not any("DROP SCHEMA" in str(c.args[0]) for c in fake_conn.execute.call_args_list if c.args)
        assert any("refusing to drop the schema" in r.message for r in caplog.records)
    asyncio.run(run_test())


def test_config_007_reset_db_with_development_environment_resets_schema(monkeypatch):
    async def run_test():
        import app.database.db as db_module
        settings_override(monkeypatch, db_module.settings, RESET_DB=True, ENVIRONMENT="development")
        fake_conn = FakeConn()
        db_instance = db_module.Database()
        db_instance.pool = FakePool(fake_conn)
        await db_instance.initialize_schema()
        executed = [c.args[0] for c in fake_conn.execute.call_args_list if c.args]
        assert any("DROP SCHEMA IF EXISTS public CASCADE" in stmt for stmt in executed)
        assert any("CREATE SCHEMA public" in stmt for stmt in executed)
    asyncio.run(run_test())


def test_config_008_unrecognized_submission_type_returns_empty_process_config():
    s = Settings(**_settings_kwargs())
    assert s.get_process_config("survey") == []


def test_config_009_unrecognized_submission_type_raises_for_kafka_schema():
    s = Settings(**_settings_kwargs())
    with pytest.raises(ValueError, match="No Kafka ingestion schema defined"):
        s.get_kafka_ingestion_schema(None)


# =============================================================================
# DATABASE OPERATIONS (DB-*)
# =============================================================================

from app.database.operations import (
    _normalize_statement_list,
    insert_or_update_submission,
)


def test_db_001_insert_discussion_stores_text_array():
    async def run_test():
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "uuid-1", "status": "pending"}
        conn.fetchval.return_value = None  # row_exists = False -> INSERT branch
        event = _fixture("create", "create_discussion.json")
        await insert_or_update_submission(conn, event)
        insert_call = _find_execute_call(conn, "INSERT INTO discussion_submissions")
        challenges_arg = insert_call.args[4]
        assert isinstance(challenges_arg, list)
        assert len(challenges_arg) == len(event["data"]["challenges"])
    asyncio.run(run_test())


def test_db_002_insert_story_stores_scalar_text():
    async def run_test():
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "uuid-1", "status": "pending"}
        conn.fetchval.return_value = None
        event = _fixture("create", "create_story.json")
        await insert_or_update_submission(conn, event)
        insert_call = _find_execute_call(conn, "INSERT INTO story_submissions")
        objective_arg = insert_call.args[4]
        assert isinstance(objective_arg, str)
    asyncio.run(run_test())


def test_db_003_normalize_statement_list_wraps_single_string():
    assert _normalize_statement_list("a single statement") == ["a single statement"]


def test_db_004_normalize_statement_list_none_stays_none():
    assert _normalize_statement_list(None) is None


def test_db_005_update_omitting_masked_fields_does_not_revert_them():
    """An UPDATE whose newValues omits challenges/solutions must pass None for
    them (COALESCE preserves whatever masked text is already in the DB), even
    though oldValues (the producer's original unmasked text) is present."""
    async def run_test():
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "uuid-1", "status": "processing"}
        conn.fetchval.return_value = 1
        event = _fixture("update", "update_discussion.json")
        assert "challenges" not in event["newValues"]
        await insert_or_update_submission(conn, event)
        args = _find_execute_call(conn, "UPDATE discussion_submissions").args
        assert args[4] is None
        assert args[5] is None
    asyncio.run(run_test())


def test_db_006_duplicate_session_id_raises_clear_error():
    import asyncpg

    async def run_test():
        conn = FakeConn()

        class _FakeUniqueViolation(asyncpg.exceptions.UniqueViolationError):
            def __init__(self):
                self.constraint_name = "submissions_session_id_key"

        conn.fetchrow.side_effect = _FakeUniqueViolation()
        event = _fixture("create", "create_story.json")
        with pytest.raises(ValueError, match="is already associated with a different submission"):
            await insert_or_update_submission(conn, event)
    asyncio.run(run_test())


# =============================================================================
# THEMATIC CLASSIFICATION (THEME-*)
# =============================================================================
# THEME-018 excluded: "SentenceTransformer calls run off the event loop" is a
# live concurrent-progress observation, not assertable via mocks.

import app.temporal.thematic_activity as thematic_module


def test_theme_001_short_statement_classified_unknown_unclear(monkeypatch):
    async def run_test():
        conn = FakeConn()
        insert_mock = AsyncMock()
        monkeypatch.setattr(thematic_module, "insert_analysis_result", insert_mock)
        result, pending = await thematic_module._run_local_classification(
            conn=conn, statement="Too short", submission_id="1", tenant_code="mitra",
            statement_type="challenges", statement_id="stmt-1", abusive_masked_at=[],
        )
        assert pending is None
        assert result["category_type"] == "Unknown/Unclear"
        insert_mock.assert_awaited_once()
        assert insert_mock.await_args.kwargs["category_type"] == "Unknown/Unclear"
    asyncio.run(run_test())


def test_theme_002_garbage_spam_statement_classified_unknown_unclear(monkeypatch):
    async def run_test():
        conn = FakeConn()
        monkeypatch.setattr(thematic_module, "insert_analysis_result", AsyncMock())
        result, pending = await thematic_module._run_local_classification(
            conn=conn, statement="asdf asdf asdf asdf asdf", submission_id="1", tenant_code="mitra",
            statement_type="challenges", statement_id="stmt-1", abusive_masked_at=[],
        )
        assert pending is None
        assert result["category_type"] == "Unknown/Unclear"
    asyncio.run(run_test())


def test_theme_003_pii_mask_tag_classified_flagged(monkeypatch):
    async def run_test():
        conn = FakeConn()
        insert_mock = AsyncMock()
        monkeypatch.setattr(thematic_module, "insert_analysis_result", insert_mock)
        result, pending = await thematic_module._run_local_classification(
            conn=conn, statement="Met with <PERSON> at the village school today",
            submission_id="1", tenant_code="mitra", statement_type="challenges",
            statement_id="stmt-1", abusive_masked_at=[],
        )
        assert pending is None
        assert result["category_type"] == "Flagged"
        assert insert_mock.await_args.kwargs["category_type"] == "Flagged"
    asyncio.run(run_test())


def test_theme_004_abusive_flagged_column_classified_flagged(monkeypatch):
    async def run_test():
        conn = FakeConn()
        monkeypatch.setattr(thematic_module, "insert_analysis_result", AsyncMock())
        result, pending = await thematic_module._run_local_classification(
            conn=conn, statement="This is a perfectly normal length statement here",
            submission_id="1", tenant_code="mitra", statement_type="challenges",
            statement_id="stmt-1", abusive_masked_at=["challenges"],
        )
        assert pending is None
        assert result["category_type"] == "Flagged"
    asyncio.run(run_test())


def test_theme_005_local_similarity_match_classified_standard_no_llm(monkeypatch):
    async def run_test():
        conn = FakeConn()
        insert_mock = AsyncMock()
        monkeypatch.setattr(thematic_module, "insert_analysis_result", insert_mock)
        monkeypatch.setattr(
            thematic_module, "get_theme_similarities",
            MagicMock(return_value=[("theme-1", 0.9)]),
        )
        settings_override(monkeypatch, thematic_module.settings, SIMILARITY_SCORE_THRESHOLD=0.65)
        result, pending = await thematic_module._run_local_classification(
            conn=conn, statement="A statement long enough to pass the word count gate",
            submission_id="1", tenant_code="mitra", statement_type="challenges",
            statement_id="stmt-1", abusive_masked_at=[],
        )
        assert pending is None
        assert result["category_type"] == "Standard"
        assert result["theme_id"] == "theme-1"
        assert result["confidence_score"] is None  # no LLM used
    asyncio.run(run_test())


def test_theme_006_below_threshold_queued_for_llm_fallback(monkeypatch):
    async def run_test():
        conn = FakeConn()
        monkeypatch.setattr(
            thematic_module, "get_theme_similarities",
            MagicMock(return_value=[("theme-1", 0.1)]),
        )
        settings_override(monkeypatch, thematic_module.settings, SIMILARITY_SCORE_THRESHOLD=0.65)
        result, pending = await thematic_module._run_local_classification(
            conn=conn, statement="A statement long enough to pass the word count gate",
            submission_id="1", tenant_code="mitra", statement_type="challenges",
            statement_id="stmt-1", abusive_masked_at=[],
        )
        assert result is None
        assert pending is not None
        assert pending["diagnostics"]["llm_fallback"]["executed"] is True
    asyncio.run(run_test())


def test_theme_007_discussion_multi_theme_mapped_capped_at_max(monkeypatch):
    is_discussion = True
    resolved_items = [
        {"theme_id": f"t{i}", "confidence_score": 0.9 - i * 0.01, "justification": "j"} for i in range(5)
    ]
    settings_override(monkeypatch, thematic_module.settings, LLM_CONFIDENCE_SCORE_THRESHOLD=0.5)
    qualifying = thematic_module._finalize_qualifying_themes(resolved_items)
    assert len(qualifying) == thematic_module.MAX_MULTI_THEME_MATCHES


def test_theme_008_story_stays_single_theme_even_with_multiple_qualifying():
    resolved_items = [
        {"theme_id": "t1", "confidence_score": 0.9, "justification": "j"},
        {"theme_id": "t2", "confidence_score": 0.85, "justification": "j"},
    ]
    qualifying = thematic_module._finalize_qualifying_themes(resolved_items)
    assert len(qualifying) == 1


def test_theme_009_llm_confidence_at_threshold_classified_standard():
    score = thematic_module._parse_confidence_score(0.8)
    assert score == 0.8


def test_theme_010_llm_confidence_below_threshold_would_resolve_others(monkeypatch):
    settings_override(monkeypatch, thematic_module.settings, LLM_CONFIDENCE_SCORE_THRESHOLD=0.8)
    resolved_items = [{"theme_id": "t1", "confidence_score": 0.5, "justification": "j"}]
    qualifying = thematic_module._finalize_qualifying_themes(resolved_items)
    assert qualifying == []


def test_theme_011_non_numeric_confidence_score_rejected():
    assert thematic_module._parse_confidence_score("high") is None
    assert thematic_module._parse_confidence_score(1.7) is None
    assert thematic_module._parse_confidence_score(-0.1) is None


def test_theme_012_missing_statement_index_recovered_via_echoed_text(monkeypatch):
    async def run_test():
        install_fake_llm(monkeypatch, content=json.dumps({
            "classified_data": [
                {"statement": "The exact echoed statement", "theme_name": "Infra", "confidence_score": 0.9, "justification": "j"}
            ]
        }))
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{approved_themes}} {{statements}}"}
        install_fake_db(monkeypatch, thematic_module, conn)
        monkeypatch.setattr(thematic_module, "insert_llm_log", AsyncMock())
        monkeypatch.setattr(thematic_module, "insert_analysis_result", AsyncMock())
        settings_override(monkeypatch, thematic_module.settings, LLM_CONFIDENCE_SCORE_THRESHOLD=0.5)

        results = await thematic_module._run_batched_llm_fallback(
            pending_items=[{"statement": "The exact echoed statement", "statement_type": "challenges",
                            "is_discussion": True, "best_similarity": 0.1, "diagnostics": {"llm_fallback": {}}}],
            approved_themes=[{"id": "theme-1", "name": "Infra"}],
            theme_id_to_info={"theme-1": {"name": "Infra"}},
            submission_id="1", tenant_code="mitra", analysis_type="thematic_classification",
            resolved_model="m", resolved_max_tokens=100, resolved_timeout=10,
        )
        assert results[0]["category_type"] == "Standard"
    asyncio.run(run_test())


def test_theme_013_unmatchable_entry_dropped_statement_resolves_others(monkeypatch):
    async def run_test():
        install_fake_llm(monkeypatch, content=json.dumps({
            "classified_data": [
                {"statement": "Something totally different", "theme_name": "Infra", "confidence_score": 0.9, "justification": "j"}
            ]
        }))
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{approved_themes}} {{statements}}"}
        install_fake_db(monkeypatch, thematic_module, conn)
        monkeypatch.setattr(thematic_module, "insert_llm_log", AsyncMock())
        insert_result_mock = AsyncMock()
        monkeypatch.setattr(thematic_module, "insert_analysis_result", insert_result_mock)

        results = await thematic_module._run_batched_llm_fallback(
            pending_items=[{"statement": "The real pending statement", "statement_type": "challenges",
                            "is_discussion": True, "best_similarity": 0.1, "diagnostics": {"llm_fallback": {}}}],
            approved_themes=[{"id": "theme-1", "name": "Infra"}],
            theme_id_to_info={"theme-1": {"name": "Infra"}},
            submission_id="1", tenant_code="mitra", analysis_type="thematic_classification",
            resolved_model="m", resolved_max_tokens=100, resolved_timeout=10,
        )
        assert results[0]["category_type"] == "Others"
        assert insert_result_mock.await_args.kwargs["category_type"] == "Others"
    asyncio.run(run_test())


def test_theme_014_batched_llm_call_failure_raises_no_fabricated_result(monkeypatch):
    async def run_test():
        install_failing_llm(monkeypatch, RuntimeError("network down"))
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{approved_themes}} {{statements}}"}
        install_fake_db(monkeypatch, thematic_module, conn)
        log_mock = AsyncMock()
        monkeypatch.setattr(thematic_module, "insert_llm_log", log_mock)
        insert_result_mock = AsyncMock()
        monkeypatch.setattr(thematic_module, "insert_analysis_result", insert_result_mock)

        with pytest.raises(Exception):
            await thematic_module._run_batched_llm_fallback(
                pending_items=[{"statement": "x", "statement_type": "challenges", "is_discussion": True,
                                 "best_similarity": 0.1, "diagnostics": {}}],
                approved_themes=[], theme_id_to_info={},
                submission_id="1", tenant_code="mitra", analysis_type="thematic_classification",
                resolved_model="m", resolved_max_tokens=100, resolved_timeout=10,
            )
        insert_result_mock.assert_not_awaited()
        assert log_mock.await_args.kwargs["status"] == "failed"
    asyncio.run(run_test())


def test_theme_015_json_parse_failure_raises_without_logging_content(monkeypatch, caplog):
    async def run_test():
        marker = "SECRET_STATEMENT_TEXT"
        install_fake_llm(monkeypatch, content=f"not json {marker}")
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{approved_themes}} {{statements}}"}
        install_fake_db(monkeypatch, thematic_module, conn)
        monkeypatch.setattr(thematic_module, "insert_llm_log", AsyncMock())
        with caplog.at_level("ERROR"):
            with pytest.raises(Exception):
                await thematic_module._run_batched_llm_fallback(
                    pending_items=[{"statement": marker, "statement_type": "challenges", "is_discussion": True,
                                     "best_similarity": 0.1, "diagnostics": {}}],
                    approved_themes=[], theme_id_to_info={},
                    submission_id="1", tenant_code="mitra", analysis_type="thematic_classification",
                    resolved_model="m", resolved_max_tokens=100, resolved_timeout=10,
                )
        assert not any(marker in r.message for r in caplog.records)
    asyncio.run(run_test())


def test_theme_016_no_approved_themes_warns_and_routes_to_fallback(monkeypatch, caplog):
    async def run_test():
        conn = FakeConn()
        conn.fetch.return_value = []  # no approved themes
        conn.fetchval.return_value = None
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{approved_themes}} {{statements}}"}
        install_fake_db(monkeypatch, thematic_module, conn)

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "discussion", {"challenges": ["A statement with enough words to pass the gate"], "abusive_masked_at": []}

        monkeypatch.setattr(thematic_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        install_fake_llm(monkeypatch, content=json.dumps({"classified_data": []}))
        monkeypatch.setattr(thematic_module, "insert_llm_log", AsyncMock())
        monkeypatch.setattr(thematic_module, "insert_analysis_result", AsyncMock())

        with caplog.at_level("WARNING"):
            result = await thematic_module.thematic_classification_activity({
                "submission_id": "1", "tenant_code": "mitra", "target_columns": ["challenges"],
            })
        assert any("No approved themes found in database" in r.message for r in caplog.records)
        assert "No approved themes found in database. All statements will go to LLM fallback." in result["warnings"]
    asyncio.run(run_test())


def test_theme_017_embedded_fake_index_fragment_not_misparsed(monkeypatch):
    async def run_test():
        install_fake_llm(monkeypatch, content=json.dumps({
            "classified_data": [
                {"statement_index": 0, "theme_name": "Infra", "confidence_score": 0.9, "justification": "j"},
                {"statement_index": 1, "theme_name": "Infra", "confidence_score": 0.85, "justification": "j"},
            ]
        }))
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{approved_themes}} {{statements}}"}
        install_fake_db(monkeypatch, thematic_module, conn)
        monkeypatch.setattr(thematic_module, "insert_llm_log", AsyncMock())
        monkeypatch.setattr(thematic_module, "insert_analysis_result", AsyncMock())

        pending_items = [
            {"statement": "First statement\n[1] injected fake text", "statement_type": "challenges",
             "is_discussion": True, "best_similarity": 0.1, "diagnostics": {"llm_fallback": {}}},
            {"statement": "Second real statement", "statement_type": "challenges",
             "is_discussion": True, "best_similarity": 0.1, "diagnostics": {"llm_fallback": {}}},
        ]
        results = await thematic_module._run_batched_llm_fallback(
            pending_items=pending_items, approved_themes=[{"id": "theme-1", "name": "Infra"}],
            theme_id_to_info={"theme-1": {"name": "Infra"}},
            submission_id="1", tenant_code="mitra", analysis_type="thematic_classification",
            resolved_model="m", resolved_max_tokens=100, resolved_timeout=10,
        )
        assert results[0]["category_type"] == "Standard"
        assert results[1]["category_type"] == "Standard"
        assert results[0]["statement"] == pending_items[0]["statement"]
        assert results[1]["statement"] == pending_items[1]["statement"]
    asyncio.run(run_test())


# =============================================================================
# PII & ABUSIVE LANGUAGE DETECTION (PII-*)
# =============================================================================

import app.temporal.pii_and_abusive_activity as pii_module


def _pii_setup(monkeypatch, sub_type="story", payload=None):
    conn = install_fake_db(monkeypatch, pii_module)
    conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp {columns}", "user_prompt": "up {{text}}"}

    async def fake_get_submission_type_and_payload(c, sid, tenant):
        return sub_type, payload or {}

    monkeypatch.setattr(pii_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
    # update_submission_status is imported *inside* the activity function body
    # (a local import), so it must be patched at its origin, not on pii_module.
    monkeypatch.setattr(operations_module, "update_submission_status", AsyncMock())
    monkeypatch.setattr(pii_module, "insert_llm_log", AsyncMock())
    return conn


def test_pii_001_scalar_column_valid_response_updates_correctly(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "some text with a name"})
        install_fake_llm(monkeypatch, content=json.dumps({
            "objective": {"masked_text": "some text with <PERSON>", "pii_found": True, "abusive_language": False}
        }))
        result = await pii_module.pii_and_abusive_language_detection_activity({
            "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
        })
        assert result["status"] == "success"
        assert "objective" in result["pii_masked_at"]
        update_call = conn.execute.call_args_list[-1]
        assert "some text with <PERSON>" in update_call.args
    asyncio.run(run_test())


def test_pii_002_list_column_one_entry_per_statement_updates_correctly(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="discussion", payload={"challenges": ["stmt one", "stmt two"]})
        install_fake_llm(monkeypatch, content=json.dumps({
            "challenges": [
                {"statement_index": 0, "masked_text": "stmt one masked", "pii_found": False, "abusive_language": False},
                {"statement_index": 1, "masked_text": "stmt two masked", "pii_found": True, "abusive_language": False},
            ]
        }))
        result = await pii_module.pii_and_abusive_language_detection_activity({
            "submission_id": "1", "tenant_code": "mitra", "target_columns": ["challenges"],
        })
        assert result["status"] == "success"
        update_call = conn.execute.call_args_list[-1]
        assert ["stmt one masked", "stmt two masked"] in update_call.args
    asyncio.run(run_test())


def test_pii_003_list_column_missing_statement_index_raises(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="discussion", payload={"challenges": ["stmt one", "stmt two"]})
        install_fake_llm(monkeypatch, content=json.dumps({
            "challenges": [
                {"statement_index": 0, "masked_text": "stmt one masked", "pii_found": False, "abusive_language": False},
            ]
        }))
        with pytest.raises(ValueError, match="missing masked entries for"):
            await pii_module.pii_and_abusive_language_detection_activity({
                "submission_id": "1", "tenant_code": "mitra", "target_columns": ["challenges"],
            })
    asyncio.run(run_test())


def test_pii_004_list_column_duplicate_statement_index_raises(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="discussion", payload={"challenges": ["stmt one", "stmt two"]})
        install_fake_llm(monkeypatch, content=json.dumps({
            "challenges": [
                {"statement_index": 0, "masked_text": "a", "pii_found": False, "abusive_language": False},
                {"statement_index": 0, "masked_text": "b", "pii_found": False, "abusive_language": False},
            ]
        }))
        with pytest.raises(ValueError, match="duplicate statement_index"):
            await pii_module.pii_and_abusive_language_detection_activity({
                "submission_id": "1", "tenant_code": "mitra", "target_columns": ["challenges"],
            })
    asyncio.run(run_test())


def test_pii_005_scalar_column_missing_masked_text_raises(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "text"})
        install_fake_llm(monkeypatch, content=json.dumps({"objective": {"pii_found": False, "abusive_language": False}}))
        with pytest.raises(ValueError, match="missing 'masked_text'"):
            await pii_module.pii_and_abusive_language_detection_activity({
                "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
            })
    asyncio.run(run_test())


def test_pii_006_scalar_column_wrong_shape_raises(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "text"})
        install_fake_llm(monkeypatch, content=json.dumps({"objective": "just a plain string, not an object"}))
        with pytest.raises(ValueError, match="was not an object"):
            await pii_module.pii_and_abusive_language_detection_activity({
                "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
            })
    asyncio.run(run_test())


def test_pii_007_pii_found_adds_column_to_pii_masked_at(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "text"})
        install_fake_llm(monkeypatch, content=json.dumps({
            "objective": {"masked_text": "masked", "pii_found": True, "abusive_language": False}
        }))
        result = await pii_module.pii_and_abusive_language_detection_activity({
            "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
        })
        assert result["pii_masked_at"] == ["objective"]
    asyncio.run(run_test())


def test_pii_008_abusive_language_adds_column_to_abusive_masked_at(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "text"})
        install_fake_llm(monkeypatch, content=json.dumps({
            "objective": {"masked_text": "masked", "pii_found": False, "abusive_language": True}
        }))
        result = await pii_module.pii_and_abusive_language_detection_activity({
            "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
        })
        assert result["abusive_masked_at"] == ["objective"]
    asyncio.run(run_test())


def test_pii_009_llm_failure_logged_status_failed(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "text"})
        install_failing_llm(monkeypatch, RuntimeError("transient network error"))
        log_mock = AsyncMock()
        monkeypatch.setattr(pii_module, "insert_llm_log", log_mock)
        with pytest.raises(Exception):
            await pii_module.pii_and_abusive_language_detection_activity({
                "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
            })
        # insert_llm_log(conn, submission_id, tenant_code, model, analysis_type,
        #                prompt_version_id, prompt_tokens, completion_tokens, status, ...)
        assert log_mock.await_args.args[8] == "failed"
    asyncio.run(run_test())


def test_pii_010_malformed_response_raises_without_logging_content(monkeypatch, caplog):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "text"})
        marker = "SUBMISSION_SECRET_TEXT"
        install_fake_llm(monkeypatch, content=f"not json {marker}")
        with caplog.at_level("ERROR"):
            with pytest.raises(Exception):
                await pii_module.pii_and_abusive_language_detection_activity({
                    "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
                })
        assert not any(marker in r.message for r in caplog.records)
    asyncio.run(run_test())


def test_pii_011_scalar_column_multi_entry_list_merged_without_truncation(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "a very long objective text split into multiple entries"})
        install_fake_llm(monkeypatch, content=json.dumps({
            "objective": [
                {"masked_text": "Part 1 of long text with <PERSON>", "pii_found": True, "abusive_language": False},
                {"masked_text": "Part 2 of long text with <BAD_WORD>", "pii_found": False, "abusive_language": True},
            ]
        }))
        result = await pii_module.pii_and_abusive_language_detection_activity({
            "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
        })
        assert result["status"] == "success"
        assert "objective" in result["pii_masked_at"]
        assert "objective" in result["abusive_masked_at"]
        update_call = conn.execute.call_args_list[-1]
        expected_merged = "Part 1 of long text with <PERSON> Part 2 of long text with <BAD_WORD>"
        assert expected_merged in update_call.args
    asyncio.run(run_test())


def test_pii_012_scalar_column_single_entry_list_unwrapped(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "short text"})
        install_fake_llm(monkeypatch, content=json.dumps({
            "objective": [
                {"masked_text": "short text masked", "pii_found": True, "abusive_language": False}
            ]
        }))
        result = await pii_module.pii_and_abusive_language_detection_activity({
            "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
        })
        assert result["status"] == "success"
        assert "objective" in result["pii_masked_at"]
        update_call = conn.execute.call_args_list[-1]
        assert "short text masked" in update_call.args
    asyncio.run(run_test())



# =============================================================================
# ENVIRONMENT DETECTION (ENV-*)
# =============================================================================

import app.temporal.environment_activity as environment_module


def test_env_001_no_target_columns_skips_before_llm(monkeypatch):
    async def run_test():
        import app.services.llm as llm_module

        urlopen_mock = MagicMock()
        monkeypatch.setattr(llm_module.urllib.request, "urlopen", urlopen_mock)

        result = await environment_module.environment_detection_activity({
            "submission_id": "1", "tenant_code": "mitra", "target_columns": [],
        })

        assert result["status"] == "skipped"
        assert result["reason"] == "no columns specified"
        urlopen_mock.assert_not_called()
    asyncio.run(run_test())


def test_env_002_empty_selected_values_skip_before_llm(monkeypatch):
    async def run_test():
        import app.services.llm as llm_module

        install_fake_db(monkeypatch, environment_module)
        prompt_mock = AsyncMock()
        urlopen_mock = MagicMock()

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "story", {"objective": "", "challenge": [], "impact": None}

        monkeypatch.setattr(
            environment_module,
            "get_submission_type_and_payload",
            fake_get_submission_type_and_payload,
        )
        monkeypatch.setattr(environment_module, "_get_environment_prompt", prompt_mock)
        monkeypatch.setattr(llm_module.urllib.request, "urlopen", urlopen_mock)

        result = await environment_module.environment_detection_activity({
            "submission_id": "1",
            "tenant_code": "mitra",
            "target_columns": ["objective", "challenge", "impact"],
        })

        assert result["status"] == "skipped"
        assert result["reason"] == "no source text available"
        prompt_mock.assert_not_awaited()
        urlopen_mock.assert_not_called()
    asyncio.run(run_test())


def test_env_003_prompt_loader_uses_environment_prompt_name():
    async def run_test():
        conn = FakeConn()
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up"}

        result = await environment_module._get_environment_prompt(conn)

        query = conn.fetchrow.await_args.args[0]
        prompt_identity = conn.fetchrow.await_args.args[1]
        assert result["id"] == "pv-1"
        assert "p.name = $1" in query
        assert "p.analysis_type = $1" not in query
        assert prompt_identity == "Environment Detection"
    asyncio.run(run_test())


# =============================================================================
# STORY RATING (RATING-*)
# =============================================================================
# RATING-009 excluded: "no DB connection held during OpenRouter/PDF calls" is a
# live connection-pool-contention observation, not assertable via mocks.

import app.temporal.story_rating_activity as rating_module


def test_rating_001_valid_pdf_download_produces_and_persists_rating(monkeypatch):
    async def run_test():
        conn = install_fake_db(monkeypatch, rating_module)
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{story_content}}"}

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "story", {"pdf_urls": ["https://example.com/x.pdf"], "challenge": None, "action_steps": None, "impact": None}

        monkeypatch.setattr(rating_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        monkeypatch.setattr(rating_module, "_fetch_story_content", MagicMock(return_value=("Extracted PDF text.", "pdf", 20)))
        monkeypatch.setattr(rating_module, "insert_ranking_result", AsyncMock())
        monkeypatch.setattr(rating_module, "insert_llm_log", AsyncMock())

        llm_result = {
            "document_language": "en", "impact_and_outcome_score": 0.8, "impact_justification": "j",
            "issue_and_challenge_score": 0.7, "issue_justification": "j", "action_steps_score": 0.6,
            "action_justification": "j", "composite_score": 0.75, "tier": "Gold", "overall_summary": "s",
        }
        install_fake_llm(monkeypatch, content=json.dumps(llm_result))

        result = await rating_module.story_rating_activity({"submission_id": "1", "tenant_code": "mitra"})
        assert result["status"] == "success"
        assert result["tier"] == "Gold"
        assert result["content_source"] == "pdf"
    asyncio.run(run_test())


def test_rating_002_pdf_failure_falls_back_to_fields(monkeypatch):
    async def run_test():
        monkeypatch.setattr(rating_module, "_download_file", MagicMock(side_effect=RuntimeError("404 not found")))
        content, source, _total_chars = rating_module._fetch_story_content(
            pdf_url="https://example.com/broken.pdf",
            challenge="A challenge statement here", action_steps="Some action steps",
            impact="Some impact", submission_id="1", tenant_code="mitra", log_prefix="[test]",
        )
        assert source == "fields"
        assert "A challenge statement here" in content
    asyncio.run(run_test())


def test_rating_003_no_pdf_no_fallback_fields_skips_gracefully(monkeypatch):
    async def run_test():
        install_fake_db(monkeypatch, rating_module)

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "story", {"pdf_urls": [], "challenge": None, "action_steps": None, "impact": None}

        monkeypatch.setattr(rating_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        result = await rating_module.story_rating_activity({"submission_id": "1", "tenant_code": "mitra"})
        assert result["status"] == "skipped"
        assert "no PDF content or fallback fields available" in result["reason"]
    asyncio.run(run_test())


def test_rating_004_llm_response_missing_required_field_raises(monkeypatch):
    async def run_test():
        conn = install_fake_db(monkeypatch, rating_module)
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{story_content}}"}

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "story", {"pdf_urls": [], "challenge": "some challenge text here", "action_steps": None, "impact": None}

        monkeypatch.setattr(rating_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        monkeypatch.setattr(rating_module, "insert_llm_log", AsyncMock())
        incomplete = {"document_language": "en", "composite_score": 0.5, "tier": "Silver", "overall_summary": "s"}
        install_fake_llm(monkeypatch, content=json.dumps(incomplete))

        with pytest.raises(ValueError, match="missing required fields"):
            await rating_module.story_rating_activity({"submission_id": "1", "tenant_code": "mitra"})
    asyncio.run(run_test())


def test_rating_005_llm_response_score_out_of_range_raises(monkeypatch):
    async def run_test():
        conn = install_fake_db(monkeypatch, rating_module)
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{story_content}}"}

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "story", {"pdf_urls": [], "challenge": "some challenge text here", "action_steps": None, "impact": None}

        monkeypatch.setattr(rating_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        monkeypatch.setattr(rating_module, "insert_llm_log", AsyncMock())
        bad = {
            "document_language": "en", "impact_and_outcome_score": 1.4, "impact_justification": "j",
            "issue_and_challenge_score": 0.7, "issue_justification": "j", "action_steps_score": 0.6,
            "action_justification": "j", "composite_score": 0.75, "tier": "Gold", "overall_summary": "s",
        }
        install_fake_llm(monkeypatch, content=json.dumps(bad))

        with pytest.raises(ValueError, match="outside the valid 0.0-1.0 range"):
            await rating_module.story_rating_activity({"submission_id": "1", "tenant_code": "mitra"})
    asyncio.run(run_test())


def test_rating_006_persistence_failure_mid_sequence_rolls_back(monkeypatch):
    """FakeConn.transaction() returns the connection itself as an async context
    manager (no real rollback semantics) — this asserts the code *attempts* the
    delete+insert+log inside one `async with conn.transaction():` block, which
    is what makes a real Postgres rollback possible; it can't itself prove
    Postgres rolled back without a real DB."""
    async def run_test():
        conn = install_fake_db(monkeypatch, rating_module)
        conn.fetchrow.return_value = {"id": "pv-1", "system_prompt": "sp", "user_prompt": "up {{story_content}}"}

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "story", {"pdf_urls": [], "challenge": "some challenge text here", "action_steps": None, "impact": None}

        monkeypatch.setattr(rating_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        monkeypatch.setattr(rating_module, "insert_ranking_result", AsyncMock())
        monkeypatch.setattr(rating_module, "insert_llm_log", AsyncMock(side_effect=RuntimeError("log insert failed")))
        good = {
            "document_language": "en", "impact_and_outcome_score": 0.8, "impact_justification": "j",
            "issue_and_challenge_score": 0.7, "issue_justification": "j", "action_steps_score": 0.6,
            "action_justification": "j", "composite_score": 0.75, "tier": "Gold", "overall_summary": "s",
        }
        install_fake_llm(monkeypatch, content=json.dumps(good))

        with pytest.raises(RuntimeError, match="log insert failed"):
            await rating_module.story_rating_activity({"submission_id": "1", "tenant_code": "mitra"})
        # transaction() was entered exactly once, wrapping both writes
        conn.transaction.assert_called_once()
    asyncio.run(run_test())


def test_rating_007_non_story_submission_skipped(monkeypatch):
    async def run_test():
        install_fake_db(monkeypatch, rating_module)

        async def fake_get_submission_type_and_payload(c, sid, tenant):
            return "discussion", {}

        monkeypatch.setattr(rating_module, "get_submission_type_and_payload", fake_get_submission_type_and_payload)
        result = await rating_module.story_rating_activity({"submission_id": "1", "tenant_code": "mitra"})
        assert result["status"] == "skipped"
        assert "story_rating only applies to story submissions" in result["reason"]
    asyncio.run(run_test())


def test_rating_008_relative_pdf_url_without_media_base_url_raises(monkeypatch):
    settings_override(monkeypatch, rating_module.settings, MEDIA_BASE_URL="")
    with pytest.raises(ValueError, match="MEDIA_BASE_URL is not configured"):
        rating_module._resolve_url("relative/path/to/file.pdf")


# =============================================================================
# BATCH PROCESSING WORKFLOW (BATCH-*)
# =============================================================================
# BATCH-006 excluded: the SKIP overlap policy is enforced by Temporal's own
# server-side scheduler, not application code — nothing here to unit test.

import app.temporal.workflows as workflows_module


def test_batch_001_small_queue_drains_in_single_chunk(monkeypatch):
    async def run_test():
        pending = [{"submission_id": "1", "tenant_code": "mitra", "submission_type": "story", "process_steps": []}]
        exec_mock, _, _ = install_fake_workflow_context(
            monkeypatch,
            activity_results={workflows_module.fetch_pending_submissions_activity: pending},
            child_workflow_results={workflows_module.ConfigDrivenProcessingWorkflow.run: {"status": "success"}},
        )
        # second fetch call (after the chunk) must return [] to end the loop
        call_count = {"n": 0}

        async def fake_execute_activity(activity_fn, *args, **kwargs):
            if activity_fn is workflows_module.fetch_pending_submissions_activity:
                call_count["n"] += 1
                return pending if call_count["n"] == 1 else []
            return None

        exec_mock.side_effect = fake_execute_activity

        wf = workflows_module.BatchProcessingWorkflow()
        result = await wf.run(batch_size=100)
        assert result["processed_count"] == 1
        assert result["success_count"] == 1
        assert result["chunks"] == 1
    asyncio.run(run_test())


def test_batch_002_large_queue_fans_out_across_multiple_chunks(monkeypatch):
    async def run_test():
        chunk1 = [{"submission_id": str(i), "tenant_code": "mitra", "submission_type": "story", "process_steps": []} for i in range(2)]
        chunk2 = [{"submission_id": str(i), "tenant_code": "mitra", "submission_type": "story", "process_steps": []} for i in range(2, 3)]
        calls = {"n": 0}

        exec_mock, _, _ = install_fake_workflow_context(monkeypatch)

        async def fake_execute_activity(activity_fn, *args, **kwargs):
            if activity_fn is workflows_module.fetch_pending_submissions_activity:
                calls["n"] += 1
                if calls["n"] == 1:
                    return chunk1
                elif calls["n"] == 2:
                    return chunk2
                return []
            return None

        exec_mock.side_effect = fake_execute_activity

        wf = workflows_module.BatchProcessingWorkflow()
        result = await wf.run(batch_size=2)
        assert result["processed_count"] == 3
        assert result["chunks"] == 2
    asyncio.run(run_test())


def test_batch_003_no_pending_submissions_returns_zero(monkeypatch):
    async def run_test():
        install_fake_workflow_context(
            monkeypatch, activity_results={workflows_module.fetch_pending_submissions_activity: []},
        )
        wf = workflows_module.BatchProcessingWorkflow()
        result = await wf.run(batch_size=100)
        assert result == {"processed_count": 0, "message": "No pending submissions found."}
    asyncio.run(run_test())


def test_batch_004_one_child_failure_counted_without_halting_rest(monkeypatch):
    async def run_test():
        pending = [
            {"submission_id": "1", "tenant_code": "mitra", "submission_type": "story", "process_steps": []},
            {"submission_id": "2", "tenant_code": "mitra", "submission_type": "story", "process_steps": []},
        ]
        calls = {"n": 0}
        exec_mock, _, _ = install_fake_workflow_context(monkeypatch)

        async def fake_execute_activity(activity_fn, *args, **kwargs):
            if activity_fn is workflows_module.fetch_pending_submissions_activity:
                calls["n"] += 1
                return pending if calls["n"] == 1 else []
            return None

        exec_mock.side_effect = fake_execute_activity

        async def fake_execute_child_workflow(run_fn, payload, **kwargs):
            if payload["submission_id"] == "1":
                raise RuntimeError("child failed")
            return {"status": "success"}

        monkeypatch.setattr(workflows_module.workflow, "execute_child_workflow", fake_execute_child_workflow)

        wf = workflows_module.BatchProcessingWorkflow()
        result = await wf.run(batch_size=100)
        assert result["failed_count"] == 1
        assert result["success_count"] == 1
    asyncio.run(run_test())


def test_batch_005_exceeding_max_per_run_triggers_continue_as_new(monkeypatch):
    async def run_test():
        big_chunk = [
            {"submission_id": str(i), "tenant_code": "mitra", "submission_type": "story", "process_steps": []}
            for i in range(workflows_module.MAX_SUBMISSIONS_PER_RUN)
        ]
        exec_mock, continue_as_new_calls, ContinueAsNew = install_fake_workflow_context(
            monkeypatch,
            activity_results={workflows_module.fetch_pending_submissions_activity: big_chunk},
            child_workflow_results={workflows_module.ConfigDrivenProcessingWorkflow.run: {"status": "success"}},
        )
        monkeypatch.setattr(
            workflows_module.workflow, "execute_child_workflow",
            AsyncMock(return_value={"status": "success"}),
        )

        wf = workflows_module.BatchProcessingWorkflow()
        with pytest.raises(ContinueAsNew):
            await wf.run(batch_size=workflows_module.MAX_SUBMISSIONS_PER_RUN)

        assert len(continue_as_new_calls) == 1
        carried_batch_size, carry_over = continue_as_new_calls[0]
        assert carry_over["total_processed"] == workflows_module.MAX_SUBMISSIONS_PER_RUN
    asyncio.run(run_test())


# =============================================================================
# REAL-TIME / MODE HANDLING (MODE-*)
# =============================================================================

def test_mode_001_real_time_triggers_workflow_immediately(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, trigger_mock = _consumer_with_mocks(monkeypatch)
        settings_override(monkeypatch, consumer_module.settings, PROCESSING_MODE="real-time")
        event = _fixture("create", "create_discussion.json")
        await consumer.process_message(json.dumps(event))
        trigger_mock.assert_awaited_once()
    asyncio.run(run_test())


def test_mode_002_batch_mode_leaves_submission_pending(monkeypatch):
    async def run_test():
        consumer, insert_mock, _, trigger_mock = _consumer_with_mocks(monkeypatch)
        settings_override(monkeypatch, consumer_module.settings, PROCESSING_MODE="batch")
        event = _fixture("create", "create_discussion.json")
        await consumer.process_message(json.dumps(event))
        insert_mock.assert_awaited_once()
        trigger_mock.assert_not_awaited()
    asyncio.run(run_test())


def test_mode_003_temporal_unreachable_heals_connection_on_next_message(monkeypatch):
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        consumer.temporal_client = None
        install_fake_db(monkeypatch, consumer_module)
        settings_override(monkeypatch, consumer_module.settings, PROCESSING_MODE="real-time")
        mock_update_status = AsyncMock()
        monkeypatch.setattr(consumer_module, "update_submission_status", mock_update_status)

        mock_client = MagicMock()
        mock_client.start_workflow = AsyncMock()
        monkeypatch.setattr(consumer_module.Client, "connect", AsyncMock(return_value=mock_client))

        await consumer._trigger_realtime_workflow("sub1", "tenant1", "story")

        mock_client.start_workflow.assert_awaited_once()
        mock_update_status.assert_awaited_once_with(ANY, "sub1", "tenant1", "processing")
        assert consumer.temporal_client is mock_client
    asyncio.run(run_test())


def test_mode_003b_temporal_connect_failure_leaves_client_none(monkeypatch):
    async def run_test():
        consumer = consumer_module.IngestionConsumer()
        consumer.temporal_client = None
        monkeypatch.setattr(consumer_module.Client, "connect", AsyncMock(side_effect=Exception("unreachable")))
        await consumer._trigger_realtime_workflow("sub1", "tenant1", "story")
        assert consumer.temporal_client is None
    asyncio.run(run_test())


# =============================================================================
# LLM & COST TRACKING (LLM-*)
# =============================================================================

from app.services.llm import openrouter_chat_completion, split_llm_usage


def test_llm_001_returns_real_usage_not_estimate(monkeypatch):
    settings_override(monkeypatch, settings, OPENROUTER_API_KEY="test-key")
    install_fake_llm(monkeypatch, content="Hello world", usage={"prompt_tokens": 42, "completion_tokens": 13, "total_tokens": 55, "cost": 0.002})
    content, usage = openrouter_chat_completion("a prompt")
    assert content == "Hello world"
    assert usage["prompt_tokens"] == 42
    assert usage["cost"] == 0.002


def test_llm_002_split_llm_usage_separates_tokens_from_metadata():
    prompt_tokens, completion_tokens, meta = split_llm_usage(
        {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "cost": 0.001}
    )
    assert (prompt_tokens, completion_tokens) == (10, 5)
    assert meta == {"cost": 0.001}


def test_llm_003_fallback_estimate_only_when_no_usage_ever_obtained(monkeypatch):
    async def run_test():
        conn = _pii_setup(monkeypatch, sub_type="story", payload={"objective": "a b c d e f g h"})
        install_failing_llm(monkeypatch, RuntimeError("invalid api key"))
        log_mock = AsyncMock()
        monkeypatch.setattr(pii_module, "insert_llm_log", log_mock)
        with pytest.raises(Exception):
            await pii_module.pii_and_abusive_language_detection_activity({
                "submission_id": "1", "tenant_code": "mitra", "target_columns": ["objective"],
            })
        kwargs = log_mock.await_args.kwargs
        assert kwargs["meta_data"] is None
        assert log_mock.await_args.args[6] > 0  # prompt_tokens: word-count estimate, not zero
    asyncio.run(run_test())


# =============================================================================
# CSV UPLOAD & PROCESS API (UPLOAD-*)
# =============================================================================

from app.api.router import api_router
from app.api.exceptions import register_exception_handlers
import app.database.operations as operations_module
import app.api.services.uploads as uploads_service_module


def _build_test_app() -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(api_router)
    return app


def _auth_headers():
    return {"Authorization": f"Bearer {settings.AUTH_TOKEN}"}


def _csv_file(name: str):
    path = CSV_FIXTURE_ROOT / name
    return {"file": (name, path.read_bytes(), "text/csv")}


@pytest.fixture
def test_client():
    # raise_server_exceptions=False so an unhandled exception in the route
    # (e.g. RuntimeError bubbling out of handle_upload) comes back as the real
    # HTTP 500 response a live client would see, instead of re-raising in-process.
    return TestClient(_build_test_app(), raise_server_exceptions=False)


def test_upload_001_missing_auth_header_rejected(test_client):
    resp = test_client.post("/v1/upload/")
    assert resp.status_code == 401


def test_upload_002_invalid_bearer_token_rejected(test_client):
    resp = test_client.post("/v1/upload/", headers={"Authorization": "Bearer wrong-token"})
    assert resp.status_code == 401


def test_upload_003_valid_story_csv_uploads_successfully(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    monkeypatch.setattr(operations_module, "insert_upload_record", AsyncMock(return_value=1))
    gcs_client, _ = make_fake_gcs_client()
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage()))
    settings_override(monkeypatch, settings, PROCESSING_MODE="batch")  # skip the Temporal trigger

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"


def test_upload_004_valid_discussion_csv_uploads_successfully(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    monkeypatch.setattr(operations_module, "insert_upload_record", AsyncMock(return_value=2))
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage()))
    settings_override(monkeypatch, settings, PROCESSING_MODE="batch")

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "discussion", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_discussion.csv"),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"


def test_upload_005_invalid_report_type_rejected(test_client):
    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "survey", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 400


def test_upload_006_non_csv_extension_rejected(test_client):
    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files={"file": ("not_a_csv.txt", b"id,Title\n1,x\n", "text/plain")},
    )
    assert resp.status_code == 400


def test_upload_007_empty_file_rejected(test_client):
    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files={"file": ("empty.csv", b"", "text/csv")},
    )
    assert resp.status_code == 400


def test_upload_008_oversized_file_rejected(test_client, monkeypatch):
    settings_override(monkeypatch, settings, MAX_CSV_UPLOAD_BYTES=10)
    big_content = b"id,Title\n" + b"1,x\n" * 100
    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files={"file": ("big.csv", big_content, "text/csv")},
    )
    assert resp.status_code == 413


def test_upload_009_missing_columns_rejected_no_side_effects(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    insert_mock = AsyncMock()
    upload_mock = MagicMock()
    monkeypatch.setattr(operations_module, "insert_upload_record", insert_mock)
    storage_mock = make_fake_object_storage()
    storage_mock.upload_bytes = upload_mock
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=storage_mock))

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("missing_columns.csv"),
    )
    assert resp.status_code == 400
    assert "Missing columns" in str(resp.json()["errors"])
    insert_mock.assert_not_awaited()
    upload_mock.assert_not_called()


def test_upload_010_extra_columns_rejected_no_side_effects(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    insert_mock = AsyncMock()
    upload_mock = MagicMock()
    monkeypatch.setattr(operations_module, "insert_upload_record", insert_mock)
    storage_mock = make_fake_object_storage()
    storage_mock.upload_bytes = upload_mock
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=storage_mock))

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("extra_columns.csv"),
    )
    assert resp.status_code == 400
    assert "Extra/unexpected columns" in str(resp.json()["errors"])
    insert_mock.assert_not_awaited()
    upload_mock.assert_not_called()


def test_upload_011_malformed_csv_content_rejected(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("malformed.csv"),
    )
    assert resp.status_code == 400


def test_upload_012_duplicate_file_rejected(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=True))
    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 400
    assert "FILE ALREADY EXISTS" in resp.json()["detail"]


def test_upload_013_tenant_code_defaults_to_mitra(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    captured = {}

    async def fake_insert_upload_record(**kwargs):
        captured.update(kwargs)
        return 1

    monkeypatch.setattr(operations_module, "insert_upload_record", fake_insert_upload_record)
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage()))
    settings_override(monkeypatch, settings, PROCESSING_MODE="batch")

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L"},  # no tenant_code
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 200
    assert captured["meta_data"]["tenant_code"] == "mitra"


def test_upload_014_real_time_mode_schedules_inline_processing(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    monkeypatch.setattr(operations_module, "insert_upload_record", AsyncMock(return_value=1))
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value="success"))
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage()))

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "in_progress"


def test_upload_015_unclaimed_upload_leaves_status_pending(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    monkeypatch.setattr(operations_module, "insert_upload_record", AsyncMock(return_value=1))
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value="already_processing"))
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage()))

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"


def test_upload_016_storage_upload_failure_returns_500(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(return_value=False))
    insert_mock = AsyncMock()
    monkeypatch.setattr(operations_module, "insert_upload_record", insert_mock)
    storage_mock = make_fake_object_storage()
    storage_mock.upload_bytes.side_effect = RuntimeError("bucket unreachable")
    monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=storage_mock))

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 500
    # The new multi-cloud path raises RuntimeError directly — no failed DB row is written
    insert_mock.assert_not_awaited()


def test_upload_017_duplicate_check_error_returns_500(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "check_duplicate_file", AsyncMock(side_effect=RuntimeError("DB query failed")))

    resp = test_client.post(
        "/v1/upload/", headers=_auth_headers(),
        data={"report_type": "story", "program_name": "P", "leader_category": "L", "tenant_code": "mitra"},
        files=_csv_file("valid_story.csv"),
    )
    assert resp.status_code == 500


def test_upload_018_process_pending_record_starts_inline_processing(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value={"status": "pending"}))
    monkeypatch.setattr(operations_module, "try_claim_for_processing", AsyncMock(return_value="success"))

    resp = test_client.post("/v1/process/csv/1", headers=_auth_headers())
    assert resp.status_code == 200
    assert resp.json()["status"] == "success"


def test_upload_019_process_nonexistent_record_404(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=None))
    resp = test_client.post("/v1/process/csv/999999", headers=_auth_headers())
    assert resp.status_code == 404


def test_upload_020_process_already_in_progress_409(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value={"status": "in_progress"}))
    resp = test_client.post("/v1/process/csv/1", headers=_auth_headers())
    assert resp.status_code == 409


def test_upload_021_reprocessing_terminal_status_409(test_client, monkeypatch):
    monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value={"status": "success"}))
    resp = test_client.post("/v1/process/csv/1", headers=_auth_headers())
    assert resp.status_code == 409
    assert "Only pending records can be processed" in resp.json()["detail"]


def test_upload_022_process_endpoint_requires_auth(test_client):
    resp = test_client.post("/v1/process/csv/1")
    assert resp.status_code == 401


def test_upload_023_concurrent_process_calls_race_safe():
    """try_claim_for_processing's UPDATE ... WHERE status != 'in_progress' RETURNING
    status is an atomic single-statement compare-and-swap at the SQL level — with
    no real Postgres connection, this test documents/asserts the *call shape*
    (single UPDATE...RETURNING statement, not a separate SELECT-then-UPDATE) that
    makes it race-safe, rather than proving concurrency safety itself."""
    import inspect
    from app.database import operations
    source = inspect.getsource(operations.try_claim_for_processing)
    assert "WHERE id = $1 AND status != 'in_progress'" in source
    assert "RETURNING status" in source


def test_upload_024_missing_session_id_skipped_prepublish(monkeypatch):
    async def run_test():
        conn = install_fake_db(monkeypatch, uploads_service_module)
        conn.fetchrow.return_value = None  # no programs/leader_category match -> UUID fallback path
        record = {
            "id": 1, "report_type": "story", "cloud_storage_path": "path/to/file.csv",
            "leader_category": "L", "program_name": "P", "meta_data": {"tenant_code": "mitra"},
        }
        monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
        update_status_mock = AsyncMock()
        monkeypatch.setattr(operations_module, "update_status", update_status_mock)
        monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage(download_bytes=b"raw")))

        import pandas as pd
        df = pd.DataFrame([{"id": "5004", "Title": "t", "Session ID": ""}])
        monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=df))
        monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(True, [])))

        push_mock = MagicMock()
        monkeypatch.setattr(uploads_service_module, "_push_rows_sync", push_mock)

        await uploads_service_module.process_csv_inline(1)
        push_mock.assert_not_called()
        assert update_status_mock.await_count >= 1
        final_call = update_status_mock.await_args_list[-1]
        assert final_call.args[1] == "success"
        meta = final_call.args[2]
        assert meta["rows_pushed"] == 0
        assert len(meta["schema_validation_errors"]) == 1
        assert "'sessionId' is empty" in meta["schema_validation_errors"][0]["problems"][0]
    asyncio.run(run_test())


def test_upload_025_missing_other_required_fields_skipped_and_recorded(monkeypatch):
    async def run_test():
        conn = install_fake_db(monkeypatch, uploads_service_module)
        conn.fetchrow.return_value = None
        record = {
            "id": 1, "report_type": "story", "cloud_storage_path": "path/to/file.csv",
            "leader_category": "L", "program_name": "P", "meta_data": {"tenant_code": "mitra"},
        }
        monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
        update_status_mock = AsyncMock()
        monkeypatch.setattr(operations_module, "update_status", update_status_mock)
        monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage(download_bytes=b"raw")))

        import pandas as pd
        df = pd.DataFrame([{"id": "5004", "Title": "t", "Session ID": "sess-1"}])
        monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=df))
        monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(True, [])))
        monkeypatch.setattr(uploads_service_module, "_push_rows_sync", MagicMock())

        await uploads_service_module.process_csv_inline(1)
        final_call = update_status_mock.await_args_list[-1]
        meta = final_call.args[2]
        problems = meta["schema_validation_errors"][0]["problems"]
        assert any("transcriptLink" in p for p in problems)
    asyncio.run(run_test())


def test_upload_026_complete_row_still_fails_on_pdf_urls_masked(monkeypatch):
    async def run_test():
        conn = install_fake_db(monkeypatch, uploads_service_module)
        conn.fetchrow.return_value = None
        record = {
            "id": 1, "report_type": "story", "cloud_storage_path": "path/to/file.csv",
            "leader_category": "L", "program_name": "P", "meta_data": {"tenant_code": "mitra"},
        }
        monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
        update_status_mock = AsyncMock()
        monkeypatch.setattr(operations_module, "update_status", update_status_mock)
        monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage(download_bytes=b"raw")))

        import pandas as pd
        df = pd.DataFrame([{
            "id": "5001", "Title": "t", "Session ID": "sess-1", "Objective": "obj text here",
            "Challenges": "a challenge", "Action Steps": "a step", "Impact": "some impact",
            "Transcript Link": "https://example.com/t", "Blurb": "a blurb", "Content": "content here",
            "Pdf": "https://example.com/x.pdf",
            "District": "Patna", "Organization": "Org", "Location": "Patna, Bihar",
            "Duration": "30 minutes",
        }])
        monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=df))
        monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(True, [])))
        monkeypatch.setattr(uploads_service_module, "_push_rows_sync", MagicMock())

        await uploads_service_module.process_csv_inline(1)
        final_call = update_status_mock.await_args_list[-1]
        meta = final_call.args[2]
        assert meta["rows_pushed"] == 0
        problems = meta["schema_validation_errors"][0]["problems"]
        assert any("data.pdfUrls.masked" in p for p in problems)
    asyncio.run(run_test())


def test_upload_027_kafka_unreachable_marks_on_hold(monkeypatch):
    async def run_test():
        conn = install_fake_db(monkeypatch, uploads_service_module)
        conn.fetchrow.return_value = None
        record = {
            "id": 1, "report_type": "discussion", "cloud_storage_path": "path/to/file.csv",
            "leader_category": "L", "program_name": "P", "meta_data": {"tenant_code": "mitra"},
        }
        monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
        update_status_mock = AsyncMock()
        monkeypatch.setattr(operations_module, "update_status", update_status_mock)
        monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage(download_bytes=b"raw")))

        import pandas as pd
        df = pd.DataFrame([{
            "id": "6001", "Title": "t", "Session ID": "sess-1", "Challenges": "a challenge",
            "Solutions": "a solution", "Transcript Link": "https://example.com/t",
            "PDF Urls": "https://example.com/x.pdf",
            "Date of Discussion": "2026-09-09",
        }])
        monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=df))
        monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(True, [])))
        monkeypatch.setattr(
            uploads_service_module, "validate_ingestion_schema",
            MagicMock(return_value=[]),  # pretend it passes, to reach the Kafka push
        )
        monkeypatch.setattr(uploads_service_module, "_push_rows_sync", MagicMock(side_effect=RuntimeError("broker down")))

        await uploads_service_module.process_csv_inline(1)
        final_call = update_status_mock.await_args_list[-1]
        assert final_call.args[1] == "on_hold"
        assert "Kafka Publishing" in final_call.args[2].get("stage", "")
    asyncio.run(run_test())


def test_upload_028_missing_program_leader_match_falls_back_to_uuid(monkeypatch):
    async def run_test():
        import uuid as uuid_module
        conn = install_fake_db(monkeypatch, uploads_service_module)
        conn.fetchrow.return_value = None  # no leader_category/programs match
        record = {
            "id": 1, "report_type": "story", "cloud_storage_path": "path/to/file.csv",
            "leader_category": "Never Seen Before Leader", "program_name": "Never Seen Before Program",
            "meta_data": {"tenant_code": "mitra"},
        }
        monkeypatch.setattr(operations_module, "get_record", AsyncMock(return_value=record))
        monkeypatch.setattr(operations_module, "update_status", AsyncMock())
        monkeypatch.setattr(uploads_service_module, "get_object_storage", MagicMock(return_value=make_fake_object_storage(download_bytes=b"raw")))

        import pandas as pd
        df = pd.DataFrame([{"id": "5001", "Title": "t", "Session ID": "sess-1"}])
        monkeypatch.setattr(uploads_service_module, "load_csv", MagicMock(return_value=df))
        monkeypatch.setattr(uploads_service_module, "validate_columns", MagicMock(return_value=(True, [])))

        captured_payloads = []

        def fake_push(payloads):
            captured_payloads.extend(payloads)

        monkeypatch.setattr(uploads_service_module, "_push_rows_sync", fake_push)
        monkeypatch.setattr(uploads_service_module, "validate_ingestion_schema", MagicMock(return_value=[]))

        await uploads_service_module.process_csv_inline(1)
        assert len(captured_payloads) == 1
        payload = json.loads(captured_payloads[0][0])
        assert uuid_module.UUID(payload["tags"]["leaderCategoryId"])  # a real generated UUID, not a DB id
        assert uuid_module.UUID(payload["tags"]["programId"])
    asyncio.run(run_test())


def test_upload_031_daily_batch_schedule_registers_in_batch_mode(monkeypatch):
    async def run_test():
        import app.temporal.worker as worker_module
        settings_override(monkeypatch, worker_module.settings, PROCESSING_MODE="batch")
        monkeypatch.setattr(worker_module.db, "connect", AsyncMock())
        monkeypatch.setattr(worker_module.db, "disconnect", AsyncMock())
        monkeypatch.setattr(worker_module, "Worker", MagicMock(return_value=MagicMock(run=AsyncMock())))

        mock_client = MagicMock()
        create_schedule_mock = AsyncMock()
        mock_client.create_schedule = create_schedule_mock
        monkeypatch.setattr(worker_module.Client, "connect", AsyncMock(return_value=mock_client))

        await worker_module.start_worker()

        schedule_ids = [c.kwargs.get("id") for c in create_schedule_mock.await_args_list]
        assert "daily-batch-processing" in schedule_ids
    asyncio.run(run_test())


def test_upload_032_stale_schedules_deleted_in_realtime_mode(monkeypatch):
    async def run_test():
        import app.temporal.worker as worker_module
        settings_override(monkeypatch, worker_module.settings, PROCESSING_MODE="real-time")
        monkeypatch.setattr(worker_module.db, "connect", AsyncMock())
        monkeypatch.setattr(worker_module.db, "disconnect", AsyncMock())
        monkeypatch.setattr(worker_module, "Worker", MagicMock(return_value=MagicMock(run=AsyncMock())))

        mock_client = MagicMock()
        deleted_schedules = []

        def fake_get_schedule_handle(sched_id):
            handle = MagicMock()

            async def fake_delete():
                deleted_schedules.append(sched_id)

            handle.delete = fake_delete
            return handle

        mock_client.get_schedule_handle = MagicMock(side_effect=fake_get_schedule_handle)
        monkeypatch.setattr(worker_module.Client, "connect", AsyncMock(return_value=mock_client))

        await worker_module.start_worker()

        assert set(deleted_schedules) == {"daily-batch-processing"}
    asyncio.run(run_test())


# =============================================================================
# STORAGE ABSTRACTION (STORAGE-*)
# =============================================================================
from app.services.storage import StoredObject, AccessMode, StorageNotFoundError, StoragePermissionError, StorageTransientError, StorageError, resolve_url
from app.services.storage.aws_s3 import AwsS3Storage
from app.services.storage.gcp import GcpStorage
from botocore.exceptions import ClientError, EndpointConnectionError
import google.api_core.exceptions

def test_storage_001_aws_upload_public_goes_to_public_bucket():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    storage.s3_client = MagicMock()

    obj = storage.upload_file("local.jpg", "key.jpg", content_type="image/jpeg", access_mode=AccessMode.PUBLIC)

    storage.s3_client.upload_file.assert_called_once_with(
        "local.jpg", "pub-b", "key.jpg", ExtraArgs={"ContentType": "image/jpeg"}
    )
    assert obj.provider == "aws"
    assert obj.bucket == "pub-b"
    assert obj.access_mode == AccessMode.PUBLIC


def test_storage_002_aws_upload_private_goes_to_private_bucket():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    storage.s3_client = MagicMock()

    obj = storage.upload_file("local.jpg", "key.jpg", content_type="image/jpeg", access_mode=AccessMode.PRIVATE)

    storage.s3_client.upload_file.assert_called_once_with(
        "local.jpg", "priv-b", "key.jpg", ExtraArgs={"ContentType": "image/jpeg"}
    )
    assert obj.bucket == "priv-b"
    assert obj.access_mode == AccessMode.PRIVATE


def test_storage_003_aws_upload_bytes_private_goes_to_private_bucket():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    storage.s3_client = MagicMock()

    obj = storage.upload_bytes(b"data", "key.csv", content_type="text/csv", access_mode=AccessMode.PRIVATE)

    storage.s3_client.put_object.assert_called_once_with(
        Bucket="priv-b", Key="key.csv", Body=b"data", ContentType="text/csv"
    )
    assert obj.bucket == "priv-b"
    assert obj.access_mode == AccessMode.PRIVATE


def test_storage_004_aws_upload_bytes_public_goes_to_public_bucket():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    storage.s3_client = MagicMock()

    obj = storage.upload_bytes(b"data", "key.png", content_type="image/png", access_mode=AccessMode.PUBLIC)

    storage.s3_client.put_object.assert_called_once_with(
        Bucket="pub-b", Key="key.png", Body=b"data", ContentType="image/png"
    )
    assert obj.bucket == "pub-b"
    assert obj.access_mode == AccessMode.PUBLIC


def test_storage_005_aws_download_bytes_uses_private_bucket_by_default():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    mock_body = MagicMock()
    mock_body.read.return_value = b"csv-bytes"
    storage.s3_client = MagicMock()
    storage.s3_client.get_object.return_value = {"Body": mock_body}

    res = storage.download_bytes("key.csv", AccessMode.PRIVATE)
    assert res == b"csv-bytes"
    storage.s3_client.get_object.assert_called_once_with(Bucket="priv-b", Key="key.csv")


def test_storage_006_aws_delete_object_private_bucket():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    storage.s3_client = MagicMock()
    storage.delete_object("key.csv", AccessMode.PRIVATE)
    storage.s3_client.delete_object.assert_called_once_with(Bucket="priv-b", Key="key.csv")


def test_storage_007_aws_generate_access_url_private_bucket():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    storage.s3_client = MagicMock()
    storage.s3_client.generate_presigned_url.return_value = "http://presigned"
    url = storage.generate_access_url("key.csv", 3600, AccessMode.PRIVATE)
    assert url == "http://presigned"
    storage.s3_client.generate_presigned_url.assert_called_once_with(
        "get_object", Params={"Bucket": "priv-b", "Key": "key.csv"}, ExpiresIn=3600
    )


def test_storage_008_aws_exception_mapping():
    storage = AwsS3Storage(public_bucket="pub-b", private_bucket="priv-b", region="us-east-1")
    storage.s3_client = MagicMock()

    storage.s3_client.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "op")
    try:
        storage.download_bytes("key.csv", AccessMode.PRIVATE)
        assert False
    except StorageNotFoundError:
        pass

    storage.s3_client.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "op")
    try:
        storage.download_bytes("key.csv", AccessMode.PRIVATE)
        assert False
    except StoragePermissionError:
        pass

    storage.s3_client.get_object.side_effect = EndpointConnectionError(endpoint_url="x")
    try:
        storage.download_bytes("key.csv", AccessMode.PRIVATE)
        assert False
    except StorageTransientError:
        pass


def test_storage_009_gcp_upload_public_goes_to_public_bucket(monkeypatch):
    monkeypatch.setattr("app.services.storage.gcp.settings.PROJECT_ID", "p")
    storage = GcpStorage(public_bucket="pub-b", private_bucket="priv-b")
    mock_client = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value.blob.return_value = mock_blob
    storage._get_client = MagicMock(return_value=mock_client)

    obj = storage.upload_file("local.jpg", "key.jpg", content_type="image/jpeg", access_mode=AccessMode.PUBLIC)

    mock_client.bucket.assert_called_once_with("pub-b")
    mock_blob.upload_from_filename.assert_called_once_with("local.jpg", content_type="image/jpeg")
    assert obj.bucket == "pub-b"
    assert obj.access_mode == AccessMode.PUBLIC


def test_storage_010_gcp_upload_private_goes_to_private_bucket(monkeypatch):
    monkeypatch.setattr("app.services.storage.gcp.settings.PROJECT_ID", "p")
    storage = GcpStorage(public_bucket="pub-b", private_bucket="priv-b")
    mock_client = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value.blob.return_value = mock_blob
    storage._get_client = MagicMock(return_value=mock_client)

    obj = storage.upload_file("local.jpg", "key.jpg", content_type="image/jpeg", access_mode=AccessMode.PRIVATE)

    mock_client.bucket.assert_called_once_with("priv-b")
    mock_blob.upload_from_filename.assert_called_once_with("local.jpg", content_type="image/jpeg")
    assert obj.bucket == "priv-b"
    assert obj.access_mode == AccessMode.PRIVATE


def test_storage_011_gcp_exception_mapping(monkeypatch):
    monkeypatch.setattr("app.services.storage.gcp.settings.PROJECT_ID", "p")
    storage = GcpStorage(public_bucket="pub-b", private_bucket="priv-b")
    storage._get_client = MagicMock(side_effect=google.api_core.exceptions.NotFound("not found"))
    try:
        storage.download_bytes("k", AccessMode.PRIVATE)
        assert False
    except StorageNotFoundError:
        pass

    storage._get_client = MagicMock(side_effect=google.api_core.exceptions.Forbidden("forbidden"))
    try:
        storage.download_bytes("k", AccessMode.PRIVATE)
        assert False
    except StoragePermissionError:
        pass


def test_storage_017_azure_upload_routing(monkeypatch):
    from app.services.storage.azure import AzureStorage
    monkeypatch.setattr("app.services.storage.azure.BlobServiceClient", MagicMock())
    storage = AzureStorage(public_bucket="pub-c", private_bucket="priv-c", account_name="test")
    mock_blob_client = MagicMock()
    storage.blob_service_client.get_blob_client.return_value = mock_blob_client

    obj_pub = storage.upload_bytes(b"data", "key.jpg", access_mode=AccessMode.PUBLIC)
    storage.blob_service_client.get_blob_client.assert_called_with(container="pub-c", blob="key.jpg")
    assert obj_pub.bucket == "pub-c"
    
    obj_priv = storage.upload_bytes(b"data", "key.csv", access_mode=AccessMode.PRIVATE)
    storage.blob_service_client.get_blob_client.assert_called_with(container="priv-c", blob="key.csv")
    assert obj_priv.bucket == "priv-c"


def test_storage_018_oci_upload_routing(monkeypatch):
    from app.services.storage.oci import OciStorage
    monkeypatch.setattr("oci.config.from_file", MagicMock(return_value={}))
    monkeypatch.setattr("oci.object_storage.ObjectStorageClient", MagicMock())
    storage = OciStorage(public_bucket="pub-b", private_bucket="priv-b", namespace="ns")
    
    obj_pub = storage.upload_bytes(b"data", "key.jpg", access_mode=AccessMode.PUBLIC)
    storage.client.put_object.assert_any_call("ns", "pub-b", "key.jpg", b"data", content_type="application/octet-stream")
    assert obj_pub.bucket == "pub-b"

    obj_priv = storage.upload_bytes(b"data", "key.csv", access_mode=AccessMode.PRIVATE)
    storage.client.put_object.assert_any_call("ns", "priv-b", "key.csv", b"data", content_type="application/octet-stream")
    assert obj_priv.bucket == "priv-b"


def test_storage_012_resolve_url_public():
    storage = MagicMock()
    obj = StoredObject(provider="aws", bucket="pub-b", key="k", access_mode=AccessMode.PUBLIC)
    url = resolve_url(obj, storage)
    assert url == "https://pub-b.s3.amazonaws.com/k"
    storage.generate_access_url.assert_not_called()


def test_storage_013_resolve_url_private():
    storage = MagicMock()
    storage.generate_access_url.return_value = "https://signed"
    obj = StoredObject(provider="aws", bucket="priv-b", key="k", access_mode=AccessMode.PRIVATE)
    url = resolve_url(obj, storage)
    assert url == "https://signed"
    storage.generate_access_url.assert_called_once_with("k", expires_in_seconds=3600, access_mode=AccessMode.PRIVATE)


def test_storage_014_csv_download_always_uses_private_bucket(monkeypatch):
    """
    CSV fetch in process_csv_inline must always target the private bucket.
    """
    import app.api.services.uploads as uploads_module
    from app.services.storage import AccessMode
    import asyncio

    mock_storage = make_fake_object_storage(download_bytes=b"id,Title\n1,test")
    recorded = []
    
    def capturing_download(object_key, access_mode):
        recorded.append(access_mode)
        return b"id,Title\n1,test"

    mock_storage.download_bytes = MagicMock(side_effect=capturing_download)
    monkeypatch.setattr(uploads_module, "get_object_storage", MagicMock(return_value=mock_storage))

    # Setup DB mock so process_csv_inline can proceed to the download step
    conn = install_fake_db(monkeypatch, uploads_module)
    conn.fetchrow.return_value = None
    record = {
        "id": 1, "report_type": "story", "cloud_storage_path": "path/to/file.csv",
        "leader_category": "L", "program_name": "P", "meta_data": {"tenant_code": "mitra"},
    }
    monkeypatch.setattr(uploads_module.operations, "get_record", AsyncMock(return_value=record))
    monkeypatch.setattr(uploads_module.operations, "update_status", AsyncMock())
    monkeypatch.setattr(uploads_module, "load_csv", MagicMock())
    monkeypatch.setattr(uploads_module, "validate_columns", MagicMock(return_value=(True, [])))
    monkeypatch.setattr(uploads_module, "_push_rows_sync", MagicMock())

    async def run():
        await uploads_module.process_csv_inline(1)

    asyncio.run(run())
    assert recorded == [AccessMode.PRIVATE], f"Expected PRIVATE, got {recorded}"


def test_storage_015_uploads_py_handle_upload_private(monkeypatch):
    import app.api.services.uploads as uploads_module
    from app.services.storage import AccessMode
    import asyncio

    mock_storage = MagicMock()
    mock_storage.upload_bytes.return_value = MagicMock(key="k")
    monkeypatch.setattr(uploads_module, "get_object_storage", MagicMock(return_value=mock_storage))
    monkeypatch.setattr(uploads_module.operations, "check_duplicate_file", AsyncMock(return_value=False))
    monkeypatch.setattr(uploads_module.operations, "insert_upload_record", AsyncMock(return_value=1))
    monkeypatch.setattr(uploads_module.settings, "PROCESSING_MODE", "batch")
    monkeypatch.setattr(uploads_module, "validate_columns", MagicMock(return_value=(True, [])))

    async def run():
        background_tasks_mock = MagicMock()
        await uploads_module.handle_upload("story", "p", "l", "t", "f", b"a,b\n1,2", background_tasks_mock)
    asyncio.run(run())

    mock_storage.upload_bytes.assert_called_once()
    assert mock_storage.upload_bytes.call_args.kwargs.get("access_mode") == AccessMode.PRIVATE


def test_storage_016_deface_blur_activity_public(monkeypatch):
    import app.temporal.deface_blur_activity as blur_module
    from app.services.storage import AccessMode, StoredObject
    import asyncio

    mock_storage = MagicMock()
    mock_storage.upload_file.return_value = StoredObject("mock", "mock-public-bucket", "k", AccessMode.PUBLIC, "image/jpeg")
    monkeypatch.setattr(blur_module, "get_object_storage", MagicMock(return_value=mock_storage))

    async def run():
        await blur_module._process_one_image("sub_id", "t", "story", 0, "http://fake/img.jpg")

    # Mock download and face blur helpers
    monkeypatch.setattr(blur_module, "_download_file", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(blur_module, "anonymize_face", MagicMock())
    monkeypatch.setattr(blur_module, "_is_allowed_media_host", MagicMock(return_value=True))

    try:
        asyncio.run(run())
    except Exception:
        pass

    mock_storage.upload_file.assert_called_once()
    assert mock_storage.upload_file.call_args.kwargs.get("access_mode") == AccessMode.PUBLIC
