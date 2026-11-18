"""
Unit tests (no database, no Temporal, no LLM) of the two halves of "a relabelled bot session is reprocessed only when
that changes something":

  * relabel_needs_reextraction (app/database/feedback_bot_versions.py): the pure decision. Only the NAMES of the fields
    the old and the new questionnaire label ask are compared (ExtractionConfig.fields_for); no config = reprocess.
  * the consumer's dispatch of the sessions an ingest SET BACK TO 'pending' (IngestionConsumer._dispatch_reset_sessions):
    real-time mode starts them (capped, only those still 'pending', never one that already has a running workflow),
    batch mode starts none.

The real-Postgres behaviour (which sessions the ingest and the restamp tool reset) is in tests/test_feedback_relabel_pg.py.
Synthetic data only.
"""
import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.exceptions import WorkflowAlreadyStartedError

import app.kafka.consumer as consumer_module
from app.database.feedback_bot_versions import relabel_needs_reextraction
from app.services.feedback_extraction import parse_extraction_config
from conftest import install_fake_db
from seed_support import PERSONAS, extraction_config_v1


def _config(fields):
    """An ExtractionConfig parsed from the seeded Youth one with its `fields` replaced (by field dicts)."""
    raw = json.loads(json.dumps(extraction_config_v1("Youth")))
    raw["fields"] = fields
    raw.pop("copy_to_submission", None)      # it names the seeded state / district fields, which are replaced here
    return parse_extraction_config(raw)


def _field(name, versions=None, **extra):
    field = {"name": name, "kind": "free_text", "description": f"the {name}"}
    if versions is not None:
        field["versions"] = versions
    field.update(extra)
    return field


# ------------------------------------------------------------------------------------------ the decision

def test_equal_field_sets_need_no_reextraction():
    config = _config([_field("a"), _field("b", ["V1", "V2"])])
    assert relabel_needs_reextraction(config, "V1", "V2") is False
    assert relabel_needs_reextraction(config, "V2", "V1") is False


def test_a_field_asked_in_one_version_only_needs_reextraction_both_ways():
    config = _config([_field("a"), _field("only_v2", ["V2"])])
    assert relabel_needs_reextraction(config, "V1", "V2") is True
    assert relabel_needs_reextraction(config, "V2", "V1") is True


def test_swapped_fields_of_equal_count_still_differ():
    # same NUMBER of fields, different names: a size comparison would call this equal
    config = _config([_field("annual", ["V1"]), _field("monthly", ["V2"])])
    assert relabel_needs_reextraction(config, "V1", "V2") is True


def test_a_field_that_only_a_later_version_asks_does_not_matter_between_two_other_versions():
    config = _config([_field("a"), _field("only_v3", ["V3"])])
    assert relabel_needs_reextraction(config, "V1", "V2") is False
    assert relabel_needs_reextraction(config, "V2", "V3") is True


def test_applies_when_chains_are_not_considered_only_fields_for_names():
    # `child` depends on `parent`'s extracted value; that dependency is about values, not about which fields exist
    fields = [_field("parent", ["V1", "V2"], kind="category", categories=["x", "y"]),
              _field("child", ["V1", "V2"], applies_when={"field": "parent", "in": ["x"]})]
    config = _config(fields)
    assert relabel_needs_reextraction(config, "V1", "V2") is False


def test_no_config_is_the_safe_side():
    assert relabel_needs_reextraction(None, "V1", "V2") is True


def test_the_same_label_never_needs_it_with_a_config_and_is_still_safe_without():
    config = _config([_field("a"), _field("only_v2", ["V2"])])
    assert relabel_needs_reextraction(config, "V2", "V2") is False
    assert relabel_needs_reextraction(None, "V2", "V2") is True


def test_the_seeded_personas_decide_as_documented():
    youth = parse_extraction_config(extraction_config_v1("Youth"))
    general = parse_extraction_config(extraction_config_v1("General Public"))
    assert relabel_needs_reextraction(youth, "V1", "V2") is True       # annual (V1) <-> monthly (V2) household income
    assert relabel_needs_reextraction(general, "V1", "V2") is False    # no version-dependent field
    assert "General Public" in PERSONAS


# ------------------------------------------------------------------------------------------ the dispatch

def _consumer(monkeypatch, pending, mode="real-time"):
    """A consumer whose database says that `pending` of the asked ids are still 'pending'; _trigger is a mock."""
    consumer = consumer_module.IngestionConsumer()
    conn = install_fake_db(monkeypatch, consumer_module)
    conn.fetch = AsyncMock(return_value=[{"submission_id": sid} for sid in pending])
    trigger = AsyncMock(return_value="started")
    monkeypatch.setattr(consumer, "_trigger_realtime_workflow", trigger)
    monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", mode)
    return consumer, conn, trigger


def test_real_time_dispatches_every_reset_session_once(monkeypatch):
    async def run():
        consumer, conn, trigger = _consumer(monkeypatch, ["s1", "s2", "s3"])
        await consumer._dispatch_reset_sessions(["s1", "s2", "s3"], "mitra", "feedback")
        assert [c.args for c in trigger.await_args_list] == [("s1", "mitra", "feedback"), ("s2", "mitra", "feedback"),
                                                              ("s3", "mitra", "feedback")]
        sql, tenant, ids = conn.fetch.await_args.args
        assert "status = 'pending'" in sql and tenant == "mitra" and ids == ["s1", "s2", "s3"]
    asyncio.run(run())


def test_only_sessions_that_are_still_pending_are_started(monkeypatch):
    async def run():
        consumer, _, trigger = _consumer(monkeypatch, ["s2"])          # s1 moved on (running / finished) meanwhile
        await consumer._dispatch_reset_sessions(["s1", "s2"], "mitra", "feedback")
        assert [c.args[0] for c in trigger.await_args_list] == ["s2"]
    asyncio.run(run())


def test_batch_mode_starts_nothing_and_does_not_even_query(monkeypatch):
    async def run():
        consumer, conn, trigger = _consumer(monkeypatch, ["s1"], mode="batch")
        await consumer._dispatch_reset_sessions(["s1"], "mitra", "feedback")
        trigger.assert_not_awaited()
        conn.fetch.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize("nothing", [None, [], "not-a-list"])
def test_nothing_to_dispatch_is_a_no_op(monkeypatch, nothing):
    async def run():
        consumer, conn, trigger = _consumer(monkeypatch, ["s1"])
        await consumer._dispatch_reset_sessions(nothing, "mitra", "feedback")
        trigger.assert_not_awaited()
        conn.fetch.assert_not_awaited()
    asyncio.run(run())


def test_the_number_started_per_ingest_is_capped_and_the_rest_stay_pending_with_a_warning(monkeypatch, caplog):
    async def run():
        monkeypatch.setattr(consumer_module, "MAX_RELABEL_DISPATCH_PER_INGEST", 3)
        ids = [f"s{n}" for n in range(1, 8)]
        consumer, _, trigger = _consumer(monkeypatch, ids)
        with caplog.at_level(logging.WARNING, logger="analytics_service.kafka.consumer"):
            await consumer._dispatch_reset_sessions(ids, "mitra", "feedback")
        assert [c.args[0] for c in trigger.await_args_list] == ["s1", "s2", "s3"]
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("4 relabelled session(s) over the per-ingest limit of 3" in m for m in warnings)
    asyncio.run(run())


def test_the_default_cap_is_one_hundred():
    assert consumer_module.MAX_RELABEL_DISPATCH_PER_INGEST == 100


def test_a_failure_on_one_session_does_not_stop_the_others_and_logs_no_text(monkeypatch, caplog):
    async def run():
        consumer, _, trigger = _consumer(monkeypatch, ["s1", "s2"])
        trigger.side_effect = [RuntimeError("respondent said something private"), "started"]
        with caplog.at_level(logging.INFO, logger="analytics_service.kafka.consumer"):
            await consumer._dispatch_reset_sessions(["s1", "s2"], "mitra", "feedback")
        assert trigger.await_count == 2
        assert "private" not in caplog.text and "RuntimeError" in caplog.text
    asyncio.run(run())


def test_a_session_with_a_running_workflow_is_not_started_twice_and_its_row_is_left_alone(monkeypatch):
    """The real _trigger_realtime_workflow: Temporal refuses a second RUNNING workflow with the same id."""
    async def run():
        consumer = consumer_module.IngestionConsumer()
        conn = install_fake_db(monkeypatch, consumer_module)
        conn.fetch = AsyncMock(return_value=[{"submission_id": "s1"}])
        consumer.temporal_client = MagicMock()
        consumer.temporal_client.start_workflow = AsyncMock(
            side_effect=WorkflowAlreadyStartedError("realtime-s1-mitra", "ConfigDrivenProcessingWorkflow"))
        monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", "real-time")
        monkeypatch.setattr(consumer_module.feedback_operations, "get_feedback_process_steps",
                            AsyncMock(return_value=[{"name": "pii_and_abusive"}]))
        assert await consumer._trigger_realtime_workflow("s1", "mitra", "feedback") == "already_running"
        await consumer._dispatch_reset_sessions(["s1"], "mitra", "feedback")
        assert consumer.temporal_client.start_workflow.await_count == 2      # both attempts were refused by Temporal
        conn.execute.assert_not_awaited()                                    # never marked 'processing' by the loser
        assert consumer.temporal_client is not None                          # not treated as a connection problem
    asyncio.run(run())


def test_the_workflow_id_is_the_claim_a_finished_session_set_back_to_pending_is_started_again(monkeypatch):
    async def run():
        consumer = consumer_module.IngestionConsumer()
        conn = install_fake_db(monkeypatch, consumer_module)
        consumer.temporal_client = MagicMock()
        consumer.temporal_client.start_workflow = AsyncMock()
        monkeypatch.setattr(consumer_module.feedback_operations, "get_feedback_process_steps",
                            AsyncMock(return_value=[{"name": "pii_and_abusive"}]))
        assert await consumer._trigger_realtime_workflow("s1", "mitra", "feedback") == "started"
        assert consumer.temporal_client.start_workflow.await_args.kwargs["id"] == "realtime-s1-mitra"
        sql = conn.execute.await_args.args[0]
        assert "SET status = 'processing'" in sql and "status = 'pending'" in sql
    asyncio.run(run())


# ------------------------------------------------------------------------------------------ process_message wiring

def _wired_consumer(monkeypatch, insert_result, mode="real-time"):
    consumer = consumer_module.IngestionConsumer()
    install_fake_db(monkeypatch, consumer_module)
    monkeypatch.setattr(consumer_module, "insert_or_update_submission", AsyncMock(return_value=insert_result))
    monkeypatch.setattr(consumer_module, "validate_ingestion_schema", MagicMock(return_value=[]))
    monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", mode)

    async def no_schema(event, event_type):
        return {}, []
    monkeypatch.setattr(consumer, "_feedback_schema_for_event", no_schema)
    trigger = AsyncMock(return_value="started")
    dispatch = AsyncMock()
    monkeypatch.setattr(consumer, "_trigger_realtime_workflow", trigger)
    monkeypatch.setattr(consumer, "_dispatch_reset_sessions", dispatch)
    return consumer, trigger, dispatch


def _event():
    return {"submissionId": "new-1", "submissionType": "feedback", "sessionId": "x" * 32, "tenantCode": "mitra",
            "eventType": "create", "tags": {}, "data": {}}


def test_process_message_dispatches_the_reset_sessions_after_the_ingested_one(monkeypatch):
    async def run():
        consumer, trigger, dispatch = _wired_consumer(monkeypatch, {
            "submission_id": "new-1", "status": "pending", "changed": True,
            "restamped_submission_ids": ["a", "b"], "reset_submission_ids": ["b"]})
        await consumer.process_message(json.dumps(_event()))
        trigger.assert_awaited_once_with("new-1", "mitra", "feedback")
        dispatch.assert_awaited_once_with(["b"], "mitra", "feedback")        # only the one that was reset
    asyncio.run(run())


def test_process_message_also_dispatches_when_only_other_sessions_changed(monkeypatch):
    async def run():
        consumer, trigger, dispatch = _wired_consumer(monkeypatch, {
            "submission_id": "new-1", "status": "success", "changed": False,
            "restamped_submission_ids": ["a"], "reset_submission_ids": ["a"]})
        await consumer.process_message(json.dumps(_event()))
        trigger.assert_not_awaited()                                          # the ingested session is unchanged
        dispatch.assert_awaited_once_with(["a"], "mitra", "feedback")
    asyncio.run(run())


def test_process_message_in_batch_mode_starts_no_workflow_for_anyone(monkeypatch):
    async def run():
        consumer = consumer_module.IngestionConsumer()
        conn = install_fake_db(monkeypatch, consumer_module)
        conn.fetch = AsyncMock(return_value=[{"submission_id": "b"}])
        monkeypatch.setattr(consumer_module, "insert_or_update_submission", AsyncMock(return_value={
            "submission_id": "new-1", "status": "pending", "changed": True,
            "restamped_submission_ids": ["b"], "reset_submission_ids": ["b"]}))
        monkeypatch.setattr(consumer_module, "validate_ingestion_schema", MagicMock(return_value=[]))
        monkeypatch.setattr(consumer_module.settings, "PROCESSING_MODE", "batch")

        async def no_schema(event, event_type):
            return {}, []
        monkeypatch.setattr(consumer, "_feedback_schema_for_event", no_schema)
        trigger = AsyncMock()
        monkeypatch.setattr(consumer, "_trigger_realtime_workflow", trigger)
        await consumer.process_message(json.dumps(_event()))
        trigger.assert_not_awaited()
        conn.fetch.assert_not_awaited()
    asyncio.run(run())
