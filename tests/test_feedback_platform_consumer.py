"""
Consumer side of the generic feedback platform: identifiers are validated BEFORE they become config-cache keys (B12),
the event's adapter must be the adapter of the persona's config (A4), a config without a valid adapter is an operator
error, and a source needs no list in code (A2). In-memory fakes only (no Postgres / Kafka).
"""
import asyncio
import copy
import json

import pytest

from app.kafka import consumer as consumer_module
from app.kafka.consumer import FEEDBACK_CONFIG_REPLAY_HINT, _format_identifiers, _log_safe
from seed_support import gforms_config_v1, mitra_config_v1
from tests.test_feedback_consumer import _active_config, _consumer, _feedback_event, _gforms_event, _patch_active_config


def _schema(event, event_type="create", config=None, source=None):
    """(schema, problems, loader calls) of the consumer's schema selection for `event`."""
    consumer = consumer_module.IngestionConsumer()
    calls = []

    async def loader(program_id, src, *, tenant_code=None, session_type=None):
        calls.append((program_id, src, tenant_code, session_type))
        return _active_config(src, config if config is not None else (
            gforms_config_v1("Youth") if src == "google_forms" else mitra_config_v1("Youth")))

    consumer_module.feedback_operations.get_active_feedback_config = loader
    try:
        schema, problems = asyncio.run(consumer._feedback_schema_for_event(event, event_type))
    finally:
        pass
    return schema, problems, calls


@pytest.fixture(autouse=True)
def _restore_loader():
    original = consumer_module.feedback_operations.get_active_feedback_config
    yield
    consumer_module.feedback_operations.get_active_feedback_config = original


@pytest.mark.parametrize("mutate", [
    lambda e: e.update(tenantCode="mitra\nFORGED"),
    lambda e: e.update(tenantCode="t" * 65),
    lambda e: e["data"].update(sessionType="Youth_Voice\r\nFORGED"),
    lambda e: e["data"].update(sessionType="y" * 1_000_000),
    lambda e: e["data"].update(source="Not A Label"),
    lambda e: e["data"].update(source="s" * 41),
    lambda e: e["tags"].update(programId="not-a-uuid"),
    lambda e: e.update(submissionId="1\nFORGED"),
])
def test_unsafe_identifiers_are_rejected_before_the_config_cache_is_touched(mutate):
    event = _feedback_event()
    mutate(event)
    schema, problems, calls = _schema(event)
    assert schema is None and len(problems) > 0
    assert calls == []                                            # no cache lookup, no database query
    assert "FORGED" not in " ".join(problems) and "y" * 50 not in " ".join(problems)


def test_a_huge_session_type_never_becomes_a_cache_key(monkeypatch):
    consumer = consumer_module.IngestionConsumer()

    async def never(*args):
        raise AssertionError("the cache was used for an unvalidated identifier")

    monkeypatch.setattr(consumer.feedback_config_cache, "get", never)
    event = _feedback_event()
    event["data"]["sessionType"] = "y" * 5_000_000
    schema, problems = asyncio.run(consumer._feedback_schema_for_event(event, "create"))
    assert schema is None and problems


def test_a_delete_event_with_unsafe_ids_falls_back_to_the_envelope_check_which_rejects_them():
    delete = {"submissionId": "1\nFORGED", "submissionType": "feedback", "sessionId": "s", "tenantCode": "mitra",
              "eventType": "delete", "eventPublishedAt": "2026-10-15T08:54:43.000Z"}
    schema, problems, calls = _schema(delete, "delete")
    assert schema is None and problems == [] and calls == []
    from app.services.ingestion_validation import validate_ingestion_schema
    (problem,) = validate_ingestion_schema(delete, "feedback", "delete", feedback_schema=None)
    assert "submissionId" in problem and "FORGED" not in problem


def test_a_valid_event_of_any_safe_source_resolves_its_config():
    event = _feedback_event()
    event["data"]["source"] = "ptm_app"
    schema, problems, calls = _schema(event, config=_ptm_like_config())
    assert problems == [] and schema is not None
    assert calls == [(event["tags"]["programId"], "ptm_app", "mitra", "Youth_Voice")]


def _ptm_like_config():
    config = copy.deepcopy(mitra_config_v1("Youth"))
    return config


def test_the_event_adapter_must_equal_the_config_adapter():
    event = _feedback_event()
    event["data"]["adapter"] = "wide_form"                        # the Youth mitra config says conversation_rows
    schema, problems, _ = _schema(event)
    assert schema is None and len(problems) == 1
    assert "does not match the adapter" in problems[0] and "conversation_rows" in problems[0]
    assert FEEDBACK_CONFIG_REPLAY_HINT in problems[0]
    ok = _feedback_event()
    assert _schema(ok)[1] == []


def test_a_config_without_a_valid_adapter_is_an_operator_error():
    for mutate in (lambda c: c.pop("adapter"), lambda c: c.update(adapter="excel")):
        config = mitra_config_v1("Youth")
        mutate(config)
        schema, problems, _ = _schema(_feedback_event(), config=config)
        assert schema is None and len(problems) == 1
        assert problems[0].startswith("Feedback upload config is invalid:") and "adapter" in problems[0]


def test_a_config_with_keys_of_the_other_adapter_is_invalid():
    config = mitra_config_v1("Youth")
    config["forms"] = {}
    schema, problems, _ = _schema(_feedback_event(), config=config)
    assert schema is None and "only adapter wide_form reads" in problems[0]


def test_process_message_dead_letters_an_event_whose_adapter_differs_from_its_config(monkeypatch):
    async def run():
        consumer, _, insert_mock, _, trigger_mock, dlq_mock = _consumer(monkeypatch)
        # the shared helper stubs validate_ingestion_schema out: use the real one here
        monkeypatch.setattr(consumer_module, "validate_ingestion_schema",
                            __import__("app.services.ingestion_validation", fromlist=["x"]).validate_ingestion_schema)
        event = _gforms_event()                                   # a wide_form event ...
        await consumer.process_message(json.dumps(event))         # ... against the seeded google_forms config: fine
        insert_mock.assert_awaited_once()
        insert_mock.reset_mock()
        event["data"]["adapter"] = "conversation_rows"            # ... but claiming the other adapter
        await consumer.process_message(json.dumps(event))
        insert_mock.assert_not_awaited()
        dlq_mock.assert_awaited_once()
        assert "does not match the adapter" in dlq_mock.await_args.args[1]
    asyncio.run(run())


def test_log_lines_cannot_be_forged_by_identifiers():
    assert _log_safe("1\nFORGED [ERROR]") == "1?FORGED [ERROR]"
    assert "\r" not in _log_safe("a\r\nb") and len(_log_safe("x" * 5000)) <= 131
    line = _format_identifiers({"submissionId": "1\nFORGED", "tenantCode": "mitra", "sessionId": None})
    assert "\n" not in line and line == "submissionId=1?FORGED, tenantCode=mitra"


# ---- InvalidCacheKeyError gets its own DLQ reason (names the field, never the value) ----

def test_an_invalid_cache_key_is_dead_lettered_with_the_failed_field_named_and_no_value(monkeypatch):
    from app.services.feedback_config_cache import InvalidCacheKeyError
    consumer = consumer_module.IngestionConsumer()
    secret = "SECRET-VALUE-12345"

    async def refuse(*args):
        raise InvalidCacheKeyError("session_type is not a valid label (letters, digits and a few separators, "
                                   "at most 64 characters; got 18 characters)")

    monkeypatch.setattr(consumer.feedback_config_cache, "get", refuse)
    event = _feedback_event()
    event["data"]["sessionType"] = secret                         # passes the event-level check; the cache refuses
    schema, problems = asyncio.run(consumer._feedback_schema_for_event(event, "create"))
    assert schema is None and len(problems) == 1
    assert problems[0].startswith("Invalid feedback config key") and "session_type" in problems[0]
    assert secret not in problems[0] and "Feedback upload config is invalid" not in problems[0]
    # a delete event falls back to the envelope check, as for every other unresolvable config
    delete = _feedback_event("delete")
    assert asyncio.run(consumer._feedback_schema_for_event(delete, "delete")) == (None, [])


def test_process_message_sends_the_invalid_cache_key_reason_to_the_dlq(monkeypatch):
    from app.services.feedback_config_cache import InvalidCacheKeyError

    async def run():
        consumer, _, insert_mock, _, _, dlq_mock = _consumer(monkeypatch)

        async def refuse(*args):
            raise InvalidCacheKeyError("tenant_code is not a valid label (got 7 characters)")

        monkeypatch.setattr(consumer.feedback_config_cache, "get", refuse)
        monkeypatch.setattr(consumer_module, "validate_ingestion_schema",
                            __import__("app.services.ingestion_validation", fromlist=["x"]).validate_ingestion_schema)
        await consumer.process_message(json.dumps(_feedback_event()))
        insert_mock.assert_not_awaited()
        reason = dlq_mock.await_args.args[1]
        assert "Invalid feedback config key" in reason and "tenant_code" in reason
    asyncio.run(run())
