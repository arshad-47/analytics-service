"""
Shared test fakes/fixtures for tests/unit_testing.py.

Everything here is a pure in-memory fake — no test in this suite ever
connects to a real Postgres, Kafka, Temporal, or GCS endpoint. The
conventions established in the (now-absorbed) test_mode_logic.py /
test_kafka_events.py are kept: plain unittest.mock (MagicMock/AsyncMock)
plus pytest's built-in monkeypatch fixture, async test bodies run via
asyncio.run() rather than pytest-asyncio.
"""
import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Fake asyncpg pool/connection — used for every db.pool.acquire() call site.
# ---------------------------------------------------------------------------

class FakeConn:
    """
    Stands in for an asyncpg.Connection. All query methods are AsyncMocks
    with sane empty-result defaults; tests override .return_value/.side_effect
    per case. Also usable as its own async context manager for conn.transaction().
    """

    def __init__(self):
        self.fetchrow = AsyncMock(return_value=None)
        self.fetchval = AsyncMock(return_value=None)
        self.fetch = AsyncMock(return_value=[])
        self.execute = AsyncMock(return_value="")
        self.transaction = MagicMock(return_value=self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    """Stands in for db.pool — .acquire() returns a FakeConn as an async context manager."""

    def __init__(self, conn: Optional[FakeConn] = None):
        self.conn = conn or FakeConn()

    def acquire(self):
        return self.conn


def install_fake_db(monkeypatch, module, conn: Optional[FakeConn] = None) -> FakeConn:
    """
    Patches `module.db.pool` with a FakePool wrapping `conn` (or a fresh FakeConn).
    `module` is whichever module-under-test imported `db` (e.g. app.kafka.consumer,
    app.temporal.pii_and_abusive_activity). Returns the FakeConn for assertions.
    """
    fake_conn = conn or FakeConn()
    monkeypatch.setattr(module.db, "pool", FakePool(fake_conn), raising=False)
    return fake_conn


# ---------------------------------------------------------------------------
# Fake confluent_kafka Producer / AdminClient
# ---------------------------------------------------------------------------

def make_fake_kafka_producer(delivery_error=None, flush_remaining: int = 0):
    """
    A MagicMock standing in for confluent_kafka.Producer. `.produce()` invokes
    the given callback immediately with `delivery_error` (None = success),
    matching the real client's eventual-callback behavior closely enough for
    the code under test (which only inspects the callback's error arg after
    flush()). `.flush()` returns `flush_remaining` (0 = everything delivered).
    """
    producer = MagicMock()

    def _produce(topic, value=None, key=None, headers=None, callback=None):
        if callback:
            callback(delivery_error, None)

    producer.produce = MagicMock(side_effect=_produce)
    producer.poll = MagicMock(return_value=0)
    producer.flush = MagicMock(return_value=flush_remaining)
    return producer


def make_fake_admin_client(topics_exist: bool = True):
    """
    A MagicMock standing in for confluent_kafka.admin.AdminClient. `.create_topics()`
    returns a dict of topic name -> a future-like MagicMock whose `.result()` either
    succeeds (topics_exist=False, i.e. freshly created) or raises a
    "already exists"-style exception (topics_exist=True), matching how
    consumer.py's `_ensure_topics_exist` treats both as non-fatal.
    """
    def _create_topics(new_topics):
        futures = {}
        for nt in new_topics:
            future = MagicMock()
            if topics_exist:
                future.result = MagicMock(side_effect=Exception("Topic already exists."))
            else:
                future.result = MagicMock(return_value=None)
            futures[nt.topic] = future
        return futures

    client = MagicMock()
    client.create_topics = MagicMock(side_effect=_create_topics)
    return client


# ---------------------------------------------------------------------------
# Fake ObjectStorage / boto3 / GCS clients
# ---------------------------------------------------------------------------
from app.services.storage import StoredObject, AccessMode

def make_fake_object_storage(download_bytes: bytes = b""):
    """
    A MagicMock standing in for ObjectStorage protocol.

    Routing matches real adapters: PUBLIC → mock-public-bucket,
    PRIVATE → mock-private-bucket.
    """
    storage = MagicMock()

    def _bucket(access_mode: AccessMode) -> str:
        return "mock-public-bucket" if access_mode == AccessMode.PUBLIC else "mock-private-bucket"

    def fake_upload_file(local_file_path, object_key, content_type=None, access_mode=AccessMode.PRIVATE):
        return StoredObject(
            provider=    "mock",
            bucket=      _bucket(access_mode),
            key=         object_key,
            access_mode= access_mode,
            content_type=content_type,
        )

    def fake_upload_bytes(data, object_key, content_type=None, access_mode=AccessMode.PRIVATE):
        return StoredObject(
            provider=    "mock",
            bucket=      _bucket(access_mode),
            key=         object_key,
            access_mode= access_mode,
            content_type=content_type,
        )

    storage.upload_file   = MagicMock(side_effect=fake_upload_file)
    storage.upload_bytes  = MagicMock(side_effect=fake_upload_bytes)
    storage.download_bytes = MagicMock(return_value=download_bytes)
    storage.delete_object  = MagicMock()
    storage.generate_access_url = MagicMock(return_value="https://mock-signed-url")
    return storage

def make_fake_gcs_client(download_bytes: bytes = b""):
    """
    A MagicMock standing in for google.cloud.storage.Client. Returns
    (client, blob) so tests can assert on blob.upload_from_string /
    upload_from_filename / download_as_bytes calls directly.
    """
    blob = MagicMock()
    blob.download_as_bytes = MagicMock(return_value=download_bytes)
    bucket = MagicMock()
    bucket.blob = MagicMock(return_value=blob)
    client = MagicMock()
    client.bucket = MagicMock(return_value=bucket)
    return client, blob


# ---------------------------------------------------------------------------
# Fake LLM (OpenRouter) HTTP responses — patches urllib.request.urlopen
# ---------------------------------------------------------------------------

class _FakeHTTPResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def fake_llm_response(content: str, usage: Optional[Dict[str, Any]] = None):
    """
    Builds a fake urllib.request.urlopen(...) context manager returning an
    OpenRouter-shaped chat-completion JSON body, for patching
    app.services.llm.urllib.request.urlopen. `content` can itself be malformed
    JSON — that's the point for PII-003..006 / RATING-004/005 / THEME-011..015
    / SEC-003/004 style "staged bad LLM response" cases.
    """
    body = {
        "choices": [{"message": {"content": content}}],
        "usage": usage if usage is not None else {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }
    return _FakeHTTPResponse(json.dumps(body).encode("utf-8"))


def install_fake_llm(monkeypatch, content: str, usage: Optional[Dict[str, Any]] = None):
    """Patches app.services.llm.urllib.request.urlopen to return `content`/`usage`."""
    import app.services.llm as llm_module
    response = fake_llm_response(content, usage)
    monkeypatch.setattr(llm_module.urllib.request, "urlopen", MagicMock(return_value=response))


def install_failing_llm(monkeypatch, exc: Exception):
    """Patches urlopen to raise `exc` (simulating a network/HTTP failure)."""
    import app.services.llm as llm_module
    monkeypatch.setattr(llm_module.urllib.request, "urlopen", MagicMock(side_effect=exc))


# ---------------------------------------------------------------------------
# Temporal workflow-context patching — lets CsvProcessingWorkflow.run() /
# CsvBatchProcessingWorkflow.run() / BatchProcessingWorkflow.run() /
# ConfigDrivenProcessingWorkflow.run() be called as plain coroutines with no
# real (or ephemeral) Temporal server at all.
# ---------------------------------------------------------------------------

def install_fake_workflow_context(monkeypatch, activity_results: Optional[Dict[Any, Any]] = None,
                                   child_workflow_results: Optional[Dict[Any, Any]] = None):
    """
    Patches app.temporal.workflows.workflow.execute_activity/execute_child_workflow/
    now/continue_as_new/logger so workflow .run() methods can be invoked directly.

    `activity_results`: maps an activity function object -> the value
    execute_activity should return for calls to that activity (or raise, if the
    mapped value is an Exception instance).
    `child_workflow_results`: same idea, keyed by child workflow .run method.

    Returns a MagicMock recording every execute_activity call (`.call_args_list`)
    for call-count/ordering assertions, and a `continue_as_new_calls` list capturing
    any continue_as_new(...) invocations (since it doesn't actually restart anything
    here — the workflow body's `return`/loop-exit right after it, matching how the
    real SDK never returns control past a continue_as_new call, is emulated by
    raising a sentinel _ContinueAsNew exception the test can catch).
    """
    import app.temporal.workflows as workflows_module

    activity_results = activity_results or {}
    child_workflow_results = child_workflow_results or {}
    continue_as_new_calls = []

    async def fake_execute_activity(activity_fn, *args, **kwargs):
        if activity_fn in activity_results:
            result = activity_results[activity_fn]
            if isinstance(result, Exception):
                raise result
            return result
        return None

    async def fake_execute_child_workflow(run_fn, *args, **kwargs):
        if run_fn in child_workflow_results:
            result = child_workflow_results[run_fn]
            if isinstance(result, Exception):
                raise result
            return result
        return None

    class _ContinueAsNew(Exception):
        pass

    def fake_continue_as_new(args=None, **kwargs):
        continue_as_new_calls.append(args)
        raise _ContinueAsNew()

    execute_activity_mock = AsyncMock(side_effect=fake_execute_activity)
    monkeypatch.setattr(workflows_module.workflow, "execute_activity", execute_activity_mock)
    monkeypatch.setattr(workflows_module.workflow, "execute_child_workflow", fake_execute_child_workflow)
    monkeypatch.setattr(workflows_module.workflow, "now", MagicMock(return_value=__import__("datetime").datetime(2026, 1, 1)))
    monkeypatch.setattr(workflows_module.workflow, "continue_as_new", fake_continue_as_new)
    monkeypatch.setattr(workflows_module.workflow, "logger", MagicMock())

    return execute_activity_mock, continue_as_new_calls, _ContinueAsNew


# ---------------------------------------------------------------------------
# Settings override helper
# ---------------------------------------------------------------------------

def settings_override(monkeypatch, settings_obj, **overrides):
    """Convenience wrapper for repeated monkeypatch.setattr(settings, k, v) calls."""
    for key, value in overrides.items():
        monkeypatch.setattr(settings_obj, key, value)


# ---------------------------------------------------------------------------
# TEST_CASES.csv sync — after a run of tests/unit_testing.py, reflect each
# test's pass/fail outcome back onto its Test ID row's Status column and
# print a one-line summary. Test function names follow the convention
# test_<module>_<3-digit-id>_<description> (e.g. test_kafka_020_...  ->
# KAFKA-020). Four IDs are permanently excluded from this sync and keep
# whatever Status they already have in the sheet: KAFKA-021/BATCH-006 have
# only a narrower automated approximation (not a full validation of the
# documented behavior), and THEME-018/RATING-009 have no automated coverage
# at all (they need a live, concurrently-loaded system to observe) — all
# four are already Verified from manual QA. See TEST_CASES.csv's notes.
# ---------------------------------------------------------------------------

_TEST_ID_RE = re.compile(r"^test_(kafka|sec|config|db|theme|pii|rating|batch|mode|llm|upload)_(\d{3})")
_STATUS_SYNC_EXCLUDE = {"KAFKA-021", "BATCH-006", "THEME-018", "RATING-009"}
_CSV_PATH = Path(__file__).parent / "TEST_CASES.csv"

_test_outcomes: Dict[str, str] = {}  # Test ID -> "passed" | "failed" | "skipped"


def pytest_runtest_logreport(report):
    if report.when == "call":
        outcome = report.outcome
    elif report.when == "setup" and report.outcome in ("failed", "skipped"):
        outcome = report.outcome
    else:
        return

    func_name = report.nodeid.split("::")[-1].split("[")[0]  # strip parametrize suffix
    match = _TEST_ID_RE.match(func_name)
    if not match:
        return
    module, number = match.groups()
    test_id = f"{module.upper()}-{number}"

    # A Test ID can be covered by more than one test function (e.g.
    # MODE-003 / MODE-003b); any failure among them marks the ID failing.
    if outcome == "failed" or _test_outcomes.get(test_id) == "failed":
        _test_outcomes[test_id] = "failed"
    elif test_id not in _test_outcomes:
        _test_outcomes[test_id] = outcome


def pytest_sessionfinish(session, exitstatus):
    if not _test_outcomes or not _CSV_PATH.exists():
        return

    with open(_CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    header = rows[0]
    id_idx = header.index("Test ID")
    status_idx = header.index("Status")

    verified = failing = skipped = not_run = excluded = 0
    for row in rows[1:]:
        test_id = row[id_idx]
        if test_id in _STATUS_SYNC_EXCLUDE:
            excluded += 1
            continue
        outcome = _test_outcomes.get(test_id)
        if outcome == "passed":
            row[status_idx] = "Verified"
            verified += 1
        elif outcome == "failed":
            row[status_idx] = "Failing"
            failing += 1
        elif outcome == "skipped":
            skipped += 1
        else:
            not_run += 1  # no test function ran for this ID this session — status left untouched

    with open(_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        writer.writerows(rows)

    summary = (
        f"TEST_CASES.csv sync — {verified} verified, {failing} failing, "
        f"{skipped} skipped, {not_run} not run this session, {excluded} excluded"
    )
    terminal = session.config.pluginmanager.get_plugin("terminalreporter")
    if terminal:
        terminal.write_line("")
        terminal.write_line(summary)
    else:
        print(summary)
