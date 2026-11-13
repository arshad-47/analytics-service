import asyncio
import json
import logging
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

from app.temporal.migration_activity import run_migration_activity
from app.temporal.workflows import MigrationWorkflow
from app.config import settings
import sys

# Import SubmittedIdTracker from migrate-scripts/migrate.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "migrate-scripts"))
from migrate import SubmittedIdTracker


def test_migration_activity_success():
    """Verify run_migration_activity invokes migrate.py and returns success."""
    async def _run():
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"=== Run End Result: success ===", b""))

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc)) as mock_exec:
            result = await run_migration_activity({"dry_run": True})

            assert result["status"] == "success"
            assert result["returncode"] == 0
            assert result["dry_run"] is True
            assert "success" in result["output_summary"]

            # Ensure --dry-run was passed
            mock_exec.assert_called_once()
            called_args = mock_exec.call_args[0]
            assert "--dry-run" in called_args

    asyncio.run(_run())


def test_migration_activity_failure_raises():
    """Verify run_migration_activity raises RuntimeError when script returns non-zero."""
    async def _run():
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.communicate = AsyncMock(return_value=(b"", b"Database connection error"))

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc)):
            try:
                await run_migration_activity({})
                assert False, "Should have raised RuntimeError"
            except RuntimeError as exc:
                assert "Database connection error" in str(exc)

    asyncio.run(_run())


def test_migration_activity_terminates_subprocess_on_timeout():
    """Verify run_migration_activity terminates and kills child process on timeout/cancellation."""
    async def _run():
        mock_proc = MagicMock()
        mock_proc.pid = 99999
        mock_proc.terminate = MagicMock()
        mock_proc.kill = MagicMock()
        mock_proc.wait = AsyncMock(return_value=0)

        # communicate raises TimeoutError
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError("timed out"))

        with patch("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc)):
            try:
                await run_migration_activity({"timeout_seconds": 0.01})
                assert False, "Should have raised RuntimeError on timeout"
            except RuntimeError as exc:
                assert "cancelled or timed out" in str(exc)

            mock_proc.terminate.assert_called_once()
            mock_proc.wait.assert_called()

    asyncio.run(_run())


def test_submitted_id_tracker_lifecycle(tmp_path: Path):
    """Verify SubmittedIdTracker records sent IDs, holds in-flight IDs, and prunes confirmed IDs."""
    cache_file = tmp_path / "sent_cache.json"
    tracker = SubmittedIdTracker(cache_file, ttl_hours=1.0)
    log = logging.getLogger("test")

    # 1. Record sent IDs: sub1, sub2, sub3
    tracker.record_sent_ids(["sub1", "sub2", "sub3"], log)
    assert cache_file.exists()

    # 2. Before consumer runs: confirmed DB IDs are empty.
    # All 3 should be in unconfirmed set (held in-flight)
    unconfirmed = tracker.load_unconfirmed_ids(set(), log)
    assert unconfirmed == {"sub1", "sub2", "sub3"}

    # 3. Consumer runs and persists sub1 and sub2 into DB.
    # sub1 and sub2 should be pruned; only sub3 remains in-flight.
    unconfirmed_after = tracker.load_unconfirmed_ids({"sub1", "sub2"}, log)
    assert unconfirmed_after == {"sub3"}

    # 4. Check TTL expiry: manually set sub3's timestamp to 2 hours ago
    cached_data = json.loads(cache_file.read_text())
    cached_data["sub3"] = time.time() - 7200  # 2 hours ago (> 1 hour TTL)
    cache_file.write_text(json.dumps(cached_data))

    # sub3 has expired; should not be held in-flight anymore, allowing retry
    unconfirmed_expired = tracker.load_unconfirmed_ids(set(), log)
    assert unconfirmed_expired == set()


def test_migration_config_defaults():
    """Verify migration cron configuration default values on the Settings model."""
    from app.config import Settings
    assert Settings.model_fields["ENABLE_MIGRATION_CRON"].default is False
    assert Settings.model_fields["MIGRATION_CRON_SCHEDULE"].default == "0 */2 * * *"


def test_check_pending_migration_uploads_fails_fast():
    """Verify check_pending_migration_uploads raises RuntimeError when DATABASE_URL is missing."""
    from migrate import check_pending_migration_uploads, Config
    cfg = MagicMock(spec=Config)
    cfg.analytics_db_url = ""
    log = logging.getLogger("test")

    import pytest
    with pytest.raises(RuntimeError) as exc_info:
        check_pending_migration_uploads(cfg, log)
    assert "No DATABASE_URL configured" in str(exc_info.value)


def test_post_csv_to_upload_api_sends_target_tenant():
    """Verify post_csv_to_upload_api sends the configured target tenant in the multipart payload."""
    from migrate import post_csv_to_upload_api, Config
    cfg = MagicMock(spec=Config)
    cfg.api_base_url = "http://localhost:8000"
    cfg.auth_token = "test-token"
    log = logging.getLogger("test")

    captured_requests = []

    mock_resp = MagicMock()
    mock_resp.read.return_value = b'{"status": "success"}'
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.__exit__.return_value = None

    def mock_urlopen(req, *args, **kwargs):
        captured_requests.append(req)
        return mock_resp

    with patch("urllib.request.urlopen", side_effect=mock_urlopen):
        ok = post_csv_to_upload_api(
            cfg=cfg,
            report_type="story",
            program_name="Test Prog",
            leader_category="Test Ldr",
            csv_content="id,Title\n1,Test",
            filename="test.csv",
            tenant_code="custom-tenant",
            log=log,
        )
        assert ok is True
        assert len(captured_requests) == 1
        body = captured_requests[0].data.decode("utf-8")
        assert 'name="tenant_code"\r\n\r\ncustom-tenant\r\n' in body


def test_build_story_csv_row_matches_expected_columns():
    """Verify build_story_csv_row includes expected columns and conditionally adds masked columns."""
    from migrate import build_story_csv_row
    from app.config import Settings

    enriched = {
        "submission_id": "123",
        "title": "My Story",
        "user_name": "Test User",
        "designation": "Teacher",
        "location": "Patna",
        "state": "Bihar",
        "district": "Patna",
        "organization": "Test Org",
        "created_at": "2026-11-13T10:00:00Z",
        "images": [],
        "pdf_url": "",
        "session_id": "sess-123",
        "survey_params": {},
        "raw_params": {},
    }
    raw = {}

    # Case 1: When expected_columns includes masked_blurb and masked_content
    expected_with_masked = json.loads(Settings.model_fields["STORY_CSV_COLUMN"].default)
    row_with_masked = build_story_csv_row(enriched, raw, expected_columns=expected_with_masked)
    for col in expected_with_masked:
        assert col in row_with_masked, f"Missing required column '{col}' in story CSV row"
    assert row_with_masked["masked_blurb"] == ""
    assert row_with_masked["masked_content"] == ""
    assert row_with_masked["Location"] == "Patna, Bihar"

    # Case 2: When expected_columns does NOT include masked columns (e.g. deployed .env without them)
    expected_without_masked = [c for c in expected_with_masked if c not in ("masked_blurb", "masked_content")]
    row_without_masked = build_story_csv_row(enriched, raw, expected_columns=expected_without_masked)
    assert "masked_blurb" not in row_without_masked
    assert "masked_content" not in row_without_masked
    for col in expected_without_masked:
        assert col in row_without_masked


def test_format_location_with_state():
    """Verify format_location_with_state properly encodes state as last comma-separated component."""
    from migrate import format_location_with_state

    assert format_location_with_state("Patna", "Bihar") == "Patna, Bihar"
    assert format_location_with_state("Patna, Bihar", "Bihar") == "Patna, Bihar"
    assert format_location_with_state("", "Bihar") == "Bihar"
    assert format_location_with_state("Patna", None) == "Patna"


def test_build_discussion_csv_row_recovers_aliases():
    """Verify build_discussion_csv_row recovers challenges, solutions, and participant counts from aliases."""
    from migrate import build_discussion_csv_row

    enriched = {
        "submission_id": "456",
        "title": "Chaupal Meet",
        "user_name": "Speaker",
        "location": "Gaya",
        "state": "Bihar",
        "district": "Gaya",
        "organization": "Partner",
        "created_at": "2026-11-13T10:00:00Z",
        "images": [],
        "pdf_url": "",
        "session_id": "sess-456",
        "survey_params": {
            "challenges": ["Lack of road", "No water"],
            "solutions": ["Built canal"],
            "attendees": "25",
        },
        "raw_params": {},
    }
    raw = {}
    row = build_discussion_csv_row(enriched, raw)

    assert row["Challenges"] == "Lack of road; No water"
    assert row["Solutions"] == "Built canal"
    assert row["Participant Count"] == "25"
    assert row["User Location"] == "Gaya, Bihar"


def test_build_discussion_csv_row_returns_none_when_count_zero():
    """Verify build_discussion_csv_row returns None when participant count is 0 or missing, rather than fabricating 1."""
    from migrate import build_discussion_csv_row

    enriched = {
        "submission_id": "789",
        "title": "Chaupal Without Count",
        "user_name": "Speaker",
        "location": "Gaya",
        "state": "Bihar",
        "district": "Gaya",
        "organization": "Partner",
        "created_at": "2026-11-13T10:00:00Z",
        "images": [],
        "pdf_url": "",
        "session_id": "sess-789",
        "survey_params": {},
        "raw_params": {},
    }
    raw = {}
    row = build_discussion_csv_row(enriched, raw)
    assert row is None


def test_config_requires_database_url():
    """Verify Config raises ValueError when DATABASE_URL is not provided or empty."""
    import os
    import pytest
    from migrate import Config

    with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False):
        with pytest.raises(ValueError) as exc:
            Config()
        assert "DATABASE_URL environment variable is required" in str(exc.value)


def test_config_validates_tenant_slugs():
    """Verify Config rejects empty or multiple TENANT_SLUGS."""
    import os
    import pytest
    from migrate import Config

    # Empty TENANT_SLUGS
    with patch.dict(os.environ, {"DATABASE_URL": "postgresql://mock", "TENANT_SLUGS": ""}, clear=False):
        with pytest.raises(ValueError) as exc:
            Config()
        assert "TENANT_SLUGS environment variable must be set" in str(exc.value)

    # Multi-value TENANT_SLUGS
    with patch.dict(os.environ, {"DATABASE_URL": "postgresql://mock", "TENANT_SLUGS": "tenant1,tenant2"}, clear=False):
        with pytest.raises(ValueError) as exc:
            Config()
        assert "Multi-tenant sync is not supported" in str(exc.value)


def test_organization_resolver_fails_on_corrupt_file(tmp_path: Path):
    """Verify OrganizationResolver raises ValueError on invalid/corrupted json."""
    import pytest
    from migrate import OrganizationResolver

    bad_file = tmp_path / "bad_orgs.json"
    bad_file.write_text("{ corrupt json ...", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        OrganizationResolver(bad_file)





