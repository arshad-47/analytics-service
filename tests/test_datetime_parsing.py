"""
The story / discussion upload (`format_datetime`) and the feedback upload read a date cell with the SAME reader
(app/services/datetime_parsing.py), so a cell means the same thing in both. Pure tests. SYNTHETIC values.
"""
from datetime import timezone

import pytest

from app.api.services.feedback_uploads import _parse_datetime
from app.api.services.uploads import format_datetime
from app.services.datetime_parsing import parse_datetime_text

# (cell, the instant both uploads must read, as event text) ; None = unreadable in both
CELLS = [
    ("November 16, 2026, 9:09:20.296 AM ", "2026-11-16T09:09:20.296Z"),
    ("November 16, 2026, 9:09:20.296 PM", "2026-11-16T21:09:20.296Z"),
    ("Nov 6, 2026, 9:09 AM", "2026-11-06T09:09:00.000Z"),
    ("November 16, 2026", "2026-11-16T00:00:00.000Z"),
    ("2026-11-16T09:09:20.296Z", "2026-11-16T09:09:20.296Z"),
    ("2026-11-16", "2026-11-16T00:00:00.000Z"),
    ("2026-10-15T08:54:43+05:30", "2026-10-15T03:24:43.000Z"),       # an offset is converted to UTC, not dropped
    ("2026-10-15T08:54:43", "2026-10-15T08:54:43.000Z"),             # no offset: read as UTC
    ("05/11/2026 11:39", "2026-11-05T11:39:00.000Z"),                # numeric dates are day-first in both
    ("15/10/2026 08:54:43", "2026-10-15T08:54:43.000Z"),
    ("15/10/2026", "2026-10-15T00:00:00.000Z"),                      # the date alone, day-first, slashes or dashes
    ("15-10-2026", "2026-10-15T00:00:00.000Z"),
    ("05-11-2026", "2026-11-05T00:00:00.000Z"),
    ("15-10-2026 08:54", "2026-10-15T08:54:00.000Z"),
    ("10/15/2026 08:54", None),                                      # numeric month-first is never guessed
    ("10-15-2026", None),
    ("November 31, 2026, 9:09 AM", None),
    ("not a date", None),
]


@pytest.mark.parametrize("cell, expected", CELLS)
def test_both_uploads_read_a_cell_the_same_way(cell, expected):
    feedback = _parse_datetime(cell, timezone.utc)
    read_by_feedback = None if feedback is None else feedback.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    assert read_by_feedback == expected
    story_or_discussion = format_datetime(cell, with_ms=True)
    assert story_or_discussion == (expected if expected is not None else cell)    # unreadable text is returned unchanged


def test_the_seconds_only_form_of_the_event_text_drops_the_milliseconds():
    assert format_datetime("November 16, 2026, 9:09:20.296 AM", with_ms=False) == "2026-11-16T09:09:20Z"


def test_an_empty_cell_is_the_current_time_unless_the_caller_says_otherwise():
    for empty in (None, "", "   "):
        assert format_datetime(empty, with_ms=False, fallback_to_now=False) is None
        assert format_datetime(empty, with_ms=False).endswith("Z") and format_datetime(empty, with_ms=False)[:2] == "20"


def test_a_value_without_an_offset_is_read_in_the_zone_the_caller_names():
    from zoneinfo import ZoneInfo
    read = parse_datetime_text("November 16, 2026, 9:09:20.296 AM", ZoneInfo("Asia/Kolkata"))
    assert read.astimezone(timezone.utc).strftime("%H:%M:%S") == "03:39:20"
