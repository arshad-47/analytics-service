"""
One reader of the date-time text of an uploaded CSV, shared by the feedback upload (Mitra conversations) and the
story / discussion upload (`format_datetime` in app/api/services/uploads.py), so the same cell is read the same way
everywhere.

What is accepted, in this order:
  1. ISO 8601 ('2026-10-15T08:54:43+05:30', 'Z' suffix, a plain date);
  2. day-first numeric dates, as a spreadsheet saved in a European or Indian locale writes them ('15/10/2026 08:54');
  3. dates with the month written out, as the Metabase export writes them ('November 16, 2026, 9:09:20.296 AM'): full or
     abbreviated month name, a comma or a space after the year, a 12-hour clock with AM / PM or a 24-hour one, with or
     without seconds and milliseconds, or the date alone;
  4. any extra strptime patterns the caller passes (a feedback config's `datetime.formats`).
Numeric MONTH-FIRST dates (10/15/2026) are NOT guessed: 03/04/2026 is ambiguous and a wrong guess would move a record by
months, so such a value is unparsable. A month NAME is never ambiguous. A value with an offset is converted to UTC; a
value without one is read in the caller's fallback timezone. Anything else is unparsable (None).
"""
from datetime import datetime, timezone, tzinfo
from typing import Optional, Tuple

import pandas as pd
import re

_ISO_LIKE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")

DEFAULT_DAY_FIRST_FORMATS: Tuple[str, ...] = (
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d/%m/%Y", "%d-%m-%Y",
)


def _text_month_formats() -> Tuple[str, ...]:
    formats = []
    for month in ("%B", "%b"):
        for separator in (", ", " "):
            for clock in ("%I:%M:%S.%f %p", "%I:%M:%S %p", "%I:%M %p", "%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
                formats.append(f"{month} %d, %Y{separator}{clock}")
        formats.append(f"{month} %d, %Y")
    return tuple(formats)


TEXT_MONTH_FORMATS: Tuple[str, ...] = _text_month_formats()
# Every format read after ISO 8601 when the caller names none: the day-first ones, then the written-out-month ones.
DEFAULT_ACCEPTED_FORMATS: Tuple[str, ...] = DEFAULT_DAY_FIRST_FORMATS + TEXT_MONTH_FORMATS


def parse_datetime_text(value: str, fallback_tz: tzinfo = timezone.utc,
                        formats: Tuple[str, ...] = DEFAULT_ACCEPTED_FORMATS) -> Optional[datetime]:
    """Aware datetime, or None when the text is empty or unparsable (see the module docstring for what is read)."""
    text = value.strip()
    if text == "":
        return None
    dt: Optional[datetime] = None
    try:
        dt = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text)
    except ValueError:
        if _ISO_LIKE_RE.match(text):
            try:
                ts = pd.Timestamp(text)
            except (ValueError, OverflowError):
                return None
            if pd.isna(ts):
                return None
            dt = ts.to_pydatetime()
    if dt is None:
        for pattern in formats:
            try:
                dt = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=fallback_tz)
    try:
        dt.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None  # year 0001 / 9999 at an offset: not representable in UTC, so it is an unparsable date
    return dt
