#!/usr/bin/env python3
"""Split the Mitra all-persona export CSV into upload-ready per-persona files.

The feedback upload API takes ONE persona (``session_type``) per upload and has
a size cap, so this tool writes one or more CSV files per ``session_type``,
never splitting a session across files and keeping each file <= ``--max-mb``.

Usage:
    python tools/split_mitra_export.py INPUT_CSV --out-dir DIR [--max-mb 80]
                                       [--prefix NAME] [--force]
                                       [--type-col session_type] [--session-col session]

``--type-col`` / ``--session-col`` name the persona column and the session-id column of the export (the defaults are
the Mitra names). Headers are matched like the uploader does: surrounding spaces and the case are ignored and a
UTF-8 BOM is dropped; session ids and session types are compared stripped. A blank line in the input is skipped.

A ``session_type`` becomes part of a file name, so it must be 1 to 64 characters of letters, digits, ``_``, ``.``
and ``-`` (anything else, such as a path separator, is refused and nothing is written); the same rule applies to
``--prefix``. Two session types that differ only in case are refused (they would share one file on a
case-insensitive file system). Output files are created with mode 0600 (they hold conversations), the output
directory with 0700 when this tool creates it, and no file is ever written outside ``--out-dir``.

Streaming, two passes over the input (low memory):
  1. size every session (rows + bytes as they will be written),
  2. route each row to its part file.

The summary (``split_summary.json``, stdout table, logs) contains aggregates
only: never message text, session ids or submission ids.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import sys
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

log = logging.getLogger("split_mitra_export")

SESSION_TYPE_COL = "session_type"
SESSION_COL = "session"
SUMMARY_NAME = "split_summary.json"
# What may become part of an output file name (a session_type, the --prefix): no separator, no space, no control char.
_NAME_PART = re.compile(r"[A-Za-z0-9._-]{1,64}")
_FILE_MODE = 0o600
_DIR_MODE = 0o700
DEFAULT_MAX_MB = 80
# Output files kept open at once while routing rows (a typical process limit is 256 on macOS, 1024 on Linux).
DEFAULT_MAX_OPEN_FILES = 64


class SplitError(Exception):
    """Validation or usage error; nothing is written when this is raised."""


@dataclass
class _SessionInfo:
    session_type: str
    rows: int = 0
    bytes: int = 0


@dataclass
class _Part:
    session_type: str
    sessions: int = 0
    rows: int = 0
    bytes: int = 0
    name: str = ""


def _encode_row(row: Sequence[str]) -> bytes:
    """Encode one row exactly as it will be written to an output file."""
    buf = io.StringIO(newline="")
    csv.writer(buf, lineterminator="\n").writerow(row)
    return buf.getvalue().encode("utf-8")


def _open_reader(path: Path):
    # utf-8-sig: a BOM written by a spreadsheet export is dropped instead of becoming part of the first header.
    return open(path, "r", encoding="utf-8-sig", newline="")


def _column_index(header: Sequence[str], wanted: str) -> int:
    """Index of the header cell that equals `wanted`, ignoring case and surrounding spaces (the first one wins, like
    the uploader). Raises SplitError when there is none."""
    target = wanted.strip().lower()
    for i, cell in enumerate(header):
        if cell.strip().lower() == target:
            return i
    raise SplitError(f"required column '{wanted}' is missing from the header")


def _check_name_part(value: str, what: str) -> str:
    if _NAME_PART.fullmatch(value) is None:
        raise SplitError(
            f"{what} must be 1 to 64 characters of letters, digits, '_', '.' or '-' (it becomes part of a file name)"
        )
    return value


def _scan(input_path: Path, type_col: str = SESSION_TYPE_COL, session_col: str = SESSION_COL):
    """Pass 1. Returns (header, sessions dict in first-appearance order, total_rows). Sessions are keyed by the
    STRIPPED session id and carry the stripped session type."""
    sessions: Dict[str, _SessionInfo] = {}
    total_rows = 0
    case_names: Dict[str, str] = {}
    with _open_reader(input_path) as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            raise SplitError("input file is empty (no header row)")
        type_idx = _column_index(header, type_col)
        sess_idx = _column_index(header, session_col)
        ncols = len(header)
        for n, row in enumerate(reader, start=1):
            if len(row) == 0:
                continue  # a blank line (for example a trailing one): the uploader's CSV reader skips it too
            if len(row) != ncols:
                raise SplitError(
                    f"data row {n}: expected {ncols} fields, got {len(row)}"
                )
            stype = row[type_idx].strip()
            sid = row[sess_idx].strip()
            if stype == "":
                raise SplitError(f"data row {n}: blank {type_col}")
            if sid == "":
                raise SplitError(f"data row {n}: blank {session_col}")
            if _NAME_PART.fullmatch(stype) is None:
                # Deliberately no value in the message: it is not a usable file name (a path, a control character).
                raise SplitError(
                    f"data row {n}: {type_col} must be 1 to 64 characters of letters, digits, '_', '.' or '-' "
                    "(it becomes part of a file name)"
                )
            seen = case_names.setdefault(stype.casefold(), stype)
            if seen != stype:
                raise SplitError(
                    f"data row {n}: two {type_col} values differ only in case; they would share one file on a "
                    "case-insensitive file system. Fix the export first"
                )
            info = sessions.get(sid)
            if info is None:
                info = sessions[sid] = _SessionInfo(session_type=stype)
            elif info.session_type != stype:
                # Deliberately no session id in the message.
                raise SplitError(
                    f"data row {n}: a session has more than one distinct "
                    f"{type_col}"
                )
            info.rows += 1
            info.bytes += len(_encode_row(row))
            total_rows += 1
    return header, sessions, total_rows


def _pack(sessions: Dict[str, _SessionInfo], header_bytes: int, max_bytes: int):
    """Greedy packing per session_type. Returns {type: [_Part]} and the
    session -> (type, part index) routing map."""
    parts: Dict[str, List[_Part]] = {}
    route: Dict[str, tuple] = {}
    oversize = 0
    for sid, info in sessions.items():
        plist = parts.setdefault(info.session_type, [])
        if info.bytes + header_bytes > max_bytes:
            oversize += 1
        cur = plist[-1] if plist else None
        if cur is None or (cur.bytes + info.bytes > max_bytes):
            cur = _Part(session_type=info.session_type, bytes=header_bytes)
            plist.append(cur)
        cur.sessions += 1
        cur.rows += info.rows
        cur.bytes += info.bytes
        route[sid] = (info.session_type, len(plist) - 1)
    return parts, route, oversize


class _WriterCache:
    """
    Least-recently-used cache of at most `limit` open CSV writers over already created part files. A part whose writer
    was evicted is re-opened in APPEND mode (the header was written once, when the file was created), so the rows of
    every part keep their input order and no header is repeated.
    """

    def __init__(self, limit: int):
        self._limit = limit
        self._open: "OrderedDict[tuple, tuple]" = OrderedDict()   # key -> (file handle, csv writer)

    def writer(self, key: tuple, path: Path):
        found = self._open.get(key)
        if found is not None:
            self._open.move_to_end(key)
            return found[1]
        while len(self._open) >= self._limit:
            _, (old_handle, _) = self._open.popitem(last=False)
            old_handle.close()
        fd = os.open(path, os.O_WRONLY | os.O_APPEND)
        handle = os.fdopen(fd, "w", encoding="utf-8", newline="")
        writer = csv.writer(handle, lineterminator="\n")
        self._open[key] = (handle, writer)
        return writer

    def close_all(self) -> None:
        while len(self._open) > 0:
            _, (handle, _) = self._open.popitem(last=False)
            handle.close()


def _file_name(prefix: str, stype: str, idx: int, nparts: int) -> str:
    if nparts == 1:
        return f"{prefix}{stype}.csv"
    return f"{prefix}{stype}_part{idx + 1}.csv"


def split_export(
    input_path,
    out_dir,
    max_bytes: int,
    prefix: str = "",
    force: bool = False,
    *,
    type_col: str = SESSION_TYPE_COL,
    session_col: str = SESSION_COL,
    max_open_files: int = DEFAULT_MAX_OPEN_FILES,
) -> dict:
    """Split ``input_path`` into per-persona CSVs under ``out_dir``.

    Returns the summary dict (aggregates only). Raises SplitError on validation
    problems or when an output exists and ``force`` is False; in those cases
    nothing is written. ``type_col`` / ``session_col`` name the persona and the
    session-id columns (matched ignoring case and surrounding spaces).
    ``max_open_files`` bounds the output files open at the same time (the others
    are closed and re-opened in append mode when a row for them arrives).
    """
    csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
    input_path = Path(input_path)
    out_dir = Path(out_dir)
    if max_bytes <= 0:
        raise SplitError("max size must be positive")
    if max_open_files < 1:
        raise SplitError("max open files must be at least 1")

    if prefix != "":
        _check_name_part(prefix, "--prefix")
    header, sessions, total_rows = _scan(input_path, type_col, session_col)
    header_bytes = len(_encode_row(header))
    parts, route, oversize = _pack(sessions, header_bytes, max_bytes)
    if oversize > 0:
        log.warning(
            "%d session(s) exceed the size limit on their own and are kept whole "
            "in their own part",
            oversize,
        )

    # Name parts and check for collisions before writing anything.
    named: List[_Part] = []
    for stype, plist in parts.items():
        for i, part in enumerate(plist):
            part.name = _file_name(prefix, stype, i, len(plist))
            named.append(part)
    # Every name is built from checked parts, so it cannot leave out_dir; the resolved path is verified anyway, and
    # two names that differ only in case (for example 'X_part1' of a type X and a type named 'x_part1') are refused.
    resolved_dir = out_dir.resolve()
    lowered: Dict[str, str] = {}
    for p in named:
        if (resolved_dir / p.name).resolve().parent != resolved_dir:
            raise SplitError("an output file name would fall outside --out-dir")
        earlier = lowered.setdefault(p.name.casefold(), p.name)
        if earlier == p.name and sum(1 for q in named if q.name == p.name) > 1:
            # Not only case: two outputs with the SAME name, for example the part 1 of 'X' (X_part1.csv, when X needs
            # several files) and a session type literally named 'X_part1' (X_part1.csv). Both would be written to
            # the same temporary file and the second rename would fail after rows were lost.
            raise SplitError(
                "two outputs would get the same file name (a session type that needs several files produces "
                "'<type>_partN.csv', which another session type can also produce); rename a session type or use --prefix")
        if earlier != p.name:
            raise SplitError("two output file names differ only in case; they would collide on a case-insensitive file system")
    if not force:
        existing = [p.name for p in named if (out_dir / p.name).exists()]
        if (out_dir / SUMMARY_NAME).exists():
            existing.append(SUMMARY_NAME)
        if len(existing) > 0:
            raise SplitError(
                "refusing to overwrite existing file(s) in out dir: "
                + ", ".join(sorted(existing))
                + " (use --force)"
            )

    out_dir.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)

    # Pass 2: route rows. Write to .tmp files, then rename on success. The files are created (header only) one at a
    # time; the rows are then appended through a bounded cache of open writers (see _WriterCache), so the number of
    # simultaneously open files never depends on how many parts the export needs.
    tmp_paths: Dict[tuple, Path] = {}
    cache = _WriterCache(max_open_files)
    try:
        for stype, plist in parts.items():
            for i, part in enumerate(plist):
                tmp = out_dir / (part.name + ".tmp")
                tmp.unlink(missing_ok=True)
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _FILE_MODE)
                tmp_paths[(stype, i)] = tmp
                with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
                    csv.writer(fh, lineterminator="\n").writerow(header)
        sess_idx = _column_index(header, session_col)
        with _open_reader(input_path) as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                if len(row) == 0:
                    continue
                key = route[row[sess_idx].strip()]
                cache.writer(key, tmp_paths[key]).writerow(row)
    except BaseException:
        cache.close_all()
        for tmp in tmp_paths.values():
            tmp.unlink(missing_ok=True)
        raise
    cache.close_all()
    for (stype, i), tmp in tmp_paths.items():
        tmp.replace(out_dir / parts[stype][i].name)

    summary = {
        "input_rows": total_rows,
        "input_sessions": len(sessions),
        "max_bytes": max_bytes,
        "files": [
            {
                "name": p.name,
                "session_type": p.session_type,
                "sessions": p.sessions,
                "rows": p.rows,
                "bytes": (out_dir / p.name).stat().st_size,
            }
            for p in named
        ],
    }
    summary["totals"] = {
        "files": len(named),
        "sessions": sum(f["sessions"] for f in summary["files"]),
        "rows": sum(f["rows"] for f in summary["files"]),
        "bytes": sum(f["bytes"] for f in summary["files"]),
    }
    summary_tmp = out_dir / (SUMMARY_NAME + ".tmp")
    summary_tmp.unlink(missing_ok=True)
    fd = os.open(summary_tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _FILE_MODE)
    with os.fdopen(fd, "w", encoding="utf-8") as sfh:
        sfh.write(json.dumps(summary, indent=2) + "\n")
    summary_tmp.replace(out_dir / SUMMARY_NAME)
    written = {p.name for p in named}
    stale = sorted(
        f.name for stype in parts for f in out_dir.glob(f"{prefix}{stype}_part*.csv") if f.name not in written
    )
    if len(stale) > 0:
        log.warning(
            "%d older part file(s) from an earlier run are still in the out dir and are not in the summary: %s",
            len(stale), ", ".join(stale),
        )
    return summary


def _print_table(summary: dict) -> None:
    files = summary["files"]
    width = max([len("file")] + [len(f["name"]) for f in files])
    print(f"{'file':<{width}}  {'session_type':<20} {'sessions':>8} {'rows':>8} {'MB':>8}")
    for f in files:
        print(
            f"{f['name']:<{width}}  {f['session_type']:<20} {f['sessions']:>8} "
            f"{f['rows']:>8} {f['bytes'] / 1_000_000:>8.2f}"
        )
    t = summary["totals"]
    print(
        f"{'TOTAL':<{width}}  {'':<20} {t['sessions']:>8} {t['rows']:>8} "
        f"{t['bytes'] / 1_000_000:>8.2f}"
    )
    print(
        f"input: {summary['input_sessions']} sessions, {summary['input_rows']} rows"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Split the Mitra all-persona export into per-persona upload CSVs."
    )
    ap.add_argument("input_csv")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--max-mb", type=float, default=DEFAULT_MAX_MB,
                    help="max size per output file in MB (1 MB = 1,000,000 bytes; default 80)")
    ap.add_argument("--prefix", default="")
    ap.add_argument("--type-col", default=SESSION_TYPE_COL,
                    help="header of the persona column (default session_type; case and spaces are ignored)")
    ap.add_argument("--session-col", default=SESSION_COL,
                    help="header of the session-id column (default session; case and spaces are ignored)")
    ap.add_argument("--force", action="store_true",
                    help="overwrite existing output files")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        summary = split_export(
            args.input_csv,
            args.out_dir,
            int(args.max_mb * 1_000_000),
            prefix=args.prefix,
            force=args.force,
            type_col=args.type_col,
            session_col=args.session_col,
        )
    except SplitError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    _print_table(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
