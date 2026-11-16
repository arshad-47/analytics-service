"""
Record and inspect the questionnaire versions (V1, V2, V3 ...) of the feedback bots. Versions are data in the
database, not code: when a persona's bot questions change, record the new version here, no deploy needed.

    python tools/questionnaire_versions.py list    [--session-type Youth_Voice] [SELECTORS]
    python tools/questionnaire_versions.py set     --session-type Youth_Voice --key V3 \
                                                   --effective-from 2026-12-01T10:30:00+05:30 [--note "..."] \
                                                   [--no-restamp] [--dry-run] [SELECTORS]
    python tools/questionnaire_versions.py restamp [--session-type Youth_Voice] [--dry-run] [SELECTORS]
    python tools/questionnaire_versions.py detect  [--session-type Youth_Voice] [--min-sessions 20]
                                                   [--baseline-sessions 100] [--min-coverage 0.3]
                                                   [--include-wording] [SELECTORS]

SELECTORS = [--tenant-code T] [--program-id UUID | --program-name NAME] [--source S]. A persona is identified by
(tenant, program, session_type, source): a session_type is not unique across programs and a source is a free label.
`set` (and `restamp` with a --session-type) must end up on exactly ONE config and otherwise refuses, listing the
candidates and writing nothing; `list` / `detect` / `restamp` without --session-type cover every matching config
(each config only ever touches the sessions of its own tenant, program, session_type and source). Only sources whose
upload config has `questionnaire_versions` are covered. When a session_type is shared, the keys of the printed result
are tenant/program/session_type/source instead of the bare session_type.

`detect` is read-only: it compares the questions the stored sessions were asked with what each recorded version
asks and, when a new question set appeared, proposes the next key and the instant it started, with the `set`
command to run (--dry-run first). Nothing changes until a person runs `set`.

`set` appends a new version (or moves the date of an existing one), writes a new config version (the old one
stays as history and can be re-activated to roll back) and relabels the already stored sessions in the same
transaction. Use --dry-run first: it prints what would change and writes nothing. `restamp` only relabels from
the current windows (needed after editing the config by hand). Reads DATABASE_URL like the application does.
Prints JSON with counts only.

A change is picked up by the Kafka consumers only when their config cache entry expires (60 seconds): for up to one
minute new events are still stamped with the old windows (run `restamp` afterwards to relabel them).
"""
import argparse
import asyncio
import getpass
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import feedback_operations  # noqa: E402
from app.database.db import db  # noqa: E402


async def _run(args):
    await db.connect()
    try:
        selectors = dict(tenant_code=args.tenant_code, program_id=args.program_id,
                         program_name=args.program_name, source=args.source)
        if args.command == "list":
            return await feedback_operations.list_questionnaire_versions(args.session_type, **selectors)
        if args.command == "detect":
            return await feedback_operations.detect_questionnaire_versions(
                args.session_type, min_sessions=args.min_sessions,
                baseline_sessions=args.baseline_sessions, min_coverage=args.min_coverage,
                include_wording=args.include_wording, **selectors)
        if args.command == "restamp":
            return await feedback_operations.restamp_questionnaire_versions(
                args.session_type, dry_run=args.dry_run, **selectors)
        return await feedback_operations.set_questionnaire_version(
            args.session_type, args.key, args.effective_from, **selectors,
            created_by=getpass.getuser(), change_note=args.note,
            restamp=not args.no_restamp, dry_run=args.dry_run,
        )
    finally:
        await db.disconnect()


def _add_selectors(parser: argparse.ArgumentParser) -> None:
    """The options that say WHICH persona is meant (a session_type alone is not unique)."""
    parser.add_argument("--tenant-code", help="the tenant; needed when several tenants have the persona")
    parser.add_argument("--program-id", help="the program's UUID (exact)")
    parser.add_argument("--program-name", help="the program's name; use --program-id when two programs share it")
    parser.add_argument("--source", help="the source label, needed when several sources have a versioned config")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    listing = sub.add_parser("list", help="show the recorded versions and the stored sessions per label")
    listing.add_argument("--session-type", help="only this persona key, for example Youth_Voice")
    _add_selectors(listing)

    setting = sub.add_parser("set", help="record a version (new key: appended; existing key: date moved)")
    setting.add_argument("--session-type", required=True, help="persona key, for example Youth_Voice")
    setting.add_argument("--key", required=True, help="version label, for example V3")
    setting.add_argument("--effective-from", required=True,
                         help="ISO instant WITH a UTC offset, for example 2026-12-01T10:30:00+05:30")
    setting.add_argument("--note", help="change note stored on the new config version")
    setting.add_argument("--no-restamp", action="store_true", help="do not relabel the stored sessions")
    setting.add_argument("--dry-run", action="store_true", help="compute and print, write nothing")
    _add_selectors(setting)

    restamp = sub.add_parser("restamp", help="relabel the stored sessions from the current windows")
    restamp.add_argument("--session-type", help="only this persona key")
    restamp.add_argument("--dry-run", action="store_true", help="report what would change, write nothing")
    _add_selectors(restamp)

    detecting = sub.add_parser("detect", help="propose a new version from the questions the stored sessions were asked")
    detecting.add_argument("--session-type", help="only this persona key")
    _add_selectors(detecting)
    detecting.add_argument("--min-sessions", type=int, default=20,
                           help="a question must be asked in this many sessions to count as new (default 20)")
    detecting.add_argument("--baseline-sessions", type=int, default=100,
                           help="sessions per version used to learn what it asks (default 100)")
    detecting.add_argument("--min-coverage", type=float, default=0.3,
                           help="share of the sessions since it first appeared that must ask a new question (default 0.3)")
    detecting.add_argument("--include-wording", action="store_true",
                           help="also propose a version when the only new questions are close wording variants of known ones")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = asyncio.run(_run(args))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
