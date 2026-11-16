"""
Show, migrate and change the per-bot feedback settings that live in the persona's upload config (config_version),
not in .env: max_csv_upload_bytes, ingestion_schema, process_steps and the optional expect_stages.

    python tools/feedback_config.py show    [--session-type Youth_Voice] [--tenant-code T] [--source S]
                                            [--program-id UUID | --program-name NAME]
    python tools/feedback_config.py migrate [--dry-run]
    python tools/feedback_config.py migrate-legacy --tenant-code T [--map KEY=SESSION_TYPE ...] [--map-file F.json] \
                                        [--source S] [--retire-config CONFIG_ID ...] [--dry-run]
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra \
                                        --key max_csv_upload_bytes --value 50000000 [--dry-run]
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra \
                                        --key process_steps --json '[{"name": "feedback_metadata_extraction"}]'
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra \
                                        --key expect_stages --json '["AGE_IDENTIFICATION", "STATE_IDENTIFICATION"]'
    python tools/feedback_config.py set --session-type Youth_Voice --source mitra --key expect_stages --unset

A persona is identified by (tenant, program, session_type, source): a session_type is not unique across programs
and a source is a free label. `set` therefore needs --session-type and --source and refuses (listing the
candidates, writing nothing) when more than one config still matches; add --tenant-code and --program-id (or
--program-name) to pick one. The same selectors narrow `show`.

`show` prints the settings of every active feedback_upload config and lists what is missing or invalid.
`migrate` adds the settings that are missing (from the seed defaults) to every ACTIVE feedback_upload config of an
older database (seeds never overwrite, so those keep the config version they were created with): it writes a NEW
config version (the old one is deactivated and kept as history), never overwrites an existing key and is idempotent.
It sweeps every tenant, program and source; a missing key it cannot derive (the `adapter` of a source that is not a
legacy one) is reported, never invented.
`migrate-legacy` is the ONE-OFF upgrade of a database created before session_type, questionnaire versions and adapters
existed (release 1.3: one config row per Mitra bot, no persona key on configs or stored sessions); see
app/database/feedback_legacy_upgrade.py and the README ("Upgrading an existing database"). For ONE tenant it gives
each legacy upload config its session_type from YOUR mapping (KEY = a program id, `leader:<leader category name>` or a
program name that is unique in the tenant; a config the mapping does not cover is reported as unmapped, never guessed),
writes ONE new config version per config that adds only the missing keys (session_type, adapter derived from the legacy
source, questionnaire_versions V1, opening_row auto, column_map, completion_map, forms[*].questionnaire_version V1,
plus the keys `migrate` adds), fills session_type / questionnaire_version of the stored sessions where they are NULL,
creates the unique index of `config` once the data allows it, and prints counts and ids only. Several config rows of
one source and program (the old one-row-per-bot design) cannot both stay: it lists them as duplicates, and
--retire-config deletes the ones you do not keep (their versions with them). It is tenant-scoped, idempotent (a second
run changes nothing) and takes --dry-run (everything computed, nothing written). Run it once with --dry-run first.
`set` changes one setting after validating it, again as a new config version. Scalars (max_csv_upload_bytes) take
--value; lists / objects take --json (a JSON text). Both take --dry-run (compute, print, write nothing). Reads
DATABASE_URL like the application does. Prints JSON (settings, counts); never respondent data.

`show` and `set` also validate the adapter-related keys of the whole config (adapter, column_map, completion_map and
the keys that belong to the other adapter): a problem shows under "adapter_config" in `show`, and `set --key adapter`
refuses a switch the config would not survive.

A change is picked up by the Kafka consumers only when their config cache entry expires (60 seconds): for up to one
minute events are still validated under the old config (the upload route reads the config uncached).
"""
import argparse
import asyncio
import getpass
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import feedback_legacy_upgrade, feedback_operations  # noqa: E402
from app.database.db import db  # noqa: E402
from app.services.feedback_constants import (  # noqa: E402
    CONFIG_KEY_ADAPTER,
    CONFIG_KEY_EXPECT_STAGES,
    CONFIG_KEY_INGESTION_SCHEMA,
    CONFIG_KEY_MAX_CSV_UPLOAD_BYTES,
    CONFIG_KEY_PROCESS_STEPS,
)

KEYS = (CONFIG_KEY_ADAPTER, CONFIG_KEY_MAX_CSV_UPLOAD_BYTES, CONFIG_KEY_INGESTION_SCHEMA, CONFIG_KEY_PROCESS_STEPS,
        CONFIG_KEY_EXPECT_STAGES)


def parse_set_value(key: str, value_text, json_text):
    """
    The Python value of `set`'s --value / --json for `key`. Exactly one of the two must be given (the caller has
    checked --unset). --value is a plain integer, only for max_csv_upload_bytes; --json is JSON text for any key
    (for max_csv_upload_bytes it must then be a JSON integer). Raises ValueError with a clear message.
    """
    if value_text is not None and json_text is not None:
        raise ValueError("give either --value or --json, not both")
    if value_text is None and json_text is None:
        raise ValueError("give --value (a number) or --json (a JSON text); --unset only works for expect_stages")
    if value_text is not None:
        if key != CONFIG_KEY_MAX_CSV_UPLOAD_BYTES:
            raise ValueError(f"--value is only for {CONFIG_KEY_MAX_CSV_UPLOAD_BYTES}; use --json for {key}")
        try:
            return int(value_text.strip())
        except ValueError:
            raise ValueError(f"--value {value_text!r} is not an integer")
    try:
        return json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"--json is not valid JSON: {exc}")


async def _run(args):
    await db.connect()
    try:
        if args.command == "show":
            listing = await feedback_operations.list_feedback_config(
                args.session_type, tenant_code=args.tenant_code, program_id=args.program_id,
                program_name=args.program_name, source=args.source)
            invalid = await feedback_legacy_upgrade.adapter_problems_for({
                "session_type": args.session_type, "source": args.source, "tenant_code": args.tenant_code,
                "program_id": args.program_id, "program_name": args.program_name})
            for entry in listing["configs"]:
                reason = invalid.get((entry["tenant_code"], entry["program_id"], entry["session_type"], entry["source"]))
                if reason is not None:
                    entry["problems"] = {**entry["problems"], "adapter_config": reason}
            return listing
        if args.command == "migrate":
            return await feedback_operations.migrate_feedback_config(dry_run=args.dry_run, created_by=getpass.getuser())
        if args.command == "migrate-legacy":
            file_text = Path(args.map_file).read_text(encoding="utf-8") if args.map_file is not None else None
            return await feedback_legacy_upgrade.migrate_legacy(
                tenant_code=args.tenant_code,
                session_type_map=feedback_legacy_upgrade.parse_session_type_map(args.map, file_text),
                source=args.source, retire_config_ids=args.retire_config, dry_run=args.dry_run,
                created_by=getpass.getuser())
        if args.unset and (args.value is not None or args.json is not None):
            raise ValueError("--unset takes neither --value nor --json")
        value = None if args.unset else parse_set_value(args.key, args.value, args.json)
        reason = await feedback_legacy_upgrade.adapter_problem_after_set(
            args.session_type, args.source, args.key, value, unset=args.unset, tenant_code=args.tenant_code,
            program_id=args.program_id, program_name=args.program_name)
        if reason is not None:
            raise ValueError(f"the config would be invalid after this change: {reason}")
        return await feedback_operations.set_feedback_config_value(
            args.session_type, args.source, args.key, value, tenant_code=args.tenant_code,
            program_id=args.program_id, program_name=args.program_name, unset=args.unset, created_by=getpass.getuser(), change_note=args.note, dry_run=args.dry_run,
        )
    finally:
        await db.disconnect()


def _add_selectors(parser: argparse.ArgumentParser, *, source_required: bool) -> None:
    """The options that say WHICH config is meant (a session_type alone is not unique)."""
    parser.add_argument("--source", required=source_required,
                        help="the source label of the config, for example mitra or google_forms")
    parser.add_argument("--tenant-code", help="the tenant; needed when several tenants have the persona")
    parser.add_argument("--program-id", help="the program's UUID (exact)")
    parser.add_argument("--program-name", help="the program's name; use --program-id when two programs share it")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    show = sub.add_parser("show", help="print the per-bot settings of the active feedback upload configs")
    show.add_argument("--session-type", help="only this persona key, for example Youth_Voice")
    _add_selectors(show, source_required=False)

    migrate = sub.add_parser("migrate", help="add the missing settings (seed defaults) to the active configs")
    migrate.add_argument("--dry-run", action="store_true", help="report what would change, write nothing")

    legacy = sub.add_parser("migrate-legacy", help="one-off upgrade of a database created before session_type")
    legacy.add_argument("--tenant-code", required=True, help="the tenant to upgrade (one at a time)")
    legacy.add_argument("--map", action="append", default=[], metavar="KEY=SESSION_TYPE",
                        help="persona key of a program: KEY is a program id, leader:<leader category name> or a "
                             "program name unique in the tenant; repeat for every program")
    legacy.add_argument("--map-file", help="the same mapping as a JSON object {KEY: SESSION_TYPE}")
    legacy.add_argument("--source", help="only the configs of this source label")
    legacy.add_argument("--retire-config", action="append", default=[], metavar="CONFIG_ID",
                        help="delete this duplicate config row (and its versions); repeat as needed")
    legacy.add_argument("--dry-run", action="store_true", help="compute and print, write nothing")

    setting = sub.add_parser("set", help="change one setting (new config version)")
    setting.add_argument("--session-type", required=True, help="persona key, for example Youth_Voice")
    _add_selectors(setting, source_required=True)
    setting.add_argument("--key", required=True, choices=KEYS, help="the setting to change")
    setting.add_argument("--value", help="a number (max_csv_upload_bytes only), for example 50000000")
    setting.add_argument("--json", help="a JSON text (any setting), for example '[{\"name\": \"x\"}]'")
    setting.add_argument("--unset", action="store_true", help="remove the optional expect_stages")
    setting.add_argument("--note", help="change note stored on the new config version")
    setting.add_argument("--dry-run", action="store_true", help="validate and print, write nothing")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = asyncio.run(_run(args))
    except (ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
