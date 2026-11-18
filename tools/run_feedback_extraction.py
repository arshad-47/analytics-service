"""
Run the feedback metadata extraction on stored sessions WITHOUT Temporal, or show the extraction config.

    python tools/run_feedback_extraction.py run --session-type Youth_Voice [--program-id UUID] [--limit 50] [--submission-id 12345]
                                                [--only-unprocessed] [--dry-run] [--concurrency 4]
                                                [--model NAME] [--max-tokens N] [--timeout SECONDS] [--top 15]
    python tools/run_feedback_extraction.py show-config [--session-type Youth_Voice] [--program-id UUID]

`run` does exactly what the workflow step feedback_metadata_extraction does for each selected session (same code,
app/database/feedback_extraction_operations.extract_submission): ONE LLM call with the complete conversation,
then the field rows (feedback_extracted_data) and the PII flag (feedback_submissions.pii_detected) are written in
one transaction; a re-run overwrites. --only-unprocessed selects the sessions whose pii_detected is still NULL.
--dry-run calls the LLM and prints the aggregates but writes nothing (no rows, no llm log). A conversation with no
respondent answer makes no LLM call. The LLM is the one configured for the application (OPENROUTER_* settings),
sessions go to an external provider: confirm the approval for the data before running on real sessions.

Each session is extracted with the prompt its program's extraction config selects (`prompt_name`, required); a name
that does not resolve to a prompt with an active version fails the session loudly.

It prints AGGREGATES ONLY as JSON: counts per field and status, the most frequent canonical values of the category
fields (for a free-text field such as district only the number of distinct values; for an integer field such as age
the number of distinct values and the smallest and largest), PII counts and types, token usage, error types. Never
conversation text, respondent answers, raw values or free-text values. Reads DATABASE_URL like the application does.
`show-config` prints the active extraction config versions (configuration, no respondent data): per persona the
prompt name ("(none)" for a config that names none: it cannot run) and every field with its kind and the sensitive / provisional / multi / versions / applies_when
flags, plus the full config JSON. The seed of these configs is seed_config.sql (it never overwrites an existing
database); an existing database gets new content through a NEW config_version (docs/feedback-extraction-bands.md,
"Changing a band or a field").
"""
import argparse
import asyncio
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database.db import db  # noqa: E402
from app.database.feedback_extraction_operations import LlmCallError, extract_submission  # noqa: E402
from app.services.feedback_extraction import KIND_INTEGER, ExtractionOutputError  # noqa: E402
from app.services.feedback_extraction_constants import (  # noqa: E402
    CONFIG_TYPE_FEEDBACK_EXTRACTION,
    KIND_CATEGORY,
    KIND_FREE_TEXT,
    STATUS_EXTRACTED,
)

NO_PROMPT_LABEL = "(none)"
from app.services.llm import split_llm_usage  # noqa: E402


async def _select_submissions(conn, tenant_code: str, session_type: str, submission_id: Optional[str],
                              only_unprocessed: bool, limit: Optional[int], program_id: Optional[str] = None) -> List[str]:
    conditions = ["fs.tenant_code = $1", "fs.session_type = $2"]
    params: List[Any] = [tenant_code, session_type]
    if program_id is not None:
        # the same session_type can exist in two programs of a tenant: only the sessions of this program
        params.append(program_id)
        conditions.append(
            f"EXISTS (SELECT 1 FROM submissions s WHERE s.submission_id = fs.submission_id "
            f"AND s.tenant_code = fs.tenant_code AND s.program_id = ${len(params)}::uuid)")
    if submission_id is not None:
        params.append(submission_id)
        conditions.append(f"fs.submission_id = ${len(params)}")
    if only_unprocessed:
        conditions.append("fs.pii_detected IS NULL")
    sql = ("SELECT fs.submission_id FROM feedback_submissions fs WHERE " + " AND ".join(conditions)
           + " ORDER BY fs.created_at, fs.submission_id")
    if limit is not None:
        params.append(limit)
        sql += f" LIMIT ${len(params)}"
    return [r["submission_id"] for r in await conn.fetch(sql, *params)]


def _new_aggregates(session_type: str, dry_run: bool) -> Dict[str, Any]:
    return {
        "session_type": session_type,
        "mode": "dry_run (nothing written)" if dry_run else "write",
        "submissions": {"selected": 0, "processed": 0, "failed": 0, "llm_calls": 0, "no_respondent_answer": 0,
                        "oversized_respondent_message": 0, "conversation_too_long": 0},
        "fields": {},
        "pii": {"detected": 0, "not_detected": 0, "types": {}},
        "tokens": {"prompt": 0, "completion": 0},
        "errors": {},
    }


def _add_usage(agg: Dict[str, Any], usage: Any) -> None:
    """One more billed call: it counts in llm_calls and the token totals."""
    agg["submissions"]["llm_calls"] += 1
    prompt_tokens, completion_tokens, _ = split_llm_usage(usage if isinstance(usage, dict) else {})
    agg["tokens"]["prompt"] += prompt_tokens
    agg["tokens"]["completion"] += completion_tokens


def _add_outcome(agg: Dict[str, Any], outcome, top: int, values: Dict[str, Counter]) -> None:
    subs = agg["submissions"]
    subs["processed"] += 1
    if outcome.run.llm_called:
        subs["llm_calls"] += 1
        prompt_tokens, completion_tokens, _ = split_llm_usage(outcome.run.usage)
        agg["tokens"]["prompt"] += prompt_tokens
        agg["tokens"]["completion"] += completion_tokens
        if outcome.run.masking is not None:      # the optional masking call is a second, billed call of the session
            _add_usage(agg, outcome.run.masking.usage)
    else:
        # no call was made: no respondent answer, or a respondent message too long to send (flagged pii TRUE)
        subs[outcome.run.skip_reason] += 1
    for field in outcome.run.result.fields:
        # The sessions of a persona can come from several programs / sources, each with its own extraction config,
        # and the same field name can have another kind in each: every kind that contributed is remembered, and
        # _finish prints values only when ALL of them agree on a kind that may be printed.
        entry = agg["fields"].setdefault(field.field_name, {"status": Counter(), "kinds": set()})
        entry["kinds"].add(outcome.field_kinds.get(field.field_name))
        entry["status"][field.status] += 1
        # Counted here; whether any value is printed is decided in _finish. A value is never empty now ('Not answered',
        # 'N/A' ...), so a free-text or whole-number field counts only what was really extracted (words / numbers of the
        # respondent), and every other kind holds config labels only.
        if field.value is not None and (outcome.field_kinds.get(field.field_name) not in (KIND_FREE_TEXT, KIND_INTEGER)
                                        or field.status == STATUS_EXTRACTED):
            values.setdefault(field.field_name, Counter())[field.value] += 1
    pii = agg["pii"]
    if outcome.run.result.pii_detected:
        pii["detected"] += 1
    else:
        pii["not_detected"] += 1
    for t in outcome.run.result.pii_types:
        pii["types"][t] = pii["types"].get(t, 0) + 1


def _call_was_made(exc: BaseException) -> bool:
    """
    True when the failure happened AFTER a model call: a failed call, an answer the contract refuses (it carries the
    call's usage), or a result found stale after the call. A setup or configuration failure raised before any call
    made none and is not counted.
    """
    return isinstance(exc, (LlmCallError, ExtractionOutputError)) or isinstance(getattr(exc, "usage", None), dict)


def _add_failed_call(agg: Dict[str, Any], exc: BaseException) -> None:
    """The call of a failed session still happened and was billed: it counts in llm_calls and the token totals."""
    prior = getattr(exc, "prior_usage", None)
    if isinstance(prior, dict):                  # a masking call that worked before the failing one
        _add_usage(agg, prior)
    if not _call_was_made(exc):
        return
    _add_usage(agg, getattr(exc, "usage", None))


def _finish(agg: Dict[str, Any], top: int, values: Dict[str, Counter]) -> Dict[str, Any]:
    for name, entry in agg["fields"].items():
        entry["status"] = dict(sorted(entry["status"].items()))
        counter = values.get(name, Counter())
        entry["distinct_values"] = len(counter)
        kinds = entry.pop("kinds")
        kind = next(iter(kinds)) if len(kinds) == 1 else None   # contributing configs that disagree: nothing printable
        if kind == KIND_CATEGORY:
            entry["top_values"] = dict(counter.most_common(top))
        elif kind == KIND_INTEGER and len(counter) > 0:
            numbers = [int(v) for v in counter if re.fullmatch(r"-?\d+", v) is not None]   # the labels are no numbers
            if len(numbers) > 0:
                entry["min_value"], entry["max_value"] = min(numbers), max(numbers)
        # Every other kind (free text such as district; an unknown kind) holds what the model copied from the
        # respondent: only the number of distinct values and the status counts are printed, never the values.
        # An integer field (age) prints the distinct count and the smallest and largest value only.
    agg["pii"]["types"] = dict(sorted(agg["pii"]["types"].items()))
    agg["errors"] = dict(sorted(agg["errors"].items()))
    return agg


async def _run(args) -> Dict[str, Any]:
    dry_run = args.dry_run
    agg = _new_aggregates(args.session_type, dry_run)
    values: Dict[str, Counter] = {}
    overrides = {"llm_model": args.model, "max_tokens": args.max_tokens, "llm_timeout_seconds": args.timeout}
    async with db.pool.acquire() as conn:
        ids = await _select_submissions(conn, args.tenant_code, args.session_type, args.submission_id,
                                        args.only_unprocessed, args.limit, args.program_id)
    agg["submissions"]["selected"] = len(ids)
    semaphore = asyncio.Semaphore(args.concurrency)

    async def one(submission_id: str) -> None:
        async with semaphore:
            try:
                outcome = await extract_submission(
                    db.pool, submission_id, args.tenant_code, llm_overrides=overrides,
                    write=not dry_run,
                )
            except Exception as exc:  # noqa: BLE001  counted by type, the message may hold provider text
                agg["submissions"]["failed"] += 1
                agg["errors"][type(exc).__name__] = agg["errors"].get(type(exc).__name__, 0) + 1
                _add_failed_call(agg, exc)
                return
            _add_outcome(agg, outcome, args.top, values)

    await asyncio.gather(*(one(i) for i in ids))
    return _finish(agg, args.top, values)


def summarise_config(config: Any) -> Dict[str, Any]:
    """
    {"prompt_name": <name or "(none)">, "fields": [{"name", "kind", "sensitive", "provisional", "multi", "versions", "applies_when"}, ...]} of a stored
    config JSON, read leniently (a malformed config is still shown, with what can be read); no band lists.
    """
    name = config.get("prompt_name") if isinstance(config, dict) else None
    fields = config.get("fields") if isinstance(config, dict) else None
    summary: List[Dict[str, Any]] = []
    for item in fields if isinstance(fields, list) else []:
        if not isinstance(item, dict):
            continue
        summary.append({"name": item.get("name"), "kind": item.get("kind"),
                        "sensitive": item.get("sensitive") is True, "provisional": item.get("provisional") is True,
                        "multi": item.get("multi") is True, "versions": item.get("versions"),
                        "applies_when": item.get("applies_when")})
    return {"prompt_name": name if isinstance(name, str) else NO_PROMPT_LABEL, "fields": summary}


async def _show_config(args) -> List[Dict[str, Any]]:
    conditions = ["c.config_type = $1", "cv.is_active"]
    params: List[Any] = [CONFIG_TYPE_FEEDBACK_EXTRACTION]
    if args.session_type is not None:
        params.append(args.session_type)
        conditions.append(f"c.session_type = ${len(params)}")
    if args.program_id is not None:
        params.append(args.program_id)
        conditions.append(f"c.program_id = ${len(params)}::uuid")
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT c.session_type, c.config_source, c.program_id, c.id AS config_id, cv.id AS config_version_id, "
            "cv.version, cv.change_note, cv.config FROM config c JOIN config_version cv ON cv.config_id = c.id "
            "WHERE " + " AND ".join(conditions) + " ORDER BY c.session_type, c.program_id", *params)
    out = []
    for r in rows:
        config = json.loads(r["config"]) if isinstance(r["config"], str) else r["config"]
        out.append({
            "session_type": r["session_type"], "config_source": r["config_source"],
            "program_id": str(r["program_id"]) if r["program_id"] is not None else None,
            "config_id": str(r["config_id"]), "config_version_id": str(r["config_version_id"]),
            "version": r["version"], "change_note": r["change_note"], "summary": summarise_config(config),
            "config": config,
        })
    return out


async def _main(args):
    await db.connect()
    try:
        if args.command == "show-config":
            return await _show_config(args)
        return await _run(args)
    finally:
        await db.disconnect()


def _uuid(text: str) -> str:
    import uuid
    try:
        return str(uuid.UUID(text))
    except ValueError:
        raise argparse.ArgumentTypeError("must be a program uuid") from None


def _positive(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="extract the metadata of stored sessions and print aggregates")
    run.add_argument("--session-type", required=True, help="persona key, for example Youth_Voice")
    run.add_argument("--tenant-code", required=True, help="tenant of the sessions (required: there is no default tenant)")
    run.add_argument("--limit", type=_positive, help="at most this many sessions (oldest first)")
    run.add_argument("--program-id", type=_uuid, help="only the sessions of this program (uuid); the same session_type "
                                                      "can exist in more than one program of a tenant")
    run.add_argument("--submission-id", help="only this session")
    run.add_argument("--only-unprocessed", action="store_true", help="only sessions whose pii_detected is still NULL")
    run.add_argument("--dry-run", action="store_true", help="call the LLM, print aggregates, write nothing")
    run.add_argument("--concurrency", type=_positive, default=4, help="sessions in flight at once (default 4)")
    run.add_argument("--model", help="LLM model for this run (default: the config's llm, then OPENROUTER_MODEL)")
    run.add_argument("--max-tokens", type=_positive, help="max tokens of the answer for this run")
    run.add_argument("--timeout", type=_positive, help="LLM timeout in seconds for this run")
    run.add_argument("--top", type=_positive, default=15, help="most frequent values listed per field (default 15)")

    show = sub.add_parser("show-config", help="print the active extraction config versions")
    show.add_argument("--session-type", help="only this persona key")
    show.add_argument("--program-id", type=_uuid, help="only the configs of this program (uuid)")

    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = asyncio.run(_main(args))
    except (ValueError, RuntimeError) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
