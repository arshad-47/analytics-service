"""
Test support for the seeded configs: the ONE place that reads seed_config.sql.

The seeds of the leader categories, programs, `config` and `config_version` rows live in seed_config.sql (repo root)
and nowhere else: there is no Python copy of an id or of a config's content. The tests that need them go through
this module:

  * `seed_config(conn)`                         executes seed_config.sql (the way app/database/db.py does)
  * `load_seed_config(type, source, session)`   the version-1 JSON of one `-- CONFIG` block, as a fresh dict
  * `PERSONAS`                                  persona name -> ids and names read from the SQL (same keys the
                                                tests always used: leader_category_id, program_id, config_id,
                                                gforms_config_id, session_type, bot_id, bot_name, config_name,
                                                first_question_key / first_question_text of the Mitra config,
                                                extraction_config_id, program_name)
  * `mitra_config_v1` / `gforms_config_v1` / `extraction_config_v1`(persona)   fresh dicts by persona name
  * `extraction_config_id(session_type)`        the id of that persona's extraction config
  * `seed_config_blocks()`                      every CONFIG block with its parsed ids (for the SQL-file tests)
  * `SEED_CONFIG_COUNT` / `SEED_UPLOAD_CONFIG_COUNT` / `SEED_EXTRACTION_CONFIG_COUNT`   counts derived from the blocks
  * `SEED_NOTE_UPLOAD` / `SEED_NOTE_EXTRACTION` the seeded change_note of each config type

The file format it parses (kept regular on purpose; tests/test_seed_config.py checks it):

    -- PERSONA <persona name> <session_type>        followed by the leader_category and programs INSERTs
    -- CONFIG <config_type> <config_source> <session_type>
    INSERT INTO config ... SELECT '<id>'::uuid, '<source>', '<name>', '<type>', '<program id>'::uuid, ...
    INSERT INTO config_version ... SELECT '<id>'::uuid, 1, TRUE, $cfg$ <json> $cfg$::jsonb, 'seed', '<note>'
"""
import copy
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
SEED_CONFIG_FILE = ROOT / "seed_config.sql"

CONFIG_TYPE_UPLOAD = "feedback_upload"
CONFIG_TYPE_EXTRACTION = "feedback_extraction"
SOURCE_MITRA = "mitra"
SOURCE_GOOGLE_FORMS = "google_forms"
SOURCE_ANY = "any"

_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_STR = r"'((?:[^']|'')*)'"
_STR_OR_NULL = r"(?:'((?:[^']|'')*)'|NULL)"

_PERSONA_MARKER = re.compile(r"^-- PERSONA (.+) (\S+)$", re.MULTILINE)
_CONFIG_MARKER = re.compile(r"^-- CONFIG (\S+) (\S+) (\S+)$", re.MULTILINE)
_BLOCK_END = re.compile(r"^-- (?:PERSONA|CONFIG) |^-- =====", re.MULTILINE)
_LEADER_ROW = re.compile(
    rf"INSERT INTO leader_category .*?SELECT '({_UUID})'::uuid, {_STR}, {_STR}\n", re.DOTALL)
_PROGRAM_ROW = re.compile(
    rf"INSERT INTO programs .*?SELECT '({_UUID})'::uuid, '({_UUID})'::uuid, {_STR}, {_STR}\n", re.DOTALL)
_CONFIG_ROW = re.compile(
    rf"INSERT INTO config \(.*?\)\nSELECT '({_UUID})'::uuid, {_STR}, {_STR}, {_STR}, '({_UUID})'::uuid, {_STR}, NULL, "
    rf"{_STR}, {_STR_OR_NULL}, {_STR_OR_NULL}\n", re.DOTALL)
_VERSION_ROW = re.compile(
    rf"INSERT INTO config_version .*?SELECT '({_UUID})'::uuid, 1, TRUE, \$cfg\$\n(.*?)\n\$cfg\$::jsonb, {_STR}, {_STR}\n",
    re.DOTALL)


def _unq(text: Optional[str]) -> Optional[str]:
    return None if text is None else text.replace("''", "'")


def seed_config_sql() -> str:
    return SEED_CONFIG_FILE.read_text(encoding="utf-8")


async def seed_config(conn) -> None:
    """Executes seed_config.sql on `conn` (what initialize_schema does at every start)."""
    await conn.execute(seed_config_sql())


def _blocks_of(marker: "re.Pattern[str]", text: str):
    ends = [m.start() for m in _BLOCK_END.finditer(text)]
    for m in marker.finditer(text):
        end = min([e for e in ends if e > m.start()] + [len(text)])
        yield m, text[m.end():end]


def _parse() -> Dict[str, Any]:
    text = seed_config_sql()
    personas: Dict[str, Dict[str, Any]] = {}
    for m, body in _blocks_of(_PERSONA_MARKER, text):
        leader = _LEADER_ROW.search(body)
        program = _PROGRAM_ROW.search(body)
        if leader is None or program is None:
            raise ValueError(f"seed_config.sql: PERSONA block {m.group(1)!r} lacks a leader_category or programs row")
        personas[m.group(1)] = {
            "session_type": m.group(2),
            "leader_category_id": leader.group(1),
            "leader_category_name": _unq(leader.group(2)),
            "tenant_code": _unq(leader.group(3)),
            "program_id": program.group(1),
            "program_name": _unq(program.group(3)),
        }
        if program.group(2) != leader.group(1):
            raise ValueError(f"seed_config.sql: program of {m.group(1)!r} does not point at its leader category")
    blocks: List[Dict[str, Any]] = []
    for m, body in _blocks_of(_CONFIG_MARKER, text):
        row = _CONFIG_ROW.search(body)
        version = _VERSION_ROW.search(body)
        if row is None or version is None:
            raise ValueError(f"seed_config.sql: CONFIG block {m.group(0)!r} is not regular")
        blocks.append({
            "config_type": m.group(1), "config_source": m.group(2), "session_type": m.group(3),
            "id": row.group(1), "source_in_row": _unq(row.group(2)), "name": _unq(row.group(3)),
            "type_in_row": _unq(row.group(4)), "program_id": row.group(5), "tenant_code": _unq(row.group(6)),
            "session_type_in_row": _unq(row.group(7)), "bot_id": _unq(row.group(8)), "bot_name": _unq(row.group(9)),
            "version_config_id": version.group(1), "config": json.loads(version.group(2)),
            "created_by": _unq(version.group(3)), "change_note": _unq(version.group(4)),
        })
    return {"personas": personas, "blocks": blocks}


_PARSED = _parse()


def seed_config_blocks() -> List[Dict[str, Any]]:
    """Every `-- CONFIG` block of the file (parsed ids, names, JSON as a dict); fresh copies."""
    return copy.deepcopy(_PARSED["blocks"])


def _find(config_type: str, config_source: str, session_type: str) -> Dict[str, Any]:
    for block in _PARSED["blocks"]:
        if (block["config_type"], block["config_source"], block["session_type"]) == (
                config_type, config_source, session_type):
            return block
    raise KeyError(f"seed_config.sql has no CONFIG block {config_type} {config_source} {session_type}")


def load_seed_config(config_type: str, config_source: str, session_type: str) -> Dict[str, Any]:
    """The version-1 config JSON of the `-- CONFIG <type> <source> <session_type>` block, as a fresh dict."""
    return copy.deepcopy(_find(config_type, config_source, session_type)["config"])


def _build_personas() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for name, p in _PARSED["personas"].items():
        st = p["session_type"]
        mitra = _find(CONFIG_TYPE_UPLOAD, SOURCE_MITRA, st)
        first_question = mitra["config"]["questions"][0]
        gforms = next((b for b in _PARSED["blocks"] if (b["config_type"], b["config_source"], b["session_type"])
                       == (CONFIG_TYPE_UPLOAD, SOURCE_GOOGLE_FORMS, st)), None)
        out[name] = {
            "leader_category_id": p["leader_category_id"],
            "program_id": p["program_id"],
            "program_name": p["program_name"],
            "config_id": mitra["id"],
            "gforms_config_id": gforms["id"] if gforms is not None else None,
            "extraction_config_id": _find(CONFIG_TYPE_EXTRACTION, SOURCE_ANY, st)["id"],
            "session_type": st,
            "bot_id": mitra["bot_id"],
            "bot_name": mitra["bot_name"],
            "config_name": mitra["name"],
            "first_question_key": first_question["key"],
            "first_question_text": first_question["text"],
        }
    return out


PERSONAS: Dict[str, Dict[str, Any]] = _build_personas()
SESSION_TYPE_TO_PERSONA: Dict[str, str] = {p["session_type"]: name for name, p in PERSONAS.items()}
GFORMS_PERSONAS = tuple(name for name, p in PERSONAS.items() if p["gforms_config_id"] is not None)
FEEDBACK_PROGRAM_NAME = next(iter(PERSONAS.values()))["program_name"]
# counts DERIVED from the parsed seed (no test that is not about the seed content repeats a number of the file)
SEED_CONFIG_COUNT = len(_PARSED["blocks"])
SEED_UPLOAD_CONFIG_COUNT = sum(1 for b in _PARSED["blocks"] if b["config_type"] == CONFIG_TYPE_UPLOAD)
SEED_EXTRACTION_CONFIG_COUNT = sum(1 for b in _PARSED["blocks"] if b["config_type"] == CONFIG_TYPE_EXTRACTION)
SEED_NOTE_UPLOAD = _find(CONFIG_TYPE_UPLOAD, SOURCE_MITRA, "Youth_Voice")["change_note"]
SEED_NOTE_EXTRACTION = _find(CONFIG_TYPE_EXTRACTION, SOURCE_ANY, "Youth_Voice")["change_note"]


def mitra_config_v1(persona: str) -> Dict[str, Any]:
    return load_seed_config(CONFIG_TYPE_UPLOAD, SOURCE_MITRA, PERSONAS[persona]["session_type"])


def gforms_config_v1(persona: str) -> Dict[str, Any]:
    if persona not in GFORMS_PERSONAS:
        raise KeyError(f"persona {persona!r} has no Google Forms definition")
    return load_seed_config(CONFIG_TYPE_UPLOAD, SOURCE_GOOGLE_FORMS, PERSONAS[persona]["session_type"])


def extraction_config_v1(persona: str) -> Dict[str, Any]:
    return load_seed_config(CONFIG_TYPE_EXTRACTION, SOURCE_ANY, PERSONAS[persona]["session_type"])


def extraction_config_id(session_type: str) -> str:
    return _find(CONFIG_TYPE_EXTRACTION, SOURCE_ANY, session_type)["id"]
