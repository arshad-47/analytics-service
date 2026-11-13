#!/usr/bin/env python3
"""
SL Prod DB (via Metabase API Key / Session) -> Delta Sync -> Analytics Upload API -> Kafka -> Analytics DB + Temporal

Pipeline Flow:
1. Load config-driven programs from programs_config.json.
2. Query Analytics DB (via local PostgreSQL / asyncpg) to fetch existing (submission_id, session_id).
3. Query SL Prod DB (via Metabase API token/session) to fetch candidate (submission_id, session_id).
4. Delta Set = Candidate IDs - Existing IDs (Strictly fetch only new/missing IDs).
5. For Delta IDs, fetch full submission records from SL Prod DB.
6. Enrich, apply geo-aliasing (mapping.json), clean media URLs, and map tenant to 'mitra'.
7. Package into CSV format expected by Analytics Upload API and POST to /v1/upload/.
8. Upload API handles validation, GCS storage, Kafka publishing, DB persistence, and Temporal orchestration.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator, Optional, Set, Tuple

try:
    import asyncpg
except ImportError:
    asyncpg = None

try:
    import urllib.request
    import urllib.error
    import urllib.parse
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DEFAULT_LOG_DIR = (PROJECT_ROOT / "logs") if (PROJECT_ROOT / "logs").exists() else (BASE_DIR / "logs")

try:
    from dotenv import load_dotenv
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        env_file = BASE_DIR.parent / ".env"
    load_dotenv(env_file)
except ImportError:
    pass

UTC = timezone.utc
WM_KEY = "all"


# --------------------------------------------------------------------------- helpers
def parse_dt(s: str) -> datetime:
    s_clean = s.strip()
    try:
        dt = datetime.fromisoformat(s_clean.replace("Z", "+00:00"))
    except ValueError:
        try:
            from dateutil import parser as dt_parser
            dt = dt_parser.parse(s_clean)
        except Exception:
            raise
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def iso_z(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = parse_dt(dt)
        except Exception:
            return dt.strip() or None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean_empty(value: Any) -> Any:
    """Normalize empty values ('', whitespace, [], {}) to None."""
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, (list, dict, tuple)):
        return value if len(value) else None
    return value


def get_first_non_empty(*vals: Any) -> Any:
    """Return the first non-null/non-empty value from candidates, or None."""
    for v in vals:
        cleaned = clean_empty(v)
        if cleaned is not None:
            return cleaned
    return None


def to_int(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, str) and re.fullmatch(r"\s*\d+\s*", v):
        return int(v)
    return None


def to_list(v: Any) -> Optional[list[str]]:
    """Convert text / JSON-string / list / pipe-delimited string to list[str] or None."""
    v = clean_empty(v)
    if v is None:
        return None
    if isinstance(v, str):
        if v[0] in "[{":
            try:
                return to_list(json.loads(v))
            except ValueError:
                pass
        if " | " in v:
            v = [p.strip() for p in v.split(" | ")]
        else:
            v = [re.sub(r"^\s*(?:[-*•]|\d+[.)])\s*", "", p) for p in re.split(r"\r?\n+", v)]
    if isinstance(v, dict):
        v = list(v.values())
    out = [str(x).strip() for x in v if clean_empty(x) is not None]
    return out or None


def as_dict(v: Any) -> dict:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return {}
    return v if isinstance(v, dict) else {}


def format_location_with_state(location: Optional[str], state: Optional[str]) -> str:
    """Format location string so that state is encoded as the final comma-separated component.

    The Analytics Upload API (row_to_json) derives state from location.split(',')[-1].
    Ensuring 'Location, State' guarantees the detected state is preserved in Analytics.
    """
    loc = (location or "").strip()
    st = (state or "").strip()
    if not st:
        return loc
    if not loc:
        return st
    if loc.lower().endswith(st.lower()):
        return loc
    return f"{loc}, {st}"


def params_of(other_params: Any) -> tuple[dict, dict]:
    """Parse other_params JSON into (raw_params, survey_params).

    raw_params  – the top-level other_params dict.
    survey_params – the nested english_json dict (if present), otherwise raw_params.
    """
    raw_params = as_dict(other_params)
    english_json = raw_params.get("english_json")
    return raw_params, (english_json if isinstance(english_json, dict) else raw_params)


def get_nested_param(survey_params: dict, raw_params: dict, key: str) -> Any:
    """Look for key in survey_params first; fallback to raw_params."""
    return get_first_non_empty(survey_params.get(key), raw_params.get(key))


def extract_org_from_params(survey_params: dict, raw_params: dict) -> Optional[str]:
    """Extract organization name from various possible survey response keys."""
    org = get_first_non_empty(
        survey_params.get("organization"), raw_params.get("organization"),
        survey_params.get("organisation"), raw_params.get("organisation"),
        survey_params.get("org"), raw_params.get("org"),
        survey_params.get("org_name"), raw_params.get("org_name"),
        survey_params.get("partner"), raw_params.get("partner"),
        survey_params.get("partner_name"), raw_params.get("partner_name"),
    )
    if isinstance(org, dict):
        return get_first_non_empty(org.get("organization"), org.get("organisation"), org.get("name"), org.get("org"))
    return get_first_non_empty(org)


# --------------------------------------------------------------------------- config
class Config:
    def __init__(self) -> None:
        e = os.environ
        csv_fn = lambda k, d="": [x.strip() for x in e.get(k, d).split(",") if x.strip()]
        self.metabase_url = e.get("METABASE_URL", "").rstrip("/")
        if self.metabase_url and not self.metabase_url.startswith(("http://", "https://")):
            self.metabase_url = f"https://{self.metabase_url}"
        self.metabase_api_key = e.get("METABASE_API_KEY", "").strip()
        self.metabase_database_id = int(e.get("METABASE_DATABASE_ID")) if e.get("METABASE_DATABASE_ID") else None
        self.db_dsn = e.get("SL_DB_DSN", "")
        self.analytics_db_url = e.get("DATABASE_URL", "").strip()
        if not self.analytics_db_url:
            raise ValueError("DATABASE_URL environment variable is required for Analytics DB.")
        self.api_base_url = e.get("ANALYTICS_API_BASE_URL", "http://localhost:8000").rstrip("/")
        self.auth_token = e.get("AUTH_TOKEN", "").strip()

        self.programs_config_path = Path(e.get("PROGRAMS_CONFIG_PATH") or (BASE_DIR / "programs_config.json"))
        self.tenants = csv_fn("TENANT_SLUGS", "shikshalokamstaging")
        if not self.tenants:
            raise ValueError("TENANT_SLUGS environment variable must be set and non-empty.")
        if len(self.tenants) > 1:
            raise ValueError(f"Multi-tenant sync is not supported; TENANT_SLUGS must specify exactly one tenant slug, got: {self.tenants}")
        self.tenant_slug = self.tenants[0]

        tenant_map_raw = e.get("TENANT_CODE_MAP", "{}") or '{"shikshalokamstaging": "mitra"}'
        try:
            self.tenant_code_map = json.loads(tenant_map_raw)
        except Exception as exc:
            raise ValueError(f"Failed to parse TENANT_CODE_MAP as JSON: {exc}") from exc

        if self.tenant_slug not in self.tenant_code_map:
            raise ValueError(f"Configured TENANT_SLUGS '{self.tenant_slug}' is not mapped in TENANT_CODE_MAP: {self.tenant_code_map}")
        self.target_tenant_code = self.tenant_code_map[self.tenant_slug]
        self.discussion_types = csv_fn("DISCUSSION_SESSION_TYPES", "shikshalokam_chaupal,guest-discussion")
        self.excluded_sources = csv_fn("EXCLUDE_PROFILE_SOURCES", "elevate")
        self.unknown_policy = e.get("UNKNOWN_POLICY", "skip").lower()
        self.batch_size = int(e.get("BATCH_SIZE") or e.get("MIGRATION_BATCH_SIZE", 200))
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be greater than zero.")
        self.redact_user = e.get("REDACT_USER_NAME", "false").lower() == "true"
        self.lock_file = Path(e.get("MIGRATE_LOCK_FILE", "/tmp/migration.lock"))
        self.log_dir = Path(e.get("MIGRATE_LOG_DIR") or e.get("LOG_DIR", DEFAULT_LOG_DIR))
        self.sent_cache_file = Path(e.get("MIGRATE_SENT_CACHE_FILE") or (self.log_dir / ".sent_submissions_cache.json"))

        story_cols_raw = e.get("STORY_CSV_COLUMN")
        self.story_columns = None
        if story_cols_raw:
            try:
                parsed = json.loads(story_cols_raw)
                if isinstance(parsed, list):
                    self.story_columns = parsed
            except Exception:
                pass

        # Defaults from env or fallbacks
        self.default_state = e.get("DEFAULT_STATE")
        self.default_district = e.get("DEFAULT_DISTRICT")
        self.default_organization = e.get("DEFAULT_ORGANIZATION")

        # Load program mappings if present
        self.programs_config = {}
        if self.programs_config_path.exists():
            try:
                self.programs_config = json.loads(self.programs_config_path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise RuntimeError(f"Could not load programs configuration from {self.programs_config_path}") from exc

        # Program defaults: prioritize programs_config.json "defaults", fallback to env or standard values
        cfg_defaults = self.programs_config.get("defaults", {})
        self.default_program_name = cfg_defaults.get("program_name") or e.get("DEFAULT_PROGRAM_NAME", "Shiksha Chaupals")
        self.default_leader_category_name = cfg_defaults.get("leader_category") or e.get("DEFAULT_LEADER_CATEGORY_NAME", "Women leader (WL)")

        # Collect configured session types across programs
        configured_st = []
        for _, p_conf in self.programs_config.get("programs", {}).items():
            for st in p_conf.get("session_types", []):
                if st and st not in configured_st:
                    configured_st.append(st)
        self.all_session_types = configured_st or (self.discussion_types + ["guest-mi-story", "guided-validation", "mi-story"])


# --------------------------------------------------------------------------- logging
class RunIdFilter(logging.Filter):
    """Logging filter that injects a unique execution run_id into every log message."""
    run_id = "-"

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = RunIdFilter.run_id
        return True


def setup_logging(cfg: Config) -> logging.Logger:
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | run=%(run_id)s | %(message)s")
    log = logging.getLogger("migration")
    log.setLevel(logging.INFO)
    flt = RunIdFilter()

    app_handler = RotatingFileHandler(cfg.log_dir / "migration.log", maxBytes=10_000_000, backupCount=10)
    app_handler.setLevel(logging.INFO)
    app_handler.setFormatter(fmt)
    app_handler.addFilter(flt)
    log.addHandler(app_handler)

    err_handler = RotatingFileHandler(cfg.log_dir / "migration.error.log", maxBytes=10_000_000, backupCount=10)
    err_handler.setLevel(logging.ERROR)
    err_handler.setFormatter(fmt)
    err_handler.addFilter(flt)
    log.addHandler(err_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)
    stream_handler.addFilter(flt)
    log.addHandler(stream_handler)

    return log


# --------------------------------------------------------------------------- geo detection
class GeoMapping:
    """Matches free-form text against state and district aliases defined in mapping.json."""
    def __init__(self, path: Path) -> None:
        data = json.loads(path.read_text(encoding="utf-8"))
        self.state_rx = {s: re.compile(r"\b(?:%s)\b" % "|".join(re.escape(a) for a in v["aliases"]))
                         for s, v in data.items()}
        self.aliases = sorted(
            ((re.compile(rf"\b{re.escape(a)}\b"), s, d) for s, v in data.items()
             for d, names in v["districts"].items() for a in names),
            key=lambda t: -len(t[0].pattern))

    def detect(self, *texts: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        state_only = None
        for t in texts:
            t = (t or "").lower()
            if not t:
                continue
            for rx, state, district in self.aliases:
                if rx.search(t):
                    return state, district
            for state, rx in self.state_rx.items():
                if rx.search(t) and not state_only:
                    state_only = state
        return state_only, None


class OrganizationResolver:
    """Resolves organization names using external organizations.json mapping."""
    def __init__(self, path: Path) -> None:
        self.state_district_map: dict[str, dict[str, str]] = {}
        self.keyword_rules: list[tuple[re.Pattern, str]] = []
        self.default_org = ""

        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError(f"Invalid organizations.json format in {path}: expected a JSON object.")
            self.state_district_map = data.get("state_district_map", {})
            self.default_org = data.get("default_organization", "")
            for rule in data.get("keyword_rules", []):
                pat = rule.get("pattern")
                target = rule.get("organization")
                if pat and target:
                    self.keyword_rules.append((re.compile(pat, re.I), target))

    def resolve(self, state: Optional[str], district: Optional[str],
                location: Optional[str], raw_org: Optional[str]) -> Optional[str]:
        # 1. Check keyword regex matches against raw_org or location text
        combined_text = f"{raw_org or ''} {location or ''}".strip()
        if combined_text:
            for pattern, org_name in self.keyword_rules:
                if pattern.search(combined_text):
                    return org_name

        # 2. Check state + district map
        if state and district:
            dist_map = self.state_district_map.get(state, {})
            if district in dist_map:
                return dist_map[district]

        # 3. Direct district lookup across states
        if district:
            for dist_map in self.state_district_map.values():
                if district in dist_map:
                    return dist_map[district]

        return self.default_org or None


# --------------------------------------------------------------------------- metabase client
class MetabaseClient:
    def __init__(self, cfg: Config, log: logging.Logger) -> None:
        self.cfg = cfg
        self.log = log
        self.api_key = cfg.metabase_api_key
        self.session_token = None
        self.db_id = cfg.metabase_database_id

    def login(self) -> None:
        if not self.api_key:
            raise RuntimeError("METABASE_API_KEY is not configured in environment.")
        self.log.info("Using configured Metabase API Key for queries.")
        if self.db_id is None:
            self.resolve_database_id()

    def get_headers(self) -> dict:
        headers = {}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        elif self.session_token:
            headers["X-Metabase-Session"] = self.session_token
        return headers

    def resolve_database_id(self) -> None:
        if self.cfg.metabase_database_id is None:
            raise RuntimeError("METABASE_DATABASE_ID is not configured in environment.")
        self.db_id = self.cfg.metabase_database_id

    def execute_query(self, sql: str) -> list[dict]:
        query_dict = {
            "database": self.db_id,
            "type": "native",
            "native": {"query": sql, "template-tags": {}},
            "parameters": []
        }
        url = f"{self.cfg.metabase_url}/api/dataset/json"
        form_data = urllib.parse.urlencode({"query": json.dumps(query_dict)}).encode("utf-8")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            **self.get_headers()
        }
        req = urllib.request.Request(url, data=form_data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    if data.get("status") == "failed" or "error" in data:
                        err_msg = data.get("error") or data
                        raise RuntimeError(f"Metabase query execution failed: {err_msg}")
                    if "data" in data and "rows" in data["data"]:
                        cols = [c["name"] for c in data["data"]["cols"]]
                        return [dict(zip(cols, row)) for row in data["data"]["rows"]]
                return []
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Metabase query execution failed (HTTP {exc.code}): {err_body}") from exc


# --------------------------------------------------------------------------- submitted ID persistence & consumer sync tracker
class SubmittedIdTracker:
    """
    Tracks submitted submission IDs locally until consumer persistence is confirmed in Postgres.
    Prevents re-uploading submissions during Kafka consumer lag when csv_uploads is already marked 'success'
    but the Kafka consumer has not yet committed rows to the submissions table.
    """

    def __init__(self, cache_path: Path, ttl_hours: float = 2.0):
        self.cache_path = cache_path
        self.ttl_seconds = ttl_hours * 3600

    def load_unconfirmed_ids(self, confirmed_db_ids: Set[str], log: logging.Logger) -> Set[str]:
        """
        Loads tracked IDs, purges those confirmed in Postgres, purges expired IDs (> TTL),
        and returns the set of unconfirmed IDs to exclude from candidate delta.
        """
        if not self.cache_path.exists():
            return set()

        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return set()
        except Exception as exc:
            log.warning("Could not read submitted IDs cache (%s): %s", self.cache_path, exc)
            return set()

        now_ts = datetime.now(timezone.utc).timestamp()
        cleaned: dict[str, float] = {}
        unconfirmed: set[str] = set()

        for sub_id, sent_ts in data.items():
            sub_id_str = str(sub_id)
            if sub_id_str in confirmed_db_ids:
                # Confirmed in PostgreSQL submissions table by consumer — safe to purge from tracker
                continue
            if (now_ts - sent_ts) < self.ttl_seconds:
                cleaned[sub_id_str] = sent_ts
                unconfirmed.add(sub_id_str)
            else:
                log.info(
                    "Tracked submission ID %s expired after %.1f hours without DB confirmation; eligible for retry.",
                    sub_id_str,
                    (now_ts - sent_ts) / 3600,
                )

        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(cleaned, indent=2), encoding="utf-8")
        except Exception as exc:
            log.warning("Could not update submitted IDs cache (%s): %s", self.cache_path, exc)

        return unconfirmed

    def record_sent_ids(self, sub_ids: list[str], log: logging.Logger) -> None:
        """Records successfully uploaded submission IDs with the current UTC timestamp."""
        if not sub_ids:
            return

        now_ts = datetime.now(timezone.utc).timestamp()
        existing: dict[str, float] = {}

        if self.cache_path.exists():
            try:
                data = json.loads(self.cache_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    existing = data
            except Exception:
                existing = {}

        for sid in sub_ids:
            existing[str(sid)] = now_ts

        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
            log.info("Recorded %d submitted ID(s) in persistent in-flight cache.", len(sub_ids))
        except Exception as exc:
            log.warning("Failed to persist sent IDs to cache (%s): %s", self.cache_path, exc)


# --------------------------------------------------------------------------- analytics db fetcher
def check_pending_migration_uploads(cfg: Config, log: logging.Logger) -> list[dict]:
    """
    Check if any previous migration CSV uploads are still pending or in progress
    in the Analytics DB tracker table (csv_uploads).
    Returns a list of matching records. Raises on connection/query failure.
    """
    db_url = cfg.analytics_db_url
    if not db_url:
        raise RuntimeError("No DATABASE_URL configured for Analytics DB.")

    if not asyncpg:
        raise RuntimeError("asyncpg is required to connect to Analytics PostgreSQL DB, but it is not installed.")

    try:
        async def _check():
            conn = await asyncpg.connect(db_url, timeout=10)
            try:
                rows = await conn.fetch(
                    """
                    SELECT id, file_name, status, updated_at
                    FROM csv_uploads
                    WHERE file_name LIKE 'migration_%'
                      AND (
                          status IN ('pending', 'in_progress')
                          OR (status = 'success' AND updated_at > NOW() - interval '2 minutes')
                      )
                      AND updated_at > NOW() - interval '2 hours'
                    ORDER BY id ASC
                    """
                )
                return [dict(r) for r in rows]
            finally:
                await conn.close()
        return asyncio.run(_check())
    except Exception as exc:
        log.error("Failed to query Analytics DB for active csv_uploads: %s", exc)
        raise


def fetch_existing_analytics_data(cfg: Config, log: logging.Logger) -> Tuple[Set[str], Optional[datetime]]:
    """
    Fetches already ingested submission IDs and the latest update timestamp from Analytics DB.
    Returns (set_of_submission_ids, max_updated_at).
    """
    existing_ids: Set[str] = set()
    latest_ts: Optional[datetime] = None

    db_url = cfg.analytics_db_url
    if not db_url:
        raise RuntimeError("No DATABASE_URL configured for Analytics DB.")

    if not asyncpg:
        raise RuntimeError("asyncpg is required to connect to Analytics PostgreSQL DB, but it is not installed.")

    try:
        target_tenant = cfg.target_tenant_code
        async def _fetch():
            conn = await asyncpg.connect(db_url, timeout=10)
            try:
                rows = await conn.fetch(
                    "SELECT submission_id, status FROM submissions WHERE tenant_code = $1",
                    target_tenant,
                )
                valid_persisted_ids = {
                    str(r["submission_id"]) for r in rows
                    if r["submission_id"] and (r["status"] is None or r["status"] != "failed")
                }
                failed_db_ids = {
                    str(r["submission_id"]) for r in rows
                    if r["submission_id"] and r["status"] == "failed"
                }
                return valid_persisted_ids, failed_db_ids
            finally:
                await conn.close()
        existing_ids, failed_ids = asyncio.run(_fetch())
    except Exception as exc:
        log.error("Could not query Analytics DB directly via asyncpg: %s", exc)
        raise

    log.info("Fetched %d existing valid submission IDs (%d failed) from Analytics PostgreSQL DB for tenant '%s'.",
             len(existing_ids), len(failed_ids), target_tenant)

    # Reconcile with locally tracked in-flight submitted IDs to guard against Kafka consumer lag.
    # Exclude IDs sent in the last N hours unless they show up as failed in DB.
    tracker = SubmittedIdTracker(cfg.sent_cache_file)
    unconfirmed_ids = tracker.load_unconfirmed_ids(existing_ids | failed_ids, log)
    # If any unconfirmed ID is actually marked as failed in DB, do not suppress it; allow retry
    in_flight_to_exclude = unconfirmed_ids - failed_ids
    if in_flight_to_exclude:
        log.info("Holding %d recently submitted ID(s) in-flight (waiting for Kafka consumer persistence).", len(in_flight_to_exclude))
        existing_ids = existing_ids | in_flight_to_exclude

    log.info("Total effective existing submission IDs (persisted + in-flight): %d.", len(existing_ids))
    return existing_ids, latest_ts


# --------------------------------------------------------------------------- SQL formatting
def format_sql(raw_sql: str, params: dict) -> str:
    sql = raw_sql
    # Format window_start / window_end
    if params.get("window_start"):
        ws = params["window_start"].isoformat()
        sql = sql.replace("%(window_start)s::timestamptz", f"'{ws}'::timestamptz")
        sql = sql.replace("%(window_start)s::text", f"'{ws}'")
    else:
        sql = sql.replace("%(window_start)s::timestamptz", "NULL::timestamptz")
        sql = sql.replace("%(window_start)s::text", "NULL")

    if params.get("window_end"):
        we = params["window_end"].isoformat()
        sql = sql.replace("%(window_end)s::timestamptz", f"'{we}'::timestamptz")
        sql = sql.replace("%(window_end)s::text", f"'{we}'")
    else:
        sql = sql.replace("%(window_end)s::timestamptz", "NULL::timestamptz")
        sql = sql.replace("%(window_end)s::text", "NULL")

    def format_text_array(name: str) -> str:
        values = params.get(name, [])
        if not values:
            return "ARRAY[]::text[]"
        escaped_values = (str(value).replace("'", "''") for value in values)
        return "ARRAY[" + ", ".join(f"'{value}'" for value in escaped_values) + "]::text[]"

    for name in ("tenants", "submission_ids", "discussion_types", "session_types", "excluded_sources"):
        sql = sql.replace(f"%({name})s::text[]", format_text_array(name))
    return sql


def fetch_candidate_ids(source: Any, cfg: Config) -> list[dict]:
    sql_path = BASE_DIR / "candidate_ids.sql"
    sql = sql_path.read_text(encoding="utf-8")
    params = {
        "tenants": cfg.tenants,
        "session_types": cfg.all_session_types,
        "discussion_types": cfg.discussion_types,
        "excluded_sources": cfg.excluded_sources,
    }
    formatted_sql = format_sql(sql, params)
    if isinstance(source, MetabaseClient):
        return source.execute_query(formatted_sql)
    else:
        raise RuntimeError("Direct PostgreSQL connection to SL Prod DB is not supported without MetabaseClient.")


# --------------------------------------------------------------------------- transformation & classification
def classify_by_config(row: dict, cfg: Config) -> tuple[Optional[str], str, str]:
    """Returns (submission_type, program_name, leader_category) using programs_config.json session_types or fallbacks."""
    session_type = row.get("session_type")

    # 1. Match against configured programs by session_type
    programs = cfg.programs_config.get("programs", {})
    for _, p_conf in programs.items():
        sub_type = p_conf.get("submission_type")
        s_types = p_conf.get("session_types", [])
        if session_type and session_type in s_types:
            return sub_type, p_conf.get("name", cfg.default_program_name), p_conf.get("leader_category", cfg.default_leader_category_name)

    # 2. Fall back to standard discussion session types
    if session_type in cfg.discussion_types:
        return "discussion", cfg.default_program_name, cfg.default_leader_category_name

    if cfg.unknown_policy == "story":
        return "story", cfg.default_program_name, cfg.default_leader_category_name
    return None, cfg.default_program_name, cfg.default_leader_category_name


def clean_image_urls(urls: Optional[list]) -> list[str]:
    cleaned = []
    for u in urls or []:
        if not u or not isinstance(u, str):
            continue
        u = u.strip()
        if u in ("https://mohini-static.shikshalokam.org/", "http://mohini-static.shikshalokam.org/"):
            continue
        if re.search(r"\.(jpe?g|png|webp|gif|bmp)$", u, re.IGNORECASE):
            cleaned.append(u)
        elif "/" in u and not u.endswith("/"):
            path_part = u.split("://", 1)[-1].split("/", 1)[-1] if "/" in u.split("://", 1)[-1] else ""
            if path_part.strip():
                cleaned.append(u)
    return cleaned


def enrich_row(row: dict, geo: GeoMapping, org_resolver: OrganizationResolver, cfg: Config, sub_type: str) -> dict:
    raw_params, survey_params = params_of(row.get("other_params"))
    loc = get_first_non_empty(
        survey_params.get("location"), raw_params.get("location"),
        survey_params.get("user_location"), raw_params.get("user_location"),
        survey_params.get("userLocation"), raw_params.get("userLocation"),
        survey_params.get("place"), raw_params.get("place"),
        row.get("story_location"),
    )
    raw_district = get_first_non_empty(
        survey_params.get("district"), raw_params.get("district"),
        survey_params.get("user_district"), raw_params.get("user_district"),
        survey_params.get("userDistrict"), raw_params.get("userDistrict"),
        row.get("story_district"),
    )
    raw_state = get_first_non_empty(
        survey_params.get("state"), raw_params.get("state"),
        survey_params.get("user_state"), raw_params.get("user_state"),
        survey_params.get("userState"), raw_params.get("userState"),
        row.get("story_state"),
    )
    raw_org = extract_org_from_params(survey_params, raw_params)
    d_state, d_district = geo.detect(
        loc, raw_district, raw_state,
        survey_params.get("block"), raw_params.get("block"),
        survey_params.get("cluster"), raw_params.get("cluster"),
        survey_params.get("school_name"), raw_params.get("school_name"),
        raw_org,
    )
    state = get_first_non_empty(d_state, raw_state, cfg.default_state)
    district = get_first_non_empty(d_district, raw_district, cfg.default_district)
    derived = org_resolver.resolve(state or d_state, district or d_district, loc, raw_org)
    org = get_first_non_empty(derived, raw_org, cfg.default_organization) if sub_type == "discussion" else get_first_non_empty(raw_org, derived, cfg.default_organization)

    images = clean_image_urls(to_list(row.get("image_urls") or get_first_non_empty(survey_params.get("imageUrls"), raw_params.get("imageUrls"))))
    pdf_url = clean_empty(row.get("pdf_url")) or get_first_non_empty(survey_params.get("pdfUrl"), raw_params.get("pdfUrl"))

    return {
        "title": row.get("title") or "Untitled",
        "user_name": "[REDACTED]" if cfg.redact_user else get_first_non_empty(survey_params.get("user_name"), raw_params.get("user_name")),
        "designation": get_first_non_empty(survey_params.get("designation"), survey_params.get("role")),
        "location": loc,
        "district": district,
        "state": state,
        "organization": org,
        "created_at": iso_z(row.get("story_created_at")),
        "images": images,
        "pdf_url": pdf_url,
        "session_id": row.get("session_id"),
        "submission_id": str(row.get("chat_session_id")),
        "survey_params": survey_params,
        "raw_params": raw_params,
    }


# --------------------------------------------------------------------------- upload api client
def post_csv_to_upload_api(cfg: Config, report_type: str, program_name: str, leader_category: str,
                           csv_content: str, filename: str, tenant_code: str, log: logging.Logger) -> bool:
    """
    Submits generated CSV batch to Analytics API (/v1/upload/).
    Upload API validates, publishes to Kafka, persists to DB, and starts Temporal.
    """
    url = f"{cfg.api_base_url}/v1/upload/"
    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex[:16]}"
    body = io.BytesIO()

    fields = {
        "report_type": report_type,
        "program_name": program_name,
        "leader_category": leader_category,
        "tenant_code": tenant_code,
    }
    for name, value in fields.items():
        body.write(f"--{boundary}\r\n".encode("utf-8"))
        body.write(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        body.write(f"{value}\r\n".encode("utf-8"))

    # Attach file
    body.write(f"--{boundary}\r\n".encode("utf-8"))
    body.write(f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode("utf-8"))
    body.write(b"Content-Type: text/csv\r\n\r\n")
    body.write(csv_content.encode("utf-8"))
    body.write(b"\r\n")
    body.write(f"--{boundary}--\r\n".encode("utf-8"))

    req = urllib.request.Request(
        url,
        data=body.getvalue(),
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Authorization": f"Bearer {cfg.auth_token}",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            res_data = json.loads(resp.read().decode("utf-8"))
            log.info("Uploaded %s CSV successfully: %s", report_type, res_data)
            return True
    except Exception as exc:
        log.error("Failed to POST %s CSV to Upload API (%s): %s", report_type, url, exc)
        return False


def build_story_csv_row(enriched_row: dict, raw_row: dict, expected_columns: Optional[list[str]] = None) -> dict:
    survey_params, raw_params = enriched_row["survey_params"], enriched_row["raw_params"]
    challenges = get_first_non_empty(to_list(raw_row.get("project_challenges")), to_list(survey_params.get("problem_statement")))
    row = {
        "id": enriched_row["submission_id"],
        "Title": enriched_row["title"],
        "User name": enriched_row["user_name"] or "",
        "Designation": enriched_row["designation"] or "",
        "Location": format_location_with_state(enriched_row["location"], enriched_row["state"]),
        "District": enriched_row["district"] or "",
        "Organization": enriched_row["organization"] or "",
        "Report Created At": enriched_row["created_at"],
        "Objective": raw_row.get("objective") or "",
        "Challenges": "; ".join(challenges) if challenges else "",
        "Action Steps": "; ".join(to_list(raw_row.get("action_steps")) or []),
        "Impact": raw_row.get("impact") or "",
        "Duration": get_first_non_empty(survey_params.get("duration")) or "",
        "Blurb": raw_row.get("blurb") or "",
        "Content": raw_row.get("content") or "",
        "Images": json.dumps(enriched_row["images"]),
        "Pdf": enriched_row["pdf_url"] or "",
        "Transcript Link": enriched_row["session_id"] or "",
        "Session ID": enriched_row["session_id"] or "",
    }
    # Only include masked columns if expected by the upload schema (e.g. STORY_CSV_COLUMN)
    if expected_columns:
        cols_lower = {c.lower(): c for c in expected_columns}
        if "masked_blurb" in cols_lower:
            row[cols_lower["masked_blurb"]] = ""
        if "masked_content" in cols_lower:
            row[cols_lower["masked_content"]] = ""
    return row


def build_discussion_csv_row(enriched_row: dict, raw_row: dict) -> dict:
    survey_params, raw_params = enriched_row["survey_params"], enriched_row["raw_params"]
    pri = as_dict(get_nested_param(survey_params, raw_params, "pri_member"))
    school = as_dict(get_nested_param(survey_params, raw_params, "school_representative"))

    # Extract participant counts from chat survey params if present
    men_cnt = to_int(get_nested_param(survey_params, raw_params, "men")) or to_int(get_nested_param(survey_params, raw_params, "men_count")) or 0
    women_cnt = to_int(get_nested_param(survey_params, raw_params, "women")) or to_int(get_nested_param(survey_params, raw_params, "women_count")) or 0
    children_cnt = to_int(get_nested_param(survey_params, raw_params, "children")) or to_int(get_nested_param(survey_params, raw_params, "children_count")) or 0

    # Total participant count: check all common survey aliases or sum components
    raw_total = (
        to_int(get_nested_param(survey_params, raw_params, "participants_count"))
        or to_int(get_nested_param(survey_params, raw_params, "participant_count"))
        or to_int(get_nested_param(survey_params, raw_params, "total_participants"))
        or to_int(get_nested_param(survey_params, raw_params, "participants"))
        or to_int(get_nested_param(survey_params, raw_params, "no_of_participants"))
        or to_int(get_nested_param(survey_params, raw_params, "total"))
        or to_int(get_nested_param(survey_params, raw_params, "attendees"))
    )
    total_cnt = raw_total if (raw_total is not None and raw_total > 0) else (men_cnt + women_cnt + children_cnt)
    # If participant count is missing or non-positive, do not fabricate 1 count (avoid injecting false KPIs).
    # Return None so pipeline orchestrator can skip and log the uncounted discussion row.
    if total_cnt <= 0:
        return None

    disc_challenges = get_first_non_empty(
        to_list(get_nested_param(survey_params, raw_params, "challenges_faced")),
        to_list(get_nested_param(survey_params, raw_params, "challenges")),
        to_list(get_nested_param(survey_params, raw_params, "challenge")),
        to_list(raw_row.get("project_challenges")),
    )
    disc_solutions = get_first_non_empty(
        to_list(get_nested_param(survey_params, raw_params, "solutions_discussed")),
        to_list(get_nested_param(survey_params, raw_params, "solutions")),
        to_list(get_nested_param(survey_params, raw_params, "solution")),
    )

    return {
        "id": enriched_row["submission_id"],
        "Title": enriched_row["title"],
        "User name": enriched_row["user_name"] or "",
        "User Location": format_location_with_state(enriched_row["location"], enriched_row["state"]),
        "District": enriched_row["district"] or "",
        "Participant Count": str(total_cnt),
        "Men": str(men_cnt),
        "Women": str(women_cnt),
        "Children": str(children_cnt),
        "Date of Discussion": enriched_row["created_at"],
        "Organization": enriched_row["organization"] or "",
        "Challenges": "; ".join(disc_challenges) if disc_challenges else "",
        "Solutions": "; ".join(disc_solutions) if disc_solutions else "",
        "Author": enriched_row["user_name"] or "",
        "Language": "en",
        "Report Created At": enriched_row["created_at"],
        "Transcript Link": enriched_row["session_id"] or "",
        "Image Urls": json.dumps(enriched_row["images"]),
        "PDF Urls": enriched_row["pdf_url"] or "",
        "Session ID": enriched_row["session_id"] or "",
        "PRI Member Name": pri.get("name") or "",
        "PRI Member Designation": pri.get("designation") or "",
        "School Representative Name": school.get("name") or "",
        "School Representative Designation": school.get("designation") or "",
    }


# --------------------------------------------------------------------------- pipeline orchestrator
def run_pipeline(cfg: Config, source: Any, geo: GeoMapping, org_resolver: OrganizationResolver,
                 log: logging.Logger, run_id: str, dry_run: bool) -> str:
    # Check for in-flight migration uploads to avoid racing async ingestion
    pending_uploads = check_pending_migration_uploads(cfg, log)
    if pending_uploads:
        pending_ids = [str(u["id"]) for u in pending_uploads]
        if dry_run:
            log.warning("Notice: Found %d active migration upload(s) still in progress in Analytics DB (IDs: %s). "
                        "Continuing dry-run preview...", len(pending_uploads), ", ".join(pending_ids))
        else:
            log.warning("Previous migration upload(s) still in progress in Analytics DB (IDs: %s). "
                        "Exiting now to prevent duplicate re-uploads while background ingestion completes.",
                        ", ".join(pending_ids))
            return "success"

    log.info("Step 1: Querying existing submission IDs and watermark in Analytics DB...")
    existing_ids, latest_analytics_ts = fetch_existing_analytics_data(cfg, log)
    log.info("Found %d existing submission IDs in Analytics DB.", len(existing_ids))

    log.info("Step 2: Fetching candidate IDs from SL [MItra] DB...")
    candidates = fetch_candidate_ids(source, cfg)
    candidate_id_set = {str(r["submission_id"]) for r in candidates if r.get("submission_id")}
    log.info("Found %d candidate submission IDs in SL Prod DB.", len(candidate_id_set))

    # Delta:
    # Strictly new submissions: ID not present in Analytics DB
    new_ids = candidate_id_set - existing_ids
    delta_ids = sorted(list(new_ids), key=lambda x: int(x) if x.isdigit() else x)
    log.info("Step 3: Computed Delta: %d new submission(s) to fetch and ingest.", len(delta_ids))

    if not delta_ids:
        log.info("No delta to process. Analytics DB is already fully synchronized!")
        return "success"

    # Fetch full data for delta_ids in batches
    sql_path = BASE_DIR / "submission.sql"
    sql = sql_path.read_text(encoding="utf-8")

    story_rows = []
    discussion_rows = []
    sent_tuples = []

    # Process in chunks of batch_size
    chunk_size = cfg.batch_size
    total_raw_rows = 0
    skipped_classification = 0
    for i in range(0, len(delta_ids), chunk_size):
        chunk = delta_ids[i:i + chunk_size]
        params = {
            "tenants": cfg.tenants,
            "submission_ids": chunk,
            "session_types": cfg.all_session_types,
            "discussion_types": cfg.discussion_types,
            "excluded_sources": cfg.excluded_sources,
            "window_start": None,
            "window_end": None,
        }
        formatted_sql = format_sql(sql, params)
        if isinstance(source, MetabaseClient):
            rows = source.execute_query(formatted_sql)
        else:
            raise RuntimeError("Direct PostgreSQL connection to SL Prod DB is not supported without MetabaseClient.")

        total_raw_rows += len(rows)
        for row in rows:
            sub_id = str(row.get("chat_session_id"))
            sub_type, prog_name, leader_cat = classify_by_config(row, cfg)
            if not sub_type:
                skipped_classification += 1
                continue

            enriched = enrich_row(row, geo, org_resolver, cfg, sub_type)
            if sub_type == "story":
                story_rows.append((prog_name, leader_cat, build_story_csv_row(enriched, row, cfg.story_columns), (row["story_id"], sub_type, sub_id)))
            else:
                disc_row = build_discussion_csv_row(enriched, row)
                if disc_row is None:
                    log.warning(
                        "Skipping discussion submission %s (story_id: %s): missing or non-positive participant count.",
                        sub_id, row.get("story_id")
                    )
                    skipped_classification += 1
                    continue
                discussion_rows.append((prog_name, leader_cat, disc_row, (row["story_id"], sub_type, sub_id)))

    log.info("Query returned %d row(s) from SL Prod DB across all chunks (skipped %d unclassified).",
             total_raw_rows, skipped_classification)
    log.info("Prepared %d story row(s) and %d discussion row(s) for ingestion.", len(story_rows), len(discussion_rows))

    if dry_run:
        log.info("[DRY-RUN] Sample Story row: %s", story_rows[0][2] if story_rows else "None")
        log.info("[DRY-RUN] Sample Discussion row: %s", discussion_rows[0][2] if discussion_rows else "None")
        return "success"

    # Post batches to Upload API
    success = True
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    # Helper to post rows of a specific report_type
    def post_batch_group(rows: list, r_type: str) -> bool:
        if not rows:
            return True
        grouped_rows = {}
        for row in rows:
            grouped_rows.setdefault((row[0], row[1]), []).append(row)

        upload_parts = [
            ((prog, ldr), group_rows[start:start + cfg.batch_size])
            for (prog, ldr), group_rows in grouped_rows.items()
            for start in range(0, len(group_rows), cfg.batch_size)
        ]
        success = True
        multiple_groups = len(upload_parts) > 1
        for part_index, ((prog, ldr), part_rows) in enumerate(upload_parts, 1):
            buf = io.StringIO()
            fieldnames = list(part_rows[0][2].keys())
            writer = csv.DictWriter(buf, fieldnames=fieldnames)
            writer.writeheader()
            for _, _, r_dict, meta_tuple in part_rows:
                writer.writerow(r_dict)
                sent_tuples.append(meta_tuple)
            group_suffix = f"_group_{part_index}" if multiple_groups else ""
            fn = f"migration_{r_type}_{run_id}_{ts}{group_suffix}.csv"
            if not post_csv_to_upload_api(cfg, r_type, prog, ldr, buf.getvalue(), fn, cfg.target_tenant_code, log):
                success = False
            else:
                batch_sub_ids = [str(meta_tuple[2]) for _, _, _, meta_tuple in part_rows if len(meta_tuple) > 2 and meta_tuple[2]]
                tracker = SubmittedIdTracker(cfg.sent_cache_file)
                tracker.record_sent_ids(batch_sub_ids, log)
        return success

    if not post_batch_group(story_rows, "story"):
        success = False

    if not post_batch_group(discussion_rows, "discussion"):
        success = False

    return "success" if success else "failed"


# --------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="dry-run only; do not post to Upload API")
    args = ap.parse_args()

    cfg = Config()
    log = setup_logging(cfg)
    if not args.dry_run and not cfg.auth_token:
        log.error("AUTH_TOKEN is required for uploads.")
        return 1
    RunIdFilter.run_id = uuid.uuid4().hex[:8]

    cfg.lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock = open(cfg.lock_file, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.warning("Another run is active - exiting.")
        return 0

    log.info("=== Run Start: Delta Sync & Upload API Ingestion ===")
    try:
        geo_path = BASE_DIR / "mapping.json"
        geo = GeoMapping(geo_path)

        org_path = BASE_DIR / "organizations.json"
        org_resolver = OrganizationResolver(org_path)

        source = None
        if cfg.metabase_url and cfg.metabase_api_key:
            mb = MetabaseClient(cfg, log)
            mb.login()
            source = mb
        else:
            log.error("METABASE_API_KEY (with METABASE_URL) not configured.")
            return 1

        result = run_pipeline(cfg, source, geo, org_resolver, log, RunIdFilter.run_id, args.dry_run)
    except Exception:
        log.exception("Pipeline run crashed")
        return 1

    log.info("=== Run End Result: %s ===", result)
    return 0 if result == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
