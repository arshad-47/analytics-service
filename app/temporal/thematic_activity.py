from app.database.operations import fetch_challenge_and_solution_statements_for_submission
import asyncio
import json
import logging
import math
import re
import threading
from typing import Dict, Any, List, Optional, Tuple
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    fetch_child_statements,
    insert_llm_log,
    insert_analysis_result,
)
from app.services.classifier import load_setfit_model, predict_setfit_batch
from app.services.llm import openrouter_chat_completion, split_llm_usage

logger = logging.getLogger("analytics_service.temporal.activities")


def _is_garbage_or_spam(text: str) -> bool:
    """
    Detects spam/garbage text patterns beyond the word-count gate.
    Examples: "test test", "aaa aaa aaa", "123 456", repeated single tokens.
    """
    cleaned_text = text.strip()
    if not cleaned_text:
        return True

    # 1. No alphabetic characters at all (English or Indic scripts)
    if not re.search(r'[a-zA-Z\u0900-\u097F\u0980-\u09FF]', cleaned_text):
        return True

    # 2. Consecutive repeated characters: 4 or more (e.g., "aaaa", "....", "----")
    if re.search(r'(.)\1{3,}', cleaned_text):
        return True

    words = [w.strip().lower() for w in cleaned_text.split() if w.strip()]
    if not words:
        return True

    # 3. Common placeholder words/mashing (case-insensitive)
    placeholders = {
        "test", "testing", "demo", "dummy", "asdf", "ghjk", "qwerty", 
        "placeholder", "abc", "xyz", "nothing", "none", "nil", "n/a", "na"
    }
    if (len(words) == 1 and words[0] in placeholders) or all(w in placeholders for w in words):
        return True

    # 4. Keyboard mashes (words of length >= 6 with zero vowels)
    vowels = set("aeiouy")
    for w in words:
        if len(w) >= 6 and re.match(r'^[a-z]+$', w):
            if not any(char in vowels for char in w):
                return True

    # 5. Repetitive text spam: if unique words make up less than 30% of total words (for statements with 3+ words)
    if len(words) >= 3:
        unique_ratio = len(set(words)) / len(words)
        if unique_ratio < 0.3:
            return True

    # 6. All words are the same
    if len(set(words)) == 1 and len(words) > 1:
        return True

    return False


async def _fetch_approved_themes(conn) -> list:
    """Fetches all approved themes from the themes table."""
    rows = await conn.fetch(
        "SELECT id, name, definitions, keywords, examples FROM themes WHERE status ILIKE 'approved'"
    )
    return [dict(row) for row in rows]


async def _get_theme_classification_prompt(conn, analysis_type: str) -> dict:
    """
    Fetches the latest active theme_classification prompt version.
    Returns dict with 'id', 'system_prompt', 'user_prompt'.
    """
    row = await conn.fetchrow(
        """
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE (p.analysis_type = $1)
          AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC
        LIMIT 1
        """,
        analysis_type
    )
    if not row:
        raise RuntimeError(f"No active {analysis_type} prompt version found in the database.")
    return dict(row)


def _build_themes_text(approved_themes: list) -> str:
    """
    Formats approved themes into a text block for prompt substitution.
    Each theme includes its id, name, and definition.
    """
    lines = []
    for theme in approved_themes:
        theme_id = theme["id"]
        name = theme.get("name", "")
        definition = theme.get("definitions") or ""
        keywords = theme.get("keywords") or ""
        lines.append(
            f"Theme ID: {theme_id}\n"
            f"Theme Name: {name}\n"
            f"Definition: {definition}\n"
            f"Keywords: {keywords}\n"
        )
    return "\n---\n".join(lines)


def _parse_confidence_score(raw: Any) -> Optional[float]:
    """Parses an LLM-returned confidence score, rejecting non-numeric, non-finite,
    or out-of-range values instead of raising (a single malformed entry must not
    take down the rest of the batch)."""
    try:
        score = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(score) or not (0.0 <= score <= 1.0):
        return None
    return score


def _resolve_theme_id(theme_name: Optional[str], theme_id_to_info: dict) -> Optional[str]:
    """Matches an LLM-returned theme_name to an approved theme_id by name
    (the LLM's own theme_id field is not trusted — it may be stale or mismatched)."""
    for tid, tinfo in theme_id_to_info.items():
        if tinfo.get("name", "").lower().strip() == str(theme_name or "").lower().strip():
            return tid
    return None


def _finalize_qualifying_themes(
    resolved_items: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Dedupes resolved LLM classification items by theme_id (keeping the highest-confidence
    instance per theme) and caps to a single theme.
    """
    best_by_theme: Dict[str, Dict[str, Any]] = {}
    for item in resolved_items:
        tid = item.get("theme_id")
        if not tid:
            continue
        conf = item["confidence_score"]
        
        if conf < settings.LLM_CONFIDENCE_SCORE_THRESHOLD:
            continue
            
        existing = best_by_theme.get(tid)
        if existing is None or conf > existing["confidence_score"]:
            best_by_theme[tid] = item

    qualifying = sorted(best_by_theme.values(), key=lambda x: x["confidence_score"], reverse=True)
    return qualifying[:1]


async def _run_local_classification(
    conn,
    statement: str,
    submission_id: str,
    tenant_code: str,
    statement_type: str,
    abusive_masked_at: list,
    analysis_type: str,
    statement_id: Optional[str] = None,
    setfit_conf: Optional[float] = None,
    setfit_pred: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Runs the word-count/garbage gate and safety check for statements SetFit was
    not confident enough to resolve directly. Both gates must pass before the
    statement is queued for the batched LLM fallback.

    Returns (finished_result, pending_item):
      - finished_result: resolved as Unknown/Unclear or Flagged — row already written.
      - pending_item: passed both gates; ready for LLM fallback.
    """
    diagnostics = {
        "word_count_check": {
            "passed": False,
            "word_count": 0,
        },
        "safety_check": {
            "passed": False,
            "is_flagged": False
        },
        "llm_fallback": {
            "executed": False,
            "confidence_score": None,
            "passed": False,
        },
    }
    result = {
        "statement": statement,
        "category_type": None,
        "theme_id": None,
        "confidence_score": None,
        "diagnostics": diagnostics,
    }

    logger.info(f"\n[Thematic Pipeline] =================== Evaluating Statement: '{statement}' ===================")

    # --- Step 2: Word-count / garbage gate ---
    word_count = len(statement.strip().split())
    diagnostics["word_count_check"]["word_count"] = word_count
    logger.info(f"[Thematic Pipeline] Step 2: Checking word-count threshold (words={word_count}, minimum={settings.MINIMUM_THEME_WORD_COUNT})")
    if word_count < settings.MINIMUM_THEME_WORD_COUNT or _is_garbage_or_spam(statement):
        result["category_type"] = "Unknown/Unclear"
        await insert_analysis_result(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            statement_id=statement_id,
            theme_id=None,
            analysis_type=analysis_type,
            statement_type=statement_type,
            category_type="Unknown/Unclear",
            meta_data=diagnostics,
        )
        logger.info("[Thematic Pipeline] -> FAILED word-count/garbage gate. Marked Unknown/Unclear.")
        return result, None

    diagnostics["word_count_check"]["passed"] = True
    logger.info("[Thematic Pipeline] -> PASSED word-count/garbage gate.")

    # --- Step 3: Safety check ---
    # PII masking runs once per column (see pii_and_abusive_activity.py), so
    # pii_masked_at is a column-level flag — checking column membership here would
    # mark every split statement in that column as Flagged even if only one of
    # them actually contained PII. The masking prompt replaces sensitive spans with
    # tags (<PERSON>, <PHONE>, <ID>, <LOCATION>), so check the statement text itself
    # for one of those tags instead — only the actually-masked statement(s) get flagged.
    abusive_cols = abusive_masked_at or []
    has_pii_tag = bool(re.search(r'<[A-Z]+>', statement))
    # Abusive language is flagged but never replaced with a tag, so there's no
    # per-statement textual signal to key off — this stays column-level.
    is_abusive_flagged_column = statement_type in abusive_cols

    flagged = has_pii_tag or is_abusive_flagged_column
    diagnostics["safety_check"]["is_flagged"] = flagged
    diagnostics["safety_check"]["pii_tag_detected"] = has_pii_tag
    diagnostics["safety_check"]["abusive_flagged_column"] = is_abusive_flagged_column
    if flagged:
        result["category_type"] = "Flagged"
        await insert_analysis_result(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            statement_id=statement_id,
            theme_id=None,
            analysis_type=analysis_type,
            statement_type=statement_type,
            category_type="Flagged",
            meta_data=diagnostics,
        )
        reason = "contains a PII mask tag" if has_pii_tag else f"column {statement_type} was flagged for abusive content"
        logger.info(f"[Thematic Pipeline] -> FAILED safety check. Statement {reason}.")
        return result, None

    diagnostics["safety_check"]["passed"] = True
    logger.info("[Thematic Pipeline] -> PASSED safety check. Queuing for LLM fallback.")

    # Both gates passed — queue for batched LLM fallback
    diagnostics["llm_fallback"]["executed"] = True
    pending_item = {
        "statement": statement,
        "statement_type": statement_type,
        "setfit_conf": setfit_conf,
        "setfit_pred": setfit_pred,
        "diagnostics": diagnostics,
    }
    return None, pending_item


async def _run_batched_llm_fallback(
    pending_items: List[Dict[str, Any]],
    approved_themes: list,
    theme_id_to_info: dict,
    submission_id: str,
    tenant_code: str,
    analysis_type: str,
    resolved_model: str,
    resolved_max_tokens: int,
    resolved_timeout: int,
) -> List[Dict[str, Any]]:
    """
    Classifies every pending_item in ONE combined LLM call instead of one call per
    statement. The fixed cost of the prompt — the rules plus the full approved-themes
    catalog — is paid once for the whole batch instead of once per statement, which is
    where nearly all of the token cost comes from when a submission has several
    statements needing the fallback.

    Each statement is given a numeric index in the prompt; the model is required to
    echo that index (statement_index) on every classified_data entry so results map
    back to their source statement unambiguously — a raw text match would be fragile
    if two statements were similar or the model paraphrased the echoed text.

    Acquires its own DB connections rather than taking one from the caller, so no
    pool connection sits idle for the duration of the timeout-bound LLM call below.
    """
    logger.info(f"[Thematic Pipeline] Batched LLM fallback: {len(pending_items)} statement(s) in a single call")

    prompt_version_id = None
    full_prompt = ""
    response_text = ""
    usage: Dict[str, Any] = {}
    items_by_index: Dict[int, List[Dict[str, Any]]] = {}
    llm_result = None

    try:
        async with db.pool.acquire() as conn:
            prompt_data = await _get_theme_classification_prompt(conn, analysis_type)
        prompt_version_id = str(prompt_data["id"])
        system_prompt = prompt_data["system_prompt"]
        user_prompt = prompt_data["user_prompt"]

        themes_text = _build_themes_text(approved_themes)
        # Serialized as a JSON array (not newline-delimited "[n] text" lines) — a
        # statement containing an embedded newline followed by something resembling
        # "[n]" would otherwise be indistinguishable from a real index boundary and
        # could corrupt the statement_index -> source statement mapping.
        statements_text = json.dumps(
            [
                {"statement_index": idx, "statement": item["statement"]}
                for idx, item in enumerate(pending_items)
            ],
            ensure_ascii=False,
            indent=2,
        )

        user_prompt = user_prompt.replace("{{approved_themes}}", themes_text)
        user_prompt = user_prompt.replace("{{statements}}", statements_text)
        user_prompt = user_prompt.replace("{{statement}}", statements_text)
        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        logger.info(
            f"[Thematic Pipeline] Sending batched prompt for {len(pending_items)} statement(s) "
            f"to LLM ({resolved_model}, timeout={resolved_timeout}s)..."
        )
        response_text, usage = await asyncio.to_thread(
            openrouter_chat_completion,
            prompt=full_prompt,
            model=resolved_model,
            max_tokens=resolved_max_tokens,
            timeout=resolved_timeout,
        )

        json_str = response_text
        if "```json" in json_str:
            json_str = json_str.split("```json")[1].split("```")[0].strip()
        elif "```" in json_str:
            json_str = json_str.split("```")[1].split("```")[0].strip()

        llm_result = json.loads(json_str)
        raw_classified_data = llm_result.get("classified_data", [])
        logger.info(f"[Thematic Pipeline] Batched LLM call succeeded; returned {len(raw_classified_data)} classified_data entry/entries.")

        text_to_index = {item["statement"].strip().lower(): idx for idx, item in enumerate(pending_items)}

        for item in raw_classified_data:
            idx = item.get("statement_index")
            if not isinstance(idx, int) or not (0 <= idx < len(pending_items)):
                echoed = str(item.get("challenge") or item.get("statement") or "").strip().lower()
                fallback_idx = text_to_index.get(echoed)
                if fallback_idx is None:
                    logger.warning(f"[Thematic Pipeline] Could not match a classified_data entry back to a source statement (missing/invalid statement_index, no text match): {item}")
                    continue
                logger.warning(f"[Thematic Pipeline] classified_data entry had missing/invalid statement_index; matched by echoed text instead (index={fallback_idx}).")
                idx = fallback_idx

            confidence_score = _parse_confidence_score(item.get("confidence_score"))
            if confidence_score is None:
                logger.warning(f"[Thematic Pipeline] classified_data entry at index {idx} has an invalid confidence_score ({item.get('confidence_score')!r}); skipping entry.")
                continue

            item_theme_name = item.get("theme_name")
            items_by_index.setdefault(idx, []).append({
                "theme_id": _resolve_theme_id(item_theme_name, theme_id_to_info),
                "theme_name": item_theme_name,
                "confidence_score": confidence_score,
                "justification": item.get("justification"),
            })

        prompt_tokens, completion_tokens, usage_meta = split_llm_usage(usage)
        async with db.pool.acquire() as conn:
            await insert_llm_log(
                conn,
                submission_id=submission_id,
                tenant_code=tenant_code,
                model_name=resolved_model or settings.OPENROUTER_MODEL,
                analysis_type=analysis_type,
                prompt_version_id=prompt_version_id,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                status="success",
                meta_data=usage_meta or None,
            )

    except Exception as e:
        logger.error(f"[Thematic Pipeline] Batched LLM fallback failed for {len(pending_items)} statement(s): {e}")
        if prompt_version_id is not None:
            try:
                if usage:
                    prompt_tokens, completion_tokens, usage_meta = split_llm_usage(usage)
                else:
                    prompt_tokens = len(full_prompt.split()) if full_prompt else 0
                    completion_tokens = len(response_text.split()) if response_text else 0
                    usage_meta = None
                async with db.pool.acquire() as conn:
                    await insert_llm_log(
                        conn,
                        submission_id=submission_id,
                        tenant_code=tenant_code,
                        model_name=resolved_model or settings.OPENROUTER_MODEL,
                        analysis_type=analysis_type,
                        prompt_version_id=prompt_version_id,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        status="failed",
                        error_message=str(e),
                        meta_data=usage_meta,
                    )
            except Exception as log_err:
                logger.error(f"[Thematic Pipeline] Failed to log batched LLM failure to llm_logs: {log_err}")
        raise

    results = []
    async with db.pool.acquire() as conn:
        for idx, pending in enumerate(pending_items):
            statement = pending["statement"]
            statement_type = pending["statement_type"]
            diagnostics = pending["diagnostics"]

            result = {
                "statement": statement,
                "category_type": None,
                "theme_id": None,
                "confidence_score": None,
                "diagnostics": diagnostics,
            }

            resolved_items = items_by_index.get(idx, [])
            llm_confidence = None
            llm_justification = None
            qualifying_llm = []

            if resolved_items:
                best_item = max(resolved_items, key=lambda x: x["confidence_score"])
                llm_confidence = best_item["confidence_score"]
                llm_justification = best_item["justification"]
                qualifying_llm = _finalize_qualifying_themes(resolved_items)

            result["confidence_score"] = llm_confidence
            diagnostics["llm_fallback"]["confidence_score"] = llm_confidence
            diagnostics["llm_fallback"]["complete_llm_response"] = llm_result

            if qualifying_llm:
                diagnostics["llm_fallback"]["passed"] = True
                is_multi = len(qualifying_llm) > 1
                result["category_type"] = "Standard"
                result["theme_id"] = qualifying_llm[0]["theme_id"]
                result["matched_themes"] = [
                    {"theme_id": item["theme_id"], "confidence_score": item["confidence_score"]}
                    for item in qualifying_llm
                ]

                for item in qualifying_llm:
                    setfit_pred = pending.get("setfit_pred")
                    setfit_threshold = settings.get_setfit_theme_threshold(setfit_pred) if setfit_pred else None
                    await insert_analysis_result(
                        conn,
                        submission_id=submission_id,
                        tenant_code=tenant_code,
                        statement_id=pending.get("statement_id"),
                        theme_id=item["theme_id"],
                        analysis_type=analysis_type,
                        statement_type=statement_type,
                        category_type="Standard",
                        ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                        ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                        model_confidence_score=pending.get("setfit_conf"),
                        model_prediction=setfit_pred,
                        llm_prediction=item["theme_name"],
                        threshold=setfit_threshold,
                        llm_confidence_score=item["confidence_score"],
                        justification=item["justification"],
                        multi_theme_mapped=is_multi,
                        meta_data=diagnostics,
                    )

                theme_names = [theme_id_to_info.get(item["theme_id"], {}).get("name", "?") for item in qualifying_llm]
                logger.info(
                    f"LLM match{'es' if is_multi else ''} (batched): '{statement[:60]}...' → {theme_names} "
                    f"(conf={[round(item['confidence_score'], 2) for item in qualifying_llm]})"
                )
                # Propagate each qualifying theme result to duplicate (child) statements
                parent_stmt_id = pending.get("statement_id")
                _children = await fetch_child_statements(conn, parent_stmt_id, submission_id, tenant_code)
                for _child in _children:
                    for item in qualifying_llm:
                        setfit_pred = pending.get("setfit_pred")
                        setfit_threshold = settings.get_setfit_theme_threshold(setfit_pred) if setfit_pred else None
                        await insert_analysis_result(
                            conn,
                            submission_id=submission_id,
                            tenant_code=tenant_code,
                            statement_id=_child["id"],
                            theme_id=item["theme_id"],
                            analysis_type=analysis_type,
                            statement_type=_child["statement_type"],
                            category_type="Standard",
                            ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                            ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                            model_confidence_score=pending.get("setfit_conf"),
                            model_prediction=setfit_pred,
                            llm_prediction=item["theme_name"],
                            threshold=setfit_threshold,
                            llm_confidence_score=item["confidence_score"],
                            justification=item["justification"],
                            multi_theme_mapped=is_multi,
                            meta_data={"deduped_from": str(parent_stmt_id)},
                        )
                    logger.debug("[Thematic Pipeline] Copied LLM Standard result from parent [%s] to child [%s]", parent_stmt_id, _child["id"])
            else:
                result["category_type"] = "Others"
                setfit_pred = pending.get("setfit_pred")
                setfit_threshold = settings.get_setfit_theme_threshold(setfit_pred) if setfit_pred else None
                await insert_analysis_result(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    statement_id=pending.get("statement_id"),
                    theme_id=None,
                    analysis_type=analysis_type,
                    statement_type=statement_type,
                    category_type="Others",
                    ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                    ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                    model_confidence_score=pending.get("setfit_conf"),
                    model_prediction=setfit_pred,
                    threshold=setfit_threshold,
                    llm_confidence_score=llm_confidence,
                    justification=llm_justification,
                    meta_data=diagnostics,
                )
                logger.info(f"Statement marked Others (low confidence, batched): {statement[:80]}...")
                # Propagate Others result to duplicate (child) statements
                parent_stmt_id = pending.get("statement_id")
                _children = await fetch_child_statements(conn, parent_stmt_id, submission_id, tenant_code)
                for _child in _children:
                    setfit_pred = pending.get("setfit_pred")
                    setfit_threshold = settings.get_setfit_theme_threshold(setfit_pred) if setfit_pred else None
                    await insert_analysis_result(
                        conn,
                        submission_id=submission_id,
                        tenant_code=tenant_code,
                        statement_id=_child["id"],
                        theme_id=None,
                        analysis_type=analysis_type,
                        statement_type=_child["statement_type"],
                        category_type="Others",
                        ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                        ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                        model_confidence_score=pending.get("setfit_conf"),
                        model_prediction=setfit_pred,
                        threshold=setfit_threshold,
                        llm_confidence_score=llm_confidence,
                        justification=llm_justification,
                        meta_data={"deduped_from": str(parent_stmt_id)},
                    )
                    logger.debug("[Thematic Pipeline] Copied Others result from parent [%s] to child [%s]", parent_stmt_id, _child["id"])

            results.append(result)

    return results


@activity.defn
async def thematic_classification_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that performs thematic classification on Challenge and solution statements.

    Logic:
      1. Fetch parent statements classified as 'Challenge' or 'Solution or Action'.
      2. Run SetFit batch inference on the fetched statements.
      3. Confident SetFit hits (>= dynamic threshold) are safety-checked and inserted. Results are automatically propagated to child statements.
      4. Low-confidence statements go through `_run_local_classification` for word-count and safety checks.
      5. Statements passing all checks are batched into ONE LLM fallback call, and results are propagated to child statements.
    """
    submission_id = params.get("submission_id")
    tenant_code = params.get("tenant_code")
    resolved_model = params.get("llm_model") or params.get("model") or settings.OPENROUTER_MODEL
    resolved_max_tokens = params.get("max_tokens") or settings.LLM_MAX_TOKENS
    resolved_timeout = params.get("llm_timeout_seconds") or params.get("timeout") or settings.LLM_TIMEOUT_SECONDS
    analysis_type = params.get("analysis_type", "thematic_classification")

    if not submission_id or not tenant_code:
        raise ValueError("submission_id and tenant_code are required.")

    logger.info(f"[Thematic Pipeline] Starting activity for submission={submission_id}, tenant={tenant_code}")

    async with db.pool.acquire() as conn:
        statements = await fetch_challenge_and_solution_statements_for_submission(conn, submission_id, tenant_code)
        if not statements:
            logger.info(f"[Thematic Pipeline] No Challenge/Solution statements found for submission={submission_id}.")
            return {
                "status": "success",
                "processed": 0,
                "results": [],
                "warnings": ["No Challenge or Solution statements found for submission."],
            }

        approved_themes = await _fetch_approved_themes(conn)
        abusive_masked_at_row = await conn.fetchrow(
            """
            SELECT ss.abusive_masked_at FROM submissions sub
            LEFT JOIN story_submissions ss
                ON ss.submission_id = sub.submission_id AND ss.tenant_code = sub.tenant_code
            LEFT JOIN discussion_submissions ds
                ON ds.submission_id = sub.submission_id AND ds.tenant_code = sub.tenant_code
            WHERE sub.submission_id = $1 AND sub.tenant_code = $2
            """,
            submission_id, tenant_code, analysis_type
        )
        abusive_masked_at: List[str] = (
            list(abusive_masked_at_row["abusive_masked_at"] or [])
            if abusive_masked_at_row and abusive_masked_at_row["abusive_masked_at"]
            else []
        )

    warnings = []
    if not approved_themes:
        warn_msg = "No approved themes found in database. All statements will go to LLM fallback."
        logger.warning(warn_msg)
        warnings.append(warn_msg)

    theme_id_to_info = {str(t["id"]): t for t in approved_themes}

    # Clear previous theme analysis_results for idempotency
    async with db.pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM analysis_results WHERE submission_id = $1 AND tenant_code = $2 AND analysis_type = $3",
            submission_id, tenant_code, analysis_type
        )

    # Step 1 — SetFit theme model: batch inference on all Challenge statements
    setfit_model = await asyncio.to_thread(
        load_setfit_model,
        settings.SETFIT_THEME_MODEL_ID,
        settings.SETFIT_THEME_MODEL_VERSION,
    )
    texts = [s["raw_statement"] for s in statements]
    setfit_preds, setfit_confs = await asyncio.to_thread(predict_setfit_batch, setfit_model, texts)

    setfit_resolved_count = 0
    pending_items: List[Dict] = []
    all_results: List[Dict] = []

    async with db.pool.acquire() as conn:
        for stmt, pred, conf in zip(statements, setfit_preds, setfit_confs):
            statement      = stmt["raw_statement"]
            statement_type = stmt["statement_type"]
            statement_id   = stmt["statement_id"]

            try:
                theme_threshold = settings.get_setfit_theme_threshold(str(pred))
            except ValueError:
                logger.warning(
                    f"[Thematic Pipeline] SetFit predicted unmapped theme label '{pred}' for statement "
                    f"{statement_id}; routing to LLM fallback instead of failing the submission."
                )
                theme_threshold = None

            if theme_threshold is not None and conf >= theme_threshold:
                has_pii_tag = bool(re.search(r'<[A-Z]+>', statement))
                is_abusive_flagged_column = statement_type in abusive_masked_at
                flagged = has_pii_tag or is_abusive_flagged_column

                if flagged:
                    reason = "PII mask tag" if has_pii_tag else f"abusive column ({statement_type})"
                    logger.info(f"[Thematic Pipeline] SetFit confident but FLAGGED ({reason}): '{statement[:80]}'")
                    await insert_analysis_result(
                        conn,
                        submission_id=submission_id,
                        tenant_code=tenant_code,
                        statement_id=statement_id,
                        analysis_type=analysis_type,
                        statement_type=statement_type,
                        category_type="Flagged",
                        ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                        ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                        model_confidence_score=conf,
                        model_prediction=pred,
                        threshold=theme_threshold,
                    )
                    # Propagate result to duplicate (child) statements
                    _children = await fetch_child_statements(conn, statement_id, submission_id, tenant_code)
                    for _child in _children:
                        await insert_analysis_result(
                            conn,
                            submission_id=submission_id,
                            tenant_code=tenant_code,
                            statement_id=_child["id"],
                            analysis_type=analysis_type,
                            statement_type=_child["statement_type"],
                            category_type="Flagged",
                            ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                            ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                            model_confidence_score=conf,
                            model_prediction=pred,
                            threshold=theme_threshold,
                            meta_data={"deduped_from": str(statement_id)},
                        )
                        logger.debug("[Thematic Pipeline] Copied Flagged result from parent [%s] to child [%s]", statement_id, _child["id"])
                    all_results.append({"statement": statement, "category_type": "Flagged"})
                    setfit_resolved_count += 1
                    continue

                resolved_theme_id = _resolve_theme_id(pred, theme_id_to_info)
                cat_type = "Standard" if resolved_theme_id else "Others"

                logger.info(
                    f"[Thematic Pipeline] SetFit resolved '{statement[:60]}' → theme='{pred}' ({cat_type}, conf={conf:.3f})"
                )
                await insert_analysis_result(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    statement_id=statement_id,
                    analysis_type=analysis_type,
                    statement_type=statement_type,
                    theme_id=resolved_theme_id,
                    category_type=cat_type,
                    ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                    ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                    model_confidence_score=conf,
                    model_prediction=pred,
                    threshold=theme_threshold,
                )
                # Propagate result to duplicate (child) statements
                _children = await fetch_child_statements(conn, statement_id, submission_id, tenant_code)
                for _child in _children:
                    await insert_analysis_result(
                        conn,
                        submission_id=submission_id,
                        tenant_code=tenant_code,
                        statement_id=_child["id"],
                        analysis_type=analysis_type,
                        statement_type=_child["statement_type"],
                        theme_id=resolved_theme_id,
                        category_type=cat_type,
                        ml_model_name=settings.SETFIT_THEME_MODEL_ID,
                        ml_model_version=settings.SETFIT_THEME_MODEL_VERSION,
                        model_confidence_score=conf,
                        model_prediction=pred,
                        threshold=theme_threshold,
                        meta_data={"deduped_from": str(statement_id)},
                    )
                    logger.debug("[Thematic Pipeline] Copied %s result from parent [%s] to child [%s]", cat_type, statement_id, _child["id"])
                all_results.append({
                    "statement": statement,
                    "category_type": cat_type,
                    "theme_id": resolved_theme_id,
                })
                setfit_resolved_count += 1

            else:
                logger.info(
                    f"[Thematic Pipeline] SetFit conf {conf:.3f} < threshold {theme_threshold:.3f} for theme '{pred}' on statement '{statement[:60]}' — queued for local pipeline."
                )
                finished_result, pending_item = await _run_local_classification(
                    conn=conn,
                    statement=statement,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    statement_type=statement_type,
                    abusive_masked_at=abusive_masked_at,
                    analysis_type=analysis_type,
                    statement_id=statement_id,
                    setfit_conf=conf,
                    setfit_pred=pred,
                )
                if finished_result is not None:
                    all_results.append(finished_result)
                else:
                    pending_item["statement_id"] = statement_id
                    pending_items.append(pending_item)

    if pending_items:
        fallback_results = await _run_batched_llm_fallback(
            pending_items=pending_items,
            approved_themes=approved_themes,
            theme_id_to_info=theme_id_to_info,
            submission_id=submission_id,
            tenant_code=tenant_code,
            analysis_type=analysis_type,
            resolved_model=resolved_model,
            resolved_max_tokens=resolved_max_tokens,
            resolved_timeout=resolved_timeout,
        )
        all_results.extend(fallback_results)

    logger.info(
        f"[Thematic Pipeline] Done. Processed {len(all_results)} statement(s) "
        f"({setfit_resolved_count} by SetFit, {len(all_results) - setfit_resolved_count} by fallback/gates)."
    )

    return {
        "status": "success",
        "submission_id": submission_id,
        "processed": len(all_results),
        "setfit_resolved": setfit_resolved_count,
        "results": all_results,
        "warnings": warnings,
    }
