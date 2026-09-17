import asyncio
import json
import logging
import math
import re
from typing import Dict, Any, List, Optional, Tuple
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    insert_llm_log,
    insert_analysis_result,
    get_submission_type_and_payload,
)
from app.services.classifier import build_theme_embeddings, get_theme_similarities

logger = logging.getLogger("analytics_service.temporal.activities")

# Discussion statements may legitimately cover several distinct barriers in one
# sentence, so they're allowed to map to multiple themes. Story objectives are
# a single narrative and stay single-theme regardless of how many themes qualify.
MAX_MULTI_THEME_MATCHES = 3


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
    # If the text matches a placeholder or all words are placeholders
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
        WHERE p.analysis_type = $1 AND pv.is_active = TRUE
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
        definition = theme.get("definitions", "") or ""
        keywords = theme.get("keywords", "") or ""
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
    resolved_items: List[Dict[str, Any]],
    is_discussion: bool,
) -> List[Dict[str, Any]]:
    """
    Dedupes resolved LLM classification items by theme_id (keeping the highest-confidence
    instance per theme), filters to those clearing LLM_CONFIDENCE_SCORE_THRESHOLD, and caps
    to a single theme for stories (or MAX_MULTI_THEME_MATCHES for discussions).
    """
    best_by_theme: Dict[str, Dict[str, Any]] = {}
    for item in resolved_items:
        tid = item["theme_id"]
        if tid is None:
            continue
        if tid not in best_by_theme or item["confidence_score"] > best_by_theme[tid]["confidence_score"]:
            best_by_theme[tid] = item

    qualifying = [
        item for item in best_by_theme.values()
        if item["confidence_score"] >= settings.LLM_CONFIDENCE_SCORE_THRESHOLD
    ]
    qualifying.sort(key=lambda x: x["confidence_score"], reverse=True)

    if not is_discussion:
        # Story submissions stay single-theme regardless of how many themes qualify
        qualifying = qualifying[:1]
    elif len(qualifying) > MAX_MULTI_THEME_MATCHES:
        logger.warning(
            f"[Thematic Pipeline] {len(qualifying)} qualifying themes for one statement; capping to top {MAX_MULTI_THEME_MATCHES}."
        )
        qualifying = qualifying[:MAX_MULTI_THEME_MATCHES]

    return qualifying


async def _run_local_classification(
    conn,
    statement: str,
    submission_id: str,
    tenant_code: str,
    statement_type: str,
    theme_vectors: dict,
    theme_id_to_info: dict,
    abusive_masked_at: list,
    is_discussion: bool,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Runs Steps 2, 3 and 6 for one statement — the word-count/garbage gate, the safety
    check, and the local embedding match — none of which need an LLM call.

    Returns (finished_result, pending_item), exactly one of which is non-None:
      - finished_result: the statement resolved without the LLM (Unknown/Unclear,
        Flagged, or a local embedding match clearing SIMILARITY_SCORE_THRESHOLD).
        Its analysis_results row(s) are already written.
      - pending_item: the statement needs the LLM fallback batch. Carries everything
        the batched call needs to finish classification later.
    """
    diagnostics = {
        "word_count_check": {
            "passed": False,
            "word_count": 0,
            "threshold": settings.MINIMUM_THEME_WORD_COUNT,
        },
        "safety_check": {
            "passed": False,
            "is_flagged": False,
        },
        "local_embedding_compare": {
            "similarity_score": 0.0,
            "threshold": settings.SIMILARITY_SCORE_THRESHOLD,
            "passed": False,
        },
        "llm_fallback": {
            "executed": False,
            "confidence_score": None,
            "threshold": settings.LLM_CONFIDENCE_SCORE_THRESHOLD,
            "passed": False,
        }
    }
    result = {
        "statement": statement,
        "category_type": None,
        "theme_id": None,
        "similarity_score": None,
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
            theme_id=None,
            analysis_type="theme",
            statement_type=statement_type,
            category_type="Unknown/Unclear",
            meta_data=diagnostics,
        )
        logger.info("[Thematic Pipeline] -> FAILED word-count/garbage gate. Marked Unknown/Unclear.")
        return result, None

    diagnostics["word_count_check"]["passed"] = True
    logger.info("[Thematic Pipeline] -> PASSED word-count/garbage gate.")

    # --- Step 3: Safety check ---
    logger.info(f"[Thematic Pipeline] Step 3: Checking statement safety (statement-level PII tag scan + column-level abuse flag for {statement_type})")
    abusive_cols = abusive_masked_at or []

    # PII masking runs once per column (see pii_and_abusive_activity.py), so
    # pii_masked_at is a column-level flag — checking column membership here would
    # mark every split statement in that column as Flagged even if only one of
    # them actually contained PII. The masking prompt replaces sensitive spans with
    # tags (<PERSON>, <PHONE>, <ID>, <LOCATION>), so check the statement text itself
    # for one of those tags instead — only the actually-masked statement(s) get flagged.
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
            theme_id=None,
            analysis_type="theme",
            statement_type=statement_type,
            category_type="Flagged",
            meta_data=diagnostics,
        )
        reason = "contains a PII mask tag" if has_pii_tag else f"column {statement_type} was flagged for abusive content"
        logger.info(f"[Thematic Pipeline] -> FAILED safety check. Statement {reason}.")
        return result, None

    diagnostics["safety_check"]["passed"] = True
    logger.info("[Thematic Pipeline] -> PASSED safety check.")

    # --- Step 5 (themes already fetched) ---
    # --- Step 6: Local embedding classification ---
    logger.info("[Thematic Pipeline] Step 6: Comparing against approved themes using local SentenceTransformer embeddings")
    # SentenceTransformer.encode() is CPU-bound and synchronous — run it off the
    # event loop so it doesn't block other activities on this Temporal worker.
    theme_similarities = await asyncio.to_thread(get_theme_similarities, statement, theme_vectors)
    best_theme_id, best_similarity = theme_similarities[0] if theme_similarities else (None, 0.0)
    result["similarity_score"] = best_similarity
    diagnostics["local_embedding_compare"]["similarity_score"] = best_similarity

    if best_similarity >= settings.SIMILARITY_SCORE_THRESHOLD and best_theme_id:
        diagnostics["local_embedding_compare"]["passed"] = True

        qualifying_local = [
            (tid, score) for tid, score in theme_similarities
            if score >= settings.SIMILARITY_SCORE_THRESHOLD
        ]
        if not is_discussion:
            # Story submissions stay single-theme regardless of how many themes clear the threshold
            qualifying_local = qualifying_local[:1]
        elif len(qualifying_local) > MAX_MULTI_THEME_MATCHES:
            logger.warning(
                f"[Thematic Pipeline] Local match found {len(qualifying_local)} themes above threshold; capping to top {MAX_MULTI_THEME_MATCHES}."
            )
            qualifying_local = qualifying_local[:MAX_MULTI_THEME_MATCHES]

        is_multi = len(qualifying_local) > 1
        result["category_type"] = "Standard"
        result["theme_id"] = qualifying_local[0][0]
        result["matched_themes"] = [{"theme_id": tid, "similarity_score": score} for tid, score in qualifying_local]

        for tid, score in qualifying_local:
            await insert_analysis_result(
                conn,
                submission_id=submission_id,
                tenant_code=tenant_code,
                theme_id=tid,
                analysis_type="theme",
                statement_type=statement_type,
                category_type="Standard",
                model_confidence_score=score,
                multi_theme_mapped=is_multi,
                meta_data=diagnostics,
            )

        theme_names = [theme_id_to_info.get(tid, {}).get("name", "?") for tid, _ in qualifying_local]
        logger.info(
            f"[Thematic Pipeline] -> SUCCESSFUL local embedding match{'es' if is_multi else ''}: "
            f"'{statement[:50]}...' → {theme_names} (sim={[round(s, 3) for _, s in qualifying_local]} >= threshold={settings.SIMILARITY_SCORE_THRESHOLD:.3f})"
        )
        return result, None

    logger.info(f"[Thematic Pipeline] -> LOCAL MATCH similarity {best_similarity:.3f} was below threshold={settings.SIMILARITY_SCORE_THRESHOLD:.3f}. Queued for batched LLM fallback.")

    diagnostics["llm_fallback"]["executed"] = True
    pending_item = {
        "statement": statement,
        "statement_type": statement_type,
        "is_discussion": is_discussion,
        "best_similarity": best_similarity,
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

        from app.services.llm import openrouter_chat_completion, split_llm_usage
        response_text, usage = await asyncio.to_thread(
            openrouter_chat_completion,
            full_prompt, model=resolved_model, max_tokens=resolved_max_tokens, timeout=resolved_timeout,
        )

        # Clean markdown wrappers
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            if lines[0].startswith("```json") or lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()

        # Fix unquoted UUIDs in the JSON if the LLM outputted them without quotes
        cleaned = re.sub(
            r'"theme_id"\s*:\s*([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
            r'"theme_id": "\1"',
            cleaned
        )

        try:
            llm_result = json.loads(cleaned)
        except json.JSONDecodeError as jde:
            logger.error(f"JSON parsing failed for batched LLM response. Error: {jde} (response length={len(response_text)})")
            raise

        classified_items = []
        if "classified_data" in llm_result and isinstance(llm_result["classified_data"], list):
            classified_items = llm_result["classified_data"]
        else:
            classified_items = [llm_result]

        # Defensive fallback for entries missing a valid statement_index: match by
        # the echoed statement text (best-effort only — this is why statement_index
        # is the primary, required mapping mechanism).
        text_to_index = {item["statement"].strip().lower(): idx for idx, item in enumerate(pending_items)}

        for item in classified_items:
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
                # Real usage is only available if the API call itself succeeded (e.g. a
                # later JSON-parse failure) — fall back to a word-count estimate only
                # when we never got a response to report actual billed tokens for.
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

        # Re-raise rather than falling through to Step 9 with an empty
        # items_by_index — an HTTP/parsing/logging failure here is an
        # infrastructure error, not a legitimate low-confidence classification.
        # Swallowing it would silently persist every pending statement as a
        # valid-looking "Others" result and report the activity as successful,
        # which would prevent Temporal's configured RetryPolicy from ever
        # retrying a transient failure (matching how pii_and_abusive_activity.py
        # and story_rating_activity.py already re-raise from their own LLM
        # failure handlers instead of persisting a fabricated result).
        raise

    # --- Step 9: Finalize each pending item using its grouped classified_data entries.
    # Own connection scope — acquired fresh now that the LLM call (if any) has returned.
    results = []
    async with db.pool.acquire() as conn:
        for idx, pending in enumerate(pending_items):
            statement = pending["statement"]
            statement_type = pending["statement_type"]
            is_discussion = pending["is_discussion"]
            best_similarity = pending["best_similarity"]
            diagnostics = pending["diagnostics"]

            result = {
                "statement": statement,
                "category_type": None,
                "theme_id": None,
                "similarity_score": best_similarity,
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
                qualifying_llm = _finalize_qualifying_themes(resolved_items, is_discussion)

            result["confidence_score"] = llm_confidence
            diagnostics["llm_fallback"]["confidence_score"] = llm_confidence
            # Complete raw LLM response for this batch call, stored on every statement that
            # went through the fallback (not just the entries that ended up qualifying) so
            # the full context is available for audit/debugging from any one row. Reaching
            # this point at all means the try block above succeeded, so llm_result is
            # always set (a batch call failure re-raises instead of reaching Step 9).
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
                    await insert_analysis_result(
                        conn,
                        submission_id=submission_id,
                        tenant_code=tenant_code,
                        theme_id=item["theme_id"],
                        analysis_type="theme",
                        statement_type=statement_type,
                        category_type="Standard",
                        confidence_score=item["confidence_score"],
                        model_confidence_score=best_similarity,
                        justification=item["justification"],
                        multi_theme_mapped=is_multi,
                        meta_data=diagnostics,
                    )

                theme_names = [theme_id_to_info.get(item["theme_id"], {}).get("name", "?") for item in qualifying_llm]
                logger.info(
                    f"LLM match{'es' if is_multi else ''} (batched): '{statement[:60]}...' → {theme_names} "
                    f"(conf={[round(item['confidence_score'], 2) for item in qualifying_llm]})"
                )
            else:
                # Others — vague, off-taxonomy, low confidence, or the batch call failed entirely
                result["category_type"] = "Others"
                await insert_analysis_result(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    theme_id=None,
                    analysis_type="theme",
                    statement_type=statement_type,
                    category_type="Others",
                    confidence_score=llm_confidence,
                    model_confidence_score=best_similarity,
                    justification=llm_justification,
                    meta_data=diagnostics,
                )
                logger.info(f"Statement marked Others (low confidence, batched): {statement[:80]}...")

            results.append(result)

    return results


@activity.defn
async def thematic_classification_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that performs category-type-gated thematic classification.

    Pipeline (per statement, via _run_local_classification):
      Step 1:  Read column config, extract text
      Step 1b: For discussions, each TEXT[] array element is its own statement (no parsing needed)
      Step 2:  Word-count / garbage gate → Unknown/Unclear
      Step 3:  Safety check (no LLM) → Flagged
      Step 4:  STOP point for Unknown/Flagged
      Step 5:  Fetch approved themes
      Step 6:  Local embedding classification → Standard, or queued for fallback

    Then once for the whole submission (via _run_batched_llm_fallback), covering every
    statement across every column that didn't clear the local threshold:
      Step 7:  Build ONE LLM prompt listing all queued statements by index
      Step 8:  Call LLM once, parse confidence per statement_index
      Step 9:  Finalize category_type per statement (Standard / Others)
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    target_columns = params["target_columns"]
    analysis_type = params.get("analysis_type", "thematic_classification")
    resolved_model = params.get("llm_model") or settings.OPENROUTER_MODEL
    resolved_max_tokens = params.get("max_tokens") or settings.LLM_MAX_TOKENS
    resolved_timeout = params.get("llm_timeout_seconds") or settings.LLM_TIMEOUT_SECONDS

    if not target_columns:
        return {"status": "skipped", "reason": "no columns specified"}

    async with db.pool.acquire() as conn:
        sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)

        # --- Step 5: Fetch approved themes (once per activity invocation) ---
        warnings = []
        approved_themes = await _fetch_approved_themes(conn)
        if not approved_themes:
            warn_msg = "No approved themes found in database. All statements will go to LLM fallback."
            logger.warning(warn_msg)
            warnings.append(warn_msg)

        # Build embedding vectors for approved themes (once). Same as below — this
        # calls SentenceTransformer.encode() synchronously, so it's offloaded to a
        # thread rather than blocking the event loop.
        theme_id_to_info = {str(t["id"]): t for t in approved_themes}
        theme_vectors = await asyncio.to_thread(build_theme_embeddings, approved_themes) if approved_themes else {}

        abusive_masked_at = payload.get("abusive_masked_at") or []

        # Clear existing analysis results for this submission's theme analysis
        await conn.execute(
            "DELETE FROM analysis_results WHERE submission_id = $1 AND tenant_code = $2 AND analysis_type = 'theme'",
            submission_id, tenant_code
        )

        all_results = []
        # Statements that don't clear the local embedding threshold anywhere in this
        # submission are batched into a single LLM call at the end, instead of one
        # call per statement — the fixed cost of the prompt (rules + the full
        # approved-themes catalog) is then paid once per submission, not once per
        # statement needing the fallback.
        pending_items = []

        for col in target_columns:
            # Map column name to DB column
            db_col = "challenge" if col == "challenges" and sub_type == "story" else col
            raw_value = payload.get(db_col)
            if not raw_value:
                continue

            # --- Step 1b: Build the list of statements to classify ---
            is_discussion = "discussion" in sub_type
            # Branch on the column's actual runtime shape, not submission type —
            # TEXT[] columns (discussion's challenges/solutions, story's
            # challenge/action_steps; see operations.py's _normalize_statement_list)
            # come back from asyncpg as a native Python list, one element per
            # discrete statement. No delimiter/JSON parsing needed (and none would
            # be safe, since a statement could legitimately contain any given
            # delimiter character itself). Scalar TEXT columns (e.g. a story's
            # objective) are processed as a single unit.
            if isinstance(raw_value, list):
                statements = [str(s).strip() for s in raw_value if s and str(s).strip()]
            else:
                raw_text = str(raw_value).strip()
                statements = [raw_text] if raw_text else []

            if not statements:
                continue

            for statement in statements:
                finished_result, pending_item = await _run_local_classification(
                    conn=conn,
                    statement=statement,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    statement_type=col,
                    theme_vectors=theme_vectors,
                    theme_id_to_info=theme_id_to_info,
                    abusive_masked_at=abusive_masked_at,
                    is_discussion=is_discussion,
                )
                if finished_result is not None:
                    all_results.append(finished_result)
                else:
                    pending_items.append(pending_item)

    # conn released above — _run_batched_llm_fallback manages its own connection
    # scopes internally so no pool connection is held idle during its LLM call.
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

    # Summary
    quality_counts = {}
    multi_theme_statement_count = 0
    for r in all_results:
        q = r.get("category_type", "unknown")
        quality_counts[q] = quality_counts.get(q, 0) + 1
        if len(r.get("matched_themes") or []) > 1:
            multi_theme_statement_count += 1

    return {
        "status": "success",
        "total_statements": len(all_results),
        "quality_breakdown": quality_counts,
        "multi_theme_statement_count": multi_theme_statement_count,
        "warnings": warnings,
        "results": all_results,
    }
