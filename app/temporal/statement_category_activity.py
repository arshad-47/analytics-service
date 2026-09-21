import asyncio
import json
import logging
from typing import Any, Dict

from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    fetch_child_statements,
    fetch_statements_for_submission,
    insert_analysis_result,
    update_submission_status,
)

from app.services.classifier import load_setfit_model, predict_setfit_batch

logger = logging.getLogger("analytics_service.temporal.statement_category")


async def _get_statement_category_prompt(conn) -> Dict[str, Any]:
    row = await conn.fetchrow("""
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.name = 'Statement Category' AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC LIMIT 1
    """)
    if not row:
        raise RuntimeError("No active statement category prompt version found in the database.")
    return dict(row)


def _llm_classify(
    text: str,
    system_prompt: str,
    user_prompt: str,
    model: str = None,
    max_tokens: int = None,
    timeout: int = None,
):
    """Call the LLM to classify a single statement (blocking)."""
    from app.services.llm import openrouter_chat_completion

    u_prompt = user_prompt.replace("{{text}}", text)
    prompt = f"{system_prompt}\n\n{u_prompt}"
    response_text, usage = openrouter_chat_completion(
        prompt,
        model=model,
        max_tokens=max_tokens,
        timeout=timeout,
    )

    # Clean markdown wrappers if present
    cleaned = response_text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines[0].startswith("```json") or lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM response: %s (raw: %s)", e, response_text[:200])
        result = {"category": "Other", "confidence": 0.0, "justification": "LLM parse error"}

    return result, usage


@activity.defn
async def statement_category_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity: classifies each statement for a submission using a
    SetFit model, falling back to LLM when confidence is below threshold.
    Results are stored in the analysis_results table.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    llm_model = params.get("llm_model")
    max_tokens = params.get("max_tokens")
    llm_timeout_seconds = params.get("llm_timeout_seconds")
    analysis_type = params.get("analysis_type", "statement_category")
    thresholds = settings.SETFIT_CONFIDENCE_THRESHOLD

    logger.info(
        "Starting statement categorization for submission=%s tenant=%s (thresholds=%s)",
        submission_id, tenant_code, thresholds,
    )

    # 1. Fetch original statements (skip duplicates with parent_id set)
    async with db.pool.acquire() as conn:
        statements = await fetch_statements_for_submission(conn, submission_id, tenant_code)

    if not statements:
        logger.info("No statements found for submission=%s — skipping.", submission_id)
        return {"status": "skipped", "reason": "no statements found"}

    logger.info("Found %d statements to classify for submission=%s", len(statements), submission_id)

    # 1.5. Fetch the LLM prompt from database
    async with db.pool.acquire() as conn:
        prompt_data = await _get_statement_category_prompt(conn)
    system_prompt = prompt_data["system_prompt"]
    user_prompt = prompt_data["user_prompt"]

    # 2. Load the SetFit model (blocking — run in thread)
    model = await asyncio.to_thread(
        load_setfit_model,
        settings.SETFIT_MODEL_ID,
        settings.SETFIT_MODEL_VERSION,
    )

    # 3. Batch predict with SetFit (blocking — run in thread)
    texts = [s["raw_statement"] for s in statements]
    predictions, confidence_scores = await asyncio.to_thread(predict_setfit_batch, model, texts)

    # 4. Identify statements needing LLM fallback
    results = []
    fallback_tasks = []
    fallback_indices = []
    semaphore = asyncio.Semaphore(5)

    async def _bounded_llm_call(stmt_id, text, sys_prompt, usr_prompt, model, max_tok, timeout):
        async with semaphore:
            try:
                res, _ = await asyncio.to_thread(
                    _llm_classify,
                    text,
                    sys_prompt,
                    usr_prompt,
                    model,
                    max_tok,
                    timeout,
                )
                return res
            except Exception as e:
                logger.error("LLM fallback failed for statement [%s]: %s", stmt_id, e)
                return {"category": "Other", "confidence": 0.0, "justification": f"LLM error: {e}"}

    for i, stmt in enumerate(statements):
        model_pred = str(predictions[i])
        model_conf = float(confidence_scores[i])

        category_key = model_pred.lower()
        current_threshold = thresholds.get(category_key, 0.80)

        # Initialize result template
        result_dict = {
            "statement_id": stmt["id"],
            "statement_type": stmt["statement_type"],
            "model_pred": model_pred,
            "model_conf": model_conf,
            "llm_pred": None,
            "llm_conf": None,
            "justification": None,
            "final_category": model_pred,
            "current_threshold": current_threshold,
        }
        results.append(result_dict)

        if model_conf < current_threshold:
            logger.info(
                "Statement [%s] confidence %.2f < threshold %.2f — scheduling LLM fallback.",
                stmt["id"], model_conf, current_threshold,
            )
            fallback_indices.append(i)
            fallback_tasks.append(
                _bounded_llm_call(
                    stmt["id"],
                    stmt["raw_statement"],
                    system_prompt,
                    user_prompt,
                    llm_model,
                    max_tokens,
                    llm_timeout_seconds,
                )
            )

    VALID_CATEGORIES = {"Challenge", "Solution or Action", "Other"}
    _CATEGORY_LOOKUP = {c.lower(): c for c in VALID_CATEGORIES}

    # 4.5 Execute LLM fallbacks concurrently
    if fallback_tasks:
        logger.info("Executing %d LLM fallback calls concurrently...", len(fallback_tasks))
        fallback_results = await asyncio.gather(*fallback_tasks)
        
        # Merge back into results
        for fallback_idx, llm_res in zip(fallback_indices, fallback_results):
            stmt_id = results[fallback_idx]["statement_id"]
            
            # Validate Category
            raw_pred = str(llm_res.get("category") or "").strip()
            llm_pred = _CATEGORY_LOOKUP.get(raw_pred.lower(), "Other")
            if llm_pred != raw_pred:
                logger.warning(
                    "LLM returned unknown category %r for statement [%s] — defaulting to 'Other'.",
                    raw_pred, stmt_id,
                )
            
            # Validate Confidence
            try:
                llm_conf = min(max(float(llm_res.get("confidence") or 0.0), 0.0), 1.0)
            except (TypeError, ValueError):
                llm_conf = 0.0

            results[fallback_idx]["llm_pred"] = llm_pred
            results[fallback_idx]["llm_conf"] = llm_conf
            results[fallback_idx]["justification"] = llm_res.get("justification")
            
            if llm_pred:
                results[fallback_idx]["final_category"] = llm_pred

    # 5. Bulk insert into analysis_results (parent statements only)
    child_copy_count = 0
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            # Idempotency: Clear any existing statement_category results for this submission
            await conn.execute(
                """
                DELETE FROM analysis_results 
                WHERE submission_id = $1 
                  AND tenant_code = $2 
                  AND analysis_type = $3
                """,
                submission_id,
                tenant_code,
                analysis_type,
            )

            for r in results:
                await insert_analysis_result(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    statement_id=r["statement_id"],
                    analysis_type=analysis_type,
                    analysis_column=[r["statement_type"]],
                    ml_model_name=settings.SETFIT_MODEL_ID,
                    ml_model_version=settings.SETFIT_MODEL_VERSION,
                    model_confidence_score=r["model_conf"],
                    model_prediction=r["model_pred"],
                    llm_confidence_score=r["llm_conf"],
                    llm_prediction=r["llm_pred"],
                    threshold=r.get("current_threshold", 0.80),
                    justification=r["justification"],
                )

                # 5a. Propagate the same result to any duplicate (child) statements so
                #     every statement_id has its own analysis_results row.  Consumers
                #     never need to walk parent_id chains to read classification output.
                children = await fetch_child_statements(
                    conn,
                    parent_statement_id=r["statement_id"],
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                )
                for child in children:
                    await insert_analysis_result(
                        conn,
                        submission_id=submission_id,
                        tenant_code=tenant_code,
                        statement_id=child["id"],
                        analysis_type=analysis_type,
                        analysis_column=[child["statement_type"]],
                        ml_model_name=settings.SETFIT_MODEL_ID,
                        ml_model_version=settings.SETFIT_MODEL_VERSION,
                        model_confidence_score=r["model_conf"],
                        model_prediction=r["model_pred"],
                        llm_confidence_score=r["llm_conf"],
                        llm_prediction=r["llm_pred"],
                        threshold=r.get("current_threshold", 0.80),
                        justification=r["justification"],
                        meta_data={"deduped_from": str(r["statement_id"])},
                    )
                    child_copy_count += 1
                    logger.debug(
                        "Copied statement_category result from parent [%s] to child [%s]",
                        r["statement_id"], child["id"],
                    )

    model_only_count = sum(1 for r in results if not r["llm_pred"])
    llm_fallback_count = sum(1 for r in results if r["llm_pred"])

    logger.info(
        "Statement categorization complete for submission=%s: %d total, %d model-only, %d LLM-fallback, %d child copies",
        submission_id, len(results), model_only_count, llm_fallback_count, child_copy_count,
    )

    return {
        "status": "success",
        "total": len(results),
        "model_only": model_only_count,
        "llm_fallback": llm_fallback_count,
        "child_copies": child_copy_count,
    }
