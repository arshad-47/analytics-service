"""
Local embedding-based theme classifier — Step 6 of the thematic classification pipeline.

Uses SentenceTransformer (all-MiniLM-L6-v2 by default) to encode statements
and approved themes, then computes cosine similarity to find the best match.
"""
import logging
import threading
import torch
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from setfit import SetFitModel
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from app.config import settings

logger = logging.getLogger("analytics_service.services.classifier")

# Module-level model cache — loaded once per worker process
_model: Optional[SentenceTransformer] = None
# Real OS-thread lock, not asyncio.Lock — this is called from separate threads
# via asyncio.to_thread (build_theme_embeddings/get_theme_similarities), not
# concurrently within one event loop.
_model_lock = threading.Lock()


def _get_model() -> SentenceTransformer:
    global _model
    if _model is not None:
        return _model
    with _model_lock:
        if _model is None:
            model_name = settings.EMBEDDING_MODEL_NAME
            logger.info(f"Loading SentenceTransformer model '{model_name}'...")
            _model = SentenceTransformer(model_name)
            logger.info(f"SentenceTransformer model '{model_name}' loaded successfully.")
    return _model


def build_theme_embeddings(approved_themes: List[Dict[str, Any]]) -> Dict[str, np.ndarray]:
    model = _get_model()
    theme_vectors: Dict[str, np.ndarray] = {}

    for theme in approved_themes:
        theme_id = str(theme["id"])
        name = theme.get("name", "") or ""
        definition = theme.get("definitions", "") or theme.get("definition", "") or ""
        keywords = theme.get("keywords", "") or ""
        examples = theme.get("examples", "") or ""

        base_text = f"Theme: {name}. Definition: {definition}. Keywords: {keywords}."
        base_emb = np.array(model.encode(base_text)).reshape(1, -1)

        example_list = [ex.strip() for ex in examples.split("|") if ex.strip()]
        if example_list:
            example_embs = np.array(model.encode(example_list))
            vectors = np.vstack([base_emb, example_embs])
        else:
            vectors = base_emb

        theme_vectors[theme_id] = vectors

    return theme_vectors


def get_theme_similarities(
    statement: str,
    theme_vectors: Dict[str, np.ndarray],
) -> List[Tuple[str, float]]:
    """
    Computes each theme's best (max) cosine similarity to the statement.

    Returns a list of (theme_id, similarity_score) sorted by score descending.
    Empty list if no themes available.
    """
    if not theme_vectors:
        return []

    model = _get_model()
    stmt_emb = model.encode(statement).reshape(1, -1)

    scores: List[Tuple[str, float]] = []
    for theme_id, vectors in theme_vectors.items():
        sims = cosine_similarity(stmt_emb, vectors)[0]
        scores.append((theme_id, float(np.max(sims))))

    scores.sort(key=lambda pair: pair[1], reverse=True)
    return scores


def classify_statement(
    statement: str,
    theme_vectors: Dict[str, np.ndarray],
    theme_id_to_info: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], float]:
    scores = get_theme_similarities(statement, theme_vectors)
    if not scores:
        return None, 0.0
    return scores[0]


# Shared SetFit Model Loader & Batch Predictor

_setfit_models_cache: Dict[Tuple[str, str], Any] = {}
_setfit_models_lock = threading.Lock()


def load_setfit_model(model_id: str, revision: str = "main"):
    cache_key = (model_id, revision)
    if cache_key not in _setfit_models_cache:
        with _setfit_models_lock:
            if cache_key not in _setfit_models_cache:
                logger.info(f"Loading SetFit model '{model_id}' (revision={revision})...")
                model = SetFitModel.from_pretrained(model_id, revision=revision)
                if revision != "main":
                    model.model_body = SentenceTransformer(model_id, revision=revision)
                    logger.info(
                        f"Patched model body for '{model_id}' revision='{revision}' "
                        f"(embedding dim: {model.model_body.get_sentence_embedding_dimension()})."
                    )
                _setfit_models_cache[cache_key] = model
                logger.info(f"SetFit model '{model_id}' loaded successfully.")
    return _setfit_models_cache[cache_key]


def predict_setfit_batch(model, texts: List[str]) -> Tuple[List[str], List[float]]:
    raw_probs = model.predict_proba(texts)
    if hasattr(raw_probs, "cpu"):
        probs = raw_probs.cpu().numpy()
    else:
        probs = np.asarray(raw_probs)
        
    pred_idx = probs.argmax(axis=1)
    confs = probs.max(axis=1).tolist()
    preds = [str(model.labels[i]) for i in pred_idx]
    return preds, confs
