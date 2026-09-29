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
from huggingface_hub import snapshot_download

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
                # SetFitModel.from_pretrained has a bug: it builds the SentenceTransformer
                # body by calling SentenceTransformer(model_id) without forwarding the
                # `revision` argument, so it always reads from 'main' regardless of what
                # revision you requested.
                #
                # Permanent fix: use snapshot_download to resolve the exact local
                # directory for the requested revision (downloads on first use, then
                # returns the cached path instantly on subsequent calls). Passing the
                # local snapshot path to from_pretrained forces SetFit to read every
                # file — including the SentenceTransformer body — from that exact
                # revision's directory, so 'main', 'v2', or any other tag each get
                # their own isolated, correct snapshot.
                snapshot_path = snapshot_download(repo_id=model_id, revision=revision)
                model = SetFitModel.from_pretrained(snapshot_path)
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


# Shared HuggingFace Sequence Classification Model Loader & Batch Predictor
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import json
import os

_hf_models_cache: Dict[Tuple[str, str], Any] = {}
_hf_models_lock = threading.Lock()


def load_hf_sequence_classification_model(model_id: str, revision: str = "main"):
    cache_key = (model_id, revision)
    if cache_key not in _hf_models_cache:
        with _hf_models_lock:
            if cache_key not in _hf_models_cache:
                logger.info(f"Loading HF Multi-Theme model '{model_id}' (revision={revision})...")
                snapshot_path = snapshot_download(repo_id=model_id, revision=revision)
                device = "cuda" if torch.cuda.is_available() else "cpu"
                tokenizer = AutoTokenizer.from_pretrained(snapshot_path)
                model = AutoModelForSequenceClassification.from_pretrained(snapshot_path)
                model.to(device)
                model.eval()
                
                # Try to load label_config.json if available
                class_names = []
                label_cfg_path = os.path.join(snapshot_path, "label_config.json")
                if os.path.exists(label_cfg_path):
                    with open(label_cfg_path, "r") as f:
                        cfg = json.load(f)
                        class_names = cfg.get("labels", [])
                
                if not class_names:
                    class_names = [
                        model.config.id2label[i] if i in model.config.id2label else model.config.id2label[str(i)]
                        for i in range(model.config.num_labels)
                    ]
                if len(class_names) != model.config.num_labels:
                    raise ValueError(f"Label count mismatch: {len(class_names)} class names for {model.config.num_labels} model labels.")
                    
                _hf_models_cache[cache_key] = {
                    "tokenizer": tokenizer,
                    "model": model,
                    "device": device,
                    "class_names": class_names
                }
                logger.info(f"HF Multi-Theme model '{model_id}' loaded successfully.")
    return _hf_models_cache[cache_key]


def predict_hf_multi_theme_batch(
    tokenizer,
    model,
    device,
    class_names,
    texts: List[str]
) -> Tuple[List[List[str]], List[List[float]]]:
    """
    Predicts multiple themes per text based on TOP_K and MIN_CONFIDENCE from settings.
    """
    top_k = settings.HF_THEME_TOP_K
    min_confidence = settings.HF_THEME_MIN_CONFIDENCE
    
    # Fill empty strings to avoid model crashes
    safe_texts = [str(t) if t else "" for t in texts]
    
    with torch.no_grad():
        inputs = tokenizer(
            safe_texts, 
            padding=True, 
            truncation=True, 
            max_length=128, 
            return_tensors="pt"
        )
        inputs = {k: v.to(device) for k, v in inputs.items()}
        outputs = model(**inputs)
        # Apply sigmoid to convert raw logits to probabilities (0.0 to 1.0)
        probabilities = torch.sigmoid(outputs.logits).cpu().numpy()
        
    batch_multi_themes = []
    batch_multi_confs = []
    
    for probs in probabilities:
        top_indexes = np.argsort(probs)[-top_k:][::-1]
        themes = []
        confs = []
        
        for i, idx in enumerate(top_indexes):
            theme = class_names[idx]
            conf = float(probs[idx])
            
            if conf < min_confidence:
                break
                
            themes.append(theme)
            confs.append(conf)
            
        # 4. Mutually Exclusive Filter for 'Unknown/Unclear'
        if len(themes) > 1 and "Unknown/Unclear" in themes:
            unclear_idx = themes.index("Unknown/Unclear")
            if unclear_idx == 0:
                themes = [themes[0]]
                confs = [confs[0]]
            else:
                themes.pop(unclear_idx)
                confs.pop(unclear_idx)
                
        batch_multi_themes.append(themes)
        batch_multi_confs.append(confs)
        
    return batch_multi_themes, batch_multi_confs

