import asyncio
import numpy as np
from unittest.mock import AsyncMock, patch, MagicMock, call
import pytest
import torch

from app.services.classifier import predict_hf_multi_theme_batch
from app.temporal.thematic_activity import thematic_classification_activity, _run_batched_llm_fallback
from app.config import settings


def test_predict_hf_multi_theme_batch_top_k():
    # Mock settings
    settings.HF_THEME_TOP_K = 2
    settings.HF_THEME_MIN_CONFIDENCE = 0.2

    # Mock tokenizer and model
    tokenizer = MagicMock()
    tokenizer.return_value = {"input_ids": torch.tensor([[1]])}
    model = MagicMock()
    
    # We want to mock outputs.logits such that torch.sigmoid(outputs.logits) returns our desired probabilities.
    class_names = ["Theme A", "Theme B", "Theme C", "Theme D"]
    
    # Probabilities: 
    # Row 0: A=0.8, B=0.6, C=0.9, D=0.1
    # Expected: C=0.9, A=0.8 (since top_k is 2)
    p = np.array([[0.8, 0.6, 0.9, 0.1]])
    
    with patch("torch.sigmoid") as mock_sigmoid:
        mock_sigmoid.return_value.cpu.return_value.numpy.return_value = p
        
        themes, confs = predict_hf_multi_theme_batch(
            tokenizer, model, "cpu", class_names, ["test"]
        )
        
        assert len(themes) == 1
        assert themes[0] == ["Theme C", "Theme A"]
        assert np.allclose(confs[0], [0.9, 0.8])


def test_predict_hf_multi_theme_batch_mutual_exclusion():
    settings.HF_THEME_TOP_K = 3
    settings.HF_THEME_MIN_CONFIDENCE = 0.1
    tokenizer = MagicMock()
    tokenizer.return_value = {"input_ids": torch.tensor([[1], [2]])}
    model = MagicMock()
    
    class_names = ["Unknown/Unclear", "Theme A", "Theme B", "Theme C"]
    
    # Row 0: Unknown is top (0.9), Theme A is second (0.8) -> Should drop Theme A
    # Row 1: Theme A is top (0.9), Unknown is second (0.8) -> Should drop Unknown
    p = np.array([
        [0.9, 0.8, 0.2, 0.1],
        [0.8, 0.9, 0.2, 0.1]
    ])
    
    with patch("torch.sigmoid") as mock_sigmoid:
        mock_sigmoid.return_value.cpu.return_value.numpy.return_value = p
        
        themes, confs = predict_hf_multi_theme_batch(
            tokenizer, model, "cpu", class_names, ["test1", "test2"]
        )
        
        # Row 0
        assert themes[0] == ["Unknown/Unclear"]
        assert np.allclose(confs[0], [0.9])
        
        # Row 1
        assert themes[1] == ["Theme A", "Theme B"] # B (0.1) is third, Unknown was dropped
        assert "Unknown/Unclear" not in themes[1]


@pytest.mark.asyncio
async def test_run_batched_llm_fallback_persistence():
    # Test that _run_batched_llm_fallback correctly persists model attributes for the `else` branch
    conn = AsyncMock()
    submission_id = "sub1"
    tenant_code = "ten1"
    analysis_type = "thematic_classification"
    
    pending_items = [
        {
            "statement_id": "st1",
            "statement": "statement text",
            "model_pred": "Old Pred",
            "model_conf": 0.45,
            "model_threshold": 0.80,
            "ml_model_name": "MyModel",
            "ml_model_version": "v1",
            "raw_statement": "statement text",
            "statement_type": "challenge",
            "diagnostics": {"llm_fallback": {}}
        }
    ]
    
    mock_acquire = MagicMock()
    conn = AsyncMock()
    mock_acquire.return_value.__aenter__.return_value = conn

    with patch("app.temporal.thematic_activity.insert_analysis_result", new_callable=AsyncMock) as mock_insert, \
         patch("app.temporal.thematic_activity.insert_llm_log", new_callable=AsyncMock), \
         patch("app.temporal.thematic_activity._get_theme_classification_prompt", return_value={"id": 1, "system_prompt": "s", "user_prompt": "u"}), \
         patch("app.temporal.thematic_activity.db") as mock_db:
        
        mock_db.pool.acquire = mock_acquire
        
        with patch("app.temporal.thematic_activity.openrouter_chat_completion", return_value=('{"classified_data": []}', {"prompt_tokens": 1, "completion_tokens": 1})):
            # Force LLM to return empty, triggering `else` branch
            await _run_batched_llm_fallback(
                pending_items=pending_items,
                approved_themes=[],
                theme_id_to_info={},
                submission_id=submission_id,
                tenant_code=tenant_code,
                analysis_type=analysis_type,
                resolved_model="model",
                resolved_max_tokens=100,
                resolved_timeout=10
            )
            
            mock_insert.assert_called_once()
            kwargs = mock_insert.call_args.kwargs
            
            # Check persistence
            assert kwargs["category_type"] == "Others"
            assert kwargs["model_prediction"] == "Old Pred"
            assert kwargs["model_confidence_score"] == 0.45
            assert kwargs["threshold"] == 0.80
            assert kwargs["ml_model_name"] == "MyModel"
            assert kwargs["ml_model_version"] == "v1"


@pytest.mark.asyncio
async def test_hf_model_id_empty_fallback():
    # If HF_THEME_MODEL_ID is empty, it should use SetFit for all statements
    settings.HF_THEME_MODEL_ID = ""
    
    params = {
        "submission_id": "sub1",
        "tenant_code": "ten1"
    }
    
    mock_acquire = MagicMock()
    conn = AsyncMock()
    mock_acquire.return_value.__aenter__.return_value = conn
    
    # Mock dependencies
    with patch("app.temporal.thematic_activity.db") as mock_db, \
         patch("app.temporal.thematic_activity.copy_parent_analysis_results", return_value=0), \
         patch("app.temporal.thematic_activity.fetch_challenge_and_solution_statements_for_submission") as mock_fetch, \
         patch("app.temporal.thematic_activity._fetch_approved_themes", return_value=[{"id": 1, "name": "Theme A"}]), \
         patch("app.temporal.thematic_activity.load_setfit_model") as mock_load_setfit, \
         patch("app.temporal.thematic_activity.predict_setfit_batch", return_value=(["Theme A"], [0.9])), \
         patch("app.temporal.thematic_activity.load_hf_sequence_classification_model") as mock_load_hf:
        
        mock_db.pool.acquire = mock_acquire
        
        # Provide one discussion challenge statement
        mock_fetch.return_value = [{
            "id": "st1",
            "statement_id": "st1",
            "raw_statement": "test",
            "statement_type": "challenge",
            "submission_type": "discussion"
        }]
        
        with patch("app.temporal.thematic_activity.insert_analysis_result", new_callable=AsyncMock):
            await thematic_classification_activity(params)
            
            # HF shouldn't be loaded
            mock_load_hf.assert_not_called()
            
            # SetFit should be loaded and called for the statement
            mock_load_setfit.assert_called_once()
