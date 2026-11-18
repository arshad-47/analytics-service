"""
The seeded extraction configs carry their own `llm.max_tokens` (the config's `llm` block, not the .env): the masking call
writes every answer out again, so a long Kannada or Hindi session needs more than the 2048-token global default
(31 of 455 real General Public sessions were cut off at 2048). One value serves both calls of the activity.
"""
import pytest

from app.database.feedback_extraction_operations import resolve_llm_settings
from app.services import feedback_extraction as fx
from seed_support import PERSONAS, extraction_config_v1

MASKING_CEILING = 10000


@pytest.mark.parametrize("persona", sorted(PERSONAS))
def test_every_seeded_config_sets_its_own_max_tokens_and_it_beats_the_env_default(persona):
    config = fx.parse_extraction_config(extraction_config_v1(persona))
    assert config.llm["max_tokens"] == MASKING_CEILING
    assert resolve_llm_settings(config, {})["max_tokens"] == MASKING_CEILING


def test_a_workflow_step_override_still_wins_and_a_config_without_it_falls_back_to_the_env_default():
    config = fx.parse_extraction_config(extraction_config_v1("Youth"))
    assert resolve_llm_settings(config, {"max_tokens": 1234})["max_tokens"] == 1234
    raw = extraction_config_v1("Youth")
    raw["llm"] = {}
    from app.config import settings
    assert resolve_llm_settings(fx.parse_extraction_config(raw), {})["max_tokens"] == settings.LLM_MAX_TOKENS
