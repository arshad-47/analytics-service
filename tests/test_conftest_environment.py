"""
The environment defaults of tests/conftest.py: hermetic (an unroutable DATABASE_URL) for the normal suite, but the old
harness (tests/unit_testing.py) keeps the DATABASE_URL of the developer's configuration. Pure: only the decision
functions are called, with a fake argv and a fake environment (nothing is set in the real environment).
"""
import pytest

from conftest import (
    KEEP_DATABASE_URL_ENV_VAR,
    TEST_ENVIRONMENT,
    environment_defaults_for,
    invocation_targets_old_harness,
)


@pytest.mark.parametrize("argv", [
    ["tests/unit_testing.py"],
    ["tests/unit_testing.py", "-q"],
    ["-q", "tests/unit_testing.py::test_upload_003_valid_story_csv_uploads_successfully"],
    ["/abs/path/analytics-service/tests/unit_testing.py", "-k", "upload"],
    ["tests/unit_testing.py", "tests/test_feedback_consumer.py"],
])
def test_a_run_that_names_the_old_harness_keeps_the_configured_database_url(argv):
    assert invocation_targets_old_harness(argv, {}) is True
    defaults = environment_defaults_for(argv, {})
    assert "DATABASE_URL" not in defaults
    # every other default still applies
    assert defaults == {k: v for k, v in TEST_ENVIRONMENT.items() if k != "DATABASE_URL"}


@pytest.mark.parametrize("argv", [
    [],
    ["tests"],
    ["tests", "-q", "--ignore=tests/unit_testing.py", "--ignore=tests/test_hf_thematic.py"],
    ["tests", "--ignore", "tests/unit_testing.py"],
    ["tests", "--deselect", "tests/unit_testing.py::test_upload_003_valid_story_csv_uploads_successfully"],
    ["tests/test_feedback_consumer.py"],
    ["tests/not_unit_testing.py"],
    ["-k", "unit_testing"],
])
def test_every_other_run_keeps_the_hermetic_unroutable_database_url(argv):
    assert invocation_targets_old_harness(argv, {}) is False
    assert environment_defaults_for(argv, {}) == TEST_ENVIRONMENT
    assert "127.0.0.1:1" in environment_defaults_for(argv, {})["DATABASE_URL"]


def test_the_environment_variable_keeps_the_database_url_for_any_run():
    environ = {KEEP_DATABASE_URL_ENV_VAR: "1"}
    assert invocation_targets_old_harness(["tests"], environ) is True
    assert "DATABASE_URL" not in environment_defaults_for(["tests"], environ)
    assert invocation_targets_old_harness(["tests"], {KEEP_DATABASE_URL_ENV_VAR: "0"}) is False


def test_the_decision_does_not_change_the_real_environment():
    import os
    before = dict(os.environ)
    environment_defaults_for(["tests/unit_testing.py"])
    assert dict(os.environ) == before
