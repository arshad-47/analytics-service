"""
app/services/llm.py must not write respondents' words to the logs or into error messages by default: the prompt is
not logged (model and size only), the provider's error body is neither logged nor put in the exception, and a
clearly named opt-in setting (LLM_LOG_PAYLOADS, off by default) restores the full debug output. The HTTP layer is
FAKED (urlopen is patched): no network, no real LLM call. SYNTHETIC data only.
"""
import io
import json
import logging
import urllib.error

import pytest

import app.services.llm as llm
from app.config import settings

SECRET = "zzq-child-words-9876543210"


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return json.dumps(self._payload).encode("utf-8")


@pytest.fixture(autouse=True)
def _key(monkeypatch):
    monkeypatch.setattr(settings, "OPENROUTER_API_KEY", "test-key-not-real")
    monkeypatch.setattr(settings, "LLM_LOG_PAYLOADS", False)


def _http_error(code, body):
    return urllib.error.HTTPError("http://example.invalid", code, "err", {}, io.BytesIO(body.encode("utf-8")))


def test_the_setting_exists_and_is_off_by_default():
    assert type(settings).model_fields["LLM_LOG_PAYLOADS"].default is False
    assert "LLM_LOG_PAYLOADS=false" in open(".env.example", encoding="utf-8").read()


def test_the_prompt_is_not_logged_even_at_debug(monkeypatch, caplog):
    monkeypatch.setattr(llm.urllib.request, "urlopen",
                        lambda req, timeout=None: _Response({"choices": [{"message": {"content": "ok"}}], "usage": {}}))
    with caplog.at_level(logging.DEBUG):
        content, _ = llm.openrouter_chat_completion(f"prompt with {SECRET}", model="m1", max_tokens=5, timeout=3)
    assert content == "ok"
    assert SECRET not in caplog.text
    assert "model=m1" in caplog.text and "prompt_chars=" in caplog.text      # model and size are logged


def test_the_provider_error_body_is_neither_logged_nor_in_the_exception(monkeypatch, caplog):
    def boom(req, timeout=None):
        raise _http_error(403, json.dumps({"error": {"message": "flagged", "flagged_input": SECRET}}))

    monkeypatch.setattr(llm.urllib.request, "urlopen", boom)
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(RuntimeError) as caught:
            llm.openrouter_chat_completion(f"prompt {SECRET}", model="m1")
    assert SECRET not in caplog.text and SECRET not in str(caught.value) and "flagged" not in str(caught.value)
    assert "HTTP 403" in str(caught.value) and "status=403" in caplog.text and "model=m1" in caplog.text
    assert caught.value.__cause__ is None and caught.value.__suppress_context__ is True      # the HTTPError (with its body) is not chained


def test_the_status_survives_for_the_extraction_error_sanitiser(monkeypatch):
    from app.database.feedback_extraction_operations import sanitize_llm_error

    def boom(req, timeout=None):
        raise _http_error(429, SECRET)

    monkeypatch.setattr(llm.urllib.request, "urlopen", boom)
    with pytest.raises(RuntimeError) as caught:
        llm.openrouter_chat_completion("p")
    failure = sanitize_llm_error(caught.value)
    assert str(failure) == "LLM call failed (RuntimeError, HTTP 429)" and failure.status == 429


def test_a_connection_error_names_only_its_class(monkeypatch, caplog):
    def boom(req, timeout=None):
        raise urllib.error.URLError(f"cannot reach host for {SECRET}")

    monkeypatch.setattr(llm.urllib.request, "urlopen", boom)
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(RuntimeError) as caught:
            llm.openrouter_chat_completion("p")
    assert "URLError" in str(caught.value) and SECRET not in str(caught.value) and SECRET not in caplog.text


def test_an_unexpected_response_shape_does_not_echo_the_content(monkeypatch):
    monkeypatch.setattr(llm.urllib.request, "urlopen", lambda req, timeout=None: _Response({"data": SECRET}))
    with pytest.raises(RuntimeError, match="Unexpected OpenRouter response shape") as caught:
        llm.openrouter_chat_completion("p")
    assert SECRET not in str(caught.value) and "data" in str(caught.value)


def test_the_opt_in_setting_turns_the_payload_logging_back_on(monkeypatch, caplog):
    monkeypatch.setattr(settings, "LLM_LOG_PAYLOADS", True)

    def boom(req, timeout=None):
        raise _http_error(400, f"body {SECRET}")

    monkeypatch.setattr(llm.urllib.request, "urlopen", boom)
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(RuntimeError) as caught:
            llm.openrouter_chat_completion(f"prompt {SECRET}")
    assert caplog.text.count(SECRET) == 2          # the prompt (DEBUG) and the error body (ERROR)
    assert SECRET not in str(caught.value)         # the exception stays clean even then (it reaches Temporal and llm_logs)


def test_a_normal_call_still_returns_content_and_usage(monkeypatch):
    sent = {}

    def fake(req, timeout=None):
        sent["body"] = json.loads(req.data.decode("utf-8"))
        sent["timeout"] = timeout
        return _Response({"choices": [{"message": {"content": "  hello  "}}], "usage": {"prompt_tokens": 2}})

    monkeypatch.setattr(llm.urllib.request, "urlopen", fake)
    assert llm.openrouter_chat_completion("p", model="m2", max_tokens=7, timeout=9) == ("hello", {"prompt_tokens": 2})
    assert sent["body"]["model"] == "m2" and sent["body"]["max_tokens"] == 7 and sent["timeout"] == 9
