"""Tests for the Surveyor webhook dispatcher."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import httpx

import pytest
pytestmark = pytest.mark.unit

from aqueduct.config import WebhookEndpointConfig
from aqueduct.surveyor.webhook import fire_webhook


def _cfg(url: str, **kwargs) -> WebhookEndpointConfig:
    return WebhookEndpointConfig(url=url, **kwargs)


def test_fire_webhook_threading_behavior():
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=200)

        thread = fire_webhook(_cfg("http://example.com/hook"), {"foo": "bar"})

        assert isinstance(thread, threading.Thread)
        assert thread.daemon is True
        thread.join(timeout=2)


def test_fire_webhook_payload_verification():
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=200)

        payload = {"run_id": "abc", "status": "error"}
        fire_webhook(_cfg("http://api.test/webhook"), payload).join(timeout=2)

        mock_req.assert_called_once()
        args, kwargs = mock_req.call_args
        # httpx.request(method, url, json=..., headers=..., timeout=...)
        assert args[1] == "http://api.test/webhook"
        assert kwargs["json"] == payload
        assert kwargs["headers"]["Content-Type"] == "application/json"
        assert args[0] == "POST"


def test_fire_webhook_server_error_logged(capsys):
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=500)

        fire_webhook(_cfg("http://fail.test"), {"a": 1}).join(timeout=2)

        captured = capsys.readouterr()
        assert "500" in captured.err


def test_fire_webhook_url_error_swallowed(capsys):
    with patch("httpx.request", side_effect=httpx.ConnectError("DNS failure")):
        fire_webhook(_cfg("http://non-existent"), {"a": 1}).join(timeout=2)

        captured = capsys.readouterr()
        assert "failed" in captured.err


def test_fire_webhook_generic_exception_swallowed(capsys):
    with patch("httpx.request", side_effect=RuntimeError("Panic!")):
        fire_webhook(_cfg("http://panic.test"), {"a": 1}).join(timeout=2)

        captured = capsys.readouterr()
        assert "Panic!" in captured.err


def test_fire_webhook_template_vars_resolution():
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=200)

        config = WebhookEndpointConfig(
            url="http://test.com",
            method="PUT",
            headers={"Authorization": "Bearer ${AUTH_TOKEN}"},
            payload={"msg": "Blueprint ${blueprint_name} failed in ${failed_module}"},
        )
        template_vars = {
            "blueprint_name": "MyBlueprint",
            "failed_module": "raw_users",
            "AUTH_TOKEN": "secret-123"
        }

        fire_webhook(config, {}, template_vars=template_vars).join(timeout=2)

        mock_req.assert_called_once()
        _, kwargs = mock_req.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer secret-123"
        assert kwargs["json"]["msg"] == "Blueprint MyBlueprint failed in raw_users"


def test_fire_webhook_environ_fallback(monkeypatch):
    monkeypatch.setenv("GLOBAL_KEY", "env-value")
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=200)

        config = WebhookEndpointConfig(
            url="http://test.com",
            payload={"key": "${GLOBAL_KEY}"}
        )

        fire_webhook(config, {}).join(timeout=2)

        assert mock_req.call_args[1]["json"]["key"] == "env-value"


# ── Phase 46 — envelope format + delivery retry ──────────────────────────────

def test_fire_webhook_envelope_format():
    """payload: null + event= → standardized envelope."""
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=200)
        fire_webhook(
            _cfg("http://env.test"),
            full_payload={"run_id": "abc", "status": "error"},
            template_vars={"run_id": "abc"},
            event="on_failure",
        ).join(timeout=2)

        _, kwargs = mock_req.call_args
        body = kwargs["json"]
        assert body["event"] == "on_failure"
        assert "timestamp" in body
        assert body["run_id"] == "abc"
        assert body["data"]["run_id"] == "abc"
        assert body["data"]["status"] == "error"


def test_fire_webhook_no_event_legacy_raw_payload():
    """event=None → raw payload sent as-is (no envelope)."""
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=200)
        fire_webhook(
            _cfg("http://raw.test"),
            full_payload={"run_id": "abc", "status": "error"},
        ).join(timeout=2)

        _, kwargs = mock_req.call_args
        assert kwargs["json"] == {"run_id": "abc", "status": "error"}
        assert "event" not in kwargs["json"]


def test_fire_webhook_custom_payload_wins_over_envelope():
    """Explicit payload: template wins — no envelope wrapping."""
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=200)
        config = WebhookEndpointConfig(
            url="http://custom.test",
            payload={"custom": "data", "run_id": "${run_id}"},
        )
        fire_webhook(config, {"ignored": "payload"}, template_vars={"run_id": "r-1"}, event="on_failure").join(timeout=2)

        _, kwargs = mock_req.call_args
        body = kwargs["json"]
        assert body["custom"] == "data"
        assert body["run_id"] == "r-1"
        assert "event" not in body


def test_fire_webhook_retry_429(capsys):
    """429 → retried once (2 total attempts)."""
    with patch("httpx.request") as mock_req:
        mock_req.side_effect = [
            MagicMock(status_code=429, headers={}),
            MagicMock(status_code=200),
        ]
        fire_webhook(_cfg("http://retry.test"), {"a": 1}).join(timeout=5)
        assert mock_req.call_count == 2
        captured = capsys.readouterr()
        assert "retrying" in captured.err


def test_fire_webhook_retry_500(capsys):
    """500 → retried once."""
    with patch("httpx.request") as mock_req:
        mock_req.side_effect = [
            MagicMock(status_code=500, headers={}),
            MagicMock(status_code=200),
        ]
        fire_webhook(_cfg("http://retry500.test"), {"a": 1}).join(timeout=5)
        assert mock_req.call_count == 2


def test_fire_webhook_non_retryable_4xx_no_retry(capsys):
    """400 → single attempt, no retry."""
    with patch("httpx.request") as mock_req:
        mock_req.return_value = MagicMock(status_code=400)
        fire_webhook(_cfg("http://badreq.test"), {"a": 1}).join(timeout=5)
        assert mock_req.call_count == 1
        captured = capsys.readouterr()
        assert "400" in captured.err


def test_fire_webhook_network_error_retried(capsys):
    """httpx.RequestError → retried once."""
    with patch("httpx.request") as mock_req:
        mock_req.side_effect = [
            httpx.ConnectError("DNS fail"),
            MagicMock(status_code=200),
        ]
        fire_webhook(_cfg("http://network.test"), {"a": 1}).join(timeout=5)
        assert mock_req.call_count == 2
        captured = capsys.readouterr()
        assert "retrying" in captured.err
