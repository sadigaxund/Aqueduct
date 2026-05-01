"""Tests for the Surveyor webhook dispatcher."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import httpx

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
