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
