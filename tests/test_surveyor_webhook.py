"""Tests for the Surveyor webhook dispatcher."""

from __future__ import annotations

import json
import threading
import urllib.error
from unittest.mock import MagicMock, patch

from aqueduct.surveyor.webhook import fire_webhook


def test_fire_webhook_threading_behavior():
    # Verify it returns a started daemon thread
    with patch("urllib.request.urlopen") as mock_url:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__.return_value = mock_resp
        mock_url.return_value = mock_resp

        payload = {"foo": "bar"}
        thread = fire_webhook("http://example.com/hook", payload)

        assert isinstance(thread, threading.Thread)
        assert thread.daemon is True
        assert thread.is_alive() or thread.ident is not None
        
        # Cleanup
        thread.join(timeout=1)


def test_fire_webhook_payload_verification():
    # Verify correct URL, headers, and JSON body
    with patch("urllib.request.Request") as mock_req_class, \
         patch("urllib.request.urlopen") as mock_url:
        
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__.return_value = mock_resp
        mock_url.return_value = mock_resp
        
        payload = {"run_id": "abc", "status": "error"}
        fire_webhook("http://api.test/webhook", payload).join(timeout=1)
        
        # Verification
        mock_req_class.assert_called_once()
        args, kwargs = mock_req_class.call_args
        assert args[0] == "http://api.test/webhook"
        assert json.loads(kwargs["data"].decode("utf-8")) == payload
        assert kwargs["headers"] == {"Content-Type": "application/json"}
        assert kwargs["method"] == "POST"


def test_fire_webhook_server_error_logged(capsys):
    # Verify HTTP 500 logs to stderr but doesn't raise
    with patch("urllib.request.urlopen") as mock_url:
        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_resp.__enter__.return_value = mock_resp
        mock_url.return_value = mock_resp
        
        fire_webhook("http://fail.test", {"a": 1}).join(timeout=1)
        
        captured = capsys.readouterr()
        assert "returned HTTP 500" in captured.err


def test_fire_webhook_url_error_swallowed(capsys):
    # Verify URLError (e.g. host not found) is swallowed and logged
    with patch("urllib.request.urlopen") as mock_url:
        mock_url.side_effect = urllib.error.URLError("DNS failure")
        
        fire_webhook("http://non-existent", {"a": 1}).join(timeout=1)
        
        captured = capsys.readouterr()
        assert "failed: DNS failure" in captured.err


def test_fire_webhook_generic_exception_swallowed(capsys):
    # Verify generic Exception is swallowed and logged
    with patch("urllib.request.urlopen") as mock_url:
        mock_url.side_effect = RuntimeError("Panic!")
        
        fire_webhook("http://panic.test", {"a": 1}).join(timeout=1)
        
        captured = capsys.readouterr()
        assert "raised unexpected error: Panic!" in captured.err
