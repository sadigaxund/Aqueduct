"""`webhooks.*.health_probe` — per-endpoint doctor probe depth (Phase 70).

Default changed from a real POST (`full`) to a non-mutating HTTP OPTIONS
request (`options`) so `aqueduct doctor` no longer risks triggering
consumer-side side effects on every webhook it checks. `connect` drops to a
TCP/TLS-only reachability probe with no HTTP request sent at all.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from aqueduct.config import WebhookEndpointConfig
from aqueduct.doctor.checks_io import check_webhook

pytestmark = pytest.mark.unit


class TestHealthProbeConfigField:
    def test_default_is_options(self):
        cfg = WebhookEndpointConfig(url="https://hooks.example/notify")
        assert cfg.health_probe == "options"

    def test_accepts_connect(self):
        cfg = WebhookEndpointConfig(url="https://x.test", health_probe="connect")
        assert cfg.health_probe == "connect"

    def test_accepts_full(self):
        cfg = WebhookEndpointConfig(url="https://x.test", health_probe="full")
        assert cfg.health_probe == "full"

    def test_rejects_invalid_value(self):
        with pytest.raises(Exception):
            WebhookEndpointConfig(url="https://x.test", health_probe="bogus")


class TestCheckWebhookOptionsMode:
    def test_options_sends_no_body_and_uses_options_method(self):
        mock_resp = MagicMock(status_code=200)
        with patch("httpx.request", return_value=mock_resp) as mock_req:
            result = check_webhook("https://x.test/hook", "POST", None, 10, "options")
        assert result.status == "ok"
        args, kwargs = mock_req.call_args
        assert args[0] == "OPTIONS"
        assert "json" not in kwargs

    def test_options_405_counts_as_reachable(self):
        mock_resp = MagicMock(status_code=405)
        with patch("httpx.request", return_value=mock_resp):
            result = check_webhook("https://x.test/hook", "POST", None, 10, "options")
        assert result.status == "ok"

    def test_options_5xx_is_warn(self):
        mock_resp = MagicMock(status_code=502)
        with patch("httpx.request", return_value=mock_resp):
            result = check_webhook("https://x.test/hook", "POST", None, 10, "options")
        assert result.status == "warn"

    def test_options_network_error_is_fail(self):
        import httpx
        with patch("httpx.request", side_effect=httpx.ConnectError("refused")):
            result = check_webhook("https://x.test/hook", "POST", None, 10, "options")
        assert result.status == "fail"


class TestCheckWebhookFullMode:
    def test_full_sends_real_post_with_probe_payload(self):
        mock_resp = MagicMock(status_code=200)
        with patch("httpx.request", return_value=mock_resp) as mock_req:
            result = check_webhook("https://x.test/hook", "POST", None, 10, "full")
        assert result.status == "ok"
        args, kwargs = mock_req.call_args
        assert args[0] == "POST"
        assert kwargs["json"]["event"] == "doctor_probe"


class TestCheckWebhookConnectMode:
    def test_connect_never_calls_httpx(self):
        with patch("httpx.request") as mock_req:
            with patch("socket.create_connection") as mock_sock:
                mock_sock.return_value.__enter__.return_value = MagicMock()
                result = check_webhook("http://x.test/hook", "POST", None, 10, "connect")
        mock_req.assert_not_called()
        assert result.status == "ok"

    def test_connect_https_wraps_tls(self):
        with patch("socket.create_connection") as mock_sock, \
             patch("ssl.create_default_context") as mock_ctx:
            mock_sock.return_value.__enter__.return_value = MagicMock()
            mock_ctx.return_value.wrap_socket.return_value.__enter__.return_value = MagicMock()
            result = check_webhook("https://x.test/hook", "POST", None, 10, "connect")
        assert result.status == "ok"
        mock_ctx.return_value.wrap_socket.assert_called_once()

    def test_connect_refused_is_fail(self):
        with patch("socket.create_connection", side_effect=ConnectionRefusedError("refused")):
            result = check_webhook("http://x.test/hook", "POST", None, 10, "connect")
        assert result.status == "fail"

    def test_connect_unparseable_url_is_fail(self):
        result = check_webhook("not-a-url", "POST", None, 10, "connect")
        assert result.status == "fail"
