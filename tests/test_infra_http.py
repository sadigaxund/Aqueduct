"""Tests for aqueduct/infra/http.py — the shared outbound-HTTP transport.

Before this file, http.py's primitives (retry_after_seconds, backoff_delay,
sign_body, deliver_with_retry, the RETRYABLE_*_STATUS constants) were covered
only indirectly through webhook/agent-provider tests (2026-08-01 audit-yolo
finding, infra unit). Direct coverage here pins the contracts those callers
rely on so a change to any of them fails at the source, not three call sites
downstream.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from aqueduct.infra.http import (
    RETRYABLE_DELIVERY_STATUS,
    RETRYABLE_PROVIDER_STATUS,
    backoff_delay,
    deliver_with_retry,
    retry_after_seconds,
    sign_body,
)

pytestmark = [pytest.mark.unit]


# ── retry_after_seconds ──────────────────────────────────────────────────────

def _resp_with_header(value: str | None) -> MagicMock:
    resp = MagicMock()
    resp.headers = {"retry-after": value} if value is not None else {}
    return resp


def test_retry_after_seconds_parses_delta_seconds():
    assert retry_after_seconds(_resp_with_header("5")) == 5.0


def test_retry_after_seconds_missing_header_returns_none():
    assert retry_after_seconds(_resp_with_header(None)) is None


def test_retry_after_seconds_non_numeric_returns_none():
    assert retry_after_seconds(_resp_with_header("not-a-number")) is None


def test_retry_after_seconds_negative_clamped_to_zero():
    assert retry_after_seconds(_resp_with_header("-10")) == 0.0


# ── backoff_delay ─────────────────────────────────────────────────────────────

def test_backoff_delay_no_jitter_is_deterministic_exponential():
    assert backoff_delay(1, 2.0) == 2.0
    assert backoff_delay(2, 2.0) == 4.0
    assert backoff_delay(3, 2.0) == 8.0


def test_backoff_delay_jitter_zero_is_same_as_no_jitter():
    assert backoff_delay(2, 2.0, jitter=0.0) == backoff_delay(2, 2.0)


def test_backoff_delay_jitter_stays_within_documented_bounds():
    base_delay = backoff_delay(2, 2.0)  # 4.0, jitter-free reference
    for _ in range(50):
        delayed = backoff_delay(2, 2.0, jitter=0.5)
        assert base_delay <= delayed <= base_delay * 1.5


# ── sign_body ─────────────────────────────────────────────────────────────────

def test_sign_body_produces_matching_hmac_sha256():
    body = {"b": 2, "a": 1}
    content, signature = sign_body(body, "shh-secret")
    expected_digest = hmac.new(b"shh-secret", content, hashlib.sha256).hexdigest()
    assert signature == f"sha256={expected_digest}"


def test_sign_body_serialises_compact_and_key_sorted():
    body = {"b": 2, "a": 1}
    content, _ = sign_body(body, "shh-secret")
    assert content == b'{"a":1,"b":2}'
    assert json.loads(content) == body


def test_sign_body_different_secrets_produce_different_signatures():
    body = {"x": 1}
    _, sig1 = sign_body(body, "secret-one")
    _, sig2 = sign_body(body, "secret-two")
    assert sig1 != sig2


# ── RETRYABLE_DELIVERY_STATUS vs RETRYABLE_PROVIDER_STATUS ──────────────────

def test_retryable_status_sets_are_distinct_frozensets():
    assert isinstance(RETRYABLE_DELIVERY_STATUS, frozenset)
    assert isinstance(RETRYABLE_PROVIDER_STATUS, frozenset)
    assert RETRYABLE_DELIVERY_STATUS != RETRYABLE_PROVIDER_STATUS


def test_gateway_5xx_retryable_for_delivery_not_for_provider():
    """500/502/504 are transient proxy hiccups for best-effort delivery, but a
    real error from an LLM endpoint (never behind the same kind of gateway
    hop) — deliberately not retried there."""
    for code in (500, 502, 504):
        assert code in RETRYABLE_DELIVERY_STATUS
        assert code not in RETRYABLE_PROVIDER_STATUS


def test_provider_overloaded_529_retryable_only_for_provider():
    assert 529 in RETRYABLE_PROVIDER_STATUS
    assert 529 not in RETRYABLE_DELIVERY_STATUS


def test_both_sets_share_429_and_503():
    for code in (429, 503):
        assert code in RETRYABLE_DELIVERY_STATUS
        assert code in RETRYABLE_PROVIDER_STATUS


# ── deliver_with_retry ───────────────────────────────────────────────────────

def _mock_response(status_code: int) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    return resp


def test_deliver_with_retry_succeeds_on_first_2xx_no_retry():
    with patch("httpx.request", return_value=_mock_response(200)) as mock_request:
        deliver_with_retry(
            "POST", "https://example.com/hook",
            json={"a": 1}, headers={}, timeout=5, label="test",
            attempts=3, backoff_seconds=0,
        )
    assert mock_request.call_count == 1


def test_deliver_with_retry_retries_on_retryable_status_then_succeeds():
    with patch(
        "httpx.request", side_effect=[_mock_response(503), _mock_response(200)]
    ) as mock_request, patch("time.sleep") as mock_sleep:
        deliver_with_retry(
            "POST", "https://example.com/hook",
            json={"a": 1}, headers={}, timeout=5, label="test",
            attempts=3, backoff_seconds=0,
        )
    assert mock_request.call_count == 2
    mock_sleep.assert_called_once()


def test_deliver_with_retry_gives_up_after_exhausting_attempts():
    with patch(
        "httpx.request", return_value=_mock_response(503)
    ) as mock_request, patch("time.sleep"):
        deliver_with_retry(
            "POST", "https://example.com/hook",
            json={"a": 1}, headers={}, timeout=5, label="test",
            attempts=2, backoff_seconds=0,
        )
    assert mock_request.call_count == 2  # never a 3rd try


def test_deliver_with_retry_does_not_retry_non_retryable_status():
    with patch("httpx.request", return_value=_mock_response(404)) as mock_request:
        deliver_with_retry(
            "POST", "https://example.com/hook",
            json={"a": 1}, headers={}, timeout=5, label="test",
            attempts=3, backoff_seconds=0,
        )
    assert mock_request.call_count == 1


def test_deliver_with_retry_retries_on_request_error():
    with patch(
        "httpx.request",
        side_effect=[httpx.ConnectError("boom"), _mock_response(200)],
    ) as mock_request, patch("time.sleep"):
        deliver_with_retry(
            "POST", "https://example.com/hook",
            json={"a": 1}, headers={}, timeout=5, label="test",
            attempts=3, backoff_seconds=0,
        )
    assert mock_request.call_count == 2


def test_deliver_with_retry_never_raises_on_unexpected_exception():
    """The docstring's 'Never raises' contract — an unrelated exception from
    the transport must not escape (it runs inside a daemon thread with no
    caller to report failure to)."""
    with patch("httpx.request", side_effect=RuntimeError("unexpected")):
        deliver_with_retry(
            "POST", "https://example.com/hook",
            json={"a": 1}, headers={}, timeout=5, label="test",
            attempts=3, backoff_seconds=0,
        )  # must return normally, not raise
