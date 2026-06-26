"""Shared outbound-HTTP transport primitives.

One place for retry policy, backoff, and fire-and-forget delivery. The webhook
and OpenLineage daemon-delivery paths build on `deliver_with_retry` +
`fire_and_forget`; the LLM-provider path reuses `retry_after_seconds` /
`backoff_delay` / the retryable-status constants for its budget-aware loop.

Best-effort delivery prints to **stderr** (not `logging`) on purpose — these run
in daemon threads where the run result is authoritative and operators tail
stderr; the format is stable (`[aqueduct] <label> …`).
"""

from __future__ import annotations

import hashlib
import hmac
import json as _json
import random
import sys
import threading
import time
from typing import Any, Callable

import httpx

# Transient statuses worth retrying. Two sets — they legitimately differ:
#   DELIVERY: best-effort outbound POSTs (webhook / OpenLineage / CI callback).
#             Gateway 5xx (500/502/504) are transient proxy hiccups worth a retry.
#   PROVIDER: LLM endpoints. 529 = Anthropic "overloaded"; a 500/502/504 from a
#             model endpoint is usually a real error, not a transient hop, so it
#             is deliberately NOT retried.
RETRYABLE_DELIVERY_STATUS = frozenset({429, 500, 502, 503, 504})
RETRYABLE_PROVIDER_STATUS = frozenset({429, 503, 529})

DEFAULT_DELIVERY_ATTEMPTS = 2          # total tries per delivery (1 retry)
DEFAULT_DELIVERY_BACKOFF_SECONDS = 2.0


def retry_after_seconds(response: Any) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds form) to float, or None."""
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def backoff_delay(attempt: int, base: float, *, jitter: float = 0.0) -> float:
    """Exponential backoff for a 1-based ``attempt``: ``base * 2**(attempt-1)``.

    ``jitter`` adds up to ``jitter`` fraction of random extra delay (0 disables,
    keeping the delay deterministic for the single-retry default).
    """
    delay = base * (2 ** (attempt - 1))
    if jitter:
        delay *= 1.0 + random.random() * jitter
    return delay


def sign_body(body: Any, secret: str) -> tuple[bytes, str]:
    """Serialise ``body`` to canonical JSON bytes and HMAC-SHA256-sign them.

    Returns ``(content_bytes, "sha256=<hex digest>")``. The receiver recomputes
    HMAC-SHA256 over the *exact* request body with the shared secret and compares
    to the ``X-Aqueduct-Signature`` header to verify authenticity + integrity.
    The body is serialised here (compact, key-sorted) so the signed bytes are
    exactly the bytes sent — pass ``content=content_bytes`` to the transport, not
    ``json=body`` (which would re-serialise differently).
    """
    content = _json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), content, hashlib.sha256).hexdigest()
    return content, f"sha256={digest}"


def fire_and_forget(target: Callable[[], None], *, name: str) -> threading.Thread:
    """Run ``target()`` in a daemon thread so the caller is never blocked."""
    thread = threading.Thread(target=target, daemon=True, name=name)
    thread.start()
    return thread


def deliver_with_retry(
    method: str,
    url: str,
    *,
    json: Any = None,
    content: bytes | None = None,
    headers: dict[str, str],
    timeout: float,
    label: str,
    retryable_status: frozenset[int] = RETRYABLE_DELIVERY_STATUS,
    attempts: int = DEFAULT_DELIVERY_ATTEMPTS,
    backoff_seconds: float = DEFAULT_DELIVERY_BACKOFF_SECONDS,
    jitter: float = 0.0,
) -> None:
    """Best-effort delivery: POST/PUT/PATCH with bounded retry on transient
    statuses (``retryable_status``) or network errors. **Never raises** — logs
    to stderr and returns. Intended to run inside :func:`fire_and_forget`.

    A non-retryable ``>=400`` status, an exhausted retry budget, or an
    unexpected exception all stop the loop. ``2xx``/``3xx`` returns immediately.
    """
    for attempt in range(1, attempts + 1):
        retryable = False
        try:
            if content is not None:
                resp = httpx.request(method, url, content=content, headers=headers, timeout=timeout)
            else:
                resp = httpx.request(method, url, json=json, headers=headers, timeout=timeout)
            if resp.status_code < 400:
                return
            retryable = resp.status_code in retryable_status
            _log(label, method, url, f"returned HTTP {resp.status_code}", retryable, attempt, attempts)
        except httpx.RequestError as exc:
            retryable = True
            _log(label, method, url, f"failed: {exc}", retryable, attempt, attempts)
        except Exception as exc:  # noqa: BLE001 — a delivery bug must never crash the run
            print(f"[aqueduct] {label} {method} {url!r} raised unexpected error: {exc}", file=sys.stderr)
            return
        if not retryable or attempt >= attempts:
            return
        time.sleep(backoff_delay(attempt, backoff_seconds, jitter=jitter))


def _log(label: str, method: str, url: str, what: str, retryable: bool, attempt: int, attempts: int) -> None:
    suffix = f" — retrying ({attempt}/{attempts})" if retryable and attempt < attempts else ""
    print(f"[aqueduct] {label} {method} {url!r} {what}{suffix}", file=sys.stderr)
