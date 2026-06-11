"""Webhook dispatcher — fires HTTP requests with configurable method, headers, and payload.

Delivery is best-effort: all network calls run in a daemon thread so the CLI
is never blocked.  Failures are logged to stderr and silently swallowed —
the blueprint result is authoritative.

Payload templating
------------------
When WebhookEndpointConfig.payload is set, each string value in the dict is
rendered by substituting ${VAR} tokens.  Resolution order:

  1. Built-in failure vars passed by the caller (run_id, blueprint_id, etc.)
  2. os.environ (useful for secrets in header values)
  3. Leave token as-is if not found

When payload is None, the full ``full_payload`` dict is sent unchanged
(backward-compatible with the original behaviour).
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
from typing import Any

import httpx

from aqueduct.redaction import redact as _redact


# ── Template rendering ────────────────────────────────────────────────────────

_TOKEN_RE = re.compile(r"\$\{([^}]+)\}")


def _render_value(value: Any, vars: dict[str, str]) -> Any:
    """Substitute ${VAR} tokens in a string value.  Non-strings pass through."""
    if not isinstance(value, str):
        return value
    def _replace(m: re.Match) -> str:
        key = m.group(1)
        if key in vars:
            return str(vars[key])
        return os.environ.get(key, m.group(0))
    return _TOKEN_RE.sub(_replace, value)


def _render_dict(template: dict[str, Any], vars: dict[str, str]) -> dict[str, Any]:
    return {k: _render_value(v, vars) for k, v in template.items()}


# ── Public API ────────────────────────────────────────────────────────────────

# Delivery statuses worth one retry — transient by nature. 4xx other than
# 429 means the request itself is wrong; retrying it identically is noise.
_RETRYABLE_DELIVERY_STATUS = frozenset({429, 500, 502, 503, 504})
_DELIVERY_ATTEMPTS = 2          # total tries per webhook fire
_DELIVERY_BACKOFF_SECONDS = 2.0


def fire_webhook(
    config: "WebhookEndpointConfig",  # type: ignore[name-defined]  # noqa: F821
    full_payload: dict[str, Any],
    template_vars: dict[str, str] | None = None,
    event: str | None = None,
) -> threading.Thread:
    """POST/PUT/PATCH a payload to a configured endpoint in a background thread.

    Args:
        config:        WebhookEndpointConfig with url, method, headers, payload, timeout.
        full_payload:  Event data sent when config.payload is None — wrapped in the
                       standardized envelope when ``event`` is given (see below).
        template_vars: Built-in variables available for ${VAR} substitution in
                       config.payload values and config.headers values.
                       Keys: run_id, blueprint_id, blueprint_name, failed_module,
                       error_message, error_type, started_at, attempt — plus, on
                       patch events (Phase 46): patch_id, root_cause, rationale,
                       confidence, category.
        event:         Event name (``on_failure`` / ``on_success`` /
                       ``on_patch_pending`` / ``on_ci_patch``). Phase 46: when set
                       and ``config.payload`` is None, the default body is the
                       standardized envelope ``{event, timestamp, run_id,
                       blueprint_id, data}`` with the event-specific payload under
                       ``data``. ``None`` preserves the legacy raw-payload body.

    Returns:
        The daemon thread that was started (callers may join() if needed).

    Delivery is best-effort with ONE in-thread retry on transient statuses
    (429/5xx) or network errors — never blocks the run, never raises.
    """
    vars: dict[str, str] = template_vars or {}

    # Render headers (e.g. Authorization: "Bearer ${SLACK_TOKEN}")
    rendered_headers: dict[str, str] = {
        "Content-Type": "application/json",
        **_render_dict(config.headers, vars),
    }

    # Build final payload. The body is scrubbed of registered @aq.secret() values
    # so a resolved secret cannot leak via webhook delivery. Headers and URL are
    # NOT scrubbed — those are user-authored config (e.g. `Authorization: Bearer
    # @aq.secret('SLACK_TOKEN')`) and represent the intended transmission of a
    # credential to the destination.
    if config.payload is not None:
        body = _redact(_render_dict(config.payload, vars))
    elif event is not None:
        from datetime import datetime, timezone
        body = _redact({
            "event": event,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "run_id": vars.get("run_id"),
            "blueprint_id": vars.get("blueprint_id"),
            "data": full_payload,
        })
    else:
        body = _redact(full_payload)

    url = config.url
    method = config.method
    timeout = config.timeout

    def _send() -> None:
        import time as _time
        for attempt in range(1, _DELIVERY_ATTEMPTS + 1):
            retryable = False
            try:
                resp = httpx.request(
                    method,
                    url,
                    json=body,
                    headers=rendered_headers,
                    timeout=timeout,
                )
                if resp.status_code < 400:
                    return
                retryable = resp.status_code in _RETRYABLE_DELIVERY_STATUS
                print(
                    f"[surveyor] webhook {method} {url!r} returned HTTP {resp.status_code}"
                    + (f" — retrying ({attempt}/{_DELIVERY_ATTEMPTS})" if retryable and attempt < _DELIVERY_ATTEMPTS else ""),
                    file=sys.stderr,
                )
            except httpx.RequestError as exc:
                retryable = True
                print(
                    f"[surveyor] webhook {method} {url!r} failed: {exc}"
                    + (f" — retrying ({attempt}/{_DELIVERY_ATTEMPTS})" if attempt < _DELIVERY_ATTEMPTS else ""),
                    file=sys.stderr,
                )
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[surveyor] webhook {method} {url!r} raised unexpected error: {exc}",
                    file=sys.stderr,
                )
                return
            if not retryable or attempt >= _DELIVERY_ATTEMPTS:
                return
            _time.sleep(_DELIVERY_BACKOFF_SECONDS)

    thread = threading.Thread(target=_send, daemon=True, name="surveyor-webhook")
    thread.start()
    return thread
