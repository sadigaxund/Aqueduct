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

def fire_webhook(
    config: "WebhookEndpointConfig",  # type: ignore[name-defined]  # noqa: F821
    full_payload: dict[str, Any],
    template_vars: dict[str, str] | None = None,
) -> threading.Thread:
    """POST/PUT/PATCH a payload to a configured endpoint in a background thread.

    Args:
        config:        WebhookEndpointConfig with url, method, headers, payload, timeout.
        full_payload:  Fallback payload sent when config.payload is None (full FailureContext).
        template_vars: Built-in variables available for ${VAR} substitution in
                       config.payload values and config.headers values.
                       Keys: run_id, blueprint_id, blueprint_name, failed_module,
                       error_message, error_type, started_at, attempt.

    Returns:
        The daemon thread that was started (callers may join() if needed).
    """
    vars: dict[str, str] = template_vars or {}

    # Render headers (e.g. Authorization: "Bearer ${SLACK_TOKEN}")
    rendered_headers: dict[str, str] = {
        "Content-Type": "application/json",
        **_render_dict(config.headers, vars),
    }

    # Build final payload
    if config.payload is not None:
        body = _render_dict(config.payload, vars)
    else:
        body = full_payload

    url = config.url
    method = config.method
    timeout = config.timeout

    def _send() -> None:
        try:
            resp = httpx.request(
                method,
                url,
                json=body,
                headers=rendered_headers,
                timeout=timeout,
            )
            if resp.status_code >= 400:
                print(
                    f"[surveyor] webhook {method} {url!r} returned HTTP {resp.status_code}",
                    file=sys.stderr,
                )
        except httpx.RequestError as exc:
            print(
                f"[surveyor] webhook {method} {url!r} failed: {exc}",
                file=sys.stderr,
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[surveyor] webhook {method} {url!r} raised unexpected error: {exc}",
                file=sys.stderr,
            )

    thread = threading.Thread(target=_send, daemon=True, name="surveyor-webhook")
    thread.start()
    return thread
