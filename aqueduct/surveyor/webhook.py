"""Webhook dispatcher — fires HTTP POST with FailureContext payload.

Uses only stdlib (urllib.request) to avoid adding a requests dependency.
All network calls run in a daemon thread so the CLI is never blocked.
Failures are logged to stderr and silently swallowed — webhook delivery is
best-effort; the pipeline result is authoritative.
"""

from __future__ import annotations

import json
import sys
import threading
import urllib.error
import urllib.request
from typing import Any


def fire_webhook(
    url: str,
    payload: dict[str, Any],
    timeout: int = 10,
) -> threading.Thread:
    """POST JSON payload to url in a background daemon thread.

    Args:
        url:     HTTP(S) endpoint to POST to.
        payload: JSON-serialisable dict (typically FailureContext.to_dict()).
        timeout: Socket timeout in seconds.

    Returns:
        The daemon thread that was started (callers may join() if needed).
    """

    def _post() -> None:
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = resp.status
            if status >= 400:
                print(
                    f"[surveyor] webhook POST to {url!r} returned HTTP {status}",
                    file=sys.stderr,
                )
        except urllib.error.URLError as exc:
            print(
                f"[surveyor] webhook POST to {url!r} failed: {exc.reason}",
                file=sys.stderr,
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[surveyor] webhook POST to {url!r} raised unexpected error: {exc}",
                file=sys.stderr,
            )

    thread = threading.Thread(target=_post, daemon=True, name="surveyor-webhook")
    thread.start()
    return thread
