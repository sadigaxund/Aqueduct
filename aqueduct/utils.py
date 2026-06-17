"""Shared utility functions used across layers.

Nothing here imports from any ``aqueduct.*`` subpackage, so every layer
can import from this module without creating a cycle.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def format_error_loc(loc: tuple[Any, ...]) -> str:
    """Format a Pydantic ``loc`` tuple as a human-readable dotted path.

    ``loc`` is a tuple whose elements are ``str`` (field names) or ``int``
    (list indices).  The returned string uses ``.field`` for field names
    and ``[N]`` for list indices, e.g. ``"operations[0].op"``.
    """
    parts: list[str] = []
    for part in loc:
        if isinstance(part, int):
            parts.append(f"[{part}]")
        elif parts:
            parts.append(f".{part}")
        else:
            parts.append(str(part))
    return "".join(parts) or "<root>"


def utcnow_iso() -> str:
    """ISO-8601 UTC timestamp, e.g. ``"2026-06-17T12:34:56.789012+00:00"``."""
    return datetime.now(tz=UTC).isoformat()
