"""Error-signature engine — stable hash for budget loops + coaching.

A ``signature`` is a 3-tuple ``(error_class, where, normalized_message)`` plus
a stable short hash. It is the dedup key reused by:

* Phase 34 multi-axis budget — ``same_error_consecutive`` /
  ``same_signature_overall`` / ``progress_stalled``.
* Phase 33 Part C coaching loop — clusters per-model failure modes by
  ``(model, signature)``.

This module is intentionally pure: no I/O, no agent state, no LLM calls. Wiring
into the heal loop happens in Tasks 86–88.

Public API
----------
``make_signature(error_class, where, message)`` — explicit constructor.
``from_validation_error(exc)``  — Pydantic ``ValidationError`` → signature.
``from_json_decode_error(exc)`` — JSON parse failure → signature.
``from_exception(exc, where=None)`` — generic fallback.
``from_apply_error(error_class, message, where=None)`` — apply-time gate
rejections (guardrail violation, lineage gate, explain regression, sandbox).
``from_text(text, error_class="reprompt", where=None)`` — last-resort plain
text input (e.g. friendly reprompt strings).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

__all__ = [
    "ErrorSignature",
    "make_signature",
    "from_validation_error",
    "from_json_decode_error",
    "from_exception",
    "from_apply_error",
    "from_text",
]

_MAX_MESSAGE_CHARS = 240
_DIGIT_RE = re.compile(r"\d+")
_QUOTED_DOUBLE_RE = re.compile(r'"[^"]*"')
_QUOTED_SINGLE_RE = re.compile(r"'[^']*'")
_PATH_RE = re.compile(r"(?:/[\w.\-]+)+")
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_WS_RE = re.compile(r"\s+")


def _normalize_message(text: str) -> str:
    """Strip volatile bits so the same failure shape hashes identically."""
    if not text:
        return ""
    s = _ANSI_RE.sub("", text)
    s = _QUOTED_DOUBLE_RE.sub('"X"', s)
    s = _QUOTED_SINGLE_RE.sub("'X'", s)
    s = _PATH_RE.sub("/PATH", s)
    s = _DIGIT_RE.sub("N", s)
    s = _WS_RE.sub(" ", s).strip().lower()
    if len(s) > _MAX_MESSAGE_CHARS:
        s = s[:_MAX_MESSAGE_CHARS]
    return s


def _loc_to_where(loc: tuple[Any, ...]) -> str:
    """Render a Pydantic loc tuple as ``operations[0].op`` style path."""
    parts: list[str] = []
    for p in loc:
        if isinstance(p, int):
            parts.append(f"[{p}]")
        elif parts:
            parts.append(f".{p}")
        else:
            parts.append(str(p))
    return "".join(parts) or "<root>"


@dataclass(frozen=True)
class ErrorSignature:
    """Stable dedup key for an LLM/apply/runtime failure.

    Equality + hashing are by ``hash`` only, so two signatures with the same
    canonical content are interchangeable as dict keys / set members.
    """

    error_class: str
    where: str
    normalized_message: str
    hash: str

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ErrorSignature) and self.hash == other.hash

    def __hash__(self) -> int:
        return hash(self.hash)

    def to_dict(self) -> dict[str, str]:
        return {
            "error_class": self.error_class,
            "where": self.where,
            "normalized_message": self.normalized_message,
            "hash": self.hash,
        }


def make_signature(error_class: str, where: str, message: str) -> ErrorSignature:
    """Build a signature from already-extracted parts."""
    ec = (error_class or "unknown").strip() or "unknown"
    w = (where or "<root>").strip() or "<root>"
    msg = _normalize_message(message)
    payload = json.dumps([ec, w, msg], sort_keys=True, ensure_ascii=False)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return ErrorSignature(error_class=ec, where=w, normalized_message=msg, hash=digest)


def from_validation_error(exc: Any) -> ErrorSignature:
    """Pydantic ``ValidationError`` → signature based on the first error.

    Using the first error keeps the signature stable across reprompts where
    the model fixes one issue but a later issue is still reported — the
    second turn moves on to a NEW signature, which is the behaviour the
    progress-stalled axis relies on.
    """
    try:
        errors = exc.errors(include_url=False)
    except TypeError:  # older pydantic shim
        errors = exc.errors()
    if not errors:
        return make_signature("validation_error", "<root>", str(exc))
    first = errors[0]
    where = _loc_to_where(tuple(first.get("loc", ())))
    etype = str(first.get("type") or "validation_error")
    msg = str(first.get("msg") or "")
    return make_signature(etype, where, msg)


def from_json_decode_error(exc: Any) -> ErrorSignature:
    """JSON parse failure → signature (column/line normalized out)."""
    return make_signature("json_decode_error", "<root>", str(getattr(exc, "msg", exc)))


def from_exception(exc: BaseException, where: str | None = None) -> ErrorSignature:
    """Generic fallback for anything not handled by a more specific helper."""
    return make_signature(type(exc).__name__, where or "<root>", str(exc))


def from_apply_error(
    error_class: str,
    message: str,
    where: str | None = None,
) -> ErrorSignature:
    """Apply-time gate rejection (guardrail, lineage, explain, sandbox)."""
    return make_signature(error_class, where or "<root>", message)


def from_text(
    text: str,
    error_class: str = "reprompt",
    where: str | None = None,
) -> ErrorSignature:
    """Last-resort: signature from a plain text reprompt string.

    Use when you only have the friendly bullet-formatted output from
    ``_format_reprompt_error`` (no live exception object).
    """
    return make_signature(error_class, where or "<root>", text)
