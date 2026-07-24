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
``from_failure_context(ctx)`` — pipeline ``FailureContext`` → (exact, coarse)
signature pair for the Phase 45 heal cache (exact keys replay/pending-reuse
within a blueprint; coarse drops the module id for cross-blueprint coaching).
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
    "from_failure_context",
]

_MAX_MESSAGE_CHARS = 240
_DIGIT_RE = re.compile(r"\d+")
_QUOTED_DOUBLE_RE = re.compile(r'"[^"]*"')
_QUOTED_SINGLE_RE = re.compile(r"'[^']*'")
# Spark 4 backtick-quoted identifiers, e.g. `` `i`.`bad_col` `` in
# UNRESOLVED_COLUMN messages. Same collapse-to-placeholder treatment as the
# quote rules above, so two typo'd column names hash to one signature instead
# of spawning duplicate heal runs.
_BACKTICK_RE = re.compile(r"`[^`]*`")
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
    s = _BACKTICK_RE.sub("`X`", s)
    s = _PATH_RE.sub("/PATH", s)
    s = _DIGIT_RE.sub("N", s)
    s = _WS_RE.sub(" ", s).strip().lower()
    if len(s) > _MAX_MESSAGE_CHARS:
        s = s[:_MAX_MESSAGE_CHARS]
    return s


def _loc_to_where(loc: tuple[Any, ...]) -> str:
    """Render a Pydantic loc tuple as ``operations[0].op`` style path."""
    from aqueduct.utils import format_error_loc
    return format_error_loc(loc)


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
    # The execution engine this failure occurred on (e.g. "spark", "duckdb").
    # Folded into `hash` by make_signature() — a shared-core failure (a plain
    # FileNotFoundError, say) must NOT hash identically across engines, or a
    # patch cached from a Spark run could be replayed into a DuckDB run whose
    # PatchSpec grammar assumptions don't hold. Required, not optional: an
    # unlabeled signature is a silent cross-engine collision waiting to happen.
    engine: str = "unknown"

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
            "engine": self.engine,
        }


def make_signature(error_class: str, where: str, message: str, *, engine: str) -> ErrorSignature:
    """Build a signature from already-extracted parts.

    ``engine`` is required and folded into the hash — see ``ErrorSignature.engine``.
    """
    ec = (error_class or "unknown").strip() or "unknown"
    w = (where or "<root>").strip() or "<root>"
    msg = _normalize_message(message)
    eng = (engine or "unknown").strip() or "unknown"
    payload = json.dumps([ec, w, msg, eng], sort_keys=True, ensure_ascii=False)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return ErrorSignature(error_class=ec, where=w, normalized_message=msg, hash=digest, engine=eng)


def from_validation_error(exc: Any, *, engine: str) -> ErrorSignature:
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
        return make_signature("validation_error", "<root>", str(exc), engine=engine)
    first = errors[0]
    where = _loc_to_where(tuple(first.get("loc", ())))
    etype = str(first.get("type") or "validation_error")
    msg = str(first.get("msg") or "")
    return make_signature(etype, where, msg, engine=engine)


def from_json_decode_error(exc: Any, *, engine: str) -> ErrorSignature:
    """JSON parse failure → signature (column/line normalized out)."""
    return make_signature("json_decode_error", "<root>", str(getattr(exc, "msg", exc)), engine=engine)


def from_exception(exc: BaseException, where: str | None = None, *, engine: str) -> ErrorSignature:
    """Generic fallback for anything not handled by a more specific helper."""
    return make_signature(type(exc).__name__, where or "<root>", str(exc), engine=engine)


def from_apply_error(
    error_class: str,
    message: str,
    where: str | None = None,
    *,
    engine: str,
) -> ErrorSignature:
    """Apply-time gate rejection (guardrail, lineage, explain, sandbox)."""
    return make_signature(error_class, where or "<root>", message, engine=engine)


def from_failure_context(ctx: Any) -> tuple[ErrorSignature, ErrorSignature]:
    """Pipeline ``FailureContext`` → ``(exact, coarse)`` signature pair.

    *exact* — ``(error_class, failed_module, normalized_message)``; keys the
    blueprint-local heal cache (pending-patch reuse + exact replay, where
    patch ops reference module ids so the module must match).

    *coarse* — same with ``where`` pinned to ``<any>``; keys cross-blueprint
    coaching where module ids differ but the failure shape is the same.

    Error-class priority: Spark condition (``error_class``) → user-defined
    Assert label (``error_type``) → innermost throwable type
    (``root_exception["type"]``) → ``"unknown"``. Message prefers the
    innermost throwable's message over the full ``error_message`` blob.

    The engine is read off ``ctx.engine`` (stamped by ``Surveyor`` on every
    ``FailureContext`` it builds — see ``Surveyor.__init__``'s required
    ``engine`` param) so callers holding a ``FailureContext`` never need to
    thread the engine through separately.
    """
    _root = getattr(ctx, "root_exception", None)
    if not isinstance(_root, dict):
        _root = {}
    error_class = (
        getattr(ctx, "error_class", None)
        or getattr(ctx, "error_type", None)
        or _root.get("type")
        or "unknown"
    )
    message = _root.get("message") or getattr(ctx, "error_message", "") or ""
    where = getattr(ctx, "failed_module", None) or "<root>"
    engine = getattr(ctx, "engine", None) or "unknown"
    # str() coercion: duck-typed contexts (tests pass mocks) must never
    # break signature hashing — real FailureContext fields are already str.
    exact = make_signature(str(error_class), str(where), str(message), engine=str(engine))
    coarse = make_signature(str(error_class), "<any>", str(message), engine=str(engine))
    return exact, coarse


def from_text(
    text: str,
    error_class: str = "reprompt",
    where: str | None = None,
    *,
    engine: str,
) -> ErrorSignature:
    """Last-resort: signature from a plain text reprompt string.

    Use when you only have the friendly bullet-formatted output from
    ``_format_reprompt_error`` (no live exception object).
    """
    return make_signature(error_class, where or "<root>", text, engine=engine)
