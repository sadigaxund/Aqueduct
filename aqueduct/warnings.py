"""Aqueduct warning infrastructure — standardized format + per-rule suppression.

Every diagnostic emitted by the engine carries a stable short `rule_id` so
users can copy the ID straight from the terminal into the suppress list
in `aqueduct.yml`:

    warnings:
      suppress:
        - kafka_checkpoint_stale
        - perf_8c

Output format (overrides `warnings.formatwarning` in the CLI):

    AQ-WARN [rule_id] message

This is intentionally distinct from Spark's `WARNING: ...` and Python's
default `path:line: Category: message` so engine diagnostics are easy to
filter visually and via `grep AQ-WARN`.

Public surface:
    AqueductWarning  — category subclass; all engine warnings use it
    emit(rule_id, message, *, suppress=None) — primary emission helper;
                                              respects the suppress set
    install_cli_formatter() — replaces warnings.formatwarning so
                              AqueductWarning instances render as
                              `AQ-WARN [id] msg`. Idempotent.
"""

from __future__ import annotations

import warnings as _w
from typing import Iterable


class AqueductWarning(UserWarning):
    """All engine-emitted warnings inherit from this category."""


_INSTALLED = False
_AQ_PREFIX = "[aqueduct:"

# Process-global fallback suppress set — populated once at CLI startup from
# `config.warnings.suppress` + `--suppress-warning` flags. Library callers may
# still pass an explicit `suppress=` to `emit()` which takes priority.
_DEFAULT_SUPPRESS: set[str] = set()
_DEFAULT_SILENCE_ALL: bool = False


def set_default_suppress(suppress: Iterable[str] | None, silence_all: bool = False) -> None:
    """CLI startup hook — install the process-wide suppress defaults.

    Idempotent. Library users (`import aqueduct`) don't need to call this —
    they pass `suppress=` explicitly when calling `compile()` / `execute()`,
    or accept all warnings.
    """
    global _DEFAULT_SUPPRESS, _DEFAULT_SILENCE_ALL
    _DEFAULT_SUPPRESS = set(suppress or ())
    _DEFAULT_SILENCE_ALL = bool(silence_all)


def emit(rule_id: str, message: str, *, suppress: Iterable[str] | None = None) -> None:
    """Emit one Aqueduct warning unless `rule_id` is in `suppress`.

    Resolution order for the active suppress set:
      1. Explicit `suppress=` argument (library callers, e.g. `compile()`)
      2. Process-global default installed via `set_default_suppress()`
    Whichever is non-empty wins; if both are provided, the explicit set wins.

    `silence_all` (process-global) short-circuits every emission to a no-op.

    Never raises.
    """
    try:
        if _DEFAULT_SILENCE_ALL:
            return
        active = set(suppress) if suppress is not None else _DEFAULT_SUPPRESS
        if rule_id in active:
            return
        _w.warn(f"{_AQ_PREFIX}{rule_id}] {message}", category=AqueductWarning, stacklevel=3)
    except Exception:
        pass  # warning emission must never affect the host process


def _aq_format(message, category, filename, lineno, line=None) -> str:
    """`warnings.formatwarning` replacement.

    Renders AqueductWarning as `AQ-WARN [rule_id] message\\n`. Falls back to
    the default Python format for non-Aqueduct warnings so we never hide
    third-party diagnostics or Python's own DeprecationWarning et al.
    """
    msg = str(message)
    if category is AqueductWarning or isinstance(message, AqueductWarning):
        if msg.startswith(_AQ_PREFIX):
            # "[aqueduct:rule_id] body" → "AQ-WARN [rule_id] body"
            body = msg[len(_AQ_PREFIX):]
            try:
                rid, rest = body.split("] ", 1)
                return f"AQ-WARN [{rid}] {rest}\n"
            except ValueError:
                return f"AQ-WARN {body}\n"
        return f"AQ-WARN {msg}\n"
    return _w.WarningMessage(message, category, filename, lineno, None, line).__str__() + "\n"


def install_cli_formatter() -> None:
    """Install the AQ-WARN formatter once per process. Idempotent."""
    global _INSTALLED
    if _INSTALLED:
        return
    _w.formatwarning = _aq_format  # type: ignore[assignment]
    _INSTALLED = True


def resolve_suppress(config_suppress: Iterable[str] | None,
                     cli_suppress: Iterable[str] | None = None) -> set[str]:
    """Merge config + CLI suppression lists into a single set."""
    out: set[str] = set()
    if config_suppress:
        out.update(s for s in config_suppress if s)
    if cli_suppress:
        out.update(s for s in cli_suppress if s)
    return out
