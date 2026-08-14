"""Consolidated CLI output funnel — compose style + redaction + format.

Single entry point for all user-facing output.  Commands route through
``emit()`` for structured results and ``warn()`` for diagnostic warnings
so styling, redaction, and ``--format`` rendering stay consistent.

AGENTS.md rule: "CLI output speaks ONE vocabulary" — lives here now.
"""

from __future__ import annotations

import json
from typing import Any

import click

from aqueduct.cli.style import info as _style_info
from aqueduct.cli.style import warn as _style_warn


def format_bytes(n: int | None) -> str:
    """Human-readable byte size for text-format output (raw ints kept in
    json/csv — this is a text-rendering helper only, never applied before
    ``emit(fmt="json")``). Shared by ``report --profile`` and the run
    transcript's Handoff step lines so a byte count reads the same way in
    both places."""
    if n is None:
        return "-"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


def emit(
    data: Any,
    *,
    fmt: str = "text",
    redact: bool = True,
    err: bool = False,
    **render_opts: Any,
) -> None:
    """Structured-output entry point.

    Args:
        data: The result data to render.
        fmt: ``"json"`` → JSON-serialise (no styling, no colour).
             ``"text"`` → human-readable rendering.
        redact: When True, run values through ``aqueduct.redaction.redact``
                before printing.
        err: Print to stderr instead of stdout.
    """
    if redact:
        from aqueduct import redaction as _redaction

        data = _redaction.redact(data)

    if fmt == "json":
        click.echo(json.dumps(data, indent=2, default=str), err=err)
        return

    # text (default) or unrecognised format — human-readable fallback
    if isinstance(data, str):
        click.echo(data, err=err)
    else:
        click.echo(str(data), err=err)


def emit_info(message: str, *, err: bool = False) -> None:
    """Dim informational line — an OBSERVATION, not a warning.

    The third state the funnel was missing. ``warn()`` above carries a
    ``rule_id`` because a warning asserts that something is wrong and the
    user must be able to suppress it by name. Some output asserts nothing:
    the warn-only perf note (``aqueduct/patch/perf_attribution.py``)
    reports a measured ratio with no threshold behind it, so rendering it
    as ``⚠`` would claim a verdict the code deliberately refuses to make,
    and rendering it through ``emit()`` would put a diagnostic into the
    structured-result channel. Routes to ``style.info`` so the one output
    vocabulary still owns the styling.
    """
    _style_info(message, err=err)


def warn(
    rule_id: str,
    message: str,
    *,
    module: str | None = None,
    prefix: str = "",
    err: bool = True,
) -> None:
    """Render a diagnostic warning with a stable ``rule_id``.

    Output: ``⚠ [rule_id] message`` via ``style.warn``. With a ``prefix``
    (e.g. ``"   ↳ "`` for warnings nested under a module summary line) the
    icon is dropped — ``{prefix}[rule_id] message``, dim prefix + yellow
    body — so nested lines don't repeat the ⚠ the roll-up header carries.

    ``module`` is reserved for future per-module routing (Phase 3 extension).
    """
    if prefix:
        click.echo(
            click.style(prefix, fg="bright_black")
            + click.style(f"[{rule_id}] {message}", fg="yellow"),
            err=err,
        )
    else:
        _style_warn(f"[{rule_id}] {message}", err=err)
