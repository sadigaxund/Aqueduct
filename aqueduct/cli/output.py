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

from aqueduct.cli.style import warn as _style_warn


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


def warn(
    rule_id: str,
    message: str,
    *,
    module: str | None = None,
    prefix: str = "",
    err: bool = True,
) -> None:
    """Render a diagnostic warning with a stable ``rule_id``.

    Output: ``{prefix}⚠ [rule_id] message`` via ``style.warn``.

    ``module`` is reserved for future per-module routing (Phase 3 extension).
    ``prefix`` is prepended before the icon (e.g. ``"  ↳ "`` for indented
    per-module warnings).
    """
    _style_warn(f"{prefix}[{rule_id}] {message}", err=err)
