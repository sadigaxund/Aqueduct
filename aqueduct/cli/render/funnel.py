"""Consolidated CLI output funnel — compose style + redaction + format.

Single entry point for all user-facing output.  Commands route through
``emit()`` for structured results and ``warn()`` for diagnostic warnings
so styling, redaction, and ``--format`` rendering stay consistent.

AGENTS.md rule: "CLI output speaks ONE vocabulary" — lives here now.
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

import click

from aqueduct.cli.render.style import ICON as _ICON
from aqueduct.cli.render.style import colorize_line as _colorize_line
from aqueduct.cli.render.style import info as _style_info
from aqueduct.cli.render.style import warn as _style_warn
from aqueduct.cli.render.wrap import wrap_line as _wrap_line


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
        click.echo(json.dumps(data, indent=2, default=str, ensure_ascii=False), err=err)
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


# ---------------------------------------------------------------------------
# Gutter context — lets a block of nested echo() calls (e.g. a heal
# transcript block) inherit a tree gutter (``│ ``) without every call site
# repeating it. Explicit ``gutter=`` on echo() overrides the stack.
# ---------------------------------------------------------------------------

_gutter_stack: list[tuple[str, int]] = []


@contextlib.contextmanager
def gutter(prefix: str, hang: int = 0):
    """Push ``(prefix, hang)`` onto the module-level gutter stack for the
    duration of the ``with`` block. Reentrant (nested ``with gutter(...)``
    blocks stack correctly) and exception-safe (the push is always popped)."""
    _gutter_stack.append((prefix, hang))
    try:
        yield
    finally:
        _gutter_stack.pop()


def _current_gutter() -> tuple[str, int]:
    return _gutter_stack[-1] if _gutter_stack else ("", 0)


def echo(
    text: str,
    *,
    err: bool = True,
    gutter: str = "",
    hang: int = 0,
    verbose: bool = False,
    max_lines: int | None = None,
    hint: str | None = None,
    style: dict[str, Any] | None = None,
    fmt: str = "text",
) -> None:
    """General narrative line emitter — the funnel's TTY-aware primitive.

    Runs ``text`` through ``wrap_line`` (truncation/wrap policy), applies
    ``style`` (a ``click.style`` kwarg dict, whole-line) when given, then
    ``colorize_line`` (so the ✓/✗/⚠/ⓘ/tree icon vocabulary keeps working
    without every call site hand-styling), and ``click.echo``s each produced
    line.

    Stream rule: narrative defaults to **stderr** (``err=True``) — final
    results belong on stdout via ``result()``/``emit()``. When ``gutter=``
    is not given, the active ``gutter()`` context (if any) is inherited.
    ``fmt="json"`` (or anything other than ``"text"``) skips ``colorize_line``
    so no colour/icons leak into machine-readable output paths that happen
    to route narrative through here.
    """
    eff_gutter, eff_hang = (gutter, hang) if gutter else _current_gutter()
    lines = _wrap_line(
        text,
        gutter=eff_gutter,
        hang=eff_hang,
        err=err,
        verbose=verbose,
        max_lines=max_lines,
        hint=hint,
    )
    for line in lines:
        if style:
            line = click.style(line, **style)
        if fmt == "text":
            line = _colorize_line(line)
        click.echo(line, err=err)


def info(msg: str, **kw: Any) -> None:
    """Dim ``· message`` narrative line — thin wrapper over ``echo``."""
    kw.setdefault("style", {"fg": "bright_black"})
    echo(f"{_ICON['info']} {msg}", **kw)


def success(msg: str, **kw: Any) -> None:
    """Green ``✓ message`` narrative line — thin wrapper over ``echo``."""
    echo(f"{_ICON['ok']} {msg}", **kw)


def warn_line(msg: str, **kw: Any) -> None:
    """Yellow ``⚠ message`` narrative line — thin wrapper over ``echo``.

    Named ``warn_line`` (not ``warn``) because ``warn(rule_id, message, ...)``
    above already owns that name with a different, rule_id-carrying
    signature that must not change.
    """
    echo(f"{_ICON['warn']} {msg}", **kw)


def error(msg: str, **kw: Any) -> None:
    """Red ``✗ message`` narrative line — thin wrapper over ``echo``."""
    echo(f"{_ICON['fail']} {msg}", **kw)


def result(data: Any, *, fmt: str = "text", redact: bool = True, **kw: Any) -> None:
    """Final-result entry point — thin alias for ``emit(..., err=False)``.

    Documents the stdout/result channel: narrative goes through
    ``echo``/``info``/``success``/``warn_line``/``error`` (stderr by
    default); final results (module tree, summaries, ``--format json``)
    go through ``result``/``emit`` (stdout by default).
    """
    kw.pop("err", None)
    emit(data, fmt=fmt, redact=redact, err=False, **kw)
