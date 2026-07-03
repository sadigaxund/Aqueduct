"""Central CLI presentation: icons, colors, message + warning rendering.
Single source of truth so error/status styling stays consistent across commands.
"""
from __future__ import annotations

import logging
import os
import sys

import click

ICON = {"ok": "\u2713", "fail": "\u2717", "warn": "\u26a0", "skip": "-", "info": "\u00b7", "header": "\u25b6"}
COLOR = {"ok": "green", "fail": "red", "warn": "yellow", "skip": None, "info": "bright_black", "header": "cyan"}


def _color_enabled() -> bool:
    """Colour only when writing to a real terminal (and NO_COLOR unset)."""
    return not os.environ.get("NO_COLOR") and sys.stderr.isatty()


class StyledLogFormatter(logging.Formatter):
    """Render log records to match the CLI's icon/colour vocabulary instead of the
    default ``LEVEL: message`` \u2014 WARNING \u2192 ``\u26a0`` (yellow), ERROR/CRITICAL \u2192
    ``\u2717`` (red), else dim ``\u00b7``. ``verbose`` prepends the logger name.
    Colour is gated on a TTY so piped/redirected logs stay plain.
    """

    def __init__(self, *, verbose: bool = False) -> None:
        super().__init__()
        self._verbose = verbose

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        if record.exc_info:
            msg = f"{msg}\n{self.formatException(record.exc_info)}"
        if record.levelno >= logging.ERROR:
            icon, color = ICON["fail"], "red"
        elif record.levelno >= logging.WARNING:
            icon, color = ICON["warn"], "yellow"
        else:
            icon, color = ICON["info"], "bright_black"
        name = f"{record.name} " if self._verbose else ""
        line = f"{icon} {name}{msg}"
        return click.style(line, fg=color) if _color_enabled() else line


def error(msg: str, *, err: bool = True) -> None:
    """Red \u2717 error line."""
    click.echo(click.style(f"{ICON['fail']} {msg}", fg="red"), err=err)


def success(msg: str) -> None:
    click.echo(click.style(f"{ICON['ok']} {msg}", fg="green"))


def warn(msg: str, *, err: bool = True) -> None:
    click.echo(click.style(f"{ICON['warn']} {msg}", fg="yellow"), err=err)


def info(msg: str, *, err: bool = False) -> None:
    """Dim, low-signal preamble line (recedes visually)."""
    click.echo(click.style(msg, fg="bright_black"), err=err)


def dim(text: str) -> str:
    """Dim-style a box-drawing separator (e.g. the ``_rule()`` header divider) or
    other structural text that isn't a ✓/✗/⚠ status line. Returns the styled
    string rather than echoing it — callers still control ``err=``."""
    return click.style(text, dim=True)


# Semantic icon → colour. ✓ success, ✗ failure, ⚠ warning, ⓘ/◆/↻/▸/⏭ accents.
_ICON_COLOR = {
    "✓": "green", "✗": "red", "⚠": "yellow",
    "ⓘ": "cyan", "◆": "cyan", "↻": "cyan", "▸": "cyan", "⏭": "cyan",
}
# Structure / sub-detail glyphs recede (dim).
_DIM_GLYPHS = ("├─", "└─", "│", "┆", "↳", "↑")
# A line is a "status/tree" line (eligible for colouring) only if, after leading
# whitespace, it starts with one of these — so `--format json` (`{`/`[`) and
# prose are never touched, and an icon embedded mid-prose is left alone.
_LEADERS = tuple(_ICON_COLOR) + _DIM_GLYPHS


def colorize_line(line: str) -> str:
    """Colour the CLI icon vocabulary on a single output line.

    The systemic styler: installed as a ``click.echo`` wrapper so *every* status
    line is coloured without each call site styling by hand (✓ green / ✗ red /
    ⚠ yellow / ⓘ◆↻▸ cyan; ``│ ├─ └─ ┆ ↳ ↑`` dimmed). Returns the line unchanged
    when colour is off, when it is already styled (contains an ANSI escape), or
    when it is not a status/tree line (doesn't start with a known glyph) — so
    JSON output and prose stay intact."""
    if not isinstance(line, str) or not _color_enabled():
        return line
    if "\x1b[" in line or not line.lstrip().startswith(_LEADERS):
        return line
    out = line
    for glyph, colour in _ICON_COLOR.items():
        if glyph in out:
            out = out.replace(glyph, click.style(glyph, fg=colour))
    for glyph in _DIM_GLYPHS:
        if glyph in out:
            out = out.replace(glyph, click.style(glyph, fg="bright_black"))
    return out


def _short_warning(msg: str, limit: int = 100) -> str:
    """First clause of a warning message (before ' --- ' or first sentence), capped."""
    seg = msg.split(" \u2014 ", 1)[0].split(". ", 1)[0].strip()
    if len(seg) > limit:
        seg = seg[: limit - 1].rstrip() + "\u2026"
    return seg


def emit_warning_pairs(
    pairs: list, *, label: str = "", verbose: bool = False, err: bool = True,
) -> None:
    """Render ``(rule_id, message)`` pairs as one collapsed ``⚠ N warnings`` block.

    The shared renderer behind the compile/session blocks (``emit_warnings``) and
    the end-of-run runtime roll-up — so all three look identical (yellow bold
    header + ``·  -v for full text`` hint; ``· [rule_id] message`` lines, short
    unless ``verbose``). Empty ``pairs`` prints nothing."""
    if not pairs:
        return
    n = len(pairs)
    prefix = f"{label} " if label else ""
    hint = "" if verbose else click.style("  ·  -v for full text", dim=True)
    click.echo(
        click.style(f"{ICON['warn']} {prefix}{n} warning{'' if n == 1 else 's'}", fg="yellow", bold=True) + hint,
        err=err,
    )
    for rid, rest in pairs:
        tag = click.style(f"[{rid}]", fg="yellow") if rid else ""
        body = rest if verbose else _short_warning(rest)
        click.echo(f"  · {tag} {body}", err=err)


def emit_warnings(caught: list, *, verbose: bool = False, err: bool = True, label: str = "") -> None:
    """Render a list of ``warnings.catch_warnings(record=True)`` records as one
    collapsed ``\u26a0 N warnings`` block.  AqueductWarning bodies of the form
    ``[aqueduct:rule_id] msg`` keep the copy-pasteable rule_id; other UserWarnings
    fall back to a ``WARNING:`` line.  Pulled out of
    ``cli.__init__._compile_with_warnings`` so ``doctor`` (and anything else)
    renders warnings identically.

    ``label``, when set, is prepended to the header (e.g. ``compile: ``) so
    adjacent warning blocks from different lifecycle phases are distinguishable.
    """
    import warnings as _warnings

    from aqueduct.warnings import AqueductWarning

    _AQ_PREFIX = "[aqueduct:"
    aq: list[tuple[str, str]] = []
    for w in caught:
        msg = str(w.message)
        if issubclass(w.category, AqueductWarning) and msg.startswith(_AQ_PREFIX):
            body = msg[len(_AQ_PREFIX):]
            try:
                rid, rest = body.split("] ", 1)
            except ValueError:
                rid, rest = "", body
            aq.append((rid, rest))
        elif issubclass(w.category, UserWarning):
            click.echo(f"WARNING: {w.message}", err=err)
        else:
            _warnings.warn_explicit(w.message, w.category, w.filename, w.lineno)

    emit_warning_pairs(aq, label=label, verbose=verbose, err=err)
