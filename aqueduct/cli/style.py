"""Central CLI presentation: icons, colors, message + warning rendering.
Single source of truth so error/status styling stays consistent across commands.
"""
from __future__ import annotations

import click

ICON = {"ok": "\u2713", "fail": "\u2717", "warn": "\u26a0", "skip": "-", "info": "\u00b7", "header": "\u25b6"}
COLOR = {"ok": "green", "fail": "red", "warn": "yellow", "skip": None, "info": "bright_black", "header": "cyan"}


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


def _short_warning(msg: str, limit: int = 100) -> str:
    """First clause of a warning message (before ' --- ' or first sentence), capped."""
    seg = msg.split(" \u2014 ", 1)[0].split(". ", 1)[0].strip()
    if len(seg) > limit:
        seg = seg[: limit - 1].rstrip() + "\u2026"
    return seg


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

    if aq:
        n = len(aq)
        prefix = f"{label} " if label else ""
        hint = "" if verbose else click.style("  \u00b7  -v for full text", dim=True)
        click.echo(
            click.style(f"{ICON['warn']} {prefix}{n} warning{'' if n == 1 else 's'}", fg="yellow", bold=True) + hint,
            err=err,
        )
        for rid, rest in aq:
            tag = click.style(f"[{rid}]", fg="yellow") if rid else ""
            body = rest if verbose else _short_warning(rest)
            click.echo(f"  \u00b7 {tag} {body}", err=err)
