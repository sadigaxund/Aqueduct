"""Terminal / width / TTY primitives shared by the render funnel.

Nothing here echoes anything — these are pure queries the wrap/funnel layers
consult. Both ``is_tty`` and ``terminal_width`` accept test-only overrides
(``AQ_FORCE_TTY``, ``COLUMNS``) so wrap/table behaviour is deterministic in
unit tests without a real pty.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import unicodedata

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def is_tty(err: bool = False) -> bool:
    """Whether the relevant output stream is a real terminal.

    Checks ``stderr`` when ``err=True``, else ``stdout``. ``AQ_FORCE_TTY``
    ("1"/"0") overrides the real check so tests can pin TTY vs. piped
    behaviour without a pty.
    """
    override = os.environ.get("AQ_FORCE_TTY")
    if override is not None:
        return override == "1"
    stream = sys.stderr if err else sys.stdout
    try:
        return bool(stream.isatty())
    except Exception:
        return False


def terminal_width(default: int = 80) -> int:
    """Current terminal width in columns.

    Reads ``COLUMNS`` first (test override), else
    ``shutil.get_terminal_size()``. Never returns less than 20 columns.
    """
    columns_env = os.environ.get("COLUMNS")
    if columns_env:
        try:
            width = int(columns_env)
        except ValueError:
            width = shutil.get_terminal_size(fallback=(default, 20)).columns
    else:
        width = shutil.get_terminal_size(fallback=(default, 20)).columns
    return max(20, width)


def strip_ansi(text: str) -> str:
    """Remove ANSI SGR escape sequences (``click.style`` output) from ``text``."""
    return _ANSI_RE.sub("", text)


def display_width(text: str) -> int:
    """Display width of ``text`` on a terminal.

    ANSI escapes are stripped first (contribute 0). East-Asian wide/fullwidth
    characters count as 2, combining marks count as 0, everything else counts
    as 1 — including the Aqueduct icon vocabulary (✓ ✗ ⚠ ⓘ ◆ · │ ├ └ ┆ ▶ ↳ ⊘ ⇄),
    several of which are Unicode "ambiguous width" and would otherwise measure
    as 2 under a naive East-Asian-width check.
    """
    stripped = strip_ansi(text)
    width = 0
    for ch in stripped:
        if unicodedata.combining(ch):
            continue
        eaw = unicodedata.east_asian_width(ch)
        if eaw in ("W", "F"):
            width += 2
        else:
            width += 1
    return width
