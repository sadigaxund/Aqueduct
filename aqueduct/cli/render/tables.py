"""Shared width-aware table renderer for interactive CLI output.

Owner ruling (Phase 85): narrative output stays hand-rolled ANSI via
``click.style`` — ``rich`` is adopted for INTERACTIVE TABLES ONLY, and only
through this module. Never import ``rich`` anywhere else in the narrative
path.

Two branches, matched to ``render/wrap.py``'s TTY policy:

* **TTY**: fixed columns (``Column(flex=False)``) are sized to their content
  (max of header/cell display width, floored at ``min_width``); exactly ONE
  flex column absorbs the remaining terminal width and truncates with a
  trailing ``…`` once its content would overflow — unless ``verbose=True``,
  which lifts truncation everywhere (full cell text, folded instead of
  cropped). Rendered via ``rich.table.Table`` on a ``rich.console.Console``,
  styled to match SCREEN 7 of ``tmp/phase85/mockups.txt``: no heavy box
  border, a plain header row plus a ``───`` rule (``rich.box.SIMPLE_HEAD``),
  dim header text.
* **Non-TTY (piped/CI)**: rich is not used at all. Plain aligned text —
  header row, an ASCII ``---`` rule, one padded row per record — sized from
  actual content (never the terminal width), full untruncated cell text, no
  ANSI, one logical record per line. This is the branch CI and ``| grep``
  see.

``render_table`` prints directly (through ``click.echo``/a real
``rich.console.Console`` with an explicit stream, never an implicit
default). ``render_table_str`` is the sibling that returns the rendered
text instead of printing it, for the one existing call site
(``benchmark_store.format_diff_table``) that had already committed to a
string-returning signature callers/tests may depend on — it is always
plain (no ANSI), matching that function's previous hand-rolled output.
"""

from __future__ import annotations

import io
import sys
from dataclasses import dataclass

import click

from aqueduct.cli.render.width import display_width, is_tty, terminal_width

_INDENT = "  "
_COL_GAP = 2  # display columns between adjacent table columns


@dataclass(frozen=True)
class Column:
    """One table column spec.

    ``flex=True`` marks the ONE column that absorbs remaining terminal
    width on a TTY and is truncated with ``…`` there when narrow. At most
    one column may set ``flex=True`` — ``render_table``/``render_table_str``
    raise ``ValueError`` otherwise.
    """

    header: str
    flex: bool = False
    align: str = "left"  # "left" | "right"
    min_width: int | None = None


def _pad(text: str, width: int, align: str) -> str:
    w = display_width(text)
    if w >= width:
        return text
    fill = " " * (width - w)
    return (fill + text) if align == "right" else (text + fill)


def _validate(columns: list[Column]) -> int:
    flex_idx = [i for i, c in enumerate(columns) if c.flex]
    if len(flex_idx) > 1:
        raise ValueError("render_table: at most one Column may set flex=True")
    return flex_idx[0] if flex_idx else -1


def _content_width(columns: list[Column], rows: list[list[str]], i: int) -> int:
    col = columns[i]
    w = display_width(col.header)
    for row in rows:
        w = max(w, display_width(row[i]))
    if col.min_width is not None:
        w = max(w, col.min_width)
    return w


def _render_plain(columns: list[Column], rows: list[list[str]]) -> list[str]:
    """Non-TTY branch: plain aligned text, full content, no rich, no ANSI."""
    widths = [_content_width(columns, rows, i) for i in range(len(columns))]
    gap = " " * _COL_GAP
    header = gap.join(_pad(c.header, w, c.align) for c, w in zip(columns, widths))
    rule = gap.join("-" * w for w in widths)
    lines = [_INDENT + header, _INDENT + rule]
    for row in rows:
        line = gap.join(_pad(cell, w, c.align) for cell, c, w in zip(row, columns, widths))
        lines.append(_INDENT + line)
    return [line.rstrip() for line in lines]


def _fixed_widths(
    columns: list[Column], rows: list[list[str]], flex_idx: int, width: int, verbose: bool
) -> list[int]:
    """Resolve every column's rendered width for the TTY branch: fixed
    columns get their content width; the flex column (if any) gets
    whatever remains of ``width`` after fixed columns + gaps + the shared
    leading indent (floored so it never collapses below 3 display columns
    — enough room for a lone ``…``)."""
    widths = [0 if i == flex_idx else _content_width(columns, rows, i) for i in range(len(columns))]
    if flex_idx < 0:
        return widths
    n = len(columns)
    overhead = sum(widths) + _COL_GAP * (n - 1) + len(_INDENT)
    flex_content_w = _content_width(columns, rows, flex_idx)
    available = width - overhead
    widths[flex_idx] = max(available, flex_content_w) if verbose else max(available, 3)
    return widths


def _build_table(
    columns: list[Column], rows: list[list[str]], *, flex_idx: int, widths: list[int], verbose: bool
):
    from rich import box as rich_box
    from rich.table import Table

    table = Table(
        box=rich_box.SIMPLE_HEAD,
        show_edge=False,
        pad_edge=False,
        padding=(0, 1, 0, 0),
        header_style="dim",
        expand=False,
    )
    for i, col in enumerate(columns):
        justify = "right" if col.align == "right" else "left"
        if i == flex_idx:
            if verbose:
                table.add_column(
                    col.header, justify=justify, width=widths[i], overflow="fold", no_wrap=False
                )
            else:
                table.add_column(
                    col.header, justify=justify, width=widths[i], overflow="ellipsis", no_wrap=True
                )
        else:
            # "crop" (not "ignore") — content is already <= width (widths[i]
            # was computed FROM this content), so cropping is a no-op, but
            # unlike "ignore" it makes rich actually pad the cell out to the
            # full column width, which is what makes `align="right"` (and
            # left-align header/body agreement) visible at all.
            table.add_column(
                col.header, justify=justify, width=widths[i], overflow="crop", no_wrap=True
            )
    for row in rows:
        table.add_row(*row)
    return table


def _console_width_for(widths: list[int]) -> int:
    """Total display width the table actually needs: fixed/flex column
    widths + inter-column gaps + the shared leading indent. Passed as the
    ``rich.console.Console`` width so rich never silently re-shrinks a
    column we already sized (which it otherwise does whenever the naive
    ``terminal_width()`` is narrower than a ``verbose=True`` flex column
    that was deliberately widened to fit its full, untruncated content)."""
    return len(_INDENT) + sum(widths) + _COL_GAP * max(0, len(widths) - 1)


def _print_table(table, *, file, width: int, color: bool) -> None:
    from rich.console import Console
    from rich.padding import Padding

    console = Console(
        file=file,
        width=width,
        no_color=not color,
        force_terminal=color or None,
        highlight=False,
    )
    console.print(Padding(table, (0, 0, 0, len(_INDENT))))


def _render_tty_lines(
    columns: list[Column], rows: list[list[str]], *, flex_idx: int, width: int, verbose: bool
) -> list[str]:
    """Render the TTY branch to plain text lines (no colour) — shared by
    ``render_table_str`` and by the pipe-safety/width unit tests, which
    assert on line CONTENT/width rather than ANSI styling."""
    widths = _fixed_widths(columns, rows, flex_idx, width, verbose)
    table = _build_table(columns, rows, flex_idx=flex_idx, widths=widths, verbose=verbose)
    console_width = _console_width_for(widths)
    buf = io.StringIO()
    _print_table(table, file=buf, width=console_width, color=False)
    lines = buf.getvalue().split("\n")
    while lines and lines[-1] == "":
        lines.pop()
    return [line.rstrip() for line in lines]


def _build_lines(
    columns: list[Column], rows: list[list[str]], *, tty: bool, verbose: bool, width: int
) -> list[str]:
    if not columns:
        return []
    if not tty:
        return _render_plain(columns, rows)
    flex_idx = _validate(columns)
    return _render_tty_lines(columns, rows, flex_idx=flex_idx, width=width, verbose=verbose)


def render_table(
    columns: list[Column],
    rows: list[list[str]],
    *,
    err: bool = False,
    verbose: bool = False,
    tty: bool | None = None,
    width: int | None = None,
) -> None:
    """Render ``rows`` under ``columns`` — the ONE entry point for
    interactive tables. Tables are results, so they default to stdout
    (``err=False``); pass ``err=True`` for a table that belongs on stderr.
    """
    if not columns:
        return
    flex_idx = _validate(columns)
    is_a_tty = is_tty(err=err) if tty is None else tty
    w = terminal_width() if width is None else width

    if not is_a_tty:
        for line in _render_plain(columns, rows):
            click.echo(line, err=err)
        return

    widths = _fixed_widths(columns, rows, flex_idx, w, verbose)
    table = _build_table(columns, rows, flex_idx=flex_idx, widths=widths, verbose=verbose)
    console_width = _console_width_for(widths)
    stream = sys.stderr if err else sys.stdout
    _print_table(table, file=stream, width=console_width, color=True)


def render_table_str(
    columns: list[Column],
    rows: list[list[str]],
    *,
    verbose: bool = False,
    tty: bool | None = None,
    width: int | None = None,
) -> str:
    """Same layout as ``render_table`` but returns the text instead of
    printing it — for callers (``benchmark_store.format_diff_table``) that
    already committed to a string-returning signature tests may assert on.
    Always plain (no ANSI/colour), matching that function's previous
    hand-rolled output.
    """
    _validate(columns)
    is_a_tty = is_tty() if tty is None else tty
    w = terminal_width() if width is None else width
    lines = _build_lines(columns, rows, tty=is_a_tty, verbose=verbose, width=w)
    return "\n".join(lines)
