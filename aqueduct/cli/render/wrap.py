"""Shared truncation/wrap primitive for the CLI render funnel.

``wrap_line`` is the ONE place line-wrapping/truncation policy lives —
replacing the four ad-hoc, char-count-based caps the Phase 85 survey found
(``concise_error`` 300 chars, ``_short_warning`` 100 chars,
``TranscriptWriter._MAX_RAW_LINES`` 40 lines, ``reprompt_reason[:300]``).
It is pure: it returns a list of already-gutter-prefixed lines and never
echoes anything itself — the funnel (``render/funnel.py``) does the echoing.

Behaviour (owner-ratified, see SCREEN 6 in tmp/phase85/mockups.txt):

* Piped/CI (not a TTY): NEVER wrap, NEVER truncate. One logical record per
  input line, full text, gutter-prefixed. ``max_lines``/``hint`` do not apply.
* TTY: wrap to the display width, breaking on whitespace (falling back to a
  hard character break for a single token that alone exceeds the available
  width). Continuation lines carry ``gutter`` plus ``hang`` extra spaces.
* ``verbose=True`` lifts *truncation* (``max_lines`` ignored, no ``hint``
  marker) but wrapping on a TTY still happens — verbosity does not disable
  wrapping.
"""

from __future__ import annotations

from aqueduct.cli.render.width import display_width, is_tty, terminal_width

_HINT_ICON_COLOR = "cyan"  # matches render/style.py's _ICON_COLOR["ⓘ"]


def _wrap_text_to_width(text: str, width: int) -> list[str]:
    """Break ``text`` into lines no wider than ``width`` display columns,
    breaking on whitespace and hard-breaking a single overlong token."""
    if width < 1:
        width = 1
    words = text.split(" ")
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = word if not current else f"{current} {word}"
        if display_width(candidate) <= width:
            current = candidate
            continue
        # Candidate doesn't fit.
        if current:
            lines.append(current)
            current = ""
        # Now place `word` alone, hard-breaking it if it alone exceeds width.
        while display_width(word) > width:
            # Hard character break: walk the word accumulating chars until
            # adding the next one would exceed width.
            piece = ""
            for ch in word:
                if display_width(piece + ch) > width:
                    break
                piece += ch
            if not piece:
                # A single character wider than `width` itself — emit it
                # anyway to guarantee forward progress (no infinite loop).
                piece = word[0]
                word = word[1:]
            else:
                word = word[len(piece) :]
            lines.append(piece)
        current = word
    if current:
        lines.append(current)
    if not lines:
        lines = [""]
    return lines


def wrap_line(
    text: str,
    *,
    gutter: str = "",
    hang: int = 0,
    width: int | None = None,
    tty: bool | None = None,
    err: bool = False,
    verbose: bool = False,
    max_lines: int | None = None,
    hint: str | None = None,
) -> list[str]:
    """Wrap/truncate ``text`` into a list of already-gutter-prefixed lines.

    See module docstring for full semantics.
    """
    is_a_tty = is_tty(err=err) if tty is None else tty

    # Split embedded newlines up front — each logical input line is treated
    # independently in both branches.
    raw_lines = text.split("\n")

    if not is_a_tty:
        # Piped/CI: never wrap, never truncate, full text, gutter-prefixed.
        return [gutter + line for line in raw_lines]

    # TTY branch.
    w = terminal_width() if width is None else width
    hang_spaces = " " * hang
    available_first = max(1, w - display_width(gutter))
    available_cont = max(1, w - display_width(gutter) - hang)

    wrapped: list[str] = []
    for i, raw in enumerate(raw_lines):
        pieces = _wrap_text_to_width(raw, available_first if i == 0 else available_cont)
        for j, piece in enumerate(pieces):
            if i == 0 and j == 0:
                wrapped.append(gutter + piece)
            else:
                wrapped.append(gutter + hang_spaces + piece)

    truncated = False
    if not verbose and max_lines is not None and len(wrapped) > max_lines:
        n_more = len(wrapped) - max_lines
        wrapped = wrapped[:max_lines]
        wrapped.append(_dim(gutter + f"({n_more} more lines)"))
        truncated = True

    if not verbose and truncated and hint:
        wrapped.append(_cyan(gutter + hang_spaces + f"ⓘ {hint}: -v"))

    return wrapped


def _dim(text: str) -> str:
    """Style a wrap-primitive-generated structural line (the ``(N more
    lines)`` truncation tail) dim. It has no recognized icon-vocabulary
    leader, so ``render/style.py``'s ``colorize_line`` (applied downstream
    by the funnel) would leave it unstyled — style it here instead.

    Deferred import to avoid a module-load-order cycle with render/style.py
    (style.py has no dependency on wrap.py, so this is just import hygiene,
    not a real cycle)."""
    import click

    return click.style(text, dim=True)


def _cyan(text: str) -> str:
    """Style the ``ⓘ ...: -v`` hint line cyan, matching
    ``render/style.py``'s ``_ICON_COLOR["ⓘ"] = "cyan"`` vocabulary."""
    import click

    return click.style(text, fg="cyan")


def truncate(
    text: str,
    limit: int,
    *,
    verbose: bool = False,
    tty: bool | None = None,
) -> str:
    """Display-width-aware single-line truncation with a trailing ``…``.

    Returns ``text`` unchanged when not a TTY (piped/CI — full text always)
    or when ``verbose`` (``-v`` lifts truncation). This is the shared
    replacement for the ad-hoc char-count caps the Phase 85 survey found
    (``concise_error`` 300, ``_short_warning`` 100, etc.) — callers migrate
    to it individually; this function only provides the primitive.
    """
    is_a_tty = is_tty() if tty is None else tty
    if not is_a_tty or verbose:
        return text
    if display_width(text) <= limit:
        return text
    if limit <= 1:
        return "…"
    piece = ""
    for ch in text:
        if display_width(piece + ch) > limit - 1:
            break
        piece += ch
    return piece.rstrip() + "…"
