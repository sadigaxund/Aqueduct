"""CLI render funnel — the ONE vocabulary for user-facing output.

Public package absorbing the former ``aqueduct/cli/output.py`` (structured
results) and ``aqueduct/cli/style.py`` (icon/colour vocabulary) into one
funnel, plus the shared terminal-width / wrap / truncate primitives
(``render/width.py``, ``render/wrap.py``) that back the TTY-aware narrative
line emitter (``echo`` and friends, in ``render/funnel.py``).

``aqueduct/cli/output.py`` and ``aqueduct/cli/style.py`` remain as thin
back-compat re-export shims — ~40 import sites across ``aqueduct/cli/*.py``
and ``tests/`` reference those module paths directly and keep working
unchanged. New code should import from here (``aqueduct.cli.render``) or
from the submodules directly (``aqueduct.cli.render.style``,
``aqueduct.cli.render.funnel``, ``aqueduct.cli.render.width``,
``aqueduct.cli.render.wrap``).
"""

from __future__ import annotations

from aqueduct.cli.render.funnel import (
    echo,
    emit,
    emit_info,
    error,
    format_bytes,
    gutter,
    info,
    result,
    success,
    warn,
    warn_line,
)
from aqueduct.cli.render.style import (
    COLOR,
    ICON,
    StyledLogFormatter,
)
from aqueduct.cli.render.style import (
    colorize_line as colorize_line,
)
from aqueduct.cli.render.style import (
    dim as dim,
)
from aqueduct.cli.render.style import (
    emit_warning_pairs as emit_warning_pairs,
)
from aqueduct.cli.render.style import (
    emit_warnings as emit_warnings,
)
from aqueduct.cli.render.style import error as style_error
from aqueduct.cli.render.style import info as style_info
from aqueduct.cli.render.style import success as style_success
from aqueduct.cli.render.style import warn as style_warn
from aqueduct.cli.render.width import (
    display_width,
    is_tty,
    strip_ansi,
    terminal_width,
)
from aqueduct.cli.render.wrap import truncate, wrap_line

__all__ = [
    # funnel.py — structured results + narrative lines
    "emit",
    "emit_info",
    "warn",
    "format_bytes",
    "echo",
    "info",
    "success",
    "warn_line",
    "error",
    "result",
    "gutter",
    # style.py — icon/colour vocabulary (module-level style.* also exposed
    # under a `style_` prefix here to avoid shadowing funnel's narrative
    # wrappers of the same short name)
    "ICON",
    "COLOR",
    "StyledLogFormatter",
    "colorize_line",
    "dim",
    "emit_warning_pairs",
    "emit_warnings",
    "style_error",
    "style_success",
    "style_warn",
    "style_info",
    # width.py
    "is_tty",
    "terminal_width",
    "display_width",
    "strip_ansi",
    # wrap.py
    "wrap_line",
    "truncate",
]
