"""Back-compat re-export shim — the implementation lives in
``aqueduct.cli.render.style``.
"""

from __future__ import annotations

from aqueduct.cli.render.style import (  # noqa: F401
    COLOR,
    ICON,
    StyledLogFormatter,
    _color_enabled,
    _short_warning,
    colorize_line,
    dim,
    emit_warning_pairs,
    emit_warnings,
    error,
    info,
    success,
    warn,
)
