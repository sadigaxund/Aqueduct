"""Back-compat re-export shim — the implementation lives in
``aqueduct.cli.render.funnel``.
"""

from __future__ import annotations

from aqueduct.cli.render.funnel import (  # noqa: F401
    emit,
    emit_info,
    format_bytes,
    warn,
)
