"""Aqueduct — Intelligent Spark Blueprint Engine.

Public API (Phase 30b — v1.0 stability contract):

    Parser:        parse, ParseError
    Warnings:      AqueductWarning (category for engine diagnostics)
    Version:       __version__

The names exported here are part of the v1.0 contract — semver applies.
Internal subpackages (`aqueduct.parser.*`, `aqueduct.compiler.*`, etc.)
may change between minor releases without notice. Import them only if
you accept the churn risk.

Deprecation policy: see README "Versioning & Stability". In short —
deprecated names keep working for one minor release with a
`DeprecationWarning`, then are removed in the next.

Exit codes (stable, see `aqueduct/exit_codes.py`):

    0  SUCCESS                 1  CONFIG_ERROR
    2  DATA_OR_RUNTIME         3  HEAL_PENDING
    4  VALIDATION_GATE         5  USAGE_ERROR
"""

from __future__ import annotations

try:
    # Sourced from installed package metadata so `aqueduct --version` always
    # reflects the actually-installed wheel, not a hardcoded literal that
    # drifts from pyproject.toml.
    from importlib.metadata import PackageNotFoundError, version as _pkg_version

    try:
        __version__ = _pkg_version("aqueduct-core")
    except PackageNotFoundError:
        __version__ = "0.0.0+unknown"
except Exception:  # pragma: no cover
    __version__ = "0.0.0+unknown"

from aqueduct.parser.parser import parse, ParseError
from aqueduct.errors import AqueductError
from aqueduct.warnings import AqueductWarning

__all__ = [
    "__version__",
    "parse",
    "ParseError",
    "AqueductError",
    "AqueductWarning",
]
