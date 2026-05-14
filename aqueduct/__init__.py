"""Aqueduct — Intelligent Spark Blueprint Engine."""

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

__all__ = ["__version__", "parse", "ParseError"]
