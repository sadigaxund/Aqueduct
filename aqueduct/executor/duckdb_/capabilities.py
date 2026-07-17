"""DuckDB engine capability declaration (Phase 78 Stage A) — loader only.

Mirrors ``aqueduct/executor/spark/capabilities.py``: the declaration itself is
DATA (``capabilities.yml`` next to this file, one explicit row per capability
leaf, earned individually — never copied from Spark's table). This module
only loads and registers it. Must stay importable without ``duckdb``
installed (it is read by the compile-time capability gate, which runs before
any DuckDB connection exists, and by the closure test) — ``load_declaration``
is pure YAML parsing, no ``duckdb`` import.
"""

from __future__ import annotations

from pathlib import Path

from aqueduct.executor.capabilities import (
    CAPABILITY_REGISTRY,  # noqa: F401 — re-exported for convenience
    load_declaration,
    register,
)
from aqueduct.executor.capability_leaves import all_leaves
from aqueduct.executor.config_leaves import all_config_leaves

DECLARATION_PATH = Path(__file__).with_name("capabilities.yml")

_ALL_LEAVES = all_leaves() | all_config_leaves()

DUCKDB = load_declaration(DECLARATION_PATH, _ALL_LEAVES)

register(DUCKDB)

__all__ = ["DECLARATION_PATH", "DUCKDB"]
