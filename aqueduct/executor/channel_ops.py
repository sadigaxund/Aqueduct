"""Channel op-name registry — re-exported from ``parser/channel_ops.py``.

The canonical definitions moved to ``aqueduct.parser.channel_ops`` so that
``parser/graph.py`` can use ``SQL_OPS`` without a `parser -> executor`
import (AGENTS.md's Layer rules only accept `parser/parser.py` importing
`executor/path_keys.py::get_path_keys` as a cross-layer exception; this
module's old location as the source of ``SQL_OPS`` was not one). This
module re-exports the same names unchanged so every existing importer
(``executor/spark/channel.py``, ``executor/duckdb_/channel.py``,
``executor/capability_leaves.py``, ``executor/probe_plugins.py``, ...)
keeps working with no changes.
"""

from __future__ import annotations

from aqueduct.parser.channel_ops import ALL_OPS, MULTI_INPUT_OPS, SINGLE_INPUT_OPS, SQL_OPS

__all__ = ["SQL_OPS", "SINGLE_INPUT_OPS", "MULTI_INPUT_OPS", "ALL_OPS"]
