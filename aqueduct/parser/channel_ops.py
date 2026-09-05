"""Channel op-name registry — engine-agnostic (no pyspark import).

Canonical home for the Channel op vocabulary. It lives in ``parser/`` (not
``executor/``) because `parser/graph.py`'s spillway/Junction-alias
validation needs ``SQL_OPS`` at parse time, and `Parser -> Compiler ->
Executor -> Surveyor` is the layer order — an `executor -> parser` import is
fine, but a `parser -> executor` import is not (see AGENTS.md's Layer rules;
the only accepted exception is `parser/parser.py` importing
`executor/path_keys.py::get_path_keys`, which this is not).

``aqueduct/executor/spark/channel.py`` imports ``pyspark`` at module level
(``from pyspark.sql import SparkSession``), so its op-name constants cannot
live there either if pyspark-free code (the capability-leaf walker, the
compile-time capability gate, and now `parser/graph.py`) needs them. Same
precedent as ``executor/path_keys.py`` and ``executor/probe_plugins.py`` —
hoist the pure data to a pyspark-free leaf and have the Spark module import
it back.

``aqueduct/executor/channel_ops.py`` re-exports everything from here
unchanged, so existing importers (``executor/spark/channel.py``,
``executor/duckdb_/channel.py``, ``executor/capability_leaves.py``, ...)
are unaffected.
"""

from __future__ import annotations

SQL_OPS: frozenset[str] = frozenset({"sql", "join"})
SINGLE_INPUT_OPS: frozenset[str] = frozenset(
    {
        "deduplicate",
        "filter",
        "select",
        "rename",
        "cast",
        "sort",
        "repartition",
        "coalesce",
        "cache",
    }
)
MULTI_INPUT_OPS: frozenset[str] = frozenset({"union"})

ALL_OPS: frozenset[str] = SQL_OPS | SINGLE_INPUT_OPS | MULTI_INPUT_OPS

__all__ = ["SQL_OPS", "SINGLE_INPUT_OPS", "MULTI_INPUT_OPS", "ALL_OPS"]
