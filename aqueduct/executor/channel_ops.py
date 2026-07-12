"""Channel op-name registry — engine-agnostic (no pyspark import).

``aqueduct/executor/spark/channel.py`` imports ``pyspark`` at module level
(``from pyspark.sql import SparkSession``), so its op-name constants cannot
be imported by code that must stay pyspark-free (the capability-leaf walker,
the compile-time capability gate). Same precedent as ``path_keys.py`` and
``probe_plugins.py`` — hoist the pure data to the executor top level and have
the Spark module import it back.

This module owns the canonical Channel op-name sets; ``spark/channel.py``
imports ``_ALL_OPS`` etc. from here instead of defining them locally.
"""

from __future__ import annotations

SQL_OPS: frozenset[str] = frozenset({"sql", "join"})
SINGLE_INPUT_OPS: frozenset[str] = frozenset(
    {"deduplicate", "filter", "select", "rename", "cast", "sort", "repartition", "coalesce", "cache"}
)
MULTI_INPUT_OPS: frozenset[str] = frozenset({"union"})

ALL_OPS: frozenset[str] = SQL_OPS | SINGLE_INPUT_OPS | MULTI_INPUT_OPS

__all__ = ["SQL_OPS", "SINGLE_INPUT_OPS", "MULTI_INPUT_OPS", "ALL_OPS"]
