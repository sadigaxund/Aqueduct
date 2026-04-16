"""SQL Channel executor — runs a Spark SQL query against upstream DataFrames.

Execution model:
  1. Register each upstream DataFrame as a temp view named after the upstream
     module ID.  This lets the query reference sources by their pipeline ID
     (e.g. ``SELECT * FROM read_input``).
  2. For single-input Channels, also register the upstream DataFrame as the
     reserved alias ``__input__``, allowing queries that omit the FROM clause
     or use the generic alias instead of the source module ID.
  3. Execute ``spark.sql(query)`` — returns a lazy DataFrame.  Zero actions.
  4. Drop all temp views registered in this call to avoid leaking names across
     subsequent Channel executions.

UDF registry is accepted but treated as a stub in Phase 4.  Full UDF
registration (via ``spark.udf.register``) is wired in a later phase.

Context tokens (``${ctx.*}``) are already resolved by the Compiler before the
Manifest reaches the Executor; the query string is used verbatim.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module

# Reserved alias for the single upstream in a single-input Channel.
_SINGLE_INPUT_ALIAS = "__input__"


class ChannelError(Exception):
    """Raised when a Channel module fails to execute."""


def execute_sql_channel(
    module: Module,
    upstream_dfs: dict[str, DataFrame],
    spark: SparkSession,
    udf_registry: dict | None = None,  # stub — ignored until Phase 5
) -> DataFrame:
    """Execute a ``op: sql`` Channel against upstream DataFrames.

    Args:
        module:        A Channel Module from the compiled Manifest.
        upstream_dfs:  Mapping of upstream module ID → DataFrame, in
                       topological order.  All entries are registered as
                       temp views before the query runs.
        spark:         Active SparkSession (caller owns lifecycle).
        udf_registry:  Reserved for future UDF support.  Ignored in Phase 4.

    Returns:
        Lazy result DataFrame.  No Spark actions fired.

    Raises:
        ChannelError: Config invalid, unsupported op, or SQL execution failure.
    """
    cfg = module.config

    op: str | None = cfg.get("op")
    if op != "sql":
        raise ChannelError(
            f"[{module.id}] unsupported Channel op {op!r}. Only 'sql' is implemented."
        )

    query: str | None = cfg.get("query")
    if not query or not query.strip():
        raise ChannelError(f"[{module.id}] 'query' is required and must not be empty")

    if not upstream_dfs:
        raise ChannelError(
            f"[{module.id}] Channel has no upstream DataFrames; "
            f"at least one main-port input is required"
        )

    # ── Register temp views ───────────────────────────────────────────────────
    registered: list[str] = []

    for upstream_id, df in upstream_dfs.items():
        spark.catalog.dropTempView(upstream_id)
        df.createTempView(upstream_id)
        registered.append(upstream_id)

    # Single-input convenience alias
    if len(upstream_dfs) == 1:
        spark.catalog.dropTempView(_SINGLE_INPUT_ALIAS)
        single_df = next(iter(upstream_dfs.values()))
        single_df.createTempView(_SINGLE_INPUT_ALIAS)
        registered.append(_SINGLE_INPUT_ALIAS)

    # ── Execute SQL ───────────────────────────────────────────────────────────
    try:
        result_df: DataFrame = spark.sql(query)
    except Exception as exc:
        raise ChannelError(
            f"[{module.id}] SQL execution failed: {exc}"
        ) from exc
    finally:
        # Always drop views — keeps the catalog clean across modules
        for view_name in registered:
            spark.catalog.dropTempView(view_name)

    return result_df
