"""Channel executor — dispatches all Channel op types against upstream DataFrames.

Supported ops:
  SQL-based (register temp views, run Spark SQL):
    sql          — arbitrary SQL query; upstreams available as temp views by module ID
    join         — LEFT/INNER/RIGHT/FULL/SEMI/ANTI/CROSS join with optional broadcast hint

  DataFrame API — single upstream input:
    deduplicate  — dropDuplicates() or Window+row_number() when order_by is set
    filter       — df.filter(condition)
    select       — df.select(*columns)
    rename       — df.withColumnRenamed() for one or many columns
    cast         — df.withColumn(col, col.cast(type)) for one or many columns
    sort         — df.orderBy(*exprs); NOTE: triggers a full shuffle — avoid in critical path
    repartition  — df.repartition(n[, col]); full shuffle; use to increase or rebalance partitions
    coalesce     — df.coalesce(n); no shuffle; use to shrink partition count before single-file Egress
    cache        — df.persist(StorageLevel); optional storage_level config

  DataFrame API — multi-upstream:
    union        — df.unionByName(*others, allowMissingColumns=True)

Context tokens (${ctx.*}) are already resolved by the Compiler before the
Manifest reaches the Executor; all config values are used verbatim.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module

_SINGLE_INPUT_ALIAS = "__input__"

_VALID_JOIN_TYPES = frozenset(
    {"inner", "left", "right", "full", "semi", "anti", "cross"}
)

_SQL_OPS = frozenset({"sql", "join"})
_SINGLE_INPUT_OPS = frozenset(
    {"deduplicate", "filter", "select", "rename", "cast", "sort", "repartition", "coalesce", "cache"}
)
_MULTI_INPUT_OPS = frozenset({"union"})

_ALL_OPS = _SQL_OPS | _SINGLE_INPUT_OPS | _MULTI_INPUT_OPS


class ChannelError(Exception):
    """Raised when a Channel module fails to execute."""


# ── SQL helpers ───────────────────────────────────────────────────────────────

def _build_join_query(module_id: str, cfg: dict) -> str:
    left: str | None = cfg.get("left")
    right: str | None = cfg.get("right")
    join_type: str = str(cfg.get("join_type", "inner")).lower()
    condition: str | None = cfg.get("condition")
    broadcast_side: str | None = cfg.get("broadcast_side")

    if not left:
        raise ChannelError(f"[{module_id}] op=join requires 'left'")
    if not right:
        raise ChannelError(f"[{module_id}] op=join requires 'right'")
    if join_type not in _VALID_JOIN_TYPES:
        raise ChannelError(
            f"[{module_id}] op=join invalid join_type {join_type!r}. "
            f"Valid: {sorted(_VALID_JOIN_TYPES)}"
        )
    if join_type != "cross" and not condition:
        raise ChannelError(
            f"[{module_id}] op=join requires 'condition' for join_type={join_type!r}"
        )

    hint = ""
    if broadcast_side == "left":
        hint = f"/*+ BROADCAST({left}) */ "
    elif broadcast_side == "right":
        hint = f"/*+ BROADCAST({right}) */ "

    on_clause = f" ON {condition}" if condition else ""
    return f"SELECT {hint}* FROM {left} {join_type.upper()} JOIN {right}{on_clause}"


def _run_sql(
    module_id: str,
    query: str,
    upstream_dfs: dict[str, DataFrame],
    spark: SparkSession,
) -> DataFrame:
    registered: list[str] = []
    for upstream_id, df in upstream_dfs.items():
        spark.catalog.dropTempView(upstream_id)
        df.createTempView(upstream_id)
        registered.append(upstream_id)

    if len(upstream_dfs) == 1:
        spark.catalog.dropTempView(_SINGLE_INPUT_ALIAS)
        next(iter(upstream_dfs.values())).createTempView(_SINGLE_INPUT_ALIAS)
        registered.append(_SINGLE_INPUT_ALIAS)

    try:
        return spark.sql(query)
    except Exception as exc:
        raise ChannelError(f"[{module_id}] SQL execution failed: {exc}") from exc
    finally:
        for view_name in registered:
            spark.catalog.dropTempView(view_name)


# ── DataFrame op implementations ──────────────────────────────────────────────

def _execute_deduplicate(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    key = cfg.get("key")
    order_by: str | None = cfg.get("order_by")

    key_cols: list[str] | None = None
    if key:
        key_cols = [key] if isinstance(key, str) else list(key)

    if order_by:
        # Window + row_number: pick the row ranked 1 per key (order_by determines which)
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        if not key_cols:
            raise ChannelError(
                f"[{module_id}] op=deduplicate with order_by requires 'key'"
            )
        w = Window.partitionBy(key_cols).orderBy(F.expr(order_by))
        return (
            df.withColumn("_aq_rank", F.row_number().over(w))
              .filter("_aq_rank = 1")
              .drop("_aq_rank")
        )

    return df.dropDuplicates(key_cols) if key_cols else df.dropDuplicates()


def _execute_filter(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    condition: str | None = cfg.get("condition") or cfg.get("expr")
    if not condition:
        raise ChannelError(f"[{module_id}] op=filter requires 'condition'")
    try:
        return df.filter(condition)
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=filter failed: {exc}") from exc


def _execute_select(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    columns = cfg.get("columns") or cfg.get("cols")
    if not columns:
        raise ChannelError(f"[{module_id}] op=select requires 'columns'")
    cols = [columns] if isinstance(columns, str) else list(columns)
    try:
        return df.select(*cols)
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=select failed: {exc}") from exc


def _execute_rename(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    columns = cfg.get("columns")
    if not columns:
        raise ChannelError(f"[{module_id}] op=rename requires 'columns'")

    if isinstance(columns, dict):
        mapping: dict[str, str] = columns
    elif isinstance(columns, list):
        # list of {from: old, to: new} dicts
        mapping = {}
        for item in columns:
            if isinstance(item, dict):
                old = item.get("from") or item.get("old")
                new = item.get("to") or item.get("new")
                if old and new:
                    mapping[old] = new
    else:
        raise ChannelError(
            f"[{module_id}] op=rename 'columns' must be a dict {{old: new}} "
            "or a list of {{from, to}} objects"
        )

    result = df
    for old, new in mapping.items():
        result = result.withColumnRenamed(old, new)
    return result


def _execute_cast(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    from pyspark.sql import functions as F

    columns = cfg.get("columns")
    if not columns:
        raise ChannelError(f"[{module_id}] op=cast requires 'columns'")

    if isinstance(columns, dict):
        mapping: dict[str, str] = columns
    elif isinstance(columns, list):
        # list of {column: name, type: spark_type} dicts
        mapping = {}
        for item in columns:
            if isinstance(item, dict):
                col_name = item.get("column") or item.get("name")
                col_type = item.get("type")
                if col_name and col_type:
                    mapping[col_name] = col_type
    else:
        raise ChannelError(
            f"[{module_id}] op=cast 'columns' must be a dict {{col: type}} "
            "or a list of {{column, type}} objects"
        )

    result = df
    for col_name, col_type in mapping.items():
        try:
            result = result.withColumn(col_name, F.col(col_name).cast(col_type))
        except Exception as exc:
            raise ChannelError(
                f"[{module_id}] op=cast failed for column {col_name!r} → {col_type!r}: {exc}"
            ) from exc
    return result


def _execute_sort(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    from pyspark.sql import functions as F

    order_by = cfg.get("order_by") or cfg.get("columns")
    if not order_by:
        raise ChannelError(f"[{module_id}] op=sort requires 'order_by'")

    exprs = [order_by] if isinstance(order_by, str) else list(order_by)

    def _to_col(e: str) -> "Column":
        # F.expr("amount DESC") misparses as `amount AS DESC` in expression context;
        # use F.col + .asc()/.desc() to preserve sort direction.
        parts = e.strip().split()
        col = F.col(parts[0])
        return col.desc() if len(parts) > 1 and parts[-1].upper() == "DESC" else col.asc()

    try:
        return df.orderBy(*[_to_col(e) for e in exprs])
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=sort failed: {exc}") from exc


def _execute_repartition(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    num = cfg.get("num_partitions") or cfg.get("num")
    if not num:
        raise ChannelError(f"[{module_id}] op=repartition requires 'num_partitions'")
    col: str | None = cfg.get("column") or cfg.get("col")
    try:
        return df.repartition(int(num), col) if col else df.repartition(int(num))
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=repartition failed: {exc}") from exc


def _execute_coalesce(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    num = cfg.get("num_partitions") or cfg.get("num")
    if not num:
        raise ChannelError(f"[{module_id}] op=coalesce requires 'num_partitions'")
    try:
        return df.coalesce(int(num))
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=coalesce failed: {exc}") from exc


_VALID_STORAGE_LEVELS = frozenset({
    "MEMORY_AND_DISK", "MEMORY_AND_DISK_SER",
    "MEMORY_ONLY", "MEMORY_ONLY_SER",
    "DISK_ONLY", "DISK_ONLY_2",
    "OFF_HEAP",
})


def _execute_cache(module_id: str, df: DataFrame, cfg: dict) -> DataFrame:
    from pyspark import StorageLevel

    level_name = str(cfg.get("storage_level", "MEMORY_AND_DISK")).upper()
    if level_name not in _VALID_STORAGE_LEVELS:
        raise ChannelError(
            f"[{module_id}] op=cache unknown storage_level {level_name!r}. "
            f"Valid: {sorted(_VALID_STORAGE_LEVELS)}"
        )
    level = getattr(StorageLevel, level_name)
    return df.persist(level)


def _execute_union(
    module_id: str, dfs: list[DataFrame], cfg: dict
) -> DataFrame:
    if len(dfs) < 2:
        raise ChannelError(
            f"[{module_id}] op=union requires at least 2 upstream DataFrames; "
            f"got {len(dfs)}"
        )
    allow_missing: bool = cfg.get("allow_missing_columns", True)
    result = dfs[0]
    for other in dfs[1:]:
        try:
            result = result.unionByName(other, allowMissingColumns=allow_missing)
        except Exception as exc:
            raise ChannelError(f"[{module_id}] op=union failed: {exc}") from exc
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def execute_channel(
    module: Module,
    upstream_dfs: dict[str, DataFrame],
    spark: SparkSession,
) -> DataFrame:
    """Dispatch a Channel module to the correct op handler.

    Args:
        module:       Channel Module from the compiled Manifest.
        upstream_dfs: Mapping of upstream module ID → DataFrame (topo order).
        spark:        Active SparkSession (caller owns lifecycle).

    Returns:
        Lazy result DataFrame. No Spark actions fired except op=cache (persist)
        and op=sort (full sort is deferred until action, but plan is materialised).

    Raises:
        ChannelError: Config invalid, unknown op, or execution failure.
    """
    cfg = module.config
    op: str | None = cfg.get("op")

    if not upstream_dfs:
        raise ChannelError(
            f"[{module.id}] Channel has no upstream DataFrames; "
            "at least one main-port input is required"
        )

    if op not in _ALL_OPS:
        raise ChannelError(
            f"[{module.id}] unsupported Channel op {op!r}. "
            f"Supported: {sorted(_ALL_OPS)}"
        )

    # ── SQL ops ───────────────────────────────────────────────────────────────
    if op in _SQL_OPS:
        if op == "join":
            query = _build_join_query(module.id, cfg)
        else:
            query = cfg.get("query")
            if not query or not query.strip():
                raise ChannelError(
                    f"[{module.id}] op=sql requires a non-empty 'query'"
                )
        return _run_sql(module.id, query, upstream_dfs, spark)

    # ── Multi-upstream ops ────────────────────────────────────────────────────
    if op in _MULTI_INPUT_OPS:
        return _execute_union(module.id, list(upstream_dfs.values()), cfg)

    # ── Single-upstream ops ───────────────────────────────────────────────────
    if len(upstream_dfs) > 1:
        raise ChannelError(
            f"[{module.id}] op={op!r} takes exactly one upstream DataFrame; "
            f"got {len(upstream_dfs)}. Use op=union first to combine inputs."
        )
    df = next(iter(upstream_dfs.values()))

    if op == "deduplicate":
        return _execute_deduplicate(module.id, df, cfg)
    if op == "filter":
        return _execute_filter(module.id, df, cfg)
    if op == "select":
        return _execute_select(module.id, df, cfg)
    if op == "rename":
        return _execute_rename(module.id, df, cfg)
    if op == "cast":
        return _execute_cast(module.id, df, cfg)
    if op == "sort":
        return _execute_sort(module.id, df, cfg)
    if op == "repartition":
        return _execute_repartition(module.id, df, cfg)
    if op == "coalesce":
        return _execute_coalesce(module.id, df, cfg)
    if op == "cache":
        return _execute_cache(module.id, df, cfg)

    # unreachable — _ALL_OPS guard above catches unknowns
    raise ChannelError(f"[{module.id}] unhandled op {op!r}")


# Backwards-compatible alias — executor.py imports this name
execute_sql_channel = execute_channel
