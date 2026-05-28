"""Egress writer — persists a Spark DataFrame to a target store.

Any format string supported by the active SparkSession works (parquet, csv,
delta, jdbc, kafka, avro, orc, …).  The special pseudo-format ``depot``
writes a key-value pair to the Depot DuckDB store instead of a Spark path.

The .save() call is the sanctioned Spark action in this layer.  Per the
zero-cost observability rule, no count()/show()/collect() is invoked (except
for ``depot`` Egress with ``value_expr``, which opts-in to a single agg).

Post-write maintenance (Delta only):
  OPTIMIZE   — compacts small files; optional ZORDER BY columns
  VACUUM     — removes files older than retention_hours from the Delta log
Both run synchronously after the write action and are non-fatal (logged as
warnings on failure so the pipeline does not abort on a maintenance error).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from aqueduct.parser.models import Module

logger = logging.getLogger(__name__)

SUPPORTED_MODES: frozenset[str] = frozenset({"overwrite", "append", "error", "errorifexists", "ignore", "merge"})


class EgressError(Exception):
    """Raised when an Egress module fails to write."""


def write_egress(df: DataFrame, module: Module, depot: Any = None) -> None:
    """Write df to the target described by module.config.

    Args:
        df:     DataFrame produced by upstream module(s).
        module: An Egress Module from the compiled Manifest.
        depot:  Optional DepotStore instance for ``format: depot`` writes.

    Raises:
        EgressError: Config invalid or write fails.
    """
    cfg = module.config

    fmt: str | None = cfg.get("format")
    if not fmt:
        raise EgressError(f"[{module.id}] 'format' is required in Egress config")

    # ── Depot pseudo-format ────────────────────────────────────────────────────
    if fmt == "depot":
        _write_depot(df, module, depot)
        return

    # ── Spark writer ──────────────────────────────────────────────────────────
    path: str | None = cfg.get("path")
    if not path:
        raise EgressError(f"[{module.id}] 'path' is required in Egress config")

    mode: str = cfg.get("mode", "error")
    if mode not in SUPPORTED_MODES:
        raise EgressError(
            f"[{module.id}] unsupported write mode {mode!r}. "
            f"Supported: {sorted(SUPPORTED_MODES)}"
        )

    if mode == "merge":
        _write_merge(df, module)
        return

    writer = df.write.format(fmt).mode(mode)

    partition_by: list[str] | None = cfg.get("partition_by")
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    for key, value in cfg.get("options", {}).items():
        writer = writer.option(str(key), str(value))

    try:
        writer.save(path)
    except EgressError:
        raise
    except Exception as exc:
        raise EgressError(
            f"[{module.id}] write failed to {path!r}: {exc}"
        ) from exc

    register_as: str | None = cfg.get("register_as_table")
    if register_as:
        _register_external_table(df, module.id, register_as, fmt, path)


def _register_external_table(
    df: "DataFrame",
    module_id: str,
    table_name: str,
    fmt: str,
    path: str,
) -> None:
    """Register an external table in the active Spark catalog (non-fatal)."""
    try:
        spark = df.sparkSession
        # Derive schema DDL from DataFrame — no Spark action, schema is always available
        schema_ddl = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()}"
            for field in df.schema.fields
        )
        spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} ({schema_ddl})
            USING {fmt}
            LOCATION '{path}'
        """)
        logger.info("Registered external table %r at %s", table_name, path)
    except Exception as exc:
        logger.warning(
            "[%s] register_as_table %r failed (non-fatal): %s",
            module_id, table_name, exc,
        )


def _write_merge(df: "DataFrame", module: Module) -> None:
    """Delta MERGE INTO: upsert df into an existing Delta table.

    Config keys:
      format:    must be 'delta'
      path:      path to Delta table (use delta.`path` syntax)
      table:     catalog table name (takes precedence over path)
      merge_key: column name or list of column names for the ON clause
    """
    cfg = module.config
    fmt = cfg.get("format")
    if fmt != "delta":
        raise EgressError(
            f"[{module.id}] mode=merge only supported with format=delta, got {fmt!r}"
        )

    table: str | None = cfg.get("table")
    path: str | None = cfg.get("path")
    if not table and not path:
        raise EgressError(f"[{module.id}] mode=merge requires 'path' or 'table'")

    merge_key = cfg.get("merge_key")
    if not merge_key:
        raise EgressError(f"[{module.id}] mode=merge requires 'merge_key'")
    keys: list[str] = [merge_key] if isinstance(merge_key, str) else list(merge_key)

    target = table if table else f"delta.`{path}`"
    view_name = "_aq_merge_src"
    spark = df.sparkSession

    try:
        spark.catalog.dropTempView(view_name)
    except Exception:
        pass  # view may not exist on first merge
    df.createTempView(view_name)

    on_clause = " AND ".join(
        f"_aq_target.{k} = _aq_src.{k}" for k in keys
    )
    merge_sql = (
        f"MERGE INTO {target} AS _aq_target "
        f"USING {view_name} AS _aq_src "
        f"ON {on_clause} "
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )

    try:
        spark.sql(merge_sql)
    except Exception as exc:
        raise EgressError(
            f"[{module.id}] mode=merge failed against {target!r}: {exc}"
        ) from exc
    finally:
        spark.catalog.dropTempView(view_name)

    logger.info("[%s] merge completed into %s on keys %s", module.id, target, keys)


def run_maintenance(
    spark: "SparkSession",
    module_id: str,
    path: str,
    maintenance_cfg: dict[str, Any],
) -> dict[str, Any]:
    """Run post-write maintenance operations on a Delta table.

    Runs synchronously after the Egress write action.  Both OPTIMIZE and VACUUM
    are non-fatal — failures are logged as warnings and the pipeline continues.

    Args:
        spark:           Active SparkSession.
        module_id:       Egress module ID (used in log messages).
        path:            Delta table path (must already exist on disk).
        maintenance_cfg: The ``maintenance:`` dict from module.config.

    Returns:
        Timing dict: {"optimize_ms": int | None, "vacuum_ms": int | None}
    """
    result: dict[str, Any] = {"optimize_ms": None, "vacuum_ms": None}

    if maintenance_cfg.get("optimize"):
        zorder = maintenance_cfg.get("zorder_by", [])
        if isinstance(zorder, str):
            zorder = [zorder]
        zorder_clause = f" ZORDER BY ({', '.join(str(c) for c in zorder)})" if zorder else ""
        sql = f"OPTIMIZE delta.`{path}`{zorder_clause}"
        t0 = time.monotonic()
        try:
            spark.sql(sql)
            result["optimize_ms"] = int((time.monotonic() - t0) * 1000)
            logger.info("[%s] OPTIMIZE completed in %dms", module_id, result["optimize_ms"])
        except Exception as exc:
            logger.warning("[%s] OPTIMIZE failed (non-fatal): %s", module_id, exc)

    vacuum_hours = maintenance_cfg.get("vacuum")
    if vacuum_hours is not None:
        sql = f"VACUUM delta.`{path}` RETAIN {int(vacuum_hours)} HOURS"
        t0 = time.monotonic()
        try:
            spark.sql(sql)
            result["vacuum_ms"] = int((time.monotonic() - t0) * 1000)
            logger.info("[%s] VACUUM completed in %dms", module_id, result["vacuum_ms"])
        except Exception as exc:
            logger.warning("[%s] VACUUM failed (non-fatal): %s", module_id, exc)

    return result


def _write_depot(df: "DataFrame", module: Module, depot: Any) -> None:
    """Write a KV entry to the Depot store. ``depot`` must not be None."""
    from pyspark.sql import functions as F

    cfg = module.config
    key: str | None = cfg.get("key")
    if not key:
        raise EgressError(f"[{module.id}] depot Egress requires 'key'")

    if depot is None:
        raise EgressError(
            f"[{module.id}] depot Egress configured but no DepotStore is wired. "
            "Pass --config with a valid depot store path."
        )

    value_expr: str | None = cfg.get("value_expr")
    if value_expr:
        # Opt-in Spark action: single aggregate over the DataFrame
        try:
            agg_result = df.agg(F.expr(value_expr)).collect()[0][0]
            value = "" if agg_result is None else str(agg_result)
        except Exception as exc:
            raise EgressError(
                f"[{module.id}] depot value_expr {value_expr!r} failed: {exc}"
            ) from exc
    else:
        raw_value: str | None = cfg.get("value")
        if raw_value is None:
            raise EgressError(
                f"[{module.id}] depot Egress requires 'value' or 'value_expr'"
            )
        value = str(raw_value)

    try:
        depot.put(key, value)
    except Exception as exc:
        raise EgressError(
            f"[{module.id}] depot.put({key!r}) failed: {exc}"
        ) from exc
    logger.info("Depot write: %s = %r", key, value)
