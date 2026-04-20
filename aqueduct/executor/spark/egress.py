"""Egress writer — persists a Spark DataFrame to a target store.

Any format string supported by the active SparkSession works (parquet, csv,
delta, jdbc, kafka, avro, orc, …).  The special pseudo-format ``depot``
writes a key-value pair to the Depot DuckDB store instead of a Spark path.

The .save() call is the sanctioned Spark action in this layer.  Per the
zero-cost observability rule, no count()/show()/collect() is invoked (except
for ``depot`` Egress with ``value_expr``, which opts-in to a single agg).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module

logger = logging.getLogger(__name__)

SUPPORTED_MODES: frozenset[str] = frozenset({"overwrite", "append", "error", "errorifexists", "ignore"})


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
