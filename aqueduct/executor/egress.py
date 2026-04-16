"""Egress writer — persists a Spark DataFrame to a target store.

Supported formats: parquet, csv, delta.

The .save() call is the sanctioned Spark action in this layer.  Per the
zero-cost observability rule, no count()/show()/collect() is invoked.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module

SUPPORTED_FORMATS: frozenset[str] = frozenset({"parquet", "csv", "delta"})
SUPPORTED_MODES: frozenset[str] = frozenset({"overwrite", "append", "error", "errorifexists", "ignore"})


class EgressError(Exception):
    """Raised when an Egress module fails to write."""


def write_egress(df: DataFrame, module: Module) -> None:
    """Write df to the target described by module.config.

    Args:
        df:     DataFrame produced by upstream module(s).
        module: An Egress Module from the compiled Manifest.

    Raises:
        EgressError: Config invalid or write fails.
    """
    cfg = module.config

    fmt: str | None = cfg.get("format")
    if fmt not in SUPPORTED_FORMATS:
        raise EgressError(
            f"[{module.id}] unsupported format {fmt!r}. "
            f"Supported: {sorted(SUPPORTED_FORMATS)}"
        )

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

    writer.save(path)
