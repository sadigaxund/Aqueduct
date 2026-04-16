"""Ingress reader — loads source data as a lazy Spark DataFrame.

Supported formats: parquet, csv, json.

No Spark actions are triggered here.  The returned DataFrame is lazy;
the execution plan is only materialised when downstream Egress calls .save().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module

SUPPORTED_FORMATS: frozenset[str] = frozenset({"parquet", "csv", "json"})


class IngressError(Exception):
    """Raised when an Ingress module fails to read."""


def read_ingress(module: Module, spark: SparkSession) -> DataFrame:
    """Read source data described by module.config.

    Args:
        module: An Ingress Module from the compiled Manifest.
        spark:  Active SparkSession (caller owns lifecycle).

    Returns:
        Lazy DataFrame — no Spark actions fired.

    Raises:
        IngressError: Config invalid or schema_hint mismatch.
    """
    cfg = module.config

    fmt: str | None = cfg.get("format")
    if fmt not in SUPPORTED_FORMATS:
        raise IngressError(
            f"[{module.id}] unsupported format {fmt!r}. "
            f"Supported: {sorted(SUPPORTED_FORMATS)}"
        )

    path: str | None = cfg.get("path")
    if not path:
        raise IngressError(f"[{module.id}] 'path' is required in Ingress config")

    reader = spark.read.format(fmt)

    # Format-specific defaults (user options override below)
    if fmt == "csv":
        reader = reader.option("header", str(cfg.get("header", True)).lower())
        reader = reader.option("inferSchema", str(cfg.get("infer_schema", True)).lower())

    for key, value in cfg.get("options", {}).items():
        reader = reader.option(str(key), str(value))

    try:
        df: DataFrame = reader.load(path)
    except IngressError:
        raise
    except Exception as exc:
        raise IngressError(
            f"[{module.id}] source not found or unreadable at {path!r}: {exc}"
        ) from exc

    # schema_hint check — uses df.schema (metadata only, not an action)
    schema_hint: list[dict[str, str]] | None = cfg.get("schema_hint")
    if schema_hint:
        _validate_schema_hint(module.id, df, schema_hint)

    return df


def _validate_schema_hint(
    module_id: str,
    df: DataFrame,
    schema_hint: list[dict[str, str]],
) -> None:
    """Assert df.schema satisfies all hinted fields.

    Checks presence and, when a type is given, exact simpleString() match.
    Uses df.schema — metadata property, zero Spark actions.

    Raises:
        IngressError: On missing field or type mismatch.
    """
    actual: dict[str, str] = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    for hint in schema_hint:
        name: str | None = hint.get("name")
        if not name:
            continue

        if name not in actual:
            raise IngressError(
                f"[{module_id}] schema_hint field {name!r} not found in source schema. "
                f"Available columns: {sorted(actual)}"
            )

        expected_type: str | None = hint.get("type")
        if expected_type and actual[name] != expected_type:
            raise IngressError(
                f"[{module_id}] schema_hint type mismatch on {name!r}: "
                f"expected {expected_type!r}, actual {actual[name]!r}"
            )
