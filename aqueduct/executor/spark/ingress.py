"""Ingress reader — loads source data as a lazy Spark DataFrame.

Any format string supported by the active SparkSession works (parquet, csv,
json, jdbc, kafka, avro, orc, delta, …).  Format-specific defaults are applied
for known formats; all others are passed verbatim to Spark's DataFrameReader.

No Spark actions are triggered here.  The returned DataFrame is lazy;
the execution plan is only materialised when downstream Egress calls .save().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.parser.models import Module


class IngressError(Exception):
    """Raised when an Ingress module fails to read."""


# Maps user-friendly type aliases to Spark simpleString() equivalents.
_TYPE_ALIASES: dict[str, str] = {
    "long": "bigint",
    "integer": "int",
    "bool": "boolean",
    "short": "smallint",
    "byte": "tinyint",
}


def _normalize_type(t: str) -> str:
    lower = t.lower()
    return _TYPE_ALIASES.get(lower, lower)


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
    if not fmt:
        raise IngressError(f"[{module.id}] 'format' is required in Ingress config")

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
    schema_hint_raw = cfg.get("schema_hint")
    schema_hint_mode = "strict"  # default
    schema_hint: list[dict[str, str]] | None = None
    if isinstance(schema_hint_raw, dict):
        if "columns" in schema_hint_raw:
            # nested: {mode: strict, columns: [{name, type}, ...]}
            schema_hint_mode = schema_hint_raw.get("mode", "strict")
            schema_hint = schema_hint_raw.get("columns")
        else:
            # flat: {col_name: type, ...}
            schema_hint = [{"name": k, "type": str(v)} for k, v in schema_hint_raw.items()]
    elif isinstance(schema_hint_raw, list):
        schema_hint = schema_hint_raw
    if schema_hint:
        _validate_schema_hint(module.id, df, schema_hint, mode=schema_hint_mode)

    return df


def _validate_schema_hint(
    module_id: str,
    df: DataFrame,
    schema_hint: list[dict[str, str]],
    mode: str = "strict",
) -> None:
    """Assert df.schema satisfies all hinted fields.

    Checks presence and, when a type is given, exact simpleString() match.
    Uses df.schema — metadata property, zero Spark actions.

    Raises:
        IngressError: On missing field or type mismatch.
    """
    actual: dict[str, str] = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    if mode == "strict":
        # All hinted columns must exist with matching types
        for hint in schema_hint:
            name = hint.get("name")
            if not name:
                continue
            if name not in actual:
                raise IngressError(
                    f"[{module_id}] schema_hint field {name!r} not found in source schema. "
                    f"Available columns: {sorted(actual)}"
                )
            expected_type = hint.get("type")
            if expected_type and actual[name] != _normalize_type(expected_type):
                raise IngressError(
                    f"[{module_id}] schema_hint type mismatch on {name!r}: "
                    f"expected {expected_type!r}, actual {actual[name]!r}"
                )
    elif mode == "additive":
        # Extra upstream columns are allowed; only check hinted columns
        for hint in schema_hint:
            name = hint.get("name")
            if not name:
                continue
            if name not in actual:
                raise IngressError(
                    f"[{module_id}] schema_hint field {name!r} not found in source schema. "
                    f"Available columns: {sorted(actual)}"
                )
            expected_type = hint.get("type")
            if expected_type and actual[name] != _normalize_type(expected_type):
                raise IngressError(
                    f"[{module_id}] schema_hint type mismatch on {name!r}: "
                    f"expected {expected_type!r}, actual {actual[name]!r}"
                )
    elif mode == "subset":
        # Missing optional columns are allowed; only validate present ones
        for hint in schema_hint:
            name = hint.get("name")
            if not name or name not in actual:
                continue  # missing is OK in subset mode
            expected_type = hint.get("type")
            if expected_type and actual[name] != _normalize_type(expected_type):
                raise IngressError(
                    f"[{module_id}] schema_hint type mismatch on {name!r}: "
                    f"expected {expected_type!r}, actual {actual[name]!r}"
                )
    else:
        raise IngressError(f"[{module_id}] Unknown schema_hint mode: {mode!r}. Use strict, additive, or subset.")
