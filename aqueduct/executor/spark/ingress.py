"""Ingress reader — loads source data as a lazy Spark DataFrame.

Any format string supported by the active SparkSession works (parquet, csv,
json, jdbc, kafka, avro, orc, delta, …).  Format-specific defaults are applied
for known formats; all others are passed verbatim to Spark's DataFrameReader.

No Spark actions are triggered here.  The returned DataFrame is lazy;
the execution plan is only materialised when downstream Egress calls .save().
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.models import Module
from aqueduct.executor.path_keys import PATHLESS_INGRESS_FORMATS
from aqueduct.errors import AqueductError

logger = logging.getLogger(__name__)


class IngressError(AqueductError):
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

    # Table-addressable read via catalog identifier (catalog.schema.table).
    # When ``table:`` is set, read via ``spark.read.table(table)`` — no ``path``
    # and no ``format`` required. The catalog is wired through ``spark_config``
    # (e.g. ``spark.sql.catalog.*``), entirely external to Aqueduct.
    table: str | None = cfg.get("table")
    path: str | None = cfg.get("path")
    fmt: str | None = cfg.get("format")

    if table and path:
        raise IngressError(
            f"[{module.id}] 'table' and 'path' are mutually exclusive. "
            f"Set one or the other, not both."
        )

    if table:
        # Spark's ``spark.read.table()`` returns a lazy DataFrame — no action.
        try:
            df: DataFrame = spark.read.table(table)
        except Exception as exc:
            raise IngressError(
                f"[{module.id}] table {table!r} not found or unreadable: {exc}"
            ) from exc

        # partition_filters still applies on table reads
        partition_filters: str | None = cfg.get("partition_filters")
        if partition_filters:
            try:
                df = df.where(partition_filters)
            except Exception as exc:
                raise IngressError(
                    f"[{module.id}] partition_filters {partition_filters!r} is invalid: {exc}"
                ) from exc

        # schema_hint check — uses df.schema (metadata only, not an action)
        schema_hint_raw = cfg.get("schema_hint")
        schema_hint_mode = "strict"
        schema_hint: list[dict[str, str]] | None = None
        if isinstance(schema_hint_raw, dict):
            if "columns" in schema_hint_raw:
                schema_hint_mode = schema_hint_raw.get("mode", "strict")
                schema_hint = schema_hint_raw.get("columns")
            else:
                schema_hint = [{"name": k, "type": str(v)} for k, v in schema_hint_raw.items()]
        elif isinstance(schema_hint_raw, list):
            schema_hint = schema_hint_raw
        if schema_hint:
            _validate_schema_hint(module.id, df, schema_hint, mode=schema_hint_mode)

        # on_new_columns
        if cfg.get("on_new_columns"):
            _enforce_on_new_columns(module, df, schema_hint)

        return df

    if not fmt:
        raise IngressError(f"[{module.id}] 'format' is required in Ingress config")

    # Custom Python DataSource (Spark 4.0+): import + register the user class,
    # then read by its own name(). Path is optional (custom sources may locate
    # data via options instead).
    is_custom = fmt == "custom"
    if is_custom:
        class_path = cfg.get("class")
        if not class_path:
            raise IngressError(f"[{module.id}] format=custom requires 'class'")
        from aqueduct.executor.spark.custom_source import register_custom_source

        try:
            fmt = register_custom_source(spark, str(class_path))
        except Exception as exc:
            raise IngressError(
                f"[{module.id}] custom DataSource {class_path!r}: {exc}"
            ) from exc

    # Formats that locate data via options (url/dbtable/topic/etc.), not a file path
    if not path and fmt not in PATHLESS_INGRESS_FORMATS and not is_custom:
        raise IngressError(f"[{module.id}] 'path' is required in Ingress config for format={fmt!r}")

    reader = spark.read.format(fmt)

    # Format-specific defaults (user options override below)
    if fmt == "csv":
        reader = reader.option("header", str(cfg.get("header", True)).lower())
        reader = reader.option("inferSchema", str(cfg.get("infer_schema", True)).lower())

    # Time-travel reads (Delta / Iceberg): pin a historical snapshot by version
    # or timestamp. Metadata-only — still no Spark action.
    reader = _apply_time_travel(module, reader)

    for key, value in cfg.get("options", {}).items():
        reader = reader.option(str(key), str(value))

    try:
        df: DataFrame = reader.load(path) if path else reader.load()
    except IngressError:
        raise
    except Exception as exc:
        loc = f"at {path!r}" if path else f"(format={fmt!r})"
        raise IngressError(
            f"[{module.id}] source not found or unreadable {loc}: {exc}"
        ) from exc

    # partition_filters — lazy .where() for manual predicate pushdown
    partition_filters: str | None = cfg.get("partition_filters")
    if partition_filters:
        try:
            df = df.where(partition_filters)
        except Exception as exc:
            raise IngressError(
                f"[{module.id}] partition_filters {partition_filters!r} is invalid: {exc}"
            ) from exc

    # sandbox_limit — Phase 29a Gate 3 hook. When the patch-preview sandbox
    # injects this marker into a copy of the manifest, the Ingress wraps its
    # output in `.limit(N)` so the downstream DAG sees at most N rows. Marker
    # is never set by user-authored Blueprints; it is consumed only by
    # `aqueduct.patch.preview.run_sandbox_gate`.
    sandbox_limit = cfg.get("sandbox_limit")
    if isinstance(sandbox_limit, int) and sandbox_limit > 0:
        df = df.limit(int(sandbox_limit))

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

    # on_new_columns — flag/forbid undeclared source columns (prevention side of
    # the drift story; complements `aqueduct drift`). Compares the live source
    # against a declared baseline (`known_columns` or `schema_hint` names).
    if cfg.get("on_new_columns"):
        _enforce_on_new_columns(module, df, schema_hint)

    return df


ON_NEW_COLUMNS_POLICIES: frozenset[str] = frozenset({"allow", "fail", "alert"})


def _enforce_on_new_columns(
    module: Module,
    df: DataFrame,
    schema_hint: "list[dict[str, str]] | None",
) -> None:
    """Apply the Ingress ``on_new_columns`` contract against a declared baseline.

    Baseline columns come from ``known_columns`` (explicit) or, failing that, the
    ``schema_hint`` column names. With neither, there is nothing to diff against —
    log once and skip (the policy needs a declared expectation to be meaningful).

      * ``fail``  — raise if the source has columns outside the baseline.
      * ``alert`` — log a warning naming them, then proceed.
      * ``allow`` — no-op (the default engine behaviour, made explicit).
    """
    policy = str(module.config["on_new_columns"])
    if policy not in ON_NEW_COLUMNS_POLICIES:
        raise IngressError(
            f"[{module.id}] on_new_columns={policy!r} is invalid; "
            f"use one of {sorted(ON_NEW_COLUMNS_POLICIES)}"
        )

    known = module.config.get("known_columns")
    if known:
        baseline = {str(c) for c in known}
    elif schema_hint:
        baseline = {h["name"] for h in schema_hint if h.get("name")}
    else:
        logger.warning(
            "[%s] on_new_columns set but no 'known_columns' or 'schema_hint' to "
            "compare against; skipping.", module.id,
        )
        return

    new_cols = [c for c in df.columns if c not in baseline]
    if not new_cols:
        return

    if policy == "fail":
        raise IngressError(
            f"[{module.id}] on_new_columns=fail: source has undeclared column(s) "
            f"{new_cols} not in the declared baseline {sorted(baseline)}. "
            "Add them to known_columns/schema_hint, or set on_new_columns: alert."
        )
    if policy == "alert":
        logger.warning(
            "[%s] on_new_columns=alert: source added undeclared column(s) %s.",
            module.id, new_cols,
        )


def _apply_time_travel(module: Module, reader):
    """Apply a ``time_travel:`` snapshot pin to a DataFrameReader.

    Config (mutually exclusive)::

        time_travel:
          version: 12                    # Delta versionAsOf / Iceberg snapshot
        # or
        time_travel:
          timestamp: "2026-01-01 00:00"  # Delta/Iceberg timestampAsOf

    Returns the reader unchanged when no ``time_travel`` block is present.
    """
    tt = module.config.get("time_travel")
    if tt is None:
        return reader
    if not isinstance(tt, dict):
        raise IngressError(
            f"[{module.id}] time_travel must be a mapping with 'version' or 'timestamp'"
        )
    has_version = tt.get("version") is not None
    has_timestamp = tt.get("timestamp") is not None
    if has_version and has_timestamp:
        raise IngressError(
            f"[{module.id}] time_travel accepts 'version' OR 'timestamp', not both"
        )
    if has_version:
        return reader.option("versionAsOf", int(tt["version"]))
    if has_timestamp:
        return reader.option("timestampAsOf", str(tt["timestamp"]))
    raise IngressError(
        f"[{module.id}] time_travel requires 'version' or 'timestamp'"
    )


def read_source_schema(module: Module, spark: SparkSession) -> dict[str, str]:
    """Return the live source schema as ``{column: spark_simple_type}``.

    Metadata-only: builds the lazy reader (``read_ingress``) and reads
    ``df.schema`` — zero Spark actions. Used by ``aqueduct drift`` to compare a
    source's current schema against its stored baseline without scanning data.
    Skips any sandbox/schema_hint side effects by reading a fresh reader.
    """
    df = read_ingress(module, spark)
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


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
