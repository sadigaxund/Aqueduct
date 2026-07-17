"""Ingress reader — loads source data as a lazy DuckDB relation.

Stage A scope: ``format: csv`` plus the two ungated formats every engine
accepts without a capability leaf, ``parquet`` and ``json`` (see
``aqueduct/executor/capability_leaves.py`` — only formats with a DEDICATED
engine code path are curated leaves; parquet/json have none because they need
no special-casing). Any other ``format`` value is rejected here with a clear
message — it either has no DuckDB reader (jdbc, kafka, custom, delta, depot,
all UNSUPPORTED leaves; the compiler already refuses these at compile time)
or is simply not one of the three Stage A formats.

DuckDB's ``read_parquet`` / ``read_csv`` / ``read_json`` return a
``DuckDBPyRelation`` — LAZY, same as a Spark DataFrame: no query executes
until a downstream consumer (an Egress ``COPY``, or ``.fetchall()``/``.df()``)
materializes it. No DuckDB action is triggered here.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError
from aqueduct.models import Module

logger = logging.getLogger(__name__)

# Formats with a dedicated DuckDB reader this stage. Kept in sync by hand with
# the ingress.format.* capability leaves this engine marks `supported`
# (csv) plus the two ungated formats (parquet, json) every engine accepts.
_SUPPORTED_FORMATS: frozenset[str] = frozenset({"parquet", "csv", "json"})


class IngressError(AqueductError):
    """Raised when an Ingress module fails to read."""


def read_ingress(module: Module, con: duckdb.DuckDBPyConnection, base_dir: str | None = None) -> duckdb.DuckDBPyRelation:
    """Read source data described by module.config into a lazy relation.

    Args:
        module: An Ingress Module from the compiled Manifest.
        con:    Active DuckDB connection (caller owns lifecycle).
        base_dir: Manifest.base_dir — accepted for signature parity with the
                  Spark reader; unused this stage (no custom-source resolution).

    Returns:
        Lazy ``DuckDBPyRelation`` — no query executes yet.

    Raises:
        IngressError: Config invalid, unsupported format, or read failure.
    """
    cfg = module.config
    fmt: str | None = cfg.get("format")
    path: str | None = cfg.get("path")

    if not fmt:
        raise IngressError(f"[{module.id}] 'format' is required in Ingress config")
    if fmt not in _SUPPORTED_FORMATS:
        raise IngressError(
            f"[{module.id}] format={fmt!r} is not implemented for the DuckDB engine "
            f"in Stage A. Supported: {sorted(_SUPPORTED_FORMATS)}. "
            "See docs/compatibility.md for the full capability matrix."
        )
    if not path:
        raise IngressError(f"[{module.id}] 'path' is required in Ingress config for format={fmt!r}")

    try:
        if fmt == "parquet":
            rel = con.read_parquet(path)
        elif fmt == "csv":
            header = bool(cfg.get("header", True))
            options = dict(cfg.get("options", {}))
            rel = con.read_csv(path, header=header, **_csv_kwargs(options))
        else:  # json
            rel = con.read_json(path)
    except IngressError:
        raise
    except Exception as exc:
        raise IngressError(f"[{module.id}] source not found or unreadable at {path!r}: {exc}") from exc

    partition_filters: str | None = cfg.get("partition_filters")
    if partition_filters:
        try:
            rel = rel.filter(partition_filters)
        except Exception as exc:
            raise IngressError(
                f"[{module.id}] partition_filters {partition_filters!r} is invalid: {exc}"
            ) from exc

    sandbox_limit = cfg.get("sandbox_limit")
    if isinstance(sandbox_limit, int) and sandbox_limit > 0:
        rel = rel.limit(int(sandbox_limit))

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
        _validate_schema_hint(module.id, rel, schema_hint, mode=schema_hint_mode)

    if cfg.get("on_new_columns"):
        _enforce_on_new_columns(module, rel, schema_hint)

    return rel


def _csv_kwargs(options: dict) -> dict:
    """Translate the generic Blueprint ``options:`` map to duckdb.read_csv kwargs.

    Only the option names duckdb's ``read_csv`` actually accepts pass through;
    unrecognised keys are dropped with a debug log rather than raised — Stage A
    CSV options are intentionally minimal (header + a small pass-through set),
    not the full Spark CSV reader option surface.
    """
    allowed = {"sep", "delimiter", "quotechar", "escapechar", "encoding", "compression", "dtype", "columns"}
    out = {}
    for k, v in options.items():
        key = str(k)
        if key in allowed:
            out[key] = v
        elif key == "infer_schema":
            continue  # duckdb infers by default; no direct off-switch needed for Stage A
        else:
            logger.debug("Ingress CSV option %r not recognised by DuckDB Stage A reader; ignored.", key)
    return out


ON_NEW_COLUMNS_POLICIES: frozenset[str] = frozenset({"allow", "fail", "alert"})


def _enforce_on_new_columns(
    module: Module,
    rel: duckdb.DuckDBPyRelation,
    schema_hint: list[dict[str, str]] | None,
) -> None:
    """Apply the Ingress ``on_new_columns`` contract against a declared baseline.

    Same semantics as the Spark reader's version — see that docstring.
    ``rel.columns`` is metadata (no query execution).
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
            "[runtime_ingress_new_columns_no_baseline] [%s] on_new_columns set "
            "but no 'known_columns' or 'schema_hint' to compare against; skipping.",
            module.id,
        )
        return

    new_cols = [c for c in rel.columns if c not in baseline]
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
            "[runtime_ingress_new_columns] [%s] on_new_columns=alert: source "
            "added undeclared column(s) %s.",
            module.id, new_cols,
        )


# DuckDB relation column types (rel.types) are duckdb.typing.DuckDBPyType
# objects; str() gives the canonical DuckDB type name (e.g. "BIGINT",
# "VARCHAR"). Common Spark-vocabulary aliases are normalised so a schema_hint
# written for Spark's simpleString() vocabulary (e.g. "string", "long") also
# validates on DuckDB without the author needing two Blueprints.
_TYPE_ALIASES: dict[str, str] = {
    "string": "varchar",
    "long": "bigint",
    "integer": "integer",
    "int": "integer",
    "bool": "boolean",
    "short": "smallint",
    "byte": "tinyint",
    "double": "double",
    "float": "float",
}


def _normalize_type(t: str) -> str:
    lower = t.lower()
    return _TYPE_ALIASES.get(lower, lower)


def _validate_schema_hint(
    module_id: str,
    rel: duckdb.DuckDBPyRelation,
    schema_hint: list[dict[str, str]],
    mode: str = "strict",
) -> None:
    """Assert rel's schema satisfies all hinted fields — metadata only, no execution.

    Modes mirror the Spark reader: strict (all hinted fields must exist and
    match), additive (same, extra live columns allowed), subset (missing
    hinted fields are OK; only present ones are type-checked).
    """
    actual: dict[str, str] = {
        name: _normalize_type(str(dtype)) for name, dtype in zip(rel.columns, rel.types, strict=True)
    }

    if mode not in ("strict", "additive", "subset"):
        raise IngressError(f"[{module_id}] Unknown schema_hint mode: {mode!r}. Use strict, additive, or subset.")

    for hint in schema_hint:
        name = hint.get("name")
        if not name:
            continue
        if name not in actual:
            if mode == "subset":
                continue
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


__all__ = ["IngressError", "read_ingress"]
