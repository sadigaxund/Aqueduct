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

``table:`` (Pass G2 — ``feature.table_addressing``) reads an existing
catalog table/view by name via ``con.table()`` instead of ``format:``+
``path:`` — see ``_read_table``'s docstring for the catalog defaulting
rule. Mutually exclusive with ``path:``, mirroring
``executor/spark/ingress.py``'s own ``table:``/``path:`` contract.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError, EnginePluginError
from aqueduct.models import Module

logger = logging.getLogger(__name__)

# Formats with a dedicated DuckDB reader this stage. Kept in sync by hand with
# the ingress.format.* capability leaves this engine marks `supported`
# (csv) plus the two ungated formats (parquet, json) every engine accepts.
_SUPPORTED_FORMATS: frozenset[str] = frozenset({"parquet", "csv", "json"})


class IngressError(AqueductError):
    """Raised when an Ingress module fails to read."""


def read_ingress(
    module: Module, con: duckdb.DuckDBPyConnection, base_dir: str | None = None
) -> duckdb.DuckDBPyRelation:
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
    table: str | None = cfg.get("table")

    if table and path:
        raise IngressError(
            f"[{module.id}] 'table' and 'path' are mutually exclusive. Set one or the other, not both."
        )

    if table:
        # Catalog-addressed read (Pass G2) — no format:/path: required. See
        # `_read_table`'s docstring for the catalog defaulting rule.
        rel = _read_table(module.id, con, table)
    else:
        if not fmt:
            raise IngressError(f"[{module.id}] 'format' is required in Ingress config")
        if fmt not in _SUPPORTED_FORMATS:
            raise IngressError(
                f"[{module.id}] format={fmt!r} is not implemented for the DuckDB engine "
                f"in Stage A. Supported: {sorted(_SUPPORTED_FORMATS)}. "
                "See docs/compatibility.md for the full capability matrix."
            )
        if not path:
            raise IngressError(
                f"[{module.id}] 'path' is required in Ingress config for format={fmt!r}"
            )

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
            raise IngressError(
                f"[{module.id}] source not found or unreadable at {path!r}: {exc}"
            ) from exc

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


# ── Catalog table addressing (Pass G2 — feature.table_addressing) ──────────
#
# DuckDB genuinely has a catalog — `memory` (the connection's own database,
# home for every plain `CREATE TABLE`/`CREATE VIEW`), `system` (built-ins),
# plus whatever `ATTACH` has added — so a `catalog.schema.table` three-part
# namespace is real, not fictional (measured against a live
# `DuckDBPyConnection`: `SELECT DISTINCT catalog_name FROM
# duckdb_databases()` lists exactly those). What was previously missing was
# an IMPLEMENTATION mapping a Blueprint's `table:` string onto it — not the
# absence of a catalog to resolve against.
#
# Defaulting rule (mirrors DuckDB's own, un-managed, name resolution — this
# module performs NO catalog bookkeeping of its own):
#   - Unqualified name (`orders`)              -> resolves against the
#     connection's CURRENT catalog+schema (`memory.main` for a plain
#     `:memory:`/file connection, unless a prior step in the SAME session
#     changed it with `USE`).
#   - Two-part name (`schema.table`)           -> resolves `schema` within
#     whichever catalog is CURRENT.
#   - Three-part name (`catalog.schema.table`) -> addresses a specific
#     catalog directly. That catalog must already exist — Aqueduct never
#     performs an implicit `ATTACH`; a Blueprint that needs one ATTACHed
#     arranges it itself (`engine.duckdb.attach`/session setup), the exact
#     same division of responsibility Spark's `table:` has with
#     `engine.spark.conf`'s `spark.sql.catalog.*` keys.
# An unresolvable name (missing catalog/schema/table) is `con.table()`'s own
# `CatalogException` — wrapped into `IngressError` (never left to propagate
# as a raw duckdb exception, and never silently returning an empty/missing
# result).
def _read_table(
    module_id: str, con: duckdb.DuckDBPyConnection, table: str
) -> duckdb.DuckDBPyRelation:
    """Read an existing catalog entity (table or view) by name — no query
    executes yet (``con.table()`` returns a lazy relation, same laziness
    guarantee as ``read_parquet``/``read_csv``)."""
    try:
        return con.table(table)
    except Exception as exc:
        raise IngressError(f"[{module_id}] table {table!r} not found or unreadable: {exc}") from exc


def _csv_kwargs(options: dict) -> dict:
    """Translate the generic Blueprint ``options:`` map to duckdb.read_csv kwargs.

    Only the option names duckdb's ``read_csv`` actually accepts pass through;
    unrecognised keys are dropped with a debug log rather than raised — Stage A
    CSV options are intentionally minimal (header + a small pass-through set),
    not the full Spark CSV reader option surface.
    """
    allowed = {
        "sep",
        "delimiter",
        "quotechar",
        "escapechar",
        "encoding",
        "compression",
        "dtype",
        "columns",
    }
    out = {}
    for k, v in options.items():
        key = str(k)
        if key in allowed:
            out[key] = v
        elif key == "infer_schema":
            continue  # duckdb infers by default; no direct off-switch needed for Stage A
        else:
            logger.debug(
                "Ingress CSV option %r not recognised by DuckDB Stage A reader; ignored.", key
            )
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
            module.id,
            new_cols,
        )


# DuckDB relation column types (rel.types) are duckdb.typing.DuckDBPyType
# objects; str() gives the canonical DuckDB type name (e.g. "BIGINT",
# "VARCHAR", "INTEGER[]", "STRUCT(x INTEGER)"). schema_hint field types are
# compared through the Arrow type hub (Phase 80 work package 3;
# widening added Pass G2) via ``duckdb_/type_render.py``'s
# ``schema_type_matches`` — a schema_hint written against the hub vocabulary
# (e.g. "array<int>", "timestamp_ntz") — or against Spark's simpleString()
# vocabulary via the hub's own familiar aliases ("string", "long") — validates
# on DuckDB without the author needing two Blueprints, AND a fixed-width
# numeric hint (e.g. "integer") is satisfied by DuckDB's own wider inferred
# type (DuckDB's CSV sniffer only ever infers BIGINT/DOUBLE, never a narrower
# width) — see ``schema_type_matches``'s docstring for the full reasoning.
# The actual side's ``str(dtype)`` is never independently re-parsed through
# the hub (only ``schema_type_matches``'s internal, narrow reverse table is
# used for widening) — parsing a genuinely-native ``"TIMESTAMP"`` through
# ``parse_type`` would hit the hub's bare-``timestamp`` AMBIGUITY resolution,
# silently reinterpreting a real, concrete DuckDB column type instead of
# comparing it as what it already, unambiguously, is.
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
    from aqueduct.executor.duckdb_.type_render import schema_type_matches

    actual: dict[str, str] = dict(
        zip(rel.columns, (str(dtype) for dtype in rel.types), strict=True)
    )

    if mode not in ("strict", "additive", "subset"):
        raise IngressError(
            f"[{module_id}] Unknown schema_hint mode: {mode!r}. Use strict, additive, or subset."
        )

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
        if not expected_type:
            continue
        try:
            matched = schema_type_matches(str(expected_type), actual[name])
        except EnginePluginError as exc:
            raise IngressError(
                f"[{module_id}] schema_hint type {str(expected_type)!r}: {exc}"
            ) from exc
        if not matched:
            raise IngressError(
                f"[{module_id}] schema_hint type mismatch on {name!r}: "
                f"expected {expected_type!r}, actual {actual[name].lower()!r}"
            )


__all__ = ["IngressError", "read_ingress"]
