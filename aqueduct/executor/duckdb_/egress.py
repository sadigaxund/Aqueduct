"""Egress writer — persists a DuckDB relation via ``COPY ... TO``.

Stage A scope: ``format: parquet`` / ``format: csv`` only (the two ungated
formats, same as the Ingress reader — see that module's docstring). Write
modes are limited to what ``COPY ... TO`` can express natively: DuckDB has no
append semantics in a single ``COPY`` statement, so ``overwrite`` / ``error``
/ ``errorifexists`` / ``ignore`` are implemented and ``append`` / ``merge`` /
``overwrite_partitions`` are honestly UNSUPPORTED (see ``capabilities.yml``).

The ``COPY`` execution is the sanctioned DuckDB action in this layer —
mirrors Spark egress's ``.save()`` being the one sanctioned action there.

The special pseudo-format ``depot`` writes a key-value pair to the Depot
store instead of data — a plain ``depot.put(key, value)`` Python call, never
routed through DuckDB's own SQL/relation layer at all (mirrors
``aqueduct/executor/spark/egress.py``'s ``_write_depot`` exactly, including
``value_expr``'s single opt-in aggregate action).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError
from aqueduct.models import Module

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS: frozenset[str] = frozenset({"parquet", "csv"})
SUPPORTED_MODES: frozenset[str] = frozenset(
    {"overwrite", "error", "errorifexists", "ignore", "append"}
)


class EgressError(AqueductError):
    """Raised when an Egress module fails to write."""


def write_egress(
    rel: duckdb.DuckDBPyRelation,
    module: Module,
    con: duckdb.DuckDBPyConnection,
    depot: Any = None,
    base_dir: str | None = None,
) -> None:
    """Write rel to the target described by module.config.

    Args:
        rel:    Relation produced by upstream module(s).
        module: An Egress Module from the compiled Manifest.
        con:    Active DuckDB connection (caller owns lifecycle).
        depot:  Optional DepotStore instance for ``format: depot`` writes.
        base_dir: Accepted for signature parity; unused (format: custom is
                  UNSUPPORTED — Spark-only Python DataSource API).

    Raises:
        EgressError: Config invalid, format/mode not implemented this stage,
                     or write fails.
    """
    cfg = module.config
    fmt: str | None = cfg.get("format")
    if not fmt:
        raise EgressError(f"[{module.id}] 'format' is required in Egress config")

    # ── Depot pseudo-format ────────────────────────────────────────────────
    if fmt == "depot":
        _write_depot(rel, module, depot)
        return

    if fmt not in SUPPORTED_FORMATS:
        raise EgressError(
            f"[{module.id}] format={fmt!r} is not implemented for the DuckDB engine in "
            f"Stage A. Supported: {sorted(SUPPORTED_FORMATS)}. See docs/compatibility.md."
        )

    path: str | None = cfg.get("path")
    if not path:
        raise EgressError(f"[{module.id}] 'path' is required in Egress config for format={fmt!r}")

    mode: str = cfg.get("mode", "error")
    if mode not in SUPPORTED_MODES:
        raise EgressError(
            f"[{module.id}] mode={mode!r} is not implemented for the DuckDB engine in "
            f"Stage A. Supported: {sorted(SUPPORTED_MODES)}. See docs/compatibility.md."
        )

    target = Path(path)
    exists = target.exists()

    if mode == "error" and exists:
        raise EgressError(f"[{module.id}] write target {path!r} already exists (mode=error)")
    if mode == "errorifexists" and exists:
        raise EgressError(f"[{module.id}] write target {path!r} already exists (mode=errorifexists)")
    if mode == "ignore" and exists:
        logger.info("[%s] write target %r already exists (mode=ignore); skipping write.", module.id, path)
        return

    target.parent.mkdir(parents=True, exist_ok=True)

    partition_by: list[str] | None = cfg.get("partition_by")

    # ── mode=append — NON-ATOMIC read-existing + UNION + rewrite ──────────────
    # DuckDB's `COPY ... TO` has no native append: it overwrites the file. So
    # append reads the existing file, stacks the new rows on it (UNION ALL BY
    # NAME, column-name aligned), and rewrites the whole file. This is NOT
    # atomic — a crash mid-rewrite can lose the prior data, and two concurrent
    # appenders race. Acceptable for the single-process local batch use append
    # serves in Stage A; documented as non-atomic in the capability hint.
    input_name = "__egress_input__"
    combined_name: str | None = None
    con.register(input_name, rel)
    try:
        write_rel_name = input_name
        if mode == "append" and exists:
            reader = "read_parquet" if fmt == "parquet" else "read_csv"
            combined_name = "__egress_append__"
            try:
                # Materialize the union of existing + new into a temp table BEFORE
                # the COPY truncates the file we are reading from.
                con.execute(
                    f'CREATE TEMP TABLE "{combined_name}" AS '
                    f"SELECT * FROM {reader}('{_escape(path)}') "
                    f"UNION ALL BY NAME SELECT * FROM {input_name}"
                )
                write_rel_name = f'"{combined_name}"'
            except Exception as exc:
                raise EgressError(
                    f"[{module.id}] append: could not read existing target {path!r}: {exc}"
                ) from exc

        options = _copy_options(fmt, cfg, partition_by)
        copy_sql = f"COPY {write_rel_name} TO '{_escape(path)}' ({options})"
        try:
            con.sql(copy_sql)
        except Exception as exc:
            raise EgressError(f"[{module.id}] write failed to {path!r}: {exc}") from exc
    finally:
        if combined_name is not None:
            try:
                con.execute(f'DROP TABLE IF EXISTS "{combined_name}"')
            except Exception:
                pass
        try:
            con.unregister(input_name)
        except Exception:
            pass


def _escape(path: str) -> str:
    return path.replace("'", "''")


def _copy_options(fmt: str, cfg: dict, partition_by: list[str] | None) -> str:
    # `options:` is a freeform passthrough (Blueprint parity with Spark's
    # writer options) — an author can legally set `options: {header: ...}`
    # for csv instead of (or in addition to) the top-level `header:` key.
    # DuckDB's COPY rejects a duplicate option name, so the below must never
    # emit HEADER twice: user-supplied `options` wins over the top-level
    # `header:` default, checked case-insensitively (gallery snippet
    # 11_spillway_channel hit this — `header:` defaults to true AND
    # `options: {header: "true"}` was also set).
    options_cfg = cfg.get("options", {})
    options_keys_lower = {str(k).lower() for k in options_cfg}

    parts = [f"FORMAT {fmt.upper()}"]
    if fmt == "csv" and "header" not in options_keys_lower:
        header = cfg.get("header", True)
        parts.append(f"HEADER {str(bool(header)).lower()}")
    if partition_by:
        cols = ", ".join(partition_by)
        parts.append(f"PARTITION_BY ({cols})")
        parts.append("OVERWRITE_OR_IGNORE true")
    for key, value in options_cfg.items():
        parts.append(f"{str(key).upper()} '{_escape(str(value))}'")
    return ", ".join(parts)


def _write_depot(rel: duckdb.DuckDBPyRelation, module: Module, depot: Any) -> None:
    """Write a KV entry to the Depot store. ``depot`` must not be None.

    Mirrors ``aqueduct/executor/spark/egress.py::_write_depot`` exactly:
    ``depot.put(key, value)`` is a plain Python call, engine-independent,
    never routed through this engine's own SQL/relation layer. The only
    DuckDB-specific piece is ``value_expr``'s single aggregate query — this
    engine's equivalent of Spark's single ``.collect()`` opt-in action.
    """
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
        # Opt-in DuckDB action: single aggregate query over the relation.
        try:
            agg_result = rel.aggregate(value_expr).fetchone()[0]
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


__all__ = ["EgressError", "write_egress", "SUPPORTED_FORMATS", "SUPPORTED_MODES"]
