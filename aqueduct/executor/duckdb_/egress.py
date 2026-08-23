"""Egress writer — persists a DuckDB relation via ``COPY ... TO``.

Stage A scope: ``format: parquet`` / ``format: csv`` / ``format: json`` (the
three formats DuckDB's ``COPY ... TO`` writes natively — see the Ingress
reader's module docstring for the read-side symmetry). Write modes are
limited to what ``COPY ... TO`` can express natively plus one emulated mode:
DuckDB has no append semantics in a single ``COPY`` statement, so
``overwrite`` / ``error`` / ``errorifexists`` / ``ignore`` are implemented
directly; ``append`` is implemented as a non-atomic read-existing + ``UNION
ALL BY NAME`` + rewrite (see the append branch below and its
``capabilities.yml`` hint) — the reader used to load the existing file for
that union is chosen per-format (``read_parquet``/``read_csv``/``read_json``),
never assumed; ``merge`` / ``overwrite_partitions`` are honestly UNSUPPORTED
(see ``capabilities.yml``).

The ``COPY`` execution is the sanctioned DuckDB action in this layer —
mirrors Spark egress's ``.save()`` being the one sanctioned action there.

The special pseudo-format ``depot`` writes a key-value pair to the Depot
store instead of data — a plain ``depot.put(key, value)`` Python call, never
routed through DuckDB's own SQL/relation layer at all (mirrors
``aqueduct/executor/spark/egress.py``'s ``_write_depot`` exactly, including
``value_expr``'s single opt-in aggregate action).

``on_new_columns`` (Pass F — previously ruled UNSUPPORTED with no examined
reasoning behind it; see ``_enforce_on_new_columns``) is a genuinely small
implementation on this engine: reading an existing target's column set is a
zero-scan metadata read (``LIMIT 0`` on the same reader Ingress uses), so the
schema-drift write contract (fail/alert/allow) needs no Delta/Iceberg
transaction log the way Spark's version's ``mergeSchema`` framing suggests —
DuckDB's ``COPY ... TO`` always writes a fresh file with whatever columns
the incoming relation has, so "allow"/"alert" have nothing further to DO
beyond deciding whether to warn.

``coalesce``/``repartition`` (Pass G1 — previously declared ``supported``
on both engines with zero readers anywhere, a dead knob the
``file_format_no_repartition``/``perf_delta_append_no_partition`` compiler
warnings wrongly promised would control Spark's output file count).
``coalesce`` is genuinely honoured here — see ``_copy_options`` — because a
non-partitioned ``COPY ... TO '<path>'`` already writes exactly one file by
default; setting the option explicitly pins that guarantee rather than
leaving it to an undocumented default. ``repartition`` stays honestly
``unsupported``: DuckDB has no shuffle/partition-count concept for a
``COPY`` target — ``PARTITION_BY`` groups by column VALUE, not by a target
file count, and ``PER_THREAD_OUTPUT`` cannot even combine with
``PARTITION_BY`` — so there is no lever this field could move.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError
from aqueduct.executor.duckdb_.ingress import ON_NEW_COLUMNS_POLICIES
from aqueduct.executor.models import _add_module_warning
from aqueduct.models import Module

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS: frozenset[str] = frozenset({"parquet", "csv", "json"})
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
) -> int | None:
    """Write rel to the target described by module.config.

    Args:
        rel:    Relation produced by upstream module(s).
        module: An Egress Module from the compiled Manifest.
        con:    Active DuckDB connection (caller owns lifecycle).
        depot:  Optional DepotStore instance for ``format: depot`` writes.
        base_dir: Accepted for signature parity; unused (format: custom is
                  UNSUPPORTED — Spark-only Python DataSource API).

    Returns:
        The number of rows actually written this call (``records_written``
        for ``module_metrics`` — Phase 85 D1), or ``None`` when that number
        genuinely isn't available: a ``table:``/path write skipped by
        ``mode=ignore`` (nothing was written), ``format: depot`` (not a row
        write), or ``mode=append`` onto an EXISTING path target (DuckDB has
        no native append — this engine rewrites existing+new combined via
        ``UNION ALL BY NAME``, so the ``COPY``'s own row count is the file's
        new TOTAL, not the rows added this run; recovering just the delta
        would need a second count over already-executed data, which this
        task's spec says not to add). Every other path returns a REAL
        count read off the write statement's own result — no extra scan:
        DuckDB's ``COPY``/``CREATE TABLE AS``/``INSERT INTO`` all return the
        row count as part of the write itself, including a genuine ``0``
        for an empty relation (Phase 85 D3 — a zero-row write is
        distinguishable from "not measured").

    Raises:
        EgressError: Config invalid, format/mode not implemented this stage,
                     or write fails.
    """
    cfg = module.config
    fmt: str | None = cfg.get("format")
    path: str | None = cfg.get("path")
    table: str | None = cfg.get("table")

    if table and path:
        raise EgressError(
            f"[{module.id}] 'table' and 'path' are mutually exclusive. Set one or the other, not both."
        )

    if table:
        # Catalog-addressed write (Pass G2) — no format:/path: required. See
        # `_write_table`'s docstring for the catalog defaulting rule (same
        # rule `duckdb_/ingress.py::_read_table` documents).
        mode = cfg.get("mode", "error")
        if mode not in SUPPORTED_MODES:
            raise EgressError(
                f"[{module.id}] mode={mode!r} is not implemented for the DuckDB engine in "
                f"Stage A. Supported: {sorted(SUPPORTED_MODES)}. See docs/compatibility.md."
            )
        _records_written = _write_table(rel, module, con, str(table), mode)
        register_as: str | None = cfg.get("register_as_table")
        if register_as:
            # Same "ignored, not silently dropped" treatment as Spark's
            # egress.py — writing directly to a catalog table via `table:`
            # already IS the registration; `register_as_table` has nothing
            # further to do.
            logger.warning(
                "[runtime_egress_register_as_table_ignored] [%s] "
                "register_as_table=%r ignored — module already writes to a "
                "catalog table via 'table:'. Use 'table:' to write directly.",
                module.id,
                register_as,
            )
            _add_module_warning(
                "runtime_egress_register_as_table_ignored",
                f"register_as_table={register_as!r} ignored — module already writes to a "
                "catalog table via 'table:'. Use 'table:' to write directly.",
            )
        return _records_written

    if not fmt:
        raise EgressError(f"[{module.id}] 'format' is required in Egress config")

    # ── Depot pseudo-format ────────────────────────────────────────────────
    if fmt == "depot":
        _write_depot(rel, module, depot)
        return None

    if fmt not in SUPPORTED_FORMATS:
        raise EgressError(
            f"[{module.id}] format={fmt!r} is not implemented for the DuckDB engine in "
            f"Stage A. Supported: {sorted(SUPPORTED_FORMATS)}. See docs/compatibility.md."
        )

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
        raise EgressError(
            f"[{module.id}] write target {path!r} already exists (mode=errorifexists)"
        )
    if mode == "ignore" and exists:
        logger.info(
            "[%s] write target %r already exists (mode=ignore); skipping write.", module.id, path
        )
        return None

    if cfg.get("on_new_columns") and exists:
        _enforce_on_new_columns(module, rel, con, fmt, path, str(cfg["on_new_columns"]))

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
    is_append_union = mode == "append" and exists
    con.register(input_name, rel)
    try:
        write_rel_name = input_name
        if is_append_union:
            reader = _reader_function(fmt)
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
            # `con.execute` (not `con.sql`) — a COPY statement's result set
            # (one row, column "Count") only comes back through the cursor
            # API; `con.sql(...)` on a COPY returns None. This is the SAME
            # execution the write already does — reading the count back is
            # free, not a second pass over the data.
            copy_result = con.execute(copy_sql)
        except Exception as exc:
            raise EgressError(f"[{module.id}] write failed to {path!r}: {exc}") from exc
    finally:
        if combined_name is not None:
            try:
                con.execute(f'DROP TABLE IF EXISTS "{combined_name}"')
            except Exception:
                pass  # best-effort cleanup
        try:
            con.unregister(input_name)
        except Exception:
            pass  # best-effort cleanup

    register_as: str | None = cfg.get("register_as_table")
    if register_as:
        _register_as_table(con, module.id, str(register_as), fmt, path)

    if is_append_union:
        # See the docstring: the COPY above just wrote existing+new combined,
        # so its row count is the file's new TOTAL, not this run's delta.
        return None
    try:
        _row = copy_result.fetchone()
        return int(_row[0]) if _row is not None else None
    except Exception:
        return None


def _enforce_on_new_columns(
    module: Module,
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
    fmt: str,
    path: str,
    policy: str,
) -> None:
    """Apply the ``on_new_columns`` write contract (Pass F).

    Compares the incoming relation's columns against the EXISTING target's
    columns (a zero-scan ``LIMIT 0`` metadata read — same idiom
    ``duckdb_/ingress.py``'s reader uses for its own read-side
    ``on_new_columns``/``schema_hint`` checks). ``policy``:

      * ``fail``  — raise if the relation introduces columns the target lacks.
      * ``allow`` — absorb silently; the next ``COPY`` simply writes a fresh
        file with whatever columns the relation has (no merge step needed —
        unlike Spark's Delta ``mergeSchema``, there is no existing-file
        schema to reconcile against, since ``COPY ... TO`` always replaces).
      * ``alert`` — log + record a runtime warning naming them, then absorb.

    Caller only invokes this when the target already exists (``exists=True``)
    — a first write has nothing to drift against, same as Spark's version.
    """
    if policy not in ON_NEW_COLUMNS_POLICIES:
        raise EgressError(
            f"[{module.id}] on_new_columns={policy!r} is invalid; "
            f"use one of {sorted(ON_NEW_COLUMNS_POLICIES)}"
        )

    reader = _reader_function(fmt)
    try:
        existing_cols = set(con.sql(f"SELECT * FROM {reader}('{_escape(path)}') LIMIT 0").columns)
    except Exception as exc:
        logger.debug(
            "[%s] on_new_columns: could not read existing target %r schema: %s",
            module.id,
            path,
            exc,
        )
        return  # unreadable existing target — nothing to drift against, fail-open like Spark's

    new_cols = [c for c in rel.columns if c not in existing_cols]
    if not new_cols:
        return

    if policy == "fail":
        raise EgressError(
            f"[{module.id}] on_new_columns=fail: incoming data adds column(s) "
            f"{new_cols} not present in the target schema. Set on_new_columns: "
            "allow (or alert) to evolve the schema, or fix the upstream transform."
        )
    if policy == "alert":
        logger.warning(
            "[runtime_egress_new_columns] [%s] on_new_columns=alert: schema "
            "drift — new column(s) %s added to the target. Absorbing.",
            module.id,
            new_cols,
        )
        _add_module_warning(
            "runtime_egress_new_columns",
            f"on_new_columns=alert: schema drift — new column(s) {new_cols} added to the target. Absorbing.",
        )
    # policy == "allow": silent absorb — the COPY below just writes the wider relation.


def _escape(path: str) -> str:
    return path.replace("'", "''")


def _reader_function(fmt: str) -> str:
    """Pick the DuckDB table-function reader that matches ``fmt`` exactly.

    Correctness trap (F-9): a two-way ``"read_parquet" if fmt == "parquet"
    else "read_csv"`` silently reads a JSON file AS CSV once ``json`` became
    a writable format — a wrong-answer bug, not a crash. Every writable
    format below MUST have its own reader; there is no "else" fallback."""
    if fmt == "parquet":
        return "read_parquet"
    if fmt == "json":
        return "read_json"
    return "read_csv"


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
    elif fmt == "json":
        # JSON's COPY option vocabulary is disjoint from CSV's (no HEADER/
        # DELIMITER — DuckDB rejects those on a JSON COPY) and from
        # Parquet's (no per-column COMPRESSION-encoding knobs). DuckDB's own
        # JSON-specific options are `ARRAY` (write a single JSON array
        # instead of the default newline-delimited JSON objects) and
        # `COMPRESSION` (shared with the other formats) — both already flow
        # through as freeform `options:` passthrough below (measured against
        # a real DuckDB connection: `COPY ... TO '<f>.json' (FORMAT JSON,
        # ARRAY true)` / `(FORMAT JSON, COMPRESSION gzip)` both accept a
        # quoted or bare value). No default is pinned here — DuckDB's own
        # default (newline-delimited, uncompressed) is what an author gets
        # absent an explicit `options:` override, same as csv's unset
        # `header:` falling back to its own True default above.
        pass
    if partition_by:
        cols = ", ".join(partition_by)
        parts.append(f"PARTITION_BY ({cols})")
        parts.append("OVERWRITE_OR_IGNORE true")
    elif cfg.get("coalesce") and "per_thread_output" not in options_keys_lower:
        # `coalesce` maps onto "the fewest files this engine's COPY can
        # produce for this write shape" (see egress.field.coalesce's
        # capabilities.yml hint): a non-partitioned `COPY ... TO '<path>'`
        # already writes exactly one file by default (measured against a
        # real DuckDB connection — PER_THREAD_OUTPUT cannot even combine
        # with PARTITION_BY, so there is nothing further to do in that
        # branch above), but this pins it EXPLICITLY rather than relying on
        # an undocumented default, so a future DuckDB default change cannot
        # silently reintroduce multi-file output for an author who set
        # `coalesce` expecting the single-file guarantee. Does not target
        # an exact N (DuckDB has no such knob outside partition_by) —
        # any truthy value collapses to the same single-file result, which
        # never produces MORE files than requested.
        parts.append("PER_THREAD_OUTPUT false")
    for key, value in options_cfg.items():
        parts.append(f"{str(key).upper()} '{_escape(str(value))}'")
    return ", ".join(parts)


# ── Catalog table addressing (Pass G2 — feature.table_addressing) ──────────
#
# See `duckdb_/ingress.py::_read_table`'s docstring for the full catalog
# defaulting rule (unqualified -> current catalog+schema; two-part ->
# schema.table within the current catalog; three-part -> a specific,
# already-existing catalog; no implicit ATTACH). This module writes INTO
# that catalog directly — a Blueprint-managed table DuckDB itself owns,
# the write-side mirror of Spark's `writer.saveAsTable(table)` (a
# Spark-managed table, as opposed to `register_as_table`'s external-file
# registration below).
def _write_table(
    rel: duckdb.DuckDBPyRelation,
    module: Module,
    con: duckdb.DuckDBPyConnection,
    table: str,
    mode: str,
) -> int | None:
    """Write ``rel`` into an existing-or-new catalog table named ``table``.

    Mode semantics map onto DuckDB's own DDL guards rather than a manual
    existence check (verified against a real ``DuckDBPyConnection`` —
    each behavior below is DuckDB's own, not emulated):

      - ``overwrite``            -> ``CREATE OR REPLACE TABLE`` (creates if
        absent, replaces the entire contents if present).
      - ``error``/``errorifexists`` -> plain ``CREATE TABLE`` — DuckDB
        itself raises a ``CatalogException`` ("already exists") when the
        table is already there; wrapped into ``EgressError``.
      - ``ignore``               -> ``CREATE TABLE IF NOT EXISTS`` —
        DuckDB skips the write silently (no error, no rows added) when the
        table already exists; a create-and-populate when it does not.
      - ``append``               -> ``CREATE TABLE ... AS`` when the table
        does not exist yet (first write), else ``INSERT INTO ... BY NAME``
        (column-name-aligned, same alignment discipline the path-based
        append's ``UNION ALL BY NAME`` uses) into the existing one.

    Raises:
        EgressError: an unsupported ``mode``, or the underlying DuckDB
            statement fails (table already exists under ``error``/
            ``errorifexists``, an unresolvable multi-part catalog/schema
            name, a column-count/type mismatch on ``append``, ...) — never
            silently swallowed.
    """
    input_name = "__egress_table_input__"
    con.register(input_name, rel)
    try:
        if mode == "overwrite":
            stmt = f'CREATE OR REPLACE TABLE {table} AS SELECT * FROM "{input_name}"'
        elif mode in ("error", "errorifexists"):
            stmt = f'CREATE TABLE {table} AS SELECT * FROM "{input_name}"'
        elif mode == "ignore":
            stmt = f'CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM "{input_name}"'
        elif mode == "append":
            if _table_exists(con, table):
                stmt = f'INSERT INTO {table} BY NAME SELECT * FROM "{input_name}"'
            else:
                stmt = f'CREATE TABLE {table} AS SELECT * FROM "{input_name}"'
        else:
            raise EgressError(
                f"[{module.id}] mode={mode!r} is not implemented for table: addressing "
                f"on the DuckDB engine. Supported: {sorted(SUPPORTED_MODES)}."
            )
        try:
            # `con.execute` (not `con.sql`) — see the matching comment in
            # `write_egress` above: CREATE TABLE AS / INSERT INTO both
            # return a one-row "Count" result through the cursor API for
            # free, as part of the write that already happened. For
            # `mode=ignore` when the table already existed (skipped write,
            # empty result set), `fetchone()` returns None below — reported
            # as None (no write attempted), not a fabricated 0.
            table_result = con.execute(stmt)
        except EgressError:
            raise
        except Exception as exc:
            raise EgressError(f"[{module.id}] write to table {table!r} failed: {exc}") from exc
    finally:
        try:
            con.unregister(input_name)
        except Exception:
            pass  # best-effort cleanup

    try:
        _row = table_result.fetchone()
        return int(_row[0]) if _row is not None else None
    except Exception:
        return None


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Best-effort existence check for ``append`` mode's create-vs-insert
    branch. ``con.table()`` is the same resolver Ingress's ``table:`` read
    uses — any failure (missing catalog/schema/table) means "does not
    exist yet", the correct default for a first `append` write."""
    try:
        con.table(table)
        return True
    except Exception:
        return False


def _register_as_table(
    con: duckdb.DuckDBPyConnection,
    module_id: str,
    table_name: str,
    fmt: str,
    path: str,
) -> None:
    """Register a catalog VIEW over the just-written external file — the
    DuckDB analog of Spark's ``register_as_table``
    (``CREATE EXTERNAL TABLE ... LOCATION``): a live pointer to the file,
    not a copy, so a later read through the registered name sees the
    file's CURRENT contents. Non-fatal — the write itself already
    succeeded; a registration failure only means the read-back-by-name
    convenience is unavailable, logged same as Spark's version.
    """
    try:
        reader = _reader_function(fmt)
        con.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM {reader}('{_escape(path)}')"
        )
        logger.info("Registered catalog view %r over %s", table_name, path)
    except Exception as exc:
        logger.warning(
            "[runtime_egress_register_table_failed] [%s] register_as_table %r failed (non-fatal): %s",
            module_id,
            table_name,
            exc,
        )
        _add_module_warning(
            "runtime_egress_register_table_failed",
            f"register_as_table {table_name!r} failed (non-fatal): {exc}",
        )


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
            raise EgressError(f"[{module.id}] depot Egress requires 'value' or 'value_expr'")
        value = str(raw_value)

    try:
        depot.put(key, value)
    except Exception as exc:
        raise EgressError(f"[{module.id}] depot.put({key!r}) failed: {exc}") from exc
    logger.info("Depot write: %s = %r", key, value)


__all__ = ["EgressError", "write_egress", "SUPPORTED_FORMATS", "SUPPORTED_MODES"]
