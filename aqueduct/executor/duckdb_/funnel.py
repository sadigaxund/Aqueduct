"""Funnel executor — fan-in: merges multiple relations into one.

Modes:
  ``union_all`` / ``union`` — DuckDB ``UNION [ALL]`` / ``... BY NAME``
    (column-name-aligned, matching Spark's ``unionByName``); ``BY NAME`` is
    used under ``schema_check: permissive``.
  ``coalesce`` / ``zip`` — ROW-ALIGNED merges: each input gets a synthetic
    ``row_number() OVER () - 1`` id (the DuckDB counterpart of Spark's
    ``monotonically_increasing_id``), the inputs are LEFT-joined on that id
    (base = first input, mirroring Spark's ``base.join(other, how="left")``),
    and then ``coalesce`` folds overlapping columns (first non-null
    left-to-right) while ``zip`` keeps all columns (names must be unique across
    inputs). Same semantics as the Spark handler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError
from aqueduct.models import Module

VALID_MODES: frozenset[str] = frozenset({"union_all", "union", "coalesce", "zip"})

_ROW_ID = "_aq_row_id"


class FunnelError(AqueductError):
    """Raised when a Funnel module cannot be executed."""


def _resolve_inputs(
    module_id: str, cfg: dict, upstream_rels: dict[str, duckdb.DuckDBPyRelation]
) -> list[str]:
    """Validate 'inputs' list and return the ordered upstream module IDs."""
    input_ids: list[str] = cfg.get("inputs", [])
    if not input_ids:
        raise FunnelError(f"[{module_id}] 'inputs' must list at least two upstream module IDs")
    if len(input_ids) < 2:
        raise FunnelError(f"[{module_id}] 'inputs' must contain at least 2 entries; got {len(input_ids)}")
    for iid in input_ids:
        if iid not in upstream_rels:
            raise FunnelError(
                f"[{module_id}] upstream module {iid!r} not found in frame_store. "
                f"Available: {list(upstream_rels)}"
            )
    return input_ids


def execute_funnel(
    module: Module,
    upstream_rels: dict[str, duckdb.DuckDBPyRelation],
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Merge upstream relations into one according to module.config.

    Raises:
        FunnelError: Invalid config, mode not implemented this stage (see
                     module docstring), or missing inputs.
    """
    cfg = module.config
    mode = cfg.get("mode")
    schema_check: str = cfg.get("schema_check", "strict")

    if mode not in VALID_MODES:
        raise FunnelError(
            f"[{module.id}] Funnel mode {mode!r} is not implemented for the DuckDB engine in "
            f"Stage A. Implemented: {sorted(VALID_MODES)}. See docs/compatibility.md."
        )

    input_ids = _resolve_inputs(module.id, cfg, upstream_rels)

    if schema_check not in ("strict", "permissive"):
        raise FunnelError(f"[{module.id}] unknown schema_check {schema_check!r}; use strict or permissive")

    registered: list[str] = []
    try:
        for iid in input_ids:
            con.register(iid, upstream_rels[iid])
            registered.append(iid)

        if mode in ("union_all", "union"):
            base = "UNION ALL" if mode == "union_all" else "UNION"
            # BY NAME (column-name alignment, tolerant of column-order/subset
            # differences) is only meaningful with permissive schema_check;
            # strict schema_check keeps positional UNION [ALL] semantics so a
            # genuine schema mismatch fails loudly, matching Spark's default.
            connector = f"{base} BY NAME" if schema_check == "permissive" else base
            query = f" {connector} ".join(f"SELECT * FROM {iid}" for iid in input_ids)
        elif mode == "coalesce":
            query = _row_aligned_query(module.id, input_ids, upstream_rels, coalesce=True)
        else:  # zip
            query = _row_aligned_query(module.id, input_ids, upstream_rels, coalesce=False)

        # Materialize into a uniquely-named temp table immediately, rather
        # than returning ``con.sql(query)`` unevaluated. Same real Stage A
        # trade-off documented at length in ``channel.py::_run_sql`` (and for
        # the identical reason): ``con.register()`` is a MUTABLE catalog
        # binding, not a value capture, and the `finally` block below always
        # unregisters the upstream views before the caller gets a chance to
        # consume the returned relation. Without materializing here, a
        # ``con.sql(query)`` result stays lazy and re-resolves "ing_a"/"ing_b"
        # by NAME at the caller's first ``fetchall()``/``COPY`` — which fails
        # with a ``CatalogException`` because that name was already torn
        # down. Caught by ``test_module_type_funnel_driven_through_execute``
        # (Phase 78 checkpoint 3b): the union/coalesce/zip unit tests all
        # called ``execute_funnel`` and consumed the result in the SAME local
        # scope, which happened to keep the registration alive long enough to
        # mask this; a real multi-module ``execute()`` run did not.
        import uuid

        tmp_name = f"__aq_fn_{uuid.uuid4().hex}"
        try:
            con.execute(f'CREATE TEMP TABLE "{tmp_name}" AS {query}')
        except FunnelError:
            raise
        except Exception as exc:
            raise FunnelError(f"[{module.id}] {mode} failed: {exc}") from exc
        return con.table(tmp_name)
    finally:
        for name in registered:
            try:
                con.unregister(name)
            except Exception:
                pass


def _row_aligned_query(
    module_id: str,
    input_ids: list[str],
    upstream_rels: dict[str, duckdb.DuckDBPyRelation],
    *,
    coalesce: bool,
) -> str:
    """Build the row-id LEFT-JOIN query for coalesce / zip.

    Each input becomes a CTE that tags rows with ``row_number() OVER () - 1``;
    the CTEs are LEFT-joined on that id from the first input (the base). For
    ``coalesce`` overlapping columns are folded with ``COALESCE(...)`` (first
    non-null left-to-right, first-appearance output order); for ``zip`` every
    column is kept and cross-input name collisions are rejected up front.
    """
    cols_per_input: list[list[str]] = [list(upstream_rels[iid].columns) for iid in input_ids]

    ctes = ", ".join(
        f't{i} AS (SELECT *, row_number() OVER () - 1 AS {_ROW_ID} FROM "{iid}")'
        for i, iid in enumerate(input_ids)
    )
    joins = "t0"
    for i in range(1, len(input_ids)):
        joins += f" LEFT JOIN t{i} ON t0.{_ROW_ID} = t{i}.{_ROW_ID}"

    if coalesce:
        # first-appearance order across inputs; fold every input that has the col
        ordered: list[str] = []
        holders: dict[str, list[int]] = {}
        for i, cols in enumerate(cols_per_input):
            for c in cols:
                if c not in holders:
                    holders[c] = []
                    ordered.append(c)
                holders[c].append(i)
        select_exprs = []
        for c in ordered:
            refs = [f't{i}."{c}"' for i in holders[c]]
            expr = refs[0] if len(refs) == 1 else f"COALESCE({', '.join(refs)})"
            select_exprs.append(f'{expr} AS "{c}"')
    else:  # zip — all columns kept, names must be unique across inputs
        seen: set[str] = set()
        select_exprs = []
        for i, cols in enumerate(cols_per_input):
            for c in cols:
                if c in seen:
                    raise FunnelError(
                        f"[{module_id}] zip mode requires unique column names across all "
                        f"inputs; duplicate column: {c!r}"
                    )
                seen.add(c)
                select_exprs.append(f't{i}."{c}" AS "{c}"')

    return f"WITH {ctes} SELECT {', '.join(select_exprs)} FROM {joins}"


__all__ = ["FunnelError", "execute_funnel", "VALID_MODES"]
