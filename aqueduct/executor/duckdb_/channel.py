"""Channel executor — dispatches Channel op types against upstream DuckDB relations.

Stage A scope (see ``capabilities.yml`` — the other Channel ops are honestly
UNSUPPORTED, not silently faked):

  SQL-based (register upstream relations as views, run DuckDB SQL):
    sql   — arbitrary SQL query, transpiled Spark SQL -> DuckDB SQL via
            sqlglot (the query is authored once, against Spark's dialect,
            in the same Blueprint that also runs on the spark engine).
            Upstreams available as view names by module ID, same convention
            as Spark's temp views.
    join  — LEFT/INNER/RIGHT/FULL/SEMI/ANTI/CROSS join with optional
            broadcast hint. DuckDB has no broadcast-join hint syntax
            (its optimizer chooses join strategy itself, single-process) —
            the hint is silently dropped rather than transpiled, since
            dropping it changes performance, never correctness.

  Native relational API — single upstream input:
    deduplicate  — rel.distinct() (no key) or a QUALIFY row_number() query
                   (key + order_by, DISTINCT ON semantics)
    filter       — rel.filter(condition)
    select       — rel.project(*columns)

Context tokens (${ctx.*}) are already resolved by the Compiler before the
Manifest reaches the Executor; all config values are used verbatim.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError
from aqueduct.models import Module

logger = logging.getLogger(__name__)

_SINGLE_INPUT_ALIAS = "__input__"

_VALID_JOIN_TYPES = frozenset({"inner", "left", "right", "full", "semi", "anti", "cross"})

# Stage A op set — a strict subset of aqueduct.executor.channel_ops.ALL_OPS.
# See capabilities.yml's channel.op.* rows for the honest per-op verdicts and
# the "not implemented this stage" hints for the rest.
SQL_OPS: frozenset[str] = frozenset({"sql", "join"})
SINGLE_INPUT_OPS: frozenset[str] = frozenset(
    {"deduplicate", "filter", "select", "cast", "rename", "sort"}
)
MULTI_INPUT_OPS: frozenset[str] = frozenset({"union"})
ALL_OPS: frozenset[str] = SQL_OPS | SINGLE_INPUT_OPS | MULTI_INPUT_OPS

# Spark type-name aliases -> DuckDB type names, for op=cast. Lets a Blueprint
# authored once against Spark's `cast` type vocabulary run unmodified on DuckDB
# (DuckDB accepts most names verbatim; only a few Spark spellings differ).
_CAST_TYPE_ALIASES: dict[str, str] = {
    "string": "VARCHAR",
    "long": "BIGINT",
    "int": "INTEGER",
    "integer": "INTEGER",
    "short": "SMALLINT",
    "byte": "TINYINT",
    "bool": "BOOLEAN",
    "double": "DOUBLE",
    "float": "FLOAT",
}


def _normalize_cast_type(t: str) -> str:
    return _CAST_TYPE_ALIASES.get(str(t).strip().lower(), str(t))


class ChannelError(AqueductError):
    """Raised when a Channel module fails to execute."""


# ── SQL helpers ───────────────────────────────────────────────────────────────

def _transpile(module_id: str, query: str) -> str:
    """Transpile a Spark-SQL query string to DuckDB SQL via sqlglot.

    sqlglot is already a base aqueduct-core dependency (channel SQL
    fingerprinting, ``aqueduct/compiler/fingerprint.py``) — no new dependency
    introduced here. Transpilation lets the SAME Channel SQL, authored once
    against Spark's dialect, run unmodified on both engines; a query using a
    Spark-only construct sqlglot cannot express in DuckDB's dialect raises a
    clear ChannelError rather than a confusing DuckDB parser error pointing
    at generated SQL the author never wrote.
    """
    import sqlglot

    try:
        transpiled = sqlglot.transpile(query, read="spark", write="duckdb")
    except Exception as exc:
        raise ChannelError(
            f"[{module_id}] op=sql query could not be transpiled from Spark SQL to "
            f"DuckDB SQL: {exc}. Rewrite the query in a dialect-neutral way, or avoid "
            "Spark-specific SQL functions/syntax for a Channel that must also run on duckdb."
        ) from exc
    if not transpiled:
        raise ChannelError(f"[{module_id}] op=sql query transpiled to nothing")
    if len(transpiled) > 1:
        raise ChannelError(
            f"[{module_id}] op=sql query transpiled into {len(transpiled)} statements; "
            "a Channel query must be a single SELECT."
        )
    return transpiled[0]


def _build_join_query(module_id: str, cfg: dict) -> str:
    left: str | None = cfg.get("left")
    right: str | None = cfg.get("right")
    join_type: str = str(cfg.get("join_type", "inner")).lower()
    condition: str | None = cfg.get("condition")
    # broadcast_side is accepted (Blueprint parity with Spark) but has no
    # DuckDB equivalent — dropped rather than transpiled into a hint DuckDB
    # doesn't understand. See module docstring.

    if not left:
        raise ChannelError(f"[{module_id}] op=join requires 'left'")
    if not right:
        raise ChannelError(f"[{module_id}] op=join requires 'right'")
    if join_type not in _VALID_JOIN_TYPES:
        raise ChannelError(
            f"[{module_id}] op=join invalid join_type {join_type!r}. "
            f"Valid: {sorted(_VALID_JOIN_TYPES)}"
        )
    if join_type != "cross" and not condition:
        raise ChannelError(f"[{module_id}] op=join requires 'condition' for join_type={join_type!r}")

    on_clause = f" ON {condition}" if condition else ""
    # Double-quote the FROM/JOIN table references: `left`/`right` are the
    # upstream module IDs, author-chosen strings that may collide with a SQL
    # reserved word (DuckDB's grammar reserves LEFT/RIGHT for join-type
    # syntax, so `FROM left INNER JOIN right` is a parser error, not just an
    # ambiguity). Quoting the reference is safe for any unquoted identifier
    # in `condition` that names the same module ID — DuckDB folds unquoted
    # identifiers to lowercase, and module IDs are already lowercase-safe
    # strings, so `left.id` in `condition` still resolves against `"left"`.
    return f'SELECT * FROM "{left}" {join_type.upper()} JOIN "{right}"{on_clause}'


def _run_sql(
    module_id: str,
    query: str,
    upstream_rels: dict[str, duckdb.DuckDBPyRelation],
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Run ``query`` against the upstream relations registered as views.

    Materializes into a uniquely-named temp table immediately, rather than
    returning ``con.sql(query)`` unevaluated. This is a real Stage A
    trade-off, not a stylistic choice: ``con.register()`` is a MUTABLE
    catalog binding (a "replacement scan" pointing at a Python object), not a
    value capture the way Spark's per-query ``createTempView`` effectively
    is. ``_SINGLE_INPUT_ALIAS`` ("__input__") is reused across every
    single-input SQL-based Channel module in a Manifest — if the relation
    this function returns stayed unevaluated past the point where the NEXT
    Channel module re-registers "__input__" under a different upstream, the
    first module's result would silently read the second module's data once
    something finally materializes it. Materializing here (one DuckDB query
    execution per sql/join Channel module) is what makes that impossible.
    Per-upstream-module-id views (``upstream_id``, always globally unique in
    a Manifest) are left registered for the life of the connection — safe,
    no collision risk, and needs no cleanup.

    Native ops (filter/select/deduplicate without a key) do not go through
    this function and stay fully lazy — see ``_execute_filter`` /
    ``_execute_select`` / ``_execute_deduplicate``.
    """
    import uuid

    for upstream_id, rel in upstream_rels.items():
        con.register(upstream_id, rel)

    single_input_registered = False
    if len(upstream_rels) == 1:
        con.register(_SINGLE_INPUT_ALIAS, next(iter(upstream_rels.values())))
        single_input_registered = True

    try:
        tmp_name = f"__aq_ch_{uuid.uuid4().hex}"
        try:
            con.execute(f'CREATE TEMP TABLE "{tmp_name}" AS {query}')
        except Exception as exc:
            raise ChannelError(f"[{module_id}] SQL execution failed: {exc}") from exc
        return con.table(tmp_name)
    finally:
        if single_input_registered:
            try:
                con.unregister(_SINGLE_INPUT_ALIAS)
            except Exception:
                pass  # best-effort cleanup


# ── Native relational-API op implementations ──────────────────────────────────

def _execute_deduplicate(
    module_id: str, rel: duckdb.DuckDBPyRelation, cfg: dict, con: duckdb.DuckDBPyConnection
) -> duckdb.DuckDBPyRelation:
    key = cfg.get("key")
    order_by: str | None = cfg.get("order_by")

    key_cols: list[str] | None = None
    if key:
        key_cols = [key] if isinstance(key, str) else list(key)

    if order_by:
        if not key_cols:
            raise ChannelError(f"[{module_id}] op=deduplicate with order_by requires 'key'")
        partition = ", ".join(key_cols)
        query = (
            f"SELECT * EXCLUDE (_aq_rank) FROM ("
            f"SELECT *, row_number() OVER (PARTITION BY {partition} ORDER BY {order_by}) "
            f"AS _aq_rank FROM {_SINGLE_INPUT_ALIAS}) WHERE _aq_rank = 1"
        )
        try:
            return _run_sql(module_id, query, {"__dedup_input__": rel}, con)
        except ChannelError as exc:
            raise ChannelError(f"[{module_id}] op=deduplicate failed: {exc}") from exc

    if key_cols:
        cols = ", ".join(key_cols)
        query = f"SELECT DISTINCT ON ({cols}) * FROM {_SINGLE_INPUT_ALIAS}"
        try:
            return _run_sql(module_id, query, {"__dedup_input__": rel}, con)
        except ChannelError as exc:
            raise ChannelError(f"[{module_id}] op=deduplicate failed: {exc}") from exc

    try:
        return rel.distinct()
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=deduplicate failed: {exc}") from exc


def _execute_filter(module_id: str, rel: duckdb.DuckDBPyRelation, cfg: dict) -> duckdb.DuckDBPyRelation:
    condition: str | None = cfg.get("condition") or cfg.get("expr")
    if not condition:
        raise ChannelError(f"[{module_id}] op=filter requires 'condition'")
    try:
        return rel.filter(condition)
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=filter failed: {exc}") from exc


def _execute_select(module_id: str, rel: duckdb.DuckDBPyRelation, cfg: dict) -> duckdb.DuckDBPyRelation:
    columns = cfg.get("columns") or cfg.get("cols")
    if not columns:
        raise ChannelError(f"[{module_id}] op=select requires 'columns'")
    cols = [columns] if isinstance(columns, str) else list(columns)
    try:
        return rel.project(", ".join(cols))
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=select failed: {exc}") from exc


def _parse_column_mapping(module_id: str, cfg: dict, op: str, key_a: str, key_b: str) -> dict[str, str]:
    """Parse a ``columns:`` config into ``{a: b}`` for op=rename / op=cast.

    Accepts a dict (``{old: new}`` / ``{col: type}``) or a list of
    ``{<key_a>, <key_b>}`` objects — the same two shapes the Spark handlers
    accept, so a Blueprint's ``rename`` / ``cast`` config is engine-portable.
    """
    columns = cfg.get("columns")
    if not columns:
        raise ChannelError(f"[{module_id}] op={op} requires 'columns'")
    if isinstance(columns, dict):
        return {str(k): str(v) for k, v in columns.items()}
    if isinstance(columns, list):
        mapping: dict[str, str] = {}
        for item in columns:
            if isinstance(item, dict):
                a = item.get(key_a) or item.get("column") or item.get("old") or item.get("from")
                b = item.get(key_b) or item.get("type") or item.get("new") or item.get("to")
                if a and b:
                    mapping[str(a)] = str(b)
        return mapping
    raise ChannelError(
        f"[{module_id}] op={op} 'columns' must be a mapping or a list of objects"
    )


def _execute_cast(module_id: str, rel: duckdb.DuckDBPyRelation, cfg: dict) -> duckdb.DuckDBPyRelation:
    """CAST targeted columns in place, preserving column order and the rest.

    A projection over ``rel.columns`` that wraps only the targeted columns in
    ``CAST(...)`` — same result shape as Spark's ``withColumn(col,
    col.cast(type))``. Stays lazy (``rel.project``).
    """
    mapping = _parse_column_mapping(module_id, cfg, "cast", "column", "type")
    exprs: list[str] = []
    for col in rel.columns:
        if col in mapping:
            exprs.append(f'CAST("{col}" AS {_normalize_cast_type(mapping[col])}) AS "{col}"')
        else:
            exprs.append(f'"{col}"')
    try:
        return rel.project(", ".join(exprs))
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=cast failed: {exc}") from exc


def _execute_rename(module_id: str, rel: duckdb.DuckDBPyRelation, cfg: dict) -> duckdb.DuckDBPyRelation:
    """Rename targeted columns, preserving column order and the rest."""
    mapping = _parse_column_mapping(module_id, cfg, "rename", "from", "to")
    exprs: list[str] = []
    for col in rel.columns:
        if col in mapping:
            exprs.append(f'"{col}" AS "{mapping[col]}"')
        else:
            exprs.append(f'"{col}"')
    try:
        return rel.project(", ".join(exprs))
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=rename failed: {exc}") from exc


def _execute_sort(module_id: str, rel: duckdb.DuckDBPyRelation, cfg: dict) -> duckdb.DuckDBPyRelation:
    """ORDER BY the given expression(s). ``order_by`` is a string ("age DESC")
    or a list of them, passed to DuckDB's ORDER BY verbatim (same as the
    Blueprint's Spark ``order_by``)."""
    order_by = cfg.get("order_by") or cfg.get("columns")
    if not order_by:
        raise ChannelError(f"[{module_id}] op=sort requires 'order_by'")
    exprs = [order_by] if isinstance(order_by, str) else list(order_by)
    try:
        return rel.order(", ".join(str(e) for e in exprs))
    except Exception as exc:
        raise ChannelError(f"[{module_id}] op=sort failed: {exc}") from exc


def _execute_union(
    module_id: str,
    upstream_rels: dict[str, duckdb.DuckDBPyRelation],
    cfg: dict,
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Stack all upstream relations. Mirrors Spark's ``unionByName`` (UNION ALL,
    column-name aligned) — ``allow_missing_columns`` (default True) uses DuckDB's
    ``UNION ALL BY NAME`` (tolerant of column-order / missing columns, nulls
    filled); False uses positional ``UNION ALL`` so a genuine schema mismatch
    fails loudly. Keeps ALL rows (no dedup), matching op=union's Spark semantics.
    """
    if len(upstream_rels) < 2:
        raise ChannelError(
            f"[{module_id}] op=union requires at least 2 upstream relations; got {len(upstream_rels)}"
        )
    allow_missing = bool(cfg.get("allow_missing_columns", True))
    connector = "UNION ALL BY NAME" if allow_missing else "UNION ALL"
    query = f" {connector} ".join(f"SELECT * FROM {mid}" for mid in upstream_rels)
    try:
        return _run_sql(module_id, query, upstream_rels, con)
    except ChannelError as exc:
        raise ChannelError(f"[{module_id}] op=union failed: {exc}") from exc


# ── Public API ────────────────────────────────────────────────────────────────

def execute_channel(
    module: Module,
    upstream_rels: dict[str, duckdb.DuckDBPyRelation],
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Dispatch a Channel module to the correct op handler.

    Args:
        module:        Channel Module from the compiled Manifest.
        upstream_rels: Mapping of upstream module ID -> lazy relation.
        con:           Active DuckDB connection (caller owns lifecycle).

    Returns:
        Lazy result relation. No query executes here.

    Raises:
        ChannelError: Config invalid, op not implemented this stage (Stage A
                      scope — see module docstring), or execution failure.
    """
    cfg = module.config
    op: str | None = cfg.get("op")

    if not upstream_rels:
        raise ChannelError(
            f"[{module.id}] Channel has no upstream relations; at least one main-port input is required"
        )

    if op not in ALL_OPS:
        raise ChannelError(
            f"[{module.id}] Channel op {op!r} is not implemented for the DuckDB engine in "
            f"Stage A. Implemented: {sorted(ALL_OPS)}. See docs/compatibility.md."
        )

    if op in SQL_OPS:
        if op == "join":
            query = _build_join_query(module.id, cfg)
            return _run_sql(module.id, query, upstream_rels, con)
        query = cfg.get("query")
        if not query or not query.strip():
            raise ChannelError(f"[{module.id}] op=sql requires a non-empty 'query'")
        duckdb_query = _transpile(module.id, query)
        return _run_sql(module.id, duckdb_query, upstream_rels, con)

    if op in MULTI_INPUT_OPS:  # op=union — multi-relation
        return _execute_union(module.id, upstream_rels, cfg, con)

    if len(upstream_rels) > 1:
        raise ChannelError(
            f"[{module.id}] op={op!r} takes exactly one upstream relation; "
            f"got {len(upstream_rels)}. Use op=sql to combine inputs on this engine "
            "(Funnel's union modes are also available)."
        )
    rel = next(iter(upstream_rels.values()))

    if op == "deduplicate":
        return _execute_deduplicate(module.id, rel, cfg, con)
    if op == "filter":
        return _execute_filter(module.id, rel, cfg)
    if op == "select":
        return _execute_select(module.id, rel, cfg)
    if op == "cast":
        return _execute_cast(module.id, rel, cfg)
    if op == "rename":
        return _execute_rename(module.id, rel, cfg)
    if op == "sort":
        return _execute_sort(module.id, rel, cfg)
    raise ChannelError(f"[{module.id}] unhandled op {op!r}")  # unreachable — ALL_OPS guard above catches unknowns


__all__ = [
    "ChannelError", "execute_channel", "ALL_OPS", "SQL_OPS", "SINGLE_INPUT_OPS", "MULTI_INPUT_OPS",
]
