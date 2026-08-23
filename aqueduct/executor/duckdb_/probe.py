"""Probe executor — captures observability signals from a lazy DuckDB relation
(Pass F — cross-engine remediation).

Mirrors ``aqueduct/executor/spark/probe.py``'s signal vocabulary, warning
``rule_id``s, and ``probe_signals`` schema exactly (same reason as
``duckdb_/assert_.py``: cross-engine observability queries must keep working
unmodified) — translated into DuckDB's own execution idiom: a
``duckdb.DuckDBPyRelation`` and SQL aggregate/sample expressions where Spark
uses a ``DataFrame`` and column-expression actions. Does NOT import from
``executor/spark/`` — same deliberate duplication-over-cross-package-import
discipline ``assert_.py``'s module docstring explains in full (small
engine-agnostic pieces are copied by hand rather than reaching across the
Spark/DuckDB boundary).

Signal coverage — 8 of Spark's 9 built-ins, plus ``custom``. ``execution_partitions``
is the one deliberate omission: DuckDB is single-process with no partition
concept to report (see the dispatcher's dedicated warning below) — an
"unsupported signal type" is not the same state as "unknown signal type" and
gets its own rule id so the two are never confused in the observability log.

Zero-cost-observability accounting (AGENTS.md's rule, translated for this
engine — the question is "does this signal force an extra pass over the
relation?", not "does it call a Spark action?"):

  schema_snapshot      Zero query — ``rel.columns``/``rel.types`` metadata.
  row_count_estimate   EXACT, not an estimate — see the function docstring
                        for the footer-vs-count split. NOT gated by
                        ``block_full_actions``: measured at sub-millisecond
                        cost even on a 1M-row parquet file (unlike Spark's
                        distributed ``.count()``, the reason this signal
                        exists on that engine does not transfer here).
  null_rates            One pass — a Bernoulli sample still visits every row
                        to flip its inclusion coin (same honesty Spark's
                        docstring states for `.sample()`). Gated by
                        ``block_full_actions``.
  sample_rows           ``rel.limit(n)`` — DuckDB can often satisfy LIMIT
                        without a full scan (pushed into a file scan the same
                        way Spark's partition read can). Not gated.
  value_distribution    One pass (aggregate query, optionally Bernoulli
                        sampled). Gated by ``block_full_actions``.
  distinct_count        One pass (``approx_count_distinct`` aggregate,
                        optionally sampled). Gated by ``block_full_actions``.
  data_freshness        One pass (``MAX(column)`` aggregate, optionally
                        sampled). Gated by ``block_full_actions`` unless
                        ``allow_sample: true``.
  execution_partitions  Not implemented — see above. Never gated (there is
                        nothing to run).
  threshold             One pass (SQL aggregate boolean expression,
                        transpiled from Spark SQL). Never gated — same as
                        Spark (it is the Regulator-gate signal; blocking it
                        would silently strand every Regulator open/closed).
  custom                Depends entirely on the resolved form — see
                        ``_custom``'s docstring. Inline SQL is one pass (like
                        ``threshold``); pointer/plugin callables are driver
                        code the engine cannot bound (see the
                        ``custom_probe_driver_code`` compiler warning, now
                        engine-neutral).

Config shape (YAML / dict) — identical to Spark's; see
``aqueduct/executor/spark/probe.py``'s module docstring for the full
per-signal-type key reference. Not repeated here to avoid the two drifting;
the only behavioural difference from that reference is ``row_count_estimate``
(exact here, not an estimate) and the absence of ``execution_partitions``.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import ConfigError
from aqueduct.executor.duckdb_.egress import _escape
from aqueduct.executor.models import _add_module_warning
from aqueduct.models import Module, ModuleType
from aqueduct.redaction import redact as _redact

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProbeSampling:
    """Duplicated from ``executor/spark/probe.py`` — same fields, same
    defaults, same ``aqueduct.yml`` `probes:`/`observability.retention:`
    blocks behind it. See this module's docstring for why this is a
    deliberate copy, not an import."""

    max_sample_rows: int = 100
    default_sample_fraction: float = 0.1
    # Phase 85 A1 — per-probe retention cap for the sample_rows signal type.
    # From `aqueduct.yml`'s `observability.retention.sample_rows_keep_last_n`.
    sample_rows_keep_last_n: int = 20


# ── DuckDB DDL — byte-identical to spark/probe.py's, same observability
# table both engines write into (see module docstring). ────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS probe_signals (
    run_id       VARCHAR  NOT NULL,
    probe_id     VARCHAR  NOT NULL,
    signal_type  VARCHAR  NOT NULL,
    payload      JSON     NOT NULL,
    captured_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_probe_signals_probe
    ON probe_signals (probe_id, signal_type);
"""

_NUMERIC_TYPE_PREFIXES: frozenset[str] = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
    }
)


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _json_dumps(obj: Any) -> str:
    """json.dumps that coerces DuckDB-native types (datetime, Decimal, bytes)."""

    def _default(o: Any) -> Any:
        if isinstance(o, datetime | date):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, bytes):
            return o.hex()
        return str(o)

    return json.dumps(obj, default=_default)


def _is_numeric_type(dtype: object) -> bool:
    upper = str(dtype).upper()
    return any(upper.startswith(p) for p in _NUMERIC_TYPE_PREFIXES)


def _transpile_expr(rule_type: str, expr: str) -> str:
    """Transpile a Spark-SQL scalar expression to DuckDB SQL via sqlglot.

    Same idiom + rationale as ``duckdb_/assert_.py``'s ``_transpile_expr``:
    ``expr:``/``sql:``/``passed_when:`` are authored once against Spark's
    dialect in a Blueprint that also runs on ``engine: spark``, so a Probe
    ``threshold``/``custom`` expression must run unmodified on both.
    """
    import sqlglot

    try:
        parsed = sqlglot.parse_one(expr, read="spark")
        return parsed.sql(dialect="duckdb")
    except Exception as exc:
        raise ConfigError(
            f"probe signal {rule_type} expr {expr!r} could not be transpiled from Spark SQL "
            f"to DuckDB SQL: {exc}. Rewrite the expression in a dialect-neutral way, or avoid "
            "Spark-specific SQL functions/syntax for a Probe signal that must also run on duckdb."
        ) from exc


def _sampled_fetchone(
    con: duckdb.DuckDBPyConnection,
    rel: duckdb.DuckDBPyRelation,
    fraction: float,
    select_sql: str,
) -> tuple:
    """Run ``select_sql`` over a Bernoulli sample of ``rel``.

    ``USING SAMPLE ... PERCENT (bernoulli)`` is SQL syntax, not exposed as a
    relation method (same reason ``assert_.py``'s ``null_rate`` rule does the
    same register/query/unregister dance instead of ``rel.aggregate()``).
    """
    pct = max(0.0, min(100.0, fraction * 100))
    view_name = f"__aq_probe_{uuid.uuid4().hex}__"
    con.register(view_name, rel)
    try:
        return con.sql(
            f'SELECT {select_sql} FROM "{view_name}" USING SAMPLE {pct} PERCENT (bernoulli)'
        ).fetchone()
    finally:
        con.unregister(view_name)


# ── Signal implementations ────────────────────────────────────────────────────


def _schema_snapshot(rel: duckdb.DuckDBPyRelation) -> dict[str, Any]:
    """Capture rel.columns/rel.types — zero query execution.

    ``nullable`` is always reported as ``None``: unlike Spark's DataFrame
    schema (sourced from the reader or an explicit declared schema), a
    DuckDB relation's DB-API ``description`` does not carry a per-column
    nullability flag independent of a persisted table's own constraints —
    measured against this DuckDB version (``rel.description``'s ``null_ok``
    slot is always ``None``, for both an ad hoc query and a real
    ``NOT NULL``-constrained table). Reported honestly as unknown rather
    than guessed.
    """
    fields = [
        {"name": name, "type": str(dtype), "nullable": None}
        for name, dtype in zip(rel.columns, rel.types, strict=True)
    ]
    return {"fields": fields}


def _row_count_estimate(
    rel: duckdb.DuckDBPyRelation,
    signal_cfg: dict[str, Any],
    con: duckdb.DuckDBPyConnection,
    target_module: Module | None = None,
) -> dict[str, Any]:
    """Return an EXACT row count — never an estimate on this engine.

    Two zero-vs-one-pass methods, chosen automatically (``signal_cfg``'s
    ``method``/``fraction`` keys, inherited from the Blueprint grammar for
    cross-engine parity, are accepted but not consulted — see module
    docstring):

      * ``parquet_footer`` — when the Probe attaches DIRECTLY to a
        ``format: parquet`` Ingress with no ``partition_filters``/
        ``sandbox_limit`` narrowing the read, ``num_rows`` is summed straight
        from every file's Parquet footer (``parquet_file_metadata()``) — a
        metadata read, zero rows scanned. Measured: sub-millisecond even
        against a 1,000,000-row file.
      * ``exact_count`` — every other case (csv/json Ingress, or the Probe
        attaches to a Channel/Junction/Funnel/downstream relation where the
        footer would be wrong): ``COUNT(*)`` over the relation. Also
        measured sub-millisecond on a 1,000,000-row parquet-backed relation
        (0.0007s bare, 0.0016s with a filter) — a real pass, but so cheap on
        a single-node engine that gating it behind ``block_full_actions``
        would protect against a cost that does not exist. This is the one
        place this module's accounting diverges from Spark's: Spark's
        ``row_count_estimate`` exists to dodge an expensive DISTRIBUTED
        action; that rationale does not transfer to a single-process count.
    """
    if (
        target_module is not None
        and target_module.type == ModuleType.Ingress
        and target_module.config.get("format") == "parquet"
        and not target_module.config.get("partition_filters")
        and not target_module.config.get("sandbox_limit")
    ):
        path = target_module.config.get("path")
        if path:
            try:
                row = con.sql(
                    f"SELECT sum(num_rows) FROM parquet_file_metadata('{_escape(path)}')"
                ).fetchone()
                if row is not None and row[0] is not None:
                    return {"method": "parquet_footer", "estimate": int(row[0])}
            except Exception as exc:
                logger.debug(
                    "row_count_estimate: parquet_file_metadata failed, falling back to COUNT(*): %s",
                    exc,
                )

    count = rel.aggregate("COUNT(*) AS c").fetchone()[0]
    return {"method": "exact_count", "estimate": int(count or 0)}


def _null_rates(
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Per-column null rate over a Bernoulli sample. One pass — see module
    docstring (a Bernoulli sample still visits every row)."""
    columns: list[str] = signal_cfg.get("columns") or rel.columns
    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))

    if block_full_actions:
        logger.warning(
            "[runtime_probe_blocked] Probe: block_full_actions=True; skipping null_rates sample."
        )
        _add_module_warning(
            "runtime_probe_blocked", "block_full_actions=True; skipping null_rates sample."
        )
        return {"fraction": fraction, "blocked": True, "null_rates": {c: None for c in columns}}

    select_sql = "COUNT(*) AS _total, " + ", ".join(
        f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "_null_{i}"'
        for i, c in enumerate(columns)
    )
    row = (
        _sampled_fetchone(con, rel, fraction, select_sql)
        if fraction > 0
        else rel.aggregate(select_sql).fetchone()
    )

    total = row[0] or 0
    if total == 0:
        return {"fraction": fraction, "sample_size": 0, "null_rates": {c: None for c in columns}}

    rates = {c: round((row[i + 1] or 0) / total, 6) for i, c in enumerate(columns)}
    return {"fraction": fraction, "sample_size": total, "null_rates": rates}


def _sample_rows(
    rel: duckdb.DuckDBPyRelation,
    signal_cfg: dict[str, Any],
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Fetch at most n rows as JSON-serialisable dicts via ``rel.limit(n)``.

    DuckDB can satisfy a LIMIT from a file scan's first rows without a full
    scan the same way Spark's ``limit(n).collect()`` can — not gated.
    """
    n = int(signal_cfg.get("n", 10))
    n = min(n, sampling.max_sample_rows)
    limited = rel.limit(n)
    rows = limited.fetchall()
    cols = limited.columns
    serialised = [dict(zip(cols, row, strict=True)) for row in rows]
    return {"n": n, "rows": serialised}


def _value_distribution(
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Min/max/mean/stddev + percentiles per column. One pass (aggregate
    query, optionally Bernoulli sampled) — gated by ``block_full_actions``."""
    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))
    percentiles: list[float] = signal_cfg.get("percentiles", [0.25, 0.5, 0.75])

    if block_full_actions:
        logger.warning(
            "[runtime_probe_blocked] Probe: block_full_actions=True; skipping value_distribution."
        )
        _add_module_warning(
            "runtime_probe_blocked", "block_full_actions=True; skipping value_distribution."
        )
        return {"blocked": True, "fraction": fraction, "stats": {}}

    requested: list[str] | None = signal_cfg.get("columns")
    columns = (
        requested
        if requested
        else [
            name
            for name, dtype in zip(rel.columns, rel.types, strict=True)
            if _is_numeric_type(dtype)
        ]
    )
    if not columns:
        return {"fraction": fraction, "stats": {}}

    parts: list[str] = []
    pct_literal = "[" + ", ".join(str(p) for p in percentiles) + "]" if percentiles else None
    for i, c in enumerate(columns):
        parts += [
            f'min("{c}") AS "_min_{i}"',
            f'max("{c}") AS "_max_{i}"',
            f'avg("{c}") AS "_mean_{i}"',
            f'stddev_samp("{c}") AS "_std_{i}"',
            f'count("{c}") AS "_cnt_{i}"',
        ]
        if pct_literal:
            parts.append(f'approx_quantile("{c}", {pct_literal}) AS "_pct_{i}"')
    select_sql = ", ".join(parts)

    row = (
        _sampled_fetchone(con, rel, fraction, select_sql)
        if fraction > 0
        else rel.aggregate(select_sql).fetchone()
    )

    stats: dict[str, Any] = {}
    stride = 6 if pct_literal else 5
    for i, c in enumerate(columns):
        base = i * stride
        min_v, max_v, mean_v, std_v, cnt_v = row[base : base + 5]
        pct_v = row[base + 5] if pct_literal else []
        stats[c] = {
            "min": min_v,
            "max": max_v,
            "mean": mean_v,
            "stddev": std_v,
            "count_non_null": cnt_v,
            "percentiles": dict(zip([str(p) for p in percentiles], pct_v or [], strict=False)),
        }
    return {"fraction": fraction, "stats": stats}


def _distinct_count(
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Approximate distinct-value count per column via
    ``approx_count_distinct``. One pass — gated by ``block_full_actions``."""
    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))
    columns: list[str] = signal_cfg.get("columns") or rel.columns

    if block_full_actions:
        logger.warning(
            "[runtime_probe_blocked] Probe: block_full_actions=True; skipping distinct_count."
        )
        _add_module_warning(
            "runtime_probe_blocked", "block_full_actions=True; skipping distinct_count."
        )
        return {
            "blocked": True,
            "fraction": fraction,
            "distinct_counts": {c: None for c in columns},
        }

    select_sql = ", ".join(
        f'approx_count_distinct("{c}") AS "_dc_{i}"' for i, c in enumerate(columns)
    )
    row = (
        _sampled_fetchone(con, rel, fraction, select_sql)
        if fraction > 0
        else rel.aggregate(select_sql).fetchone()
    )
    return {"fraction": fraction, "distinct_counts": {c: row[i] for i, c in enumerate(columns)}}


def _data_freshness(
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Capture MAX(column). One pass — gated by ``block_full_actions`` unless
    ``allow_sample: true``."""
    column: str | None = signal_cfg.get("column")
    if not column:
        raise ConfigError("data_freshness signal requires 'column'")

    allow_sample = bool(signal_cfg.get("allow_sample", False))
    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))

    if block_full_actions and not allow_sample:
        logger.warning(
            "[runtime_probe_blocked] Probe: block_full_actions=True; skipping "
            "data_freshness for column=%r. Set allow_sample: true to use a sample instead.",
            column,
        )
        _add_module_warning(
            "runtime_probe_blocked",
            f"block_full_actions=True; skipping data_freshness for "
            f"column={column!r}. Set allow_sample: true to use a sample instead.",
        )
        return {"blocked": True, "column": column}

    select_sql = f'max("{column}") AS mx'
    sampled = allow_sample and fraction > 0
    row = (
        _sampled_fetchone(con, rel, fraction, select_sql)
        if sampled
        else rel.aggregate(select_sql).fetchone()
    )
    return {
        "column": column,
        "max_value": row[0],
        "sampled": sampled,
        "fraction": fraction if sampled else None,
    }


def _threshold(rel: duckdb.DuckDBPyRelation, sig_cfg: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a SQL aggregate expression, transpiled from Spark SQL to
    DuckDB SQL. One pass — never gated by ``block_full_actions``, same as
    Spark (it is the Regulator-gate signal; blocking it would silently
    strand every Regulator open or closed)."""
    expr_str = sig_cfg.get("expr", "")
    if not expr_str:
        raise ConfigError("threshold signal requires an 'expr' field")

    duckdb_expr = _transpile_expr("threshold", expr_str)
    result = rel.aggregate(f"({duckdb_expr}) AS v").fetchone()[0]
    passed = bool(result) if result is not None else False
    return {"passed": passed, "value": result, "expr": expr_str}


def _custom(
    rel: duckdb.DuckDBPyRelation,
    sig_cfg: dict[str, Any],
    block_full_actions: bool = False,
    base_dir: str | None = None,
) -> dict[str, Any]:
    """Execute a user-defined custom probe signal via the shared, engine-
    agnostic resolver (``aqueduct/executor/probe_plugins.py`` — AGENTS.md is
    explicit that this resolver must not be duplicated).

    * **inline SQL** — ``sql``/``passed_when``, each transpiled from Spark
      SQL and evaluated as one aggregate query. One pass each — same cost
      class as ``threshold``.
    * **module pointer / entry-point plugin** — resolve a callable and
      invoke ``fn(rel, sig_cfg)``; it must return a dict. The callable runs
      as ordinary Python against a live ``DuckDBPyRelation`` — the engine
      cannot enforce what it does with it (see the ``custom_probe_driver_code``
      compiler warning).
    """
    from aqueduct.executor.probe_plugins import custom_signal_source, resolve_callable

    source = custom_signal_source(sig_cfg)

    if source == "sql":
        out: dict[str, Any] = {"custom": True}
        sql_expr = sig_cfg.get("sql")
        if sql_expr:
            duckdb_expr = _transpile_expr("custom", sql_expr)
            out["estimate"] = rel.aggregate(f"({duckdb_expr}) AS v").fetchone()[0]
        passed_when = sig_cfg.get("passed_when")
        if passed_when:
            duckdb_passed = _transpile_expr("custom", passed_when)
            result = rel.aggregate(f"({duckdb_passed}) AS v").fetchone()[0]
            out["passed"] = bool(result) if result is not None else False
        return out

    fn = resolve_callable(sig_cfg, base_dir)
    call_cfg = {**sig_cfg, "block_full_actions": block_full_actions}
    result = fn(rel, call_cfg)
    if not isinstance(result, dict):
        raise ConfigError(f"custom probe callable must return a dict, got {type(result).__name__}")
    return {"custom": True, **result}


# ── Public API ────────────────────────────────────────────────────────────────


def _stdout_report_lines(sig_type: str, payload: Any) -> list[str]:
    """Human-readable lines for `report: stdout` (per signal). Duplicated
    verbatim from ``spark/probe.py`` — pure formatting, no engine dependency,
    kept identical so the CLI renders the same shape on either engine."""
    if not isinstance(payload, dict):
        return [f"{sig_type}: {payload}"]
    scalars = {k: v for k, v in payload.items() if not isinstance(v, dict | list)}
    nested = {k: v for k, v in payload.items() if isinstance(v, dict | list)}
    head = "  ·  ".join(f"{k}={v}" for k, v in scalars.items())
    lines = [f"{sig_type}: {head}" if head else f"{sig_type}:"]
    for k, v in nested.items():
        if isinstance(v, dict):
            lines.extend(f"  {k}.{kk}: {vv}" for kk, vv in v.items())
        else:
            lines.extend(f"  {k}[{i}]: {vv}" for i, vv in enumerate(v))
    return lines


def execute_probe(
    module: Module,
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    store_dir: Path,
    block_full_actions: bool = False,
    observability_store: Any = None,
    sampling: ProbeSampling = ProbeSampling(),
    base_dir: str | None = None,
    target_module: Module | None = None,
) -> tuple[str, ...]:
    """Capture observability signals for a single Probe module.

    Writes one row per signal to the configured observability store
    (``store_dir/observability.db`` for DuckDB by default) — same table,
    same shape ``evaluate_regulator()`` (``aqueduct/surveyor/surveyor.py``)
    already reads engine-agnostically, so a Regulator gated on a DuckDB
    Probe works with zero changes on that side.

    Args:
        module:        The Probe Module from the compiled Manifest.
        rel:           Lazy relation produced by the module this Probe taps.
        con:           Active DuckDB connection (caller owns lifecycle).
        run_id:        Run identifier from the Executor.
        store_dir:     Root observability store directory (DuckDB fallback path).
        block_full_actions: Forward to per-signal helpers (see each signal's
                       docstring for which ones honour it).
        observability_store: Optional Phase 28 obs-store backend. When None, a
                       default DuckDB store at ``store_dir/observability.db``
                       is constructed.
        sampling:      Probe sampling governance (max_sample_rows cap +
                       default_sample_fraction) — the SAME ``config.probes.*``
                       knobs Spark reads, now genuinely consumed on this engine.
        base_dir:      Manifest.base_dir — lets a `custom` signal's `module:`
                       pointer resolve a sibling .py file next to the blueprint.
        target_module: The `attach_to` target Module (from the compiled
                       Manifest) — lets `row_count_estimate` recognise a
                       direct parquet-Ingress tap and use the footer path.

    Returns:
        Report lines for the CLI when the Probe declares ``report: stdout``;
        empty tuple otherwise. Persistence to ``probe_signals`` always happens
        regardless — ``report: stdout`` is additive.

    Raises:
        Nothing — all exceptions are caught and logged. Probe failure must
        never halt the blueprint.
    """
    _report_stdout = str(module.config.get("report", "")).lower() == "stdout"
    _notes: list[str] = []
    try:
        signals: list[dict[str, Any]] = module.config.get("signals", [])
        if not signals:
            logger.debug("Probe %r has no signals configured; skipping.", module.id)
            return ()

        store_dir.mkdir(parents=True, exist_ok=True)

        if observability_store is None:
            from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

            observability_store = DuckDBObservabilityStore(store_dir / "observability.db")

        with observability_store.connect() as cur:
            cur.execute(_DDL)

            for sig_cfg in signals:
                sig_type = sig_cfg.get("type")
                try:
                    if sig_type == "schema_snapshot":
                        payload = _schema_snapshot(rel)
                    elif sig_type == "row_count_estimate":
                        payload = _row_count_estimate(
                            rel, sig_cfg, con, target_module=target_module
                        )
                    elif sig_type == "null_rates":
                        payload = _null_rates(
                            rel,
                            con,
                            sig_cfg,
                            block_full_actions=block_full_actions,
                            sampling=sampling,
                        )
                    elif sig_type == "sample_rows":
                        payload = _sample_rows(rel, sig_cfg, sampling=sampling)
                    elif sig_type == "value_distribution":
                        payload = _value_distribution(
                            rel,
                            con,
                            sig_cfg,
                            block_full_actions=block_full_actions,
                            sampling=sampling,
                        )
                    elif sig_type == "distinct_count":
                        payload = _distinct_count(
                            rel,
                            con,
                            sig_cfg,
                            block_full_actions=block_full_actions,
                            sampling=sampling,
                        )
                    elif sig_type == "data_freshness":
                        payload = _data_freshness(
                            rel,
                            con,
                            sig_cfg,
                            block_full_actions=block_full_actions,
                            sampling=sampling,
                        )
                    elif sig_type == "execution_partitions":
                        # Deliberate backstop, not the primary guard (Pass G2):
                        # `probe.signal.execution_partitions` is now a governed
                        # `unsupported` capability leaf on this engine
                        # (capabilities.yml), so a Blueprint compiled through
                        # the normal `compile()` path is refused at COMPILE
                        # time (`aqueduct/compiler/capability_check.py`)
                        # before this code ever runs. This branch stays for
                        # a programmatic caller that builds a Manifest/Module
                        # and calls `execute_probe` directly, bypassing the
                        # capability gate — same reasoning as
                        # `validate_probes`'s missing-`attach_to` case (see
                        # AGENTS.md's Common Pitfalls). Never remove without
                        # confirming every `execute_probe` call site is
                        # gate-checked first.
                        logger.warning(
                            "[runtime_probe_signal_unsupported] Probe %r: execution_partitions has no "
                            "DuckDB equivalent (single-process engine, no partition concept); skipping.",
                            module.id,
                        )
                        _add_module_warning(
                            "runtime_probe_signal_unsupported",
                            f"Probe {module.id!r}: execution_partitions has no DuckDB equivalent "
                            "(no partition concept); skipping.",
                        )
                        continue
                    elif sig_type == "threshold":
                        payload = _threshold(rel, sig_cfg)
                    elif sig_type == "custom":
                        payload = _custom(
                            rel, sig_cfg, block_full_actions=block_full_actions, base_dir=base_dir
                        )
                    else:
                        logger.warning(
                            "[runtime_probe_unknown_signal] Probe %r: unknown signal type %r; skipping.",
                            module.id,
                            sig_type,
                        )
                        _add_module_warning(
                            "runtime_probe_unknown_signal",
                            f"Probe {module.id!r}: unknown signal type {sig_type!r}; skipping.",
                        )
                        continue

                    if sig_type == "sample_rows":
                        # Phase 85 A1 — the ONLY signal type that persists
                        # real sampled data ROW content; every other signal
                        # here is aggregate/statistical. Route it through
                        # the same `_redact()` the failure path
                        # (surveyor.py) already uses.
                        payload = _redact(payload)

                    cur.execute(
                        """
                        INSERT INTO probe_signals
                            (run_id, probe_id, signal_type, payload, captured_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [run_id, module.id, sig_type, _json_dumps(payload), _utcnow_iso()],
                    )

                    if sig_type == "sample_rows":
                        # Phase 85 A1 — retention cap: keep only the most
                        # recent `sample_rows_keep_last_n` rows per probe_id
                        # for this signal type. Mirrors the Spark twin and
                        # `record_explain_snapshot`'s rolling-window prune.
                        try:
                            _stale = cur.execute(
                                """
                                SELECT run_id, captured_at FROM probe_signals
                                WHERE probe_id = ? AND signal_type = 'sample_rows'
                                ORDER BY captured_at DESC
                                """,
                                [module.id],
                            ).fetchall()
                            if len(_stale) > sampling.sample_rows_keep_last_n:
                                for _rid, _ in _stale[sampling.sample_rows_keep_last_n :]:
                                    cur.execute(
                                        """
                                        DELETE FROM probe_signals
                                        WHERE probe_id = ? AND signal_type = 'sample_rows'
                                          AND run_id = ?
                                        """,
                                        [module.id, _rid],
                                    )
                        except Exception:
                            pass  # sample_rows rotation is best-effort housekeeping

                    if _report_stdout:
                        try:
                            _notes.extend(_stdout_report_lines(sig_type, payload))
                        except (
                            Exception
                        ):  # noqa: BLE001 — report formatting must never fail the probe
                            pass
                except Exception as exc:
                    logger.warning(
                        "[runtime_probe_signal_error] Probe %r signal %r failed: %s",
                        module.id,
                        sig_type,
                        exc,
                    )
                    _add_module_warning(
                        "runtime_probe_signal_error",
                        f"Probe {module.id!r} signal {sig_type!r} failed: {exc}",
                    )
    except Exception as exc:
        logger.warning(
            "[runtime_probe_error] execute_probe %r: unexpected error: %s", module.id, exc
        )
        _add_module_warning(
            "runtime_probe_error", f"execute_probe {module.id!r}: unexpected error: {exc}"
        )
    return tuple(_notes)


__all__ = ["ProbeSampling", "execute_probe"]
