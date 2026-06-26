"""Probe executor — captures observability signals from a lazy DataFrame.

Signals are written to DuckDB (``store_dir/observability.db``) so the Surveyor
can serve them via ``get_probe_signal()``.  For ``schema_snapshot`` a JSON
file is also written to ``store_dir/snapshots/<run_id>/<probe_id>_schema.json``
for human inspection.

Supported signal types
──────────────────────
schema_snapshot      Captures ``df.schema`` — zero Spark action.

row_count_estimate   Two methods:
  method: spark_listener   No action; queries ``module_metrics`` table in
                           ``observability.db`` for the upstream module's stage count.
                           Returns ``estimate: null`` if no row exists yet.
                           Note: Spark stage fusion can group multiple logical
                           modules into one stage — the estimate reflects the
                           fused stage's recordsWritten, not a per-module count.
                           Treat as an approximation, not exact attribution.
  method: sample           ``df.sample(fraction).count()`` — FULL DATASET SCAN.
                           sample() is a row-level filter, not a partition prune.
                           All data is read; (1-fraction) of rows are discarded
                           after I/O. Blocked when ``block_full_actions=True``.

null_rates           ``df.sample(fraction)`` → per-column null rates.
                     FULL DATASET SCAN — see row_count_estimate note above.
                     Blocked when ``block_full_actions=True``.

sample_rows          ``df.limit(n).collect()`` — fetches at most ``n`` rows.
                     Spark can satisfy LIMIT from the first partition(s) without
                     scanning the full dataset. Not blocked.

value_distribution   Min / max / mean / stddev / configurable percentiles per
                     column, computed on a sample (default fraction=0.1).
                     Only numeric-typed columns are included automatically when
                     ``columns`` is omitted.
                     Blocked when ``block_full_actions=True``.

distinct_count       Approximate distinct-value count per column via
                     ``approx_count_distinct``, computed on a sample
                     (default fraction=0.1).
                     Blocked when ``block_full_actions=True``.

data_freshness       ``max(column)`` — latest timestamp / date value in a
                     column. Computed on a sample (default fraction=0.0 = full
                     scan) because max from a sample can be inaccurate.
                     Blocked when ``block_full_actions=True`` unless
                     ``allow_sample=true`` is set (trades accuracy for safety).

partition_stats      Number of Spark partitions via ``df.rdd.getNumPartitions()``
                     — zero Spark action. Never blocked.

Config shape (YAML / dict)
──────────────────────────
type: Probe
attach_to: my_ingress
config:
  signals:
    - type: schema_snapshot

    - type: row_count_estimate
      method: sample       # sample | spark_listener (default: sample)
      fraction: 0.1

    - type: null_rates
      columns: [region, amount]   # omit → all columns
      fraction: 0.1

    - type: sample_rows
      n: 20

    - type: value_distribution
      columns: [amount, fare]     # omit → all numeric columns
      fraction: 0.1               # 0.0 = full scan
      percentiles: [0.25, 0.5, 0.75]   # default

    - type: distinct_count
      columns: [region, status]   # omit → all columns
      fraction: 0.1               # 0.0 = full scan

    - type: data_freshness
      column: event_time          # required; timestamp or date column
      allow_sample: false         # true = use fraction below (less accurate)
      fraction: 0.1               # only used when allow_sample: true

    - type: partition_stats
      # no config keys — always zero Spark action

    - type: threshold
      expr: "MAX(amount) > 0"       # SQL aggregate expression; must evaluate to boolean
      # Writes {"passed": true/false, "value": <result>, "expr": "<expr>"} to probe_signals.
      # This is the ONLY built-in signal type that produces a "passed" key — required for
      # Regulator gate evaluation. All other built-in signal types write raw metrics only.
      # One Spark action (df.agg(expr).collect()).

    - type: custom                  # Phase 60 — user-defined signal (exactly one form)
      # form 1 — inline SQL (declarative, no code):
      sql: "percentile(amount, 0.99)"   # value expression → "estimate"
      passed_when: "MAX(amount) < 1e6"  # optional boolean → "passed" (Regulator gate)
      # form 2 — module pointer (mirrors UDF contract; code lives in an importable pkg):
      # module: myorg.aq_probes
      # entry: p99_latency
      # form 3 — entry-point plugin (setuptools group 'aqueduct.probe_signals'):
      # plugin: p99_latency
      # Callable contract: fn(df, sig_cfg) -> {"estimate", "metadata", "passed"}.
      # SECURITY: callables run on the driver as code (UDF trust model). The
      # engine cannot enforce zero-cost — an inline-SQL/pointer callable that does
      # .collect()/.count() is on you. See custom_probe_driver_code warning.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from aqueduct.models import Module

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProbeSampling:
    max_sample_rows: int = 100
    default_sample_fraction: float = 0.1


# ── DuckDB DDL ────────────────────────────────────────────────────────────────

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


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _json_dumps(obj: Any) -> str:
    """json.dumps that coerces Spark-native types (datetime, Decimal, bytes)."""
    def _default(o: Any) -> Any:
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, bytes):
            return o.hex()
        return str(o)
    return json.dumps(obj, default=_default)


# ── Signal implementations ────────────────────────────────────────────────────

def _schema_snapshot(
    probe_id: str,
    run_id: str,
    df: DataFrame,
    store_dir: Path,
) -> dict[str, Any]:
    """Capture df.schema — zero Spark actions.

    Phase 53 — the payload is persisted only to the ``probe_signals``
    observability table (by ``execute_probe``); the former local
    ``snapshots/<run_id>/<probe>_schema.json`` sidecar was dropped so a driver
    pod on an ephemeral filesystem leaves no artefacts."""
    # df.schema is always available on a lazy DataFrame
    fields = [
        {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
        for f in df.schema.fields
    ]
    return {"fields": fields}


def _row_count_estimate(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    probe_id: str = "",
    run_id: str = "",
    store_dir: Path | None = None,
    block_full_actions: bool = False,
    observability_store: Any = None,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Estimate row count. sample method only triggers on a fraction.

    `observability_store` is the Phase 28 obs-store backend (DuckDB / Postgres). When
    None, a default DuckDB store is built from `store_dir/observability.db`.
    """
    method = signal_cfg.get("method", "sample")

    if method == "spark_listener":
        attach_to = signal_cfg.get("attach_to") or probe_id
        if observability_store is None and store_dir is not None:
            try:
                from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
                db_path = store_dir / "observability.db"
                if not db_path.exists():
                    return {"method": "spark_listener", "estimate": None}
                observability_store = DuckDBObservabilityStore(db_path)
            except Exception as exc:
                logger.debug("spark_listener row_count_estimate setup failed: %s", exc)
                return {"method": "spark_listener", "estimate": None}
        if observability_store is not None:
            try:
                with observability_store.connect() as cur:
                    row = cur.execute(
                        """
                        SELECT records_written, records_read
                        FROM module_metrics
                        WHERE run_id = ? AND module_id = ?
                        ORDER BY captured_at DESC LIMIT 1
                        """,
                        [run_id, attach_to],
                    ).fetchone()
                if row:
                    count = row[0] or row[1]
                    return {"method": "spark_listener", "estimate": count}
            except Exception as exc:
                logger.debug("spark_listener row_count_estimate query failed: %s", exc)
        return {"method": "spark_listener", "estimate": None}

    # method: sample
    if block_full_actions:
        logger.warning("Probe %r: block_full_actions=True; skipping row_count_estimate sample.", probe_id)
        return {"method": "sample", "blocked": True, "estimate": None}
    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))
    sample_count = df.sample(fraction=fraction).count()
    estimate = int(round(sample_count / fraction)) if fraction > 0 else 0
    return {"method": "sample", "fraction": fraction, "sample_count": sample_count, "estimate": estimate}


def _null_rates(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Compute per-column null rates on a random sample."""
    from pyspark.sql import functions as F

    columns: list[str] = signal_cfg.get("columns") or df.columns
    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))

    if block_full_actions:
        logger.warning("Probe: block_full_actions=True; skipping null_rates sample.")
        return {"fraction": fraction, "blocked": True, "null_rates": {c: None for c in columns}}

    sample_df = df.sample(fraction=fraction).select(columns)
    total = sample_df.count()

    if total == 0:
        return {"fraction": fraction, "sample_size": 0, "null_rates": {c: None for c in columns}}

    null_counts_row = sample_df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
    ).collect()[0].asDict()

    rates = {c: round(null_counts_row[c] / total, 6) for c in columns}
    return {"fraction": fraction, "sample_size": total, "null_rates": rates}


def _sample_rows(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Fetch at most n rows as JSON-serialisable dicts.

    Uses limit(n).collect() so Spark can satisfy the request from the first
    partition(s) without scanning the full dataset.
    """
    n = int(signal_cfg.get("n", 10))
    n = min(n, sampling.max_sample_rows)
    rows = df.limit(n).collect()
    serialised = [row.asDict(recursive=True) for row in rows]
    return {"n": n, "rows": serialised}


def _value_distribution(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Min/max/mean/stddev + percentiles per column on a sample."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType

    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))
    percentiles: list[float] = signal_cfg.get("percentiles", [0.25, 0.5, 0.75])

    if block_full_actions:
        logger.warning("Probe: block_full_actions=True; skipping value_distribution.")
        return {"blocked": True, "fraction": fraction, "stats": {}}

    # Default to numeric columns only when caller didn't specify
    requested: list[str] | None = signal_cfg.get("columns")
    if requested:
        columns = requested
    else:
        columns = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    if not columns:
        return {"fraction": fraction, "stats": {}}

    source = df.sample(fraction=fraction).select(columns) if fraction > 0 else df.select(columns)

    # Batch: one agg() for min/max/mean/stddev + one approxQuantile call
    agg_exprs = []
    for c in columns:
        agg_exprs += [
            F.min(c).alias(f"_min_{c}"),
            F.max(c).alias(f"_max_{c}"),
            F.mean(c).alias(f"_mean_{c}"),
            F.stddev_samp(c).alias(f"_std_{c}"),
            F.count(c).alias(f"_cnt_{c}"),
        ]
    agg_row = source.agg(*agg_exprs).collect()[0].asDict()

    # approxQuantile is a separate driver call (no Spark action, uses quantile summary)
    quantiles: dict[str, list[float]] = {}
    if percentiles:
        for c in columns:
            try:
                quantiles[c] = source.approxQuantile(c, percentiles, 0.01)
            except Exception:
                quantiles[c] = []

    stats: dict[str, Any] = {}
    for c in columns:
        stats[c] = {
            "min": agg_row.get(f"_min_{c}"),
            "max": agg_row.get(f"_max_{c}"),
            "mean": agg_row.get(f"_mean_{c}"),
            "stddev": agg_row.get(f"_std_{c}"),
            "count_non_null": agg_row.get(f"_cnt_{c}"),
            "percentiles": dict(zip([str(p) for p in percentiles], quantiles.get(c, []))),
        }

    return {"fraction": fraction, "stats": stats}


def _distinct_count(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Approximate distinct-value count per column via approx_count_distinct."""
    from pyspark.sql import functions as F

    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))
    columns: list[str] = signal_cfg.get("columns") or df.columns

    if block_full_actions:
        logger.warning("Probe: block_full_actions=True; skipping distinct_count.")
        return {"blocked": True, "fraction": fraction, "distinct_counts": {c: None for c in columns}}

    source = df.sample(fraction=fraction).select(columns) if fraction > 0 else df.select(columns)
    agg_exprs = [F.approx_count_distinct(c).alias(c) for c in columns]
    row = source.agg(*agg_exprs).collect()[0].asDict()
    return {"fraction": fraction, "distinct_counts": {c: row[c] for c in columns}}


def _data_freshness(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
    sampling: ProbeSampling = ProbeSampling(),
) -> dict[str, Any]:
    """Capture the max value of a timestamp/date column."""
    from pyspark.sql import functions as F

    column: str | None = signal_cfg.get("column")
    if not column:
        raise ValueError("data_freshness signal requires 'column'")

    allow_sample = bool(signal_cfg.get("allow_sample", False))
    fraction = float(signal_cfg.get("fraction", sampling.default_sample_fraction))

    if block_full_actions and not allow_sample:
        logger.warning(
            "Probe: block_full_actions=True; skipping data_freshness for column=%r. "
            "Set allow_sample: true to use a sample instead.",
            column,
        )
        return {"blocked": True, "column": column}

    source = df.sample(fraction=fraction) if (allow_sample and fraction > 0) else df
    max_val = source.select(F.max(F.col(column)).alias("max_val")).collect()[0]["max_val"]
    return {
        "column": column,
        "max_value": max_val,
        "sampled": allow_sample and fraction > 0,
        "fraction": fraction if (allow_sample and fraction > 0) else None,
    }


def _partition_stats(df: DataFrame) -> dict[str, Any]:
    """Capture Spark partition count — zero Spark action."""
    return {"num_partitions": df.rdd.getNumPartitions()}


def _custom(
    df: DataFrame,
    sig_cfg: dict[str, Any],
    block_full_actions: bool = False,
) -> dict[str, Any]:
    """Execute a user-defined custom probe signal (Phase 60).

    Three forms (resolved by ``executor/probe_plugins.py``, exactly one):

    * **inline SQL** — evaluate ``sql`` (a value expression → ``estimate``)
      and/or ``passed_when`` (a boolean expression → ``passed`` gate verdict).
      Each is a single ``df.selectExpr(...).collect()`` action.
    * **module pointer / entry-point plugin** — resolve a callable and invoke
      ``fn(df, sig_cfg)``; it must return a dict (``estimate``/``metadata``/
      ``passed``). ``block_full_actions`` is surfaced into the passed config so
      well-behaved plugins can honour the zero-cost contract — but the engine
      cannot enforce what arbitrary driver code does.

    The returned payload always carries ``"custom": True``. A ``passed`` key (SQL
    ``passed_when`` or returned by a callable) is read by the Regulator gate just
    like the built-in ``threshold`` signal.
    """
    from aqueduct.executor.probe_plugins import custom_signal_source, resolve_callable

    source = custom_signal_source(sig_cfg)

    if source == "sql":
        out: dict[str, Any] = {"custom": True}
        sql_expr = sig_cfg.get("sql")
        if sql_expr:
            out["estimate"] = df.selectExpr(sql_expr).collect()[0][0]
        passed_when = sig_cfg.get("passed_when")
        if passed_when:
            result = df.selectExpr(passed_when).collect()[0][0]
            out["passed"] = bool(result) if result is not None else False
        return out

    # callable form (pointer or plugin)
    fn = resolve_callable(sig_cfg)
    call_cfg = {**sig_cfg, "block_full_actions": block_full_actions}
    result = fn(df, call_cfg)
    if not isinstance(result, dict):
        raise ValueError(
            f"custom probe callable must return a dict, got {type(result).__name__}"
        )
    return {"custom": True, **result}


def _threshold(df: DataFrame, sig_cfg: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a SQL aggregate expression and write a boolean 'passed' verdict.

    This is the only signal type that produces {"passed": bool} — the key that
    evaluate_regulator() looks for when deciding whether to open a Regulator gate.

    One Spark action: df.selectExpr(expr).collect()[0][0].
    The expression must evaluate to a truthy/falsy scalar (e.g. "MAX(amount) > 0",
    "COUNT(*) >= 1000", "SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) > 0").
    """

    expr_str = sig_cfg.get("expr", "")
    if not expr_str:
        raise ValueError("threshold signal requires an 'expr' field")

    result = df.selectExpr(expr_str).collect()[0][0]
    passed = bool(result) if result is not None else False
    return {"passed": passed, "value": result, "expr": expr_str}


# ── Public API ────────────────────────────────────────────────────────────────

def execute_probe(
    module: Module,
    df: DataFrame,
    spark: SparkSession,  # noqa: ARG001 — reserved for SparkListener wiring
    run_id: str,
    store_dir: Path,
    block_full_actions: bool = False,
    observability_store: Any = None,
    sampling: ProbeSampling = ProbeSampling(),
) -> None:
    """Capture observability signals for a single Probe module.

    Writes one row per signal to the configured observability store
    (``store_dir/observability.db`` for DuckDB by default; Postgres `obs.probe_signals`
    when wired through Phase 28's `StoreBundle`).
    Every signal — including ``schema_snapshot`` — is persisted to
    ``probe_signals`` only (Phase 53 dropped the local ``snapshots/`` sidecar).

    Args:
        module:    The Probe Module from the compiled Manifest.
        df:        Lazy DataFrame produced by the module this Probe taps.
        spark:     Active SparkSession (reserved for SparkListener extension).
        run_id:    Run identifier from the Executor.
        store_dir: Root observability store directory (used for snapshots/
                   and for the DuckDB fallback path).
        block_full_actions: Forward to per-signal helpers.
        observability_store: Optional Phase 28 obs-store backend. When None, a default
                   DuckDB store at ``store_dir/observability.db`` is constructed.
        sampling: Probe sampling governance (max_sample_rows cap + default_sample_fraction).

    Raises:
        Nothing — all exceptions are caught and logged.  Probe failure must
        never halt the blueprint.
    """
    try:
        signals: list[dict[str, Any]] = module.config.get("signals", [])
        if not signals:
            logger.debug("Probe %r has no signals configured; skipping.", module.id)
            return

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
                        payload = _schema_snapshot(module.id, run_id, df, store_dir)
                    elif sig_type == "row_count_estimate":
                        payload = _row_count_estimate(
                            df, sig_cfg,
                            probe_id=module.attach_to or module.id,
                            run_id=run_id,
                            store_dir=store_dir,
                            block_full_actions=block_full_actions,
                            observability_store=observability_store,
                            sampling=sampling,
                        )
                    elif sig_type == "null_rates":
                        payload = _null_rates(df, sig_cfg, block_full_actions=block_full_actions, sampling=sampling)
                    elif sig_type == "sample_rows":
                        payload = _sample_rows(df, sig_cfg, sampling=sampling)
                    elif sig_type == "value_distribution":
                        payload = _value_distribution(df, sig_cfg, block_full_actions=block_full_actions, sampling=sampling)
                    elif sig_type == "distinct_count":
                        payload = _distinct_count(df, sig_cfg, block_full_actions=block_full_actions, sampling=sampling)
                    elif sig_type == "data_freshness":
                        payload = _data_freshness(df, sig_cfg, block_full_actions=block_full_actions, sampling=sampling)
                    elif sig_type == "partition_stats":
                        payload = _partition_stats(df)
                    elif sig_type == "threshold":
                        payload = _threshold(df, sig_cfg)
                    elif sig_type == "custom":
                        payload = _custom(df, sig_cfg, block_full_actions=block_full_actions)
                    else:
                        logger.warning("Probe %r: unknown signal type %r; skipping.", module.id, sig_type)
                        continue

                    cur.execute(
                        """
                        INSERT INTO probe_signals
                            (run_id, probe_id, signal_type, payload, captured_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [run_id, module.id, sig_type, _json_dumps(payload), _utcnow_iso()],
                    )
                except Exception as exc:
                    logger.warning(
                        "Probe %r signal %r failed: %s", module.id, sig_type, exc
                    )
    except Exception as exc:
        logger.warning("execute_probe %r: unexpected error: %s", module.id, exc)
