"""Probe executor — captures observability signals from a lazy DataFrame.

Signals are written to DuckDB (``store_dir/signals.db``) so the Surveyor
can serve them via ``get_probe_signal()``.  For ``schema_snapshot`` a JSON
file is also written to ``store_dir/signals/<run_id>/<probe_id>_schema.json``
for human inspection.

Supported signal types
──────────────────────
schema_snapshot      Captures ``df.schema`` (zero Spark action — schema is
                     always available on a lazy DataFrame).

row_count_estimate   Two methods:
  method: spark_listener   No action; queries ``module_metrics`` table in
                           ``signals.db`` for the upstream module's stage count.
                           Returns ``estimate: null`` if no row exists yet.
  method: sample           ``df.sample(fraction).count()`` — allowed since
                           it operates on a fraction, not the full dataset.

null_rates           ``df.sample(fraction)`` → collect per-column null counts
                     → compute percentages.  Never touches the full DataFrame.

sample_rows          ``df.take(n)`` — fetches at most ``n`` rows.

Config shape (YAML / dict)
──────────────────────────
type: Probe
attach_to: my_ingress
config:
  signals:
    - type: schema_snapshot
    - type: row_count_estimate
      method: sample
      fraction: 0.1
    - type: null_rates
      columns: [region, amount]
      fraction: 0.1
    - type: sample_rows
      n: 20
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from aqueduct.parser.models import Module

logger = logging.getLogger(__name__)

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
    return datetime.now(tz=timezone.utc).isoformat()


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
    """Capture df.schema — zero Spark actions."""
    # df.schema is always available on a lazy DataFrame
    fields = [
        {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
        for f in df.schema.fields
    ]
    payload: dict[str, Any] = {"fields": fields}

    # Write human-readable JSON file too
    sig_dir = store_dir / "signals" / run_id
    sig_dir.mkdir(parents=True, exist_ok=True)
    (sig_dir / f"{probe_id}_schema.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    return payload


def _row_count_estimate(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    probe_id: str = "",
    run_id: str = "",
    store_dir: "Path | None" = None,
    block_full_actions: bool = False,
) -> dict[str, Any]:
    """Estimate row count. sample method only triggers on a fraction."""
    method = signal_cfg.get("method", "sample")

    if method == "spark_listener":
        # Query the module_metrics row written by the executor for this module's upstream.
        # The attach_to module_id is the same as probe_id's attach_to — passed via store_dir.
        attach_to = signal_cfg.get("attach_to") or probe_id
        if store_dir:
            try:
                import duckdb
                db_path = store_dir / "signals.db"
                if db_path.exists():
                    conn = duckdb.connect(str(db_path))
                    try:
                        row = conn.execute(
                            """
                            SELECT records_written, records_read
                            FROM module_metrics
                            WHERE run_id = ? AND module_id = ?
                            ORDER BY captured_at DESC LIMIT 1
                            """,
                            [run_id, attach_to],
                        ).fetchone()
                    finally:
                        conn.close()
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
    fraction = float(signal_cfg.get("fraction", 0.1))
    sample_count = df.sample(fraction=fraction).count()
    estimate = int(round(sample_count / fraction)) if fraction > 0 else 0
    return {"method": "sample", "fraction": fraction, "sample_count": sample_count, "estimate": estimate}


def _null_rates(
    df: DataFrame,
    signal_cfg: dict[str, Any],
    block_full_actions: bool = False,
) -> dict[str, Any]:
    """Compute per-column null rates on a random sample."""
    from pyspark.sql import functions as F

    columns: list[str] = signal_cfg.get("columns") or df.columns
    fraction = float(signal_cfg.get("fraction", 0.1))

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
) -> dict[str, Any]:
    """Fetch at most n rows as JSON-serialisable dicts."""
    n = int(signal_cfg.get("n", 10))
    rows = df.take(n)
    serialised = [row.asDict(recursive=True) for row in rows]
    return {"n": n, "rows": serialised}


# ── Public API ────────────────────────────────────────────────────────────────

def execute_probe(
    module: Module,
    df: DataFrame,
    spark: SparkSession,  # noqa: ARG001 — reserved for SparkListener wiring
    run_id: str,
    store_dir: Path,
    block_full_actions: bool = False,
) -> None:
    """Capture observability signals for a single Probe module.

    Writes one DuckDB row per signal to ``store_dir/signals.db``.
    For ``schema_snapshot``, also writes a JSON file.

    Args:
        module:    The Probe Module from the compiled Manifest.
        df:        Lazy DataFrame produced by the module this Probe taps.
        spark:     Active SparkSession (reserved for SparkListener extension).
        run_id:    Run identifier from the Executor.
        store_dir: Root observability store directory.

    Raises:
        Nothing — all exceptions are caught and logged.  Probe failure must
        never halt the pipeline.
    """
    try:
        import duckdb

        signals: list[dict[str, Any]] = module.config.get("signals", [])
        if not signals:
            logger.debug("Probe %r has no signals configured; skipping.", module.id)
            return

        store_dir.mkdir(parents=True, exist_ok=True)
        db_path = store_dir / "signals.db"

        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(_DDL)

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
                        )
                    elif sig_type == "null_rates":
                        payload = _null_rates(df, sig_cfg, block_full_actions=block_full_actions)
                    elif sig_type == "sample_rows":
                        payload = _sample_rows(df, sig_cfg)
                    else:
                        logger.warning("Probe %r: unknown signal type %r; skipping.", module.id, sig_type)
                        continue

                    conn.execute(
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
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("execute_probe %r: unexpected error: %s", module.id, exc)
