"""Phase 29b — Gate 4: post-patch physical-plan regression check.

Sits next to the lineage and sandbox gates in
`aqueduct/patch/preview.py`. After the sandbox gate passes, the patched Manifest is
compiled and `df.explain(mode="formatted")` is captured per module. Counts
of `Exchange`, `BatchEvalPython`, and `BroadcastExchange` nodes are compared
against the pre-patch baseline stored in `observability.explain_snapshot`.

Warn-only by default. Aggressive mode may block on regression via the
`agent.block_on_explain_regression` knob.

Plan extraction uses the `_jdf.queryExecution().explainString()` py4j call —
same pattern as `aqueduct/executor/spark/metrics.py:_hadoop_fs_bytes` — so we
get the formatted plan as a string without redirecting stdout. Spark 3.0–4.x
stable.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExplainRegression:
    """A single Gate 4 finding for one module."""

    module_id: str
    metric: str            # "exchange" | "python_udf" | "broadcast"
    before: int
    after: int
    detail: str


@dataclass
class ExplainGateResult:
    status: str = "pass"   # "pass" | "warn" | "fail" | "skip"
    regressions: list[ExplainRegression] = field(default_factory=list)
    detail: str = ""
    duration_ms: int = 0
    baseline_run_id: str | None = None


# ── Plan extraction ───────────────────────────────────────────────────────────

def _formatted_plan(df: Any) -> str:
    """Return `df.explain(mode='formatted')` text via py4j (no stdout capture)."""
    try:
        jdf = df._jdf
        qe = jdf.queryExecution()
        # Prefer .sparkSession — accessing .sql_ctx (even via hasattr) triggers
        # a pyspark Deprecation/UserWarning and breaks when pyspark removes it.
        spark = df.sparkSession if hasattr(df, "sparkSession") else df.sql_ctx.sparkSession
        jvm = spark._jvm
        formatted = jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
        return qe.explainString(formatted)
    except Exception as exc:
        logger.debug("explain capture failed: %s", exc)
        try:
            return df._jdf.queryExecution().toString()
        except Exception:
            return ""


def _count_markers(plan: str) -> tuple[int, int, int]:
    """Return `(exchange, python_udf, broadcast)` counts from a formatted plan."""
    if not plan:
        return (0, 0, 0)
    return (
        plan.count("Exchange "),
        plan.count("BatchEvalPython"),
        plan.count("BroadcastExchange"),
    )


def capture_plan_snapshot(df: Any) -> dict[str, Any]:
    """Capture per-module plan signal — returns dict ready for Surveyor."""
    plan = _formatted_plan(df)
    exch, udf, bcast = _count_markers(plan)
    return {
        "exchange_count": exch,
        "python_udf_count": udf,
        "broadcast_count": bcast,
        "plan_text": plan,
    }


# ── explain gate ────────────────────────────────────────────────────────────────────

def run_explain_gate(
    baseline_by_module: dict[str, dict],
    after_by_module: dict[str, dict],
    *,
    touched_modules: list[str] | None = None,
) -> ExplainGateResult:
    """Compare post-patch plan counts against pre-patch baseline.

    Args:
        baseline_by_module:  Output of `Surveyor.latest_explain_snapshots()`
                             — `{module_id: {exchange_count, python_udf_count,
                             broadcast_count, plan_text, run_id, ...}}`.
        after_by_module:     Same shape, captured from the patched Manifest
                             after a sandbox/compile run.
        touched_modules:     Optional restriction — only diff these modules.
                             Default = all modules present in both maps.

    Status:
      `pass`  no regression on any compared module
      `warn`  at least one metric increased (exchange/udf) or broadcast lost
      `skip`  no baseline rows present — first run after Phase 29b deploy,
              caller should not block on this.
    """
    t0 = time.monotonic()
    result = ExplainGateResult()

    if not baseline_by_module:
        result.status = "skip"
        result.detail = "no pre-patch explain_snapshot rows; baseline not yet established"
        result.duration_ms = int((time.monotonic() - t0) * 1000)
        return result

    modules = touched_modules or sorted(set(baseline_by_module) & set(after_by_module))
    baseline_run_ids: set[str] = set()

    for mid in modules:
        before = baseline_by_module.get(mid)
        after = after_by_module.get(mid)
        if not before or not after:
            continue
        if before.get("run_id"):
            baseline_run_ids.add(before["run_id"])

        b_exch = int(before.get("exchange_count") or 0)
        b_udf = int(before.get("python_udf_count") or 0)
        b_bcast = int(before.get("broadcast_count") or 0)
        a_exch = int(after.get("exchange_count") or 0)
        a_udf = int(after.get("python_udf_count") or 0)
        a_bcast = int(after.get("broadcast_count") or 0)

        if a_exch > b_exch:
            result.regressions.append(ExplainRegression(
                module_id=mid, metric="exchange",
                before=b_exch, after=a_exch,
                detail=f"module {mid!r} gained {a_exch - b_exch} shuffle node(s)",
            ))
        if a_udf > b_udf:
            result.regressions.append(ExplainRegression(
                module_id=mid, metric="python_udf",
                before=b_udf, after=a_udf,
                detail=f"module {mid!r} gained {a_udf - b_udf} Python UDF node(s)",
            ))
        if a_bcast < b_bcast:
            result.regressions.append(ExplainRegression(
                module_id=mid, metric="broadcast",
                before=b_bcast, after=a_bcast,
                detail=f"module {mid!r} lost {b_bcast - a_bcast} broadcast hint(s)",
            ))

    if result.regressions:
        result.status = "warn"
        result.detail = f"{len(result.regressions)} plan regression(s) detected"
    else:
        result.detail = f"no plan regression across {len(modules)} module(s)"

    if baseline_run_ids:
        result.baseline_run_id = sorted(baseline_run_ids)[0]
    result.duration_ms = int((time.monotonic() - t0) * 1000)
    return result
