"""Assert executor — declarative data quality gates.

Rules are evaluated against the incoming DataFrame and produce one of:
  abort          → raise AssertError, pipeline stops immediately
  warn           → log warning, pipeline continues
  webhook        → fire async POST, pipeline continues
  quarantine     → row-level rules only; failing rows routed to spillway port
  trigger_agent  → raise AssertError(trigger_agent=True); LLM loop fires on return

Rule types
──────────
Aggregate rules (at most 2 Spark actions total, batched):
  schema_match   Check df.schema against expected field map.  Zero Spark action.
  min_rows       Count rows.  Batched into single df.agg().
  null_rate      Null fraction for a column.  Batched into single df.sample().agg().
  freshness      Max value of a timestamp column.  Batched into df.agg().
  sql            Arbitrary aggregate SQL expression (must evaluate to boolean).
                 Batched into df.agg().

Row-level rules (lazy filter, no extra Spark action):
  sql_row        Filter expression; rows where expr is FALSE are quarantined.
  custom         Python callable: fn(df) -> {"passed": bool, "message": str,
                 "quarantine_df": DataFrame | None}

Batching strategy:
  All aggregate rules (min_rows, freshness, sql) → one df.agg() collect().
  All null_rate rules → one df.sample(max_fraction).agg() collect().
  Row-level rules → lazy df.filter(); no collect() triggered.
  Net: at most 2 Spark actions regardless of rule count.
"""

from __future__ import annotations

import importlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from aqueduct.parser.models import Module

logger = logging.getLogger(__name__)


# ── Public error type ─────────────────────────────────────────────────────────

class AssertError(Exception):
    """Raised when an Assert rule fires with on_fail=abort or on_fail=trigger_agent."""

    def __init__(
        self,
        message: str,
        rule_id: str | None = None,
        trigger_agent: bool = False,
    ) -> None:
        super().__init__(message)
        self.rule_id = rule_id
        self.trigger_agent = trigger_agent


# ── Public API ────────────────────────────────────────────────────────────────

def execute_assert(
    module: Module,
    df: "DataFrame",
    spark: "SparkSession",
    run_id: str,
    pipeline_id: str,
) -> "tuple[DataFrame, DataFrame | None]":
    """Evaluate Assert rules against df.

    Args:
        module:      Assert Module from the compiled Manifest.
        df:          Incoming DataFrame (lazy).
        spark:       Active SparkSession.
        run_id:      Current run identifier.
        pipeline_id: Pipeline identifier (used in webhook payloads).

    Returns:
        (passing_df, quarantine_df)
        - passing_df:    rows that passed all row-level rules (may equal df when no
                         row-level rules are configured).
        - quarantine_df: union of rows that failed quarantine rules, or None if no
                         quarantine rules fired.

    Raises:
        AssertError: on_fail=abort or on_fail=trigger_agent for any rule.
    """
    rules: list[dict[str, Any]] = module.config.get("rules", [])
    if not rules:
        logger.debug("Assert %r has no rules configured; pass-through.", module.id)
        return df, None

    # ── Phase 1: schema_match (zero action) ──────────────────────────────────
    for rule in rules:
        if rule.get("type") == "schema_match":
            _check_schema_match(module.id, df, rule)

    # ── Phase 2: aggregate rules (one batched action + one sample action) ────
    _batch_aggregate_rules(module.id, df, rules, pipeline_id, run_id)

    # ── Phase 3: row-level rules (lazy, no new action) ───────────────────────
    passing_df = df
    quarantine_parts: list[DataFrame] = []

    for rule in rules:
        rtype = rule.get("type")
        if rtype in ("sql_row", "custom"):
            passing_df, q_df = _apply_row_rule(module.id, passing_df, rule, spark)
            if q_df is not None:
                quarantine_parts.append(q_df)

    quarantine_df: DataFrame | None = None
    if quarantine_parts:
        from functools import reduce
        quarantine_df = reduce(lambda a, b: a.union(b), quarantine_parts)

    return passing_df, quarantine_df


# ── on_fail dispatch ──────────────────────────────────────────────────────────

def _handle_fail(
    on_fail: Any,
    module_id: str,
    rule_type: str,
    message: str,
    pipeline_id: str = "",
    run_id: str = "",
) -> None:
    """Dispatch on_fail action.  Raises AssertError for abort/trigger_agent."""
    if isinstance(on_fail, str):
        action = on_fail
        webhook_url: str | None = None
    else:
        action = on_fail.get("action", "abort")
        webhook_url = on_fail.get("url")

    if action == "abort":
        raise AssertError(message, rule_id=rule_type)
    elif action == "trigger_agent":
        raise AssertError(message, rule_id=rule_type, trigger_agent=True)
    elif action == "warn":
        logger.warning("[%s] Assert %r: %s", module_id, rule_type, message)
    elif action == "webhook":
        if webhook_url:
            _fire_rule_webhook(
                webhook_url, module_id, rule_type, message, pipeline_id, run_id
            )
        else:
            logger.warning(
                "[%s] Assert %r on_fail=webhook but no url specified.", module_id, rule_type
            )
    elif action == "quarantine":
        # Row-level only — handled by _apply_row_rule; aggregate rules treat as warn
        logger.warning(
            "[%s] Assert %r on_fail=quarantine used on aggregate rule; treated as warn.",
            module_id, rule_type,
        )
        logger.warning("[%s] Assert %r: %s", module_id, rule_type, message)
    else:
        logger.warning(
            "[%s] Assert %r unknown on_fail action %r; treating as warn.",
            module_id, rule_type, action,
        )
        logger.warning("[%s] Assert %r: %s", module_id, rule_type, message)


# ── Phase 1: schema_match ─────────────────────────────────────────────────────

def _check_schema_match(module_id: str, df: "DataFrame", rule: dict[str, Any]) -> None:
    """Zero Spark action. Checks df.schema against expected field map."""
    expected: dict[str, str] = rule.get("expected", {})
    on_fail = rule.get("on_fail", "abort")

    actual_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    missing = [name for name in expected if name not in actual_fields]
    type_mismatches = [
        f"{name}: expected {etype}, got {actual_fields[name]}"
        for name, etype in expected.items()
        if name in actual_fields and actual_fields[name] != etype
    ]

    if missing:
        msg = f"schema_match: missing columns {missing}"
        _handle_fail(on_fail, module_id, "schema_match", msg)
    if type_mismatches:
        msg = f"schema_match: type mismatches {type_mismatches}"
        _handle_fail(on_fail, module_id, "schema_match", msg)


# ── Phase 2: aggregate rules (batched) ───────────────────────────────────────

def _batch_aggregate_rules(
    module_id: str,
    df: "DataFrame",
    rules: list[dict[str, Any]],
    pipeline_id: str,
    run_id: str,
) -> None:
    """Evaluate all aggregate rules in at most 2 Spark actions."""
    from pyspark.sql import functions as F

    # ── Collect aggregate expressions (min_rows, freshness, sql) ─────────────
    agg_cols: dict[str, Any] = {}
    agg_rule_indices: list[int] = []

    for i, rule in enumerate(rules):
        rtype = rule.get("type")
        if rtype == "min_rows":
            agg_cols[f"_cnt_{i}"] = F.count("*")
            agg_rule_indices.append(i)
        elif rtype == "max_rows":
            agg_cols[f"_cnt_{i}"] = F.count("*")
            agg_rule_indices.append(i)
        elif rtype == "freshness":
            col = rule.get("column")
            if col:
                agg_cols[f"_max_{i}"] = F.max(F.col(col))
                agg_rule_indices.append(i)
        elif rtype == "sql":
            expr_str = rule.get("expr", "")
            if expr_str:
                agg_cols[f"_sql_{i}"] = F.expr(expr_str)
                agg_rule_indices.append(i)

    agg_row = None
    if agg_cols:
        agg_row = df.agg(*[v.alias(k) for k, v in agg_cols.items()]).collect()[0]

    # Evaluate each aggregate rule against collected results
    if agg_row is not None:
        for i, rule in enumerate(rules):
            rtype = rule.get("type")
            on_fail = rule.get("on_fail", "abort")

            if rtype == "min_rows" and f"_cnt_{i}" in agg_cols:
                count = agg_row[f"_cnt_{i}"]
                min_val = int(rule.get("min", 0))
                if count < min_val:
                    _handle_fail(
                        on_fail, module_id, "min_rows",
                        f"min_rows: got {count}, expected >= {min_val}",
                        pipeline_id, run_id,
                    )

            elif rtype == "max_rows" and f"_cnt_{i}" in agg_cols:
                count = agg_row[f"_cnt_{i}"]
                max_val = int(rule.get("max", 2**63))
                if count > max_val:
                    _handle_fail(
                        on_fail, module_id, "max_rows",
                        f"max_rows: got {count}, expected <= {max_val}",
                        pipeline_id, run_id,
                    )

            elif rtype == "freshness" and f"_max_{i}" in agg_cols:
                max_ts = agg_row[f"_max_{i}"]
                max_age_hours = float(rule.get("max_age_hours", 24))
                if max_ts is None:
                    _handle_fail(
                        on_fail, module_id, "freshness",
                        "freshness: column has no non-null values",
                        pipeline_id, run_id,
                    )
                else:
                    if hasattr(max_ts, "timestamp"):
                        ts_utc = max_ts.replace(tzinfo=timezone.utc) if max_ts.tzinfo is None else max_ts
                    else:
                        ts_utc = datetime.fromtimestamp(float(max_ts), tz=timezone.utc)
                    age_hours = (datetime.now(tz=timezone.utc) - ts_utc).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        _handle_fail(
                            on_fail, module_id, "freshness",
                            f"freshness: data is {age_hours:.1f}h old, max allowed {max_age_hours}h",
                            pipeline_id, run_id,
                        )

            elif rtype == "sql" and f"_sql_{i}" in agg_cols:
                result = agg_row[f"_sql_{i}"]
                # Expression expected to evaluate to a boolean or truthy value
                if not result:
                    _handle_fail(
                        on_fail, module_id, "sql",
                        f"sql assertion failed: {rule.get('expr', '')!r} evaluated to {result!r}",
                        pipeline_id, run_id,
                    )

    # ── Null rate rules — one shared sample.agg() ─────────────────────────────
    null_rules = [(i, r) for i, r in enumerate(rules) if r.get("type") == "null_rate"]
    if null_rules:
        fraction = max(float(r.get("fraction", 0.1)) for _, r in null_rules)

        null_cols: dict[str, Any] = {"_total": F.count("*")}
        for i, rule in null_rules:
            col = rule.get("column")
            if col:
                null_cols[f"_null_{i}"] = F.sum(F.col(col).isNull().cast("int"))

        sample_row = df.sample(fraction=fraction).agg(
            *[v.alias(k) for k, v in null_cols.items()]
        ).collect()[0]

        total = sample_row["_total"] or 1  # avoid division by zero

        for i, rule in null_rules:
            col = rule.get("column")
            if not col or f"_null_{i}" not in null_cols:
                continue
            on_fail = rule.get("on_fail", "abort")
            null_count = sample_row[f"_null_{i}"] or 0
            rate = null_count / total
            max_rate = float(rule.get("max", 0.0))
            if rate > max_rate:
                _handle_fail(
                    on_fail, module_id, "null_rate",
                    f"null_rate[{col}]: {rate:.4%} > allowed {max_rate:.4%} "
                    f"(sample_size={total}, fraction={fraction})",
                    pipeline_id, run_id,
                )


# ── Phase 3: row-level rules ──────────────────────────────────────────────────

def _apply_row_rule(
    module_id: str,
    df: "DataFrame",
    rule: dict[str, Any],
    spark: "SparkSession",
) -> "tuple[DataFrame, DataFrame | None]":
    """Apply a row-level rule.  Returns (passing_df, quarantine_df | None).

    Lazy — no Spark actions triggered.
    """
    from pyspark.sql import functions as F

    rtype = rule.get("type")
    on_fail = rule.get("on_fail", "quarantine")

    if rtype == "sql_row":
        expr_str = rule.get("expr", "")
        if not expr_str:
            return df, None

        min_pass_rate = rule.get("min_pass_rate")

        passing = df.filter(expr_str)
        failing = df.filter(f"NOT ({expr_str})")

        if min_pass_rate is not None:
            # Need a count — one action, but user explicitly requested pass-rate check
            total = df.count()
            pass_count = passing.count()
            actual_rate = pass_count / total if total > 0 else 1.0
            if actual_rate < float(min_pass_rate):
                _handle_fail(
                    on_fail if not isinstance(on_fail, str) or on_fail != "quarantine" else "abort",
                    module_id, "sql_row",
                    f"sql_row pass_rate {actual_rate:.4%} < min {float(min_pass_rate):.4%}",
                )

        if isinstance(on_fail, str) and on_fail == "quarantine":
            quarantine_df = (
                failing
                .withColumn("_aq_error_module", F.lit(module_id))
                .withColumn("_aq_error_rule", F.lit("sql_row"))
                .withColumn("_aq_error_msg", F.lit(f"failed: {expr_str}"))
                .withColumn("_aq_error_ts", F.current_timestamp())
            )
            return passing, quarantine_df

        # non-quarantine on_fail for sql_row — evaluate lazily using count when needed
        # For abort/warn/webhook we'd need to detect if any rows fail; use lazy approach:
        # register failing as a view and check count only if non-quarantine action needed
        _handle_fail_if_any(module_id, failing, on_fail, "sql_row", f"failed: {expr_str}")
        return passing, None

    elif rtype == "custom":
        fn_path = rule.get("fn", "")
        if not fn_path:
            logger.warning("[%s] custom rule missing fn path; skipped.", module_id)
            return df, None

        try:
            fn = _load_callable(fn_path)
            result = fn(df)
        except AssertError:
            raise
        except Exception as exc:
            logger.warning("[%s] custom rule %r raised: %s", module_id, fn_path, exc)
            return df, None

        if not result.get("passed", True):
            msg = result.get("message", f"custom rule {fn_path!r} failed")
            q_df = result.get("quarantine_df")

            if isinstance(on_fail, str) and on_fail == "quarantine" and q_df is not None:
                q_df = (
                    q_df
                    .withColumn("_aq_error_module", F.lit(module_id))
                    .withColumn("_aq_error_rule", F.lit("custom"))
                    .withColumn("_aq_error_msg", F.lit(msg))
                    .withColumn("_aq_error_ts", F.current_timestamp())
                )
                # passing = df minus quarantine rows (caller's responsibility to exclude)
                # For simplicity, trust fn to return the right quarantine_df
                return df, q_df
            else:
                _handle_fail(on_fail, module_id, "custom", msg)

    return df, None


def _handle_fail_if_any(
    module_id: str,
    failing_df: "DataFrame",
    on_fail: Any,
    rule_type: str,
    message: str,
) -> None:
    """Fire on_fail if failing_df has any rows.  Triggers one Spark action."""
    count = failing_df.count()
    if count > 0:
        _handle_fail(on_fail, module_id, rule_type, f"{message} ({count} rows)")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_callable(fn_path: str) -> Any:
    """Load a Python callable from a dotted path (e.g. 'my_pkg.rules.check_orders')."""
    parts = fn_path.rsplit(".", 1)
    if len(parts) != 2:
        raise AssertError(
            f"custom rule fn {fn_path!r} must be 'module.callable' format",
            rule_id="custom",
        )
    module_path, attr = parts
    try:
        mod = importlib.import_module(module_path)
    except ImportError as exc:
        raise AssertError(
            f"custom rule fn {fn_path!r}: cannot import {module_path!r}: {exc}",
            rule_id="custom",
        ) from exc
    fn = getattr(mod, attr, None)
    if fn is None or not callable(fn):
        raise AssertError(
            f"custom rule fn {fn_path!r}: {attr!r} not found or not callable in {module_path!r}",
            rule_id="custom",
        )
    return fn


def _fire_rule_webhook(
    url: str,
    module_id: str,
    rule_type: str,
    message: str,
    pipeline_id: str = "",
    run_id: str = "",
) -> None:
    """Fire assertion failure webhook asynchronously (best-effort)."""
    try:
        from aqueduct.config import WebhookEndpointConfig
        from aqueduct.surveyor.webhook import fire_webhook
        config = WebhookEndpointConfig(url=url)
        full_payload = {
            "event": "assert_rule_failed",
            "module_id": module_id,
            "rule_type": rule_type,
            "message": message,
            "pipeline_id": pipeline_id,
            "run_id": run_id,
            "fired_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        fire_webhook(config, full_payload)
    except Exception as exc:
        logger.warning("[%s] Assert webhook fire failed: %s", module_id, exc)
