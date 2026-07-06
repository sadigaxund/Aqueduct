"""Assert executor — declarative data quality gates.

Rules are evaluated against the incoming DataFrame and produce one of:
  abort          → raise AssertError, blueprint stops immediately
  warn           → log warning, blueprint continues
  webhook        → fire async POST, blueprint continues
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

Post-row-level rules (evaluated after quarantine is known):
  spillway_rate  Max fraction of rows allowed in quarantine. 2 Spark actions
                 (df.count + quarantine.count) when evaluated.

Batching strategy:
  All aggregate rules (min_rows, freshness, sql) → one df.agg() collect().
  All null_rate rules → one df.sample(max_fraction).agg() collect().
  Row-level rules → lazy df.filter(); no collect() triggered.
  spillway_rate → 2 actions post-row-level (only when configured).
  Net: at most 2 Spark actions for aggregate path; spillway_rate adds 2 more if used.
"""

from __future__ import annotations

import importlib
import logging
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from aqueduct.errors import AqueductError
from aqueduct.executor.models import _add_module_warning
from aqueduct.executor.spark.error_columns import (
    AQ_ERROR_MODULE,
    AQ_ERROR_MSG,
    AQ_ERROR_RULE,
    AQ_ERROR_TS,
    AQ_ERROR_TYPE,
)
from aqueduct.models import Module

logger = logging.getLogger(__name__)


class AssertRuleType(StrEnum):
    """Rule type vocabulary — scalar string values matching Blueprint config keys."""

    SCHEMA_MATCH = "schema_match"
    MIN_ROWS = "min_rows"
    MAX_ROWS = "max_rows"
    FRESHNESS = "freshness"
    NOT_NULL = "not_null"
    SQL = "sql"
    SQL_ROW = "sql_row"
    CUSTOM = "custom"
    SPILLWAY_RATE = "spillway_rate"


class AssertOnFailAction(StrEnum):
    """``on_fail`` action vocabulary — scalar string values matching Blueprint config."""

    ABORT = "abort"
    WARN = "warn"
    WEBHOOK = "webhook"
    QUARANTINE = "quarantine"
    TRIGGER_AGENT = "trigger_agent"


# ── Public error type ─────────────────────────────────────────────────────────

class AssertError(AqueductError):
    """Raised when an Assert rule fires with on_fail=abort or on_fail=trigger_agent."""

    def __init__(
        self,
        message: str,
        rule_id: str | None = None,
        trigger_agent: bool = False,
        error_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.rule_id = rule_id
        self.trigger_agent = trigger_agent
        self.error_type = error_type


# ── Public API ────────────────────────────────────────────────────────────────

def execute_assert(
    module: Module,
    df: DataFrame,
    spark: SparkSession,
    run_id: str,
    blueprint_id: str,
    base_dir: str = "",
) -> tuple[DataFrame, DataFrame | None]:
    """Evaluate Assert rules against df.

    Args:
        module:       Assert Module from the compiled Manifest.
        df:           Incoming DataFrame (lazy).
        spark:        Active SparkSession.
        run_id:       Current run identifier.
        blueprint_id: Blueprint identifier (used in webhook payloads).
        base_dir:     Manifest.base_dir — the blueprint's directory, used to
                      resolve a `type: custom` rule's `fn:` as a sibling .py
                      file (see infra/module_loading.py).

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
        if rule.get("type") == AssertRuleType.SCHEMA_MATCH:
            _check_schema_match(module.id, df, rule)

    # ── Phase 2: aggregate rules (one batched action + one sample action) ────
    _batch_aggregate_rules(module.id, df, rules, blueprint_id, run_id)

    # ── Phase 3: row-level rules (lazy, no new action) ───────────────────────
    passing_df = df
    quarantine_parts: list[DataFrame] = []

    for rule in rules:
        rtype = rule.get("type")
        if rtype in (AssertRuleType.SQL_ROW, AssertRuleType.CUSTOM):
            passing_df, q_df = _apply_row_rule(module.id, passing_df, rule, spark, base_dir)
            if q_df is not None:
                quarantine_parts.append(q_df)
        elif rtype == AssertRuleType.FRESHNESS:
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)
            action = on_fail if isinstance(on_fail, str) else on_fail.get("action", AssertOnFailAction.ABORT)
            if action == AssertOnFailAction.QUARANTINE:
                col = rule.get("column")
                if not col:
                    raise AssertError(
                        f"[{module.id}] freshness rule requires 'column'",
                        rule_id="freshness",
                    )
                max_age_hours = float(rule.get("max_age_hours", 24))
                from pyspark.sql import functions as F
                from pyspark.sql.types import (
                    DoubleType,
                    FloatType,
                    IntegerType,
                    LongType,
                    ShortType,
                )
                hours_int = int(max_age_hours)
                minutes_int = round((max_age_hours - hours_int) * 60)
                interval = f"INTERVAL {hours_int} HOURS {minutes_int} MINUTES" if minutes_int else f"INTERVAL {hours_int} HOURS"
                cutoff = F.current_timestamp() - F.expr(interval)
                col_type = passing_df.schema[col].dataType
                if isinstance(col_type, (LongType, DoubleType, FloatType, IntegerType, ShortType)):
                    col_expr = F.to_timestamp(F.col(col).cast("long"))
                else:
                    col_expr = F.col(col)
                fresh_expr = col_expr >= cutoff
                is_null = F.col(col).isNull()
                # NULLs fail freshness — route to quarantine (ISSUE-015)
                passing_filter = fresh_expr & ~is_null
                q_df = (
                    passing_df.filter(~passing_filter)
                    .withColumn(AQ_ERROR_MODULE, F.lit(module.id))
                    .withColumn(AQ_ERROR_RULE, F.lit(AssertRuleType.FRESHNESS.value))
                    .withColumn(AQ_ERROR_TYPE, F.lit(rule.get("error_type") or "freshness"))
                    .withColumn(AQ_ERROR_MSG, F.lit(f"column {col!r} failed freshness check"))
                    .withColumn(AQ_ERROR_TS, F.current_timestamp())
                )
                passing_df = passing_df.filter(passing_filter)
                quarantine_parts.append(q_df)
        elif rtype == AssertRuleType.NOT_NULL:
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)
            action = on_fail if isinstance(on_fail, str) else on_fail.get("action", AssertOnFailAction.ABORT)
            if action == AssertOnFailAction.QUARANTINE:
                col = rule.get("column")
                if not col:
                    raise AssertError(
                        f"[{module.id}] not_null rule requires 'column'",
                        rule_id="not_null",
                    )
                from pyspark.sql import functions as F
                is_null = F.col(col).isNull()
                q_df = (
                    passing_df.filter(is_null)
                    .withColumn(AQ_ERROR_MODULE, F.lit(module.id))
                    .withColumn(AQ_ERROR_RULE, F.lit(AssertRuleType.NOT_NULL.value))
                    .withColumn(AQ_ERROR_TYPE, F.lit(rule.get("error_type") or "not_null"))
                    .withColumn(AQ_ERROR_MSG, F.lit(f"column {col!r} contains null"))
                    .withColumn(AQ_ERROR_TS, F.current_timestamp())
                )
                passing_df = passing_df.filter(~is_null)
                quarantine_parts.append(q_df)

    quarantine_df: DataFrame | None = None
    if quarantine_parts:
        from functools import reduce
        quarantine_df = reduce(lambda a, b: a.union(b), quarantine_parts)

    # ── Phase 4: spillway_rate (post-row-level; needs quarantine count) ───────
    spillway_rules = [(i, r) for i, r in enumerate(rules) if r.get("type") == AssertRuleType.SPILLWAY_RATE]
    if spillway_rules:
        _check_spillway_rate(module.id, df, quarantine_df, spillway_rules, blueprint_id, run_id)

    return passing_df, quarantine_df


# ── on_fail dispatch ──────────────────────────────────────────────────────────

def _handle_fail(
    on_fail: Any,
    module_id: str,
    rule_type: str,
    message: str,
    blueprint_id: str = "",
    run_id: str = "",
    error_type: str | None = None,
) -> None:
    """Dispatch on_fail action.  Raises AssertError for abort/trigger_agent."""
    if isinstance(on_fail, str):
        action = on_fail
        webhook_url: str | None = None
    else:
        action = on_fail.get("action", AssertOnFailAction.ABORT)
        webhook_url = on_fail.get("url")

    if action == AssertOnFailAction.ABORT:
        raise AssertError(message, rule_id=rule_type, error_type=error_type)
    elif action == AssertOnFailAction.TRIGGER_AGENT:
        raise AssertError(message, rule_id=rule_type, trigger_agent=True, error_type=error_type)
    elif action == AssertOnFailAction.WARN:
        logger.warning("[runtime_assert] [%s] Assert [%s]: %s", module_id, rule_type, message)
        _add_module_warning("runtime_assert", f"Assert [{rule_type}]: {message}")
    elif action == AssertOnFailAction.WEBHOOK:
        if webhook_url:
            _fire_rule_webhook(
                webhook_url, module_id, rule_type, message, blueprint_id, run_id
            )
        else:
            logger.warning(
                "[runtime_assert_webhook] [%s] Assert [%s] on_fail=webhook but no url specified.",
                module_id, rule_type,
            )
            _add_module_warning(
                "runtime_assert_webhook",
                f"Assert [{rule_type}] on_fail=webhook but no url specified.",
            )
    elif action == AssertOnFailAction.QUARANTINE:
        # Row-level only — handled by _apply_row_rule; aggregate rules treat as warn
        logger.warning(
            "[runtime_assert_quarantine_aggregate] [%s] Assert [%s] on_fail=quarantine used "
            "on aggregate rule; treated as warn.",
            module_id, rule_type,
        )
        _add_module_warning(
            "runtime_assert_quarantine_aggregate",
            f"Assert [{rule_type}] on_fail=quarantine used on aggregate rule; treated as warn.",
        )
        logger.warning("[runtime_assert] [%s] Assert [%s]: %s", module_id, rule_type, message)
        _add_module_warning("runtime_assert", f"Assert [{rule_type}]: {message}")
    else:
        logger.warning(
            "[runtime_assert_unknown_action] [%s] Assert [%s] unknown on_fail action %r; "
            "treating as warn.",
            module_id, rule_type, action,
        )
        _add_module_warning(
            "runtime_assert_unknown_action",
            f"Assert [{rule_type}] unknown on_fail action {action!r}; treating as warn.",
        )
        logger.warning("[runtime_assert] [%s] Assert [%s]: %s", module_id, rule_type, message)
        _add_module_warning("runtime_assert", f"Assert [{rule_type}]: {message}")


# ── Phase 1: schema_match ─────────────────────────────────────────────────────

def _check_schema_match(module_id: str, df: DataFrame, rule: dict[str, Any]) -> None:
    """Zero Spark action. Checks df.schema against expected field map."""
    expected: dict[str, str] = rule.get("expected", {})
    on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)

    actual_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    missing = [name for name in expected if name not in actual_fields]
    type_mismatches = [
        f"{name}: expected {etype}, got {actual_fields[name]}"
        for name, etype in expected.items()
        if name in actual_fields and actual_fields[name] != etype
    ]

    error_type = rule.get("error_type")
    if missing:
        msg = f"schema_match: missing columns {missing}"
        _handle_fail(on_fail, module_id, AssertRuleType.SCHEMA_MATCH, msg, error_type=error_type)
    if type_mismatches:
        msg = f"schema_match: type mismatches {type_mismatches}"
        _handle_fail(on_fail, module_id, AssertRuleType.SCHEMA_MATCH, msg, error_type=error_type)


# ── Phase 2: aggregate rules (batched) ───────────────────────────────────────

def _batch_aggregate_rules(
    module_id: str,
    df: DataFrame,
    rules: list[dict[str, Any]],
    blueprint_id: str,
    run_id: str,
) -> None:
    """Evaluate all aggregate rules in at most 2 Spark actions."""
    from pyspark.sql import functions as F

    # ── Collect aggregate expressions (min_rows, freshness, sql) ─────────────
    agg_cols: dict[str, Any] = {}
    agg_rule_indices: list[int] = []

    for i, rule in enumerate(rules):
        rtype = rule.get("type")
        if rtype == AssertRuleType.MIN_ROWS:
            agg_cols[f"_cnt_{i}"] = F.count("*")
            agg_rule_indices.append(i)
        elif rtype == AssertRuleType.MAX_ROWS:
            agg_cols[f"_cnt_{i}"] = F.count("*")
            agg_rule_indices.append(i)
        elif rtype == AssertRuleType.FRESHNESS:
            col = rule.get("column")
            if col:
                agg_cols[f"_max_{i}"] = F.max(F.col(col))
                agg_rule_indices.append(i)
        elif rtype == AssertRuleType.SQL:
            expr_str = rule.get("expr", "")
            if expr_str:
                agg_cols[f"_sql_{i}"] = F.expr(expr_str)
                agg_rule_indices.append(i)
        elif rtype == AssertRuleType.NOT_NULL:
            col = rule.get("column")
            if col:
                agg_cols[f"_notnull_{i}"] = F.sum(F.col(col).isNull().cast("int"))
                agg_rule_indices.append(i)

    agg_row = None
    if agg_cols:
        agg_row = df.agg(*[v.alias(k) for k, v in agg_cols.items()]).collect()[0]

    # Evaluate each aggregate rule against collected results
    if agg_row is not None:
        for i, rule in enumerate(rules):
            rtype = rule.get("type")
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)

            if rtype == AssertRuleType.MIN_ROWS and f"_cnt_{i}" in agg_cols:
                count = agg_row[f"_cnt_{i}"]
                min_val = int(rule.get("min", 0))
                if count < min_val:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.MIN_ROWS,
                        f"min_rows: got {count}, expected >= {min_val}",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )

            elif rtype == AssertRuleType.MAX_ROWS and f"_cnt_{i}" in agg_cols:
                count = agg_row[f"_cnt_{i}"]
                max_val = int(rule.get("max", 2**63))
                if count > max_val:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.MAX_ROWS,
                        f"max_rows: got {count}, expected <= {max_val}",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )

            elif rtype == AssertRuleType.FRESHNESS and f"_max_{i}" in agg_cols:
                max_ts = agg_row[f"_max_{i}"]
                max_age_hours = float(rule.get("max_age_hours", 24))
                if max_ts is None:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.FRESHNESS,
                        "freshness: column has no non-null values",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )
                else:
                    if hasattr(max_ts, "timestamp"):
                        ts_utc = max_ts.replace(tzinfo=UTC) if max_ts.tzinfo is None else max_ts
                    else:
                        try:
                            ts_utc = datetime.fromtimestamp(float(max_ts), tz=UTC)
                        except (ValueError, TypeError):
                            col = rule.get("column", "?")
                            _handle_fail(
                                on_fail, module_id, AssertRuleType.FRESHNESS,
                                f"freshness: column '{col}' has non-numeric value {max_ts!r}",
                                blueprint_id, run_id, error_type=rule.get("error_type"),
                            )
                            continue
                    age_hours = (datetime.now(tz=UTC) - ts_utc).total_seconds() / 3600
                    if age_hours > max_age_hours:
                        _handle_fail(
                            on_fail, module_id, AssertRuleType.FRESHNESS,
                            f"freshness: data is {age_hours:.1f}h old, max allowed {max_age_hours}h",
                            blueprint_id, run_id, error_type=rule.get("error_type"),
                        )

            elif rtype == AssertRuleType.SQL and f"_sql_{i}" in agg_cols:
                result = agg_row[f"_sql_{i}"]
                # Expression expected to evaluate to a boolean or truthy value
                if not result:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.SQL,
                        f"sql assertion failed: {rule.get('expr', '')!r} evaluated to {result!r}",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )

            elif rtype == AssertRuleType.NOT_NULL and f"_notnull_{i}" in agg_cols:
                null_count = agg_row[f"_notnull_{i}"] or 0
                col = rule.get("column", "?")
                if null_count > 0:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.NOT_NULL,
                        f"not_null[{col!r}]: {null_count} null value(s) found",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
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
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)
            null_count = sample_row[f"_null_{i}"] or 0
            rate = null_count / total
            max_rate = float(rule.get("max", 0.0))
            if rate > max_rate:
                _handle_fail(
                    on_fail, module_id, "null_rate",
                    f"null_rate[{col}]: {rate:.4%} > allowed {max_rate:.4%} "
                    f"(sample_size={total}, fraction={fraction})",
                    blueprint_id, run_id, error_type=rule.get("error_type"),
                )


# ── Phase 4: spillway_rate ────────────────────────────────────────────────────

def _check_spillway_rate(
    module_id: str,
    df: DataFrame,
    quarantine_df: DataFrame | None,
    spillway_rules: list[tuple[int, dict[str, Any]]],
    blueprint_id: str,
    run_id: str,
) -> None:
    """Evaluate spillway_rate rules after row-level rules have produced quarantine_df.

    Triggers at most 2 Spark actions: df.count() and quarantine_df.count().
    """
    total = df.count()
    quarantine_count = quarantine_df.count() if quarantine_df is not None else 0
    actual_rate = quarantine_count / total if total > 0 else 0.0

    for _i, rule in spillway_rules:
        max_rate = float(rule.get("max", 1.0))
        on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)
        if actual_rate > max_rate:
            _handle_fail(
                on_fail, module_id, AssertRuleType.SPILLWAY_RATE,
                f"spillway_rate: {actual_rate:.4%} of rows quarantined "
                f"({quarantine_count}/{total}), max allowed {max_rate:.4%}",
                blueprint_id, run_id, error_type=rule.get("error_type"),
            )


# ── Phase 3: row-level rules ──────────────────────────────────────────────────

def _apply_row_rule(
    module_id: str,
    df: DataFrame,
    rule: dict[str, Any],
    spark: SparkSession,
    base_dir: str = "",
) -> tuple[DataFrame, DataFrame | None]:
    """Apply a row-level rule.  Returns (passing_df, quarantine_df | None).

    Lazy — no Spark actions triggered.
    """
    from pyspark.sql import functions as F

    rtype = rule.get("type")
    on_fail = rule.get("on_fail", AssertOnFailAction.QUARANTINE)

    if rtype == AssertRuleType.SQL_ROW:
        expr_str = rule.get("expr", "")
        if not expr_str:
            return df, None

        min_pass_rate = rule.get("min_pass_rate")

        passing = df.filter(expr_str)
        failing = df.filter(f"NOT ({expr_str})")

        if min_pass_rate is not None:
            # User explicitly requested a pass-rate check — needs an action.
            # One agg job (count(*) + count_if) instead of two full count()
            # passes over df and passing.
            _counts = df.agg(
                F.count("*").alias("_total"),
                F.expr(f"count_if({expr_str})").alias("_pass"),
            ).collect()[0]
            total = _counts["_total"]
            pass_count = _counts["_pass"] or 0
            actual_rate = pass_count / total if total > 0 else 1.0
            if actual_rate < float(min_pass_rate):
                _handle_fail(
                    on_fail if not isinstance(on_fail, str) or on_fail != AssertOnFailAction.QUARANTINE else AssertOnFailAction.ABORT,
                    module_id, AssertRuleType.SQL_ROW,
                    f"sql_row pass_rate {actual_rate:.4%} < min {float(min_pass_rate):.4%}",
                )

        if isinstance(on_fail, str) and on_fail == AssertOnFailAction.QUARANTINE:
            quarantine_df = (
                failing
                .withColumn(AQ_ERROR_MODULE, F.lit(module_id))
                .withColumn(AQ_ERROR_RULE, F.lit(AssertRuleType.SQL_ROW.value))
                .withColumn(AQ_ERROR_TYPE, F.lit(rule.get("error_type") or AssertRuleType.SQL_ROW.value))
                .withColumn(AQ_ERROR_MSG, F.lit(f"failed: {expr_str}"))
                .withColumn(AQ_ERROR_TS, F.current_timestamp())
            )
            return passing, quarantine_df

        # non-quarantine on_fail for sql_row — evaluate lazily using count when needed
        # For abort/warn/webhook we'd need to detect if any rows fail; use lazy approach:
        # register failing as a view and check count only if non-quarantine action needed
        _handle_fail_if_any(module_id, failing, on_fail, AssertRuleType.SQL_ROW, f"failed: {expr_str}", error_type=rule.get("error_type"))
        return passing, None

    elif rtype == AssertRuleType.CUSTOM:
        fn_path = rule.get("fn", "")
        if not fn_path:
            logger.warning("[runtime_assert_custom_missing_fn] [%s] custom rule missing fn path; skipped.", module_id)
            _add_module_warning("runtime_assert_custom_missing_fn", "custom rule missing fn path; skipped.")
            return df, None

        try:
            fn = _load_callable(fn_path, base_dir)
            result = fn(df)
        except AssertError:
            raise
        except Exception as exc:
            logger.warning("[runtime_assert_custom_error] [%s] custom rule %r raised: %s", module_id, fn_path, exc)
            _add_module_warning("runtime_assert_custom_error", f"custom rule {fn_path!r} raised: {exc}")
            return df, None

        if not result.get("passed", True):
            msg = result.get("message", f"custom rule {fn_path!r} failed")
            q_df = result.get("quarantine_df")

            if isinstance(on_fail, str) and on_fail == AssertOnFailAction.QUARANTINE and q_df is not None:
                q_df = (
                    q_df
                    .withColumn(AQ_ERROR_MODULE, F.lit(module_id))
                    .withColumn(AQ_ERROR_RULE, F.lit(AssertRuleType.CUSTOM.value))
                    .withColumn(AQ_ERROR_TYPE, F.lit(rule.get("error_type") or AssertRuleType.CUSTOM.value))
                    .withColumn(AQ_ERROR_MSG, F.lit(msg))
                    .withColumn(AQ_ERROR_TS, F.current_timestamp())
                )
                # passing = df minus quarantine rows (caller's responsibility to exclude)
                # For simplicity, trust fn to return the right quarantine_df
                return df, q_df
            else:
                _handle_fail(on_fail, module_id, AssertRuleType.CUSTOM, msg, error_type=rule.get("error_type"))

    return df, None


def _handle_fail_if_any(
    module_id: str,
    failing_df: DataFrame,
    on_fail: Any,
    rule_type: str,
    message: str,
    error_type: str | None = None,
) -> None:
    """Fire on_fail if failing_df has any rows.  Triggers one Spark action."""
    from pyspark.sql import functions as F

    stamped = (
        failing_df
        .withColumn(AQ_ERROR_MODULE, F.lit(module_id))
        .withColumn(AQ_ERROR_RULE, F.lit(rule_type))
        .withColumn(AQ_ERROR_TYPE, F.lit(error_type or rule_type))
        .withColumn(AQ_ERROR_MSG, F.lit(message))
        .withColumn(AQ_ERROR_TS, F.current_timestamp())
    )
    count = stamped.count()
    if count > 0:
        _handle_fail(on_fail, module_id, rule_type, f"{message} ({count} rows)", error_type=error_type)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_callable(fn_path: str, base_dir: str = "") -> Any:
    """Load a Python callable from a dotted path (e.g. 'my_pkg.rules.check_orders').

    ``base_dir`` (Manifest.base_dir) lets ``fn: rules.check_orders`` resolve a
    ``rules.py`` sitting next to the blueprint — see infra/module_loading.py."""
    from aqueduct.infra.module_loading import load_module

    parts = fn_path.rsplit(".", 1)
    if len(parts) != 2:
        raise AssertError(
            f"custom rule fn {fn_path!r} must be 'module.callable' format",
            rule_id=AssertRuleType.CUSTOM,
        )
    module_path, attr = parts
    try:
        mod = load_module(module_path, base_dir)
    except ImportError as exc:
        raise AssertError(
            f"custom rule fn {fn_path!r}: cannot import {module_path!r}: {exc}",
            rule_id=AssertRuleType.CUSTOM,
        ) from exc
    fn = getattr(mod, attr, None)
    if fn is None or not callable(fn):
        raise AssertError(
            f"custom rule fn {fn_path!r}: {attr!r} not found or not callable in {module_path!r}",
            rule_id=AssertRuleType.CUSTOM,
        )
    return fn


def _fire_rule_webhook(
    url: str,
    module_id: str,
    rule_type: str,
    message: str,
    blueprint_id: str = "",
    run_id: str = "",
) -> None:
    """Fire assertion failure webhook asynchronously (best-effort)."""
    try:
        from aqueduct.infra.http import _deliver_webhook_payload
        full_payload = {
            "event": "assert_rule_failed",
            "module_id": module_id,
            "rule_type": rule_type,
            "message": message,
            "blueprint_id": blueprint_id,
            "run_id": run_id,
            "fired_at": datetime.now(tz=UTC).isoformat(),
        }
        _deliver_webhook_payload(url, full_payload)
    except Exception as exc:
        logger.warning("[runtime_assert_webhook_fire_failed] [%s] Assert webhook fire failed: %s", module_id, exc)
        _add_module_warning("runtime_assert_webhook_fire_failed", f"Assert webhook fire failed: {exc}")
