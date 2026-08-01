"""Assert executor — declarative data quality gates, DuckDB engine (Pass D).

Mirrors ``aqueduct/executor/spark/assert_.py``'s semantics exactly — same rule
vocabulary, same ``on_fail`` vocabulary, same phase ordering, same warning
``rule_id``s and message text (so cross-engine observability queries, e.g.
``aqueduct/stores/queries.py``'s ``_ASSERT_FAIL_PREFIXES``, keep working
unmodified) — translated into DuckDB's own execution idiom: a
``duckdb.DuckDBPyRelation`` and SQL aggregate/filter expressions where Spark
uses a ``DataFrame`` and column-expression actions. There is no separate
"batch into N Spark actions" accounting on this engine (a lazy relation is a
query plan, not a distributed job graph the way an action-count budget
matters for), but the same PHASE STRUCTURE is kept — one aggregate query for
every aggregate rule, one sampled query for ``null_rate``, lazy filters for
row-level rules, two count queries for ``spillway_rate`` — because it is what
Spark's own file organizes its rule types around, and staying in that shape
makes the two engines' behavior easy to compare rule-by-rule.

Rule types (10 — matches ``spark/assert_.py``'s ``AssertRuleType`` and
``parser.schema.AssertRuleSchema.type``'s Literal exactly, ``null_rate``
included; a cross-engine remediation pass added ``NULL_RATE`` to Spark's
enum after finding its dispatcher had reached it only via a raw string
literal, never a promoted member — see CHANGELOG. Formalized here from the
start because gallery snippet ``12_assert_null_sampling`` uses it directly
with ``on_fail: abort``):

  Aggregate rules (one ``rel.aggregate()`` query, all rules batched together):
    schema_match   Check ``rel.columns``/``rel.types`` against an expected
                   field map. Zero query execution (metadata only).
    min_rows       ``COUNT(*)``.
    max_rows       ``COUNT(*)`` (same aggregate column as min_rows).
    freshness      ``MAX(column)``.
    not_null       ``SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END)``.
    sql            Arbitrary aggregate SQL boolean expression, transpiled
                   from Spark SQL to DuckDB SQL via sqlglot (same idiom as
                   ``duckdb_/channel.py``'s ``op: sql``) so the same
                   Blueprint's ``expr:`` runs unmodified on both engines.

  Null-rate rule (one separate sampled SQL query — needs ``USING SAMPLE``,
  which is SQL-only, not exposed as a relation method):
    null_rate      Null fraction for a column over a Bernoulli sample.

  Row-level rules (lazy ``rel.filter()``/``rel.project()``, no query runs
  until a downstream consumer materializes the relation):
    sql_row        Filter expression (transpiled, same as ``sql``); rows
                   where it is FALSE are quarantine candidates.
    custom         Python callable: ``fn(rel) -> {"passed": bool, "message":
                   str, "quarantine_df": DuckDBPyRelation | None}`` — same
                   contract shape as Spark's, engine-native object type.

  Post-row-level rule (evaluated after quarantine is known — two count
  queries when configured):
    spillway_rate  Max fraction of rows allowed in quarantine.

Five ``on_fail`` actions, all real on this engine — none is faked:
  abort          → raise AssertError, blueprint stops immediately.
  warn           → log warning, blueprint continues.
  webhook        → fire async POST via ``infra/http.py`` (engine-agnostic —
                   already used by this same package's retry-exhaustion path
                   in ``executor.py``).
  quarantine     → row-level rules only; failing rows routed to the
                   ``spillway`` port — the SAME machinery Channel's
                   ``spillway_condition`` already feeds (``feature.spillway``
                   is ``supported`` on this engine; this module produces the
                   same ``_aq_error_*``-stamped relation, dispatched into
                   ``frame_store[f"{module.id}.spillway"]`` by
                   ``executor.py``, not a second path).
  trigger_agent  → raise AssertError(trigger_agent=True); the CLI's healing
                   loop fires on return, same as Spark's.

Layer note: this module is under ``executor/duckdb_/`` and imports
``duckdb`` only inside function bodies (TYPE_CHECKING at module scope), same
discipline as every other module in this package. It does NOT import
anything from ``executor/spark/`` — including the spillway error-column name
constants — mirroring the precedent already set by this package's retry
helpers and ``executor.py``'s own hardcoded ``_aq_error_type`` filter string:
small, engine-agnostic constants are duplicated by hand rather than reached
across the Spark/DuckDB package boundary (see ``executor.py``'s retry-helper
comment for the same rationale spelled out in full).
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import AqueductError
from aqueduct.executor.models import _add_module_warning
from aqueduct.models import Module

logger = logging.getLogger(__name__)

# Spillway error-column names — mirrors ``executor/spark/error_columns.py``
# byte-for-byte (see module docstring's layer note for why this is a
# deliberate duplication, not an import).
_AQ_ERROR_MODULE = "_aq_error_module"
_AQ_ERROR_TYPE = "_aq_error_type"
_AQ_ERROR_MSG = "_aq_error_msg"
_AQ_ERROR_TS = "_aq_error_ts"
_AQ_ERROR_RULE = "_aq_error_rule"


class AssertRuleType(StrEnum):
    """Rule type vocabulary — scalar string values matching Blueprint config keys.

    Ten members — matches ``spark/assert_.py``'s ``AssertRuleType`` exactly
    (including ``NULL_RATE``, added there in a cross-engine remediation pass —
    see module docstring) and ``parser.schema.AssertRuleSchema.type``'s
    Literal.
    """

    SCHEMA_MATCH = "schema_match"
    MIN_ROWS = "min_rows"
    MAX_ROWS = "max_rows"
    FRESHNESS = "freshness"
    NOT_NULL = "not_null"
    SQL = "sql"
    SQL_ROW = "sql_row"
    CUSTOM = "custom"
    SPILLWAY_RATE = "spillway_rate"
    NULL_RATE = "null_rate"


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
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    blueprint_id: str,
    base_dir: str = "",
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation | None]:
    """Evaluate Assert rules against ``rel``.

    Args:
        module:       Assert Module from the compiled Manifest.
        rel:          Incoming relation (lazy).
        con:          Active DuckDB connection (caller owns lifecycle).
        run_id:       Current run identifier.
        blueprint_id: Blueprint identifier (used in webhook payloads).
        base_dir:     Manifest.base_dir — the blueprint's directory, used to
                      resolve a ``type: custom`` rule's ``fn:`` as a sibling
                      .py file (see ``infra/module_loading.py``).

    Returns:
        (passing_rel, quarantine_rel)
        - passing_rel:    rows that passed all row-level rules (may equal
                          ``rel`` when no row-level rules are configured).
        - quarantine_rel: union of rows that failed quarantine rules, or
                          None if no quarantine rules fired.

    Raises:
        AssertError: on_fail=abort or on_fail=trigger_agent for any rule.
    """
    rules: list[dict[str, Any]] = module.config.get("rules", [])
    if not rules:
        logger.debug("Assert %r has no rules configured; pass-through.", module.id)
        return rel, None

    # ── Phase 1: schema_match (zero query execution) ─────────────────────────
    for rule in rules:
        if rule.get("type") == AssertRuleType.SCHEMA_MATCH:
            _check_schema_match(module.id, rel, rule)

    # ── Phase 2: aggregate rules (one batched aggregate query) + null_rate ───
    _batch_aggregate_rules(module.id, rel, rules, blueprint_id, run_id, con)

    # ── Phase 3: row-level rules (lazy, no query runs yet) ────────────────────
    passing_rel = rel
    quarantine_parts: list[duckdb.DuckDBPyRelation] = []

    for rule in rules:
        rtype = rule.get("type")
        if rtype in (AssertRuleType.SQL_ROW, AssertRuleType.CUSTOM):
            passing_rel, q_rel = _apply_row_rule(module.id, passing_rel, rule, con, base_dir)
            if q_rel is not None:
                quarantine_parts.append(q_rel)
        elif rtype == AssertRuleType.FRESHNESS:
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)
            action = on_fail if isinstance(on_fail, str) else on_fail.get("action", AssertOnFailAction.ABORT)
            if action == AssertOnFailAction.QUARANTINE:
                passing_rel, q_rel = _freshness_row_quarantine(module.id, passing_rel, rule)
                quarantine_parts.append(q_rel)
        elif rtype == AssertRuleType.NOT_NULL:
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)
            action = on_fail if isinstance(on_fail, str) else on_fail.get("action", AssertOnFailAction.ABORT)
            if action == AssertOnFailAction.QUARANTINE:
                passing_rel, q_rel = _not_null_row_quarantine(module.id, passing_rel, rule)
                quarantine_parts.append(q_rel)

    quarantine_rel: duckdb.DuckDBPyRelation | None = None
    if quarantine_parts:
        quarantine_rel = quarantine_parts[0]
        for part in quarantine_parts[1:]:
            try:
                quarantine_rel = quarantine_rel.union(part)
            except Exception as exc:
                raise AssertError(
                    f"[{module.id}] failed to union quarantine relations: {exc}"
                ) from exc

    # ── Phase 4: spillway_rate (post-row-level; needs quarantine count) ──────
    spillway_rules = [(i, r) for i, r in enumerate(rules) if r.get("type") == AssertRuleType.SPILLWAY_RATE]
    if spillway_rules:
        _check_spillway_rate(module.id, rel, quarantine_rel, spillway_rules, blueprint_id, run_id)

    return passing_rel, quarantine_rel


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
    """Dispatch on_fail action. Raises AssertError for abort/trigger_agent."""
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
            _fire_rule_webhook(webhook_url, module_id, rule_type, message, blueprint_id, run_id)
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
        # Row-level only — handled by _apply_row_rule/the freshness/not_null
        # row-quarantine helpers; aggregate rules treat as warn (same quirk
        # as Spark's — see spark/assert_.py's identical branch).
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

# ``rel.types`` items are ``duckdb.typing.DuckDBPyType``; ``str()`` gives the
# canonical DuckDB spelling ("BIGINT", "VARCHAR", ...). ``expected``'s values
# are Blueprint-authored hub spellings ("int", "string", "decimal(18,4)", the
# same vocabulary Spark's ``simpleString()`` output happens to share aliases
# with) — rendered through the SAME Arrow type-hub translation
# ``duckdb_/ingress.py``'s ``schema_hint`` validation already uses for
# exactly this cross-engine comparison (see that module's
# ``_normalize_actual_type``/``_normalize_hint_type``), so a Blueprint
# authored once against Spark's type vocabulary validates unmodified here.
def _normalize_actual_type(t: object) -> str:
    return str(t).strip().lower()


def _normalize_expected_type(module_id: str, t: str) -> str:
    from aqueduct.errors import EnginePluginError
    from aqueduct.executor.duckdb_.type_render import normalize_type_spelling

    try:
        return normalize_type_spelling(str(t)).lower()
    except EnginePluginError as exc:
        raise AssertError(
            f"[{module_id}] schema_match expected type {str(t)!r}: {exc}",
            rule_id=AssertRuleType.SCHEMA_MATCH,
        ) from exc


def _check_schema_match(module_id: str, rel: duckdb.DuckDBPyRelation, rule: dict[str, Any]) -> None:
    """Zero query execution. Checks rel.columns/rel.types against an expected field map."""
    expected: dict[str, str] = rule.get("expected", {}) or {}
    on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)

    actual_fields = {
        name: _normalize_actual_type(dtype) for name, dtype in zip(rel.columns, rel.types, strict=True)
    }

    missing = [name for name in expected if name not in actual_fields]
    type_mismatches = [
        f"{name}: expected {etype}, got {actual_fields[name]}"
        for name, etype in expected.items()
        if name in actual_fields and actual_fields[name] != _normalize_expected_type(module_id, etype)
    ]

    error_type = rule.get("error_type")
    if missing:
        msg = f"schema_match: missing columns {missing}"
        _handle_fail(on_fail, module_id, AssertRuleType.SCHEMA_MATCH, msg, error_type=error_type)
    if type_mismatches:
        msg = f"schema_match: type mismatches {type_mismatches}"
        _handle_fail(on_fail, module_id, AssertRuleType.SCHEMA_MATCH, msg, error_type=error_type)


# ── SQL expression transpile (Spark SQL -> DuckDB SQL) ────────────────────────

def _transpile_expr(module_id: str, rule_type: str, expr: str) -> str:
    """Transpile a Spark-SQL scalar expression to DuckDB SQL via sqlglot.

    Same idiom + rationale as ``duckdb_/channel.py``'s ``_transpile`` (which
    transpiles a full ``SELECT`` for ``op: sql``): ``expr:`` is authored once
    against Spark's dialect in a Blueprint that also runs on ``engine:
    spark``, so an Assert ``sql``/``sql_row`` rule's expression must run
    unmodified on both. ``sqlglot.parse_one`` (not ``transpile``, which
    expects a full statement) parses a bare expression directly.
    """
    import sqlglot

    try:
        parsed = sqlglot.parse_one(expr, read="spark")
        return parsed.sql(dialect="duckdb")
    except Exception as exc:
        raise AssertError(
            f"[{module_id}] rule {rule_type} expr {expr!r} could not be transpiled from Spark SQL "
            f"to DuckDB SQL: {exc}. Rewrite the expression in a dialect-neutral way, or avoid "
            "Spark-specific SQL functions/syntax for an Assert rule that must also run on duckdb.",
            rule_id=rule_type,
        ) from exc


# ── Phase 2: aggregate rules (batched) + null_rate (sampled) ─────────────────

def _batch_aggregate_rules(
    module_id: str,
    rel: duckdb.DuckDBPyRelation,
    rules: list[dict[str, Any]],
    blueprint_id: str,
    run_id: str,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Evaluate all aggregate rules in one batched ``rel.aggregate()`` query,
    plus one separate sampled query for any ``null_rate`` rules."""
    agg_exprs: dict[str, str] = {}

    for i, rule in enumerate(rules):
        rtype = rule.get("type")
        if rtype in (AssertRuleType.MIN_ROWS, AssertRuleType.MAX_ROWS):
            agg_exprs[f"_cnt_{i}"] = "COUNT(*)"
        elif rtype == AssertRuleType.FRESHNESS:
            col = rule.get("column")
            if col:
                agg_exprs[f"_max_{i}"] = f'MAX("{col}")'
        elif rtype == AssertRuleType.SQL:
            expr_str = rule.get("expr", "")
            if expr_str:
                duckdb_expr = _transpile_expr(module_id, AssertRuleType.SQL, expr_str)
                agg_exprs[f"_sql_{i}"] = f"({duckdb_expr})"
        elif rtype == AssertRuleType.NOT_NULL:
            col = rule.get("column")
            if col:
                agg_exprs[f"_notnull_{i}"] = f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END)'

    agg_row: dict[str, Any] | None = None
    if agg_exprs:
        cols_order = list(agg_exprs.keys())
        expr_sql = ", ".join(f"{v} AS {k}" for k, v in agg_exprs.items())
        try:
            fetched = rel.aggregate(expr_sql).fetchone()
        except Exception as exc:
            raise AssertError(f"[{module_id}] aggregate rule evaluation failed: {exc}") from exc
        agg_row = dict(zip(cols_order, fetched, strict=True))

    if agg_row is not None:
        for i, rule in enumerate(rules):
            rtype = rule.get("type")
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)

            if rtype == AssertRuleType.MIN_ROWS and f"_cnt_{i}" in agg_row:
                count = agg_row[f"_cnt_{i}"]
                min_val = int(rule.get("min", 0))
                if count < min_val:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.MIN_ROWS,
                        f"min_rows: got {count}, expected >= {min_val}",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )

            elif rtype == AssertRuleType.MAX_ROWS and f"_cnt_{i}" in agg_row:
                count = agg_row[f"_cnt_{i}"]
                max_val = int(rule.get("max", 2**63))
                if count > max_val:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.MAX_ROWS,
                        f"max_rows: got {count}, expected <= {max_val}",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )

            elif rtype == AssertRuleType.FRESHNESS and f"_max_{i}" in agg_row:
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

            elif rtype == AssertRuleType.SQL and f"_sql_{i}" in agg_row:
                result = agg_row[f"_sql_{i}"]
                if not result:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.SQL,
                        f"sql assertion failed: {rule.get('expr', '')!r} evaluated to {result!r}",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )

            elif rtype == AssertRuleType.NOT_NULL and f"_notnull_{i}" in agg_row:
                null_count = agg_row[f"_notnull_{i}"] or 0
                col = rule.get("column", "?")
                if null_count > 0:
                    _handle_fail(
                        on_fail, module_id, AssertRuleType.NOT_NULL,
                        f"not_null[{col!r}]: {null_count} null value(s) found",
                        blueprint_id, run_id, error_type=rule.get("error_type"),
                    )

    # ── null_rate rules — one shared sampled query (needs USING SAMPLE,
    # which is SQL syntax, not exposed as a relation method — hence the
    # register/query/unregister dance instead of .aggregate()). ─────────────
    null_rules = [(i, r) for i, r in enumerate(rules) if r.get("type") == AssertRuleType.NULL_RATE]
    if null_rules:
        fraction = max(float(r.get("fraction", 0.1)) for _, r in null_rules)
        pct = max(0.0, min(100.0, fraction * 100))

        null_exprs: dict[str, str] = {"_total": "COUNT(*)"}
        for i, rule in null_rules:
            col = rule.get("column")
            if col:
                null_exprs[f"_null_{i}"] = f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END)'

        cols_order = list(null_exprs.keys())
        expr_sql = ", ".join(f"{v} AS {k}" for k, v in null_exprs.items())
        view_name = f"__aq_assert_nullrate_{uuid.uuid4().hex}__"
        con.register(view_name, rel)
        try:
            fetched = con.sql(
                f'SELECT {expr_sql} FROM "{view_name}" USING SAMPLE {pct} PERCENT (bernoulli)'
            ).fetchone()
        except Exception as exc:
            raise AssertError(f"[{module_id}] null_rate sample evaluation failed: {exc}") from exc
        finally:
            con.unregister(view_name)
        sample_row = dict(zip(cols_order, fetched, strict=True))

        total = sample_row["_total"] or 1  # avoid division by zero

        for i, rule in null_rules:
            col = rule.get("column")
            if not col or f"_null_{i}" not in null_exprs:
                continue
            on_fail = rule.get("on_fail", AssertOnFailAction.ABORT)
            null_count = sample_row[f"_null_{i}"] or 0
            rate = null_count / total
            max_rate = float(rule.get("max", 0.0))
            if rate > max_rate:
                _handle_fail(
                    on_fail, module_id, AssertRuleType.NULL_RATE,
                    f"null_rate[{col}]: {rate:.4%} > allowed {max_rate:.4%} "
                    f"(sample_size={total}, fraction={fraction})",
                    blueprint_id, run_id, error_type=rule.get("error_type"),
                )


# ── Phase 4: spillway_rate ────────────────────────────────────────────────────

def _rel_count(rel: duckdb.DuckDBPyRelation) -> int:
    return rel.aggregate("COUNT(*) AS c").fetchone()[0]


def _check_spillway_rate(
    module_id: str,
    rel: duckdb.DuckDBPyRelation,
    quarantine_rel: duckdb.DuckDBPyRelation | None,
    spillway_rules: list[tuple[int, dict[str, Any]]],
    blueprint_id: str,
    run_id: str,
) -> None:
    """Evaluate spillway_rate rules after row-level rules have produced
    quarantine_rel. Triggers two count queries: rel and quarantine_rel."""
    try:
        total = _rel_count(rel)
        quarantine_count = _rel_count(quarantine_rel) if quarantine_rel is not None else 0
    except Exception as exc:
        raise AssertError(f"[{module_id}] spillway_rate count evaluation failed: {exc}") from exc
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


# ── Quarantine row-stamping ────────────────────────────────────────────────────

def _sql_literal(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _stamp_error_columns(
    module_id: str,
    rel: duckdb.DuckDBPyRelation,
    rule_type: str,
    error_type: str,
    message: str,
) -> duckdb.DuckDBPyRelation:
    """Project the 5 spillway system columns onto failing rows — same column
    names/semantics as Spark's (``executor/spark/error_columns.py``)."""
    try:
        return rel.project(
            f"*, {_sql_literal(module_id)} AS {_AQ_ERROR_MODULE}, "
            f"{_sql_literal(rule_type)} AS {_AQ_ERROR_RULE}, "
            f"{_sql_literal(error_type)} AS {_AQ_ERROR_TYPE}, "
            f"{_sql_literal(message)} AS {_AQ_ERROR_MSG}, "
            f"current_timestamp AS {_AQ_ERROR_TS}"
        )
    except Exception as exc:
        raise AssertError(f"[{module_id}] failed to stamp quarantine error columns: {exc}") from exc


# ── Phase 3: row-level rules (sql_row, custom) ────────────────────────────────

def _apply_row_rule(
    module_id: str,
    rel: duckdb.DuckDBPyRelation,
    rule: dict[str, Any],
    con: duckdb.DuckDBPyConnection,
    base_dir: str = "",
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation | None]:
    """Apply a row-level rule. Returns (passing_rel, quarantine_rel | None).

    Lazy — no query runs until a downstream consumer materializes the result,
    except ``min_pass_rate`` (needs its own aggregate) and the count inside
    ``_handle_fail_if_any`` for a non-quarantine ``sql_row`` on_fail.
    """
    rtype = rule.get("type")
    on_fail = rule.get("on_fail", AssertOnFailAction.QUARANTINE)

    if rtype == AssertRuleType.SQL_ROW:
        expr_str = rule.get("expr", "")
        if not expr_str:
            return rel, None
        duckdb_expr = _transpile_expr(module_id, AssertRuleType.SQL_ROW, expr_str)

        min_pass_rate = rule.get("min_pass_rate")

        try:
            passing = rel.filter(duckdb_expr)
            failing = rel.filter(f"NOT ({duckdb_expr})")
        except Exception as exc:
            raise AssertError(f"[{module_id}] sql_row filter {expr_str!r} failed: {exc}") from exc

        if min_pass_rate is not None:
            try:
                counts = rel.aggregate(
                    f"COUNT(*) AS _total, count_if({duckdb_expr}) AS _pass"
                ).fetchone()
            except Exception as exc:
                raise AssertError(f"[{module_id}] sql_row min_pass_rate evaluation failed: {exc}") from exc
            total, pass_count = counts[0], counts[1] or 0
            actual_rate = pass_count / total if total > 0 else 1.0
            if actual_rate < float(min_pass_rate):
                _handle_fail(
                    on_fail if not isinstance(on_fail, str) or on_fail != AssertOnFailAction.QUARANTINE else AssertOnFailAction.ABORT,
                    module_id, AssertRuleType.SQL_ROW,
                    f"sql_row pass_rate {actual_rate:.4%} < min {float(min_pass_rate):.4%}",
                )

        if isinstance(on_fail, str) and on_fail == AssertOnFailAction.QUARANTINE:
            quarantine_rel = _stamp_error_columns(
                module_id, failing, AssertRuleType.SQL_ROW.value,
                rule.get("error_type") or AssertRuleType.SQL_ROW.value,
                f"failed: {expr_str}",
            )
            return passing, quarantine_rel

        _handle_fail_if_any(module_id, failing, on_fail, AssertRuleType.SQL_ROW, f"failed: {expr_str}", error_type=rule.get("error_type"))
        return passing, None

    elif rtype == AssertRuleType.CUSTOM:
        fn_path = rule.get("fn", "")
        if not fn_path:
            logger.warning("[runtime_assert_custom_missing_fn] [%s] custom rule missing fn path; skipped.", module_id)
            _add_module_warning("runtime_assert_custom_missing_fn", "custom rule missing fn path; skipped.")
            return rel, None

        try:
            fn = _load_custom_callable(fn_path, base_dir)
            result = fn(rel)
        except AssertError:
            raise
        except Exception as exc:
            logger.warning("[runtime_assert_custom_error] [%s] custom rule %r raised: %s", module_id, fn_path, exc)
            _add_module_warning("runtime_assert_custom_error", f"custom rule {fn_path!r} raised: {exc}")
            return rel, None

        if not result.get("passed", True):
            msg = result.get("message", f"custom rule {fn_path!r} failed")
            q_rel = result.get("quarantine_df")

            if isinstance(on_fail, str) and on_fail == AssertOnFailAction.QUARANTINE and q_rel is not None:
                q_rel = _stamp_error_columns(
                    module_id, q_rel, AssertRuleType.CUSTOM.value,
                    rule.get("error_type") or AssertRuleType.CUSTOM.value, msg,
                )
                # passing = rel minus quarantine rows (caller's responsibility
                # to exclude) — trust fn to return the right quarantine_df,
                # same contract as Spark's.
                return rel, q_rel
            else:
                _handle_fail(on_fail, module_id, AssertRuleType.CUSTOM, msg, error_type=rule.get("error_type"))

    return rel, None


def _handle_fail_if_any(
    module_id: str,
    failing_rel: duckdb.DuckDBPyRelation,
    on_fail: Any,
    rule_type: str,
    message: str,
    error_type: str | None = None,
) -> None:
    """Fire on_fail if failing_rel has any rows. Triggers one count query."""
    stamped = _stamp_error_columns(module_id, failing_rel, rule_type, error_type or rule_type, message)
    try:
        count = _rel_count(stamped)
    except Exception as exc:
        raise AssertError(f"[{module_id}] row-count evaluation failed: {exc}") from exc
    if count > 0:
        _handle_fail(on_fail, module_id, rule_type, f"{message} ({count} rows)", error_type=error_type)


# ── freshness / not_null row-level quarantine (Phase 3 continuation) ─────────

_NUMERIC_TYPE_PREFIXES: frozenset[str] = frozenset({
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL",
})


def _column_type(module_id: str, rel: duckdb.DuckDBPyRelation, col: str, rule_type: str) -> str:
    if col not in rel.columns:
        raise AssertError(f"[{module_id}] {rule_type} rule column {col!r} not found in relation", rule_id=rule_type)
    return str(rel.types[rel.columns.index(col)]).upper()


def _freshness_row_quarantine(
    module_id: str, rel: duckdb.DuckDBPyRelation, rule: dict[str, Any],
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
    col = rule.get("column")
    if not col:
        raise AssertError(f"[{module_id}] freshness rule requires 'column'", rule_id=AssertRuleType.FRESHNESS)
    max_age_hours = float(rule.get("max_age_hours", 24))
    hours_int = int(max_age_hours)
    minutes_int = round((max_age_hours - hours_int) * 60)
    interval_sql = (
        f"INTERVAL '{hours_int} hours {minutes_int} minutes'" if minutes_int
        else f"INTERVAL '{hours_int} hours'"
    )
    col_type = _column_type(module_id, rel, col, AssertRuleType.FRESHNESS)
    is_numeric = any(col_type.startswith(p) for p in _NUMERIC_TYPE_PREFIXES)
    col_expr_sql = f'to_timestamp(CAST("{col}" AS BIGINT))' if is_numeric else f'"{col}"'
    fresh_sql = f'({col_expr_sql} >= (current_timestamp - {interval_sql}))'
    # NULLs fail freshness — route to quarantine (mirrors ISSUE-015, same as Spark's).
    passing_filter = f'{fresh_sql} AND "{col}" IS NOT NULL'
    failing_filter = f'NOT ({fresh_sql} AND "{col}" IS NOT NULL)'
    try:
        failing_rel = rel.filter(failing_filter)
        q_rel = _stamp_error_columns(
            module_id, failing_rel, AssertRuleType.FRESHNESS.value,
            rule.get("error_type") or "freshness", f"column {col!r} failed freshness check",
        )
        passing_rel = rel.filter(passing_filter)
    except AssertError:
        raise
    except Exception as exc:
        raise AssertError(f"[{module_id}] freshness quarantine filter failed: {exc}") from exc
    return passing_rel, q_rel


def _not_null_row_quarantine(
    module_id: str, rel: duckdb.DuckDBPyRelation, rule: dict[str, Any],
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
    col = rule.get("column")
    if not col:
        raise AssertError(f"[{module_id}] not_null rule requires 'column'", rule_id=AssertRuleType.NOT_NULL)
    try:
        failing_rel = rel.filter(f'"{col}" IS NULL')
        q_rel = _stamp_error_columns(
            module_id, failing_rel, AssertRuleType.NOT_NULL.value,
            rule.get("error_type") or "not_null", f"column {col!r} contains null",
        )
        passing_rel = rel.filter(f'"{col}" IS NOT NULL')
    except AssertError:
        raise
    except Exception as exc:
        raise AssertError(f"[{module_id}] not_null quarantine filter failed: {exc}") from exc
    return passing_rel, q_rel


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_custom_callable(fn_path: str, base_dir: str = "") -> Any:
    """Load a Python callable from a dotted path (e.g. 'my_pkg.rules.check_orders').

    Goes through ``aqueduct.infra.module_loading.load_callable`` — the ONE
    way to import Blueprint-referenced user code (see that module's
    docstring: this exact "bare importlib" bug shipped independently 5 times
    before being centralised there). ``base_dir`` (Manifest.base_dir) lets
    ``fn: rules.check_orders`` resolve a ``rules.py`` sitting next to the
    Blueprint.
    """
    from aqueduct.infra.module_loading import load_callable

    try:
        return load_callable(fn_path, base_dir)
    except ValueError as exc:
        raise AssertError(
            f"custom rule fn {fn_path!r} must be 'module.callable' format",
            rule_id=AssertRuleType.CUSTOM,
        ) from exc
    except ImportError as exc:
        raise AssertError(
            f"custom rule fn {fn_path!r}: cannot import module: {exc}",
            rule_id=AssertRuleType.CUSTOM,
        ) from exc
    except AttributeError as exc:
        raise AssertError(
            f"custom rule fn {fn_path!r}: callable not found: {exc}",
            rule_id=AssertRuleType.CUSTOM,
        ) from exc


def _fire_rule_webhook(
    url: str,
    module_id: str,
    rule_type: str,
    message: str,
    blueprint_id: str = "",
    run_id: str = "",
) -> None:
    """Fire assertion failure webhook asynchronously (best-effort) — same
    engine-agnostic delivery path ``executor.py``'s retry-exhaustion webhook
    already uses on this engine."""
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


__all__ = ["AssertError", "AssertOnFailAction", "AssertRuleType", "execute_assert"]
