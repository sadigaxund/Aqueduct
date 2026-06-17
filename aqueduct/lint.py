"""Static Blueprint linting — the engine behind ``aqueduct lint``.

Style and correctness checks that sit *beyond* schema validation (``validate``
only parses + type-checks) and *beyond* the compiler's performance warnings
(which fire during ``compile``/``run``). Each rule carries a stable
``AQ-LINTNNN`` id and a severity (``warn`` | ``error``):

  - **Structural rules** walk the parsed module/edge graph. They catch smells
    the parser's graph validation doesn't reject (orphan modules, duplicate
    edges, non-descriptive labels) — the blueprint is schema-valid, just untidy.
  - **SQL rules** parse Channel ``op: sql`` queries with sqlglot (already a
    dependency, same ``dialect="spark"`` as lineage extraction) and flag
    likely-wrong constructs (cartesian joins, ``SELECT *`` into an Egress,
    aggregate/GROUP BY mismatches, un-aliased self-joins).

All initial rules ship as ``warn`` (advisory). ``aqueduct lint --strict``
promotes every finding to ``error`` so CI can gate on a clean blueprint.

Rules are intentionally conservative — a false *positive* erodes trust faster
than a missed *negative*. When a rule cannot decide, it stays silent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Iterator

from aqueduct.parser.models import Blueprint
from aqueduct.parser.models import ModuleType

logger = logging.getLogger(__name__)

# Bumped when the finding shape or rule-id contract changes (consumed by the
# `--format json` payload; downstream CI tooling keys off it).
LINT_SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class LintFinding:
    """One lint result. ``module_id`` is None for blueprint-wide findings."""

    rule_id: str           # "AQ-LINT001"
    severity: str          # "warn" | "error"
    message: str
    module_id: str | None = None


# ── structural rules ──────────────────────────────────────────────────────────

def _rule_unused_module(bp: Blueprint) -> Iterator[LintFinding]:
    """AQ-LINT001 — a module no edge / depends_on / spillway / attach_to touches."""
    if len(bp.modules) <= 1:
        return
    touched: set[str] = set()
    for e in bp.edges:
        touched.add(e.from_id)
        touched.add(e.to_id)
    for m in bp.modules:
        touched.update(m.depends_on)
        if m.spillway:
            touched.add(m.id)        # the module produces the spillway output
            touched.add(m.spillway)
        if m.attach_to:
            touched.add(m.id)        # a Probe tap is "connected" via attach_to
            touched.add(m.attach_to)
    for m in bp.modules:
        if m.id not in touched:
            yield LintFinding(
                "AQ-LINT001", "warn",
                f"module {m.id!r} ({m.type}) is not referenced by any edge, "
                "depends_on, spillway, or attach_to — it is orphaned and will "
                "not execute as part of the DAG",
                m.id,
            )


def _rule_label(bp: Blueprint) -> Iterator[LintFinding]:
    """AQ-LINT002 — empty label, or a label that just repeats the id."""
    for m in bp.modules:
        label = (m.label or "").strip()
        if not label:
            yield LintFinding(
                "AQ-LINT002", "warn",
                f"module {m.id!r} has an empty label — a human-readable label "
                "improves readability and gives the heal LLM better context",
                m.id,
            )
        elif label == m.id:
            yield LintFinding(
                "AQ-LINT002", "warn",
                f"module {m.id!r} label just repeats its id — a descriptive "
                "label improves readability and heal-LLM context",
                m.id,
            )


def _rule_duplicate_edge(bp: Blueprint) -> Iterator[LintFinding]:
    """AQ-LINT003 — the same (from, to, port) edge declared more than once."""
    counts: dict[tuple[str, str, str], int] = {}
    for e in bp.edges:
        key = (e.from_id, e.to_id, e.port)
        counts[key] = counts.get(key, 0) + 1
    for (from_id, to_id, port), n in counts.items():
        if n > 1:
            yield LintFinding(
                "AQ-LINT003", "warn",
                f"duplicate edge {from_id!r} → {to_id!r} (port={port!r}) "
                f"declared {n} times — remove the redundant edge",
                to_id,
            )


# ── SQL rule helpers ────────────────────────────────────────────────────────────

def _sql_channels(bp: Blueprint) -> Iterator[tuple[str, str]]:
    """Yield ``(module_id, query)`` for every Channel with ``op: sql``."""
    for m in bp.modules:
        if m.type != ModuleType.Channel:
            continue
        cfg = m.config or {}
        if cfg.get("op") != "sql":
            continue
        query = cfg.get("query")
        if isinstance(query, str) and query.strip():
            yield m.id, query


def _parse_sql(query: str):
    """Parse a SparkSQL string with sqlglot; return the expression or None.

    Mirrors lineage extraction (``dialect="spark"``). Any parse failure or a
    missing sqlglot install yields None — lint never errors on unparseable SQL,
    it simply skips that query (validation already covers hard syntax errors at
    runtime; lint is best-effort static analysis).
    """
    try:
        import sqlglot
    except Exception:  # pragma: no cover — sqlglot is a core dep
        logger.debug("sqlglot not available; skipping SQL lint rules")
        return None
    try:
        return sqlglot.parse_one(query, dialect="spark")
    except Exception as exc:
        logger.debug("sqlglot parse failed; skipping SQL lint for query: %s", exc)
        return None


def _top_select(stmt):
    """Return the top-level SELECT of a parsed statement, or None."""
    import sqlglot.expressions as exp

    if stmt is None:
        return None
    if isinstance(stmt, exp.Select):
        return stmt
    return stmt.find(exp.Select)


# ── SQL rules ─────────────────────────────────────────────────────────────────

def _rule_self_join_collision(bp: Blueprint) -> Iterator[LintFinding]:
    """AQ-LINT004 — same relation referenced 2+ times without distinct aliases."""
    import sqlglot.expressions as exp

    for mid, query in _sql_channels(bp):
        stmt = _parse_sql(query)
        if stmt is None:
            continue
        by_name: dict[str, list[str]] = {}
        for tbl in stmt.find_all(exp.Table):
            by_name.setdefault(tbl.name, []).append(tbl.alias or "")
        for name, aliases in by_name.items():
            if len(aliases) < 2:
                continue
            # Collision when two references share the same alias key (an empty
            # string == no alias). `t JOIN t` and `t a JOIN t a` both collide;
            # `t a JOIN t b` is fine.
            seen: set[str] = set()
            if any(a in seen or seen.add(a) for a in aliases):  # type: ignore[func-returns-value]
                yield LintFinding(
                    "AQ-LINT004", "warn",
                    f"Channel {mid!r}: relation {name!r} is referenced "
                    f"{len(aliases)} times without distinct aliases — Spark "
                    "self-joins need explicit, distinct aliases or column "
                    "references are ambiguous",
                    mid,
                )


def _rule_cartesian_join(bp: Blueprint) -> Iterator[LintFinding]:
    """AQ-LINT010 — a plain ``JOIN`` with no ON/USING condition.

    sqlglot synthesizes ``ON TRUE`` for a condition-less ``JOIN``, so the
    cartesian signal is an ``on`` that is the boolean literal ``TRUE`` (and no
    ``USING``). Explicit ``CROSS JOIN`` and comma-joins (which sqlglot
    normalises to ``CROSS``) are left alone — assumed intentional.
    """
    import sqlglot.expressions as exp

    for mid, query in _sql_channels(bp):
        stmt = _parse_sql(query)
        if stmt is None:
            continue
        for join in stmt.find_all(exp.Join):
            if (join.args.get("kind") or "").upper() == "CROSS":
                continue
            if join.args.get("using"):
                continue
            on = join.args.get("on")
            real_condition = on is not None and not (
                isinstance(on, exp.Boolean) and on.this is True
            )
            if real_condition:
                continue
            yield LintFinding(
                "AQ-LINT010", "warn",
                f"Channel {mid!r}: JOIN without an ON/USING condition produces "
                "a cartesian product — add a join predicate, or write CROSS "
                "JOIN explicitly if the cross product is intended",
                mid,
            )
            break  # one finding per channel is enough


def _rule_star_into_egress(bp: Blueprint) -> Iterator[LintFinding]:
    """AQ-LINT011 — a ``SELECT *`` Channel that feeds directly into an Egress."""
    import sqlglot.expressions as exp

    egress_ids = {m.id for m in bp.modules if m.type == ModuleType.Egress}
    if not egress_ids:
        return
    downstream: dict[str, list[str]] = {}
    for e in bp.edges:
        if e.port == "main":
            downstream.setdefault(e.from_id, []).append(e.to_id)
    for mid, query in _sql_channels(bp):
        if not any(t in egress_ids for t in downstream.get(mid, [])):
            continue
        sel = _top_select(_parse_sql(query))
        if sel is None:
            continue
        if any(p.find(exp.Star) for p in sel.expressions):
            yield LintFinding(
                "AQ-LINT011", "warn",
                f"Channel {mid!r} uses SELECT * and feeds directly into an "
                "Egress — an upstream schema change silently changes the "
                "written schema; list output columns explicitly",
                mid,
            )


def _rule_groupby_mismatch(bp: Blueprint) -> Iterator[LintFinding]:
    """AQ-LINT012 — aggregate + bare column in SELECT with no GROUP BY."""
    import sqlglot.expressions as exp

    for mid, query in _sql_channels(bp):
        sel = _top_select(_parse_sql(query))
        if sel is None or sel.args.get("group"):
            continue
        projections = sel.expressions
        has_agg = any(p.find(exp.AggFunc) for p in projections)
        has_bare_col = any(
            isinstance(p.this if isinstance(p, exp.Alias) else p, exp.Column)
            for p in projections
        )
        if has_agg and has_bare_col:
            yield LintFinding(
                "AQ-LINT012", "warn",
                f"Channel {mid!r}: SELECT mixes aggregate function(s) with a "
                "non-aggregated column but has no GROUP BY — Spark rejects this "
                "at runtime; add a GROUP BY clause or aggregate the column",
                mid,
            )


# ── registry + entrypoint ───────────────────────────────────────────────────────

_RULES: tuple[Callable[[Blueprint], Iterator[LintFinding]], ...] = (
    _rule_unused_module,
    _rule_label,
    _rule_duplicate_edge,
    _rule_self_join_collision,
    _rule_cartesian_join,
    _rule_star_into_egress,
    _rule_groupby_mismatch,
)


def run_lint(blueprint: Blueprint) -> list[LintFinding]:
    """Run every registered lint rule over a parsed Blueprint.

    A rule that raises is logged and skipped — one buggy rule never aborts the
    whole lint pass. Findings are returned sorted by (rule_id, module_id,
    message) for stable, diff-friendly output.
    """
    findings: list[LintFinding] = []
    for rule in _RULES:
        try:
            findings.extend(rule(blueprint))
        except Exception:  # pragma: no cover — defensive
            logger.debug("lint rule %s raised", getattr(rule, "__name__", "?"), exc_info=True)
    findings.sort(key=lambda f: (f.rule_id, f.module_id or "", f.message))
    return findings
