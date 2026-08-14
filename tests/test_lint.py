from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.lint import LintFinding, run_lint
from aqueduct.parser.parser import ParseError, parse

pytestmark = pytest.mark.unit


import tempfile


def bp_yml(content: str, tmp_path: Path | None = None) -> Path:
    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp(prefix="aq-lint-"))
    p = tmp_path / "bp.yml"
    p.write_text(content)
    return p


FIXTURE_INGRESS_CHANNEL_EGRESS = """
aqueduct: "1.0"
id: lint_fix
name: Test
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: data/in.parquet
  - id: ch
    type: Channel
    label: Clean
    config:
      op: sql
      query: "SELECT * FROM src"
  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: data/out.parquet
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""


# ── AQ-LINT001: unused module ──────────────────────────────────────────────────


class TestUnusedModule:
    def test_orphan_module_detected(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_orphan
name: Orphan
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: data/in.parquet}
  - id: orphan
    type: Channel
    label: Not Tied
    config: {op: sql, query: "SELECT 1"}
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: data/out.parquet}
edges:
  - from: src
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        f001 = [f for f in findings if f.rule_id == "AQ-LINT001"]
        assert len(f001) == 1
        assert f001[0].module_id == "orphan"
        # Regression (audit 2026-08-01, failure_taxonomy #2 — advice-rot
        # message): an orphaned Channel DOES get scheduled and aborts the
        # run ("Channel has no main-port incoming edges") — it is not
        # skipped like an orphaned Probe. The message must say so.
        assert "will not execute" not in f001[0].message
        assert "run will fail" in f001[0].message

    def test_single_module_no_finding(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_single
name: Single
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: data/in.parquet}
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT001" for f in findings)

    def test_probe_with_attach_to_not_flagged(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_probe
name: Probe Attached
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: data/in.parquet}
  - id: prb
    type: Probe
    label: Tap
    attach_to: src
    config: {signals: [{type: schema_snapshot}]}
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: data/out.parquet}
edges:
  - from: src
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT001" for f in findings)


# ── AQ-LINT002: label ──────────────────────────────────────────────────────────


class TestLabel:
    def test_empty_label_found(self, tmp_path):
        p = bp_yml(FIXTURE_INGRESS_CHANNEL_EGRESS.replace("label: Source", "label: ''"))
        bp = parse(str(p))
        findings = run_lint(bp)
        f002 = [f for f in findings if f.rule_id == "AQ-LINT002"]
        assert any("empty label" in f.message for f in f002)

    def test_label_equals_id_found(self, tmp_path):
        p = bp_yml(FIXTURE_INGRESS_CHANNEL_EGRESS.replace("label: Source", "label: src"))
        bp = parse(str(p))
        findings = run_lint(bp)
        f002 = [f for f in findings if f.rule_id == "AQ-LINT002"]
        assert any("repeats its id" in f.message for f in f002)

    def test_descriptive_label_clean(self, tmp_path):
        p = bp_yml(FIXTURE_INGRESS_CHANNEL_EGRESS)
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT002" for f in findings)


# ── AQ-LINT003: duplicate edge ─────────────────────────────────────────────────


class TestDuplicateEdge:
    def test_duplicate_detected(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_dup
name: Dup
modules:
  - id: a
    type: Ingress
    label: A
    config: {format: parquet, path: d.parquet}
  - id: b
    type: Egress
    label: B
    config: {format: parquet, path: d2.parquet}
edges:
  - from: a
    to: b
  - from: a
    to: b
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        f003 = [f for f in findings if f.rule_id == "AQ-LINT003"]
        assert len(f003) == 1
        assert "2 times" in f003[0].message

    def test_no_duplicate_clean(self, tmp_path):
        p = bp_yml(FIXTURE_INGRESS_CHANNEL_EGRESS)
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT003" for f in findings)


# ── AQ-LINT004: self-join collision ────────────────────────────────────────────


class TestSelfJoinCollision:
    def test_unaliased_self_join_detected(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_sj
name: SelfJoin
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM src JOIN src"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        f004 = [f for f in findings if f.rule_id == "AQ-LINT004"]
        assert len(f004) == 1

    def test_distinct_aliases_clean(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_sj_ok
name: SelfJoinOK
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM src x JOIN src y"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT004" for f in findings)

    def test_schema_qualified_distinct_relations_clean(self, tmp_path):
        """Regression (audit 2026-08-01): keying on `tbl.name` alone treated
        `sales.orders` and `hr.orders` — two distinct, fully-qualified
        relations that happen to share a bare table name — as a self-join."""
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_sj_qualified
name: SelfJoinQualified
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM sales.orders JOIN hr.orders ON sales.orders.id = hr.orders.id"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT004" for f in findings)

    def test_subquery_scoped_reference_clean(self, tmp_path):
        """Regression (audit 2026-08-01): `find_all(exp.Table)` on the whole
        statement counted a table inside a nested subquery as a second
        reference to the outer table of the same name — `t JOIN (SELECT *
        FROM t) sub` is a legal, non-ambiguous query (the inner `t` is
        scoped to `sub`), not a self-join collision."""
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_sj_subquery
name: SelfJoinSubquery
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM src JOIN (SELECT * FROM src) sub ON src.id = sub.id"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT004" for f in findings)


# ── AQ-LINT010: cartesian join ─────────────────────────────────────────────────


class TestCartesianJoin:
    def test_join_without_on_detected(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_cart
name: Cartesian
modules:
  - id: a
    type: Ingress
    label: A
    config: {format: parquet, path: d1.parquet}
  - id: b
    type: Ingress
    label: B
    config: {format: parquet, path: d2.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM a JOIN b"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d3.parquet}
edges:
  - from: a
    to: ch
  - from: b
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        f010 = [f for f in findings if f.rule_id == "AQ-LINT010"]
        assert len(f010) == 1

    def test_join_with_on_clean(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_cart_ok
name: CartesianOK
modules:
  - id: a
    type: Ingress
    label: A
    config: {format: parquet, path: d1.parquet}
  - id: b
    type: Ingress
    label: B
    config: {format: parquet, path: d2.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM a JOIN b ON a.x = b.y"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d3.parquet}
edges:
  - from: a
    to: ch
  - from: b
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT010" for f in findings)

    def test_cross_join_not_flagged(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_cross
name: Cross
modules:
  - id: a
    type: Ingress
    label: A
    config: {format: parquet, path: d1.parquet}
  - id: b
    type: Ingress
    label: B
    config: {format: parquet, path: d2.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM a CROSS JOIN b"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d3.parquet}
edges:
  - from: a
    to: ch
  - from: b
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT010" for f in findings)


# ── AQ-LINT011: SELECT * into Egress ───────────────────────────────────────────


class TestStarIntoEgress:
    def test_select_star_into_egress_detected(self, tmp_path):
        p = bp_yml(FIXTURE_INGRESS_CHANNEL_EGRESS)
        bp = parse(str(p))
        findings = run_lint(bp)
        f011 = [f for f in findings if f.rule_id == "AQ-LINT011"]
        assert len(f011) == 1

    def test_select_star_not_feeding_egress_clean(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_star_ok
name: StarOK
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT * FROM src"
edges:
  - from: src
    to: ch
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT011" for f in findings)

    def test_select_star_into_spillway_egress_detected(self, tmp_path):
        """Regression (audit 2026-08-01): the downstream map only followed
        `port == "main"` edges (an include-list), so a Channel's `spillway`
        edge into a quarantine Egress — a documented pattern — was invisible
        to this rule even though it is a direct data feed into an Egress."""
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_star_spillway
name: StarSpillway
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    spillway: quarantine
    config:
      op: sql
      query: "SELECT * FROM src"
  - id: quarantine
    type: Egress
    label: Quarantine
    config: {format: parquet, path: d2.parquet, mode: append}
edges:
  - from: src
    to: ch
  - from: ch
    to: quarantine
    port: spillway
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        f011 = [f for f in findings if f.rule_id == "AQ-LINT011"]
        assert len(f011) == 1
        assert f011[0].module_id == "ch"


# ── AQ-LINT012: GROUP BY mismatch ──────────────────────────────────────────────


class TestGroupByMismatch:
    def test_agg_without_groupby_detected(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_gb
name: GroupBy
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT region, SUM(amount) FROM src"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        f012 = [f for f in findings if f.rule_id == "AQ-LINT012"]
        assert len(f012) == 1

    def test_with_groupby_clean(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_gb_ok
name: GroupByOK
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT region, SUM(amount) FROM src GROUP BY region"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT012" for f in findings)

    def test_window_function_no_groupby_clean(self, tmp_path):
        """Regression (audit 2026-08-01, failure_taxonomy #3 — dangerous
        remediation advice): `p.find(exp.AggFunc)` descended into a Window,
        so a legal window-function query (`SUM(...) OVER (...)` needs no
        GROUP BY) was flagged with a fix ("add a GROUP BY clause") that
        collapses the per-row window result and changes the query's
        meaning."""
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_gb_window
name: GroupByWindow
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT region, SUM(amount) OVER (PARTITION BY region) AS total FROM src"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT012" for f in findings)

    def test_scalar_subquery_aggregate_no_groupby_clean(self, tmp_path):
        """Regression (audit 2026-08-01): an AggFunc inside a scalar
        subquery projection belongs to that subquery's own self-contained
        scope, not to the outer SELECT's aggregation — a bare column
        alongside it needs no GROUP BY on the outer query."""
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_gb_subquery
name: GroupBySubquery
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SELECT id, (SELECT SUM(amount) FROM other WHERE other.id = src.id) AS total FROM src"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(f.rule_id == "AQ-LINT012" for f in findings)


# ── Resilience ─────────────────────────────────────────────────────────────────


class TestRunLintResilience:
    def test_unparseable_sql_skipped(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_bad_sql
name: BadSQL
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: d.parquet}
  - id: ch
    type: Channel
    label: Ch
    config:
      op: sql
      query: "SEL ECT 1"
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d2.parquet}
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        assert not any(
            f.rule_id.startswith("AQ-LINT01") for f in findings if f.rule_id != "AQ-LINT011"
        )

    def test_findings_sorted_by_rule_id_then_module(self, tmp_path):
        p = bp_yml(
            """
aqueduct: "1.0"
id: lint_sorted
name: Sorted
modules:
  - id: z_mod
    type: Ingress
    label: Z
    config: {format: parquet, path: d.parquet}
  - id: a_mod
    type: Ingress
    label: ""
    config: {format: parquet, path: d2.parquet}
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: d3.parquet}
edges:
  - from: z_mod
    to: sink
  - from: a_mod
    to: sink
"""
        )
        bp = parse(str(p))
        findings = run_lint(bp)
        for i in range(len(findings) - 1):
            a = (findings[i].rule_id, findings[i].module_id or "")
            b = (findings[i + 1].rule_id, findings[i + 1].module_id or "")
            assert a <= b, f"findings not sorted: {a} > {b}"
