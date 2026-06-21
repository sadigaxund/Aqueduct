"""Phase 68 — fleet (cross-run + cross-blueprint) read-time aggregates.

Backend-agnostic, no textual/pyspark. Seeds per-blueprint DuckDB files under a
routing root and checks the read-time merge in Python (no materialised copies).
"""
from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

from aqueduct.stores import queries as q

pytestmark = pytest.mark.unit

_DDL = """
CREATE TABLE IF NOT EXISTS run_records (
    run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR,
    started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results VARCHAR
);
CREATE TABLE IF NOT EXISTS healing_outcomes (
    blueprint_id VARCHAR, run_id VARCHAR, failure_category VARCHAR
);
"""


def _cfg(path=".aqueduct/observability.db", backend="duckdb"):
    return SimpleNamespace(
        stores=SimpleNamespace(observability=SimpleNamespace(path=path, backend=backend))
    )


def _seed(path, blueprint_id, rows, heals=None):
    """rows: list[(run_id, status, started_at)]; heals: list[(run_id, category)]."""
    c = duckdb.connect(str(path))
    c.execute(_DDL)
    for run_id, status, started in rows:
        c.execute(
            "INSERT INTO run_records VALUES (?,?,?,?::timestamptz, now(), '[]')",
            [run_id, blueprint_id, status, started],
        )
    for run_id, cat in (heals or []):
        c.execute("INSERT INTO healing_outcomes VALUES (?,?,?)", [blueprint_id, run_id, cat])
    c.close()


def _routed_cfg(tmp_path, monkeypatch):
    """Per-blueprint routing root with two blueprints seeded."""
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".aqueduct" / "observability"
    (root / "alpha").mkdir(parents=True)
    (root / "beta").mkdir(parents=True)
    _seed(root / "alpha" / "observability.db", "alpha",
          [("a1", "success", "2026-06-18"), ("a2", "error", "2026-06-19")],
          heals=[("a2", "SchemaError")])
    _seed(root / "beta" / "observability.db", "beta",
          [("b1", "success", "2026-06-19")])
    return _cfg()


def test_fleet_summary_merges_blueprints(tmp_path, monkeypatch):
    cfg = _routed_cfg(tmp_path, monkeypatch)
    summ = {s.blueprint_id: s for s in q.fleet_summary(cfg)}
    assert set(summ) == {"alpha", "beta"}
    assert summ["alpha"].runs == 2
    assert summ["alpha"].successes == 1 and summ["alpha"].errors == 1
    assert summ["alpha"].success_rate == 0.5
    assert summ["alpha"].heal_attempts == 1
    assert summ["beta"].runs == 1 and summ["beta"].success_rate == 1.0


def test_fleet_summary_sorted_by_last_run_desc(tmp_path, monkeypatch):
    cfg = _routed_cfg(tmp_path, monkeypatch)
    rows = q.fleet_summary(cfg)
    # alpha's last run (06-19) ties beta (06-19); both present, most-recent first
    assert rows[0].last_run >= rows[-1].last_run


def test_runs_over_time_merges_days(tmp_path, monkeypatch):
    cfg = _routed_cfg(tmp_path, monkeypatch)
    counts = q.runs_over_time(cfg)
    by_day = {}
    for dc in counts:
        by_day[dc.day] = by_day.get(dc.day, 0) + dc.count
    assert by_day["2026-06-18"] == 1   # alpha a1
    assert by_day["2026-06-19"] == 2   # alpha a2 + beta b1


def test_failure_categories(tmp_path, monkeypatch):
    cfg = _routed_cfg(tmp_path, monkeypatch)
    dist = q.failure_categories(cfg)
    assert dist.get("SchemaError") == 1


def test_fleet_summary_empty_when_no_stores(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert q.fleet_summary(_cfg()) == []


def test_discover_location_only_dir(tmp_path, monkeypatch):
    """A non-default path that is a DIRECTORY routes to per-blueprint files under it."""
    monkeypatch.chdir(tmp_path)
    base = tmp_path / "custom_obs"
    (base / "alpha").mkdir(parents=True)
    duckdb.connect(str(base / "alpha" / "observability.db")).close()
    handles = q.discover_stores(_cfg(path=str(base)))
    assert "alpha" in [h.label for h in handles]
