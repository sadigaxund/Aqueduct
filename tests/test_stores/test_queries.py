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


def _cfg(path=None, backend="duckdb"):  # None = default routing root (2.0)
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


# ── run_detail: multi-row-per-module_id merge (Phase 81/82 §10.9 Handoff) ───
#
# A synthetic Handoff module gets TWO `module_metrics` rows under the SAME
# module_id — the write side (bytes_written, its own duration) and the read
# side (bytes_read, its own duration), never both fields on one row. Every
# ordinary module still only ever gets one row. `run_detail` used to build
# its `by_id` map with a plain dict comprehension keyed by module_id, so the
# SECOND row silently overwrote the first — a Handoff module's profile
# reported only whichever side's row happened to come last, dropping the
# other side's bytes entirely.


class _DuckDBStore:
    """Minimal store matching the ``store.connect() -> RelationalCursor``
    contract ``run_detail`` needs, built directly to avoid pulling in the
    full Surveyor (same pattern as ``tests/test_executor/test_spill.py``)."""

    def __init__(self, path):
        self._path = path

    def connect(self):
        import contextlib

        from aqueduct.stores.base import RelationalCursor

        @contextlib.contextmanager
        def _cm():
            conn = duckdb.connect(str(self._path))
            try:
                yield RelationalCursor(conn.cursor(), paramstyle="qmark")
            finally:
                conn.close()

        return _cm()


def _seed_module_metrics_store(path):
    from aqueduct.executor.models import MODULE_METRICS_DDL

    c = duckdb.connect(str(path))
    c.execute(_DDL)
    c.execute(MODULE_METRICS_DDL)
    c.execute(
        "INSERT INTO run_records VALUES (?,?,?,?::timestamptz, now(), ?)",
        ["r1", "bp", "success", "2026-06-18",
         '[{"module_id": "a", "status": "success"}, '
         '{"module_id": "a__handoff__b", "status": "success"}, '
         '{"module_id": "b", "status": "success"}]'],
    )
    # Ordinary module — one row, as always.
    c.execute(
        "INSERT INTO module_metrics VALUES ('r1', 'a', NULL, NULL, 5, 100, 50, now())"
    )
    # Handoff module — write-side row (bytes_written, duration 30) then
    # read-side row (bytes_read, duration 5) for the SAME module_id.
    c.execute(
        "INSERT INTO module_metrics VALUES ('r1', 'a__handoff__b', NULL, NULL, NULL, 519, 30, now())"
    )
    c.execute(
        "INSERT INTO module_metrics VALUES ('r1', 'a__handoff__b', NULL, 519, NULL, NULL, 5, now())"
    )
    c.close()


# ── gate_rejection_rates: status vocabulary is pass/warn/fail/skip/
# not_applicable, never 'passed' (Phase 81/82 cross-engine remediation
# fixed a query that compared against a value no gate ever writes, so
# EVERY row — including genuine passes — counted as a rejection). ─────────


def _seed_patch_simulation(path, rows):
    """rows: list[(gate, status)]."""
    c = duckdb.connect(str(path))
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS patch_simulation (
            id           VARCHAR PRIMARY KEY,
            run_id       VARCHAR,
            blueprint_id VARCHAR,
            patch_id     VARCHAR NOT NULL,
            gate         VARCHAR NOT NULL,
            status       VARCHAR NOT NULL,
            detail       VARCHAR,
            sample_rows  BIGINT,
            duration_ms  BIGINT,
            recorded_at  VARCHAR NOT NULL
        )
        """
    )
    for i, (gate, status) in enumerate(rows):
        c.execute(
            "INSERT INTO patch_simulation VALUES "
            "(?, 'r1', 'bp', ?, ?, ?, NULL, NULL, NULL, '2026-08-09')",
            [f"sim-{i}", f"patch-{i}", gate, status],
        )
    c.close()


def test_gate_rejection_rates_counts_only_fail(tmp_path, monkeypatch):
    """A realistic spread across two gates: pass/warn/fail/skip/not_applicable.

    Only 'fail' rows are rejections. 'warn' never blocks (lineage warn never
    blocks; explain warn only blocks behind a config flag this table doesn't
    capture). 'skip' is caller-treated acceptance
    (`gates_passed = sandbox_res.status in ("pass", "skip")`).
    'not_applicable' means the gate had nothing to check.
    """
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".aqueduct" / "observability" / "bp"
    root.mkdir(parents=True)
    _seed_patch_simulation(root / "observability.db", [
        ("lineage", "pass"),
        ("lineage", "warn"),
        ("lineage", "fail"),
        ("lineage", "not_applicable"),
        ("sandbox", "pass"),
        ("sandbox", "skip"),
        ("sandbox", "fail"),
        ("sandbox", "fail"),
    ])
    rates = q.gate_rejection_rates(_cfg())
    assert rates == {"lineage": 1, "sandbox": 2}


def test_run_detail_merges_a_handoff_modules_two_metrics_rows(tmp_path):
    path = tmp_path / "observability.db"
    _seed_module_metrics_store(path)
    store = _DuckDBStore(path)

    detail = q.run_detail(store, "r1")
    assert detail is not None
    profile_by_id = {p.module_id: p for p in detail.profile}

    handoff = profile_by_id["a__handoff__b"]
    assert handoff.bytes_written == 519
    assert handoff.bytes_read == 519
    assert handoff.duration_ms == 35  # 30 (write) + 5 (read), summed

    ordinary = profile_by_id["a"]
    assert ordinary.bytes_written == 100
    assert ordinary.records_written == 5
    assert ordinary.duration_ms == 50
