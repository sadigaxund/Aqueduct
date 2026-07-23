"""Backend conformance matrix (Storage Integrity).

Runs the observability READ commands against each relational backend from one
place, so "works on every backend" is a CI-proven fact, not a promise. The
DuckDB lane runs everywhere; the Postgres lane runs in the stores-tests CI lane
(real PG service) and skips locally without ``AQ_PG_DSN``.

Known gaps are encoded as ``xfail(strict=True)`` so they FAIL the build the
moment they're fixed — forcing the marker's removal. That makes the remaining
work a finite, self-updating list instead of surprise slivers.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.config import load_config
from aqueduct.stores.base import get_stores
from tests.conftest import _pg_dsn, _pg_is_reachable

# Portable DDL (valid on both DuckDB and Postgres).
_DDL = [
    """CREATE TABLE IF NOT EXISTS run_records (
        run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR,
        started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS module_metrics (
        run_id VARCHAR, module_id VARCHAR, records_read BIGINT, bytes_read BIGINT,
        records_written BIGINT, bytes_written BIGINT, duration_ms BIGINT,
        captured_at TIMESTAMPTZ)""",
    """CREATE TABLE IF NOT EXISTS column_lineage (
        blueprint_id VARCHAR, run_id VARCHAR, channel_id VARCHAR,
        output_column VARCHAR, source_table VARCHAR, source_column VARCHAR,
        captured_at TIMESTAMPTZ)""",
    # healing_outcomes has NO blueprint_id (matches the real schema) — fleet
    # aggregates must reach blueprint via a run_records join.
    """CREATE TABLE IF NOT EXISTS healing_outcomes (
        id VARCHAR, run_id VARCHAR, failure_category VARCHAR,
        resolution VARCHAR, run_success_after_patch BOOLEAN)""",
    """CREATE TABLE IF NOT EXISTS probe_signals (
        run_id VARCHAR, probe_id VARCHAR, signal_type VARCHAR,
        payload JSON, captured_at TIMESTAMPTZ)""",
]

_CONF_TABLES = ("run_records", "module_metrics", "column_lineage",
                 "healing_outcomes", "probe_signals")

# `report --trend` (aqueduct/cli/observability.py) defaults its lookback
# window to `now() - 30 days` when `--since` isn't passed. A fixed calendar
# date here would silently age out of that window as real time passes —
# exactly what broke `test_report_trend[postgres]` in CI once "today" moved
# past 2026-07-20 (30 days after the previously hardcoded 2026-06-20). Anchor
# to "now" instead so this fixture never rots.
_TS = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


@pytest.fixture(
    params=[
        pytest.param("duckdb", id="duckdb"),
        pytest.param(
            "postgres",
            id="postgres",
            marks=[
                pytest.mark.integration,
                pytest.mark.skipif(not _pg_is_reachable(), reason="Postgres not reachable (set AQ_PG_DSN)"),
            ],
        ),
    ]
)
def seeded_cfg(request, tmp_path):
    """Write an aqueduct.yml for the backend, seed one run + metrics + lineage,
    yield the config path. Cleans up Postgres rows afterward."""
    backend = request.param
    if backend == "duckdb":
        obs_path = str(tmp_path / "obs")  # 2.0: routing base DIRECTORY
    else:
        obs_path = _pg_dsn()

    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(
        "aqueduct_config: \"1.0\"\n"
        "stores:\n"
        "  observability:\n"
        f"    backend: {backend}\n"
        f"    path: \"{obs_path}\"\n"
    )

    cfg = load_config(cfg_file)
    # Seed at the blueprint-routed location (2.0: <base>/<blueprint_id>/…) so
    # the CLI's blueprint-scoped reads resolve the same file.
    store = get_stores(cfg, blueprint_id="conf_bp").observability
    with store.connect() as cur:
        for ddl in _DDL:
            cur.execute(ddl)
        if backend == "postgres":
            for tbl in _CONF_TABLES:
                try:
                    cur.execute(f"TRUNCATE TABLE {tbl} CASCADE")
                except Exception:
                    pass
        cur.execute(
            "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at, module_results) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ["rc1", "conf_bp", "success", _TS, _TS,
             json.dumps([{"module_id": "m1", "status": "success", "error": ""}])],
        )
        cur.execute(
            "INSERT INTO module_metrics (run_id, module_id, records_written, bytes_written, duration_ms, captured_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ["rc1", "m1", 100, 2048, 1234, _TS],
        )
        cur.execute(
            "INSERT INTO column_lineage (blueprint_id, run_id, channel_id, output_column, source_table, source_column, captured_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ["conf_bp", "rc1", "ch1", "out_col", "src_tbl", "src_col", _TS],
        )
        cur.execute(
            "INSERT INTO healing_outcomes (id, run_id, failure_category, resolution, run_success_after_patch) "
            "VALUES (?, ?, ?, ?, ?)",
            ["ho1", "rc1", "SchemaError", "llm", True],
        )
        cur.execute(
            "INSERT INTO probe_signals (run_id, probe_id, signal_type, payload, captured_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ["rc1", "p1", "null_rates", json.dumps({"null_rates": {"out_col": 0.25}}), _TS],
        )

    yield cfg_file, backend

    if backend == "postgres":
        with get_stores(cfg).observability.connect() as cur:
            for tbl in _CONF_TABLES:
                try:
                    cur.execute(f"TRUNCATE TABLE {tbl} CASCADE")
                except Exception:
                    pass


def _run(cfg_file, *args):
    return CliRunner().invoke(cli, [*args, "--config", str(cfg_file)])


# ── read commands: must work on every relational backend ─────────────────────

def test_report_run(seeded_cfg):
    cfg_file, _ = seeded_cfg
    res = _run(cfg_file, "report", "rc1")
    assert res.exit_code == 0, res.output
    assert "rc1" in res.output and "m1" in res.output


def test_runs(seeded_cfg):
    cfg_file, _ = seeded_cfg
    res = _run(cfg_file, "runs")
    assert res.exit_code == 0, res.output
    assert "rc1" in res.output


def test_report_profile(seeded_cfg):
    cfg_file, _ = seeded_cfg
    res = _run(cfg_file, "report", "rc1", "--profile")
    assert res.exit_code == 0, res.output
    assert "m1" in res.output


def test_lineage(seeded_cfg):
    cfg_file, _ = seeded_cfg
    res = _run(cfg_file, "lineage", "conf_bp")
    assert res.exit_code == 0, res.output
    assert "out_col" in res.output


def test_report_trend(seeded_cfg):
    """`report --trend` must work identically on DuckDB and Postgres (the JSON
    payload is now exploded in Python, no DuckDB-only json_each/json_extract)."""
    cfg_file, _ = seeded_cfg
    res = _run(cfg_file, "report", "--trend", "out_col", "--blueprint", "conf_bp")
    assert res.exit_code == 0, res.output
    assert "out_col" in res.output and "0.25" in res.output  # the seeded null-rate


# ── fleet query layer: must produce identical results on every backend ────────

def test_fleet_summary_conformance(seeded_cfg):
    from aqueduct.stores import queries as q
    cfg_file, _ = seeded_cfg
    cfg = load_config(cfg_file)
    summ = {s.blueprint_id: s for s in q.fleet_summary(cfg)}
    assert "conf_bp" in summ
    s = summ["conf_bp"]
    assert s.runs == 1 and s.successes == 1
    assert s.heal_attempts == 1   # heal count via run_records join (no blueprint_id col)


def test_runs_over_time_conformance(seeded_cfg):
    from aqueduct.stores import queries as q
    cfg_file, _ = seeded_cfg
    counts = q.runs_over_time(load_config(cfg_file))
    assert any(d.count >= 1 and d.status == "success" for d in counts)


def test_failure_categories_conformance(seeded_cfg):
    from aqueduct.stores import queries as q
    dist = q.failure_categories(load_config(seeded_cfg[0]))
    assert dist.get("SchemaError") == 1


def test_heal_coverage_conformance(seeded_cfg):
    from aqueduct.stores import queries as q
    assert q.heal_coverage(load_config(seeded_cfg[0])).get("llm") == 1
