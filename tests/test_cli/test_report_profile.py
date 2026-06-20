"""Phase 62 — `aqueduct report --profile` module resource profiling.

Pyspark-free: seeds module_metrics / run_records directly so it runs in the
unit lane (no Spark, no executor import).
"""
from __future__ import annotations

import json

import duckdb
import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit

_MM_DDL = """
CREATE TABLE IF NOT EXISTS module_metrics (
    run_id VARCHAR NOT NULL, module_id VARCHAR NOT NULL,
    records_read BIGINT, bytes_read BIGINT, records_written BIGINT,
    bytes_written BIGINT, duration_ms BIGINT, captured_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS run_records (
    run_id VARCHAR, blueprint_id VARCHAR, started_at TIMESTAMPTZ
);
"""


def _conn(store_dir):
    c = duckdb.connect(str(store_dir / "observability.db"))
    c.execute(_MM_DDL)
    return c


def _mm(c, run, mod, rw, bw, dur):
    c.execute(
        "INSERT INTO module_metrics (run_id, module_id, records_read, bytes_read, "
        "records_written, bytes_written, duration_ms, captured_at) "
        "VALUES (?,?,?,?,?,?,?, now())",
        [run, mod, None, None, rw, bw, dur],
    )


# ── run-scoped profile ──────────────────────────────────────────────────────

def test_profile_run_table_heaviest_first(tmp_path):
    c = _conn(tmp_path)
    _mm(c, "r1", "light", 10, 100, 50)
    _mm(c, "r1", "heavy", 1000, 999999, 5000)
    c.close()

    res = CliRunner().invoke(cli, ["report", "r1", "--profile", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    # heavy (5000ms) must appear before light (50ms)
    assert res.output.index("heavy") < res.output.index("light")
    assert "TOTAL" in res.output


def test_profile_run_json_shape(tmp_path):
    c = _conn(tmp_path)
    _mm(c, "r1", "a", 10, 100, 50)
    _mm(c, "r1", "b", 20, 200, 150)
    c.close()

    res = CliRunner().invoke(cli, ["report", "r1", "--profile", "--store-dir", str(tmp_path), "--format", "json"])
    assert res.exit_code == 0, res.output
    out = json.loads(res.output)
    assert out["total_duration_ms"] == 200
    assert [m["module_id"] for m in out["modules"]] == ["b", "a"]  # desc by duration


def test_profile_run_no_metrics(tmp_path):
    _conn(tmp_path).close()
    res = CliRunner().invoke(cli, ["report", "missing", "--profile", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0
    assert "No module_metrics" in res.output


# ── trend profile ───────────────────────────────────────────────────────────

def test_profile_trend_flags_slowdown(tmp_path):
    c = _conn(tmp_path)
    for i, (run, ts) in enumerate([("r1", "2026-06-17"), ("r2", "2026-06-18"), ("r3", "2026-06-19")]):
        c.execute("INSERT INTO run_records VALUES (?, 'bp', ?)", [run, ts + "T00:00:00+00:00"])
    _mm(c, "r1", "m", 100, 100, 100)
    _mm(c, "r2", "m", 100, 100, 120)
    _mm(c, "r3", "m", 100, 100, 5000)  # latest run 5000 >> avg → slowdown
    c.close()

    res = CliRunner().invoke(cli, ["report", "--profile", "--blueprint", "bp", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "slowdown" in res.output


def test_profile_requires_run_or_blueprint(tmp_path):
    _conn(tmp_path).close()
    res = CliRunner().invoke(cli, ["report", "--profile", "--store-dir", str(tmp_path)])
    assert res.exit_code != 0
    assert "needs RUN_ID" in res.output
