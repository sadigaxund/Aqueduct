"""Phase 67 — `aqueduct report --format html` self-contained run report.

Pyspark-free: seeds run_records + module_metrics directly (unit lane).
"""
from __future__ import annotations

import json

import duckdb
import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit

_DDL = """
CREATE TABLE IF NOT EXISTS run_records (
    run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR,
    started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results VARCHAR
);
CREATE TABLE IF NOT EXISTS module_metrics (
    run_id VARCHAR NOT NULL, module_id VARCHAR NOT NULL,
    records_read BIGINT, bytes_read BIGINT, records_written BIGINT,
    bytes_written BIGINT, duration_ms BIGINT, captured_at TIMESTAMPTZ NOT NULL
);
"""


def _seed(store_dir, module_results):
    c = duckdb.connect(str(store_dir / "observability.db"))
    c.execute(_DDL)
    c.execute(
        "INSERT INTO run_records VALUES (?, 'bp', 'success', now(), now(), ?)",
        ["r1", json.dumps(module_results)],
    )
    c.execute(
        "INSERT INTO module_metrics VALUES ('r1','heavy',NULL,NULL,1000,999999,5000,now())"
    )
    c.execute(
        "INSERT INTO module_metrics VALUES ('r1','light',NULL,NULL,10,100,50,now())"
    )
    c.close()


def test_html_report_renders(tmp_path):
    _seed(tmp_path, [{"module_id": "heavy", "status": "success", "error": ""},
                     {"module_id": "light", "status": "success", "error": ""}])
    res = CliRunner().invoke(cli, ["report", "r1", "--format", "html", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    out = res.output
    assert "<!DOCTYPE html>" in out
    assert "r1" in out and "bp" in out
    assert "Resource profile" in out
    # heaviest module first in the profile table
    assert out.index("heavy") < out.index("light")


def test_html_report_escapes_error(tmp_path):
    _seed(tmp_path, [{"module_id": "m", "status": "error", "error": "<script>alert(1)</script>"}])
    res = CliRunner().invoke(cli, ["report", "r1", "--format", "html", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "&lt;script&gt;" in res.output
    assert "<script>alert(1)</script>" not in res.output  # must be escaped, not raw


def test_html_report_no_metrics_ok(tmp_path):
    # run_records present, module_metrics empty → profile shows placeholder, no crash
    c = duckdb.connect(str(tmp_path / "observability.db"))
    c.execute(_DDL)
    c.execute("INSERT INTO run_records VALUES ('r1','bp','success', now(), now(), ?)",
              [json.dumps([{"module_id": "m", "status": "success", "error": ""}])])
    c.close()
    res = CliRunner().invoke(cli, ["report", "r1", "--format", "html", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "no module_metrics" in res.output
