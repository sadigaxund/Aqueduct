"""Phase 68 — Streamlit dashboard app smoke test.

Uses Streamlit's headless AppTest harness to actually RUN the dashboard script
against a seeded observability store and assert it renders with no uncaught
exception. Skips when the optional `dashboard` extra (streamlit) is absent, so it
runs in CI / when `pip install -e .[dashboard]` is present but never blocks a base
install — same pattern as the integration-marked store tests.
"""
from __future__ import annotations

import json

import duckdb
import pytest

pytestmark = pytest.mark.unit

AppTest = pytest.importorskip("streamlit.testing.v1").AppTest
pytest.importorskip("plotly")

from pathlib import Path  # noqa: E402

_APP = str(Path(__file__).resolve().parent.parent.parent / "aqueduct" / "dashboard" / "app.py")

_DDL = """
CREATE TABLE run_records (
    run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR,
    started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results VARCHAR
);
CREATE TABLE module_metrics (
    run_id VARCHAR, module_id VARCHAR, records_read BIGINT, bytes_read BIGINT,
    records_written BIGINT, bytes_written BIGINT, duration_ms BIGINT, captured_at TIMESTAMPTZ
);
CREATE TABLE column_lineage (
    blueprint_id VARCHAR, run_id VARCHAR, channel_id VARCHAR,
    output_column VARCHAR, source_table VARCHAR, source_column VARCHAR, captured_at TIMESTAMPTZ
);
"""


@pytest.fixture
def seeded_store_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    p = tmp_path / "observability.db"
    c = duckdb.connect(str(p))
    c.execute(_DDL)
    mr = json.dumps([{"module_id": "src", "status": "success", "error": ""},
                     {"module_id": "sink", "status": "success", "error": ""}])
    c.execute("INSERT INTO run_records VALUES ('r1','bp.demo','success', now(), now(), ?)", [mr])
    c.execute("INSERT INTO module_metrics VALUES ('r1','src',NULL,NULL,10,100,50,now())")
    c.execute("INSERT INTO module_metrics VALUES ('r1','sink',NULL,NULL,10,120,80,now())")
    c.execute("INSERT INTO column_lineage VALUES ('bp.demo','r1','ch','amount','src','raw_amount', now())")
    c.close()
    monkeypatch.setenv("AQ_DASH_STORE_DIR", str(tmp_path))
    monkeypatch.delenv("AQ_DASH_CONFIG", raising=False)
    return tmp_path


def test_app_renders_without_exception(seeded_store_dir):
    at = AppTest.from_file(_APP, default_timeout=30).run()
    assert not at.exception
    assert at.title  # the "Aqueduct" title rendered
    # Fleet tab KPIs present
    assert any("Blueprint" in m.label for m in at.metric)


def test_app_handles_empty_stores(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)        # no .aqueduct/observability anywhere
    monkeypatch.setenv("AQ_DASH_STORE_DIR", str(tmp_path))
    monkeypatch.delenv("AQ_DASH_CONFIG", raising=False)
    at = AppTest.from_file(_APP, default_timeout=30).run()
    assert not at.exception  # empty fleet must not crash
