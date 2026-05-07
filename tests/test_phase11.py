"""Phase 11 tests: Missing CLI Commands (report, lineage, signal, heal)."""

from __future__ import annotations

import json
from pathlib import Path
import pytest
from click.testing import CliRunner
import duckdb

from aqueduct.cli import cli
from aqueduct.surveyor.surveyor import _SIGNAL_OVERRIDES_DDL

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def obs_db(tmp_path):
    db_path = tmp_path / "obs.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE run_records (
            run_id VARCHAR,
            blueprint_id VARCHAR,
            status VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            module_results JSON
        )
    """)
    conn.execute("""
        CREATE TABLE probe_signals (
            run_id VARCHAR,
            probe_id VARCHAR,
            passed BOOLEAN,
            value DOUBLE,
            error_message VARCHAR,
            recorded_at TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE failure_contexts (
            run_id VARCHAR PRIMARY KEY,
            blueprint_id VARCHAR,
            failed_module VARCHAR,
            error_message VARCHAR,
            stack_trace VARCHAR,
            manifest_json JSON,
            provenance_json JSON,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        )
    """)
    conn.execute(_SIGNAL_OVERRIDES_DDL)
    conn.close()
    return db_path

@pytest.fixture
def lineage_db(tmp_path):
    db_path = tmp_path / "lineage.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE column_lineage (
            blueprint_id VARCHAR,
            run_id VARCHAR,
            channel_id VARCHAR,
            output_column VARCHAR,
            source_table VARCHAR,
            source_column VARCHAR
        )
    """)
    conn.close()
    return db_path

@pytest.fixture
def aq_config(tmp_path, obs_db, lineage_db):
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"""
aqueduct_config: "1.0"
stores:
  obs: {{ path: {obs_db} }}
  lineage: {{ path: {lineage_db} }}
""")
    return config_path

# ── aqueduct report ───────────────────────────────────────────────────────────

def test_report_valid_run(runner, obs_db, aq_config):
    conn = duckdb.connect(str(obs_db))
    conn.execute("INSERT INTO run_records VALUES ('run1', 'bp1', 'success', '2026-01-01 10:00:00', '2026-01-01 10:05:00', '[]')")
    conn.close()
    
    result = runner.invoke(cli, ["report", "run1", "--config", str(aq_config)])
    assert result.exit_code == 0
    assert "run_id=run1" in result.output
    assert "bp1" in result.output

def test_report_json_format(runner, obs_db, aq_config):
    conn = duckdb.connect(str(obs_db))
    conn.execute("INSERT INTO run_records VALUES ('run1', 'bp1', 'success', '2026-01-01 10:00:00', '2026-01-01 10:05:00', '[]')")
    conn.close()
    
    result = runner.invoke(cli, ["report", "run1", "--format", "json", "--config", str(aq_config)])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["run_id"] == "run1"

def test_report_unknown_run(runner, obs_db, aq_config):
    result = runner.invoke(cli, ["report", "unknown", "--config", str(aq_config)])
    assert result.exit_code == 1
    assert "not found" in result.output

# ── aqueduct lineage ──────────────────────────────────────────────────────────

def test_lineage_output(runner, lineage_db, aq_config):
    conn = duckdb.connect(str(lineage_db))
    conn.execute("INSERT INTO column_lineage VALUES ('bp1', 'run1', 'chan1', 'col1', 'src1', 'scol1')")
    conn.close()
    
    result = runner.invoke(cli, ["lineage", "bp1", "--config", str(aq_config)])
    assert result.exit_code == 0
    assert "chan1" in result.output
    assert "col1" in result.output

def test_lineage_filter_from(runner, lineage_db, aq_config):
    conn = duckdb.connect(str(lineage_db))
    conn.execute("INSERT INTO column_lineage VALUES ('bp1', 'run1', 'chan1', 'col1', 'src1', 'scol1')")
    conn.execute("INSERT INTO column_lineage VALUES ('bp1', 'run1', 'chan2', 'col2', 'src2', 'scol2')")
    conn.close()
    
    result = runner.invoke(cli, ["lineage", "bp1", "--from", "src1", "--config", str(aq_config)])
    assert "chan1" in result.output
    assert "chan2" not in result.output

def test_lineage_json(runner, lineage_db, aq_config):
    conn = duckdb.connect(str(lineage_db))
    conn.execute("INSERT INTO column_lineage VALUES ('bp1', 'run1', 'chan1', 'col1', 'src1', 'scol1')")
    conn.close()
    
    result = runner.invoke(cli, ["lineage", "bp1", "--format", "json", "--config", str(aq_config)])
    data = json.loads(result.output)
    assert len(data) == 1
    assert data[0]["channel_id"] == "chan1"

# ── aqueduct signal ───────────────────────────────────────────────────────────

def test_signal_set_override(runner, obs_db, aq_config):
    result = runner.invoke(cli, ["signal", "probe1", "--value", "false", "--config", str(aq_config)])
    assert result.exit_code == 0
    
    conn = duckdb.connect(str(obs_db))
    row = conn.execute("SELECT passed FROM signal_overrides WHERE signal_id = 'probe1'").fetchone()
    conn.close()
    assert row[0] is False

def test_signal_clear_override(runner, obs_db, aq_config):
    conn = duckdb.connect(str(obs_db))
    conn.execute("INSERT INTO signal_overrides VALUES ('probe1', false, 'reason', '2026-01-01')")
    conn.close()
    
    result = runner.invoke(cli, ["signal", "probe1", "--value", "true", "--config", str(aq_config)])
    assert result.exit_code == 0
    
    conn = duckdb.connect(str(obs_db))
    row = conn.execute("SELECT COUNT(*) FROM signal_overrides WHERE signal_id = 'probe1'").fetchone()
    conn.close()
    assert row[0] == 0

def test_signal_error_msg(runner, obs_db, aq_config):
    result = runner.invoke(cli, ["signal", "probe1", "--error", "Broken", "--config", str(aq_config)])
    assert result.exit_code == 0
    conn = duckdb.connect(str(obs_db))
    row = conn.execute("SELECT passed, error_message FROM signal_overrides").fetchone()
    conn.close()
    assert row[0] is False
    assert row[1] == "Broken"

def test_signal_conflicting_flags(runner, aq_config):
    result = runner.invoke(cli, ["signal", "p1", "--value", "true", "--error", "Msg", "--config", str(aq_config)])
    assert result.exit_code == 1
    assert "cannot combine" in result.output.lower()

# ── aqueduct heal ─────────────────────────────────────────────────────────────

def test_heal_stages_patch(runner, obs_db, aq_config, tmp_path, monkeypatch):
    from aqueduct.patch.grammar import PatchSpec, ReplaceModuleLabelOp
    
    # Setup a failed run record
    conn = duckdb.connect(str(obs_db))
    results = json.dumps([{"module_id": "m1", "status": "error", "error": "Boom"}])
    conn.execute(f"INSERT INTO run_records VALUES ('run1', 'bp1', 'error', '2026-01-01', '2026-01-01', '{results}')")
    conn.execute("""
        INSERT INTO failure_contexts 
        VALUES ('run1', 'bp1', 'm1', 'Boom', 'trace', '{}', null, '2026-01-01', '2026-01-01')
    """)
    conn.close()
    
    # Blueprint file
    bp_path = tmp_path / "bp1.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: bp1\nmodules: []\nedges: []")
    
    # Mock LLM
    mock_patch = PatchSpec(
        patch_id="p1", 
        rationale="Fixed", 
        operations=[ReplaceModuleLabelOp(op="replace_module_label", module_id="m1", label="Fixed")]
    )
    monkeypatch.setattr("aqueduct.surveyor.llm.generate_llm_patch", lambda *args, **kwargs: mock_patch)
    
    result = runner.invoke(cli, ["heal", "run1", "--config", str(aq_config), "--patches-dir", str(tmp_path / "patches")])
    assert result.exit_code == 0
    assert "patch staged" in result.output
    patches = list((tmp_path / "patches" / "pending").glob("*_p1.json"))
    assert len(patches) == 1

def test_heal_unknown_run(runner, obs_db, aq_config):
    result = runner.invoke(cli, ["heal", "unknown", "--config", str(aq_config)])
    assert result.exit_code == 1
    assert "no failure record" in result.output
