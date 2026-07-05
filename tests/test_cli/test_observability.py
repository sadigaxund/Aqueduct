"""Tests for the CLI layer: Observability commands (report, lineage, signal, heal)."""

from __future__ import annotations
import json
from pathlib import Path
import pytest
pytestmark = pytest.mark.integration
from click.testing import CliRunner
import duckdb

from aqueduct.cli import cli
from aqueduct.surveyor.surveyor import _SIGNAL_OVERRIDES_DDL

FIXTURES = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def obs_db(tmp_path):
    # 2.0: config path is a routing BASE dir — seed the routed per-blueprint file.
    db_path = tmp_path / "obs" / "bp" / "observability.db"
    db_path.parent.mkdir(parents=True)
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE run_records (run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP, module_results JSON)")
    conn.execute("CREATE TABLE failure_contexts (run_id VARCHAR PRIMARY KEY, blueprint_id VARCHAR, failed_module VARCHAR, error_message VARCHAR, stack_trace VARCHAR, manifest_json VARCHAR, provenance_json VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP)")
    conn.execute(_SIGNAL_OVERRIDES_DDL)
    conn.close()
    return db_path


@pytest.fixture
def lineage_obs_db(tmp_path):
    """Phase 38: lineage merged into observability. CLI reads from observability.db."""
    store_dir = tmp_path / "obs_store"
    store_dir.mkdir()
    db_path = store_dir / "observability.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE column_lineage (blueprint_id VARCHAR, run_id VARCHAR, channel_id VARCHAR, output_column VARCHAR, source_table VARCHAR, source_column VARCHAR)")
    conn.execute("INSERT INTO column_lineage VALUES ('bp1', 'run1', 'chan1', 'col1', 'src1', 'scol1')")
    conn.close()
    return store_dir


@pytest.fixture
def aq_config(tmp_path, obs_db):
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"aqueduct_config: '1.0'\nstores:\n  observability: {{ path: {obs_db.parent.parent} }}\n")
    return config_path


class TestReportCommand:
    def test_report_valid_run(self, cli_runner, obs_db, aq_config):
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO run_records VALUES ('run1', 'bp1', 'success', '2026-01-01 10:00:00', '2026-01-01 10:05:00', '[]')")
        conn.close()
        result = cli_runner.invoke(cli, ["report", "run1", "--config", str(aq_config)])
        assert result.exit_code == 0
        assert "run_id=run1" in result.output


class TestLineageCommand:
    def test_lineage_output(self, cli_runner, lineage_obs_db, aq_config):
        """CLI reads from <store_dir>/observability.db (Phase 38)."""
        result = cli_runner.invoke(cli, ["lineage", "bp1", "--store-dir", str(lineage_obs_db)])
        assert result.exit_code == 0, result.output
        assert "chan1" in result.output


class TestSignalCommand:
    def test_signal_set_override(self, cli_runner, obs_db, aq_config):
        result = cli_runner.invoke(cli, ["signal", "probe1", "--value", "false", "--blueprint", "bp", "--config", str(aq_config)])
        assert result.exit_code == 0
        conn = duckdb.connect(str(obs_db))
        row = conn.execute("SELECT passed FROM signal_overrides WHERE signal_id = 'probe1'").fetchone()
        conn.close()
        assert row[0] is False

    def test_signal_clear_override(self, cli_runner, obs_db, aq_config):
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO signal_overrides VALUES ('probe1', false, 'reason', '2026-01-01')")
        conn.close()
        result = cli_runner.invoke(cli, ["signal", "probe1", "--value", "true", "--blueprint", "bp", "--config", str(aq_config)])
        assert result.exit_code == 0
        conn = duckdb.connect(str(obs_db))
        count = conn.execute("SELECT COUNT(*) FROM signal_overrides WHERE signal_id = 'probe1'").fetchone()[0]
        conn.close()
        assert count == 0

    def test_close_gate_with_error_msg(self, cli_runner, obs_db, aq_config):
        result = cli_runner.invoke(cli, ["signal", "my_probe", "--error", "Stale source", "--blueprint", "bp", "--config", str(aq_config)])
        assert result.exit_code == 0
        conn = duckdb.connect(str(obs_db))
        row = conn.execute("SELECT passed, error_message FROM signal_overrides WHERE signal_id = 'my_probe'").fetchone()
        conn.close()
        assert row[0] is False
        assert "Stale source" in row[1]

    def test_conflicting_flags_exit_1(self, cli_runner, aq_config):
        result = cli_runner.invoke(cli, ["signal", "p", "--value", "true", "--error", "x", "--blueprint", "bp", "--config", str(aq_config)])
        from aqueduct.exit_codes import USAGE_ERROR
        assert result.exit_code == USAGE_ERROR

    def test_no_flags_shows_status(self, cli_runner, aq_config):
        result = cli_runner.invoke(cli, ["signal", "my_probe", "--blueprint", "bp", "--config", str(aq_config)])
        assert result.exit_code == 0
        assert "no persistent override" in result.output


class TestRunsCommand:
    def test_runs_no_db(self, cli_runner, tmp_path):
        """aqueduct runs with no obs.db -> prints 'No runs found' without error."""
        conf = tmp_path / "aq_no_db.yml"
        conf.write_text("stores:\n  observability: { path: /tmp/ghost_obs_dir }\n")
        result = cli_runner.invoke(cli, ["runs", "--config", str(conf)])
        assert result.exit_code == 0
        assert "No runs found" in result.output

    def test_runs_list_ordered(self, cli_runner, obs_db, aq_config):
        """aqueduct runs lists recent runs ordered by started_at DESC."""
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO run_records VALUES ('r1', 'bp1', 'success', '2026-01-01 10:00:00', '2026-01-01 10:01:00', '[]')")
        conn.execute("INSERT INTO run_records VALUES ('r2', 'bp1', 'success', '2026-01-01 11:00:00', '2026-01-01 11:01:00', '[]')")
        conn.close()
        result = cli_runner.invoke(cli, ["runs", "--config", str(aq_config)])
        assert result.exit_code == 0
        # r2 (11:00) should appear before r1 (10:00)
        assert result.output.find("r2") < result.output.find("r1")

    def test_runs_failed_filter(self, cli_runner, obs_db, aq_config):
        """aqueduct runs --failed -> shows only runs with status='error'."""
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO run_records VALUES ('r_ok', 'bp1', 'success', '2026-01-01 10:00:00', '2026-01-01 10:01:00', '[]')")
        conn.execute("INSERT INTO run_records VALUES ('r_err', 'bp1', 'error', '2026-01-01 11:00:00', '2026-01-01 11:01:00', '[]')")
        conn.close()
        result = cli_runner.invoke(cli, ["runs", "--failed", "--config", str(aq_config)])
        assert result.exit_code == 0
        assert "r_err" in result.output
        assert "r_ok" not in result.output

    def test_runs_blueprint_filter(self, cli_runner, obs_db, aq_config, tmp_path):
        """aqueduct runs --blueprint blueprint.yml -> filters by blueprint_id from file."""
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO run_records VALUES ('r_target', 'target_bp', 'success', '2026-01-01 10:00:00', '2026-01-01 10:01:00', '[]')")
        conn.execute("INSERT INTO run_records VALUES ('r_other', 'other_bp', 'success', '2026-01-01 11:00:00', '2026-01-01 11:01:00', '[]')")
        conn.close()
        
        bp_file = tmp_path / "target.yml"
        bp_file.write_text("aqueduct: '1.0'\nid: target_bp\nname: Target\nmodules: []\nedges: []\n")
        
        result = cli_runner.invoke(cli, ["runs", "--blueprint", str(bp_file), "--config", str(aq_config)])
        assert result.exit_code == 0
        assert "r_target" in result.output
        assert "r_other" not in result.output

    def test_runs_limit(self, cli_runner, obs_db, aq_config):
        """aqueduct runs --last 1 -> shows at most 1 row."""
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO run_records VALUES ('r1', 'bp', 'success', '2026-01-01 10:00:00', '2026-01-01 10:01:00', '[]')")
        conn.execute("INSERT INTO run_records VALUES ('r2', 'bp', 'success', '2026-01-01 11:00:00', '2026-01-01 11:01:00', '[]')")
        conn.close()
        result = cli_runner.invoke(cli, ["runs", "--last", "1", "--config", str(aq_config)])
        assert "r2" in result.output
        assert "r1" not in result.output

    def test_runs_output_columns(self, cli_runner, obs_db, aq_config):
        """default output has columns: run_id, blueprint_id, status, started_at, finished_at."""
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO run_records VALUES ('r1', 'bp1', 'success', '2026-01-01 10:00:00', '2026-01-01 10:01:00', '[]')")
        conn.close()
        result = cli_runner.invoke(cli, ["runs", "--config", str(aq_config)])
        assert "run_id" in result.output
        assert "blueprint" in result.output
        assert "status" in result.output
        assert "started" in result.output
        assert "failed_module" in result.output
