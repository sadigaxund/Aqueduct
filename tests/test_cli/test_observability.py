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
    db_path = tmp_path / "obs.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE run_records (run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP, module_results JSON)")
    conn.execute("CREATE TABLE failure_contexts (run_id VARCHAR PRIMARY KEY, blueprint_id VARCHAR, failed_module VARCHAR, error_message VARCHAR, stack_trace VARCHAR, manifest_json JSON, provenance_json JSON, started_at TIMESTAMP, finished_at TIMESTAMP)")
    conn.execute(_SIGNAL_OVERRIDES_DDL)
    conn.close()
    return db_path


@pytest.fixture
def lineage_db(tmp_path):
    db_path = tmp_path / "lineage.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE column_lineage (blueprint_id VARCHAR, run_id VARCHAR, channel_id VARCHAR, output_column VARCHAR, source_table VARCHAR, source_column VARCHAR)")
    conn.close()
    return db_path


@pytest.fixture
def aq_config(tmp_path, obs_db, lineage_db):
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"aqueduct_config: '1.0'\nstores:\n  obs: {{ path: {obs_db} }}\n  lineage: {{ path: {lineage_db} }}\n")
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
    def test_lineage_output(self, cli_runner, lineage_db, aq_config):
        conn = duckdb.connect(str(lineage_db))
        conn.execute("INSERT INTO column_lineage VALUES ('bp1', 'run1', 'chan1', 'col1', 'src1', 'scol1')")
        conn.close()
        result = cli_runner.invoke(cli, ["lineage", "bp1", "--config", str(aq_config)])
        assert result.exit_code == 0
        assert "chan1" in result.output


class TestSignalCommand:
    def test_signal_set_override(self, cli_runner, obs_db, aq_config):
        result = cli_runner.invoke(cli, ["signal", "probe1", "--value", "false", "--config", str(aq_config)])
        assert result.exit_code == 0
        conn = duckdb.connect(str(obs_db))
        row = conn.execute("SELECT passed FROM signal_overrides WHERE signal_id = 'probe1'").fetchone()
        conn.close()
        assert row[0] is False

    def test_signal_clear_override(self, cli_runner, obs_db, aq_config):
        conn = duckdb.connect(str(obs_db))
        conn.execute("INSERT INTO signal_overrides VALUES ('probe1', false, 'reason', '2026-01-01')")
        conn.close()
        result = cli_runner.invoke(cli, ["signal", "probe1", "--value", "true", "--config", str(aq_config)])
        assert result.exit_code == 0
        conn = duckdb.connect(str(obs_db))
        count = conn.execute("SELECT COUNT(*) FROM signal_overrides WHERE signal_id = 'probe1'").fetchone()[0]
        conn.close()
        assert count == 0

    def test_close_gate_with_error_msg(self, cli_runner, obs_db, aq_config):
        result = cli_runner.invoke(cli, ["signal", "my_probe", "--error", "Stale source", "--config", str(aq_config)])
        assert result.exit_code == 0
        conn = duckdb.connect(str(obs_db))
        row = conn.execute("SELECT passed, error_message FROM signal_overrides WHERE signal_id = 'my_probe'").fetchone()
        conn.close()
        assert row[0] is False
        assert "Stale source" in row[1]

    def test_conflicting_flags_exit_1(self, cli_runner, aq_config):
        result = cli_runner.invoke(cli, ["signal", "p", "--value", "true", "--error", "x", "--config", str(aq_config)])
        assert result.exit_code == 1

    def test_no_flags_shows_status(self, cli_runner, aq_config):
        result = cli_runner.invoke(cli, ["signal", "my_probe", "--config", str(aq_config)])
        assert result.exit_code == 0
        assert "no persistent override" in result.output
