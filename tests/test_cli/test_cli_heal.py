import json
import pytest
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import MagicMock, patch

from aqueduct.cli import cli

pytestmark = pytest.mark.integration

@pytest.fixture
def mock_fc_row():
    # run_id, blueprint_id, failed_module, error_message, stack_trace, manifest_json, started_at, finished_at
    return (
        "test_run",
        "test_bp",
        "m1",
        "Test error",
        "Traceback test",
        '{"id": "test_bp", "modules": [{"id": "m1", "type": "Ingress"}]}',
        "2023-01-01T00:00:00Z",
        "2023-01-01T00:01:00Z"
    )

@pytest.fixture
def test_config(tmp_path):
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("""
agent:
  provider: openai_compat
  model: "test_model"
""")
    # Create the obs.db parent directory so it doesn't fail on exist check
    (tmp_path / ".aqueduct").mkdir(exist_ok=True)
    (tmp_path / ".aqueduct" / "observability.db").touch()
    return config_path


@patch("duckdb.connect")
def test_heal_print_prompt_text(mock_connect, mock_fc_row, test_config, tmp_path):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.execute.return_value.fetchone.return_value = mock_fc_row

    runner = CliRunner()
    store_dir = tmp_path / ".aqueduct"
    result = runner.invoke(cli, ["heal", "test_run", "--print-prompt", "--config", str(test_config), "--store-dir", str(store_dir)])
    
    assert result.exit_code == 0, f"Failed with: {result.exception or result.output}"
    assert "## SYSTEM PROMPT" in result.output
    assert "## USER PROMPT" in result.output
    assert "id: test_bp" in result.output or "test_bp" in result.output


@patch("duckdb.connect")
def test_heal_print_prompt_json(mock_connect, mock_fc_row, test_config, tmp_path):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.execute.return_value.fetchone.return_value = mock_fc_row

    runner = CliRunner()
    store_dir = tmp_path / ".aqueduct"
    result = runner.invoke(cli, ["heal", "test_run", "--print-prompt", "json", "--config", str(test_config), "--store-dir", str(store_dir)])
    
    assert result.exit_code == 0, f"Failed with: {result.exception or result.output}"
    
    try:
        data = json.loads(result.output)
    except json.JSONDecodeError as e:
        pytest.fail(f"Output is not valid JSON: {e}\n{result.output}")
        
    assert "system" in data
    assert "user" in data
    assert "test_bp" in data["user"]


@patch("duckdb.connect")
def test_heal_print_prompt_no_model_succeeds(mock_connect, mock_fc_row, tmp_path):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.execute.return_value.fetchone.return_value = mock_fc_row

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("""
agent:
  provider: openai_compat
""")
    (tmp_path / ".aqueduct").mkdir(exist_ok=True)
    (tmp_path / ".aqueduct" / "observability.db").touch()
    store_dir = tmp_path / ".aqueduct"

    runner = CliRunner()
    result = runner.invoke(cli, ["heal", "test_run", "--config", str(config_path), "--store-dir", str(store_dir), "--print-prompt"])
    
    assert result.exit_code == 0, f"Failed with: {result.output}"
    assert "## SYSTEM PROMPT" in result.output


def test_heal_no_args_exit_5():
    from aqueduct.exit_codes import USAGE_ERROR
    runner = CliRunner()
    result = runner.invoke(cli, ["heal"])
    assert result.exit_code == USAGE_ERROR
    assert "✗ provide a run_id argument" in result.output

