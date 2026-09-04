import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.integration


def test_cli_stores_info(tmp_path):
    runner = CliRunner()
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")
    result = runner.invoke(cli, ["stores", "info", "--config", str(config)])
    assert result.exit_code == 0
    assert "observability" in result.output
    # assert "lineage" in result.output
    assert "depot" in result.output
