from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.exit_codes import CONFIG_ERROR, SUCCESS

pytestmark = pytest.mark.unit

RUNNER = CliRunner()


class TestOverrideCli:
    """CLI tests for --set flag on commands that accept it (benchmark-stats, run, benchmark).

    Uses benchmark-stats --store-path to avoid side effects.
    """

    def test_override_deployment_master(self, tmp_path):
        """--set agent.timeout=5 parses as for 'run' (same parsing path)."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "s.duckdb"),
            "--set", "agent.timeout=5",
        ])
        assert result.exit_code == 0

    def test_override_typo_suggests_sibling(self, tmp_path):
        """--set with config-level typo exits CONFIG_ERROR with suggestion."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "s.duckdb"),
            "--set", "agent.timout=5",
        ])
        assert result.exit_code == CONFIG_ERROR
        assert "timout" in result.output
        assert "timeout" in result.output

    def test_override_malformed_item(self, tmp_path):
        """Malformed --set item exits CONFIG_ERROR."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "s.duckdb"),
            "--set", "bad-no-eq",
        ])
        assert result.exit_code == CONFIG_ERROR

    def test_override_danger_warning(self, tmp_path):
        """--set danger.allow_multi_patch=true parses OK on benchmark-stats."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "s.duckdb"),
            "--set", "danger.allow_multi_patch=true",
        ])
        assert result.exit_code == 0

    def test_override_agent_model(self, tmp_path):
        """--set agent.model=claude-opus-4-8 parses OK."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "s.duckdb"),
            "--set", "agent.model=claude-opus-4-8",
        ])
        assert result.exit_code == 0

    def test_override_depot_watermark(self, tmp_path):
        """--set stores.depot.backend=duckdb parses on benchmark-stats."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "s.duckdb"),
            "--set", "stores.depot.backend=duckdb",
        ])
        assert result.exit_code == 0

    def test_override_benchmark_provider(self, tmp_path):
        """--set agent.provider=openai_compat overrides engine provider for benchmark."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "s.duckdb"),
            "--set", "agent.provider=openai_compat",
        ])
        assert result.exit_code == 0
