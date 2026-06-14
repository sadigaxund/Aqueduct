from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.exit_codes import CONFIG_ERROR, SUCCESS

pytestmark = pytest.mark.unit

RUNNER = CliRunner()


BP_VALID = """aqueduct: "1.0"
id: bench_cli
name: BenchCLI
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: data/in.parquet
  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: data/out.parquet
edges:
  - from: src
    to: sink
"""


class TestBenchmarkStatsCli:
    def test_benchmark_stats_no_store_prints_message(self, tmp_path):
        """benchmark-stats with no existing store prints empty message."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(tmp_path / "nope.duckdb"),
        ], catch_exceptions=False)
        assert result.exit_code == 0
        assert "benchmark store empty" in result.output.lower() or "no data" in result.output.lower()

    def test_benchmark_stats_json_format(self, tmp_path):
        """--format json emits {models, scenarios, trend}."""
        store = tmp_path / "bench.duckdb"
        result = RUNNER.invoke(cli, [
            "benchmark-stats", "--store-path", str(store), "--format", "json",
        ], catch_exceptions=False)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "models" in data
        assert "scenarios" in data
        assert "trend" in data

    def test_benchmark_stats_postgres_no_dsn_errors(self):
        """--set stores.benchmark.backend=postgres without DSN exits CONFIG_ERROR."""
        result = RUNNER.invoke(cli, [
            "benchmark-stats",
            "--set", "stores.benchmark.backend=postgres",
        ])
        assert result.exit_code == CONFIG_ERROR
