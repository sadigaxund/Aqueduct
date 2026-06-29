from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit

RUNNER = CliRunner()

CFG_VALID = """aqueduct_config: "1.0"
deployment:
  target: local
  master_url: local[*]
stores:
  observability:
    path: .aqueduct/obs.db
  depots:
    default:
      path: .aqueduct/depot.db
"""

BP_VALID = """aqueduct: "1.0"
id: test_bp
name: Test
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: data.parquet
  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: out.parquet
edges:
  - from: src
    to: sink
"""

BP_INVALID = """aqueduct: "1.0"
id: invalid
modules:
  - id: mod
    type: UnknownType
"""


class TestValidateText:
    def test_valid_blueprint(self, tmp_path):
        p = tmp_path / "bp.yml"
        p.write_text(BP_VALID)
        result = RUNNER.invoke(cli, ["validate", str(p)])
        assert result.exit_code == 0
        assert "blueprint" in result.output

    def test_invalid_blueprint(self, tmp_path):
        p = tmp_path / "bp.yml"
        p.write_text(BP_INVALID)
        result = RUNNER.invoke(cli, ["validate", str(p)])
        assert result.exit_code == 1
        assert "✗" in result.output

    def test_no_file_given_and_none_exists(self, tmp_path):
        result = RUNNER.invoke(cli, ["validate"], catch_exceptions=False)
        assert result.exit_code == 1
        assert "no file given" in result.output


class TestValidateFormatJson:
    def test_valid_blueprint_json(self, tmp_path):
        p = tmp_path / "bp.yml"
        p.write_text(BP_VALID)
        result = RUNNER.invoke(cli, ["validate", "--format", "json", str(p)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema_version"] == "1.0"
        assert data["summary"]["valid"] == 1
        assert data["summary"]["invalid"] == 0
        assert data["summary"]["passed"] is True
        assert len(data["files"]) == 1
        assert data["files"][0]["valid"] is True
        assert data["files"][0]["kind"] == "blueprint"
        assert data["files"][0]["id"] == "test_bp"

    def test_config_file_json(self, tmp_path):
        p = tmp_path / "config.yml"
        p.write_text(CFG_VALID)
        result = RUNNER.invoke(cli, ["validate", "--format", "json", str(p)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["summary"]["valid"] == 1
        assert data["files"][0]["valid"] is True
        assert data["files"][0]["kind"] == "config"
        assert "engine" in data["files"][0]
        assert "stores" in data["files"][0]

    def test_invalid_blueprint_json(self, tmp_path):
        p = tmp_path / "bp.yml"
        p.write_text(BP_INVALID)
        result = RUNNER.invoke(cli, ["validate", "--format", "json", str(p)])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["summary"]["invalid"] >= 1
        assert data["summary"]["passed"] is False
        assert data["files"][0]["valid"] is False
        assert "error" in data["files"][0]

    def test_no_file_given_json(self, tmp_path):
        result = RUNNER.invoke(cli, ["validate", "--format", "json"])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "error" in data
        assert data["summary"]["passed"] is False
        assert data["files"] == []

    def test_aqtest_file_json_valid_null_with_note(self, tmp_path):
        """A .aqtest.yml → `valid=null` + a `note`, not counted in valid/invalid, exit 0."""
        p = tmp_path / "x.aqtest.yml"
        p.write_text('aqueduct_test: "1.0"\nblueprint: bp.yml\n')
        result = RUNNER.invoke(cli, ["validate", "--format", "json", str(p)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        f = data["files"][0]
        assert f["kind"] == "aqtest"
        assert f["valid"] is None
        assert "note" in f
        # null-valid files are excluded from both valid and invalid counts
        assert data["summary"]["valid"] == 0
        assert data["summary"]["invalid"] == 0
        assert data["summary"]["total"] == 1

    def test_aqscenario_file_json_valid_null(self, tmp_path):
        """A .aqscenario.yml → `valid=null`, kind `aqscenario`, exit 0."""
        p = tmp_path / "x.aqscenario.yml"
        p.write_text('aqueduct_scenario: "1.0"\nblueprint: bp.yml\n')
        result = RUNNER.invoke(cli, ["validate", "--format", "json", str(p)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["files"][0]["kind"] == "aqscenario"
        assert data["files"][0]["valid"] is None
