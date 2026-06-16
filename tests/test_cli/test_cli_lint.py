from __future__ import annotations

import json
import re

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit

RUNNER = CliRunner()


BP_VALID = """aqueduct: "1.0"
id: lint_cli
name: Lint CLI
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

BP_HAS_FINDINGS = """aqueduct: "1.0"
id: lint_find
name: Lint Findings
modules:
  - id: src
    type: Ingress
    label: Src
    config:
      format: parquet
      path: data/in.parquet
  - id: ch
    type: Channel
    label: ""
    config:
      op: sql
      query: "SELECT * FROM src"
  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: data/out.parquet
edges:
  - from: src
    to: ch
  - from: ch
    to: sink
"""


class TestLintCliText:
    def test_clean_blueprint_exits_0(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text(BP_VALID)
        result = RUNNER.invoke(cli, ["lint", str(bp)])
        assert result.exit_code == 0
        assert "no lint findings" in result.output

    def test_findings_exits_0_default(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text(BP_HAS_FINDINGS)
        result = RUNNER.invoke(cli, ["lint", str(bp)])
        assert result.exit_code == 0
        assert "WARN" in result.output

    def test_findings_exits_1_with_strict(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text(BP_HAS_FINDINGS)
        result = RUNNER.invoke(cli, ["lint", "--strict", str(bp)])
        assert result.exit_code == 1
        assert "ERROR" in result.output

    def test_parse_error_exits_1(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text("not: valid: yaml: [[[")
        result = RUNNER.invoke(cli, ["lint", str(bp)])
        assert result.exit_code == 1
        assert "parse error" in result.output


class TestLintCliJson:
    def test_clean_blueprint_json(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text(BP_VALID)
        result = RUNNER.invoke(cli, ["lint", "--format", "json", str(bp)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema_version"] == "1.0"
        assert data["summary"]["total"] == 0
        assert data["summary"]["passed"] is True
        assert data["findings"] == []

    def test_findings_json_structure(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text(BP_HAS_FINDINGS)
        result = RUNNER.invoke(cli, ["lint", "--format", "json", str(bp)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema_version"] == "1.0"
        assert data["summary"]["total"] > 0
        assert data["summary"]["passed"] is True
        assert len(data["findings"]) > 0
        for f in data["findings"]:
            assert "rule_id" in f
            assert "severity" in f
            assert "message" in f
            assert f["severity"] == "warn"

    def test_findings_json_strict_promotes_to_error(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text(BP_HAS_FINDINGS)
        result = RUNNER.invoke(cli, ["lint", "--strict", "--format", "json", str(bp)])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["summary"]["passed"] is False
        assert data["summary"]["error"] > 0
        assert data["strict"] is True

    def test_parse_error_json(self, tmp_path):
        bp = tmp_path / "bp.yml"
        bp.write_text("not: valid: [[[")
        result = RUNNER.invoke(cli, ["lint", "--format", "json", str(bp)])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "error" in data
        assert "parse error" in data["error"]
        assert data["findings"] == []
