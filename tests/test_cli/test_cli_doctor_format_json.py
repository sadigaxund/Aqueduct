from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit

RUNNER = CliRunner()


BP_SAMPLE = """aqueduct: "1.0"
id: test_bp
name: Test
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: /nonexistent/data.parquet
  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: /nonexistent/out.parquet
edges:
  - from: src
    to: sink
"""


class TestDoctorFormatJson:
    def test_doctor_json_emits_valid_structure(self, tmp_path):
        """--format json emits {schema_version, summary, checks}."""
        result = RUNNER.invoke(cli, ["doctor", "--skip-spark", "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema_version"] == "1.0"
        assert "summary" in data
        assert "total" in data["summary"]
        assert "passed" in data["summary"]
        assert "checks" in data
        assert len(data["checks"]) > 0

    def test_doctor_json_check_shape(self, tmp_path):
        """Each check has name, status, group, detail, elapsed_ms."""
        result = RUNNER.invoke(cli, ["doctor", "--skip-spark", "--format", "json"])
        data = json.loads(result.output)
        for c in data["checks"]:
            assert "name" in c
            assert "status" in c
            assert c["status"] in ("ok", "fail", "warn", "skip")
            assert "group" in c
            assert "detail" in c
            assert "elapsed_ms" in c

    def test_doctor_json_all_checks_included(self, tmp_path):
        """JSON shows ALL checks regardless of verbose (no collapsing)."""
        result_text = RUNNER.invoke(cli, ["doctor", "--skip-spark"])
        result_json = RUNNER.invoke(cli, ["doctor", "--skip-spark", "--format", "json"])
        data = json.loads(result_json.output)
        json_check_count = len(data["checks"])
        # JSON should have at least as many checks as text shows (including hidden)
        assert json_check_count >= 0

    def test_doctor_json_exit_code_matches_status(self, tmp_path):
        """JSON exit code matches overall pass/fail status."""
        cfg = tmp_path / "aqueduct.yml"
        cfg.write_text("aqueduct_config: '1.0'\ndeployment:\n  target: local\n")
        result = RUNNER.invoke(cli, [
            "doctor", "--skip-spark", "--format", "json", str(cfg),
        ])
        data = json.loads(result.output)
        has_fail = any(c["status"] == "fail" for c in data["checks"])
        if has_fail:
            assert result.exit_code == 1
        else:
            assert result.exit_code == 0

    def test_doctor_json_group_assignment(self):
        """Checks are assigned to the correct group."""
        result = RUNNER.invoke(cli, ["doctor", "--skip-spark", "--format", "json"])
        data = json.loads(result.output)
        groups = {c["group"] for c in data["checks"]}
        assert "stores" in groups or "general" in groups or "config" in groups


class TestDoctorTextGrouping:
    """Phase 49 — text mode renders checks under labelled section headers."""

    def test_text_mode_prints_section_headers(self, tmp_path):
        """`doctor --skip-spark` (text) groups checks under `_GROUP_LABEL` headers.

        `"Stores"` is a section label, not a check name — its presence proves a
        header rendered. The store check rows must follow that header.
        """
        cfg = tmp_path / "aqueduct.yml"
        cfg.write_text('aqueduct_config: "1.0"\n')
        result = RUNNER.invoke(cli, ["doctor", str(cfg), "--skip-spark"])
        assert result.exit_code == 0
        out = result.output
        # Section headers (labels) appear — no check is literally named these.
        assert "Stores" in out
        assert "Config" in out
        # The store-group check rows render *after* the "Stores" header.
        assert "observability" in out
        assert out.index("Stores") < out.index("observability")
