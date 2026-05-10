"""Tests for the Patch layer: Guardrails and rollback functionality."""

from __future__ import annotations
import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
pytestmark = pytest.mark.unit
from click.testing import CliRunner

from aqueduct.cli import _check_guardrails, cli
from aqueduct.parser.models import AgentConfig, GuardrailsConfig
from aqueduct.surveyor.llm import LLMPatchResult
from aqueduct.patch.grammar import PatchSpec

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _make_agent(allowed_paths=(), forbidden_ops=()):
    return AgentConfig(guardrails=GuardrailsConfig(allowed_paths=allowed_paths, forbidden_ops=forbidden_ops))


def _make_patch(*ops):
    patch = MagicMock()
    patch.operations = list(ops)
    return patch


class TestGuardrails:
    def test_guardrails_no_restrictions_always_pass(self):
        agent = _make_agent()
        patch = _make_patch({"op": "remove_module", "module_id": "x"})
        assert _check_guardrails(patch, agent) is None

    def test_guardrails_forbidden_op_blocks(self):
        agent = _make_agent(forbidden_ops=("remove_module",))
        patch = _make_patch({"op": "remove_module", "module_id": "x"})
        err = _check_guardrails(patch, agent)
        assert err is not None
        assert "remove_module" in err

    def test_guardrails_allowed_paths_pass_matching(self):
        agent = _make_agent(allowed_paths=("s3a://prod/*",))
        patch = _make_patch({"op": "replace_module_config", "module_id": "x", "config": {"path": "s3a://prod/orders/"}})
        assert _check_guardrails(patch, agent) is None

    def test_guardrails_allowed_paths_block_non_matching(self):
        agent = _make_agent(allowed_paths=("s3a://prod/*",))
        patch = _make_patch({"op": "replace_module_config", "module_id": "x", "config": {"path": "s3a://staging/orders/"}})
        err = _check_guardrails(patch, agent)
        assert err is not None
        assert "s3a://staging/orders/" in err


class TestRollback:
    _MINIMAL_BP = """aqueduct: "1.0"
id: test.rollback
name: Test
modules:
  - id: m
    type: Ingress
    label: M
    config:
      format: parquet
      path: /tmp/x
edges: []
"""
    _ORIGINAL_BP = """aqueduct: "1.0"
id: test.rollback
name: Original
modules:
  - id: m
    type: Ingress
    label: M
    config:
      format: parquet
      path: /tmp/original
edges: []
"""

    def _setup_patch_env(self, tmp_path: Path, patch_id: str = "abc123") -> dict:
        bp_path = tmp_path / "blueprint.yml"
        bp_path.write_text(self._MINIMAL_BP)
        patches_root = tmp_path / "patches"
        backup_dir = patches_root / "backups"
        applied_dir = patches_root / "applied"
        backup_dir.mkdir(parents=True)
        applied_dir.mkdir(parents=True)
        backup_file = backup_dir / f"{patch_id}_20260101T000000_blueprint.yml"
        backup_file.write_text(self._ORIGINAL_BP)
        applied_record = applied_dir / f"{patch_id}.json"
        applied_record.write_text(json.dumps({
            "_aq_meta": {"run_id": "run-001", "blueprint_id": "test.rollback"},
            "operations": [],
        }))
        return {"bp_path": bp_path, "backup_file": backup_file}

    def test_patch_rollback_restores_blueprint(self, tmp_path):
        env = self._setup_patch_env(tmp_path)
        runner = CliRunner()
        with patch("subprocess.run") as mock_run:
            mock_log = MagicMock(returncode=0, stdout=f"h1\x1fSubject line\nBody with abc123\x1eENDCOMMIT")
            def side_effect(cmd, **kw):
                if "log" in cmd: return mock_log
                if "revert" in cmd:
                    env["bp_path"].write_text(env["backup_file"].read_text())
                    return MagicMock(returncode=0)
                return MagicMock(returncode=0)
            mock_run.side_effect = side_effect
            result = runner.invoke(cli, ["rollback", str(env["bp_path"]), "--to", "abc123"])
        assert result.exit_code == 0
        assert "Original" in env["bp_path"].read_text()
