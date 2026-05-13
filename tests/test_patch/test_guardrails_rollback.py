"""Tests for the Patch layer: Guardrails and rollback functionality."""

from __future__ import annotations
import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
pytestmark = pytest.mark.unit
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.patch.apply import PatchError, _check_guardrails
from aqueduct.patch.grammar import PatchSpec

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _bp_with_guardrails(allowed_paths=(), forbidden_ops=()):
    return {
        "agent": {
            "guardrails": {
                "allowed_paths": list(allowed_paths),
                "forbidden_ops": list(forbidden_ops),
            }
        }
    }


def _patch(*ops):
    return PatchSpec.model_validate({
        "patch_id": "test-patch",
        "rationale": "test",
        "operations": list(ops),
    })


class TestGuardrails:
    def test_no_restrictions_always_pass(self):
        bp = _bp_with_guardrails()
        spec = _patch({"op": "remove_module", "module_id": "x"})
        _check_guardrails(spec, bp)  # no raise

    def test_forbidden_op_blocks(self):
        bp = _bp_with_guardrails(forbidden_ops=("remove_module",))
        spec = _patch({"op": "remove_module", "module_id": "x"})
        with pytest.raises(PatchError, match="remove_module"):
            _check_guardrails(spec, bp)

    # ── set_module_config_key path enforcement ────────────────────────────────
    def test_set_key_path_matching_passes(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "set_module_config_key", "module_id": "x",
            "key": "path", "value": "s3a://prod/orders/",
        })
        _check_guardrails(spec, bp)

    def test_set_key_path_non_matching_blocks(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "set_module_config_key", "module_id": "x",
            "key": "path", "value": "s3a://staging/orders/",
        })
        with pytest.raises(PatchError, match="s3a://staging/orders/"):
            _check_guardrails(spec, bp)

    def test_set_key_non_path_key_ignored(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "set_module_config_key", "module_id": "x",
            "key": "format", "value": "/etc/passwd",
        })
        _check_guardrails(spec, bp)  # no raise — only path/output_path checked

    def test_set_key_arcade_expanded_id_skipped(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "set_module_config_key", "module_id": "arcade__ingress",
            "key": "path", "value": "s3a://staging/anywhere/",
        })
        _check_guardrails(spec, bp)  # skipped — apply step gives clearer error

    # ── replace_module_config path enforcement (regression for bypass bug) ────
    def test_replace_config_path_matching_passes(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "replace_module_config", "module_id": "x",
            "config": {"format": "parquet", "path": "s3a://prod/orders/"},
        })
        _check_guardrails(spec, bp)

    def test_replace_config_path_non_matching_blocks(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "replace_module_config", "module_id": "x",
            "config": {"format": "parquet", "path": "/etc/passwd"},
        })
        with pytest.raises(PatchError, match="/etc/passwd"):
            _check_guardrails(spec, bp)

    def test_replace_config_output_path_blocks(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "replace_module_config", "module_id": "x",
            "config": {"format": "parquet", "output_path": "/tmp/leak"},
        })
        with pytest.raises(PatchError, match="/tmp/leak"):
            _check_guardrails(spec, bp)

    # ── insert_module / add_probe / add_arcade_ref carry full module dicts ────
    def test_insert_module_path_blocks(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "insert_module",
            "module": {
                "id": "new_ingress", "type": "Ingress", "label": "x",
                "config": {"format": "parquet", "path": "s3a://attacker/data/"},
            },
            "edges_to_add": [], "edges_to_remove": [],
        })
        with pytest.raises(PatchError, match="s3a://attacker/data/"):
            _check_guardrails(spec, bp)

    def test_add_probe_path_blocks(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "add_probe",
            "module": {
                "id": "p", "type": "Probe", "label": "p",
                "attach_to": "x",
                "config": {"path": "/etc/secret", "signals": []},
            },
            "edges_to_add": [],
        })
        with pytest.raises(PatchError, match="/etc/secret"):
            _check_guardrails(spec, bp)

    def test_add_arcade_ref_path_blocks(self):
        bp = _bp_with_guardrails(allowed_paths=("s3a://prod/*",))
        spec = _patch({
            "op": "add_arcade_ref",
            "module": {
                "id": "arc", "type": "Arcade", "label": "a",
                "ref": "arcades/x.yml",
                "config": {"output_path": "/var/leak"},
            },
            "edges_to_add": [], "edges_to_remove": [],
        })
        with pytest.raises(PatchError, match="/var/leak"):
            _check_guardrails(spec, bp)


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
                if "checkout" in cmd:
                    env["bp_path"].write_text(env["backup_file"].read_text())
                    return MagicMock(returncode=0)
                if "diff-tree" in cmd:
                    return MagicMock(returncode=0, stdout="blueprint.yml\n")
                return MagicMock(returncode=0)
            mock_run.side_effect = side_effect
            result = runner.invoke(cli, ["rollback", str(env["bp_path"]), "--to", "abc123"])
        assert result.exit_code == 0
        assert "Original" in env["bp_path"].read_text()
