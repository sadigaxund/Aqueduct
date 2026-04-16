"""Integration tests for patch application orchestrator."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from aqueduct.patch.apply import (
    PatchError,
    apply_patch_file,
    apply_patch_to_dict,
    load_patch_spec,
    reject_patch,
)
from aqueduct.patch.grammar import PatchSpec


@pytest.fixture
def minimal_bp_path(tmp_path):
    path = tmp_path / "blueprint.yml"
    bp = {
        "aqueduct": "1.0",
        "id": "test.bp",
        "name": "Test Blueprint",
        "modules": [
            {"id": "in", "type": "Ingress", "config": {"format": "parquet", "path": "p1"}}
        ],
        "edges": []
    }
    path.write_text(yaml.dump(bp), encoding="utf-8")
    return path


def test_load_patch_spec_valid(tmp_path):
    patch_path = tmp_path / "patch.json"
    patch_data = {
        "patch_id": "p1",
        "rationale": "Test",
        "operations": [{"op": "replace_module_label", "module_id": "in", "label": "L"}]
    }
    patch_path.write_text(json.dumps(patch_data), encoding="utf-8")
    
    spec = load_patch_spec(patch_path)
    assert spec.patch_id == "p1"


def test_load_patch_spec_not_found(tmp_path):
    with pytest.raises(PatchError, match="Patch file not found"):
        load_patch_spec(tmp_path / "ghost.json")


def test_apply_patch_to_dict_atomic(tmp_path):
    bp = {
        "modules": [{"id": "m1", "config": {"a": 1}}],
        "edges": []
    }
    spec = PatchSpec(
        patch_id="p1",
        rationale="R",
        operations=[
            {"op": "replace_module_config", "module_id": "m1", "config": {"a": 2}},
            {"op": "replace_module_label", "module_id": "m1", "label": "L1"},
        ]
    )
    patched = apply_patch_to_dict(bp, spec)
    assert patched["modules"][0]["config"] == {"a": 2}
    assert patched["modules"][0]["label"] == "L1"
    # Ensure original BP was not mutated
    assert bp["modules"][0]["config"] == {"a": 1}


def test_apply_patch_to_dict_rollback_on_failure():
    bp = {"modules": [{"id": "m1"}]}
    spec = PatchSpec(
        patch_id="p1",
        rationale="R",
        operations=[
            {"op": "replace_module_label", "module_id": "m1", "label": "L"},
            {"op": "replace_module_config", "module_id": "ghost", "config": {}}, # Fails
        ]
    )
    with pytest.raises(PatchError, match="Operation 2/2 .* failed"):
        apply_patch_to_dict(bp, spec)


def test_apply_patch_file_lifecycle(minimal_bp_path, tmp_path):
    patch_path = tmp_path / "patch.json"
    patch_data = {
        "patch_id": "p123",
        "rationale": "Updating label",
        "operations": [{"op": "replace_module_label", "module_id": "in", "label": "New Label"}]
    }
    patch_path.write_text(json.dumps(patch_data), encoding="utf-8")
    
    patches_dir = tmp_path / "patches_root"
    result = apply_patch_file(minimal_bp_path, patch_path, patches_dir=patches_dir)
    
    assert result.patch_id == "p123"
    assert result.operations_applied == 1
    
    # 1. Verify Blueprint updated
    updated_bp = yaml.safe_load(minimal_bp_path.read_text())
    assert updated_bp["modules"][0]["label"] == "New Label"
    
    # 2. Verify backup created
    backups = list((patches_dir / "backups").glob("*.yml"))
    assert len(backups) == 1
    assert "p123" in backups[0].name
    
    # 3. Verify patch archived
    archived = patches_dir / "applied" / "patch.json"
    assert archived.exists()
    archived_data = json.loads(archived.read_text())
    assert "applied_at" in archived_data
    assert archived_data["blueprint_path"] == str(minimal_bp_path)


def test_apply_patch_file_atomic_failure_on_invalid_blueprint(minimal_bp_path, tmp_path):
    # Create a patch that makes the Blueprint invalid (e.g. unknown module type)
    patch_path = tmp_path / "invalid_patch.json"
    patch_data = {
        "patch_id": "p_bad",
        "rationale": "Breaking things",
        "operations": [{"op": "replace_module_config", "module_id": "in", "config": {"type": "GhostType"}}]
    }
    # Wait, replace_module_config replaces 'config'. 
    # To change 'type', I need to use insert/remove or just replace the whole dict?
    # Actually, Ingress config doesn't have 'type'.
    # I'll use InsertModule with a bad type.
    patch_data = {
        "patch_id": "p_bad",
        "rationale": "Breaking things",
        "operations": [
            {
                "op": "insert_module", 
                "module": {"id": "m2", "type": "InvalidType", "config": {}}
            }
        ]
    }
    patch_path.write_text(json.dumps(patch_data), encoding="utf-8")
    
    original_content = minimal_bp_path.read_text()
    
    with pytest.raises(PatchError, match="Patched Blueprint is invalid"):
        apply_patch_file(minimal_bp_path, patch_path, patches_dir=tmp_path / "p")
        
    # Verify original Blueprint remains unchanged
    assert minimal_bp_path.read_text() == original_content


def test_reject_patch(tmp_path):
    patches_dir = tmp_path / "patches"
    pending_dir = patches_dir / "pending"
    pending_dir.mkdir(parents=True)
    
    patch_path = pending_dir / "p1.json"
    patch_data = {"patch_id": "p1", "rationale": "R", "operations": []}
    patch_path.write_text(json.dumps(patch_data))
    
    rejected_path = reject_patch("p1", "Too risky", patches_dir=patches_dir)
    
    assert rejected_path.exists()
    assert not patch_path.exists()
    
    rejected_data = json.loads(rejected_path.read_text())
    assert rejected_data["rejection_reason"] == "Too risky"
    assert "rejected_at" in rejected_data


def test_reject_patch_not_found(tmp_path):
    with pytest.raises(PatchError, match="Patch 'ghost' not found"):
        reject_patch("ghost", "Reason", patches_dir=tmp_path / "p")
