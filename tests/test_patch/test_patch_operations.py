"""Unit tests for individual patch operations."""

from __future__ import annotations

import pytest
pytestmark = pytest.mark.unit
from aqueduct.patch.grammar import (
    AddArcadeRefOp,
    AddProbeOp,
    InsertModuleOp,
    RemoveModuleOp,
    ReplaceContextValueOp,
    ReplaceEdgeOp,
    ReplaceModuleConfigOp,
    ReplaceModuleLabelOp,
    ReplaceRetryPolicyOp,
    SetModuleOnFailureOp,
    SetSparkConfigOp,
)
from aqueduct.patch.operations import (
    PatchOperationError,
    apply_operation,
)


@pytest.fixture
def base_bp():
    """Returns a minimal Blueprint dict."""
    return {
        "aqueduct": "1.0",
        "id": "test.blueprint",
        "modules": [
            {"id": "in", "type": "Ingress", "config": {"path": "p1"}},
            {"id": "out", "type": "Egress", "config": {"path": "p2"}},
        ],
        "edges": [
            {"from": "in", "to": "out", "port": "main"}
        ],
        "context": {
            "env": "dev",
            "paths": {"input": "/old/path"}
        }
    }


def test_replace_module_config(base_bp):
    op = ReplaceModuleConfigOp(op="replace_module_config", module_id="in", config={"path": "new"})
    patched = apply_operation(base_bp, op)
    assert patched["modules"][0]["config"] == {"path": "new"}

    # Error: unknown module
    op = ReplaceModuleConfigOp(op="replace_module_config", module_id="ghost", config={})
    with pytest.raises(PatchOperationError, match="Module 'ghost' not found"):
        apply_operation(base_bp, op)


def test_replace_module_label(base_bp):
    op = ReplaceModuleLabelOp(op="replace_module_label", module_id="in", label="New In")
    patched = apply_operation(base_bp, op)
    assert patched["modules"][0]["label"] == "New In"


def test_insert_module(base_bp):
    new_module = {"id": "chan", "type": "Channel", "config": {"query": "SELECT *"}}
    op = InsertModuleOp(
        op="insert_module",
        module=new_module,
        edges_to_remove=[{"from": "in", "to": "out"}],
        edges_to_add=[
            {"from": "in", "to": "chan", "port": "main"},
            {"from": "chan", "to": "out", "port": "main"},
        ]
    )
    patched = apply_operation(base_bp, op)
    
    # Check module added
    assert len(patched["modules"]) == 3
    assert patched["modules"][-1]["id"] == "chan"
    
    # Check edges updated
    assert len(patched["edges"]) == 2
    assert patched["edges"][0] == {"from": "in", "to": "chan", "port": "main"}
    assert patched["edges"][1] == {"from": "chan", "to": "out", "port": "main"}

    # Error: duplicate ID
    op = InsertModuleOp(op="insert_module", module={"id": "in", "type": "Channel"})
    with pytest.raises(PatchOperationError, match="already exists"):
        apply_operation(base_bp, op)


def test_remove_module(base_bp):
    op = RemoveModuleOp(
        op="remove_module",
        module_id="in",
        edges_to_add=[{"from": "new_in", "to": "out", "port": "main"}]
    )
    patched = apply_operation(base_bp, op)
    
    # Module removed
    assert len(patched["modules"]) == 1
    assert patched["modules"][0]["id"] == "out"
    
    # Edge referencing 'in' removed, new edge added
    assert len(patched["edges"]) == 1
    assert patched["edges"][0] == {"from": "new_in", "to": "out", "port": "main"}


def test_replace_edge(base_bp):
    op = ReplaceEdgeOp(op="replace_edge", from_id="in", to_id="out", new_port="side")
    patched = apply_operation(base_bp, op)
    assert patched["edges"][0]["port"] == "side"

    # Error: unknown edge
    op = ReplaceEdgeOp(op="replace_edge", from_id="x", to_id="y", new_port="p")
    # Using wildcard for unicode arrow →
    with pytest.raises(PatchOperationError, match="Edge 'x' . 'y' not found"):
        apply_operation(base_bp, op)


def test_replace_context_value(base_bp):
    # Top-level
    op = ReplaceContextValueOp(op="replace_context_value", key="env", value="prod")
    patched = apply_operation(base_bp, op)
    assert patched["context"]["env"] == "prod"
    
    # Nested
    op = ReplaceContextValueOp(op="replace_context_value", key="paths.input", value="/new")
    patched = apply_operation(base_bp, op)
    assert patched["context"]["paths"]["input"] == "/new"

    # Error: invalid path
    op = ReplaceContextValueOp(op="replace_context_value", key="env.sub", value="val")
    with pytest.raises(PatchOperationError, match="invalid: 'env' not found or not a dict"):
        apply_operation(base_bp, op)


def test_add_probe(base_bp):
    probe = {"id": "p1", "type": "Probe", "attach_to": "in", "config": {}}
    op = AddProbeOp(
        op="add_probe",
        module=probe,
        edges_to_add=[{"from": "p1", "to": "reg", "port": "signal"}]
    )
    patched = apply_operation(base_bp, op)
    assert patched["modules"][-1]["id"] == "p1"
    assert patched["edges"][-1]["port"] == "signal"

    # Error: type mismatch
    probe["type"] = "Channel"
    op = AddProbeOp(op="add_probe", module=probe)
    with pytest.raises(PatchOperationError, match="module.type must be 'Probe'"):
        apply_operation(base_bp, op)


def test_add_arcade_ref(base_bp):
    arcade = {"id": "a1", "type": "Arcade", "ref": "lib.yml", "config": {}}
    op = AddArcadeRefOp(op="add_arcade_ref", module=arcade)
    patched = apply_operation(base_bp, op)
    assert patched["modules"][-1]["id"] == "a1"

    # Error: missing ref
    arcade.pop("ref")
    op = AddArcadeRefOp(op="add_arcade_ref", module=arcade)
    with pytest.raises(PatchOperationError, match="'module.ref' is required"):
        apply_operation(base_bp, op)


def test_set_module_on_failure(base_bp):
    op = SetModuleOnFailureOp(op="set_module_on_failure", module_id="in", on_failure={"mode": "skip"})
    patched = apply_operation(base_bp, op)
    assert patched["modules"][0]["on_failure"] == {"mode": "skip"}


def test_replace_retry_policy(base_bp):
    policy = {"max_attempts": 5}
    op = ReplaceRetryPolicyOp(op="replace_retry_policy", retry_policy=policy)
    patched = apply_operation(base_bp, op)
    assert patched["retry_policy"] == policy


# ── Phase 42: set_spark_config ────────────────────────────────────────────────


def test_apply_set_spark_config_sets_value(base_bp):
    """apply_set_spark_config sets the key/value in spark_config."""
    op = SetSparkConfigOp(
        op="set_spark_config",
        key="spark.sql.shuffle.partitions",
        value=200,
    )
    patched = apply_operation(base_bp, op)
    assert patched["spark_config"]["spark.sql.shuffle.partitions"] == 200


def test_apply_set_spark_config_auto_creates_block(base_bp):
    """apply_set_spark_config auto-creates spark_config block when absent."""
    bp = {"aqueduct": "1.0", "id": "t", "modules": []}
    op = SetSparkConfigOp(
        op="set_spark_config",
        key="spark.sql.shuffle.partitions",
        value=200,
    )
    patched = apply_operation(bp, op)
    assert "spark_config" in patched
    assert patched["spark_config"]["spark.sql.shuffle.partitions"] == 200

# ── Phase 47: replace_macro ────────────────────────────────────────────────


def test_replace_macro_success_single_line(base_bp):
    bp = dict(base_bp)
    bp["macros"] = {"mymacro": "SELECT 1"}
    from aqueduct.patch.grammar import ReplaceMacroOp
    op = ReplaceMacroOp(op="replace_macro", name="mymacro", value="SELECT 2")
    patched = apply_operation(bp, op)
    assert patched["macros"]["mymacro"] == "SELECT 2"
    # original blueprint unchanged (no macros originally)
    assert "macros" not in base_bp


def test_replace_macro_success_multiline(base_bp):
    bp = dict(base_bp)
    bp["macros"] = {"mymacro": "SELECT 1"}
    from aqueduct.patch.grammar import ReplaceMacroOp
    multi = "SELECT *\nFROM table\nWHERE col = 1"
    op = ReplaceMacroOp(op="replace_macro", name="mymacro", value=multi)
    patched = apply_operation(bp, op)
    # Ruamel stores multiline as LiteralScalarString, but accessing yields string
    assert str(patched["macros"]["mymacro"]).strip() == multi


def test_replace_macro_unknown_name_raises(base_bp):
    bp = dict(base_bp)
    bp["macros"] = {"existing": "SELECT 1"}
    from aqueduct.patch.grammar import ReplaceMacroOp
    op = ReplaceMacroOp(op="replace_macro", name="missing", value="SELECT 2")
    with pytest.raises(PatchOperationError, match="Macro"):
        apply_operation(bp, op)


def test_replace_macro_missing_macros_block_raises(base_bp):
    bp = dict(base_bp)  # no macros key
    from aqueduct.patch.grammar import ReplaceMacroOp
    op = ReplaceMacroOp(op="replace_macro", name="any", value="SELECT 1")
    with pytest.raises(PatchOperationError, match="no macros"):
        apply_operation(bp, op)
