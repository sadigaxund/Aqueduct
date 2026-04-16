"""Unit tests for PatchSpec Pydantic grammar."""

from __future__ import annotations

import json
import pytest
from pydantic import ValidationError
from aqueduct.patch.grammar import PatchSpec


def test_valid_patch_spec_parsing():
    raw_json = {
        "patch_id": "patch_123",
        "rationale": "Fixing SQL query",
        "operations": [
            {
                "op": "replace_module_config",
                "module_id": "m1",
                "config": {"query": "SELECT 1"}
            }
        ]
    }
    spec = PatchSpec.model_validate(raw_json)
    assert spec.patch_id == "patch_123"
    assert len(spec.operations) == 1
    assert spec.operations[0].op == "replace_module_config"


def test_patch_spec_empty_operations():
    # min_length=1 should prevent empty operations list
    raw_json = {
        "patch_id": "patch_123",
        "rationale": "Empty patch",
        "operations": []
    }
    with pytest.raises(ValidationError, match="at least 1 item"):
        PatchSpec.model_validate(raw_json)


def test_patch_spec_extra_field_forbidden():
    # extra="forbid" on top level
    raw_json = {
        "patch_id": "patch_123",
        "rationale": "Extra field",
        "hacker_field": "exploit",
        "operations": [
            {"op": "replace_module_label", "module_id": "m1", "label": "L"}
        ]
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PatchSpec.model_validate(raw_json)


def test_patch_operation_discriminator_mismatch():
    # discriminator="op" is used
    raw_json = {
        "patch_id": "p1",
        "rationale": "Wrong op",
        "operations": [
            {"op": "invalid_op_name", "module_id": "m1"}
        ]
    }
    # Adjusting for Pydantic v2 error message format
    with pytest.raises(ValidationError, match="invalid_op_name"):
        PatchSpec.model_validate(raw_json)


def test_patch_operation_missing_required_field():
    raw_json = {
        "patch_id": "p1",
        "rationale": "Missing field",
        "operations": [
            {"op": "replace_module_config", "module_id": "m1"}  # config is missing
        ]
    }
    with pytest.raises(ValidationError, match="Field required"):
        PatchSpec.model_validate(raw_json)


def test_patch_operation_extra_field_forbidden():
    # extra="forbid" on operation level
    raw_json = {
        "patch_id": "p1",
        "rationale": "Extra op field",
        "operations": [
            {
                "op": "replace_module_label",
                "module_id": "m1",
                "label": "L",
                "unknown_key": "val"
            }
        ]
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PatchSpec.model_validate(raw_json)


def test_patch_spec_json_schema():
    schema = PatchSpec.model_json_schema()
    assert schema["title"] == "PatchSpec"
    assert "operations" in schema["properties"]
    # Check that ops are a list of discriminated items
    ops_schema = schema["$defs"]
    assert "ReplaceModuleConfigOp" in ops_schema
    assert "ReplaceModuleLabelOp" in ops_schema
