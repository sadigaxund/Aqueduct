"""Tests for Phase 19 provenance layer: FailureContext.provenance_json,
Surveyor provenance_json building, and Agent _build_provenance_section.

Covers ⏳ items from TEST_MANIFEST.md Phase 19 section.
"""
from __future__ import annotations

import json
import pytest

pytestmark = pytest.mark.unit

from aqueduct.surveyor.models import FailureContext


# ── FailureContext.provenance_json ─────────────────────────────────────────────

def _make_ctx(**kwargs) -> FailureContext:
    defaults = dict(
        run_id="r1",
        blueprint_id="test.bp",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
    )
    defaults.update(kwargs)
    return FailureContext(**defaults)


def test_provenance_json_field_present_defaults_none():
    """provenance_json field present and defaults to None."""
    ctx = _make_ctx()
    assert ctx.provenance_json is None


def test_to_dict_includes_provenance_json():
    """to_dict() always includes provenance_json key."""
    ctx = _make_ctx()
    d = ctx.to_dict()
    assert "provenance_json" in d
    assert d["provenance_json"] is None


def test_to_dict_provenance_json_set():
    """to_dict()[provenance_json] reflects set value when provided."""
    prov = json.dumps({"blueprint_id": "test.bp", "modules": {}, "context": {}})
    ctx = _make_ctx(provenance_json=prov)
    d = ctx.to_dict()
    assert d["provenance_json"] == prov


# ── _build_provenance_section ──────────────────────────────────────────────────

from aqueduct.agent import _build_provenance_section


def test_build_provenance_section_none_returns_empty():
    """_build_provenance_section(None) → empty string."""
    assert _build_provenance_section(None) == ""


def test_build_provenance_section_invalid_json_returns_empty():
    """_build_provenance_section('not json') → empty string (not exception)."""
    assert _build_provenance_section("not valid json") == ""


def test_build_provenance_section_context_ref_shows_replace_context_hint():
    """context_ref value → 'use replace_context_value' hint shown."""
    prov = {
        "blueprint_path": "/tmp/bp.yml",
        "failed_module": {
            "module_id": "m1",
            "module_type": "Ingress",
            "config": {
                "path": {
                    "source_type": "context_ref",
                    "context_key": "paths.foo",
                    "resolved_value": "/data/foo",
                }
            },
        },
        "context": {},
    }
    result = _build_provenance_section(json.dumps(prov))
    assert "replace_context_value" in result
    assert "paths.foo" in result


def test_build_provenance_section_literal_shows_set_module_config_hint():
    """literal value → 'use set_module_config_key' hint shown."""
    prov = {
        "blueprint_path": "/tmp/bp.yml",
        "failed_module": {
            "module_id": "m1",
            "module_type": "Ingress",
            "config": {
                "path": {
                    "source_type": "literal",
                    "original_expression": "/old/path",
                    "resolved_value": "/old/path",
                }
            },
        },
        "context": {},
    }
    result = _build_provenance_section(json.dumps(prov))
    assert "set_module_config_key" in result
    assert "m1" in result


def test_build_provenance_section_env_ref_shows_env_var_no_patch_hint():
    """env_ref value → env var name shown, 'do NOT patch the Blueprint' message."""
    prov = {
        "blueprint_path": "/tmp/bp.yml",
        "failed_module": {
            "module_id": "m1",
            "module_type": "Ingress",
            "config": {
                "path": {
                    "source_type": "env_ref",
                    "env_var": "DATA_PATH",
                    "resolved_value": "/env/path",
                }
            },
        },
        "context": {},
    }
    result = _build_provenance_section(json.dumps(prov))
    assert "DATA_PATH" in result
    assert "do NOT patch" in result


def test_build_provenance_section_arcade_expanded_shows_warning():
    """arcade-expanded module → 'Arcade-expanded' and 'does NOT exist' warning shown."""
    prov = {
        "blueprint_path": "/tmp/bp.yml",
        "failed_module": {
            "module_id": "arc__submod",
            "module_type": "Channel",
            "arcade_module_id": "arc",
            "sub_blueprint_path": "arcades/sub.yml",
            "original_module_id": "submod",
            "config": {},
        },
        "context": {},
    }
    result = _build_provenance_section(json.dumps(prov))
    assert "Arcade-expanded" in result
    assert "does NOT exist in the Blueprint YAML" in result


def test_build_provenance_section_context_block_lists_all_keys():
    """context block summary lists all context keys with resolved values."""
    prov = {
        "blueprint_path": "/tmp/bp.yml",
        "failed_module": {
            "module_id": "m1",
            "module_type": "Ingress",
            "config": {},
        },
        "context": {
            "paths.foo": {"source_type": "literal", "resolved_value": "/data/foo"},
            "env.bar": {"source_type": "env_ref", "env_var": "BAR", "resolved_value": "bar_val"},
        },
    }
    result = _build_provenance_section(json.dumps(prov))
    assert "paths.foo" in result
    assert "env.bar" in result
    assert "bar_val" in result
    assert "Blueprint context values" in result


# ── Surveyor builds provenance_json via record() ──────────────────────────────

from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.compiler.provenance import ProvenanceMap, ModuleProvenance, ValueProvenance


def _make_execution_result(blueprint_id="test.bp", status="error", run_id="r1", failed_module="m1", error="err"):
    return ExecutionResult(
        blueprint_id=blueprint_id,
        run_id=run_id,
        status=status,
        module_results=[
            ModuleResult(module_id=failed_module, status=status, error=error if status == "error" else None),
        ],
    )


def _make_manifest(blueprint_id="test.bp", provenance_map=None):
    from unittest.mock import MagicMock
    m = MagicMock()
    m.blueprint_id = blueprint_id
    m.name = "Test"
    m.provenance_map = provenance_map
    m.to_dict.return_value = {"blueprint_id": blueprint_id, "modules": []}
    return m


def test_surveyor_no_provenance_map_sets_provenance_json_none(tmp_path):
    """Manifest has no provenance_map → failure_ctx.provenance_json is None.

    Phase 39 externalises provenance_json as a blob path when store_dir is set;
    materialize to check the actual content.
    """
    from aqueduct.surveyor.surveyor import Surveyor

    manifest = _make_manifest(provenance_map=None)
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("r1")
    ctx = surveyor.record(_make_execution_result())
    assert ctx is not None
    # When provenance_map is None, provenance_json is an empty string (stayed
    # inline since Phase 39 blob externalisation skips empty values). Treat as falsy.
    assert not ctx.provenance_json


def test_surveyor_with_provenance_map_sets_provenance_json(tmp_path):
    """Manifest has provenance_map → failure_ctx.provenance_json is valid JSON
    (materialised from blob path where applicable).

    Phase 39 externalises provenance_json as a blob path when store_dir is set;
    materialize to read the actual content.
    """
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.surveyor.blob_store import materialize

    pmap = ProvenanceMap(
        blueprint_id="test.bp",
        blueprint_path="/tmp/bp.yml",
        modules={
            "m1": ModuleProvenance(
                module_id="m1",
                module_type="Ingress",
                config={
                    "path": ValueProvenance(source_type="literal", resolved_value="/tmp/data"),
                },
            )
        },
        context={},
    )
    manifest = _make_manifest(provenance_map=pmap)
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("r1")
    ctx = surveyor.record(_make_execution_result())
    assert ctx is not None
    assert ctx.provenance_json is not None
    raw = materialize(ctx.provenance_json, tmp_path)
    prov_data = json.loads(raw)
    assert isinstance(prov_data, dict)
    assert prov_data.get("blueprint_id") == "test.bp"


def test_surveyor_provenance_slice_contains_only_failed_module_and_context(tmp_path):
    """provenance slice contains failed module + full context block (not all modules).

    Phase 39 externalises provenance_json as a blob path when store_dir is set;
    materialize to read the actual content.
    """
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.surveyor.blob_store import materialize
    from aqueduct.compiler.provenance import ProvenanceMap, ModuleProvenance, ValueProvenance

    pmap = ProvenanceMap(
        blueprint_id="test.bp",
        blueprint_path="/tmp/bp.yml",
        modules={
            "m1": ModuleProvenance(
                module_id="m1",
                module_type="Ingress",
                config={"path": ValueProvenance(source_type="literal", resolved_value="/fail")},
            ),
            "m2": ModuleProvenance(
                module_id="m2",
                module_type="Egress",
                config={"path": ValueProvenance(source_type="literal", resolved_value="/out")},
            ),
        },
        context={"ctx.key": ValueProvenance(source_type="literal", resolved_value="v")},
    )
    manifest = _make_manifest(provenance_map=pmap)
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("r1")
    ctx = surveyor.record(_make_execution_result(failed_module="m1"))
    assert ctx is not None
    assert ctx.provenance_json is not None
    raw = materialize(ctx.provenance_json, tmp_path)
    prov = json.loads(raw)
    # Only the failed module should be in failed_module key
    assert prov.get("failed_module") is not None
    assert prov["failed_module"]["module_id"] == "m1"
    # Context block should be there
    assert "context" in prov
    assert "ctx.key" in prov["context"]
