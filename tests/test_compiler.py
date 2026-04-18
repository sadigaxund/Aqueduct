"""Tests for the Compiler layer.

Covers: Tier 1 resolution, Arcade expansion, Probe/Spillway wiring,
Regulator compile-away, and Manifest structure.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from aqueduct.compiler.compiler import CompileError, compile
from aqueduct.compiler.models import Manifest
from aqueduct.compiler.runtime import AqFunctions, resolve_tier1_str
from aqueduct.parser.parser import parse

FIXTURES = Path(__file__).parent / "fixtures"


def _parse_and_compile(fixture: str, **kwargs) -> Manifest:
    path = FIXTURES / fixture
    bp = parse(path)
    return compile(bp, blueprint_path=path, **kwargs)


# ──────────────────────────────────────────────────────────────────────────────
# Tier 1 runtime functions
# ──────────────────────────────────────────────────────────────────────────────


class TestTier1Resolution:
    def setup_method(self):
        self.reg = AqFunctions(run_id="test-run-001")

    def test_date_today_returns_iso(self):
        result = resolve_tier1_str("@aq.date.today()", self.reg)
        # Must be a valid ISO date string matching today
        assert result == date.today().isoformat()

    def test_date_today_custom_format(self):
        result = resolve_tier1_str("@aq.date.today(format='yyyy/MM/dd')", self.reg)
        assert result == date.today().strftime("%Y/%m/%d")

    def test_date_yesterday(self):
        from datetime import timedelta
        result = resolve_tier1_str("@aq.date.yesterday()", self.reg)
        assert result == (date.today() - timedelta(days=1)).isoformat()

    def test_date_offset_positive(self):
        result = resolve_tier1_str("@aq.date.offset(base='2024-01-01', days=7)", self.reg)
        assert result == "2024-01-08"

    def test_date_offset_negative(self):
        result = resolve_tier1_str("@aq.date.offset(base='2024-01-10', days=-3)", self.reg)
        assert result == "2024-01-07"

    def test_date_month_start(self):
        result = resolve_tier1_str("@aq.date.month_start()", self.reg)
        assert result == date.today().replace(day=1).isoformat()

    def test_runtime_run_id(self):
        result = resolve_tier1_str("@aq.runtime.run_id()", self.reg)
        assert result == "test-run-001"

    def test_runtime_prev_run_id_empty(self):
        result = resolve_tier1_str("@aq.runtime.prev_run_id()", self.reg)
        assert result == ""

    @pytest.mark.xfail(reason="DepotStore.get raises CREATE TABLE error in read_only mode")
    def test_runtime_prev_run_id_exists(self, tmp_path):
        from aqueduct.depot.depot import DepotStore
        store = DepotStore(tmp_path / "depot.db")
        store.put("_last_run_id", "test-run-999")
        
        from aqueduct.compiler.runtime import AqFunctions
        reg = AqFunctions(run_id="test-run-001", depot=store)
        result = resolve_tier1_str("@aq.runtime.prev_run_id()", reg)
        assert result == "test-run-999"

    def test_runtime_timestamp_is_iso(self):
        result = resolve_tier1_str("@aq.runtime.timestamp()", self.reg)
        # Should parse as ISO datetime
        from datetime import datetime
        dt = datetime.fromisoformat(result)
        assert dt.tzinfo is not None  # must be timezone-aware

    def test_env_function(self, monkeypatch):
        monkeypatch.setenv("MY_TEST_VAR", "hello")
        result = resolve_tier1_str("@aq.env('MY_TEST_VAR')", self.reg)
        assert result == "hello"

    def test_env_missing_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(RuntimeError, match="not set"):
            resolve_tier1_str("@aq.env('MISSING_VAR')", self.reg)

    def test_depot_get_default_when_no_depot(self):
        result = resolve_tier1_str("@aq.depot.get('key', 'fallback')", self.reg)
        assert result == "fallback"

    def test_depot_get_empty_default(self):
        result = resolve_tier1_str("@aq.depot.get('some.key')", self.reg)
        assert result == ""

    def test_nested_call_resolved(self):
        # @aq.date.offset(base='@aq.date.today()', days=1) — today resolved first
        result = resolve_tier1_str(
            "@aq.date.offset(base='@aq.date.today()', days=1)", self.reg
        )
        from datetime import timedelta
        assert result == (date.today() + timedelta(days=1)).isoformat()

    def test_tier1_in_string_context(self):
        result = resolve_tier1_str("s3://bucket/@aq.date.today()/data", self.reg)
        assert result == f"s3://bucket/{date.today().isoformat()}/data"

    def test_unknown_function_raises(self):
        with pytest.raises(ValueError, match="Unknown @aq function"):
            resolve_tier1_str("@aq.does.not.exist()", self.reg)

    def test_tier1_resolved_in_manifest_context(self, tmp_path):
        """@aq.* in Blueprint context values are resolved by Compiler."""
        bp_file = tmp_path / "tier1_ctx.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n"
            "  run_date: '@aq.date.today()'\n"
            "  path: \"data/${ctx.run_date}/output\"\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n    config:\n      op: sql\n      query: SELECT 1\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        manifest = compile(bp, blueprint_path=bp_file)
        assert manifest.context["run_date"] == date.today().isoformat()
        assert manifest.context["path"] == f"data/{date.today().isoformat()}/output"

    def test_secret_missing_provider_raises(self, tmp_path):
        bp_file = tmp_path / "secret_no_provider.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n"
            "  my_secret: \"@aq.secret('pass')\"\n"
            "modules:\n  - id: m\n    type: Ingress\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        from aqueduct.compiler.compiler import CompileError
        with pytest.raises((CompileError, RuntimeError, ValueError)):
            compile(bp, blueprint_path=bp_file)


# ──────────────────────────────────────────────────────────────────────────────
# Arcade expansion
# ──────────────────────────────────────────────────────────────────────────────


class TestArcadeExpansion:
    def test_arcade_replaced_by_sub_modules(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        module_ids = {m.id for m in manifest.modules}
        # Arcade module itself must be gone
        assert "enricher" not in module_ids
        # Namespaced sub-modules must exist
        assert "enricher.step_one" in module_ids
        assert "enricher.step_two" in module_ids

    def test_non_arcade_modules_preserved(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        module_ids = {m.id for m in manifest.modules}
        assert "source" in module_ids
        assert "sink" in module_ids

    def test_total_module_count(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        # source + enricher.step_one + enricher.step_two + sink = 4
        assert len(manifest.modules) == 4

    def test_internal_arcade_edges_namespaced(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in manifest.edges}
        assert ("enricher.step_one", "enricher.step_two") in edge_pairs

    def test_parent_edges_rewired_to_entry(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in manifest.edges}
        # source → enricher  becomes  source → enricher.step_one
        assert ("source", "enricher.step_one") in edge_pairs
        assert ("source", "enricher") not in edge_pairs

    def test_parent_edges_rewired_from_exit(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in manifest.edges}
        # enricher → sink  becomes  enricher.step_two → sink
        assert ("enricher.step_two", "sink") in edge_pairs
        assert ("enricher", "sink") not in edge_pairs

    def test_missing_ref_raises(self, tmp_path):
        bp_file = tmp_path / "arcade_no_ref.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n  - id: arc\n    type: Arcade\n    label: A\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        with pytest.raises(CompileError, match="ref"):
            compile(bp, blueprint_path=bp_file)

    def test_missing_blueprint_path_with_arcade_raises(self):
        path = FIXTURES / "valid_with_arcade.yml"
        bp = parse(path)
        with pytest.raises(CompileError, match="blueprint_path"):
            compile(bp)  # no blueprint_path

    def test_arcade_missing_required_context_raises(self, tmp_path):
        arcade_file = tmp_path / "req_arcade.yml"
        arcade_file.write_text(
            "aqueduct: '1.0'\nid: arcade.req\n"
            "required_context:\n  - my_param\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        parent_file = tmp_path / "parent.yml"
        parent_file.write_text(
            f"aqueduct: '1.0'\nid: test\nname: Test\ncontext: {{}}\n"
            f"modules:\n"
            f"  - id: arc\n    type: Arcade\n    label: A\n"
            f"    ref: '{arcade_file.name}'\n"
            f"edges: []\n"
        )
        bp = parse(parent_file)
        from aqueduct.compiler.compiler import CompileError
        with pytest.raises((CompileError, ValueError, RuntimeError)):
            compile(bp, blueprint_path=parent_file)


# ──────────────────────────────────────────────────────────────────────────────
# Probe wiring
# ──────────────────────────────────────────────────────────────────────────────


class TestProbeWiring:
    def test_valid_probe_compiles(self):
        manifest = _parse_and_compile("valid_with_probe.yml")
        probe = next(m for m in manifest.modules if m.type == "Probe")
        assert probe.attach_to == "read_input"

    def test_probe_without_attach_to_raises(self, tmp_path):
        bp_file = tmp_path / "no_attach.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n  - id: p\n    type: Probe\n    label: P\n    config: {}\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        with pytest.raises(CompileError, match="attach_to"):
            compile(bp)

    def test_probe_with_invalid_attach_to_raises(self, tmp_path):
        bp_file = tmp_path / "bad_attach.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n  - id: p\n    type: Probe\n    label: P\n    attach_to: ghost\n    config: {}\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        with pytest.raises(CompileError, match="ghost"):
            compile(bp)


# ──────────────────────────────────────────────────────────────────────────────
# Regulator compile-away
# ──────────────────────────────────────────────────────────────────────────────


class TestRegulatorCompileAway:
    def test_passive_regulator_removed(self):
        manifest = _parse_and_compile("valid_with_regulator.yml")
        module_ids = {m.id for m in manifest.modules}
        assert "passive_gate" not in module_ids

    def test_active_regulator_preserved(self):
        manifest = _parse_and_compile("valid_with_regulator.yml")
        module_ids = {m.id for m in manifest.modules}
        assert "quality_gate" in module_ids

    def test_passive_regulator_edges_bypassed(self):
        manifest = _parse_and_compile("valid_with_regulator.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in manifest.edges}
        # passive_gate compiled away → quality_gate should connect directly to sink
        assert ("quality_gate", "sink") in edge_pairs
        assert ("passive_gate", "sink") not in edge_pairs

    def test_no_regulator_edges_remain(self):
        manifest = _parse_and_compile("valid_with_regulator.yml")
        for e in manifest.edges:
            assert e.from_id != "passive_gate"
            assert e.to_id != "passive_gate"


# ──────────────────────────────────────────────────────────────────────────────
# Manifest structure
# ──────────────────────────────────────────────────────────────────────────────


class TestManifestStructure:
    def test_manifest_is_frozen(self):
        from dataclasses import FrozenInstanceError
        manifest = _parse_and_compile("valid_minimal.yml")
        with pytest.raises(FrozenInstanceError):
            manifest.pipeline_id = "hacked"  # type: ignore[misc]

    def test_to_dict_is_json_serializable(self):
        import json
        manifest = _parse_and_compile("valid_minimal.yml")
        d = manifest.to_dict()
        # Should not raise
        json.dumps(d)

    def test_to_dict_contains_required_keys(self):
        manifest = _parse_and_compile("valid_minimal.yml")
        d = manifest.to_dict()
        for key in ("pipeline_id", "modules", "edges", "context", "spark_config"):
            assert key in d

    def test_manifest_pipeline_id(self):
        manifest = _parse_and_compile("valid_minimal.yml")
        assert manifest.pipeline_id == "pipeline.hello.world"

    def test_manifest_modules_are_tuple(self):
        manifest = _parse_and_compile("valid_minimal.yml")
        assert isinstance(manifest.modules, tuple)

    def test_plain_blueprint_compiles_without_path(self):
        """Blueprints with no Arcades compile fine without blueprint_path."""
        bp = parse(FIXTURES / "valid_minimal.yml")
        manifest = compile(bp)
        assert len(manifest.modules) == 3


# ──────────────────────────────────────────────────────────────────────────────
# UDF Manifest Threading
# ──────────────────────────────────────────────────────────────────────────────

class TestUdfManifestThreading:
    def test_udf_from_blueprint_to_manifest(self, tmp_path):
        bp_file = tmp_path / "udf_reg.yml"
        bp_file.write_text("""
aqueduct: '1.0'
id: udf.test
name: test
udf_registry:
  - id: my_udf
    lang: python
    module: foo
modules:
  - id: m1
    type: Channel
    label: M1
    config:
      op: sql
      query: "SELECT 1"
edges: []
""")
        bp = parse(bp_file)
        assert bp.udf_registry[0]["id"] == "my_udf"
        assert bp.udf_registry[0]["lang"] == "python"
        assert bp.udf_registry[0]["module"] == "foo"
        
        manifest = compile(bp, blueprint_path=bp_file)
        assert len(manifest.udf_registry) == 1
        assert manifest.udf_registry[0]["id"] == "my_udf"
        
        d = manifest.to_dict()
        assert "udf_registry" in d
        assert d["udf_registry"][0]["id"] == "my_udf"
