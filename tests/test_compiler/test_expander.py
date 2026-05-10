"""Tests for the Compiler layer: Arcade expansion, Probe wiring, and Regulator compile-away."""

from __future__ import annotations
from pathlib import Path
import pytest
pytestmark = pytest.mark.unit
from aqueduct.compiler.compiler import CompileError, compile
from aqueduct.compiler.models import Manifest
from aqueduct.parser.parser import parse

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _parse_and_compile(fixture: str, **kwargs) -> Manifest:
    path = FIXTURES / fixture
    bp = parse(path)
    return compile(bp, blueprint_path=path, **kwargs)


class TestArcadeExpansion:
    def test_arcade_replaced_by_sub_modules(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        module_ids = {m.id for m in manifest.modules}
        assert "enricher" not in module_ids
        assert "enricher__step_one" in module_ids
        assert "enricher__step_two" in module_ids

    def test_non_arcade_modules_preserved(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        module_ids = {m.id for m in manifest.modules}
        assert "source" in module_ids
        assert "sink" in module_ids

    def test_total_module_count(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        assert len(manifest.modules) == 4

    def test_internal_arcade_edges_namespaced(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in manifest.edges}
        assert ("enricher__step_one", "enricher__step_two") in edge_pairs

    def test_parent_edges_rewired_to_entry(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in manifest.edges}
        assert ("source", "enricher__step_one") in edge_pairs

    def test_parent_edges_rewired_from_exit(self):
        manifest = _parse_and_compile("valid_with_arcade.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in manifest.edges}
        assert ("enricher__step_two", "sink") in edge_pairs

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
            compile(bp)

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
        with pytest.raises((CompileError, ValueError, RuntimeError)):
            compile(bp, blueprint_path=parent_file)


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
        assert ("quality_gate", "sink") in edge_pairs
