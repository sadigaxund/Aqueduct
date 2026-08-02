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

    def test_funnel_inputs_referencing_arcade_id_rewired_to_exit_module(self):
        """A Funnel's config.inputs list may reference an Arcade's id
        directly (not just via edges) — expansion must rewrite that entry
        to the arcade's exit-module id. Also proves the rewrite goes
        through dataclasses.replace() rather than mutating another frozen
        Module's config dict in place: the original parsed Blueprint's
        Module objects (bp.modules) must be untouched after compile."""
        path = FIXTURES / "valid_with_arcade_funnel.yml"
        bp = parse(path)
        original_funnel = next(m for m in bp.modules if m.id == "combined")
        original_inputs_snapshot = list(original_funnel.config["inputs"])

        manifest = compile(bp, blueprint_path=path)

        funnel = next(m for m in manifest.modules if m.id == "combined")
        assert funnel.config["inputs"] == ["source_a", "enricher__step_two"]
        # The pre-compile Blueprint's own Module object must be unchanged —
        # proves the fix does not mutate a shared/aliased config dict that
        # could leak the rewrite backward into the parsed Blueprint.
        assert original_funnel.config["inputs"] == original_inputs_snapshot

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
        with pytest.raises(CompileError):
            compile(bp, blueprint_path=parent_file)

    def test_self_contained_arcade_compiles(self, tmp_path):
        """Regression for ARCADES_EXIT_MODULE_REQUIRED: an Arcade whose only
        terminal modules are Egress (data written internally, nothing returned
        to the parent) is valid as long as no parent edge consumes FROM it."""
        arcade_file = tmp_path / "selfcontained.yml"
        arcade_file.write_text(
            "aqueduct: '1.0'\nid: arcade.selfcontained\nname: SelfContained\n"
            "modules:\n"
            "  - id: ch\n    type: Channel\n    label: C\n    config: {}\n"
            "  - id: out\n    type: Egress\n    label: O\n"
            "    config:\n      format: parquet\n      path: out.parquet\n      mode: overwrite\n"
            "edges:\n  - from: ch\n    to: out\n"
        )
        parent_file = tmp_path / "parent.yml"
        parent_file.write_text(
            f"aqueduct: '1.0'\nid: test\nname: Test\ncontext: {{}}\n"
            f"modules:\n"
            f"  - id: source\n    type: Ingress\n    label: S\n"
            f"    config:\n      format: parquet\n      path: in.parquet\n"
            f"  - id: arc\n    type: Arcade\n    label: A\n    ref: '{arcade_file.name}'\n"
            f"edges:\n  - from: source\n    to: arc\n"
        )
        bp = parse(parent_file)
        manifest = compile(bp, blueprint_path=parent_file)
        module_ids = {m.id for m in manifest.modules}
        assert "arc__ch" in module_ids
        assert "arc__out" in module_ids

    def test_arcade_consumed_by_parent_still_requires_exit(self, tmp_path):
        """The exit-modules check must still fire when a parent edge consumes
        FROM an Arcade that has no non-Egress exit module."""
        arcade_file = tmp_path / "selfcontained.yml"
        arcade_file.write_text(
            "aqueduct: '1.0'\nid: arcade.selfcontained\nname: SelfContained\n"
            "modules:\n"
            "  - id: ch\n    type: Channel\n    label: C\n    config: {}\n"
            "  - id: out\n    type: Egress\n    label: O\n"
            "    config:\n      format: parquet\n      path: out.parquet\n      mode: overwrite\n"
            "edges:\n  - from: ch\n    to: out\n"
        )
        parent_file = tmp_path / "parent.yml"
        parent_file.write_text(
            f"aqueduct: '1.0'\nid: test\nname: Test\ncontext: {{}}\n"
            f"modules:\n"
            f"  - id: source\n    type: Ingress\n    label: S\n"
            f"    config:\n      format: parquet\n      path: in.parquet\n"
            f"  - id: arc\n    type: Arcade\n    label: A\n    ref: '{arcade_file.name}'\n"
            f"  - id: sink\n    type: Egress\n    label: K\n"
            f"    config:\n      format: parquet\n      path: sink.parquet\n      mode: overwrite\n"
            f"edges:\n  - from: source\n    to: arc\n  - from: arc\n    to: sink\n"
        )
        bp = parse(parent_file)
        with pytest.raises(CompileError, match="no exit modules"):
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

    def test_disabled_probe_without_attach_to_still_compiles(self, tmp_path):
        """A directly-disabled Probe (enabled: false) must not block
        compilation on its own missing attach_to — specs.md documents "a
        disabled module still compiles but is skipped at run time". Before
        this fix, validate_probes had no `m.enabled` guard, so disabling a
        Probe an author hasn't finished wiring still failed the whole
        compile — you could not disable your way out."""
        bp_file = tmp_path / "no_attach_disabled.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n"
            "  - id: ing\n    type: Ingress\n    label: In\n"
            "    config: { format: parquet, path: data.parquet }\n"
            "  - id: eg\n    type: Egress\n    label: Eg\n"
            "    config: { format: parquet, path: out.parquet, mode: overwrite }\n"
            "  - id: p\n    type: Probe\n    label: P\n    enabled: false\n    config: {}\n"
            "edges:\n  - from: ing\n    to: eg\n"
        )
        bp = parse(bp_file)
        manifest = compile(bp)
        probe = next(m for m in manifest.modules if m.id == "p")
        assert probe.enabled is False


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

    def test_expand_arcades_returns_3_tuple(self):
        from aqueduct.compiler.expander import expand_arcades
        path = FIXTURES / "valid_with_arcade.yml"
        bp = parse(path)
        result = expand_arcades(bp.modules, bp.edges, path.parent)
        assert len(result) == 3
        mods, edges, prov = result
        assert isinstance(mods, list)
        assert isinstance(edges, list)
        assert isinstance(prov, dict)

    def test_nested_arcade_provenance_tracked(self, tmp_path):
        from aqueduct.compiler.expander import expand_arcades
        nested_file = tmp_path / "nested.yml"
        nested_file.write_text(
            "aqueduct: '1.0'\nid: nested\nname: Nested\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n    config: {}\n"
            "edges: []\n"
        )
        parent_file = tmp_path / "parent.yml"
        parent_file.write_text(
            f"aqueduct: '1.0'\nid: parent\nname: Parent\n"
            f"modules:\n  - id: arc1\n    type: Arcade\n    label: A1\n    ref: '{nested_file.name}'\n"
            f"edges: []\n"
        )
        root_file = tmp_path / "root.yml"
        root_file.write_text(
            f"aqueduct: '1.0'\nid: root\nname: Root\n"
            f"modules:\n  - id: arc0\n    type: Arcade\n    label: A0\n    ref: '{parent_file.name}'\n"
            f"edges: []\n"
        )
        bp = parse(root_file)
        mods, edges, prov = expand_arcades(bp.modules, bp.edges, tmp_path)
        assert "arc0__arc1__m" in prov
        # Provenance tracked at both levels? The dictionary maps module id to provenance information.
        # Actually just making sure it doesn't crash and the nested module is in the provenance.
        assert len(prov) > 0
