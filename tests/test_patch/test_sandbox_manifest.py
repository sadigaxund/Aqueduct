"""Tests for Phase 48 — build_sandbox_manifest (aqueduct/patch/preview.py).

Covers:
- Egress modules are always stripped and snapshotted into egress_targets.
- sample_rows > 0 rewrites each Ingress config with sandbox_limit.
- sample_rows == 0 strips Egress but does NOT add sandbox_limit to Ingress.
- Edges whose endpoints were dropped (Egress removed) are removed.
- Edges between remaining modules are kept.
- Non-Ingress, non-Egress modules (Channel, Junction, etc.) are kept as-is.
- A manifest with no Egress modules returns an empty egress_targets list.
- A manifest with no Ingress modules and sample_rows > 0 is handled cleanly.
- Multiple Egress modules are all stripped.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from aqueduct.parser.models import Edge, Module
from aqueduct.compiler.models import Manifest
from aqueduct.patch.preview import build_sandbox_manifest


# ── Helpers ────────────────────────────────────────────────────────────────────

def _module(id: str, type: str, **config_kw) -> Module:
    return Module(id=id, type=type, label=id.capitalize(), config=dict(**config_kw))


def _edge(from_id: str, to_id: str) -> Edge:
    return Edge(from_id=from_id, to_id=to_id)


def _manifest(modules: list[Module], edges: list[Edge]) -> Manifest:
    return Manifest(
        blueprint_id="test.bp",
        context={},
        modules=tuple(modules),
        edges=tuple(edges),
        spark_config={},
    )


# ── Egress stripping ───────────────────────────────────────────────────────────

class TestEgressStripping:
    def test_egress_is_removed_from_modules(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p"),
             _module("eg1", "Egress", format="parquet", path="/out", mode="overwrite")],
            [_edge("in1", "eg1")],
        )
        sandboxed, targets = build_sandbox_manifest(m, sample_rows=0)
        assert all(mod.type != "Egress" for mod in sandboxed.modules)

    def test_egress_snapshot_captured(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p"),
             _module("eg1", "Egress", format="delta", path="/out", mode="append")],
            [_edge("in1", "eg1")],
        )
        _, targets = build_sandbox_manifest(m, sample_rows=0)
        assert len(targets) == 1
        t = targets[0]
        assert t["id"] == "eg1"
        assert t["format"] == "delta"
        assert t["path"] == "/out"
        assert t["mode"] == "append"

    def test_multiple_egress_all_stripped(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p"),
             _module("eg1", "Egress", format="parquet", path="/o1", mode="overwrite"),
             _module("eg2", "Egress", format="csv", path="/o2", mode="append")],
            [_edge("in1", "eg1"), _edge("in1", "eg2")],
        )
        sandboxed, targets = build_sandbox_manifest(m, sample_rows=0)
        assert len(targets) == 2
        target_ids = {t["id"] for t in targets}
        assert target_ids == {"eg1", "eg2"}
        module_ids = {mod.id for mod in sandboxed.modules}
        assert "eg1" not in module_ids
        assert "eg2" not in module_ids

    def test_no_egress_returns_empty_targets(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p")],
            [],
        )
        sandboxed, targets = build_sandbox_manifest(m, sample_rows=5)
        assert targets == []
        assert len(sandboxed.modules) == 1

    def test_egress_snapshot_keys_present_even_if_config_missing(self):
        """Egress module with minimal config — keys still appear (as None)."""
        m = _manifest(
            [_module("eg1", "Egress")],
            [],
        )
        _, targets = build_sandbox_manifest(m, sample_rows=0)
        assert len(targets) == 1
        t = targets[0]
        assert "id" in t and t["id"] == "eg1"
        assert "format" in t
        assert "path" in t
        assert "mode" in t


# ── Sandbox limit injection ────────────────────────────────────────────────────

class TestSandboxLimit:
    def test_sample_rows_positive_adds_sandbox_limit_to_ingress(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p")],
            [],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=42)
        ingress = next(mod for mod in sandboxed.modules if mod.id == "in1")
        assert ingress.config.get("sandbox_limit") == 42

    def test_sample_rows_zero_does_not_add_sandbox_limit(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p")],
            [],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=0)
        ingress = next(mod for mod in sandboxed.modules if mod.id == "in1")
        assert "sandbox_limit" not in ingress.config

    def test_sample_rows_preserves_existing_ingress_config_keys(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/data/file.parquet", header=True)],
            [],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=10)
        ingress = next(mod for mod in sandboxed.modules if mod.id == "in1")
        assert ingress.config["format"] == "parquet"
        assert ingress.config["path"] == "/data/file.parquet"
        assert ingress.config["header"] is True
        assert ingress.config["sandbox_limit"] == 10

    def test_non_ingress_modules_are_not_given_sandbox_limit(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p"),
             _module("ch1", "Channel", op="sql", query="SELECT * FROM in1")],
            [_edge("in1", "ch1")],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=5)
        ch = next(mod for mod in sandboxed.modules if mod.id == "ch1")
        assert "sandbox_limit" not in ch.config

    def test_multiple_ingress_all_receive_sandbox_limit(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p1"),
             _module("in2", "Ingress", format="csv", path="/p2"),
             _module("ch1", "Channel", op="sql", query="SELECT * FROM in1")],
            [_edge("in1", "ch1"), _edge("in2", "ch1")],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=7)
        for mod in sandboxed.modules:
            if mod.type == "Ingress":
                assert mod.config.get("sandbox_limit") == 7

    def test_no_ingress_modules_with_sample_rows_is_fine(self):
        m = _manifest(
            [_module("eg1", "Egress", format="parquet", path="/o")],
            [],
        )
        sandboxed, targets = build_sandbox_manifest(m, sample_rows=5)
        assert targets[0]["id"] == "eg1"
        assert len(sandboxed.modules) == 0


# ── Edge pruning ───────────────────────────────────────────────────────────────

class TestEdgePruning:
    def test_edge_to_egress_is_dropped(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p"),
             _module("eg1", "Egress", format="parquet", path="/o")],
            [_edge("in1", "eg1")],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=0)
        assert len(sandboxed.edges) == 0

    def test_edge_between_survivors_is_kept(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p"),
             _module("ch1", "Channel", op="sql", query="SELECT * FROM in1"),
             _module("eg1", "Egress", format="parquet", path="/o")],
            [_edge("in1", "ch1"), _edge("ch1", "eg1")],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=5)
        edge_pairs = [(e.from_id, e.to_id) for e in sandboxed.edges]
        assert ("in1", "ch1") in edge_pairs
        assert ("ch1", "eg1") not in edge_pairs

    def test_no_edges_returns_empty_edges(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p")],
            [],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=3)
        assert len(sandboxed.edges) == 0

    def test_edges_from_egress_are_also_dropped(self):
        """If an Egress feeds downstream (unusual but possible), those edges go too."""
        m = _manifest(
            [_module("eg1", "Egress", format="parquet", path="/o"),
             _module("ch1", "Channel", op="sql", query="SELECT * FROM eg1")],
            [_edge("eg1", "ch1")],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=0)
        assert len(sandboxed.edges) == 0


# ── Return type + frozen-safe replacement ─────────────────────────────────────

class TestReturnType:
    def test_returns_two_tuple(self):
        m = _manifest([], [])
        result = build_sandbox_manifest(m, sample_rows=0)
        assert isinstance(result, tuple) and len(result) == 2

    def test_sandboxed_manifest_is_manifest_instance(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p")],
            [],
        )
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=1)
        assert isinstance(sandboxed, Manifest)

    def test_original_manifest_not_mutated(self):
        """Manifest is frozen=True; build_sandbox_manifest must not attempt in-place mutation."""
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p"),
             _module("eg1", "Egress", format="parquet", path="/o")],
            [_edge("in1", "eg1")],
        )
        # Capture original state
        orig_module_ids = {mod.id for mod in m.modules}
        orig_edge_count = len(m.edges)

        build_sandbox_manifest(m, sample_rows=5)

        # Original manifest unchanged
        assert {mod.id for mod in m.modules} == orig_module_ids
        assert len(m.edges) == orig_edge_count

    def test_blueprint_id_and_spark_config_preserved(self):
        m = _manifest(
            [_module("in1", "Ingress", format="parquet", path="/p")],
            [],
        )
        m = dataclasses.replace(m, spark_config={"key": "val"})
        sandboxed, _ = build_sandbox_manifest(m, sample_rows=0)
        assert sandboxed.blueprint_id == "test.bp"
        assert sandboxed.spark_config == {"key": "val"}
