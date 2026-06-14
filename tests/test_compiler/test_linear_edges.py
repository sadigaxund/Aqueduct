"""Tests for Linear-edge sugar — compiler.py (Phase 48).

Covers the section starting at line 166 of compiler.py:
  "3.8. Linear-edge sugar"

When a Blueprint omits `edges:` entirely AND has >1 module AND every module
type is in {Ingress, Channel, Egress, Assert}, the compiler injects a
declaration-order chain of `main`-port edges, each with `injected=True`.

If any non-linear type (Junction, Funnel, Arcade, Probe, Regulator) is
present with no explicit edges, `CompileError` is raised mentioning
"Linear-edge sugar".

TEST_MANIFEST section: ### Linear-edge sugar — `compiler.py`
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.compiler.compiler import CompileError, compile as compiler_compile
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_bp(tmp_path: Path, content: str) -> Path:
    """Write blueprint YAML to a temp file and return the path."""
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text(content)
    return bp_path


# ---------------------------------------------------------------------------
# Happy-path: injection
# ---------------------------------------------------------------------------


class TestLinearEdgeInjection:
    """Blueprint omitting `edges:` with only linear module types."""

    def test_three_linear_modules_injects_two_edges(self, tmp_path):
        """Ingress → Channel → Egress with no edges: → 2 injected edges in order."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_test
name: Linear Edge Test
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: s3://bucket/in.parquet
  - id: xform
    type: Channel
    label: Transform
    config:
      sql: "SELECT * FROM src"
  - id: dst
    type: Egress
    label: Dest
    config:
      format: parquet
      path: s3://bucket/out.parquet
""")
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp, blueprint_path=bp_path)

        assert len(manifest.edges) == 2
        e0, e1 = manifest.edges
        # Declaration-order chaining
        assert e0.from_id == "src"
        assert e0.to_id == "xform"
        assert e0.port == "main"
        assert e0.injected is True

        assert e1.from_id == "xform"
        assert e1.to_id == "dst"
        assert e1.port == "main"
        assert e1.injected is True

    def test_injected_flag_serialized_in_to_dict(self, tmp_path):
        """manifest.to_dict()['edges'][0] includes 'injected': True."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_serial
name: Serialization Test
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: b
    type: Egress
    label: B
    config:
      format: parquet
      path: s3://bucket/b.parquet
""")
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp, blueprint_path=bp_path)

        d = manifest.to_dict()
        edges = d["edges"]
        assert len(edges) == 1
        assert edges[0]["injected"] is True
        assert edges[0]["from"] == "a"
        assert edges[0]["to"] == "b"
        assert edges[0]["port"] == "main"

    def test_ingress_channel_egress_assert_all_linear_types(self, tmp_path):
        """All four linear types together → still injects correctly."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_all
name: All Linear
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: s3://bucket/in.parquet
  - id: ch
    type: Channel
    label: Channel
    config:
      sql: "SELECT * FROM src"
  - id: chk
    type: Assert
    label: Check
    config:
      rules: []
  - id: dst
    type: Egress
    label: Dest
    config:
      format: parquet
      path: s3://bucket/out.parquet
""")
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp, blueprint_path=bp_path)

        assert len(manifest.edges) == 3
        assert all(e.injected is True for e in manifest.edges)
        ids = [(e.from_id, e.to_id) for e in manifest.edges]
        assert ids == [("src", "ch"), ("ch", "chk"), ("chk", "dst")]


# ---------------------------------------------------------------------------
# Error path: non-linear types without edges
# ---------------------------------------------------------------------------


class TestLinearEdgeErrorOnNonLinear:
    """Non-linear module types with no explicit edges must raise CompileError."""

    def test_junction_with_no_edges_raises(self, tmp_path):
        """Junction present + no edges → CompileError mentioning 'Linear-edge sugar'."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_junction
name: Junction No Edges
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: junc
    type: Junction
    label: J
    config:
      sql: "SELECT * FROM a"
  - id: b
    type: Egress
    label: B
    config:
      format: parquet
      path: s3://bucket/b.parquet
""")
        bp = parse(str(bp_path))
        with pytest.raises(CompileError, match="Linear-edge sugar"):
            compiler_compile(bp, blueprint_path=bp_path)

    def test_junction_error_names_offending_module(self, tmp_path):
        """CompileError from Junction module includes the module id and type."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_junction_id
name: Junction Id Test
modules:
  - id: ingress_mod
    type: Ingress
    label: In
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: bad_junction
    type: Junction
    label: J
    config:
      sql: "SELECT * FROM ingress_mod"
""")
        bp = parse(str(bp_path))
        with pytest.raises(CompileError, match=r"bad_junction.*Junction|Junction.*bad_junction"):
            compiler_compile(bp, blueprint_path=bp_path)

    def test_funnel_with_no_edges_raises(self, tmp_path):
        """Funnel present + no edges → CompileError."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_funnel
name: Funnel No Edges
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: fn
    type: Funnel
    label: F
    config:
      inputs: [a]
      sql: "SELECT * FROM a"
  - id: b
    type: Egress
    label: B
    config:
      format: parquet
      path: s3://bucket/b.parquet
""")
        bp = parse(str(bp_path))
        with pytest.raises(CompileError, match="Linear-edge sugar"):
            compiler_compile(bp, blueprint_path=bp_path)

    def test_probe_with_no_edges_raises(self, tmp_path):
        """Probe present + no edges → CompileError."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_probe
name: Probe No Edges
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: prb
    type: Probe
    label: P
    attach_to: a
    config:
      signals:
        - type: row_count
""")
        bp = parse(str(bp_path))
        with pytest.raises(CompileError, match="Linear-edge sugar"):
            compiler_compile(bp, blueprint_path=bp_path)

    def test_regulator_with_no_edges_raises(self, tmp_path):
        """Regulator present + no edges → CompileError."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_regulator
name: Regulator No Edges
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: reg
    type: Regulator
    label: R
    config:
      rules: []
""")
        bp = parse(str(bp_path))
        with pytest.raises(CompileError, match="Linear-edge sugar"):
            compiler_compile(bp, blueprint_path=bp_path)

    def test_error_message_mentions_cannot_auto_chain(self, tmp_path):
        """CompileError text mentions 'cannot be auto-chained' or 'auto-chain'."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_msg
name: Error Message Check
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: j
    type: Junction
    label: J
    config:
      sql: "SELECT * FROM a"
""")
        bp = parse(str(bp_path))
        with pytest.raises(CompileError, match=r"auto-chain"):
            compiler_compile(bp, blueprint_path=bp_path)

    def test_arcade_with_no_edges_raises(self, tmp_path):
        """Arcade present + no edges → CompileError mentioning 'Linear-edge sugar'."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_arcade
name: Arcade No Edges
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: arc
    type: Arcade
    label: Arc
    config:
      ref: sub_blueprint.yml
""")
        bp = parse(str(bp_path))
        with pytest.raises(CompileError, match="Linear-edge sugar"):
            compiler_compile(bp, blueprint_path=bp_path)


# ---------------------------------------------------------------------------
# Explicit edges: no injection
# ---------------------------------------------------------------------------


class TestExplicitEdgesNoInjection:
    """When edges are explicitly declared, no injection happens."""

    def test_explicit_edges_not_injected(self, tmp_path):
        """Blueprint with explicit edges → all edge.injected == False."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_explicit
name: Explicit Edges
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: s3://bucket/in.parquet
  - id: dst
    type: Egress
    label: Dest
    config:
      format: parquet
      path: s3://bucket/out.parquet
edges:
  - from: src
    to: dst
""")
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp, blueprint_path=bp_path)

        assert len(manifest.edges) == 1
        assert manifest.edges[0].injected is False

    def test_explicit_edges_to_dict_injected_false(self, tmp_path):
        """Explicit edges serialized with 'injected': False in to_dict()."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_serial_false
name: Explicit Serial
modules:
  - id: a
    type: Ingress
    label: A
    config:
      format: parquet
      path: s3://bucket/a.parquet
  - id: b
    type: Channel
    label: B
    config:
      sql: "SELECT * FROM a"
  - id: c
    type: Egress
    label: C
    config:
      format: parquet
      path: s3://bucket/c.parquet
edges:
  - from: a
    to: b
  - from: b
    to: c
""")
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp, blueprint_path=bp_path)

        d = manifest.to_dict()
        for edge_dict in d["edges"]:
            assert edge_dict["injected"] is False


# ---------------------------------------------------------------------------
# Single-module: no edges, no error
# ---------------------------------------------------------------------------


class TestSingleModuleNoEdges:
    """Single-module blueprints with no edges should compile cleanly."""

    def test_single_ingress_no_edges_no_error(self, tmp_path):
        """Single Ingress module with no edges → compiles, zero edges."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_single
name: Single Module
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: s3://bucket/in.parquet
""")
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp, blueprint_path=bp_path)

        assert len(manifest.edges) == 0

    def test_single_channel_no_edges_no_error(self, tmp_path):
        """Single Channel module with no edges → compiles, zero edges."""
        bp_path = _write_bp(tmp_path, """
aqueduct: "1.0"
id: le_single_ch
name: Single Channel
modules:
  - id: ch
    type: Channel
    label: Channel
    config:
      sql: "SELECT 1 AS x"
""")
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp, blueprint_path=bp_path)

        assert len(manifest.edges) == 0
