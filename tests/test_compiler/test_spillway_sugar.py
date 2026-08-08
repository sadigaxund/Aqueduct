"""Tests for `Module.spillway:` desugaring into a real spillway Edge.

Context: spillway ROUTING already worked end to end on both engines — an
Assert rule with `on_fail: quarantine` (or a Channel `spillway_condition`)
plus an explicit `{from: X, to: Y, port: spillway}` edge. The orphaned piece
was the module-level `spillway: <target>` FIELD (`parser/schema.py`): parsed,
validated (`parser/graph.py::validate_spillway_targets` — target must
exist), remapped through Arcade expansion (`compiler/expander.py`), and
serialized into the compiled Manifest — but read by NO executor on either
engine. A wrong value was rejected at parse time while a correct one did
nothing at run time.

`aqueduct/compiler/wirer.py::desugar_module_spillway` fixes this by treating
the field as authoring SUGAR that expands into a real
`Edge(from_id=module.id, to_id=target, port="spillway")` at COMPILE time
(`aqueduct/compiler/compiler.py`, step 5 — right after Arcade expansion,
before spillway/Assert-quarantine validation) — there remains exactly ONE
runtime mechanism (the edge), matching the precedent this codebase already
uses for this class of "authoring sugar -> real edge" expansion: linear-edge
sugar (`compiler.py` step 3.8, see `test_linear_edges.py`).

This file is deliberately NOT a parse-only test suite (that is what let the
field rot for months — see CHANGELOG's prior "Deferred" note on this exact
field) — every test here compiles all the way to a Manifest and inspects
`manifest.edges`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.compiler.compiler import CompileError, compile as compiler_compile
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit


def _write_bp(tmp_path: Path, content: str, name: str = "blueprint.yml") -> Path:
    bp_path = tmp_path / name
    bp_path.write_text(content, encoding="utf-8")
    return bp_path


_SUGAR_BP = """\
aqueduct: "1.0"
id: spillway_sugar_test
name: Spillway Sugar Test
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: /tmp/in.parquet}
  - id: ch
    type: Channel
    label: Channel
    spillway: rejects
    config:
      op: sql
      query: "SELECT * FROM ch"
      spillway_condition: "x < 0"
  - id: main_out
    type: Egress
    label: MainOut
    config: {format: parquet, path: /tmp/out.parquet, mode: overwrite}
  - id: rejects
    type: Egress
    label: Rejects
    config: {format: parquet, path: /tmp/rejects.parquet, mode: overwrite}
edges:
  - from: src
    to: ch
  - from: ch
    to: main_out
"""

_EXPLICIT_EDGE_BP = """\
aqueduct: "1.0"
id: spillway_explicit_test
name: Spillway Explicit Edge Test
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: /tmp/in.parquet}
  - id: ch
    type: Channel
    label: Channel
    config:
      op: sql
      query: "SELECT * FROM ch"
      spillway_condition: "x < 0"
  - id: main_out
    type: Egress
    label: MainOut
    config: {format: parquet, path: /tmp/out.parquet, mode: overwrite}
  - id: rejects
    type: Egress
    label: Rejects
    config: {format: parquet, path: /tmp/rejects.parquet, mode: overwrite}
edges:
  - from: src
    to: ch
  - from: ch
    to: main_out
  - from: ch
    to: rejects
    port: spillway
"""


class TestSpillwaySugarEquivalence:
    """A blueprint using `spillway: rejects` must behave IDENTICALLY to the
    same blueprint written with an explicit spillway edge."""

    def test_sugar_and_explicit_edge_produce_equivalent_edges(self, tmp_path):
        sugar_path = _write_bp(tmp_path, _SUGAR_BP, "sugar.yml")
        explicit_path = _write_bp(tmp_path, _EXPLICIT_EDGE_BP, "explicit.yml")

        sugar_manifest = compiler_compile(parse(str(sugar_path)), blueprint_path=sugar_path)
        explicit_manifest = compiler_compile(
            parse(str(explicit_path)), blueprint_path=explicit_path
        )

        # Same connectivity — from/to/port/error_types — on both. `injected`
        # is expected to differ (True for the compiler-synthesized sugar
        # edge, False for the hand-authored one) and is intentionally
        # excluded: it is pure provenance metadata, never read by any
        # executor (grep confirms zero `.injected` reads outside
        # serialization/docstrings), so it does not affect RUN behavior.
        def _shape(edges):
            return sorted(
                (e.from_id, e.to_id, e.port, tuple(e.error_types)) for e in edges
            )

        assert _shape(sugar_manifest.edges) == _shape(explicit_manifest.edges)

        # The spillway edge itself is present on both, identically.
        sugar_spillway = [e for e in sugar_manifest.edges if e.port == "spillway"]
        explicit_spillway = [e for e in explicit_manifest.edges if e.port == "spillway"]
        assert len(sugar_spillway) == 1
        assert len(explicit_spillway) == 1
        assert (sugar_spillway[0].from_id, sugar_spillway[0].to_id) == ("ch", "rejects")
        assert (explicit_spillway[0].from_id, explicit_spillway[0].to_id) == ("ch", "rejects")

        # Provenance: the sugar-produced edge IS marked injected; the
        # hand-authored one is not — this is the one deliberate difference.
        assert sugar_spillway[0].injected is True
        assert explicit_spillway[0].injected is False

    def test_module_spillway_field_cleared_after_desugaring(self, tmp_path):
        """Once desugared, `Module.spillway` no longer carries the target —
        the edge is the ONLY encoding, so nothing downstream can read the
        field as a second, possibly-stale source of truth."""
        sugar_path = _write_bp(tmp_path, _SUGAR_BP, "sugar.yml")
        manifest = compiler_compile(parse(str(sugar_path)), blueprint_path=sugar_path)
        ch = next(m for m in manifest.modules if m.id == "ch")
        assert ch.spillway is None


class TestSpillwaySugarAntiSilenceGuard:
    """Falsifiable guard: this must FAIL if `Module.spillway` ever stops
    producing an edge — not merely if the field stops parsing/validating."""

    def test_spillway_field_actually_produces_an_edge_in_the_manifest(self, tmp_path):
        bp_path = _write_bp(tmp_path, _SUGAR_BP)
        manifest = compiler_compile(parse(str(bp_path)), blueprint_path=bp_path)

        matching = [
            e for e in manifest.edges
            if e.from_id == "ch" and e.to_id == "rejects" and e.port == "spillway"
        ]
        assert matching, (
            "Module.spillway='rejects' did not produce a port='spillway' edge "
            f"in the compiled Manifest — spillway sugar is a silent no-op again. "
            f"manifest.edges={manifest.edges!r}"
        )

    def test_spillway_field_alone_is_enough_to_run_a_real_quarantine(self, tmp_path):
        """End-to-end proof at the Assert layer: `on_fail: quarantine` +
        `spillway:` sugar (NO explicit edge) must compile clean, the same
        way `on_fail: quarantine` + an explicit spillway edge already does
        (see `test_compiler.py`'s sibling tests for the negative case — no
        edge/sugar at all raises `CompileError`)."""
        bp_path = _write_bp(tmp_path, """\
aqueduct: "1.0"
id: spillway_sugar_assert_quarantine
name: Spillway Sugar Assert Quarantine
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: /tmp/in.parquet}
  - id: chk
    type: Assert
    label: Check
    spillway: bad_rows
    config:
      rules:
        - type: not_null
          column: id
          on_fail: quarantine
  - id: main_out
    type: Egress
    label: MainOut
    config: {format: parquet, path: /tmp/out.parquet, mode: overwrite}
  - id: bad_rows
    type: Egress
    label: BadRows
    config: {format: parquet, path: /tmp/bad.parquet, mode: overwrite}
edges:
  - from: src
    to: chk
  - from: chk
    to: main_out
""")
        manifest = compiler_compile(parse(str(bp_path)), blueprint_path=bp_path)
        matching = [
            e for e in manifest.edges
            if e.from_id == "chk" and e.to_id == "bad_rows" and e.port == "spillway"
        ]
        assert matching


class TestSpillwaySugarConflicts:
    """A module carrying BOTH the sugar field and an explicit spillway edge."""

    def test_same_target_is_idempotent_no_duplicate_edge(self, tmp_path):
        bp_path = _write_bp(tmp_path, """\
aqueduct: "1.0"
id: spillway_sugar_same_target
name: Spillway Sugar Same Target
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: /tmp/in.parquet}
  - id: ch
    type: Channel
    label: Channel
    spillway: rejects
    config:
      op: sql
      query: "SELECT * FROM ch"
      spillway_condition: "x < 0"
  - id: main_out
    type: Egress
    label: MainOut
    config: {format: parquet, path: /tmp/out.parquet, mode: overwrite}
  - id: rejects
    type: Egress
    label: Rejects
    config: {format: parquet, path: /tmp/rejects.parquet, mode: overwrite}
edges:
  - from: src
    to: ch
  - from: ch
    to: main_out
  - from: ch
    to: rejects
    port: spillway
""")
        manifest = compiler_compile(parse(str(bp_path)), blueprint_path=bp_path)
        spillway_edges = [e for e in manifest.edges if e.port == "spillway"]
        assert len(spillway_edges) == 1
        ch = next(m for m in manifest.modules if m.id == "ch")
        assert ch.spillway is None

    def test_different_target_raises_compile_error(self, tmp_path):
        bp_path = _write_bp(tmp_path, """\
aqueduct: "1.0"
id: spillway_sugar_conflict
name: Spillway Sugar Conflict
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: /tmp/in.parquet}
  - id: ch
    type: Channel
    label: Channel
    spillway: rejects_a
    config:
      op: sql
      query: "SELECT * FROM ch"
      spillway_condition: "x < 0"
  - id: main_out
    type: Egress
    label: MainOut
    config: {format: parquet, path: /tmp/out.parquet, mode: overwrite}
  - id: rejects_a
    type: Egress
    label: RejectsA
    config: {format: parquet, path: /tmp/rejects_a.parquet, mode: overwrite}
  - id: rejects_b
    type: Egress
    label: RejectsB
    config: {format: parquet, path: /tmp/rejects_b.parquet, mode: overwrite}
edges:
  - from: src
    to: ch
  - from: ch
    to: main_out
  - from: ch
    to: rejects_b
    port: spillway
""")
        with pytest.raises(CompileError, match="different target|DIFFERENT target"):
            compiler_compile(parse(str(bp_path)), blueprint_path=bp_path)


class TestSpillwaySugarArcadeExpansion:
    """`Module.spillway` set on a module INSIDE an Arcade's own sub-Blueprint
    must be remapped by Arcade expansion (`expander.py`'s existing
    `id_map.get(m.spillway)`, unchanged by this fix) and THEN desugared into
    a correctly-namespaced edge — the reason desugaring runs after Arcade
    expansion rather than before (see `desugar_module_spillway`'s
    docstring): the field is expected to survive expansion just like
    `attach_to`, and only the flat, fully-expanded module list has the real,
    namespaced target id to build an edge against."""

    def test_spillway_sugar_inside_an_arcade_gets_namespaced_and_desugared(self, tmp_path):
        (tmp_path / "arcades").mkdir()
        sub_bp = tmp_path / "arcades" / "sub.yml"
        sub_bp.write_text("""\
aqueduct: "1.0"
id: sub_with_spillway
name: Sub With Spillway
modules:
  - id: step_one
    type: Channel
    label: Step One
    spillway: rejects
    config:
      op: sql
      query: "SELECT * FROM __input__"
      spillway_condition: "x < 0"
  - id: rejects
    type: Egress
    label: Rejects
    config: {format: parquet, path: /tmp/sub_rejects.parquet, mode: overwrite}
edges: []
""", encoding="utf-8")
        parent_bp = _write_bp(tmp_path, """\
aqueduct: "1.0"
id: parent_with_arcade_spillway
name: Parent With Arcade Spillway
modules:
  - id: src
    type: Ingress
    label: Source
    config: {format: parquet, path: /tmp/in.parquet}
  - id: enricher
    type: Arcade
    label: Enricher
    ref: arcades/sub.yml
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: /tmp/out.parquet, mode: overwrite}
edges:
  - from: src
    to: enricher
  - from: enricher
    to: sink
""")
        manifest = compiler_compile(parse(str(parent_bp)), blueprint_path=parent_bp)

        module_ids = {m.id for m in manifest.modules}
        assert "enricher__step_one" in module_ids
        assert "enricher__rejects" in module_ids

        spillway_edges = [e for e in manifest.edges if e.port == "spillway"]
        assert len(spillway_edges) == 1
        assert spillway_edges[0].from_id == "enricher__step_one"
        assert spillway_edges[0].to_id == "enricher__rejects"

        step_one = next(m for m in manifest.modules if m.id == "enricher__step_one")
        assert step_one.spillway is None
