"""The fan-shape conformance matrix for cross-engine handoff (Phase 81/82
batch B) — the phase's acceptance bar per docs/specs.md §10.9: "implemented
properly" IS this matrix being green. Fan shapes are ALLOWED (they fall out
of edge-level handoff insertion at compile time), but nothing had
systematically verified they actually run against real engines until now.

Written ONCE, parametrized over ``(engine_a, engine_b)`` — the upstream and
downstream engine of the (first) boundary in each shape — and run for both
registered pairs (spark->duckdb, duckdb->spark) so a third engine registered
later is covered by adding a pair to ``ENGINE_PAIRS``, not a new file.

Four shapes, each asserting real behaviour end to end (row counts, values,
handoff presence/absence, per-module engine attribution, spill cleanup on
success) rather than "it compiled":

1. Junction fanning across a boundary — one Junction whose branches land on
   two different engines (one branch stays on the Junction's own engine,
   the other crosses).
2. Funnel merging across a boundary — a Funnel whose inputs arrive from two
   different engines (one input local, one crossing).
3. Diamond crossing twice — split via two parallel same-engine Channels,
   BOTH cross the boundary independently, rejoin at one Funnel: two
   handoffs feeding one downstream module.
4. Probe near a boundary — attached to the Spark-side module of the
   boundary (an arbitrary but fixed choice for this test; `module.type.Probe`
   is `supported` on BOTH engines as of Pass F, so a duckdb-side attachment
   would be equally legal — not exercised here, this shape only needs ONE
   real cross-engine attachment point to prove the colocation rule);
   covers both that it reports correctly AND that the Probe/Assert-
   colocation compile-time rule actually holds near a boundary (a
   mismatched explicit `engine:` pin is a CompileError).

Needs a real SparkSession (``local[1]``, Java 17) and DuckDB's bare
``:memory:`` connection — both engines auto-register via
``aqueduct.executor.capabilities.load_engines()``.

Real defects found and fixed while building this matrix (all in code, not
in this test file — see the referenced modules for the fix + regression
coverage):

- ``aqueduct/compiler/islands.py``: ``_DATA_PORTS`` was an INCLUDE-list of
  ``{"main", "spillway"}``, silently excluding every Junction branch-port
  edge (``port`` is the branch id, e.g. ``port: active``) from both engine
  inheritance and island connectivity — a Junction and its branch targets
  landed in SEPARATE islands even on the SAME engine, misrouting an
  ordinary single-engine Junction blueprint through ``run_polyglot()``.
  Fixed to an EXCLUDE-list (``"signal"`` only), matching the real
  executor's own ``_is_data_edge``.
- ``aqueduct/executor/spark/executor.py`` + ``duckdb_/executor.py``: the
  Handoff dispatch used ``_incoming_main`` (port must be ``"main"``) to
  decide WRITE vs READ side, so a Junction branch edge crossing the
  boundary directly (port = branch id) was misdispatched as the READ side
  and attempted a parquet read before anything had been written. Fixed to
  ``_incoming_data`` (any non-signal port).
- ``aqueduct/compiler/handoff.py``: the READ-side edge (``handoff -> B``)
  preserved the ORIGINAL boundary edge's port, but both engines' Handoff
  runtime always writes the read side's value under the handoff module's
  OWN bare id — so a non-"main" original port made `B`'s lookup miss
  entirely. Fixed to always emit ``port="main"`` on the read-side edge.
- Same two executor.py files: Channel (`op: sql`/`op: join`) and Funnel
  (`inputs:`) name their upstream by the ORIGINAL Blueprint id in
  Blueprint-authored text/config — authored before compilation ever knows a
  boundary will land there. Because the rewired edge's `from_id` becomes
  the handoff's generated id, the upstream dict was being keyed by that
  generated id instead of the name the SQL/`inputs:` actually reference.
  Added ``_effective_frame_key()`` (both files) to resolve back through a
  Handoff to its original ``from_module``/``port`` when building the
  by-name upstream dict for Channel and Funnel — transparent to Egress,
  Junction, and Assert, which are structurally edge-based and never
  named this lookup by id to begin with.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

from aqueduct.compiler.compiler import compile as ccompile
from aqueduct.errors import CompileError
from aqueduct.executor.models import ExecutionStatus
from aqueduct.executor.models import manifest_hash as _manifest_hash
from aqueduct.executor.orchestrator import run_polyglot
from aqueduct.models import ModuleType
from aqueduct.parser.parser import parse_dict
from aqueduct.surveyor.surveyor import Surveyor

ENGINE_PAIRS = [("spark", "duckdb"), ("duckdb", "spark")]
PAIR_IDS = ["spark_to_duckdb", "duckdb_to_spark"]


# ── shared helpers ────────────────────────────────────────────────────────


def _bp(modules, edges):
    d = {
        "aqueduct": "1.0",
        "id": "fan_shape_test_bp",
        "name": "t",
        "modules": modules,
        "edges": edges,
    }
    return parse_dict(d, base_dir=Path("/tmp"))


def _write_input(spark, engine: str, path: Path, values: list[int]) -> None:
    """Write a one-column (`n`) parquet input on *engine*."""
    if engine == "spark":
        spark.createDataFrame([(v,) for v in values], ["n"]).write.parquet(str(path))
    else:
        values_sql = ", ".join(f"({v})" for v in values)
        duckdb.sql(f"SELECT * FROM (VALUES {values_sql}) AS t(n)").to_parquet(str(path))


def _read_rows(spark, engine: str, path: Path) -> list[dict]:
    """Read back an Egress's output written by *engine* as a list of dict rows."""
    if engine == "spark":
        return [r.asDict() for r in spark.read.parquet(str(path)).collect()]
    rel = duckdb.sql(f"SELECT * FROM read_parquet('{path}')")
    cols = rel.columns
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _handoff_ids(manifest) -> set[str]:
    return {m.id for m in manifest.modules if m.type == ModuleType.Handoff}


def _engine_map(manifest) -> dict[str, str | None]:
    return {m.id: m.engine for m in manifest.modules}


def _run(manifest, run_id: str, handoff_root: Path, store_dir: Path, master_url: str = "local[1]"):
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    surveyor.start(run_id)
    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=str(handoff_root),
        store_dir=store_dir,
        surveyor=surveyor,
        master_url=master_url,
    )
    return result


def _assert_spill_cleaned_up(manifest, handoff_root: Path, run_id: str) -> None:
    manifest_h = _manifest_hash(manifest)
    run_dir = handoff_root / manifest_h / run_id
    assert not run_dir.exists(), f"spill directory {run_dir} was not cleaned up on success"


# ── Shape 1: Junction fanning across a boundary ─────────────────────────────


@pytest.mark.parametrize("engine_a,engine_b", ENGINE_PAIRS, ids=PAIR_IDS)
def test_junction_fans_across_a_boundary(spark, tmp_path, engine_a, engine_b):
    in_path = tmp_path / "in.parquet"
    out_low_path = tmp_path / "out_low"
    out_high_path = tmp_path / "out_high"
    _write_input(spark, engine_a, in_path, [0, 1, 2, 3, 4, 5, 6])

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": engine_a,
                "config": {"format": "parquet", "path": str(in_path)},
            },
            {
                "id": "route",
                "label": "route",
                "type": "Junction",
                "engine": engine_a,
                "config": {
                    "mode": "conditional",
                    "branches": [
                        {"id": "low", "condition": "n < 3"},
                        {"id": "high", "condition": "_else_"},
                    ],
                },
            },
            {
                "id": "out_low",
                "label": "out_low",
                "type": "Egress",
                "engine": engine_a,
                "config": {"format": "parquet", "path": str(out_low_path), "mode": "overwrite"},
            },
            {
                "id": "out_high",
                "label": "out_high",
                "type": "Egress",
                "engine": engine_b,
                "config": {"format": "parquet", "path": str(out_high_path), "mode": "overwrite"},
            },
        ],
        edges=[
            {"from": "in", "to": "route"},
            {"from": "route", "to": "out_low", "port": "low"},
            {"from": "route", "to": "out_high", "port": "high"},
        ],
    )
    manifest = ccompile(bp, engine=engine_a)

    # Junction and its same-engine branch stay in ONE island; the crossing
    # branch is its own island — exactly 2 islands, 1 handoff (the
    # regression this whole shape exists to guard: before the islands.py
    # fix, a Junction's branch-port edges weren't seen as data edges at all,
    # so `out_low` (SAME engine as the Junction) landed in a THIRD, separate
    # island of its own).
    assert len(manifest.islands) == 2
    assert _handoff_ids(manifest) == {"route__handoff__out_high"}

    engines = _engine_map(manifest)
    assert engines["in"] == engine_a
    assert engines["route"] == engine_a
    assert engines["out_low"] == engine_a
    assert engines["out_high"] == engine_b
    assert engines["route__handoff__out_high"] is None

    handoff_root = tmp_path / "handoff_root"
    store_dir = tmp_path / "obs"
    run_id = "junction-fan"
    result = _run(manifest, run_id, handoff_root, store_dir)
    assert result.status == ExecutionStatus.SUCCESS, result.module_results

    low_rows = _read_rows(spark, engine_a, out_low_path)
    high_rows = _read_rows(spark, engine_b, out_high_path)
    assert sorted(r["n"] for r in low_rows) == [0, 1, 2]
    assert sorted(r["n"] for r in high_rows) == [3, 4, 5, 6]

    _assert_spill_cleaned_up(manifest, handoff_root, run_id)


# ── Shape 2: Funnel merging across a boundary ───────────────────────────────


@pytest.mark.parametrize("engine_a,engine_b", ENGINE_PAIRS, ids=PAIR_IDS)
def test_funnel_merges_across_a_boundary(spark, tmp_path, engine_a, engine_b):
    in_a_path = tmp_path / "in_a.parquet"
    in_b_path = tmp_path / "in_b.parquet"
    out_path = tmp_path / "out"
    _write_input(spark, engine_a, in_a_path, [0, 1, 2])
    _write_input(spark, engine_b, in_b_path, [100, 101, 102, 103])

    bp = _bp(
        [
            {
                "id": "in_a",
                "label": "in_a",
                "type": "Ingress",
                "engine": engine_a,
                "config": {"format": "parquet", "path": str(in_a_path)},
            },
            {
                "id": "in_b",
                "label": "in_b",
                "type": "Ingress",
                "engine": engine_b,
                "config": {"format": "parquet", "path": str(in_b_path)},
            },
            # Ambiguous multi-engine parents (rule 3) force an explicit pin;
            # pin to engine_a so exactly ONE of the two inputs crosses.
            {
                "id": "merge",
                "label": "merge",
                "type": "Funnel",
                "engine": engine_a,
                "config": {"mode": "union_all", "inputs": ["in_a", "in_b"]},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": engine_a,
                "config": {"format": "parquet", "path": str(out_path), "mode": "overwrite"},
            },
        ],
        edges=[
            {"from": "in_a", "to": "merge"},
            {"from": "in_b", "to": "merge"},
            {"from": "merge", "to": "out"},
        ],
    )
    manifest = ccompile(bp, engine=engine_a)

    assert len(manifest.islands) == 2
    assert _handoff_ids(manifest) == {"in_b__handoff__merge"}

    engines = _engine_map(manifest)
    assert engines["in_a"] == engine_a
    assert engines["in_b"] == engine_b
    assert engines["merge"] == engine_a
    assert engines["out"] == engine_a

    handoff_root = tmp_path / "handoff_root"
    store_dir = tmp_path / "obs"
    run_id = "funnel-merge"
    result = _run(manifest, run_id, handoff_root, store_dir)
    assert result.status == ExecutionStatus.SUCCESS, result.module_results

    rows = _read_rows(spark, engine_a, out_path)
    assert sorted(r["n"] for r in rows) == [0, 1, 2, 100, 101, 102, 103]

    _assert_spill_cleaned_up(manifest, handoff_root, run_id)


# ── Shape 3: Diamond crossing twice ─────────────────────────────────────────


@pytest.mark.parametrize("engine_a,engine_b", ENGINE_PAIRS, ids=PAIR_IDS)
def test_diamond_crosses_the_boundary_twice(spark, tmp_path, engine_a, engine_b):
    in_path = tmp_path / "in.parquet"
    out_path = tmp_path / "out"
    _write_input(spark, engine_a, in_path, [0, 1, 2, 3, 4])

    bp = _bp(
        [
            # "src", not "in" — "in" is a SQL keyword (the IN operator) and
            # t1/t2's SQL text below references this module by literal name.
            {
                "id": "src",
                "label": "src",
                "type": "Ingress",
                "engine": engine_a,
                "config": {"format": "parquet", "path": str(in_path)},
            },
            {
                "id": "t1",
                "label": "t1",
                "type": "Channel",
                "engine": engine_a,
                "config": {"op": "sql", "query": "SELECT n, 'path1' AS tag FROM src"},
            },
            {
                "id": "t2",
                "label": "t2",
                "type": "Channel",
                "engine": engine_a,
                "config": {"op": "sql", "query": "SELECT n, 'path2' AS tag FROM src"},
            },
            # Both t1 and t2 cross independently — TWO handoffs feeding this
            # ONE Funnel.
            {
                "id": "merge",
                "label": "merge",
                "type": "Funnel",
                "engine": engine_b,
                "config": {"mode": "union_all", "inputs": ["t1", "t2"]},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": engine_b,
                "config": {"format": "parquet", "path": str(out_path), "mode": "overwrite"},
            },
        ],
        edges=[
            {"from": "src", "to": "t1"},
            {"from": "src", "to": "t2"},
            {"from": "t1", "to": "merge"},
            {"from": "t2", "to": "merge"},
            {"from": "merge", "to": "out"},
        ],
    )
    manifest = ccompile(bp, engine=engine_a)

    assert len(manifest.islands) == 2
    assert _handoff_ids(manifest) == {"t1__handoff__merge", "t2__handoff__merge"}

    engines = _engine_map(manifest)
    assert engines["src"] == engine_a
    assert engines["t1"] == engine_a
    assert engines["t2"] == engine_a
    assert engines["merge"] == engine_b
    assert engines["out"] == engine_b

    handoff_root = tmp_path / "handoff_root"
    store_dir = tmp_path / "obs"
    run_id = "diamond-cross-twice"
    result = _run(manifest, run_id, handoff_root, store_dir)
    assert result.status == ExecutionStatus.SUCCESS, result.module_results

    rows = _read_rows(spark, engine_b, out_path)
    assert len(rows) == 10
    path1 = sorted(r["n"] for r in rows if r["tag"] == "path1")
    path2 = sorted(r["n"] for r in rows if r["tag"] == "path2")
    assert path1 == [0, 1, 2, 3, 4]
    assert path2 == [0, 1, 2, 3, 4]

    _assert_spill_cleaned_up(manifest, handoff_root, run_id)


# ── Shape 4: Probe near a boundary ───────────────────────────────────────────


@pytest.mark.parametrize("engine_a,engine_b", ENGINE_PAIRS, ids=PAIR_IDS)
def test_probe_near_a_boundary_colocates_and_reports(spark, tmp_path, engine_a, engine_b):
    """Probe/Assert must colocate with their target's island (§4.3 /
    islands.py::validate_colocation). This test fixes the attachment point to
    the Spark side of the boundary (an arbitrary but stable choice — Probe is
    `supported` on both engines as of Pass F, so a duckdb-side attachment
    would be equally legal) and confirms it still reports a correct signal.

    An Egress never populates `frame_store` under its own id (it only
    writes; there is nothing downstream of it to read from), so a Probe
    cannot attach to one at all regardless of engine. A `relay` Channel is
    always inserted on the DOWNSTREAM side of the boundary so there is a
    real attachment point there too — this also means the probe attaches
    adjacent to the handoff's WRITE side when `engine_a == "spark"` (probe
    on "src") and adjacent to its READ side when `engine_b == "spark"`
    (probe on "relay"), covering both adjacencies across the two pairs.
    """
    in_path = tmp_path / "in.parquet"
    out_path = tmp_path / "out"
    _write_input(spark, engine_a, in_path, [0, 1, 2, 3, 4])

    probe_target = "src" if engine_a == "spark" else "relay"

    bp = _bp(
        [
            # "src", not "in" — "in" is a SQL keyword and `relay`'s query
            # below references this module by literal name.
            {
                "id": "src",
                "label": "src",
                "type": "Ingress",
                "engine": engine_a,
                "config": {"format": "parquet", "path": str(in_path)},
            },
            {
                "id": "relay",
                "label": "relay",
                "type": "Channel",
                "engine": engine_b,
                "config": {"op": "sql", "query": "SELECT * FROM src"},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": engine_b,
                "config": {"format": "parquet", "path": str(out_path), "mode": "overwrite"},
            },
            {
                "id": "probe",
                "label": "probe",
                "type": "Probe",
                "attach_to": probe_target,
                "config": {
                    "signals": [
                        {"type": "row_count_estimate", "method": "sample", "fraction": 1.0},
                    ]
                },
            },
        ],
        edges=[{"from": "src", "to": "relay"}, {"from": "relay", "to": "out"}],
    )
    manifest = ccompile(bp, engine=engine_a)

    # The Probe colocates with its target's island — never a 3rd island, and
    # never a handoff of its own.
    assert len(manifest.islands) == 2
    assert _handoff_ids(manifest) == {"src__handoff__relay"}
    assert _engine_map(manifest)["probe"] == "spark"

    handoff_root = tmp_path / "handoff_root"
    store_dir = tmp_path / "obs"
    run_id = "probe-near-boundary"
    result = _run(manifest, run_id, handoff_root, store_dir)
    assert result.status == ExecutionStatus.SUCCESS, result.module_results

    con = duckdb.connect(str(store_dir / "observability.db"))
    try:
        row = con.execute(
            "SELECT payload FROM probe_signals WHERE run_id = ? AND probe_id = ?",
            [run_id, "probe"],
        ).fetchone()
    finally:
        con.close()
    assert row is not None, "probe near the boundary recorded no signal"
    import json

    payload = json.loads(row[0])
    assert payload["estimate"] == 5

    _assert_spill_cleaned_up(manifest, handoff_root, run_id)


@pytest.mark.parametrize("engine_a,engine_b", ENGINE_PAIRS, ids=PAIR_IDS)
def test_probe_engine_mismatch_near_boundary_raises_compile_error(
    spark, tmp_path, engine_a, engine_b
):
    """The colocation rule holds even right at a boundary: a Probe pinned to
    the WRONG side of the very edge it taps is a CompileError, not a
    silently-inserted extra handoff."""
    in_path = tmp_path / "in.parquet"
    out_path = tmp_path / "out"
    _write_input(spark, engine_a, in_path, [0, 1])

    # Pin the probe to attach to "in" (upstream) but force its OWN engine to
    # the downstream engine — a mismatch regardless of which pair is active.
    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": engine_a,
                "config": {"format": "parquet", "path": str(in_path)},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": engine_b,
                "config": {"format": "parquet", "path": str(out_path), "mode": "overwrite"},
            },
            {
                "id": "probe",
                "label": "probe",
                "type": "Probe",
                "attach_to": "in",
                "engine": engine_b,
                "config": {"signals": [{"type": "schema_snapshot"}]},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    with pytest.raises(CompileError, match="colocate"):
        ccompile(bp, engine=engine_a)
