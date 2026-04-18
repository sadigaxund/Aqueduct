"""Tests for the Executor orchestrator and models."""

from __future__ import annotations

import re
import uuid
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from aqueduct.compiler.models import Manifest
from aqueduct.executor.executor import ExecuteError, execute
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.parser.models import Edge, Module


@pytest.fixture
def minimal_manifest(tmp_path):
    """A valid Ingress -> Egress manifest."""
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out.parquet")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.range(5).write.parquet(in_path)

    return Manifest(
        pipeline_id="test.pipeline",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )


# ── Model Tests ───────────────────────────────────────────────────────────────

def test_execution_result_is_frozen():
    res = ExecutionResult(pipeline_id="p", run_id="r", status="success", module_results=())
    with pytest.raises(FrozenInstanceError):
        res.status = "error"  # type: ignore[misc]


def test_execution_result_to_dict():
    res = ExecutionResult(
        pipeline_id="p",
        run_id="r",
        status="success",
        module_results=(ModuleResult(module_id="m1", status="success"),)
    )
    d = res.to_dict()
    assert d["pipeline_id"] == "p"
    assert d["module_results"][0]["module_id"] == "m1"


# ── Basic Orchestration Tests ─────────────────────────────────────────────────

def test_execute_success(spark: SparkSession, minimal_manifest):
    result = execute(minimal_manifest, spark)

    assert result.status == "success", result.module_results
    assert len(result.module_results) == 2
    assert all(r.status == "success" for r in result.module_results)

    out_path = minimal_manifest.modules[1].config["path"]
    assert spark.read.parquet(out_path).count() == 5


def test_execute_unsupported_module_type(spark: SparkSession):
    manifest = Manifest(
        pipeline_id="test.unsupported",
        modules=(Module(id="m1", type="Spillway", label="S1", config={}),),
        edges=(),
        context={},
        spark_config={}
    )
    with pytest.raises(ExecuteError, match="Module type 'Spillway' .* is not supported"):
        execute(manifest, spark)


def test_execute_channel_linear(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).selectExpr("id", "id * 2 as val").write.parquet(in_path)
    out_path = str(tmp_path / "out.parquet")

    manifest = Manifest(
        pipeline_id="test.channel_linear",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="chan", type="Channel", label="Chan", config={"op": "sql", "query": "SELECT * FROM in WHERE id > 2"}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="chan", port="main"),
            Edge(from_id="chan", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "success", result.module_results

    df_out = spark.read.parquet(out_path)
    assert df_out.count() == 2
    assert sorted([r[0] for r in df_out.select("id").collect()]) == [3, 4]


def test_execute_channel_multi_input(spark: SparkSession, tmp_path):
    in1_path = str(tmp_path / "in1.parquet")
    spark.range(5).selectExpr("id", "'a' as tag").write.parquet(in1_path)
    in2_path = str(tmp_path / "in2.parquet")
    spark.range(5).selectExpr("id", "'b' as tag").write.parquet(in2_path)
    out_path = str(tmp_path / "out_multi.parquet")

    manifest = Manifest(
        pipeline_id="test.channel_multi",
        modules=(
            Module(id="in1", type="Ingress", label="In1", config={"format": "parquet", "path": in1_path}),
            Module(id="in2", type="Ingress", label="In2", config={"format": "parquet", "path": in2_path}),
            Module(id="chan", type="Channel", label="Chan", config={
                "op": "sql",
                "query": "SELECT id, tag FROM in1 UNION ALL SELECT id, tag FROM in2"
            }),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in1", to_id="chan", port="main"),
            Edge(from_id="in2", to_id="chan", port="main"),
            Edge(from_id="chan", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "success", result.module_results

    df_out = spark.read.parquet(out_path)
    assert df_out.count() == 10


def test_execute_channel_error(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)

    manifest = Manifest(
        pipeline_id="test.channel_err",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="chan", type="Channel", label="Chan", config={"op": "sql", "query": "SELECT * FROM non_existent"}),
        ),
        edges=(Edge(from_id="in", to_id="chan", port="main"),),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "error"
    assert result.module_results[1].module_id == "chan"
    assert "cannot be found" in result.module_results[1].error


def test_execute_channel_missing_upstream_edge(spark: SparkSession):
    manifest = Manifest(
        pipeline_id="test.channel_no_edge",
        modules=(
            Module(id="chan", type="Channel", label="Chan", config={"op": "sql", "query": "SELECT 1"}),
        ),
        edges=(),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "error"
    assert "no main-port incoming edges" in result.module_results[0].error


def test_execute_ingress_error(spark: SparkSession):
    manifest = Manifest(
        pipeline_id="test.ingress_err",
        modules=(Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": "/ghost"}),),
        edges=(),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)

    assert result.status == "error"
    assert result.module_results[0].module_id == "in"
    assert "not found" in result.module_results[0].error.lower()


def test_execute_egress_error(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)

    manifest = Manifest(
        pipeline_id="test.egress_err",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": "/foo", "mode": "hacked"}),
        ),
        edges=(Edge(from_id="in", to_id="out", port="main"),),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)

    assert result.status == "error"
    assert result.module_results[1].status == "error"
    assert "unsupported write mode 'hacked'" in result.module_results[1].error


def test_execute_missing_upstream_edge(spark: SparkSession, tmp_path):
    manifest = Manifest(
        pipeline_id="test.no_edge",
        modules=(Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": "/foo"}),),
        edges=(),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)

    assert result.status == "error"
    assert "no main-port edge" in result.module_results[0].error


def test_execute_run_id_auto_gen(spark: SparkSession, minimal_manifest):
    result = execute(minimal_manifest, spark)
    assert re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", result.run_id)


def test_execute_run_id_supplied(spark: SparkSession, minimal_manifest):
    my_id = "custom-run-123"
    result = execute(minimal_manifest, spark, run_id=my_id)
    assert result.run_id == my_id


def test_execute_cycle_in_manifest(spark: SparkSession):
    manifest = Manifest(
        pipeline_id="test.cycle",
        modules=(
            Module(id="m1", type="Ingress", label="M1", config={"format": "p", "path": "p"}),
            Module(id="m2", type="Egress", label="M2", config={"format": "p", "path": "p"}),
        ),
        edges=(
            Edge(from_id="m1", to_id="m2", port="main"),
            Edge(from_id="m2", to_id="m1", port="main"),
        ),
        context={},
        spark_config={}
    )
    with pytest.raises(ExecuteError, match="Cycle detected"):
        execute(manifest, spark)


# ── Junction Integration ──────────────────────────────────────────────────────

def test_execute_junction_error_no_main_edge(spark: SparkSession):
    """Junction with no incoming edge → error result."""
    manifest = Manifest(
        pipeline_id="test.junc_no_edge",
        modules=(
            Module(id="j1", type="Junction", label="J1", config={
                "mode": "broadcast", "branches": [{"id": "b1"}]
            }),
        ),
        edges=(),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "error"
    assert "no main-port incoming edges" in result.module_results[0].error


def test_execute_junction_exec_error(spark: SparkSession, tmp_path):
    """Invalid Junction mode → JunctionError captured in result."""
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)

    manifest = Manifest(
        pipeline_id="test.junc_exec_err",
        modules=(
            Module(id="m1", type="Ingress", label="M1", config={"format": "parquet", "path": in_path}),
            Module(id="j1", type="Junction", label="J1", config={"mode": "bad"}),
        ),
        edges=(Edge(from_id="m1", to_id="j1", port="main"),),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "error"
    assert result.module_results[1].module_id == "j1"
    assert "unsupported mode" in result.module_results[1].error


def test_execute_junction_conditional(spark: SparkSession, tmp_path):
    """Ingress → Junction (conditional) → two Egress modules with correct row counts."""
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).selectExpr(
        "id", "CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END as tag"
    ).write.parquet(in_path)

    out_even = str(tmp_path / "even.parquet")
    out_odd = str(tmp_path / "odd.parquet")

    manifest = Manifest(
        pipeline_id="test.junc_cond",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="jn", type="Junction", label="Jn", config={
                "mode": "conditional",
                "branches": [
                    {"id": "even", "condition": "tag = 'even'"},
                    {"id": "odd",  "condition": "tag = 'odd'"},
                ]
            }),
            Module(id="out_even", type="Egress", label="OutEven", config={"format": "parquet", "path": out_even}),
            Module(id="out_odd",  type="Egress", label="OutOdd",  config={"format": "parquet", "path": out_odd}),
        ),
        edges=(
            Edge(from_id="in",  to_id="jn",       port="main"),
            Edge(from_id="jn",  to_id="out_even",  port="even"),
            Edge(from_id="jn",  to_id="out_odd",   port="odd"),
        ),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "success", result.module_results

    # ids 0,2,4 → even (3 rows); ids 1,3 → odd (2 rows)
    assert spark.read.parquet(out_even).count() == 3
    assert spark.read.parquet(out_odd).count() == 2


def test_execute_junction_broadcast(spark: SparkSession, tmp_path):
    """Ingress → Junction (broadcast) → two Egress each receiving all rows."""
    in_path = str(tmp_path / "in.parquet")
    spark.range(4).write.parquet(in_path)

    out_a = str(tmp_path / "a.parquet")
    out_b = str(tmp_path / "b.parquet")

    manifest = Manifest(
        pipeline_id="test.junc_broad",
        modules=(
            Module(id="in",    type="Ingress",  label="In",   config={"format": "parquet", "path": in_path}),
            Module(id="jn",    type="Junction", label="Jn",   config={
                "mode": "broadcast",
                "branches": [{"id": "a"}, {"id": "b"}]
            }),
            Module(id="out_a", type="Egress",   label="OutA", config={"format": "parquet", "path": out_a}),
            Module(id="out_b", type="Egress",   label="OutB", config={"format": "parquet", "path": out_b}),
        ),
        edges=(
            Edge(from_id="in", to_id="jn",    port="main"),
            Edge(from_id="jn", to_id="out_a", port="a"),
            Edge(from_id="jn", to_id="out_b", port="b"),
        ),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "success", result.module_results

    assert spark.read.parquet(out_a).count() == 4
    assert spark.read.parquet(out_b).count() == 4


# ── Funnel Integration ────────────────────────────────────────────────────────

def test_execute_funnel_error_no_data_edge(spark: SparkSession):
    """Funnel with no incoming edges → error result."""
    manifest = Manifest(
        pipeline_id="test.funn_no_edge",
        modules=(
            Module(id="f1", type="Funnel", label="F1", config={
                "mode": "union_all", "inputs": ["a", "b"]
            }),
        ),
        edges=(),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "error"
    assert "no incoming data edges" in result.module_results[0].error


def test_execute_funnel_exec_error(spark: SparkSession, tmp_path):
    """Invalid Funnel mode → FunnelError captured in result."""
    in_path = str(tmp_path / "in.parquet")
    spark.range(3).write.parquet(in_path)

    manifest = Manifest(
        pipeline_id="test.funn_exec_err",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="f1", type="Funnel", label="F1", config={
                "mode": "bad", "inputs": ["in", "in"]
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="f1", port="main"),
        ),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "error"
    assert result.module_results[1].module_id == "f1"
    assert "unsupported mode" in result.module_results[1].error


def test_execute_funnel_union_all(spark: SparkSession, tmp_path):
    """Two Ingress → Funnel (union_all) → Egress stacks rows."""
    in1_path = str(tmp_path / "in1.parquet")
    spark.range(0, 3).write.parquet(in1_path)
    in2_path = str(tmp_path / "in2.parquet")
    spark.range(3, 5).write.parquet(in2_path)
    out_path = str(tmp_path / "out.parquet")

    manifest = Manifest(
        pipeline_id="test.funn_union_all",
        modules=(
            Module(id="in1", type="Ingress", label="In1", config={"format": "parquet", "path": in1_path}),
            Module(id="in2", type="Ingress", label="In2", config={"format": "parquet", "path": in2_path}),
            Module(id="f1",  type="Funnel",  label="F1",  config={"mode": "union_all", "inputs": ["in1", "in2"]}),
            Module(id="out", type="Egress",  label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in1", to_id="f1",  port="main"),
            Edge(from_id="in2", to_id="f1",  port="main"),
            Edge(from_id="f1",  to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "success", result.module_results
    assert spark.read.parquet(out_path).count() == 5


def test_execute_junction_to_funnel_roundtrip(spark: SparkSession, tmp_path):
    """Ingress → Junction (broadcast) → Funnel (union) → Egress.

    Broadcast produces two identical branch DataFrames; union deduplicates
    back to the original row count.
    """
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    out_path = str(tmp_path / "out_roundtrip.parquet")

    manifest = Manifest(
        pipeline_id="test.roundtrip",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="j1", type="Junction", label="J1", config={
                "mode": "broadcast",
                "branches": [{"id": "path1"}, {"id": "path2"}]
            }),
            Module(id="f1", type="Funnel", label="F1", config={
                "mode": "union",
                "inputs": ["j1.path1", "j1.path2"]
            }),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="j1", port="main"),
            Edge(from_id="j1", to_id="f1", port="path1"),
            Edge(from_id="j1", to_id="f1", port="path2"),
            Edge(from_id="f1", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )

    result = execute(manifest, spark)
    assert result.status == "success", result.module_results

    # broadcast gives identical rows on both paths; union deduplicates → 5 rows
    assert spark.read.parquet(out_path).count() == 5


# ── Probe Integration ─────────────────────────────────────────────────────────

def test_execute_probe_appended_last(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    out_path = str(tmp_path / "out.parquet")
    manifest = Manifest(
        pipeline_id="test.probe_last",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
            Module(id="p1", type="Probe", label="P1", config={
                "attach_to": "in",
                "signals": [{"type": "schema_snapshot"}]
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "success"

def test_execute_probe_missing_attach_to(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    out_path = str(tmp_path / "out.parquet")
    manifest = Manifest(
        pipeline_id="test.probe_err",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
            Module(id="p1", type="Probe", label="P1", config={
                "attach_to": "missing",
                "signals": [{"type": "schema_snapshot"}]
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "success"

def test_execute_probe_failure_ignores(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    manifest = Manifest(
        pipeline_id="test.probe_no_store",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="p1", type="Probe", label="P1", config={
                "attach_to": "in",
                "signals": [{"type": "schema_snapshot"}]
            }),
        ),
        edges=(),
        context={},
        spark_config={}
    )
    # store_dir is None => probe will fail to create DB, but pipeline should stay success
    result = execute(manifest, spark, store_dir=None)
    assert result.status == "success"


# ── Regulator Integration ─────────────────────────────────────────────────────

def test_execute_regulator_open_gate_no_surveyor(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    out_path = str(tmp_path / "out.parquet")
    
    manifest = Manifest(
        pipeline_id="test.regulator_open_no_surveyor",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="reg", type="Regulator", label="Reg", config={}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="reg", port="main"),
            Edge(from_id="reg", to_id="out", port="main"),
        ),
        context={}, spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "success"
    assert spark.read.parquet(out_path).count() == 5

def test_execute_regulator_open_gate_surveyor_true(spark: SparkSession, tmp_path):
    class MockSurveyor:
        def evaluate_regulator(self, reg_id): return True
        
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    out_path = str(tmp_path / "out.parquet")
    
    manifest = Manifest(
        pipeline_id="test.regulator_open_surveyor",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="reg", type="Regulator", label="Reg", config={}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="reg", port="main"),
            Edge(from_id="reg", to_id="out", port="main"),
        ),
        context={}, spark_config={}
    )
    result = execute(manifest, spark, surveyor=MockSurveyor())
    assert result.status == "success"
    assert [mr.status for mr in result.module_results] == ["success", "success", "success"]
    assert spark.read.parquet(out_path).count() == 5

def test_execute_regulator_closed_gate_skip(spark: SparkSession, tmp_path):
    class MockSurveyor:
        def evaluate_regulator(self, reg_id): return False
        
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    manifest = Manifest(
        pipeline_id="test.regulator_closed_skip",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="reg", type="Regulator", label="Reg", config={"on_block": "skip"}),
        ),
        edges=(
            Edge(from_id="in", to_id="reg", port="main"),
        ),
        context={}, spark_config={}
    )
    result = execute(manifest, spark, surveyor=MockSurveyor())
    assert result.status == "success"
    assert result.module_results[1].status == "skipped"

def test_execute_regulator_closed_gate_abort(spark: SparkSession, tmp_path):
    class MockSurveyor:
        def evaluate_regulator(self, reg_id): return False
        
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    manifest = Manifest(
        pipeline_id="test.regulator_closed_abort",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="reg", type="Regulator", label="Reg", config={"on_block": "abort"}),
        ),
        edges=(
            Edge(from_id="in", to_id="reg", port="main"),
        ),
        context={}, spark_config={}
    )
    result = execute(manifest, spark, surveyor=MockSurveyor())
    assert result.status == "error"
    assert result.module_results[1].status == "error"
    assert "on_block=abort" in result.module_results[1].error

def test_execute_regulator_closed_gate_trigger_agent(spark: SparkSession, tmp_path):
    class MockSurveyor:
        def evaluate_regulator(self, reg_id): return False
        
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    manifest = Manifest(
        pipeline_id="test.regulator_closed_trigger",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="reg", type="Regulator", label="Reg", config={"on_block": "trigger_agent"}),
        ),
        edges=(
            Edge(from_id="in", to_id="reg", port="main"),
        ),
        context={}, spark_config={}
    )
    result = execute(manifest, spark, surveyor=MockSurveyor())
    assert result.status == "success"
    assert result.module_results[1].status == "skipped"

def test_execute_regulator_downstream_skipped(spark: SparkSession, tmp_path):
    class MockSurveyor:
        def evaluate_regulator(self, reg_id): return False
        
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    manifest = Manifest(
        pipeline_id="test.regulator_downstream_skip",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="reg", type="Regulator", label="Reg", config={"on_block": "skip"}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": "/foo"}),
        ),
        edges=(
            Edge(from_id="in", to_id="reg", port="main"),
            Edge(from_id="reg", to_id="out", port="main"),
        ),
        context={}, spark_config={}
    )
    result = execute(manifest, spark, surveyor=MockSurveyor())
    assert result.status == "success"
    assert result.module_results[1].status == "skipped"
    assert result.module_results[2].status == "skipped"

def test_execute_regulator_no_main_edge(spark: SparkSession):
    manifest = Manifest(
        pipeline_id="test.regulator_no_edge",
        modules=(
            Module(id="reg", type="Regulator", label="Reg", config={}),
        ),
        edges=(),
        context={}, spark_config={}
    )
    result = execute(manifest, spark)
    assert result.status == "error"
    assert result.module_results[0].status == "error"
    assert "no main-port incoming edges" in result.module_results[0].error

