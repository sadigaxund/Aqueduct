"""Batched `module_metrics` writes (fix/2.2.2 batch C).

`aqueduct/executor/models.py::write_module_metrics` (DuckDB's engine) and
`aqueduct/executor/spark/executor.py::_write_stage_metrics` (Spark's engine)
used to open a fresh DuckDB observability connection PER MODULE inside the
main execution loop — an N-module pipeline cost N connect()/close() cycles.
Both engines now collect rows in a `ModuleMetricsBuffer` during the run and
flush every row under ONE connection at run end, in a `finally` so a run
that fails partway through still persists the metrics it already collected —
mirroring the batched-write pattern in `aqueduct/cli/drift.py` (its
`pending_checks` list + single `record_checks` flush).

These tests cover:
  * `ModuleMetricsBuffer` itself (add/update/flush) in isolation.
  * DuckDB `execute()`: a multi-module pipeline produces exactly ONE
    `store.connect()` call for the metrics flush, and a run that fails
    partway through still flushes the rows collected before the failure.
  * Spark `execute()`: same one-connect() assertion, using the real Spark
    fixture (never a mocked SparkSession) plus the `_update_metric` gotcha
    (Spark's deferred Ingress `records_read` observation must land on the
    still-buffered row, not a SQL UPDATE against a row that was never
    written).
"""

from __future__ import annotations

import duckdb
import pytest

from aqueduct.executor.models import ModuleMetricsBuffer, resolve_observability_store
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

# ── ModuleMetricsBuffer — engine-agnostic unit tests ────────────────────────


def test_buffer_add_then_flush_writes_all_rows_under_one_connection(tmp_path, monkeypatch):
    store_dir = tmp_path / "obs"
    buf = ModuleMetricsBuffer()
    buf.add("run-1", "mod_a", {"records_read": 3, "duration_ms": 10})
    buf.add("run-1", "mod_b", {"records_written": 7, "duration_ms": 20})
    assert bool(buf) is True

    connect_calls = []
    real_connect = DuckDBObservabilityStore.connect

    def counting_connect(self, *a, **kw):
        connect_calls.append(1)
        return real_connect(self, *a, **kw)

    monkeypatch.setattr(DuckDBObservabilityStore, "connect", counting_connect)

    buf.flush(store_dir, None)

    assert len(connect_calls) == 1
    assert bool(buf) is False  # drained

    store = resolve_observability_store(store_dir, None)
    with store.connect() as cur:
        cur.execute(
            "SELECT module_id, records_read, records_written, duration_ms "
            "FROM module_metrics WHERE run_id = 'run-1' ORDER BY module_id"
        )
        rows = cur.fetchall()
    assert rows == [("mod_a", 3, None, 10), ("mod_b", None, 7, 20)]


def test_buffer_flush_is_a_noop_when_empty(tmp_path, monkeypatch):
    store_dir = tmp_path / "obs"
    buf = ModuleMetricsBuffer()

    connect_calls = []
    real_connect = DuckDBObservabilityStore.connect

    def counting_connect(self, *a, **kw):
        connect_calls.append(1)
        return real_connect(self, *a, **kw)

    monkeypatch.setattr(DuckDBObservabilityStore, "connect", counting_connect)

    buf.flush(store_dir, None)

    assert connect_calls == []
    # An empty flush must not even create the observability.db file.
    assert not (store_dir / "observability.db").exists()


def test_buffer_update_mutates_pending_row_in_place():
    buf = ModuleMetricsBuffer()
    buf.add("run-1", "ing", {"duration_ms": 5})

    updated = buf.update("ing", "records_read", 42)

    assert updated is True
    # Internal row reflects the mutation (checked via a flush + read-back).


def test_buffer_update_returns_false_for_unknown_module_id():
    buf = ModuleMetricsBuffer()
    buf.add("run-1", "ing", {"duration_ms": 5})

    # No pending row for "eg" — caller must fall back to a real SQL UPDATE.
    assert buf.update("eg", "records_read", 1) is False


def test_buffer_update_then_flush_persists_the_mutated_value(tmp_path):
    store_dir = tmp_path / "obs"
    buf = ModuleMetricsBuffer()
    buf.add("run-1", "ing", {"duration_ms": 5})
    assert buf.update("ing", "records_read", 42) is True

    buf.flush(store_dir, None)

    store = resolve_observability_store(store_dir, None)
    with store.connect() as cur:
        cur.execute("SELECT records_read FROM module_metrics WHERE module_id = 'ing'")
        (records_read,) = cur.fetchone()
    assert records_read == 42


# ── DuckDB engine: one connect() per run, not per module ────────────────────


def _module(id_, type_, config, **kw):
    from aqueduct.models import Module

    return Module(id=id_, type=type_, label=id_, config=config, **kw)


@pytest.mark.duckdb
def test_duckdb_multi_module_run_flushes_metrics_under_one_connection(tmp_path, monkeypatch):
    from aqueduct.executor.duckdb_.executor import execute
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.models import Edge, Manifest

    con = duckdb.connect(":memory:")
    src_path = str(tmp_path / "src.parquet")
    con.sql(
        "COPY (SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) t(id, name)) "
        f"TO '{src_path}' (FORMAT PARQUET)"
    )
    out_path = str(tmp_path / "out.parquet")
    store_dir = tmp_path / "obs"

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("ch", "Channel", {"op": "filter", "condition": "id > 1"}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="ch", port="main"),
        Edge(from_id="ch", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="test_bp_batch", context={}, modules=modules, edges=edges, engine_config={}
    )

    connect_calls = []
    real_connect = DuckDBObservabilityStore.connect

    def counting_connect(self, *a, **kw):
        connect_calls.append(1)
        return real_connect(self, *a, **kw)

    monkeypatch.setattr(DuckDBObservabilityStore, "connect", counting_connect)

    result = execute(manifest, con, run_id="r_batch1", store_dir=store_dir)

    assert result.status == ExecutionStatus.SUCCESS
    # 3 modules (Ingress, Channel, Egress) each wrote a module_metrics row —
    # before batching this was 3 separate connect()/close() cycles.
    assert len(connect_calls) == 1

    store = DuckDBObservabilityStore(store_dir / "observability.db")
    with store.connect() as cur:
        cur.execute(
            "SELECT module_id FROM module_metrics WHERE run_id = 'r_batch1' ORDER BY module_id"
        )
        module_ids = {r[0] for r in cur.fetchall()}
    assert module_ids == {"ing", "ch", "eg"}


@pytest.mark.duckdb
def test_duckdb_run_that_fails_partway_still_flushes_collected_metrics(tmp_path):
    """A run interrupted mid-loop by a module failure (a partial run, same
    shape as `drift.py`'s partial-audit-trail case) must still persist the
    module_metrics rows collected before the failure — a batched flush must
    not turn "partial metrics" into "no metrics"."""
    from aqueduct.executor.duckdb_.executor import execute
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.models import Edge, Manifest

    con = duckdb.connect(":memory:")
    src_path = str(tmp_path / "src.parquet")
    con.sql("COPY (SELECT * FROM (VALUES (1),(2),(3)) t(id)) " f"TO '{src_path}' (FORMAT PARQUET)")
    store_dir = tmp_path / "obs"

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        # Invalid SQL condition — Channel fails, run ends in error.
        _module("ch", "Channel", {"op": "filter", "condition": "not a valid $$ expr"}),
    )
    edges = (Edge(from_id="ing", to_id="ch", port="main"),)
    manifest = Manifest(
        blueprint_id="test_bp_partial", context={}, modules=modules, edges=edges, engine_config={}
    )

    result = execute(manifest, con, run_id="r_partial1", store_dir=store_dir)

    assert result.status == ExecutionStatus.ERROR

    store = DuckDBObservabilityStore(store_dir / "observability.db")
    with store.connect() as cur:
        cur.execute("SELECT module_id FROM module_metrics WHERE run_id = 'r_partial1'")
        module_ids = {r[0] for r in cur.fetchall()}
    # "ing" ran successfully before "ch" failed — its metrics must survive.
    assert "ing" in module_ids


# ── Spark engine: one connect() per run, plus the `_update_metric` gotcha ──


@pytest.mark.spark
@pytest.mark.integration
def test_spark_multi_module_run_flushes_metrics_under_one_connection(spark, tmp_path):
    from aqueduct.compiler.models import Manifest
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.spark.executor import execute
    from aqueduct.parser.models import Edge, Module

    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    out_path = str(tmp_path / "out.parquet")
    store_dir = tmp_path / "store"

    manifest = Manifest(
        blueprint_id="test.metrics_batch",
        modules=(
            Module(
                id="ing", type="Ingress", label="Ing", config={"format": "parquet", "path": in_path}
            ),
            Module(
                id="egr", type="Egress", label="Egr", config={"format": "parquet", "path": out_path}
            ),
        ),
        edges=(Edge(from_id="ing", to_id="egr", port="main"),),
        context={},
        engine_config={},
    )

    connect_calls = []
    real_connect = DuckDBObservabilityStore.connect

    def counting_connect(self, *a, **kw):
        connect_calls.append(1)
        return real_connect(self, *a, **kw)

    import pytest as _pytest  # local import so module-level collection never needs monkeypatch

    mp = _pytest.MonkeyPatch()
    mp.setattr(DuckDBObservabilityStore, "connect", counting_connect)
    try:
        result = execute(manifest, spark, run_id="r_spark_batch1", store_dir=store_dir)
    finally:
        mp.undo()

    assert result.status == ExecutionStatus.SUCCESS
    # Ingress + Egress each wrote a stage-metrics row, PLUS the deferred
    # `_update_metric(records_read, ...)` call after the loop — all of it
    # must land in the single end-of-run flush connection, not a connect()
    # per write.
    assert len(connect_calls) == 1

    store = DuckDBObservabilityStore(store_dir / "observability.db")
    with store.connect() as cur:
        cur.execute(
            "SELECT module_id, records_read FROM module_metrics "
            "WHERE run_id = 'r_spark_batch1' ORDER BY module_id"
        )
        rows = {r[0]: r[1] for r in cur.fetchall()}

    assert set(rows) == {"ing", "egr"}
    # The gotcha this batching change had to handle: Spark's deferred
    # Ingress `records_read` observation is applied via `_update_metric`
    # AFTER `_write_stage_metrics` already buffered the "ing" row (never
    # flushed yet) — if `_update_metric` fell back to a SQL UPDATE here it
    # would silently match zero rows and this value would be NULL.
    assert rows["ing"] == 5
