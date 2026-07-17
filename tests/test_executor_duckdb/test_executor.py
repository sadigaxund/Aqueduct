"""DuckDB engine executor tests (Phase 78 Stage A).

Engine-side mechanics for the handlers Stage A implements: Ingress
(read_parquet/read_csv), Channel (sql/join/filter/select/deduplicate),
Junction (all three modes), Funnel (union_all/union), Egress (COPY TO
parquet/csv). Not a duplicate of the Spark suite — DuckDB-specific mechanics
(COPY TO, sqlglot transpile, relation lifecycle) tested here, engine-agnostic
contract behavior stays in the parametrized-over-engines suites where they
already exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.executor.duckdb_.channel import ChannelError, execute_channel
from aqueduct.executor.duckdb_.egress import EgressError, write_egress
from aqueduct.executor.duckdb_.error_extraction import extract_duckdb_error
from aqueduct.executor.duckdb_.executor import execute
from aqueduct.executor.duckdb_.funnel import FunnelError, execute_funnel
from aqueduct.executor.duckdb_.ingress import IngressError, read_ingress
from aqueduct.executor.duckdb_.junction import JunctionError, execute_junction
from aqueduct.executor.models import ExecutionStatus
from aqueduct.executor.protocol import get_protocol
from aqueduct.models import Edge, Manifest, Module

pytestmark = pytest.mark.duckdb


def _module(id_, type_, config, **kw):
    return Module(id=id_, type=type_, label=id_, config=config, **kw)


def _write_parquet(con, tmp_path, name, rows_sql):
    path = str(tmp_path / f"{name}.parquet")
    con.sql(f"COPY ({rows_sql}) TO '{path}' (FORMAT PARQUET)")
    return path


# ── Registration ─────────────────────────────────────────────────────────

def test_duckdb_registered_via_entry_point():
    proto = get_protocol("duckdb")
    assert proto.engine == "duckdb"
    assert proto.execute is not None
    assert proto.extract_error is not None
    assert proto.prompt_rules.persona


# ── Ingress ──────────────────────────────────────────────────────────────

def test_read_ingress_parquet(duckdb_con, tmp_path):
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, 'y'")
    module = _module("ing", "Ingress", {"format": "parquet", "path": path})
    rel = read_ingress(module, duckdb_con)
    assert sorted(rel.columns) == ["a", "b"]
    assert rel.fetchall() and len(rel.fetchall()) == 2


def test_read_ingress_csv(duckdb_con, tmp_path):
    csv_path = tmp_path / "src.csv"
    csv_path.write_text("a,b\n1,x\n2,y\n")
    module = _module("ing", "Ingress", {"format": "csv", "path": str(csv_path)})
    rel = read_ingress(module, duckdb_con)
    assert sorted(rel.columns) == ["a", "b"]


def test_read_ingress_unsupported_format_raises(duckdb_con):
    module = _module("ing", "Ingress", {"format": "jdbc", "path": "whatever"})
    with pytest.raises(IngressError, match="not implemented"):
        read_ingress(module, duckdb_con)


def test_read_ingress_schema_hint_mismatch(duckdb_con, tmp_path):
    path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    module = _module(
        "ing", "Ingress",
        {"format": "parquet", "path": path, "schema_hint": {"a": "varchar"}},
    )
    with pytest.raises(IngressError, match="type mismatch"):
        read_ingress(module, duckdb_con)


# ── Channel ──────────────────────────────────────────────────────────────

def test_channel_filter(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1),(2),(3)) t(a)")
    module = _module("ch", "Channel", {"op": "filter", "condition": "a > 1"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [2, 3]


def test_channel_select(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a, 2 AS b")
    module = _module("ch", "Channel", {"op": "select", "columns": ["a"]})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.columns == ["a"]


def test_channel_deduplicate_no_key(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1),(1),(2)) t(a)")
    module = _module("ch", "Channel", {"op": "deduplicate"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [1, 2]


def test_channel_deduplicate_with_key_and_order(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1, 10), (1, 20), (2, 5)) t(k, v)")
    module = _module("ch", "Channel", {"op": "deduplicate", "key": "k", "order_by": "v DESC"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    rows = sorted(out.fetchall())
    assert rows == [(1, 20), (2, 5)]


def test_channel_sql_transpiled_from_spark_dialect(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("ch", "Channel", {"op": "sql", "query": "SELECT a + 1 AS b FROM up"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.fetchall() == [(2,)]


def test_channel_join(duckdb_con):
    # Module IDs deliberately avoid SQL reserved words (e.g. "left"/"right",
    # which DuckDB reserves for JOIN syntax even in expression position) —
    # a real Blueprint author picks module IDs, and a reserved-word ID is an
    # edge case shared by any SQL-backed engine, not something Stage A needs
    # to paper over. See aqueduct/executor/duckdb_/channel.py::_build_join_query
    # for the (real, tested) quoting this engine does for its own generated
    # FROM/JOIN table references.
    customers = duckdb_con.sql("SELECT 1 AS id, 'a' AS name")
    orders = duckdb_con.sql("SELECT 1 AS id, 100 AS amount")
    module = _module(
        "ch", "Channel",
        {
            "op": "join", "left": "customers", "right": "orders",
            "join_type": "inner", "condition": "customers.id = orders.id",
        },
    )
    out = execute_channel(module, {"customers": customers, "orders": orders}, duckdb_con)
    assert out.fetchall() == [(1, "a", 1, 100)]


def test_channel_unsupported_op_is_honest_error(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("ch", "Channel", {"op": "repartition", "num_partitions": 4})
    with pytest.raises(ChannelError, match="not implemented"):
        execute_channel(module, {"up": rel}, duckdb_con)


def test_channel_cast(duckdb_con):
    rel = duckdb_con.sql("SELECT '1' AS a, 'x' AS b")
    module = _module("ch", "Channel", {"op": "cast", "columns": {"a": "int"}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.columns == ["a", "b"]
    assert out.fetchall() == [(1, "x")]


def test_channel_rename(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a, 2 AS b")
    module = _module("ch", "Channel", {"op": "rename", "columns": {"a": "id"}})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert out.columns == ["id", "b"]
    assert out.fetchall() == [(1, 2)]


def test_channel_sort(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (3),(1),(2)) t(a)")
    module = _module("ch", "Channel", {"op": "sort", "order_by": "a DESC"})
    out = execute_channel(module, {"up": rel}, duckdb_con)
    assert [r[0] for r in out.fetchall()] == [3, 2, 1]


def test_channel_union(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("ch", "Channel", {"op": "union"})
    out = execute_channel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [1, 2]


# ── Junction ─────────────────────────────────────────────────────────────

def test_junction_conditional(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES (1),(2),(3)) t(a)")
    module = _module(
        "j", "Junction",
        {"mode": "conditional", "branches": [
            {"id": "hi", "condition": "a > 1"}, {"id": "lo", "condition": "_else_"},
        ]},
    )
    branches = execute_junction(module, rel)
    assert sorted(r[0] for r in branches["hi"].fetchall()) == [2, 3]
    assert sorted(r[0] for r in branches["lo"].fetchall()) == [1]


def test_junction_broadcast(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("j", "Junction", {"mode": "broadcast", "branches": [{"id": "x"}, {"id": "y"}]})
    branches = execute_junction(module, rel)
    assert branches["x"].fetchall() == branches["y"].fetchall() == [(1,)]


def test_junction_partition(duckdb_con):
    rel = duckdb_con.sql("SELECT * FROM (VALUES ('EU'),('US'),('EU')) t(region)")
    module = _module(
        "j", "Junction",
        {"mode": "partition", "partition_key": "region", "branches": [{"id": "eu", "value": "EU"}, {"id": "us", "value": "US"}]},
    )
    branches = execute_junction(module, rel)
    assert len(branches["eu"].fetchall()) == 2
    assert len(branches["us"].fetchall()) == 1


def test_junction_unknown_mode_raises(duckdb_con):
    rel = duckdb_con.sql("SELECT 1 AS a")
    module = _module("j", "Junction", {"mode": "bogus", "branches": []})
    with pytest.raises(JunctionError):
        execute_junction(module, rel)


# ── Funnel ───────────────────────────────────────────────────────────────

def test_funnel_union_all(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("f", "Funnel", {"mode": "union_all", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(r[0] for r in out.fetchall()) == [1, 2]


def test_funnel_union_dedupes(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 1 AS x")
    module = _module("f", "Funnel", {"mode": "union", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert out.fetchall() == [(1,)]


def test_funnel_unsupported_mode_is_honest_error(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("f", "Funnel", {"mode": "bogus", "inputs": ["a", "b"]})
    with pytest.raises(FunnelError, match="not implemented"):
        execute_funnel(module, {"a": a, "b": b}, duckdb_con)


def test_funnel_zip_row_aligned_join(duckdb_con):
    a = duckdb_con.sql("SELECT * FROM (VALUES (1,'x'),(2,'y')) t(id, name)")
    b = duckdb_con.sql("SELECT * FROM (VALUES (10),(20)) t(amount)")
    module = _module("f", "Funnel", {"mode": "zip", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(out.columns) == sorted(["id", "name", "amount"])
    rows = {r[0]: r for r in out.fetchall()}
    assert len(rows) == 2


def test_funnel_coalesce_folds_overlapping_columns(duckdb_con):
    a = duckdb_con.sql("SELECT * FROM (VALUES (1, NULL), (2, 5)) t(id, val)")
    b = duckdb_con.sql("SELECT * FROM (VALUES (100), (200)) t(val)")
    module = _module("f", "Funnel", {"mode": "coalesce", "inputs": ["a", "b"]})
    out = execute_funnel(module, {"a": a, "b": b}, duckdb_con)
    assert sorted(out.columns) == ["id", "val"]
    rows = sorted(out.fetchall())
    # first row: a.val is NULL -> falls back to b.val (100); second: a.val=5 wins
    assert rows == [(1, 100), (2, 5)]


def test_funnel_zip_duplicate_column_names_rejected(duckdb_con):
    a = duckdb_con.sql("SELECT 1 AS x")
    b = duckdb_con.sql("SELECT 2 AS x")
    module = _module("f", "Funnel", {"mode": "zip", "inputs": ["a", "b"]})
    with pytest.raises(FunnelError, match="unique column names"):
        execute_funnel(module, {"a": a, "b": b}, duckdb_con)


# ── Egress ───────────────────────────────────────────────────────────────

def test_write_egress_parquet_overwrite(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    module = _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"})
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_write_egress_csv_explicit_header_option_not_duplicated(duckdb_con, tmp_path):
    """Regression (gallery snippet 11_spillway_channel): a csv Egress with
    BOTH the default `header:` (implicit True) and an explicit
    `options: {header: ...}` must not emit `HEADER` twice in the COPY
    statement — DuckDB rejects a duplicate option name outright."""
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = str(tmp_path / "out.csv")
    module = _module(
        "eg", "Egress",
        {"format": "csv", "path": out_path, "mode": "overwrite", "options": {"header": "true"}},
    )
    write_egress(rel, module, duckdb_con)  # must not raise a duplicate-option Parser Error
    assert duckdb_con.read_csv(out_path).fetchall() == [(1,)]


def test_write_egress_error_mode_existing_target_raises(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = tmp_path / "out.parquet"
    out_path.write_bytes(b"not-really-parquet")
    module = _module("eg", "Egress", {"format": "parquet", "path": str(out_path), "mode": "error"})
    with pytest.raises(EgressError, match="already exists"):
        write_egress(rel, module, duckdb_con)


def test_write_egress_errorifexists_mode_existing_target_raises(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = tmp_path / "out.parquet"
    out_path.write_bytes(b"not-really-parquet")
    module = _module("eg", "Egress", {"format": "parquet", "path": str(out_path), "mode": "errorifexists"})
    with pytest.raises(EgressError, match="already exists"):
        write_egress(rel, module, duckdb_con)


def test_write_egress_ignore_mode_existing_target_skips_write(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = tmp_path / "out.parquet"
    out_path.write_bytes(b"original-bytes")
    module = _module("eg", "Egress", {"format": "parquet", "path": str(out_path), "mode": "ignore"})
    write_egress(rel, module, duckdb_con)  # must not raise, must not touch the file
    assert out_path.read_bytes() == b"original-bytes"


def test_write_egress_append_to_new_target_writes_once(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    module = _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "append"})
    write_egress(rel, module, duckdb_con)
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_write_egress_append_to_existing_target_unions_rows(duckdb_con, tmp_path):
    out_path = str(tmp_path / "out.parquet")
    first = duckdb_con.sql("SELECT 1 AS a")
    module = _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"})
    write_egress(first, module, duckdb_con)

    second = duckdb_con.sql("SELECT 2 AS a")
    append_module = _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "append"})
    write_egress(second, append_module, duckdb_con)

    assert sorted(r[0] for r in duckdb_con.read_parquet(out_path).fetchall()) == [1, 2]


# ── Error extraction ─────────────────────────────────────────────────────

def test_extract_duckdb_error_binder_exception(duckdb_con):
    duckdb_con.execute("CREATE TABLE t(a INT)")
    try:
        duckdb_con.execute("SELECT b FROM t")
    except Exception as exc:
        fields = extract_duckdb_error(exc)
    assert fields is not None
    assert fields["error_class"] == "BinderException"
    assert fields["object_name"] == "b"
    assert "a" in fields["suggested_columns"]


def test_extract_duckdb_error_none_input_returns_none():
    assert extract_duckdb_error(None) is None


# ── Full pipeline (Ingress -> Channel -> Egress) ────────────────────────

def test_full_pipeline_ingress_channel_egress(duckdb_con, tmp_path):
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) t(id, name)")
    out_path = str(tmp_path / "out.parquet")

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
        blueprint_id="test_bp", context={}, modules=modules, edges=edges, spark_config={},
    )

    result = execute(manifest, duckdb_con, run_id="r1")
    assert result.status == ExecutionStatus.SUCCESS
    assert {r.module_id: r.status for r in result.module_results} == {
        "ing": "success", "ch": "success", "eg": "success",
    }
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_path).fetchall()) == [2, 3]


def test_unsupported_module_type_raises_execute_error(duckdb_con):
    modules = (_module("p", "Probe", {}, attach_to="x"),)
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=(), spark_config={})
    # Probe/Assert are excluded from execution order (defense in depth — the
    # capability gate is the real enforcement point) so this manifest just
    # runs to completion with zero dispatched modules, not an ExecuteError.
    result = execute(manifest, duckdb_con, run_id="r2")
    assert result.status == ExecutionStatus.SUCCESS
    assert result.module_results == ()


# ── module.type.{Junction,Funnel,Regulator} driven through execute() ──────
# (B2 — these were already `supported` in capabilities.yml; proving the
# WHOLE module type, not just its handler function, actually runs through
# the executor's dispatch loop.)

def test_module_type_junction_driven_through_execute(duckdb_con, tmp_path):
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1),(2),(3)) t(a)")
    out_hi = str(tmp_path / "hi.parquet")
    out_lo = str(tmp_path / "lo.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("j", "Junction", {"mode": "conditional", "branches": [
            {"id": "hi", "condition": "a > 1"}, {"id": "lo", "condition": "_else_"},
        ]}),
        _module("eg_hi", "Egress", {"format": "parquet", "path": out_hi, "mode": "overwrite"}),
        _module("eg_lo", "Egress", {"format": "parquet", "path": out_lo, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="j", port="main"),
        Edge(from_id="j", to_id="eg_hi", port="hi"),
        Edge(from_id="j", to_id="eg_lo", port="lo"),
    )
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={})
    result = execute(manifest, duckdb_con, run_id="r_junction")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_hi).fetchall()) == [2, 3]
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_lo).fetchall()) == [1]


def test_module_type_funnel_driven_through_execute(duckdb_con, tmp_path):
    src_a = _write_parquet(duckdb_con, tmp_path, "a", "SELECT 1 AS x")
    src_b = _write_parquet(duckdb_con, tmp_path, "b", "SELECT 2 AS x")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing_a", "Ingress", {"format": "parquet", "path": src_a}),
        _module("ing_b", "Ingress", {"format": "parquet", "path": src_b}),
        _module("f", "Funnel", {"mode": "union_all", "inputs": ["ing_a", "ing_b"]}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing_a", to_id="f", port="main"),
        Edge(from_id="ing_b", to_id="f", port="main"),
        Edge(from_id="f", to_id="eg", port="main"),
    )
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={})
    result = execute(manifest, duckdb_con, run_id="r_funnel")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(out_path).fetchall()) == [1, 2]


def test_module_type_regulator_driven_through_execute_gate_open(duckdb_con, tmp_path):
    """No surveyor supplied -> gate defaults open, Regulator passes data through."""
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("reg", "Regulator", {}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="reg", port="main"),
        Edge(from_id="reg", to_id="eg", port="main"),
    )
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={})
    result = execute(manifest, duckdb_con, run_id="r_reg_open")
    assert result.status == ExecutionStatus.SUCCESS
    assert {r.module_id: r.status for r in result.module_results}["reg"] == "success"
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


def test_module_type_regulator_driven_through_execute_gate_closed_skips(duckdb_con, tmp_path):
    """A closed gate (on_block=skip, the default) marks the Regulator SKIPPED
    and gates downstream Egress off without failing the run."""
    from unittest.mock import MagicMock

    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("reg", "Regulator", {}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="reg", port="main"),
        Edge(from_id="reg", to_id="eg", port="main"),
    )
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={})
    surveyor = MagicMock()
    surveyor.evaluate_regulator.return_value = False
    result = execute(manifest, duckdb_con, run_id="r_reg_closed", surveyor=surveyor)
    assert result.status == ExecutionStatus.SUCCESS
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["reg"] == "skipped"
    assert statuses["eg"] == "skipped"
    assert not Path(out_path).exists()


# ── feature.spillway / feature.checkpoint driven through execute() ────────

def test_feature_spillway_driven_through_execute(duckdb_con, tmp_path):
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1),(-1),(2)) t(a)")
    main_out = str(tmp_path / "main.parquet")
    spill_out = str(tmp_path / "spill.parquet")
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("ch", "Channel", {"op": "filter", "condition": "1=1", "spillway_condition": "a < 0"}),
        _module("eg_main", "Egress", {"format": "parquet", "path": main_out, "mode": "overwrite"}),
        _module("eg_spill", "Egress", {"format": "parquet", "path": spill_out, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="ch", port="main"),
        Edge(from_id="ch", to_id="eg_main", port="main"),
        Edge(from_id="ch", to_id="eg_spill", port="spillway"),
    )
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={})
    result = execute(manifest, duckdb_con, run_id="r_spillway")
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_con.read_parquet(main_out).fetchall()) == [1, 2]
    assert sorted(r[0] for r in duckdb_con.read_parquet(spill_out).fetchall()) == [-1]


def test_feature_checkpoint_driven_through_execute(duckdb_con, tmp_path):
    src_path = _write_parquet(duckdb_con, tmp_path, "src", "SELECT 1 AS a")
    out_path = str(tmp_path / "out.parquet")
    checkpoint_root = tmp_path / "checkpoints"
    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}, checkpoint=True),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (Edge(from_id="ing", to_id="eg", port="main"),)
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={}, checkpoint=True)

    result = execute(manifest, duckdb_con, run_id="r_ckpt", checkpoint_root=checkpoint_root)
    assert result.status == ExecutionStatus.SUCCESS
    done_marker = checkpoint_root / "r_ckpt" / "ing" / "_aq_done"
    assert done_marker.exists()
    data_ckpt = checkpoint_root / "r_ckpt" / "ing" / "data" / "part-0.parquet"
    assert data_ckpt.exists()

    # Resume: Ingress reloads from the checkpoint file instead of re-reading src.
    resumed = execute(
        manifest, duckdb_con, run_id="r_ckpt_2", checkpoint_root=checkpoint_root, resume_run_id="r_ckpt",
    )
    assert resumed.status == ExecutionStatus.SUCCESS
    assert {r.module_id: r.status for r in resumed.module_results}["ing"] == "success"
    assert duckdb_con.read_parquet(out_path).fetchall() == [(1,)]


# ── Engine-invariant proof: retry_policy / module retry driven through the
# duckdb executor's own _with_retry, via a real execute() run ─────────────

def test_module_retry_driven_through_execute(duckdb_con, tmp_path, monkeypatch):
    """A Channel module fails twice then succeeds; module.retry (max_attempts=3,
    no backoff) must make the run succeed on the 3rd attempt — proving
    RetryPolicy/backoff aren't a rubber-stamp 'engine-invariant' verdict but
    are actually exercised by the duckdb executor's dispatch loop."""
    import aqueduct.executor.duckdb_.executor as executor_mod
    from aqueduct.executor.duckdb_.channel import ChannelError
    from aqueduct.parser.models import RetryPolicy

    calls = {"n": 0}
    real_execute_channel = executor_mod.execute_channel

    def flaky_execute_channel(module, upstream, con):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ChannelError("transient failure")
        return real_execute_channel(module, upstream, con)

    monkeypatch.setattr(executor_mod, "execute_channel", flaky_execute_channel)

    retry_policy = RetryPolicy(
        max_attempts=3, backoff_strategy="fixed", backoff_base_seconds=0,
        backoff_max_seconds=0, jitter=False, on_exhaustion="abort",
    )
    modules = (
        _module("ing", "Ingress", {"format": "csv", "path": "unused"}),
        _module("ch", "Channel", {"op": "filter", "condition": "1=1"}, retry=retry_policy),
    )
    edges = (Edge(from_id="ing", to_id="ch", port="main"),)
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={})

    # Swap Ingress for a direct relation registration to avoid a real file read.
    monkeypatch.setattr(
        executor_mod, "read_ingress", lambda module, con, base_dir=None: con.sql("SELECT 1 AS a")
    )

    result = execute(manifest, duckdb_con, run_id="r_retry")
    assert result.status == ExecutionStatus.SUCCESS
    assert calls["n"] == 3
    assert {r.module_id: r.status for r in result.module_results} == {"ing": "success", "ch": "success"}


def test_module_retry_exhausted_fails_run(duckdb_con, monkeypatch):
    """Retry exhaustion (on_exhaustion=abort, always-failing handler) must
    still fail the run after the declared max_attempts — the honest converse
    of the success-path retry test above."""
    import aqueduct.executor.duckdb_.executor as executor_mod
    from aqueduct.executor.duckdb_.channel import ChannelError
    from aqueduct.parser.models import RetryPolicy

    calls = {"n": 0}

    def always_fails(module, upstream, con):
        calls["n"] += 1
        raise ChannelError("permanent failure")

    monkeypatch.setattr(executor_mod, "execute_channel", always_fails)
    monkeypatch.setattr(
        executor_mod, "read_ingress", lambda module, con, base_dir=None: con.sql("SELECT 1 AS a")
    )

    retry_policy = RetryPolicy(
        max_attempts=2, backoff_strategy="fixed", backoff_base_seconds=0,
        backoff_max_seconds=0, jitter=False, on_exhaustion="abort",
    )
    modules = (
        _module("ing", "Ingress", {"format": "csv", "path": "unused"}),
        _module("ch", "Channel", {"op": "filter", "condition": "1=1"}, retry=retry_policy),
    )
    edges = (Edge(from_id="ing", to_id="ch", port="main"),)
    manifest = Manifest(blueprint_id="bp", context={}, modules=modules, edges=edges, spark_config={})

    result = execute(manifest, duckdb_con, run_id="r_retry_exhausted")
    assert result.status == ExecutionStatus.ERROR
    assert calls["n"] == 2


# ── module.type.Arcade — Arcades expand at compile time, so the executor
# never dispatches one; the proof lives at the compile+run boundary. ──────

def test_arcade_compiles_and_runs_on_duckdb(tmp_path):
    from aqueduct.compiler.compiler import compile as aq_compile
    from aqueduct.parser.parser import parse

    arcade_file = tmp_path / "sub.yml"
    arcade_file.write_text(
        "aqueduct: '1.0'\nid: arcade.sub\nname: Sub\n"
        "modules:\n"
        "  - id: ch\n    type: Channel\n    label: C\n"
        "    config: {op: filter, condition: 'a > 1'}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    parent_file = tmp_path / "parent.yml"
    src_path = tmp_path / "src.csv"
    src_path.write_text("a\n1\n2\n3\n", encoding="utf-8")
    out_path = tmp_path / "out.parquet"
    parent_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
        "modules:\n"
        f"  - id: ing\n    type: Ingress\n    label: I\n"
        f"    config: {{format: csv, path: '{src_path}'}}\n"
        "  - id: arc\n    type: Arcade\n    label: A\n    ref: 'sub.yml'\n"
        f"  - id: eg\n    type: Egress\n    label: E\n"
        f"    config: {{format: parquet, path: '{out_path}', mode: overwrite}}\n"
        "edges:\n  - from: ing\n    to: arc\n  - from: arc\n    to: eg\n",
        encoding="utf-8",
    )
    bp = parse(parent_file)
    manifest = aq_compile(bp, blueprint_path=parent_file, engine="duckdb")
    module_ids = {m.id for m in manifest.modules}
    assert "arc__ch" in module_ids

    import duckdb as duckdb_mod
    con = duckdb_mod.connect(":memory:")
    try:
        result = execute(manifest, con, run_id="r_arcade")
    finally:
        con.close()
    assert result.status == ExecutionStatus.SUCCESS
    assert sorted(r[0] for r in duckdb_mod.connect(":memory:").sql(f"SELECT * FROM read_parquet('{out_path}')").fetchall()) == [2, 3]
