"""DuckDB incremental-Channel watermark tests (Pass A, item 3).

Mirrors ``tests/test_executor/test_incremental.py`` (Spark) exactly — same
read -> substitute -> run -> persist-new-max cycle, same Depot-only contract,
same first-run sentinel — but exercised against the DuckDB engine's own
``execute()``. The Depot is engine-independent, so a Mock depot object with
the same ``get``/``put`` shape works for both engines' tests.

``materialize``/``watermark_column`` are declared Module fields (2.40), not
``config:`` keys — see ``aqueduct.parser.schema.ModuleSchema``.
"""

from __future__ import annotations

import pytest

from aqueduct.executor.duckdb_.executor import execute
from aqueduct.models import Edge, Manifest, Module

pytestmark = pytest.mark.duckdb


class MockDepot:
    def __init__(self, initial=None):
        self.data = initial or {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def put(self, key, value):
        self.data[key] = value


def _module(id_, type_, config, **kw):
    return Module(id=id_, type=type_, label=id_, config=config, **kw)


def _manifest(modules, edges):
    return Manifest(
        blueprint_id="test_bp", context={}, modules=modules, edges=edges, engine_config={}
    )


def _write_parquet(con, tmp_path, name, rows_sql):
    path = str(tmp_path / f"{name}.parquet")
    con.sql(f"COPY ({rows_sql}) TO '{path}' (FORMAT PARQUET)")
    return path


def test_incremental_watermark_no_prior(duckdb_con, tmp_path):
    """materialize=incremental, no prior watermark -> ${ctx._watermark} replaced with sentinel."""
    in_path = _write_parquet(
        duckdb_con,
        tmp_path,
        "in",
        "SELECT CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS ts, i AS id FROM range(5) t(i)",
    )
    depot = MockDepot()
    manifest = _manifest(
        modules=(
            _module("src", "Ingress", {"format": "parquet", "path": in_path}),
            _module(
                "inc",
                "Channel",
                {"op": "sql", "query": "SELECT * FROM src WHERE ts > ${ctx._watermark}"},
                materialize="incremental",
                watermark_column="ts",
            ),
        ),
        edges=(Edge(from_id="src", to_id="inc", port="main"),),
    )
    result = execute(manifest, duckdb_con, depot=depot)
    assert result.status == "success"


def test_incremental_watermark_with_prior(duckdb_con, tmp_path):
    """materialize=incremental, prior watermark in Depot -> query substituted with stored value."""
    in_path = _write_parquet(
        duckdb_con,
        tmp_path,
        "in_prior",
        "SELECT * FROM (VALUES "
        "(CAST('2024-01-01 10:00:00' AS TIMESTAMP), 1), "
        "(CAST('2024-01-01 12:00:00' AS TIMESTAMP), 2)"
        ") t(ts, id)",
    )
    depot = MockDepot({"test_bp:inc_p:_watermark": "2024-01-01 11:00:00"})
    out_path = str(tmp_path / "out_p.parquet")
    manifest = _manifest(
        modules=(
            _module("src", "Ingress", {"format": "parquet", "path": in_path}),
            _module(
                "inc_p",
                "Channel",
                {"op": "sql", "query": "SELECT * FROM src WHERE ts > ${ctx._watermark}"},
                materialize="incremental",
                watermark_column="ts",
            ),
            _module("out", "Egress", {"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="src", to_id="inc_p", port="main"),
            Edge(from_id="inc_p", to_id="out", port="main"),
        ),
    )
    result = execute(manifest, duckdb_con, depot=depot)
    assert result.status == "success"

    rel = duckdb_con.read_parquet(out_path)
    rows = rel.fetchall()
    assert len(rows) == 1
    assert rows[0][rel.columns.index("id")] == 2
    assert depot.get("test_bp:inc_p:_watermark") == "2024-01-01 12:00:00"


def test_incremental_watermark_failure_not_updated(duckdb_con, tmp_path):
    """materialize=incremental, Channel fails -> watermark NOT updated in Depot."""
    in_path = _write_parquet(
        duckdb_con,
        tmp_path,
        "in_fail",
        "SELECT CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS ts, i AS id FROM range(5) t(i)",
    )
    depot = MockDepot({"test_bp:inc_f:_watermark": "2024-01-01 09:00:00"})
    manifest = _manifest(
        modules=(
            _module("src", "Ingress", {"format": "parquet", "path": in_path}),
            _module(
                "inc_f",
                "Channel",
                {"op": "sql", "query": "SELECT * FROM non_existent"},
                materialize="incremental",
                watermark_column="ts",
            ),
        ),
        edges=(Edge(from_id="src", to_id="inc_f", port="main"),),
    )
    result = execute(manifest, duckdb_con, depot=depot)
    assert result.status == "error"
    assert depot.get("test_bp:inc_f:_watermark") == "2024-01-01 09:00:00"


def test_incremental_egress_overwrite_warning(duckdb_con, tmp_path, caplog):
    """materialize=incremental, downstream Egress has mode=overwrite -> warning logged."""
    in_path = _write_parquet(
        duckdb_con,
        tmp_path,
        "in_warn",
        "SELECT CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS ts",
    )
    manifest = _manifest(
        modules=(
            _module("src", "Ingress", {"format": "parquet", "path": in_path}),
            _module(
                "inc",
                "Channel",
                {"op": "sql", "query": "SELECT * FROM src"},
                materialize="incremental",
                watermark_column="ts",
            ),
            _module(
                "out",
                "Egress",
                {"format": "parquet", "path": str(tmp_path / "out"), "mode": "overwrite"},
            ),
        ),
        edges=(
            Edge(from_id="src", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        ),
    )
    with caplog.at_level("WARNING"):
        execute(manifest, duckdb_con)
    assert "mode=overwrite" in caplog.text


def test_incremental_no_materialize_no_logic(duckdb_con, tmp_path):
    """no materialize field -> normal Channel execution, no watermark logic."""
    in_path = _write_parquet(
        duckdb_con,
        tmp_path,
        "in_none",
        "SELECT CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS ts",
    )
    depot = MockDepot()
    manifest = _manifest(
        modules=(
            _module("src", "Ingress", {"format": "parquet", "path": in_path}),
            _module(
                "inc", "Channel", {"op": "sql", "query": "SELECT * FROM src"}, watermark_column="ts"
            ),
        ),
        edges=(Edge(from_id="src", to_id="inc", port="main"),),
    )
    execute(manifest, duckdb_con, depot=depot)
    assert not depot.data


def test_incremental_depot_none_no_crash(duckdb_con, tmp_path):
    """materialize=incremental, depot=None -> query uses sentinel, no crash."""
    in_path = _write_parquet(
        duckdb_con,
        tmp_path,
        "in_nodepot",
        "SELECT CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS ts",
    )
    manifest = _manifest(
        modules=(
            _module("src", "Ingress", {"format": "parquet", "path": in_path}),
            _module(
                "inc",
                "Channel",
                {"op": "sql", "query": "SELECT * FROM src WHERE ts > ${ctx._watermark}"},
                materialize="incremental",
                watermark_column="ts",
            ),
        ),
        edges=(Edge(from_id="src", to_id="inc", port="main"),),
    )
    result = execute(manifest, duckdb_con, depot=None)
    assert result.status == "success"


def test_incremental_two_runs_real_incremental_behavior(duckdb_con, tmp_path):
    """End-to-end proof: a second run processes only rows past the persisted
    watermark, not the whole file — the DuckDB equivalent of snippet
    19_depot_incremental's two-run walkthrough."""
    in_path = _write_parquet(
        duckdb_con,
        tmp_path,
        "in_e2e",
        "SELECT * FROM (VALUES "
        "(CAST('2024-01-01 10:00:00' AS TIMESTAMP), 1), "
        "(CAST('2024-01-01 11:00:00' AS TIMESTAMP), 2), "
        "(CAST('2024-01-01 12:00:00' AS TIMESTAMP), 3)"
        ") t(ts, id)",
    )
    out_path = str(tmp_path / "out_e2e.parquet")
    depot = MockDepot()

    def _make_manifest():
        return _manifest(
            modules=(
                _module("src", "Ingress", {"format": "parquet", "path": in_path}),
                _module(
                    "inc",
                    "Channel",
                    {"op": "sql", "query": "SELECT * FROM src WHERE ts > ${ctx._watermark}"},
                    materialize="incremental",
                    watermark_column="ts",
                ),
                _module(
                    "out", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}
                ),
            ),
            edges=(
                Edge(from_id="src", to_id="inc", port="main"),
                Edge(from_id="inc", to_id="out", port="main"),
            ),
        )

    # First run: no prior watermark -> sentinel -> full scan, all 3 rows.
    result_1 = execute(_make_manifest(), duckdb_con, depot=depot)
    assert result_1.status == "success"
    first_run_rows = duckdb_con.read_parquet(out_path).fetchall()
    assert len(first_run_rows) == 3
    assert depot.get("test_bp:inc:_watermark") == "2024-01-01 12:00:00"

    # Second run: same source file, watermark now advanced -> zero new rows.
    result_2 = execute(_make_manifest(), duckdb_con, depot=depot)
    assert result_2.status == "success"
    second_run_rows = duckdb_con.read_parquet(out_path).fetchall()
    assert len(second_run_rows) == 0
    # Watermark unchanged — MAX() over zero rows is NULL, so the executor
    # must not clobber the persisted value with a failed computation.
    assert depot.get("test_bp:inc:_watermark") == "2024-01-01 12:00:00"
