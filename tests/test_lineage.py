"""Unit tests for column-level lineage in aqueduct/compiler/lineage.py."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from aqueduct.compiler.lineage import _extract_sql_lineage, write_lineage


# ── helpers ───────────────────────────────────────────────────────────────────


def _mod(mid, mtype, config):
    m = SimpleNamespace()
    m.id = mid
    m.type = mtype
    m.config = config
    return m


def _edge(from_id, to_id, port="main"):
    e = SimpleNamespace()
    e.from_id = from_id
    e.to_id = to_id
    e.port = port
    return e


# ── _extract_sql_lineage ──────────────────────────────────────────────────────


class TestExtractSqlLineage:
    def test_simple_select_two_columns(self):
        rows = _extract_sql_lineage("ch1", "SELECT a, b FROM tbl", ["tbl"])
        out_cols = {r["output_column"] for r in rows}
        assert "a" in out_cols
        assert "b" in out_cols

    def test_source_table_populated(self):
        rows = _extract_sql_lineage("ch1", "SELECT a FROM tbl", ["tbl"])
        assert all(r["source_table"] == "tbl" for r in rows)

    def test_alias_becomes_output_column(self):
        rows = _extract_sql_lineage("ch1", "SELECT a * 2 AS doubled FROM tbl", ["tbl"])
        assert any(r["output_column"] == "doubled" and r["source_column"] == "a" for r in rows)

    def test_star_select_wildcard_row(self):
        rows = _extract_sql_lineage("ch1", "SELECT * FROM tbl", ["tbl"])
        assert len(rows) == 1
        assert rows[0]["output_column"] == "*"
        assert rows[0]["source_column"] == "*"
        assert rows[0]["source_table"] == "tbl"

    def test_invalid_sql_returns_empty_list(self):
        rows = _extract_sql_lineage("ch1", "NOT VALID SQL !!!###", ["tbl"])
        assert rows == []

    def test_single_upstream_source_table_inferred(self):
        rows = _extract_sql_lineage("ch1", "SELECT amount FROM orders", ["orders"])
        assert len(rows) >= 1
        assert all(r["source_table"] == "orders" for r in rows)

    def test_channel_id_in_every_row(self):
        rows = _extract_sql_lineage("my_channel", "SELECT x FROM src", ["src"])
        assert all(r["channel_id"] == "my_channel" for r in rows)


# ── write_lineage ─────────────────────────────────────────────────────────────


class TestWriteLineage:
    def test_creates_lineage_db(self, tmp_path):
        modules = (
            _mod("src", "Ingress", {}),
            _mod("ch1", "Channel", {"op": "sql", "query": "SELECT a FROM src"}),
        )
        edges = (_edge("src", "ch1"),)
        write_lineage("pipe1", "run1", modules, edges, tmp_path)
        assert (tmp_path / "lineage.db").exists()

    def test_inserts_rows_for_channel(self, tmp_path):
        import duckdb

        modules = (
            _mod("src", "Ingress", {}),
            _mod("ch1", "Channel", {"op": "sql", "query": "SELECT x, y FROM src"}),
        )
        edges = (_edge("src", "ch1"),)
        write_lineage("pipe1", "run1", modules, edges, tmp_path)
        conn = duckdb.connect(str(tmp_path / "lineage.db"))
        rows = conn.execute(
            "SELECT output_column FROM column_lineage WHERE channel_id = 'ch1'"
        ).fetchall()
        conn.close()
        out_cols = {r[0] for r in rows}
        assert "x" in out_cols
        assert "y" in out_cols

    def test_non_channel_modules_produce_no_rows(self, tmp_path):
        import duckdb

        modules = (
            _mod("src", "Ingress", {}),
            _mod("sink", "Egress", {}),
        )
        edges = (_edge("src", "sink"),)
        write_lineage("pipe1", "run1", modules, edges, tmp_path)
        db = tmp_path / "lineage.db"
        if db.exists():
            conn = duckdb.connect(str(db))
            count = conn.execute("SELECT COUNT(*) FROM column_lineage").fetchone()[0]
            conn.close()
            assert count == 0

    def test_internal_exception_does_not_propagate(self, tmp_path, monkeypatch):
        def bad_extract(*_args):
            raise RuntimeError("sqlglot exploded")

        monkeypatch.setattr("aqueduct.compiler.lineage._extract_sql_lineage", bad_extract)
        modules = (_mod("ch1", "Channel", {"op": "sql", "query": "SELECT a FROM src"}),)
        edges = (_edge("src", "ch1"),)
        write_lineage("pipe1", "run1", modules, edges, tmp_path)  # must not raise

    def test_duckdb_failure_does_not_propagate(self, tmp_path, monkeypatch):
        import duckdb as _duckdb

        def bad_connect(*_args, **_kw):
            raise RuntimeError("DB unavailable")

        monkeypatch.setattr(_duckdb, "connect", bad_connect)
        modules = (
            _mod("src", "Ingress", {}),
            _mod("ch1", "Channel", {"op": "sql", "query": "SELECT a FROM src"}),
        )
        edges = (_edge("src", "ch1"),)
        write_lineage("pipe1", "run1", modules, edges, tmp_path)  # must not raise
