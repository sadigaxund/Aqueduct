"""`aqueduct doctor`'s table-existence and cloud-preflight source checks must
branch on the module's RESOLVED engine, never unconditionally build a
SparkSession (Phase 85 Wave 4 P3/P4).

Prior bug: both checks unconditionally imported ``pyspark`` and built a
SparkSession for ANY module with a ``table:``/cloud-URI config, regardless
of which engine that module actually resolves to. Worst case (P4):
a DuckDB-resolved module got validated against Spark's Hadoop
``engine.spark.conf`` credentials — the wrong engine's credentials
entirely.

``tooling.doctor.table_exists`` flipped to ``supported`` for DuckDB (Part
B): ``_table_exists_check`` now opens a real, READ-ONLY connection to
``engine.duckdb.database_path`` and queries ``information_schema.tables``.
See ``aqueduct/executor/duckdb_/capabilities.yml``'s hint for the full
catalog-defaulting rule and the ``database_path``-unset skip. Its sibling
leaf, ``tooling.doctor.cloud_preflight``, stays ``unsupported`` — still an
honest skip citing that DECLARED hint instead of touching Spark at all.
Pure unit tests where possible; the real DuckDB probe cases use a real
temporary duckdb database file (duckdb IS installed in this environment) —
no mocking of duckdb itself.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import duckdb
import pytest

from aqueduct.doctor import _cloud_uri_check, _table_exists_check, run_doctor
from aqueduct.executor.capabilities import Support, get_capabilities
from aqueduct.parser.models import ModuleType

pytestmark = pytest.mark.unit


# ── table-existence ─────────────────────────────────────────────────────────


def test_table_exists_duckdb_missing_database_path_skips_and_never_touches_spark(monkeypatch):
    # Poison the SparkSession constructor — a DuckDB-resolved module must
    # never touch it, regardless of database_path configuration.
    def _boom(*a, **kw):
        raise AssertionError("table-existence check must not build a SparkSession for duckdb")

    monkeypatch.setattr("pyspark.sql.SparkSession.builder.getOrCreate", _boom, raising=False)

    # No duckdb_engine_config at all (the default) is the same honest skip
    # as an explicit database_path=":memory:" — nothing to probe ahead of
    # the run's own in-memory session.
    result = _table_exists_check("ingress:t", "db.schema.orders", "duckdb", time.monotonic())

    assert result.status == "skip"
    assert "database_path is unset" in result.detail

    result2 = _table_exists_check(
        "ingress:t",
        "orders",
        "duckdb",
        time.monotonic(),
        duckdb_engine_config={"database_path": ":memory:"},
    )
    assert result2.status == "skip"
    assert "database_path is unset" in result2.detail


def test_table_exists_duckdb_table_present_ok(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE orders (id INTEGER)")
    con.close()

    result = _table_exists_check(
        "ingress:t",
        "orders",
        "duckdb",
        time.monotonic(),
        duckdb_engine_config={"database_path": str(db_path)},
    )
    assert result.status == "ok"
    assert result.detail == "table exists: orders"


def test_table_exists_duckdb_view_present_ok(tmp_path):
    """information_schema.tables (not duckdb_tables()) is used precisely
    because it also lists views — a Blueprint's table: read goes through
    con.table(), which resolves views too."""
    db_path = tmp_path / "proj.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE VIEW orders_v AS SELECT 1 AS id")
    con.close()

    result = _table_exists_check(
        "ingress:t",
        "orders_v",
        "duckdb",
        time.monotonic(),
        duckdb_engine_config={"database_path": str(db_path)},
    )
    assert result.status == "ok"
    assert result.detail == "table exists: orders_v"


def test_table_exists_duckdb_table_absent_fails(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE other (id INTEGER)")
    con.close()

    result = _table_exists_check(
        "ingress:t",
        "orders",
        "duckdb",
        time.monotonic(),
        duckdb_engine_config={"database_path": str(db_path)},
    )
    assert result.status == "fail"
    assert result.detail == "table not found: orders"


def test_table_exists_duckdb_database_file_missing_fails(tmp_path):
    db_path = tmp_path / "does_not_exist.duckdb"

    result = _table_exists_check(
        "ingress:t",
        "orders",
        "duckdb",
        time.monotonic(),
        duckdb_engine_config={"database_path": str(db_path)},
    )
    assert result.status == "fail"
    assert "table not found: orders" in result.detail
    assert not db_path.exists(), "a read-only probe must never create the database file"


def test_table_exists_duckdb_two_part_name_resolves_schema(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA reporting")
    con.execute("CREATE TABLE reporting.daily (id INTEGER)")
    con.close()

    result = _table_exists_check(
        "ingress:t",
        "reporting.daily",
        "duckdb",
        time.monotonic(),
        duckdb_engine_config={"database_path": str(db_path)},
    )
    assert result.status == "ok"
    assert result.detail == "table exists: reporting.daily"

    missing = _table_exists_check(
        "ingress:t",
        "reporting.nope",
        "duckdb",
        time.monotonic(),
        duckdb_engine_config={"database_path": str(db_path)},
    )
    assert missing.status == "fail"


def test_table_exists_duckdb_declared_supported():
    """DuckDB now ships a real table-existence probe — the leaf and this
    doctor branch were revisited together, as the prior version of this
    test required."""
    leaf = get_capabilities("duckdb").verdict("tooling.doctor.table_exists")
    assert leaf.support == Support.SUPPORTED
    assert leaf.hint


def test_table_exists_spark_engine_still_probes_via_spark():
    mock_spark = MagicMock()
    mock_spark.catalog.tableExists.return_value = True
    import sys
    import types

    fake_pyspark_sql = types.ModuleType("pyspark.sql")
    fake_pyspark_sql.SparkSession = MagicMock()
    fake_pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.sql = fake_pyspark_sql

    import pytest as _pytest

    with _pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "pyspark", fake_pyspark)
        mp.setitem(sys.modules, "pyspark.sql", fake_pyspark_sql)
        result = _table_exists_check("ingress:t", "db.schema.orders", "spark", time.monotonic())

    assert result.status == "ok"
    assert "table exists" in result.detail


# ── cloud-object preflight ──────────────────────────────────────────────────


def test_cloud_preflight_duckdb_engine_never_touches_spark(monkeypatch):
    def _boom(*a, **kw):
        raise AssertionError("cloud preflight must not build a SparkSession for duckdb")

    monkeypatch.setattr("pyspark.sql.SparkSession.builder.getOrCreate", _boom, raising=False)

    result = _cloud_uri_check(
        "ingress:src",
        "s3a://bucket/path/to/file.parquet",
        ModuleType.Ingress,
        time.monotonic(),
        preflight=True,
        engine="duckdb",
    )

    assert result.status == "skip"
    assert "not implemented for engine 'duckdb'" in result.detail
    assert "never validated with Spark Hadoop credentials" in result.detail

    hint = get_capabilities("duckdb").verdict("tooling.doctor.cloud_preflight").hint
    assert hint, "tooling.doctor.cloud_preflight must declare a hint when unsupported"
    assert hint in result.detail


def test_cloud_preflight_duckdb_declared_unsupported():
    leaf = get_capabilities("duckdb").verdict("tooling.doctor.cloud_preflight")
    assert leaf.support == Support.UNSUPPORTED


def test_cloud_preflight_defaults_to_spark_engine_when_unspecified():
    """`engine` defaults to "spark" — every existing caller that predates
    this parameter (test_cli_doctor_preflight.py) must keep behaving
    exactly as before."""
    result = _cloud_uri_check(
        "ingress:src",
        "s3a://bucket/path/to/file.parquet",
        ModuleType.Ingress,
        time.monotonic(),
        preflight=False,
    )
    assert result.status == "skip"
    assert "cloud URI" in result.detail


# ── DuckDB session preflight (tooling.doctor.session_preflight) ────────────


def test_run_doctor_skips_duckdb_check_for_spark_engine():
    """The always-appended `duckdb` row must be a `skip` naming the
    resolved engine — not a probe attempt — when the project's
    deployment.engine is not `duckdb` (config_path=None resolves to the
    "spark" default — see aqueduct.config.DeploymentConfig). Mirrors
    test_skip_spark_short_circuits's use of skip_spark=True to avoid a real
    Spark probe."""
    results = run_doctor(config_path=None, skip_spark=True, preflight=False)

    duckdb_res = next(r for r in results if r.name == "duckdb")
    assert duckdb_res.status == "skip"
    assert "deployment.engine=spark" in duckdb_res.detail
    assert "duckdb session check not applicable" in duckdb_res.detail
