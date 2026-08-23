"""`aqueduct doctor`'s table-existence and cloud-preflight source checks must
branch on the module's RESOLVED engine, never unconditionally build a
SparkSession (Phase 85 Wave 4 P3/P4).

Prior bug: both checks unconditionally imported ``pyspark`` and built a
SparkSession for ANY module with a ``table:``/cloud-URI config, regardless
of which engine that module actually resolves to. Worst case (P4):
a DuckDB-resolved module got validated against Spark's Hadoop
``engine.spark.conf`` credentials — the wrong engine's credentials
entirely. Both leaves (``tooling.doctor.table_exists``,
``tooling.doctor.cloud_preflight``) are declared ``unsupported`` for DuckDB
with an explicit hint (``aqueduct/executor/duckdb_/capabilities.yml``); the
fix threads the resolved engine through and, for any non-Spark engine,
returns an honest skip citing that DECLARED hint instead of touching Spark
at all. Pure unit tests — no real Spark/DuckDB session needed.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from aqueduct.doctor import _cloud_uri_check, _table_exists_check
from aqueduct.executor.capabilities import Support, get_capabilities
from aqueduct.parser.models import ModuleType

pytestmark = pytest.mark.unit


# ── table-existence ─────────────────────────────────────────────────────────


def test_table_exists_duckdb_engine_skips_with_declared_hint(monkeypatch):
    # Poison the SparkSession constructor doctor would previously reach for
    # unconditionally — a DuckDB-resolved module must never touch it.
    def _boom(*a, **kw):
        raise AssertionError("table-existence check must not build a SparkSession for duckdb")

    monkeypatch.setattr("pyspark.sql.SparkSession.builder.getOrCreate", _boom, raising=False)

    result = _table_exists_check("ingress:t", "db.schema.orders", "duckdb", time.monotonic())

    assert result.status == "skip"
    assert "not implemented for engine 'duckdb'" in result.detail

    hint = get_capabilities("duckdb").verdict("tooling.doctor.table_exists").hint
    assert hint, "tooling.doctor.table_exists must declare a hint when unsupported"
    assert hint in result.detail


def test_table_exists_duckdb_declared_unsupported():
    """Falsifies the fixture assumption above — if DuckDB ever ships a real
    table-existence probe, this test and the doctor branch above must be
    revisited together."""
    leaf = get_capabilities("duckdb").verdict("tooling.doctor.table_exists")
    assert leaf.support == Support.UNSUPPORTED


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
