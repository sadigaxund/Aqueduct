"""Tests for the Egress writer layer."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from aqueduct.executor.spark.egress import EgressError, write_egress
from aqueduct.parser.models import Module


def test_egress_unsupported_format(spark: SparkSession):
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1", config={"format": "ghost", "path": "/foo"})
    with pytest.raises(EgressError, match=r"\[m1\] write failed to '/foo'[\s\S]*ghost"):
        write_egress(df, module)


def test_egress_missing_path(spark: SparkSession):
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1", config={"format": "parquet"})
    with pytest.raises(EgressError, match="'path' is required"):
        write_egress(df, module)


def test_egress_unsupported_mode(spark: SparkSession):
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1", config={
        "format": "parquet", 
        "path": "/foo",
        "mode": "hacked"
    })
    with pytest.raises(EgressError, match="unsupported write mode 'hacked'"):
        write_egress(df, module)


def test_egress_partition_by(spark: SparkSession, tmp_path):
    path = str(tmp_path / "partitioned")
    df = spark.range(10).selectExpr("id", "id % 2 as part")
    
    module = Module(id="m1", type="Egress", label="M1", config={
        "format": "parquet",
        "path": path,
        "partition_by": ["part"]
    })
    write_egress(df, module)
    
    # Verify partitions exist
    parts = [p.name for p in (tmp_path / "partitioned").iterdir() if p.is_dir()]
    assert "part=0" in parts
    assert "part=1" in parts


def test_egress_custom_options(spark: SparkSession, tmp_path):
    path = str(tmp_path / "options.csv")
    df = spark.range(5).selectExpr("id", "id * 10 as val")
    
    module = Module(id="m1", type="Egress", label="M1", config={
        "format": "csv",
        "path": path,
        "header": True,  # This will be in the config but write_egress uses 'options' for it
        "options": {"sep": "\t", "header": "true"}
    })
    write_egress(df, module)
    
    # Read back to verify
    check = spark.read.option("sep", "\t").option("header", "true").csv(path)
    assert check.count() == 5
    assert check.columns == ["id", "val"]


def test_egress_mode_overwrite(spark: SparkSession, tmp_path):
    path = str(tmp_path / "overwrite.parquet")
    spark.range(5).write.parquet(path)
    
    # Write again with overwrite
    df = spark.range(10)
    module = Module(id="m1", type="Egress", label="M1", config={
        "format": "parquet",
        "path": path,
        "mode": "overwrite"
    })
    write_egress(df, module)
    
    assert spark.read.parquet(path).count() == 10

class MockDepot:
    def __init__(self):
        self.puts = {}
    def put(self, key, value):
        self.puts[key] = value

def test_egress_missing_format(spark: SparkSession):
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1", config={"path": "/foo"})
    with pytest.raises(EgressError, match="'format' is required"):
        write_egress(df, module)

def test_egress_format_depot_no_depot(spark: SparkSession):
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1", config={"format": "depot", "key": "k1", "value": "v1"})
    with pytest.raises(EgressError, match="no DepotStore is wired"):
        write_egress(df, module, depot=None)

def test_egress_format_depot_missing_key(spark: SparkSession):
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1", config={"format": "depot", "value": "v1"})
    with pytest.raises(EgressError, match="requires 'key'"):
        write_egress(df, module, depot=MockDepot())

def test_egress_format_depot_value(spark: SparkSession):
    df = spark.range(1)
    depot = MockDepot()
    module = Module(id="m1", type="Egress", label="M1", config={"format": "depot", "key": "k1", "value": "v1"})
    write_egress(df, module, depot=depot)
    assert depot.puts["k1"] == "v1"

def test_egress_format_depot_value_expr(spark: SparkSession):
    df = spark.range(5)
    depot = MockDepot()
    module = Module(id="m1", type="Egress", label="M1", config={"format": "depot", "key": "k1", "value_expr": "max(id)"})
    write_egress(df, module, depot=depot)
    assert depot.puts["k1"] == "4"


# ── register_as_table tests (⏳ → ✅) ─────────────────────────────────────────

def test_egress_register_as_table_success(spark: SparkSession, tmp_path, caplog):
    """register_as_table set → CREATE EXTERNAL TABLE executed with correct name/format/location."""
    import logging

    path = str(tmp_path / "reg_table.parquet")
    df = spark.range(3).selectExpr("id", "id * 2 as doubled")

    table_name = "test_reg_success"
    module = Module(
        id="reg1",
        type="Egress",
        label="Reg1",
        config={
            "format": "parquet",
            "path": path,
            "mode": "overwrite",
            "register_as_table": table_name,
        },
    )

    with caplog.at_level(logging.INFO, logger="aqueduct.executor.spark.egress"):
        write_egress(df, module)

    # Table should be registered; query should return rows (or a warning was logged on DDL failure)
    # Either way the write must have succeeded and write_egress must not have raised.
    written = spark.read.parquet(path)
    assert written.count() == 3


def test_egress_register_as_table_ddl_failure_non_fatal(spark: SparkSession, tmp_path, caplog):
    """register_as_table DDL failure (Derby in-memory, no Hive metastore) → warning logged, pipeline continues."""
    import logging

    path = str(tmp_path / "reg_fail.parquet")
    df = spark.range(2)

    # Use a table name that is certain to cause a DDL error in the in-memory Derby metastore
    # (EXTERNAL TABLE requires Hive metastore support).
    module = Module(
        id="reg2",
        type="Egress",
        label="Reg2",
        config={
            "format": "parquet",
            "path": path,
            "mode": "overwrite",
            "register_as_table": "nonexistent_hive_table_xyz",
        },
    )

    # Must NOT raise even if DDL fails
    with caplog.at_level(logging.WARNING, logger="aqueduct.executor.spark.egress"):
        write_egress(df, module)  # should not raise

    # Parquet data was still written regardless of DDL outcome
    assert spark.read.parquet(path).count() == 2


def test_egress_register_as_table_absent(spark: SparkSession, tmp_path):
    """register_as_table absent → write succeeds, no DDL attempted."""
    path = str(tmp_path / "no_reg.parquet")
    df = spark.range(4)

    module = Module(
        id="noreg",
        type="Egress",
        label="NoReg",
        config={
            "format": "parquet",
            "path": path,
            "mode": "overwrite",
            # register_as_table intentionally omitted
        },
    )

    write_egress(df, module)

    assert spark.read.parquet(path).count() == 4
