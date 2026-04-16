"""Tests for the Egress writer layer."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from aqueduct.executor.egress import EgressError, write_egress
from aqueduct.parser.models import Module


def test_egress_unsupported_format(spark: SparkSession):
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1", config={"format": "ghost", "path": "/foo"})
    with pytest.raises(EgressError, match="unsupported format 'ghost'"):
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
