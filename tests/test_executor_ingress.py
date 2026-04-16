"""Tests for the Ingress reader layer."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from aqueduct.executor.ingress import IngressError, read_ingress
from aqueduct.parser.models import Module


def test_ingress_unsupported_format(spark: SparkSession):
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "ghost", "path": "/foo"})
    with pytest.raises(IngressError, match="unsupported format 'ghost'"):
        read_ingress(module, spark)


def test_ingress_missing_path(spark: SparkSession):
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "parquet"})
    with pytest.raises(IngressError, match="'path' is required"):
        read_ingress(module, spark)


def test_ingress_schema_hint_missing_col(spark: SparkSession, tmp_path):
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id AS col1").write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "ghost"}]
    })
    with pytest.raises(IngressError, match="field 'ghost' not found"):
        read_ingress(module, spark)


def test_ingress_schema_hint_wrong_type(spark: SparkSession, tmp_path):
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id AS col1").write.parquet(path)  # col1 is bigint

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "col1", "type": "string"}]
    })
    with pytest.raises(IngressError, match="type mismatch on 'col1': expected 'string', actual 'bigint'"):
        read_ingress(module, spark)


def test_ingress_valid_parquet(spark: SparkSession, tmp_path):
    path = str(tmp_path / "valid.parquet")
    spark.range(10).write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={"format": "parquet", "path": path})
    df = read_ingress(module, spark)
    
    assert df.count() == 10
    assert "id" in df.columns


def test_ingress_csv_defaults(spark: SparkSession, tmp_path):
    path = str(tmp_path / "valid.csv")
    content = "col1,col2\n1,a\n2,b"
    with open(path, "w") as f:
        f.write(content)

    module = Module(id="m1", type="Ingress", label="M1", config={"format": "csv", "path": path})
    df = read_ingress(module, spark)
    
    # Defaults: header=True, inferSchema=True
    assert df.columns == ["col1", "col2"]
    assert df.dtypes == [("col1", "int"), ("col2", "string")]


def test_ingress_custom_options(spark: SparkSession, tmp_path):
    path = str(tmp_path / "custom.csv")
    content = "1|a\n2|b"
    with open(path, "w") as f:
        f.write(content)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "csv",
        "path": path,
        "header": False,
        "options": {"sep": "|"}
    })
    df = read_ingress(module, spark)
    
    assert df.columns == ["_c0", "_c1"]
    assert df.collect()[0][0] == 1
