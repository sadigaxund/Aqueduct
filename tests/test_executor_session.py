"""Tests for the Spark session factory."""

from __future__ import annotations

from pyspark.sql import SparkSession
from aqueduct.executor.session import make_spark_session


def test_make_spark_session_returns_active_session():
    spark = make_spark_session("test-app", {})
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("spark.app.name") == "test-app"


def test_spark_config_applied_to_conf():
    config = {"spark.sql.shuffle.partitions": "7", "spark.aqueduct.test": "true"}
    spark = make_spark_session("test-cfg", config)
    assert spark.conf.get("spark.sql.shuffle.partitions") == "7"
    assert spark.conf.get("spark.aqueduct.test") == "true"


def test_get_or_create_semantics():
    spark1 = make_spark_session("app1", {})
    spark2 = make_spark_session("app1", {})
    assert spark1 is spark2
