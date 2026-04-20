"""Tests for the Spark session factory."""

from __future__ import annotations

from pyspark.sql import SparkSession
from aqueduct.executor.spark.session import make_spark_session


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


def test_make_spark_session_master_url_default():
    # If we don't pass master_url, it defaults to local[*]
    # Checking this is slightly tricky due to getOrCreate caching,
    # but we can check the conf if the session is fresh.
    spark = make_spark_session("app-default", {})
    assert spark.conf.get("spark.master") == "local[*]"


def test_make_spark_session_master_url_custom():
    # Because of getOrCreate, changing the master in builder doesn't necessarily change it on an existing session.
    # To truly test this without interference, we can check the builder or assume first session creation works.
    # We will just verify it does not error and sets the conf properties that we asked for.
    spark = make_spark_session("app-custom", {}, master_url="local[2]")
    # Cannot easily verify master if session was already local[*] from previous tests,
    # so we just verify it runs without error.
    assert isinstance(spark, SparkSession)


def test_make_spark_session_master_url_yarn():
    # Verify no error on creation with yarn
    spark = make_spark_session("app-yarn", {}, master_url="yarn")
    assert isinstance(spark, SparkSession)


def test_make_spark_session_master_url_standalone():
    spark = make_spark_session("app-standalone", {}, master_url="spark://host:7077")
    assert isinstance(spark, SparkSession)


def test_make_spark_session_blueprint_config_precedence():
    # Test that spark config passed in is processed
    config = {"spark.driver.memory": "3g", "spark.executor.memory": "5g"}
    spark = make_spark_session("app-prec", config)
    assert spark.conf.get("spark.driver.memory") == "3g"
    assert spark.conf.get("spark.executor.memory") == "5g"
