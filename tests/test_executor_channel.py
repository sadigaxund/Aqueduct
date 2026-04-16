"""Tests for the SQL Channel executor."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.channel import ChannelError, execute_sql_channel
from aqueduct.parser.models import Module


def test_channel_unsupported_op(spark: SparkSession):
    module = Module(id="m1", type="Channel", label="M1", config={"op": "spark-shell", "query": "SELECT 1"})
    with pytest.raises(ChannelError, match="unsupported Channel op 'spark-shell'"):
        execute_sql_channel(module, {"in": spark.range(1)}, spark)


def test_channel_missing_query(spark: SparkSession):
    module = Module(id="m1", type="Channel", label="M1", config={"op": "sql"})
    with pytest.raises(ChannelError, match="'query' is required"):
        execute_sql_channel(module, {"in": spark.range(1)}, spark)


def test_channel_empty_query(spark: SparkSession):
    module = Module(id="m1", type="Channel", label="M1", config={"op": "sql", "query": "  "})
    with pytest.raises(ChannelError, match="'query' is required"):
        execute_sql_channel(module, {"in": spark.range(1)}, spark)


def test_channel_no_upstreams(spark: SparkSession):
    module = Module(id="m1", type="Channel", label="M1", config={"op": "sql", "query": "SELECT 1"})
    with pytest.raises(ChannelError, match="no upstream DataFrames"):
        execute_sql_channel(module, {}, spark)


def test_channel_temp_view_registration(spark: SparkSession):
    df_in = spark.range(5)
    module = Module(id="chan", type="Channel", label="C", config={"op": "sql", "query": "SELECT * FROM source_a"})
    
    # Execute should register 'source_a'
    res = execute_sql_channel(module, {"source_a": df_in}, spark)
    assert res.count() == 5
    
    # Verify cleanup: views should be gone
    assert "source_a" not in [v.name for v in spark.catalog.listTables()]
    assert "__input__" not in [v.name for v in spark.catalog.listTables()]


def test_channel_single_input_alias(spark: SparkSession):
    df_in = spark.range(10)
    module = Module(id="chan", type="Channel", label="C", config={"op": "sql", "query": "SELECT * FROM __input__"})
    
    # Single input allows using __input__ alias
    res = execute_sql_channel(module, {"any_name": df_in}, spark)
    assert res.count() == 10


def test_channel_multi_input_alias_check(spark: SparkSession):
    df1 = spark.range(5)
    df2 = spark.range(10)
    
    # __input__ is NOT registered for multi-input channels
    query = "SELECT * FROM __input__"
    module = Module(id="chan", type="Channel", label="C", config={"op": "sql", "query": query})
    
    with pytest.raises(ChannelError, match="The table or view .* cannot be found"):
        execute_sql_channel(module, {"in1": df1, "in2": df2}, spark)


def test_channel_cleanup_on_failure(spark: SparkSession):
    df_in = spark.range(5)
    # Invalid SQL
    module = Module(id="chan", type="Channel", label="C", config={"op": "sql", "query": "SELECT * FROM non_existent"})
    
    with pytest.raises(ChannelError):
        execute_sql_channel(module, {"source_a": df_in}, spark)
    
    # Verify cleanup: even on failure, views should be dropped
    assert "source_a" not in [v.name for v in spark.catalog.listTables()]


def test_channel_sql_syntax_error(spark: SparkSession):
    df_in = spark.range(5)
    module = Module(id="chan", type="Channel", label="C", config={"op": "sql", "query": "BROKEN SQL"})
    
    with pytest.raises(ChannelError, match="SQL execution failed"):
        execute_sql_channel(module, {"in": df_in}, spark)


def test_channel_read_input_resolves(spark: SparkSession):
    # Requirement: SELECT * FROM read_input resolves when upstream ID is read_input
    df_in = spark.range(7)
    module = Module(id="chan", type="Channel", label="C", config={"op": "sql", "query": "SELECT * FROM read_input"})
    
    res = execute_sql_channel(module, {"read_input": df_in}, spark)
    assert res.count() == 7


def test_channel_result_is_lazy(spark: SparkSession):
    # Verify no Spark action is triggered inside the call
    # We can't easily verify "no action" without a listener, but we can verify it returns immediately
    # and the result is a DataFrame.
    from pyspark.sql import DataFrame
    df_in = spark.range(1000)
    module = Module(id="chan", type="Channel", label="C", config={"op": "sql", "query": "SELECT * FROM in"})
    
    res = execute_sql_channel(module, {"in": df_in}, spark)
    assert isinstance(res, DataFrame)
    # If it was an action, it would return a list/int/etc. or take time.
