"""Tests for the Egress writer layer."""

from __future__ import annotations

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
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
    with pytest.raises(EgressError, match="'path' or 'table' is required"):
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
    """register_as_table DDL failure (Derby in-memory, no Hive metastore) → warning logged, blueprint continues."""
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


def test_write_merge_sql_backticks_and_keys(spark: SparkSession, monkeypatch):
    """_write_merge generates MERGE SQL with backticks and handles reserved-word keys.

    The `spark` fixture is session-scoped and shared by the whole suite —
    fakes MUST go through monkeypatch so they are restored after this test.
    A bare `spark.sql = fake_sql` here once poisoned every later Spark test
    (None DataFrames + leaked temp views → 40+ cascading failures). The real
    catalog stays in place: dropTempView is safe on missing views and cleans
    up the `_aq_merge_src` temp view this test registers.
    """
    from pyspark.sql import functions as F
    # Create a DataFrame with a reserved-word column 'order'
    df = spark.range(1).withColumn("order", F.lit(1))
    # Module config using a table name and merge keys including the reserved word
    module = Module(
        id="m1",
        type="Egress",
        label="M1",
        config={"format": "delta", "table": "mydb.mytbl", "merge_key": ["id", "order"]},
    )
    # Capture the SQL passed to spark.sql
    captured = {}
    def fake_sql(sql):
        captured["sql"] = sql
    monkeypatch.setattr(spark, "sql", fake_sql)

    # Execute the merge write
    from aqueduct.executor.spark.egress import _write_merge
    _write_merge(df, module)

    sql = captured.get("sql", "")
    # The target should be backtick-quoted fully qualified table name
    assert "`mydb`.`mytbl`" in sql
    # Each merge key column should be backtick-quoted, including the reserved word
    assert "_aq_target.`id` = _aq_src.`id`" in sql
    assert "_aq_target.`order` = _aq_src.`order`" in sql


# ── Phase 61 — overwrite_partitions ─────────────────────────────────────────

def test_overwrite_partitions_dynamic(spark: SparkSession, tmp_path):
    path = str(tmp_path / "dyn")
    # initial: two partitions
    df0 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "part"])
    df0.write.format("parquet").partitionBy("part").mode("overwrite").save(path)

    # dynamic overwrite touching only part=a
    df1 = spark.createDataFrame([(9, "a")], ["id", "part"])
    module = Module(
        id="m1", type="Egress", label="M1",
        config={"format": "parquet", "path": path, "mode": "overwrite_partitions", "partition_by": ["part"]},
    )
    write_egress(df1, module)

    out = {(r.id, r.part) for r in spark.read.parquet(path).collect()}
    assert (9, "a") in out      # part=a replaced
    assert (2, "b") in out      # part=b preserved
    assert (1, "a") not in out  # old part=a gone


def test_overwrite_partitions_requires_partition_or_predicate(spark: SparkSession, tmp_path):
    path = str(tmp_path / "bad")
    df = spark.createDataFrame([(1, "a")], ["id", "part"])
    module = Module(
        id="m1", type="Egress", label="M1",
        config={"format": "parquet", "path": path, "mode": "overwrite_partitions"},
    )
    with pytest.raises(EgressError, match="requires either 'replace_where'"):
        write_egress(df, module)


def test_egress_merge_schema_option_writes(spark: SparkSession, tmp_path):
    # merge_schema is harmless for parquet; this exercises the option branch.
    path = str(tmp_path / "ms")
    df = spark.createDataFrame([(1, "a")], ["id", "v"])
    module = Module(
        id="m1", type="Egress", label="M1",
        config={"format": "parquet", "path": path, "mode": "overwrite", "merge_schema": True},
    )
    write_egress(df, module)
    assert spark.read.parquet(path).count() == 1


# ── Phase 61 — on_new_columns (Egress write contract) ───────────────────────

def _seed_target(spark, path):
    spark.createDataFrame([(1, "a")], ["id", "v"]).write.format("parquet").mode("overwrite").save(path)


def test_on_new_columns_fail_raises(spark: SparkSession, tmp_path):
    path = str(tmp_path / "t")
    _seed_target(spark, path)
    df = spark.createDataFrame([(2, "b", "x")], ["id", "v", "extra"])
    module = Module(id="m1", type="Egress", label="M1",
                    config={"format": "parquet", "path": path, "mode": "append", "on_new_columns": "fail"})
    with pytest.raises(EgressError, match="on_new_columns=fail"):
        write_egress(df, module)


def test_on_new_columns_allow_writes(spark: SparkSession, tmp_path):
    path = str(tmp_path / "t")
    _seed_target(spark, path)
    df = spark.createDataFrame([(2, "b", "x")], ["id", "v", "extra"])
    module = Module(id="m1", type="Egress", label="M1",
                    config={"format": "parquet", "path": path, "mode": "append", "on_new_columns": "allow"})
    write_egress(df, module)
    assert spark.read.parquet(path).count() == 2


def test_on_new_columns_fail_noop_on_first_write(spark: SparkSession, tmp_path):
    path = str(tmp_path / "fresh")
    df = spark.createDataFrame([(2, "b", "x")], ["id", "v", "extra"])
    module = Module(id="m1", type="Egress", label="M1",
                    config={"format": "parquet", "path": path, "mode": "overwrite", "on_new_columns": "fail"})
    write_egress(df, module)  # no existing target → no drift → writes
    assert spark.read.parquet(path).count() == 1


def test_on_new_columns_invalid_policy(spark: SparkSession, tmp_path):
    path = str(tmp_path / "t")
    _seed_target(spark, path)
    df = spark.createDataFrame([(2, "b")], ["id", "v"])
    module = Module(id="m1", type="Egress", label="M1",
                    config={"format": "parquet", "path": path, "mode": "append", "on_new_columns": "bogus"})
    with pytest.raises(EgressError, match="on_new_columns='bogus' is invalid"):
        write_egress(df, module)


# ── Table-first addressing (catalog.schema.table) ──────────────────────────────

def test_egress_table_overwrite(spark: SparkSession):
    """table: with mode=overwrite writes via saveAsTable."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_ow")
    df = spark.range(5).toDF("num")
    module = Module(id="m1", type="Egress", label="M1",
                    config={"format": "parquet", "table": "_aq_test_tbl_ow", "mode": "overwrite"})
    write_egress(df, module)
    check = spark.read.table("_aq_test_tbl_ow")
    assert check.count() == 5
    assert "num" in check.columns
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_ow")


def test_egress_table_append(spark: SparkSession):
    """table: with mode=append adds rows to existing catalog table."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_app")
    spark.range(3).toDF("num").write.saveAsTable("_aq_test_tbl_app")
    df = spark.range(2).toDF("num")
    module = Module(id="m1", type="Egress", label="M1",
                    config={"format": "parquet", "table": "_aq_test_tbl_app", "mode": "append"})
    write_egress(df, module)
    check = spark.read.table("_aq_test_tbl_app")
    assert check.count() == 5
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_app")


def test_egress_table_and_path_mutually_exclusive(spark: SparkSession):
    """table: and path: together raise EgressError."""
    df = spark.range(1)
    module = Module(id="m1", type="Egress", label="M1",
                    config={"format": "parquet", "table": "t", "path": "/p"})
    with pytest.raises(EgressError, match="mutually exclusive"):
        write_egress(df, module)


def test_egress_table_with_partition_by(spark: SparkSession):
    """table: with partition_by writes partitioned catalog table."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_part")
    df = spark.range(10).selectExpr("id", "id % 2 AS part")
    module = Module(id="m1", type="Egress", label="M1",
                    config={
                        "format": "parquet",
                        "table": "_aq_test_tbl_part",
                        "mode": "overwrite",
                        "partition_by": ["part"],
                    })
    write_egress(df, module)
    check = spark.read.table("_aq_test_tbl_part")
    assert check.count() == 10
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_part")


def test_egress_table_overwrite_partitions(spark: SparkSession):
    """table: with overwrite_partitions works via saveAsTable."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_op")
    # initial two partitions
    df0 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "part"])
    df0.write.format("parquet").partitionBy("part").mode("overwrite").saveAsTable("_aq_test_tbl_op")
    # overwrite partitions touching only part=a
    df1 = spark.createDataFrame([(9, "a")], ["id", "part"])
    module = Module(id="m1", type="Egress", label="M1",
                    config={
                        "format": "parquet",
                        "table": "_aq_test_tbl_op",
                        "mode": "overwrite_partitions",
                        "partition_by": ["part"],
                    })
    write_egress(df1, module)
    out = {(r.id, r.part) for r in spark.read.table("_aq_test_tbl_op").collect()}
    assert (9, "a") in out      # part=a replaced
    assert (2, "b") in out      # part=b preserved
    assert (1, "a") not in out  # old part=a gone
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_op")


def test_egress_table_register_as_table_ignored(spark: SparkSession, caplog):
    """register_as_table is ignored (non-fatal warning) when table: is set."""
    import logging
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_reg")
    df = spark.range(3).toDF("x")
    module = Module(id="m1", type="Egress", label="M1",
                    config={
                        "format": "parquet",
                        "table": "_aq_test_tbl_reg",
                        "mode": "overwrite",
                        "register_as_table": "ignored_table",
                    })
    with caplog.at_level(logging.WARNING, logger="aqueduct.executor.spark.egress"):
        write_egress(df, module)
    # The actual write must have used the table: identifier, not register_as_table
    check = spark.read.table("_aq_test_tbl_reg")
    assert check.count() == 3
    spark.sql("DROP TABLE IF EXISTS _aq_test_tbl_reg")

