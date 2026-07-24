"""Tests for the Ingress reader layer."""

from __future__ import annotations

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
from pyspark.sql import SparkSession
from aqueduct.executor.spark.ingress import IngressError, read_ingress
from aqueduct.parser.models import Module


def test_ingress_unsupported_format(spark: SparkSession):
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "ghost", "path": "/foo"})
    with pytest.raises(IngressError, match=r".*ghost.*"):
        read_ingress(module, spark)


def test_ingress_missing_path_parquet(spark: SparkSession):
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "parquet"})
    with pytest.raises(IngressError, match="'path' is required in Ingress config for format='parquet'"):
        read_ingress(module, spark)


def test_ingress_missing_path_csv(spark: SparkSession):
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "csv"})
    with pytest.raises(IngressError, match="'path' is required in Ingress config for format='csv'"):
        read_ingress(module, spark)


def test_ingress_pathless_formats_no_error(spark: SparkSession):
    for fmt in ["jdbc", "kafka", "depot"]:
        module = Module(id="m1", type="Ingress", label="M1", config={"format": fmt, "options": {"dbtable": "t"}})
        try:
            read_ingress(module, spark)
        except IngressError as exc:
            assert "'path' is required" not in str(exc)
        except Exception:
            pass


def test_ingress_jdbc_with_path(spark: SparkSession, tmp_path):
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "jdbc", "path": "jdbc:sqlite::memory:", "options": {"dbtable": "t"}})
    try:
        read_ingress(module, spark)
    except IngressError as exc:
        assert "'path' is required" not in str(exc)
    except Exception:
        pass


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


def test_ingress_missing_format(spark: SparkSession):
    module = Module(id="m1", type="Ingress", label="M1", config={"path": "/foo"})
    with pytest.raises(IngressError, match="'format' is required"):
        read_ingress(module, spark)


def test_ingress_format_custom_real_read(spark: SparkSession, tmp_path):
    """format: custom + class: imports + registers a real Spark 4.0+ Python
    DataSource, then reads through it by its own name(). This is a genuine
    round trip through spark.read.format(name).load() — schema and column
    metadata come straight from the live registered reader (metadata access
    on a custom-DataSource DataFrame is not a Spark action, so it needs no
    pyarrow in this environment)."""
    pytest.importorskip("pyspark.sql.datasource", reason="requires Spark 4.0+")

    (tmp_path / "custom_ingress_ds.py").write_text(
        "from pyspark.sql.datasource import DataSource, DataSourceReader\n\n"
        "class _Reader(DataSourceReader):\n"
        "    def read(self, partition):\n"
        "        yield (1, 'a')\n"
        "        yield (2, 'b')\n\n"
        "class CustomIngressDS(DataSource):\n"
        "    @classmethod\n"
        "    def name(cls):\n"
        "        return 'aq_test_custom_ingress'\n"
        "    def schema(self):\n"
        "        return 'id int, val string'\n"
        "    def reader(self, schema):\n"
        "        return _Reader()\n"
    )
    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "custom",
        "class": "custom_ingress_ds.CustomIngressDS",
    })
    df = read_ingress(module, spark, base_dir=str(tmp_path))
    assert df.columns == ["id", "val"]
    assert [f.dataType.simpleString() for f in df.schema.fields] == ["int", "string"]


# ── schema_hint flat dict form ────────────────────────────────────────────────

def test_schema_hint_flat_dict_pass(spark: SparkSession, tmp_path):
    """flat dict {col_name: type} → treated as strict schema check; matching → passes."""
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id AS col1").write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": {"col1": "bigint"},  # flat dict
    })
    df = read_ingress(module, spark)
    assert "col1" in df.columns


def test_schema_hint_flat_dict_wrong_type_raises(spark: SparkSession, tmp_path):
    """flat dict with wrong type → IngressError with column name and mismatch detail."""
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id AS col1").write.parquet(path)  # col1 is bigint

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": {"col1": "string"},  # wrong type
    })
    with pytest.raises(IngressError, match="type mismatch on 'col1'"):
        read_ingress(module, spark)


def test_schema_hint_flat_dict_missing_column_raises(spark: SparkSession, tmp_path):
    """flat dict with missing column → IngressError raised."""
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id AS col1").write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": {"ghost_col": "bigint"},  # non-existent
    })
    with pytest.raises(IngressError, match="field 'ghost_col' not found"):
        read_ingress(module, spark)


def test_schema_hint_nested_dict_still_works(spark: SparkSession, tmp_path):
    """nested dict {mode, columns} → still works correctly in additive mode."""
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id", "id * 2 AS extra").write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": {"mode": "additive", "columns": [{"name": "id", "type": "bigint"}]},
    })
    df = read_ingress(module, spark)
    assert "id" in df.columns
    assert "extra" in df.columns  # additive: extra columns allowed


def test_schema_hint_list_form_still_works(spark: SparkSession, tmp_path):
    """list form [{name, type}] → still works correctly."""
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id").write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "id", "type": "bigint"}],
    })
    df = read_ingress(module, spark)
    assert "id" in df.columns


# ── type alias normalization ──────────────────────────────────────────────────

def test_type_alias_long_accepted_as_bigint(spark: SparkSession, tmp_path):
    """LONG accepted as bigint."""
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id").write.parquet(path)  # id is bigint

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "id", "type": "LONG"}],
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["id"] == "bigint"


def test_type_alias_integer_accepted_as_int(spark: SparkSession, tmp_path):
    """INTEGER accepted as int."""
    path = str(tmp_path / "data.parquet")
    df_src = spark.createDataFrame([(1,), (2,)], ["n"])
    df_cast = df_src.selectExpr("CAST(n AS INT) AS n")
    df_cast.write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "n", "type": "INTEGER"}],
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["n"] == "int"


def test_type_alias_bool_accepted_as_boolean(spark: SparkSession, tmp_path):
    """BOOL accepted as boolean."""
    path = str(tmp_path / "data.parquet")
    df_src = spark.createDataFrame([(True,), (False,)], ["flag"])
    df_src.write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "flag", "type": "BOOL"}],
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["flag"] == "boolean"


def test_type_alias_short_accepted_as_smallint(spark: SparkSession, tmp_path):
    """SHORT accepted as smallint."""
    path = str(tmp_path / "data.parquet")
    df_src = spark.createDataFrame([(1,)], ["n"])
    df_cast = df_src.selectExpr("CAST(n AS SMALLINT) AS n")
    df_cast.write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "n", "type": "SHORT"}],
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["n"] == "smallint"


def test_type_alias_mixed_case_normalized(spark: SparkSession, tmp_path):
    """mixed case alias Long/STRING normalized correctly."""
    path = str(tmp_path / "data.parquet")
    spark.range(5).selectExpr("id").write.parquet(path)  # bigint

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "id", "type": "Long"}],  # mixed case
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["id"] == "bigint"


def test_type_not_in_alias_map_lowercased(spark: SparkSession, tmp_path):
    """types not in alias map lowercased verbatim: DOUBLE → double."""
    path = str(tmp_path / "data.parquet")
    df_src = spark.createDataFrame([(1.5,)], ["v"])
    df_cast = df_src.selectExpr("CAST(v AS DOUBLE) AS v")
    df_cast.write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "v", "type": "DOUBLE"}],
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["v"] == "double"


# ── Phase 80 work package 3: schema_hint now understands the hub vocabulary ─
#
# The old ``_TYPE_ALIASES`` (deleted, 5-entry dict: long/integer/bool/short/
# byte) never mapped ``timestamp_tz``/``timestamp_ntz`` at all — a
# ``timestamp_tz`` hint used to compare literally against Spark's
# ``simpleString()`` (which is ``"timestamp"``, never "timestamp_tz") and
# always mismatch. It now renders through the hub
# (``aqueduct.executor.spark.type_render``) to Spark's real DDL spelling
# before comparison.
def test_schema_hint_hub_timestamp_tz_matches_spark_instant_type(spark: SparkSession, tmp_path):
    path = str(tmp_path / "data.parquet")
    df_src = spark.createDataFrame([("2020-01-01",)], ["ts"])
    df_cast = df_src.selectExpr("CAST(ts AS TIMESTAMP) AS ts")  # Spark's instant type
    df_cast.write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "ts", "type": "timestamp_tz"}],
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["ts"] == "timestamp"


def test_schema_hint_hub_array_matches(spark: SparkSession, tmp_path):
    path = str(tmp_path / "data.parquet")
    df_src = spark.range(1).selectExpr("array(1, 2, 3) AS arr")
    df_src.write.parquet(path)

    module = Module(id="m1", type="Ingress", label="M1", config={
        "format": "parquet",
        "path": path,
        "schema_hint": [{"name": "arr", "type": "array<int>"}],
    })
    df = read_ingress(module, spark)
    assert dict(df.dtypes)["arr"] == "array<int>"


# ── partition_filters ─────────────────────────────────────────────────────────

def test_partition_filters_applied_as_where(spark: SparkSession, tmp_path):
    """partition_filters set → .where(expr) applied; returned df is filtered."""
    path = str(tmp_path / "pf_data.parquet")
    spark.range(10).write.parquet(path)
    module = Module(
        id="m1", type="Ingress", label="M1",
        config={"format": "parquet", "path": path, "partition_filters": "id >= 5"}
    )
    df = read_ingress(module, spark)
    assert df.count() == 5


def test_partition_filters_absent_no_where(spark: SparkSession, tmp_path):
    """partition_filters absent → no .where() call; df unchanged."""
    path = str(tmp_path / "pf_all.parquet")
    spark.range(10).write.parquet(path)
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "parquet", "path": path})
    df = read_ingress(module, spark)
    assert df.count() == 10


def test_partition_filters_invalid_sql_raises(spark: SparkSession, tmp_path):
    """partition_filters with invalid SQL expr → IngressError with filter in message."""
    path = str(tmp_path / "pf_bad.parquet")
    spark.range(5).write.parquet(path)
    bad_filter = "id === INVALID_SYNTAX %%%"
    module = Module(
        id="m1", type="Ingress", label="M1",
        config={"format": "parquet", "path": path, "partition_filters": bad_filter}
    )
    with pytest.raises(IngressError, match=r"partition_filters"):
        read_ingress(module, spark)


def test_partition_filters_applied_before_schema_hint(spark: SparkSession, tmp_path):
    """partition_filters applied; schema_hint check still works on filtered result."""
    path = str(tmp_path / "pf_schema.parquet")
    spark.range(10).write.parquet(path)
    module = Module(
        id="m1", type="Ingress", label="M1",
        config={
            "format": "parquet",
            "path": path,
            "partition_filters": "id < 5",
            "schema_hint": {"id": "bigint"},
        }
    )
    df = read_ingress(module, spark)
    assert df.count() == 5
    assert "id" in df.columns


def test_partition_filters_with_date_comparison(spark: SparkSession, tmp_path):
    """partition_filters with date literal expr → rows outside range excluded."""
    from pyspark.sql import Row
    from datetime import date

    path = str(tmp_path / "pf_date.parquet")
    spark.createDataFrame([
        Row(id=1, event_date=date(2024, 1, 1)),
        Row(id=2, event_date=date(2024, 6, 15)),
        Row(id=3, event_date=date(2023, 12, 31)),
    ]).write.parquet(path)

    module = Module(
        id="m1", type="Ingress", label="M1",
        config={
            "format": "parquet",
            "path": path,
            "partition_filters": "event_date >= '2024-01-01'",
        }
    )
    df = read_ingress(module, spark)
    ids = sorted(r["id"] for r in df.collect())
    assert ids == [1, 2]



def test_partition_filters_jdbc_applied_after_pathless_load(spark: SparkSession):
    """partition_filters on JDBC (no path) -> .where() applied after reader.load()."""
    from unittest.mock import MagicMock

    # Real df to be "returned" by the mocked JDBC reader
    real_df = spark.range(10)

    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = real_df

    module = Module(
        id="jdbc_in", type="Ingress", label="JDBC",
        config={
            "format": "jdbc",
            "options": {
                "url": "jdbc:postgresql://localhost/db",
                "dbtable": "users",
            },
            "partition_filters": "id >= 5",
        }
    )

    mock_spark = MagicMock()
    mock_spark.read = mock_reader
    df = read_ingress(module, mock_spark)

    # load() called without a path argument (JDBC is pathless)
    mock_reader.load.assert_called_once_with()
    # filter was applied - only rows with id >= 5 survive
    assert df.count() == 5


def test_pathless_ingress_formats_are_expected_set():
    """Verify PATHLESS_INGRESS_FORMATS matches the known pathless Ingress
    formats.  The compiler uses the same constant for inputs_fingerprint
    skip logic — a drift between the two sources would cause the compiler
    to stat() a nonexistent path or the executor to reject a valid format.
    """
    from aqueduct.executor.path_keys import PATHLESS_INGRESS_FORMATS

    assert PATHLESS_INGRESS_FORMATS == {"jdbc", "kafka", "depot"}


# ── Phase 61 — time-travel reads ────────────────────────────────────────────

from unittest.mock import MagicMock
from aqueduct.executor.spark.ingress import _apply_time_travel


def test_time_travel_version_sets_option():
    reader = MagicMock()
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "delta", "path": "/t", "time_travel": {"version": 12}})
    _apply_time_travel(module, reader)
    reader.option.assert_called_once_with("versionAsOf", 12)


def test_time_travel_timestamp_sets_option():
    reader = MagicMock()
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "delta", "path": "/t", "time_travel": {"timestamp": "2026-01-01"}})
    _apply_time_travel(module, reader)
    reader.option.assert_called_once_with("timestampAsOf", "2026-01-01")


def test_time_travel_absent_is_noop():
    reader = MagicMock()
    module = Module(id="m1", type="Ingress", label="M1", config={"format": "delta", "path": "/t"})
    assert _apply_time_travel(module, reader) is reader
    reader.option.assert_not_called()


def test_time_travel_both_raises():
    reader = MagicMock()
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "delta", "path": "/t",
                            "time_travel": {"version": 1, "timestamp": "x"}})
    with pytest.raises(IngressError, match="not both"):
        _apply_time_travel(module, reader)


def test_time_travel_empty_raises():
    reader = MagicMock()
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "delta", "path": "/t", "time_travel": {}})
    with pytest.raises(IngressError, match="requires 'version' or 'timestamp'"):
        _apply_time_travel(module, reader)


# ── Phase 61 — on_new_columns (Ingress source contract) ─────────────────────

from aqueduct.executor.spark.ingress import _enforce_on_new_columns


def test_ingress_on_new_columns_fail(spark: SparkSession):
    df = spark.createDataFrame([(1, "x")], ["id", "extra"])
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "parquet", "path": "/t", "on_new_columns": "fail",
                            "known_columns": ["id"]})
    with pytest.raises(IngressError, match="undeclared column"):
        _enforce_on_new_columns(module, df, None)


def test_ingress_on_new_columns_alert_ok(spark: SparkSession):
    df = spark.createDataFrame([(1, "x")], ["id", "extra"])
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "parquet", "path": "/t", "on_new_columns": "alert",
                            "known_columns": ["id"]})
    _enforce_on_new_columns(module, df, None)  # warns, does not raise
    assert df.columns == ["id", "extra"]


def test_ingress_on_new_columns_baseline_from_schema_hint(spark: SparkSession):
    df = spark.createDataFrame([(1, "x")], ["id", "extra"])
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "parquet", "path": "/t", "on_new_columns": "fail"})
    with pytest.raises(IngressError, match="undeclared column"):
        _enforce_on_new_columns(module, df, [{"name": "id", "type": "int"}])


def test_ingress_on_new_columns_no_baseline_skips(spark: SparkSession):
    df = spark.createDataFrame([(1, "x")], ["id", "extra"])
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"format": "parquet", "path": "/t", "on_new_columns": "fail"})
    _enforce_on_new_columns(module, df, None)  # no baseline → skip, no raise


# ── Table-first addressing (catalog.schema.table) ──────────────────────────────

def test_ingress_table_read_ok(spark: SparkSession):
    """table: reads from local session catalog."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_table_read")
    spark.range(5).toDF("num").write.saveAsTable("_aq_test_table_read")
    module = Module(id="m1", type="Ingress", label="M1", config={"table": "_aq_test_table_read"})
    df = read_ingress(module, spark)
    assert df.count() == 5
    assert "num" in df.columns
    spark.sql("DROP TABLE IF EXISTS _aq_test_table_read")


def test_ingress_table_missing_raises(spark: SparkSession):
    """table: with non-existent table raises IngressError."""
    module = Module(id="m1", type="Ingress", label="M1", config={"table": "_aq_nonexistent_xyz"})
    with pytest.raises(IngressError, match="not found or unreadable"):
        read_ingress(module, spark)


def test_ingress_table_and_path_mutually_exclusive(spark: SparkSession):
    """table: and path: together raise IngressError."""
    module = Module(id="m1", type="Ingress", label="M1",
                    config={"table": "t", "path": "/p", "format": "parquet"})
    with pytest.raises(IngressError, match="mutually exclusive"):
        read_ingress(module, spark)


def test_ingress_table_with_schema_hint(spark: SparkSession):
    """table: with schema_hint — validates columns against catalog table."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_schema_hint")
    spark.range(5).toDF("num").write.saveAsTable("_aq_test_schema_hint")
    module = Module(id="m1", type="Ingress", label="M1", config={
        "table": "_aq_test_schema_hint",
        "schema_hint": [{"name": "num", "type": "bigint"}],
    })
    df = read_ingress(module, spark)
    assert df.count() == 5
    spark.sql("DROP TABLE IF EXISTS _aq_test_schema_hint")


def test_ingress_table_with_partition_filters(spark: SparkSession):
    """table: with partition_filters — predicate is applied post-read."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_part_filter")
    spark.range(10).toDF("num").write.saveAsTable("_aq_test_part_filter")
    module = Module(id="m1", type="Ingress", label="M1", config={
        "table": "_aq_test_part_filter",
        "partition_filters": "num >= 5",
    })
    df = read_ingress(module, spark)
    assert df.count() == 5
    spark.sql("DROP TABLE IF EXISTS _aq_test_part_filter")


def test_ingress_table_requires_no_format(spark: SparkSession):
    """table: works without format: key."""
    spark.sql("DROP TABLE IF EXISTS _aq_test_no_fmt")
    spark.range(3).toDF("x").write.saveAsTable("_aq_test_no_fmt")
    module = Module(id="m1", type="Ingress", label="M1", config={"table": "_aq_test_no_fmt"})
    df = read_ingress(module, spark)
    assert df.count() == 3
    spark.sql("DROP TABLE IF EXISTS _aq_test_no_fmt")
