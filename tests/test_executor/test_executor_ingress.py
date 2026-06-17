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
