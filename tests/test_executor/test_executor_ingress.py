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
    for fmt in ["jdbc", "kafka", "depot", "dataframe"]:
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
