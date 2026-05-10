"""Tests for the Executor layer: UDF registration and execution."""

from __future__ import annotations
import json
from unittest.mock import MagicMock, patch
import pytest

class TestUdfRegistration:
    def test_unsupported_lang_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="language 'fortran' is not supported"):
            register_udfs(({"id": "my_udf", "lang": "fortran"},), mock_spark)

    def test_java_scala_missing_jar_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="'jar' is required"):
            register_udfs(({"id": "my_udf", "lang": "java"},), mock_spark)
        with pytest.raises(UDFError, match="'jar' is required"):
            register_udfs(({"id": "my_udf", "lang": "scala"},), mock_spark)

    def test_missing_module_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="'module' is required"):
            register_udfs(({"id": "my_udf", "lang": "python"},), mock_spark)

    def test_nonexistent_module_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="cannot import module"):
            register_udfs(({"id": "my_udf", "lang": "python", "module": "no.such.module"},), mock_spark)

    def test_missing_entry_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="function 'nonexistent_fn' not found"):
            register_udfs(
                ({"id": "my_udf", "lang": "python", "module": "json", "entry": "nonexistent_fn"},),
                mock_spark,
            )

    def test_successful_registration(self):
        from aqueduct.executor.spark.udf import register_udfs

        mock_spark = MagicMock()
        # json.loads is a real importable callable
        register_udfs(
            ({"id": "parse_json", "lang": "python", "module": "json", "entry": "loads", "return_type": "string"},),
            mock_spark,
        )
        mock_spark.udf.register.assert_called_once_with("parse_json", json.loads, "string")

    def test_entry_defaults_to_udf_id(self):
        from aqueduct.executor.spark.udf import register_udfs

        mock_spark = MagicMock()
        # "json" module has "loads" — use id="loads" so entry defaults to "loads"
        register_udfs(
            ({"id": "loads", "lang": "python", "module": "json", "return_type": "string"},),
            mock_spark,
        )
        mock_spark.udf.register.assert_called_once()

    def test_spark_register_failure_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        mock_spark.udf.register.side_effect = Exception("Spark refused")
        with pytest.raises(UDFError, match="spark.udf.register\\(\\) failed"):
            register_udfs(
                ({"id": "loads", "lang": "python", "module": "json", "return_type": "string"},),
                mock_spark,
            )
