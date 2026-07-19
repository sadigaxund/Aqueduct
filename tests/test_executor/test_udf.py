"""Tests for the Executor layer: UDF registration and execution."""

from __future__ import annotations
import json
from unittest.mock import MagicMock, patch
import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]

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

    def test_java_udf_missing_class_raises(self, tmp_path):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        jar = tmp_path / "geo.jar"
        jar.write_bytes(b"")
        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="'entry' .* is required"):
            register_udfs(({"id": "geo", "lang": "java", "jar": str(jar)},), mock_spark)

    def test_java_udf_registers_via_session_add_jar(self, tmp_path):
        """Regression: JAR must go through session-level `ADD JAR` SQL — the
        Python SparkContext has no addJar(), and the JVM call alone does not
        put the class on the driver's session classloader."""
        from pyspark.sql.types import StringType

        from aqueduct.executor.spark.udf import register_udfs

        jar = tmp_path / "geo.jar"
        jar.write_bytes(b"")
        mock_spark = MagicMock()
        register_udfs(
            ({"id": "geo", "lang": "java", "jar": str(jar),
              "entry": "com.example.GeoUDF", "return_type": "string"},),
            mock_spark,
        )
        mock_spark.sql.assert_called_once_with(f'ADD JAR "{jar.resolve()}"')
        mock_spark.udf.registerJavaFunction.assert_called_once_with(
            "geo", "com.example.GeoUDF", StringType()
        )
        mock_spark.sparkContext.addJar.assert_not_called()

    def test_java_udf_add_jar_failure_raises(self, tmp_path):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        jar = tmp_path / "geo.jar"
        jar.write_bytes(b"")
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("cluster refused")
        with pytest.raises(UDFError, match="failed to add JAR"):
            register_udfs(
                ({"id": "geo", "lang": "java", "jar": str(jar),
                  "entry": "com.example.GeoUDF", "return_type": "string"},),
                mock_spark,
            )

    def test_spark_register_failure_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        mock_spark.udf.register.side_effect = Exception("Spark refused")
        with pytest.raises(UDFError, match="spark.udf.register\\(\\) failed"):
            register_udfs(
                ({"id": "loads", "lang": "python", "module": "json", "return_type": "string"},),
                mock_spark,
            )

    def test_return_type_hub_timestamp_tz_renders_to_spark_instant_type(self):
        """Phase 80 work package 3: UDF ``return_type`` now routes through
        the Arrow type hub before reaching ``spark.udf.register`` —
        ``timestamp_tz`` (not itself valid Spark DDL) renders to Spark's
        real instant-type spelling, ``timestamp``."""
        from aqueduct.executor.spark.udf import register_udfs

        mock_spark = MagicMock()
        register_udfs(
            ({"id": "loads", "lang": "python", "module": "json", "return_type": "timestamp_tz"},),
            mock_spark,
        )
        mock_spark.udf.register.assert_called_once_with("loads", json.loads, "timestamp")

    def test_return_type_deleted_alias_dict_spellings_still_work(self):
        """Old alias-dict spellings (not hub canonical names) still resolve
        identically through render_native_type's raw-passthrough fallback —
        "long" isn't a Spark DDL word, hub renders it to "bigint"."""
        from aqueduct.executor.spark.udf import register_udfs

        mock_spark = MagicMock()
        register_udfs(
            ({"id": "loads", "lang": "python", "module": "json", "return_type": "long"},),
            mock_spark,
        )
        mock_spark.udf.register.assert_called_once_with("loads", json.loads, "bigint")

    def test_return_type_java_udf_hub_array_renders(self, tmp_path):
        """UDF return_type routes through the hub for java/scala too
        (`_register_java_udf`'s `_parse_datatype_string` call site)."""
        from pyspark.sql.types import ArrayType, IntegerType

        from aqueduct.executor.spark.udf import register_udfs

        jar = tmp_path / "geo.jar"
        jar.write_bytes(b"")
        mock_spark = MagicMock()
        register_udfs(
            ({"id": "geo", "lang": "java", "jar": str(jar),
              "entry": "com.example.GeoUDF", "return_type": "array<int>"},),
            mock_spark,
        )
        mock_spark.udf.registerJavaFunction.assert_called_once_with(
            "geo", "com.example.GeoUDF", ArrayType(IntegerType())
        )

    def test_resolves_via_base_dir_without_sys_path(self, tmp_path):
        """Manifest.base_dir resolves a sibling my_udfs.py next to the
        blueprint — no sys.path mutation needed."""
        from aqueduct.executor.spark.udf import register_udfs

        (tmp_path / "my_udfs.py").write_text(
            "def double(x):\n    return x * 2\n"
        )
        mock_spark = MagicMock()
        register_udfs(
            ({"id": "double", "lang": "python", "module": "my_udfs", "return_type": "bigint"},),
            mock_spark,
            base_dir=str(tmp_path),
        )
        mock_spark.udf.register.assert_called_once()

    def test_survives_stdlib_name_collision(self, tmp_path):
        """A UDF module colliding with an already-imported stdlib name (e.g.
        ``string``) must still load, and must not disturb the real module."""
        import sys

        from aqueduct.executor.spark.udf import register_udfs

        pkg_dir = tmp_path / "string"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")
        (pkg_dir / "my_udfs.py").write_text(
            "def shout(s):\n    return s.upper()\n"
        )
        sentinel = sys.modules["string"]
        mock_spark = MagicMock()
        try:
            register_udfs(
                ({"id": "shout", "lang": "python", "module": "string.my_udfs", "return_type": "string"},),
                mock_spark,
                base_dir=str(tmp_path),
            )
            mock_spark.udf.register.assert_called_once()
            assert sys.modules["string"] is sentinel
        finally:
            sys.modules.pop("string.my_udfs", None)

    def test_ship_module_to_executors_ships_flat_file(self, tmp_path):
        """A single-file (non-package) UDF module gets zipped and shipped via
        addPyFile — the flat-file branch of _ship_module_to_executors."""
        from aqueduct.executor.spark.udf import _ship_module_to_executors
        import importlib.util

        mod_file = tmp_path / "flat_udf_mod.py"
        mod_file.write_text("def triple(x):\n    return x * 3\n")
        spec = importlib.util.spec_from_file_location("flat_udf_mod", mod_file)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        mock_spark = MagicMock()
        _ship_module_to_executors(mod, mock_spark)
        mock_spark.sparkContext.addPyFile.assert_called_once()
