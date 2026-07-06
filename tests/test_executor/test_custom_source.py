"""Phase 61 — custom Python DataSource import/validation (Spark 4.0+)."""
from __future__ import annotations

import pytest

from aqueduct.errors import ConfigError

pytestmark = [pytest.mark.spark, pytest.mark.integration]

# pyspark.sql.datasource only exists on Spark 4.0+
datasource = pytest.importorskip(
    "pyspark.sql.datasource", reason="custom Python DataSource requires Spark 4.0+"
)

from aqueduct.executor.spark.custom_source import import_datasource_class  # noqa: E402


class _GoodDS(datasource.DataSource):
    @classmethod
    def name(cls):
        return "aq_test_ds"

    def schema(self):
        return "id int"


def test_import_valid_subclass():
    cls = import_datasource_class("tests.test_executor.test_custom_source._GoodDS")
    assert cls.__name__ == "_GoodDS"
    assert issubclass(cls, datasource.DataSource)
    assert cls.name() == "aq_test_ds"


def test_import_not_fully_qualified():
    with pytest.raises(ConfigError, match="fully qualified"):
        import_datasource_class("NoDot")


def test_import_not_found():
    with pytest.raises((ConfigError, ModuleNotFoundError)):
        import_datasource_class("tests.test_executor.test_custom_source.DoesNotExist")


def test_import_not_a_datasource_subclass():
    with pytest.raises(ConfigError, match="must subclass"):
        import_datasource_class("builtins.dict")


def test_import_resolves_via_base_dir_without_sys_path(tmp_path):
    """Manifest.base_dir resolves a sibling module next to the blueprint —
    no sys.path mutation needed."""
    (tmp_path / "my_ds.py").write_text(
        "from pyspark.sql.datasource import DataSource\n\n"
        "class MyDS(DataSource):\n"
        "    @classmethod\n"
        "    def name(cls):\n"
        "        return 'aq_my_ds'\n"
        "    def schema(self):\n"
        "        return 'id int'\n"
    )
    cls = import_datasource_class("my_ds.MyDS", str(tmp_path))
    assert cls.__name__ == "MyDS"
    assert cls.name() == "aq_my_ds"


def test_import_survives_stdlib_name_collision(tmp_path):
    """A DataSource module colliding with an already-imported stdlib name
    (e.g. ``csv``) must still load, and must not disturb the real module."""
    import sys

    pkg_dir = tmp_path / "csv"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    (pkg_dir / "my_ds.py").write_text(
        "from pyspark.sql.datasource import DataSource\n\n"
        "class MyDS(DataSource):\n"
        "    @classmethod\n"
        "    def name(cls):\n"
        "        return 'aq_my_ds'\n"
        "    def schema(self):\n"
        "        return 'id int'\n"
    )
    sentinel = sys.modules["csv"]
    try:
        cls = import_datasource_class("csv.my_ds.MyDS", str(tmp_path))
        assert cls.name() == "aq_my_ds"
        assert sys.modules["csv"] is sentinel
    finally:
        sys.modules.pop("csv.my_ds", None)
