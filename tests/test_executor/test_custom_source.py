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
