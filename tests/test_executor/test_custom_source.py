"""Phase 61 — custom Python DataSource import/validation (Spark 4.0+)."""
from __future__ import annotations

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

# pyspark.sql.datasource only exists on Spark 4.0+
datasource = pytest.importorskip(
    "pyspark.sql.datasource", reason="custom Python DataSource requires Spark 4.0+"
)

from aqueduct.executor.spark.custom_source import import_datasource_class


class _GoodDS(datasource.DataSource):
    @classmethod
    def name(cls):
        return "aq_test_ds"

    def schema(self):
        return "id int"


def test_import_valid_subclass():
    cls = import_datasource_class("tests.test_executor.test_custom_source._GoodDS")
    assert cls is _GoodDS
    assert cls.name() == "aq_test_ds"


def test_import_not_fully_qualified():
    with pytest.raises(ValueError, match="fully qualified"):
        import_datasource_class("NoDot")


def test_import_not_found():
    with pytest.raises((ValueError, ModuleNotFoundError, AttributeError)):
        import_datasource_class("tests.test_executor.test_custom_source.DoesNotExist")


def test_import_not_a_datasource_subclass():
    with pytest.raises(ValueError, match="must subclass"):
        import_datasource_class("builtins.dict")
