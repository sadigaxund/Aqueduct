"""aqueduct/executor/spark/__init__.py's lazy __getattr__ — execute/ExecuteError/
AssertError are resolved on access, not at package-import time, so importing any
OTHER submodule (udf, probe, error_columns, ...) doesn't unconditionally require
pyspark. Mirrors the parent aqueduct/executor/__init__.py's existing pattern."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def test_execute_resolves_lazily():
    import aqueduct.executor.spark as spark_pkg
    assert callable(spark_pkg.execute)


def test_execute_error_resolves_lazily():
    import aqueduct.executor.spark as spark_pkg
    assert issubclass(spark_pkg.ExecuteError, Exception)


def test_assert_error_resolves_lazily():
    import aqueduct.executor.spark as spark_pkg
    assert issubclass(spark_pkg.AssertError, Exception)


def test_unknown_attribute_raises_attribute_error():
    import aqueduct.executor.spark as spark_pkg
    with pytest.raises(AttributeError, match="bogus_name"):
        spark_pkg.bogus_name
