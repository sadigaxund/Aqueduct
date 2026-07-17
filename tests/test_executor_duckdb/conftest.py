"""Fixtures for DuckDB executor tests.

Unlike ``tests/test_executor/conftest.py`` (which ``collect_ignore_glob``s its
whole directory when ``pyspark`` is absent), this directory needs no such
guard — ``duckdb`` is a base aqueduct-core dependency, always installed.
"""

from __future__ import annotations

import duckdb
import pytest


@pytest.fixture
def duckdb_con():
    """A fresh in-memory DuckDB connection per test."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()
