"""Tests for aqueduct doctor."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from aqueduct.doctor import _check_format_ext_mismatch, CheckResult

pytestmark = pytest.mark.unit

class TestDoctorFormatMismatch:
    def test_parquet_with_parquet_ok(self):
        results = []
        _check_format_ext_mismatch(results, "ingress:m1", "parquet", ["data.parquet"], "/tmp/data.parquet", 100)
        assert len(results) == 0

    def test_csv_with_parquet_warns(self):
        results = []
        _check_format_ext_mismatch(results, "ingress:m1", "csv", ["data.parquet"], "/tmp/data.parquet", 100)
        assert len(results) == 1
        assert results[0].status == "warn"
        assert "format='csv' but file extension suggests different format" in results[0].detail

    def test_parquet_with_csv_warns(self):
        results = []
        _check_format_ext_mismatch(results, "ingress:m1", "parquet", ["data.csv"], "/tmp/data.csv", 100)
        assert len(results) == 1
        assert results[0].status == "warn"
        assert "format='parquet' but file extension suggests different format" in results[0].detail

    def test_delta_no_mismatch_check(self):
        results = []
        _check_format_ext_mismatch(results, "ingress:m1", "delta", ["data.csv", "data.parquet", "data"], "/tmp/data", 100)
        assert len(results) == 0

    def test_unknown_format_no_check(self):
        results = []
        _check_format_ext_mismatch(results, "ingress:m1", "weird_format", ["data.csv"], "/tmp/data.csv", 100)
        assert len(results) == 0

    def test_glob_with_mixed_extensions_warns(self):
        results = []
        _check_format_ext_mismatch(results, "ingress:m1", "parquet", ["data1.parquet", "data2.csv"], "/tmp/data*", 100)
        assert len(results) == 1
        assert results[0].status == "warn"
        assert "data2.csv" in results[0].detail

    def test_non_glob_single_file_checked(self):
        results = []
        _check_format_ext_mismatch(results, "ingress:m1", "parquet", ["data.csv"], "/tmp/data.csv", 100)
        assert len(results) == 1
        assert results[0].status == "warn"
