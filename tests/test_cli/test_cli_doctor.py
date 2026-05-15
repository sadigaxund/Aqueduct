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

class TestDoctorStoreBackends:
    def test_doctor_check_store_backend_duckdb(self, tmp_path):
        from aqueduct.doctor import check_store_backend
        from aqueduct.config import AqueductConfig
        cfg = AqueductConfig()
        cfg.stores.obs.path = str(tmp_path / "obs.db")
        result = check_store_backend("obs", cfg, is_kv_only=False)
        assert result.status == "ok"
        
    def test_doctor_check_store_backend_obs_redis_fail(self):
        from aqueduct.doctor import check_store_backend
        from aqueduct.config import AqueductConfig
        cfg = AqueductConfig()
        cfg.stores.obs.backend = "redis"
        cfg.stores.obs.path = "redis://localhost"
        result = check_store_backend("obs", cfg, is_kv_only=False)
        assert result.status == "fail"
        assert "redis not supported for obs" in result.detail.lower()

    def test_doctor_check_store_backend_postgres_invalid_dsn(self):
        from aqueduct.doctor import check_store_backend
        from aqueduct.config import AqueductConfig
        cfg = AqueductConfig()
        cfg.stores.obs.backend = "postgres"
        cfg.stores.obs.path = "postgresql://invalid:invalid@invalid/invalid"
        result = check_store_backend("obs", cfg, is_kv_only=False)
        assert result.status == "fail"

    def test_doctor_output_shows_new_stores(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        runner = CliRunner()
        config = tmp_path / "aq.yml"
        config.write_text("aqueduct_config: '1.0'")
        result = runner.invoke(cli, ["doctor", "--config", str(config)])
        assert "obs:" in result.output
        assert "lineage:" in result.output
        assert "depot:" in result.output
