"""Tests for aqueduct doctor command and store backend checks."""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli
from aqueduct.doctor import _check_format_ext_mismatch, CheckResult

pytestmark = pytest.mark.unit

@pytest.fixture(autouse=True)
def mock_spark_stop():
    with patch("pyspark.sql.SparkSession.stop"):
        yield



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
    """Tests for check_store_backend — passes the store-level config object directly."""

    def test_check_store_backend_duckdb_reachable(self, tmp_path):
        from aqueduct.doctor import check_store_backend
        from aqueduct.config import RelationalStoreConfig
        obs_dir = tmp_path / "obs"
        obs_dir.mkdir()
        # 2.0: the duckdb path is a routing base DIRECTORY (file paths rejected).
        store_cfg = RelationalStoreConfig(backend="duckdb", path=str(obs_dir))
        result = check_store_backend("observability", store_cfg)
        assert result.status == "ok"

    def test_check_store_backend_postgres_invalid_dsn(self):
        from aqueduct.doctor import check_store_backend
        from aqueduct.config import RelationalStoreConfig
        store_cfg = RelationalStoreConfig(backend="postgres", path="postgresql://invalid:invalid@invalid/invalid")
        result = check_store_backend("observability", store_cfg)
        assert result.status == "fail"

    def test_check_store_backend_redis_depot_ok_type(self):
        """Redis is valid for depot (is_kv_only=True). With no live Redis, it may fail — only check type."""
        from aqueduct.doctor import check_store_backend
        from aqueduct.config import KVStoreConfig
        store_cfg = KVStoreConfig(backend="redis", path="redis://localhost:6379/0")
        result = check_store_backend("depot", store_cfg, is_kv_only=True)
        # May be ok or fail depending on Redis availability; must not raise
        assert result.status in ("ok", "fail", "warn", "skip")

    def test_doctor_command_positional_file(self, tmp_path):
        """doctor now takes a positional file arg, not --config flag."""
        runner = CliRunner()
        config = tmp_path / "aq.yml"
        config.write_text("aqueduct_config: '1.0'")
        # positional, not --config
        result = runner.invoke(cli, ["doctor", str(config), "--skip-spark"])
        # Should not crash with "No such option: --config"
        assert "--config" not in result.output
        assert result.exit_code in (0, 1)  # may fail store checks but must not crash on bad flag

    def test_doctor_no_arg_uses_cwd_default(self, tmp_path, monkeypatch):
        """doctor with no argument checks ./aqueduct.yml."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "aqueduct.yml").write_text("aqueduct_config: '1.0'")
        runner = CliRunner()
        result = runner.invoke(cli, ["doctor", "--skip-spark"])
        assert result.exit_code in (0, 1)
        assert "aqueduct.yml" in result.output


class TestDoctorAQTest:
    def test_check_aqtest_missing_file(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        results = check_aqtest(tmp_path / "missing.aqtest.yml")
        assert len(results) == 1
        assert results[0].status == "fail"
        assert "file not found" in results[0].detail

    def test_check_aqtest_malformed_yaml(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        p = tmp_path / "bad.aqtest.yml"
        p.write_text("invalid: [yaml: true", encoding="utf-8")
        results = check_aqtest(p)
        assert results[0].status == "fail"
        assert "invalid YAML" in results[0].detail

    def test_check_aqtest_non_mapping(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        p = tmp_path / "list.aqtest.yml"
        p.write_text("- not a mapping", encoding="utf-8")
        results = check_aqtest(p)
        assert results[0].status == "fail"
        assert "top-level must be a YAML mapping" in results[0].detail

    def test_check_aqtest_invalid_version(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        p = tmp_path / "v2.aqtest.yml"
        p.write_text("aqueduct_test: '2.0'", encoding="utf-8")
        results = check_aqtest(p)
        assert results[0].status == "fail"
        assert "missing or unsupported aqueduct_test version" in results[0].detail

    def test_check_aqtest_missing_blueprint_field(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        p = tmp_path / "nobp.aqtest.yml"
        p.write_text("aqueduct_test: '1.0'", encoding="utf-8")
        results = check_aqtest(p)
        assert any(r.status == "fail" and "missing 'blueprint' field" in r.detail for r in results)

    def test_check_aqtest_blueprint_not_found(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        p = tmp_path / "badbp.aqtest.yml"
        p.write_text("aqueduct_test: '1.0'\nblueprint: missing.yml", encoding="utf-8")
        results = check_aqtest(p)
        assert any(r.status == "fail" and "does not resolve to an existing file" in r.detail for r in results)

    def test_check_aqtest_no_tests_warns(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        bp = tmp_path / "bp.yml"
        bp.write_text("aqueduct: '1.0'\nmodules: []", encoding="utf-8")
        p = tmp_path / "notests.aqtest.yml"
        p.write_text("aqueduct_test: '1.0'\nblueprint: bp.yml\ntests: []", encoding="utf-8")
        results = check_aqtest(p)
        assert any(r.status == "warn" and "no test cases declared" in r.detail for r in results)

    def test_check_aqtest_module_not_in_blueprint(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        bp = tmp_path / "bp.yml"
        bp.write_text("aqueduct: '1.0'\nid: bp\nname: BP\nmodules: [{id: m1, type: Ingress, label: M1}]", encoding="utf-8")
        p = tmp_path / "badmod.aqtest.yml"
        p.write_text("aqueduct_test: '1.0'\nblueprint: bp.yml\ntests: [{id: t1, module: m2, assertions: []}]", encoding="utf-8")
        results = check_aqtest(p)
        assert any(r.status == "fail" and "module='m2' not in blueprint" in r.detail for r in results)

    def test_check_aqtest_missing_assertions(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        bp = tmp_path / "bp.yml"
        bp.write_text("aqueduct: '1.0'\nid: bp\nname: BP\nmodules: [{id: m1, type: Ingress, label: M1}]", encoding="utf-8")
        p = tmp_path / "noassert.aqtest.yml"
        p.write_text("aqueduct_test: '1.0'\nblueprint: bp.yml\ntests: [{id: t1, module: m1}]", encoding="utf-8")
        results = check_aqtest(p)
        assert any(r.status == "fail" and "no assertions declared" in r.detail for r in results)

    def test_check_aqtest_ok(self, tmp_path):
        from aqueduct.doctor import check_aqtest
        bp = tmp_path / "bp.yml"
        bp.write_text("aqueduct: '1.0'\nid: bp\nname: BP\nmodules: [{id: m1, type: Ingress, label: M1}]", encoding="utf-8")
        p = tmp_path / "ok.aqtest.yml"
        p.write_text("aqueduct_test: '1.0'\nblueprint: bp.yml\ntests: [{id: t1, module: m1, assertions: [{type: row_count, min: 1}]}]", encoding="utf-8")
        results = check_aqtest(p)
        assert any(r.status == "ok" and "1 test case(s)" in r.detail for r in results)


class TestDoctorAQScenario:
    def test_check_aqscenario_missing_file(self, tmp_path):
        from aqueduct.doctor import check_aqscenario
        results = check_aqscenario(tmp_path / "missing.aqscenario.yml")
        assert len(results) == 1
        assert results[0].status == "fail"
        assert "file not found" in results[0].detail

    def test_check_aqscenario_invalid_blueprint(self, tmp_path):
        from aqueduct.doctor import check_aqscenario
        p = tmp_path / "badbp.aqscenario.yml"
        p.write_text("aqueduct_scenario: '1.0'\nid: s1\nblueprint: missing.yml\ninject_failure: {module: m1}", encoding="utf-8")
        results = check_aqscenario(p)
        assert any(r.status == "fail" and "does not resolve to an existing file" in r.detail for r in results)

    def test_check_aqscenario_module_mismatch(self, tmp_path):
        from aqueduct.doctor import check_aqscenario
        bp = tmp_path / "bp.yml"
        bp.write_text("aqueduct: '1.0'\nid: bp\nname: BP\nmodules: [{id: m1, type: Ingress, label: M1}]", encoding="utf-8")
        p = tmp_path / "badmod.aqscenario.yml"
        p.write_text("aqueduct_scenario: '1.0'\nid: s1\nblueprint: bp.yml\ninject_failure: {module: m2}", encoding="utf-8")
        results = check_aqscenario(p)
        assert any(r.status == "fail" and "inject_failure.module='m2' not in blueprint" in r.detail for r in results)

    def test_check_aqscenario_ok(self, tmp_path):
        from aqueduct.doctor import check_aqscenario
        bp = tmp_path / "bp.yml"
        bp.write_text("aqueduct: '1.0'\nid: bp\nname: BP\nmodules: [{id: m1, type: Ingress, label: M1}]", encoding="utf-8")
        p = tmp_path / "ok.aqscenario.yml"
        p.write_text("aqueduct_scenario: '1.0'\nid: s1\nblueprint: bp.yml\ninject_failure: {module: m1}", encoding="utf-8")
        results = check_aqscenario(p)
        assert any(r.status == "ok" and "id='s1'" in r.detail for r in results)

class TestDoctorCLIFlags:
    def test_doctor_aqtest_flag_sets_exit_1_on_fail(self, tmp_path):
        runner = CliRunner()
        aqt = tmp_path / "t.aqtest.yml"
        aqt.write_text("aqueduct_test: '1.0'\nblueprint: missing.yml", encoding="utf-8")
        result = runner.invoke(cli, ["doctor", "--aqtest", str(aqt), "--skip-spark"])
        assert "aqtest" in result.output
        assert "fail" in result.output
        assert result.exit_code == 1

    def test_doctor_additive_flags_all_reported(self, tmp_path):
        runner = CliRunner()
        bp = tmp_path / "bp.yml"
        bp.write_text("aqueduct: '1.0'\nid: bp\nname: BP\nmodules: [{id: m1, type: Ingress, label: M1}]", encoding="utf-8")
        aqt = tmp_path / "t.aqtest.yml"
        aqt.write_text("aqueduct_test: '1.0'\nblueprint: bp.yml\ntests: [{id: t1, module: m1, assertions: [{type: row_count, min: 1}]}]", encoding="utf-8")
        aqs = tmp_path / "s.aqscenario.yml"
        aqs.write_text("aqueduct_scenario: '1.0'\nid: s1\nblueprint: bp.yml\ninject_failure: {module: m1}", encoding="utf-8")
        
        result = runner.invoke(cli, ["doctor", str(bp), "--aqtest", str(aqt), "--aqscenario", str(aqs), "--skip-spark"])
        assert "config" in result.output
        assert "aqtest" in result.output
        assert "aqscenario" in result.output
        assert result.exit_code == 0
