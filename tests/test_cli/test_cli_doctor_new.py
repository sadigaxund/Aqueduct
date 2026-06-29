# tests/test_cli/test_cli_doctor_new.py
import os
import sys
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from aqueduct.doctor import (
    CheckResult,
    check_agent,
    check_cascade_tiers,
    check_storage,
    _host_port,
    _tcp_ok,
    check_spark,
    run_doctor,
)
from aqueduct.patch.explain_gate import _formatted_plan
from aqueduct.cli import cli

pytestmark = pytest.mark.unit


# ── 1 & 2 & 14 & 15. Render Collapse and Verbose Tests ────────────────────────

@pytest.mark.parametrize("verbose_flag", ["-v", "--verbose"])
def test_doctor_render_hides_skip_and_quiet_ok(tmp_path, verbose_flag):
    """Default view omits skip/quiet_ok rows; collapses to '· more' row. --verbose/-v shows all."""
    runner = CliRunner()
    config = tmp_path / "aqueduct.yml"
    config.write_text("""
aqueduct_config: '1.0'
deployment:
  engine: spark
  target: local
  master_url: "local[*]"
""", encoding="utf-8")

    # Mock run_doctor to return specific status rows
    mock_results = [
        CheckResult("config", "ok", "config ok"),
        # skip check
        CheckResult("webhook", "skip", "not configured"),
        # ok + quiet_when_ok check
        CheckResult("cloudpickle", "ok", "cloudpickle ok", quiet_when_ok=True),
        # normal ok check
        CheckResult("observability", "ok", "observability ok"),
    ]

    with patch("aqueduct.doctor.run_doctor", return_value=mock_results):
        # 1. Default (no verbose flag)
        result = runner.invoke(cli, ["doctor", str(config), "--skip-spark"])
        assert result.exit_code == 0
        assert "config" in result.output
        assert "observability" in result.output
        # Hidden rows are in collapsed "· more" line
        assert "· more" in result.output
        assert "webhook" in result.output
        assert "cloudpickle" in result.output
        assert "(ok / not applicable / not configured — --verbose)" in result.output

        # 2. Verbose (via -v or --verbose)
        result_verbose = runner.invoke(cli, ["doctor", str(config), "--skip-spark", verbose_flag])
        assert result_verbose.exit_code == 0
        assert "config" in result_verbose.output
        assert "observability" in result_verbose.output
        assert "webhook" in result_verbose.output
        assert "cloudpickle" in result_verbose.output
        assert "· more" not in result_verbose.output


def test_doctor_no_skip_no_collapse(tmp_path):
    """No skip/quiet_ok rows -> no collapsed/· more line printed."""
    runner = CliRunner()
    config = tmp_path / "aqueduct.yml"
    config.write_text("aqueduct_config: '1.0'", encoding="utf-8")

    mock_results = [
        CheckResult("config", "ok", "config ok"),
        CheckResult("observability", "ok", "observability ok"),
    ]

    with patch("aqueduct.doctor.run_doctor", return_value=mock_results):
        result = runner.invoke(cli, ["doctor", str(config), "--skip-spark"])
        assert result.exit_code == 0
        assert "· more" not in result.output


def test_doctor_all_ok_some_skip_passes(tmp_path):
    """All rows ok + some skip -> still ✓ all checks passed (skip never fails)."""
    runner = CliRunner()
    config = tmp_path / "aqueduct.yml"
    config.write_text("aqueduct_config: '1.0'", encoding="utf-8")

    mock_results = [
        CheckResult("config", "ok", "config ok"),
        CheckResult("webhook", "skip", "not configured"),
    ]

    with patch("aqueduct.doctor.run_doctor", return_value=mock_results):
        result = runner.invoke(cli, ["doctor", str(config), "--skip-spark"])
        assert result.exit_code == 0
        assert "✓ all checks passed" in result.output


# ── 4 & 17. Agent connectivity tests ──────────────────────────────────────────

def test_check_agent_anthropic_scenarios(monkeypatch):
    """check_agent with anthropic provider under various environment and config states."""
    # 1. provider=anthropic + no key + no base_url -> skip "self-healing not configured (opt-in)"
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    res_skip = check_agent("anthropic", base_url=None, model="claude-3")
    assert res_skip.status == "skip"
    assert "self-healing not configured (opt-in" in res_skip.detail

    # 2. provider=anthropic + no key + base_url set -> warn
    res_warn = check_agent("anthropic", base_url="http://localhost:8000", model="claude-3")
    assert res_warn.status == "warn"
    assert "configured provider" not in res_warn.detail # Just double checking it names configured provider / openai_compat
    assert "agent configured but ANTHROPIC_API_KEY not set" in res_warn.detail
    assert "switch agent.provider to openai_compat" in res_warn.detail
    assert "pipeline runs fine without it" in res_warn.detail

    # 3. provider=anthropic + key present -> ok
    monkeypatch.setenv("ANTHROPIC_API_KEY", "mock-key")
    res_ok = check_agent("anthropic", base_url=None, model="claude-3")
    assert res_ok.status == "ok"
    assert "ANTHROPIC_API_KEY present (API not called)" in res_ok.detail


# ── 6. _formatted_plan sql_ctx access removal test ───────────────────────────

def test_explain_formatted_plan_no_sql_ctx_access():
    """_formatted_plan uses df.sparkSession directly and NEVER accesses df.sql_ctx if present."""
    mock_spark = MagicMock()
    mock_spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString.return_value = "formatted"
    
    mock_jdf = MagicMock()
    mock_jdf.queryExecution.return_value.explainString.return_value = "MockFormattedPlan"

    class MockDF:
        def __init__(self):
            self._jdf = mock_jdf
            self.sparkSession = mock_spark

        @property
        def sql_ctx(self):
            raise AssertionError("sql_ctx was accessed!")

    df = MockDF()
    plan = _formatted_plan(df)
    assert plan == "MockFormattedPlan"


# ── 7 & 8. TCP Reachability and Parsing Tests ─────────────────────────────────

def test_host_port_parsing():
    """_host_port parses spark://h:p, http://h:p, h:p, k8s://https://h:p; bad -> None."""
    assert _host_port("spark://localhost:7077", 7077) == ("localhost", 7077)
    assert _host_port("http://my-host:8080", 80) == ("my-host", 8080)
    assert _host_port("my-host:9000", 7077) == ("my-host", 9000)
    assert _host_port("bad-url-format", 7077) is None
    assert _host_port("k8s://https://kube-api:6443", 443) == ("kube-api", 6443)
    assert _host_port("k8s://https://hostname", 443) == ("hostname", 443)


def test_tcp_ok_refused():
    """_tcp_ok returns False on refused or unroutable connection within timeout."""
    # We probe an unroutable documentation IP (RFC 5737) to ensure failure
    assert _tcp_ok("192.0.2.1", 12345, timeout=0.5) is False


def test_spark_check_tcp_reachability(monkeypatch):
    """Default check_spark (preflight=False) uses fast TCP probe and does not build session."""
    # Local mode is always ok
    ok_local, _ = check_spark("local[*]", {}, preflight=False, target="local")
    assert ok_local.status == "ok"
    assert "local mode" in ok_local.detail

    # Remote master reachable (standalone target)
    with patch("aqueduct.doctor._tcp_ok", return_value=True):
        ok_remote, _ = check_spark("spark://localhost:7077", {}, preflight=False, target="standalone")
        assert ok_remote.status == "ok"
        assert "reachable" in ok_remote.detail

    # Remote master unreachable (standalone target)
    with patch("aqueduct.doctor._tcp_ok", return_value=False):
        fail_remote, _ = check_spark("spark://localhost:7077", {}, preflight=False, target="standalone")
        assert fail_remote.status == "fail"
        assert "(Not a timeout: no SparkSession was built.)" in fail_remote.detail


def test_yarn_reachability_warns_without_hadoop_conf_dir(monkeypatch):
    """yarn target warns when HADOOP_CONF_DIR and YARN_CONF_DIR are both unset."""
    monkeypatch.delenv("HADOOP_CONF_DIR", raising=False)
    monkeypatch.delenv("YARN_CONF_DIR", raising=False)
    spark_res, _ = check_spark("yarn", {}, preflight=False, target="yarn")
    assert spark_res.status == "warn"
    assert "HADOOP_CONF_DIR" in spark_res.detail


def test_yarn_reachability_ok_with_hadoop_conf_dir(monkeypatch):
    """yarn target passes when HADOOP_CONF_DIR is set."""
    monkeypatch.setenv("HADOOP_CONF_DIR", "/etc/hadoop/conf")
    monkeypatch.delenv("YARN_CONF_DIR", raising=False)
    spark_res, _ = check_spark("yarn", {}, preflight=False, target="yarn")
    assert spark_res.status == "ok"
    assert "/etc/hadoop/conf" in spark_res.detail


def test_yarn_reachability_ok_with_yarn_conf_dir(monkeypatch):
    """yarn target passes when YARN_CONF_DIR is set (fallback)."""
    monkeypatch.delenv("HADOOP_CONF_DIR", raising=False)
    monkeypatch.setenv("YARN_CONF_DIR", "/opt/yarn/conf")
    spark_res, _ = check_spark("yarn", {}, preflight=False, target="yarn")
    assert spark_res.status == "ok"
    assert "/opt/yarn/conf" in spark_res.detail


def test_k8s_reachability_parses_api_server():
    """kubernetes target parses k8s:// URL to host:port."""
    hp = _host_port("k8s://https://kube-api.cluster.local:6443", 443)
    assert hp == ("kube-api.cluster.local", 6443)


def test_k8s_reachability_ok_when_api_server_tcp_ok(monkeypatch):
    """kubernetes target with reachable API server and k8s keys -> ok."""
    monkeypatch.delenv("HADOOP_CONF_DIR", raising=False)
    monkeypatch.delenv("YARN_CONF_DIR", raising=False)
    k8s_cfg = {"spark.kubernetes.namespace": "aqueduct", "spark.kubernetes.container.image": "img"}
    with patch("aqueduct.doctor._tcp_ok", return_value=True):
        spark_res, _ = check_spark("k8s://https://host:443", k8s_cfg, preflight=False, target="kubernetes")
    assert spark_res.status == "ok"
    assert "reachable" in spark_res.detail


def test_k8s_reachability_warns_without_k8s_keys(monkeypatch):
    """kubernetes target warns when no spark.kubernetes.* keys are present."""
    monkeypatch.delenv("HADOOP_CONF_DIR", raising=False)
    monkeypatch.delenv("YARN_CONF_DIR", raising=False)
    with patch("aqueduct.doctor._tcp_ok", return_value=True):
        spark_res, _ = check_spark("k8s://https://host:443", {}, preflight=False, target="kubernetes")
    assert spark_res.status == "warn"
    assert "spark.kubernetes" in spark_res.detail


def test_k8s_reachability_fail_when_api_server_unreachable(monkeypatch):
    """kubernetes target with unreachable API server -> fail."""
    monkeypatch.delenv("HADOOP_CONF_DIR", raising=False)
    monkeypatch.delenv("YARN_CONF_DIR", raising=False)
    with patch("aqueduct.doctor._tcp_ok", return_value=False):
        spark_res, _ = check_spark("k8s://https://host:6443", {}, preflight=False, target="kubernetes")
    assert spark_res.status == "fail"
    assert "unreachable" in spark_res.detail


# ── 9. S3A Endpoint TCP Probe and bucketless design Test ──────────────────────

def test_s3a_endpoint_tcp_probed():
    """check_storage does no bucket I/O; creds present -> ok; no keys -> warn; unreachable -> fail; GCS/ADLS -> ok."""
    # 1. Reachable + credentials present
    spark_cfg_ok = {
        "spark.hadoop.fs.s3a.endpoint": "s3-host:80",
        "spark.hadoop.fs.s3a.access.key": "key1",
        "spark.hadoop.fs.s3a.secret.key": "key2",
    }
    with patch("aqueduct.doctor._tcp_ok", return_value=True) as mock_tcp:
        res = check_storage(spark_cfg_ok, spark_ok=True)
        assert mock_tcp.call_count == 1
        assert mock_tcp.call_args[0] == ("s3-host", 80)
        assert res.status == "ok"
        assert "auth not bucket-tested" in res.detail

    # 2. Reachable + credentials missing -> warn
    spark_cfg_no_keys = {
        "spark.hadoop.fs.s3a.endpoint": "s3-host:80",
    }
    with patch("aqueduct.doctor._tcp_ok", return_value=True):
        res = check_storage(spark_cfg_no_keys, spark_ok=True)
        assert res.status == "warn"
        assert "no access/secret key" in res.detail
        assert "auth not bucket-tested" in res.detail

    # 3. Unreachable endpoint -> fail
    with patch("aqueduct.doctor._tcp_ok", return_value=False):
        res = check_storage(spark_cfg_ok, spark_ok=True)
        assert res.status == "fail"
        assert "failed" in res.detail

    # 4. GCS / ADLS ok with note
    gcs_cfg = {
        "spark.hadoop.google.cloud.auth.service.account.enable": "true"
    }
    res = check_storage(gcs_cfg, spark_ok=True)
    assert res.status == "ok"
    assert "auth not bucket-tested" in res.detail

    # 5. Verify _storage_probe_paths is removed from doctor.py
    import aqueduct.doctor as doc
    assert not hasattr(doc, "_storage_probe_paths")


# ── 10. --preflight execution test ────────────────────────────────────────────

def test_preflight_spark_session_failure():
    """--preflight makes a real make_spark_session call and returns custom failure on error."""
    with patch("aqueduct.executor.spark.session.make_spark_session", side_effect=Exception("Jars missing")):
        spark_res, storage_res = check_spark("spark://localhost:7077", {}, preflight=True)
        assert spark_res.status == "fail"
        assert "preflight session failed: spark://localhost:7077: Jars missing" in spark_res.detail
        assert storage_res.status == "skip"


# ── 11. SPARK_PROBE_TIMEOUT and ThreadPoolExecutor Removal Check ──────────────

def test_spark_probe_timeout_removed():
    """Verify SPARK_PROBE_TIMEOUT and ThreadPoolExecutor are not present/imported in doctor.py."""
    import aqueduct.doctor as doc
    assert not hasattr(doc, "SPARK_PROBE_TIMEOUT")
    # Read the file content to verify no ThreadPoolExecutor imports or calls exist
    content = Path(doc.__file__).read_text(encoding="utf-8")
    assert "ThreadPoolExecutor" not in content


# ── 12. --skip-spark short-circuit check ──────────────────────────────────────

def test_skip_spark_short_circuits():
    """--skip-spark short circuits before any probe, spark and storage checks skipped."""
    # A config with a remote master URL that would otherwise probe
    config = MagicMock()
    config.deployment.master_url = "spark://remote:7077"
    config.spark_config = {}

    with patch("aqueduct.doctor.check_spark") as mock_check_spark:
        results = run_doctor(config_path=None, skip_spark=True, preflight=False)
        assert mock_check_spark.call_count == 0
        spark_res = next(r for r in results if r.name == "spark")
        storage_res = next(r for r in results if r.name == "storage")
        assert spark_res.status == "skip"
        assert storage_res.status == "skip"
        assert "--skip-spark flag set" in spark_res.detail


# ── 13 & 16. CheckResult default fields and cloudpickle ───────────────────────

def test_check_result_defaults():
    """CheckResult has group + quiet_when_ok fields defaulting to 'general' + False."""
    res = CheckResult("test", "ok", "details")
    assert res.group == "general"
    assert res.quiet_when_ok is False


def test_cloudpickle_compat_quiet_when_ok():
    """cloudpickle compat check on Python < 3.13 returns quiet_when_ok=True."""
    from aqueduct.doctor import check_cloudpickle_compat
    res = check_cloudpickle_compat("local[*]")
    assert res.name == "cloudpickle"
    if sys.version_info < (3, 13):
        assert res.status == "ok"
        assert res.quiet_when_ok is True


# ── 18. check_storage skipped scenario ────────────────────────────────────────

def test_check_storage_skipped():
    """check_storage returns skip detail when skipped=True."""
    res = check_storage({"spark.hadoop.fs.s3a.endpoint": "s3-host:80"}, spark_ok=False, skipped=True)
    assert res.status == "skip"
    assert "not probed (--skip-spark)" in res.detail


# ── 19. cluster-stores relative duckdb paths warn test ────────────────────────

def test_cluster_stores_relative_duckdb_warns(tmp_path):
    """Relative DuckDB store paths in cluster mode warn (not fail) with a one-line message.

    NOTE: load_config() resolves relative store paths to absolute (relative to the config
    file's parent directory) before returning the AqueductConfig object.  The cluster-stores
    warn branch in run_doctor therefore never fires when using a real config file because the
    paths are already absolute by the time the check runs.

    We patch load_config inside run_doctor to return a config whose store paths are still
    the raw relative strings, which is the state the warn branch was designed to detect
    (e.g. a config written by hand and validated without path resolution).
    """
    config_file = tmp_path / "aqueduct.yml"
    config_file.write_text("""
aqueduct_config: '1.0'
deployment:
  engine: spark
  target: local
  master_url: "local[*]"
  env: cluster
stores:
  observability: {backend: duckdb, path: ".aqueduct/obs.db"}
  lineage: {backend: duckdb, path: ".aqueduct/lin.db"}
  depot: {backend: duckdb, path: ".aqueduct/depot.db"}
""", encoding="utf-8")

    from aqueduct.config import (
        load_config as _real_load_config,
        RelationalStoreConfig,
        KVStoreConfig,
        DepotMountConfig,
    )

    def _load_with_relative_paths(path=None):
        """Wrap real load_config but restore verbatim relative store paths."""
        cfg = _real_load_config(path)
        # StoresConfig is a frozen Pydantic model — rebuild it with unresolved paths.
        from aqueduct.config import StoresConfig
        new_stores = StoresConfig(
            observability=RelationalStoreConfig(backend="duckdb", path=".aqueduct/obs.db"),
            depots={"default": DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db")},
        )
        return cfg.model_copy(update={"stores": new_stores})

    runner = CliRunner()
    with patch("aqueduct.config.load_config", side_effect=_load_with_relative_paths):
        result = runner.invoke(cli, ["doctor", str(config_file), "--skip-spark"])

    assert result.exit_code == 0, result.output
    assert "⚠ cluster-stores" in result.output, result.output
    # Check the one-line warn message is present
    lines = result.output.splitlines()
    cs_line = next(line for line in lines if "cluster-stores" in line)
    assert "relative DuckDB paths" in cs_line
    assert "lost on driver restart" in cs_line
    assert "✓ all checks passed" in result.output

    # Check that check_store_backend runs real duckdb open / usability check
    from aqueduct.doctor import check_store_backend
    store_cfg = RelationalStoreConfig(backend="duckdb", path=str(tmp_path / "obs.db"))
    res = check_store_backend("observability", store_cfg)
    assert res.status == "ok"
    assert "backend=duckdb" in res.detail


# ── 20. Additive Flags scenario test ──────────────────────────────────────────

def test_doctor_additive_scenario_and_config(tmp_path):
    """doctor positional config + --aqscenario runs config probe AND scenario pre-flight in one go."""
    config_file = tmp_path / "aqueduct.yml"
    config_file.write_text("""
aqueduct_config: '1.0'
deployment:
  engine: spark
  target: local
  master_url: "local[*]"
""", encoding="utf-8")

    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text("""
aqueduct: '1.0'
id: bp
name: BP
modules: [{id: m1, type: Ingress, label: M1}]
edges: []
""", encoding="utf-8")

    scenario_file = tmp_path / "scenario.aqscenario.yml"
    scenario_file.write_text("""
aqueduct_scenario: '1.0'
id: sc1
blueprint: blueprint.yml
inject_failure: {module: m1}
""", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, [
        "doctor", str(config_file),
        "--aqscenario", str(scenario_file),
        "--skip-spark",
    ])
    assert result.exit_code == 0
    assert "config" in result.output
    assert "aqscenario" in result.output


# ── 21. Warning Infrastructure & Suppression Tests ────────────────────────────

def test_warning_suppression_and_sentinels():
    """Verify warnings.suppress: ["*"] and other blacklists/sentinels in warnings.py."""
    import aqueduct.warnings as aqw
    import warnings

    # 1. Reset defaults
    aqw.set_default_suppress([])

    # Capture warning emission
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        aqw.emit("test_rule", "msg")
        assert len(w) == 1
        assert "test_rule" in str(w[0].message)

    # 2. Silence via "*" sentinel
    aqw.set_default_suppress(["*"])
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        aqw.emit("test_rule", "msg")
        assert len(w) == 0

    # 3. Silence via specific ID
    aqw.set_default_suppress(["test_rule"])
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        aqw.emit("test_rule", "msg")
        assert len(w) == 0
        
        # But other rules still emit
        aqw.emit("another_rule", "msg")
        assert len(w) == 1


def test_warnings_config_silence_all_removed(tmp_path):
    """WarningsConfig(silence_all=...) raises ConfigError because extra is forbidden."""
    from aqueduct.config import load_config, ConfigError
    config_file = tmp_path / "aqueduct.yml"
    
    # Passing silence_all: true should raise ConfigError
    config_file.write_text("""
aqueduct_config: '1.0'
warnings:
  silence_all: true
""", encoding="utf-8")
    
    with pytest.raises(ConfigError) as exc:
        load_config(config_file)
    assert "validation error" in str(exc.value)
    assert "silence_all" in str(exc.value)


def test_compiler_warnings_silence_all():
    """Compiler with warnings_silence_all=True sets suppress to {"*"} and silences compile-time rules."""
    from aqueduct.compiler.compiler import compile as compile_bp
    from aqueduct.parser.models import Blueprint, ContextRegistry
    
    bp = Blueprint(
        id="test_bp",
        name="Test",
        aqueduct_version="1.0",
        context=ContextRegistry(values={}),
        modules=(),
        edges=()
    )
    # Compiler compile normally checks for retry/egress append warnings, etc.
    # Compile-time warning emit is suppressed when warnings_silence_all is True.
    with patch("aqueduct.warnings.emit") as mock_emit:
        compile_bp(bp, warnings_silence_all=True)
        assert mock_emit.call_count == 0


def test_run_cluster_relative_store_dir_warning(tmp_path):
    """run under env=cluster and relative store dir warns via AQ-WARN [cluster_store_path_relative]."""
    config_file = tmp_path / "aqueduct.yml"
    config_file.write_text("""
aqueduct_config: '1.0'
deployment:
  engine: spark
  target: local
  master_url: "local[*]"
  env: cluster
stores:
  observability: {backend: duckdb, path: ".aqueduct/obs.db"}
  lineage: {backend: duckdb, path: ".aqueduct/lin.db"}
  depot: {backend: duckdb, path: ".aqueduct/depot.db"}
""", encoding="utf-8")

    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text("""
aqueduct: '1.0'
id: bp
name: BP
modules: []
edges: []
""", encoding="utf-8")

    from aqueduct.config import (
        load_config as _real_load_config,
        RelationalStoreConfig,
        KVStoreConfig,
        DepotMountConfig,
    )

    def _load_with_relative_paths(path=None):
        """Wrap real load_config but restore verbatim relative store paths."""
        cfg = _real_load_config(path)
        from aqueduct.config import StoresConfig
        new_stores = StoresConfig(
            observability=RelationalStoreConfig(backend="duckdb", path=".aqueduct/obs.db"),
            depots={"default": DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db")},
        )
        return cfg.model_copy(update={"stores": new_stores})

    runner = CliRunner()
    
    # 1. Normal run -> warns via aqueduct.warnings.emit
    with patch("aqueduct.config.load_config", side_effect=_load_with_relative_paths):
        with patch("aqueduct.warnings.emit") as mock_emit:
            result = runner.invoke(cli, [
                "run", str(bp_file),
                "--config", str(config_file),
            ])
            assert result.exit_code == 0
            assert mock_emit.call_count == 1
            assert mock_emit.call_args[0][0] == "cluster_store_path_relative"
            assert "WARNING:" not in result.output  # no raw WARNING: click.echo remains


# ── 37. Doctor package split: public API resolution ───────────────────────────


def test_doctor_package_split_public_names_resolve():
    """Every public check name resolves from aqueduct.doctor; pyspark not imported eagerly."""
    from aqueduct.doctor import (
        check_cascade_tiers,
        check_config,
        check_spark,
        check_storage,
        check_store_backend,
        check_blueprint_sources,
        check_blueprint_sources_from_manifest,
        check_aqtest,
        check_aqscenario,
        check_cloudpickle_compat,
        run_doctor,
        CheckResult,
    )

    # All names are callable (functions) or classes
    assert callable(check_cascade_tiers)
    assert callable(check_config)
    assert callable(check_spark)
    assert callable(check_storage)
    assert callable(check_store_backend)
    assert callable(check_blueprint_sources)
    assert callable(check_blueprint_sources_from_manifest)
    assert callable(check_aqtest)
    assert callable(check_aqscenario)
    assert callable(check_cloudpickle_compat)
    assert callable(run_doctor)
    assert CheckResult is not None

    # Verify no pyspark exception was raised by import alone
    import aqueduct.doctor
    assert hasattr(aqueduct.doctor, "check_config")
    assert hasattr(aqueduct.doctor, "check_spark")
    # _tcp_ok, check_spark, check_blueprint_sources_from_manifest,
    # run_doctor are all accessible from the __init__ namespace
    assert hasattr(aqueduct.doctor, "_tcp_ok")
    assert hasattr(aqueduct.doctor, "run_doctor")


# ── 38. Phase 46 — check_cascade_tiers ──────────────────────────────────────

class TestCheckCascadeTiers:
    def _blueprint(self, tmp_path, agent_block: str) -> Path:
        bp = tmp_path / "blueprint.yml"
        bp.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            + agent_block
            + "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            + "edges: []\n"
        )
        return bp

    def test_no_cascade_block_returns_empty(self, tmp_path):
        bp = self._blueprint(tmp_path, "")
        results = check_cascade_tiers(bp)
        assert results == []

    def test_unparseable_blueprint_returns_empty(self, tmp_path):
        bp = tmp_path / "bad.yml"
        bp.write_text("not: valid: yaml: [")
        results = check_cascade_tiers(bp)
        assert results == []

    def test_anthropic_tier_missing_key_warns(self, tmp_path):
        bp = self._blueprint(tmp_path, "agent:\n  cascade:\n    - model: claude\n")
        with patch.dict(os.environ, {}, clear=True):
            results = check_cascade_tiers(bp)
        assert any("ANTHROPIC_API_KEY" in r.detail for r in results)
        assert all(r.status == "warn" for r in results if "ANTHROPIC_API_KEY" in r.detail)

    def test_anthropic_tier_with_key_ok(self, tmp_path):
        bp = self._blueprint(tmp_path, "agent:\n  cascade:\n    - model: claude\n")
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-test"}):
            results = check_cascade_tiers(bp)
        assert any(r.status == "ok" for r in results)

    def test_openai_compat_tier_no_base_url_warns(self, tmp_path):
        bp = self._blueprint(tmp_path,
            "agent:\n  cascade:\n    - model: gpt4\n      provider: openai_compat\n")
        results = check_cascade_tiers(bp)
        assert any("base_url" in r.detail for r in results)
        assert all(r.status == "warn" for r in results if "base_url" in r.detail)

    def test_openai_compat_tier_with_tier_base_url_ok(self, tmp_path):
        bp = self._blueprint(tmp_path,
            "agent:\n  cascade:\n    - model: gpt4\n      provider: openai_compat\n"
            "      base_url: https://tier.test/v1\n")
        results = check_cascade_tiers(bp)
        assert any(r.status == "ok" for r in results)

    def test_openai_compat_tier_with_engine_base_url_ok(self, tmp_path):
        bp = self._blueprint(tmp_path,
            "agent:\n  cascade:\n    - model: gpt4\n      provider: openai_compat\n")
        results = check_cascade_tiers(bp, engine_provider="openai_compat", engine_base_url="https://engine.test/v1")
        assert any(r.status == "ok" for r in results)

    def test_unknown_provider_warns(self, tmp_path):
        bp = self._blueprint(tmp_path, "agent:\n  cascade:\n    - model: claude\n")
        results = check_cascade_tiers(bp, engine_provider="custom")
        assert any("unknown provider" in r.detail for r in results)
        assert all(r.status == "warn" for r in results if "unknown provider" in r.detail)


# ── 39. Phase 35 — doctor _check_spillway_error_types ──────────────────────────

class TestCheckSpillwayErrorTypes:
    def _manifest(self, modules=None, edges=None):
        """Build a manifest-like object with modules and edges."""
        return type("Manifest", (), {
            "modules": modules or (),
            "edges": edges or (),
        })()

    def test_no_spillway_edges_no_warnings(self):
        from aqueduct.doctor import _check_spillway_error_types
        from aqueduct.parser.models import Module
        m = self._manifest(
            modules=(Module(id="a1", type="Assert", label="A1",
                            config={"rules": [{"error_type": "DQ"}]}),),
            edges=(),
        )
        results = _check_spillway_error_types(m)
        assert results == []

    def test_spillway_edge_matching_error_type_no_warning(self):
        from aqueduct.doctor import _check_spillway_error_types
        from aqueduct.parser.models import Module, Edge
        m = self._manifest(
            modules=(Module(id="a1", type="Assert", label="A1",
                            config={"rules": [{"error_type": "MyCheck"}]}),),
            edges=(Edge(from_id="a1", to_id="sink", port="spillway", error_types=("MyCheck",)),),
        )
        results = _check_spillway_error_types(m)
        assert results == []

    def test_spillway_edge_unknown_error_type_warns(self):
        from aqueduct.doctor import _check_spillway_error_types
        from aqueduct.parser.models import Edge
        m = self._manifest(
            modules=(),
            edges=(Edge(from_id="a1", to_id="sink", port="spillway", error_types=("BogusLabel",)),),
        )
        results = _check_spillway_error_types(m)
        assert len(results) == 1
        assert "BogusLabel" in results[0].detail
        assert results[0].status == "warn"

    def test_main_port_edge_ignored(self):
        from aqueduct.doctor import _check_spillway_error_types
        from aqueduct.parser.models import Edge
        m = self._manifest(
            edges=(Edge(from_id="a", to_id="b", port="main", error_types=("X",)),),
        )
        results = _check_spillway_error_types(m)
        assert results == []

    def test_builtin_labels_known(self):
        """SpillwayCondition, freshness, sql_row, custom are always known."""
        from aqueduct.doctor import _check_spillway_error_types
        from aqueduct.parser.models import Edge
        for label in ("SpillwayCondition", "freshness", "sql_row", "custom"):
            m = self._manifest(
                edges=(Edge(from_id="a1", to_id="sink", port="spillway", error_types=(label,)),),
            )
            results = _check_spillway_error_types(m)
            assert results == [], f"builtin label {label!r} falsely flagged as unknown"


# ── T27 Part 1: Spark version handshake (major.minor, no matrix) ────────────────

from aqueduct.doctor import _spark_version_verdict, _parse_java_major, check_java


class TestSparkVersionVerdict:
    def test_exact_match_ok(self):
        assert _spark_version_verdict("3.5.0", "3.5.0")[0] == "ok"
        assert _spark_version_verdict("3.5.1", "3.5.0")[0] == "ok"   # patch differs → still ok

    def test_minor_mismatch_warns(self):
        status, note = _spark_version_verdict("3.4.1", "3.5.0")
        assert status == "warn"
        assert "pyspark=3.5.0" in note and "Spark=3.4.1" in note

    def test_major_mismatch_warns(self):
        assert _spark_version_verdict("3.5.0", "4.0.0")[0] == "warn"


class TestJavaCheck:
    @pytest.mark.parametrize("text,expected", [
        ('openjdk version "17.0.10" 2024-01-16', 17),
        ('java version "1.8.0_292"', 8),
        ('openjdk version "11.0.21"', 11),
        ('not a version line', None),
        ("", None),
    ])
    def test_parse_java_major(self, text, expected):
        assert _parse_java_major(text) == expected

    def test_check_java_reports_version(self, monkeypatch):
        import subprocess
        monkeypatch.delenv("JAVA_HOME", raising=False)
        monkeypatch.setattr("shutil.which", lambda _: "/usr/bin/java")
        monkeypatch.setattr(subprocess, "run", lambda *a, **k: MagicMock(
            stderr='openjdk version "17.0.1"', stdout=""))
        r = check_java()
        assert r.status == "ok" and "Java 17" in r.detail

    def test_check_java_missing_warns(self, monkeypatch):
        monkeypatch.delenv("JAVA_HOME", raising=False)
        monkeypatch.setattr("shutil.which", lambda _: None)
        r = check_java()
        assert r.status == "warn" and "no java found" in r.detail


# ── T27 Part 2: preflight checks (agent ping, UDF import) ───────────────────────

from aqueduct.doctor import _check_udf_registry, check_agent as _check_agent


class _FakeManifest:
    def __init__(self, udf_registry):
        self.udf_registry = udf_registry


class TestUdfImportCheck:
    def test_default_skips_import(self):
        m = _FakeManifest(({"id": "u", "module": "json", "entry": "loads"},))
        assert _check_udf_registry(m, preflight=False) == []   # default: no import

    def test_preflight_imports_ok(self):
        m = _FakeManifest(({"id": "u", "module": "json", "entry": "loads"},))
        res = _check_udf_registry(m, preflight=True)
        assert len(res) == 1 and res[0].status == "ok"

    def test_preflight_missing_module_warns(self):
        m = _FakeManifest(({"id": "u", "module": "no_such_mod_xyz", "entry": "f"},))
        res = _check_udf_registry(m, preflight=True)
        assert res[0].status == "warn" and "cannot import" in res[0].detail

    def test_preflight_missing_entry_warns(self):
        m = _FakeManifest(({"id": "u", "module": "json", "entry": "not_a_func"},))
        res = _check_udf_registry(m, preflight=True)
        assert res[0].status == "warn" and "not found" in res[0].detail

    def test_java_udf_skipped(self):
        m = _FakeManifest(({"id": "u", "lang": "java", "module": "x"},))
        res = _check_udf_registry(m, preflight=True)
        assert res[0].status == "skip"


class TestAgentPreflightPing:
    def test_anthropic_default_no_api_call(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        r = _check_agent("anthropic", "https://api.anthropic.com", "claude-x", preflight=False)
        assert r.status == "ok" and "API not called" in r.detail

    def test_anthropic_preflight_verifies_key(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        import httpx
        monkeypatch.setattr(httpx, "get", lambda *a, **k: MagicMock(raise_for_status=lambda: None))
        r = _check_agent("anthropic", None, "claude-x", preflight=True)
        assert r.status == "ok" and "key verified" in r.detail

    def test_anthropic_preflight_bad_key_warns(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-bad")
        import httpx
        resp = MagicMock(status_code=401)
        def _raise(): raise httpx.HTTPStatusError("401", request=MagicMock(), response=resp)
        monkeypatch.setattr(httpx, "get", lambda *a, **k: MagicMock(raise_for_status=_raise, response=resp))
        r = _check_agent("anthropic", None, "claude-x", preflight=True)
        assert r.status == "warn" and "401" in r.detail


# ── T27 Part 2: store round-trip + JDBC preflight ──────────────────────────────

from types import SimpleNamespace
from aqueduct.doctor import check_store_backend, _jdbc_preflight_auth, _jdbc_result


class TestStoreRoundTrip:
    def test_duckdb_preflight_write_read(self, tmp_path):
        cfg = SimpleNamespace(backend="duckdb", path=str(tmp_path / "obs.db"))
        r = check_store_backend("observability", cfg, preflight=True)
        assert r.status == "ok" and "write+read ok" in r.detail

    def test_duckdb_default_no_roundtrip_note(self, tmp_path):
        cfg = SimpleNamespace(backend="duckdb", path=str(tmp_path / "obs.db"))
        r = check_store_backend("observability", cfg, preflight=False)
        assert r.status == "ok" and "write+read ok" not in r.detail


class TestJdbcPreflight:
    def test_non_postgres_returns_none(self):
        assert _jdbc_preflight_auth("jdbc:mysql://h:3306/db", {}) is None

    def test_result_default_is_tcp_reachable(self):
        r = _jdbc_result("src", "h", 5432, "jdbc:postgresql://h:5432/db", {}, 0.0, preflight=False)
        assert r.status == "ok" and "reachable" in r.detail and "preflight" not in r.detail

    def test_result_preflight_non_postgres_tcp_only(self):
        r = _jdbc_result("src", "h", 3306, "jdbc:mysql://h:3306/db", {}, 0.0, preflight=True)
        assert r.status == "ok" and "TCP only" in r.detail

    def test_result_preflight_postgres_attempts_auth(self):
        # No live DB → connect fails → warn (proves the auth path runs, not TCP-only)
        r = _jdbc_result("src", "127.0.0.1", 5432, "jdbc:postgresql://127.0.0.1:5432/nope",
                         {"user": "u", "password": "p"}, 0.0, preflight=True)
        assert r.status == "warn" and "connect/auth failed" in r.detail
