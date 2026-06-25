# tests/test_cli.py
import json
import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
from pathlib import Path
from unittest.mock import MagicMock
from click.testing import CliRunner
from aqueduct.agent.budget import StopReason
from aqueduct.cli import cli

FIXTURES = Path(__file__).parent.parent / "fixtures"

def test_validate_valid_blueprint():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(FIXTURES / "valid_minimal.yml")])
    assert result.exit_code == 0
    assert "✓" in result.output

def test_validate_invalid_blueprint():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(FIXTURES / "invalid_schema.yml")])
    assert result.exit_code == 1
    assert "✗" in result.output

def test_compile_outputs_json():
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml")])
    assert result.exit_code == 0
    # Find the start of JSON in case of warnings
    json_start = result.output.find('{')
    manifest = json.loads(result.output[json_start:])
    assert manifest["blueprint_id"] == "blueprint.hello.world"
    assert "modules" in manifest
    assert "edges" in manifest

def test_cli_run_writes_depot_last_run_id(tmp_path):
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    # Use absolute path for input to be robust against chdir
    input_fixture = (FIXTURES / "valid_minimal.yml").resolve()
    bp_path.write_text(f"""
aqueduct: '1.0'
id: t1
name: t1
modules:
  - id: in
    type: Ingress
    label: In
    config:
      format: csv
      path: {input_fixture}
edges: []
""")
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"""
stores:
  depot:
    path: "{tmp_path}/depot.db"
""")
    
    # Run once
    res1 = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
    assert res1.exit_code == 0, res1.output
    from aqueduct.depot.depot import DepotStore
    store = DepotStore(tmp_path / "depot.db")
    run_1_id = store.get("t1:_last_run_id")  # per-blueprint isolated key
    assert run_1_id != ""
    store.close()
    
    # Run twice
    res2 = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
    assert res2.exit_code == 0
    
    store = DepotStore(tmp_path / "depot.db")
    run_2_id = store.get("t1:_last_run_id")
    assert run_2_id != ""
    assert run_2_id != run_1_id
    store.close()

def test_compile_execution_date_parsing():
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml"), "--execution-date", "2026-01-15"])
    assert result.exit_code == 0
    assert "blueprint_id" in result.output

def test_compile_invalid_execution_date():
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml"), "--execution-date", "not-a-date"])
    assert result.exit_code != 0
    assert "must be YYYY-MM-DD" in result.output

def test_cli_run_honors_metrics_config(spark, tmp_path):
    """aqueduct run: when metrics.use_observe=false in aqueduct.yml, module_metrics row has NULL records_written"""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    in_path = tmp_path / "in.parquet"
    spark.range(1, 4).selectExpr("id AS a").write.parquet(str(in_path))
    
    bp_path.write_text(f"""
aqueduct: '1.0'
id: test_obs
name: Test Obs
modules:
  - id: in
    type: Ingress
    label: In
    config: {{format: parquet, path: {in_path}}}
  - id: out
    type: Egress
    label: Out
    config: {{format: parquet, path: {tmp_path / "out"}}}
edges:
  - from: in
    to: out
""")
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"""
metrics:
  use_observe: false
stores:
  observability: {{path: {tmp_path / "obs.db"}}}
""")
    
    # We need to ensure the CLI uses this config file.
    # The --config flag is the most direct way.
    result = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    
    # Check DuckDB directly
    import duckdb
    # CLI preserves custom filenames when configured (ISSUE-024)
    db_path = tmp_path / "obs.db"
    assert db_path.exists(), f"Obs DB not found at {db_path}. Files in tmp_path: {list(tmp_path.glob('*'))}"

    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute("SELECT records_written FROM module_metrics WHERE module_id='out'").fetchall()
    finally:
        conn.close()
    
    assert len(rows) > 0
    for (val,) in rows:
        assert val is None

class TestCLICompileShow:
    """Tests for aqueduct compile --show flags."""

    def test_compile_show_manifest_default(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml"), "--show", "manifest"])
        assert result.exit_code == 0
        # Should be valid JSON
        json_start = result.output.find('{')
        manifest = json.loads(result.output[json_start:])
        assert "blueprint_id" in manifest

    def test_compile_show_provenance(self, tmp_path):
        runner = CliRunner()
        bp_path = tmp_path / "bp.yml"
        bp_path.write_text("""
aqueduct: '1.0'
id: prov_test
name: Prov Test
context:
  base_path: /data
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: ${ctx.base_path}/in.parquet
edges: []
""", encoding="utf-8")
        result = runner.invoke(cli, ["compile", str(bp_path), "--show", "provenance"])
        assert result.exit_code == 0
        assert "# Context" in result.output
        assert "base_path" in result.output
        assert "# Module: m1" in result.output
        assert "original_expression" in result.output
        assert "${ctx.base_path}/in.parquet" in result.output

    def test_compile_show_inputs(self, tmp_path):
        runner = CliRunner()
        data_path = tmp_path / "data.csv"
        data_path.write_text("a,b,c\n1,2,3", encoding="utf-8")
        
        bp_path = tmp_path / "bp.yml"
        bp_path.write_text(f"""
aqueduct: '1.0'
id: input_test
name: Input Test
modules:
  - id: in1
    type: Ingress
    label: In1
    config: {{format: csv, path: {data_path}}}
edges: []
""", encoding="utf-8")
        result = runner.invoke(cli, ["compile", str(bp_path), "--show", "inputs"])
        assert result.exit_code == 0
        assert "module_id" in result.output
        assert "path" in result.output
        assert "in1" in result.output
        assert str(data_path) in result.output
        assert "B" in result.output

    def test_compile_show_all(self, tmp_path):
        runner = CliRunner()
        bp_path = tmp_path / "bp.yml"
        bp_path.write_text("""
aqueduct: '1.0'
id: all_test
name: All Test
modules:
  - id: in
    type: Ingress
    label: In
    config: {format: parquet, path: /tmp/in}
edges: []
""", encoding="utf-8")
        result = runner.invoke(cli, ["compile", str(bp_path), "--show", "all"])
        assert result.exit_code == 0
        assert '"blueprint_id": "all_test"' in result.output
        assert "── Provenance ──" in result.output
        assert "── Inputs fingerprint ──" in result.output

    def test_compile_show_invalid_choice(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml"), "--show", "xml"])
        assert result.exit_code != 0
        assert "Invalid value for '--show'" in result.output


def test_lineage_reads_from_observability_db(tmp_path):
    """aqueduct lineage reads from observability.db (not lineage.db)."""
    import duckdb
    runner = CliRunner()
    bp_id = "lineage_test"

    # Create a minimal aqueduct config that points to the observability store
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"""
aqueduct_config: "1.0"
stores:
  observability:
    backend: duckdb
    path: "{tmp_path / '.aqueduct' / 'observability' / bp_id / 'observability.db'}"
""")

    # Create observability.db with column_lineage data
    obs_dir = tmp_path / ".aqueduct" / "observability" / bp_id
    obs_dir.mkdir(parents=True)
    db_path = obs_dir / "observability.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE column_lineage (
            blueprint_id VARCHAR,
            channel_id VARCHAR,
            output_column VARCHAR,
            source_table VARCHAR,
            source_column VARCHAR
        )
    """)
    conn.execute(
        "INSERT INTO column_lineage VALUES (?, ?, ?, ?, ?)",
        [bp_id, "ch1", "user_id", "users", "id"],
    )
    conn.close()

    # Ensure legacy lineage.db does NOT exist
    legacy_lineage = tmp_path / ".aqueduct" / "lineage.db"
    assert not legacy_lineage.exists()

    result = runner.invoke(cli, [
        "lineage", bp_id,
        "--config", str(config_path),
    ])
    # Should exit 0 and find the data
    assert result.exit_code == 0, result.output
    assert "ch1" in result.output
    assert "user_id" in result.output
    assert "users" in result.output


def test_cli_run_postgres_observability_no_bogus_dir(tmp_path):
    """Verify that using postgres as the stores.observability.backend does NOT create a postgresql:/... directory.
    It should fall back to a safe per-pipeline local path (.aqueduct/observability/<blueprint_id>) for scratch work."""
    pytest.importorskip("psycopg2")
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    input_fixture = (FIXTURES / "valid_minimal.yml").resolve()
    bp_path.write_text(f"""
aqueduct: '1.0'
id: postgres_store_test
name: Postgres Store Test
modules:
  - id: in
    type: Ingress
    label: In
    config:
      format: csv
      path: {input_fixture}
edges: []
""")
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("""
stores:
  observability:
    backend: postgres
    path: "postgresql://aqueduct:aqueduct@127.0.0.1:5432/aqueduct_db"
  lineage:
    backend: postgres
    path: "postgresql://aqueduct:aqueduct@127.0.0.1:5432/aqueduct_db"
  depot:
    backend: postgres
    path: "postgresql://aqueduct:aqueduct@127.0.0.1:5432/aqueduct_db"
""")
    
    from unittest.mock import patch, MagicMock
    
    mock_bundle = MagicMock()
    mock_bundle.depot.backend = "postgres"
    
    with patch("aqueduct.stores.get_stores", return_value=mock_bundle), \
         patch("aqueduct.executor.get_executor") as mock_get_executor:
        
        mock_executor = MagicMock()
        mock_executor.return_value.status = "success"
        mock_get_executor.return_value = mock_executor
        
        result = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
        
        # Verify execution succeeded (exit 0)
        assert result.exit_code == 0, result.output
        
        # Check that no directory starting with "postgresql:" exists
        # in either CWD or tmp_path
        bogus_cwd = [str(p) for p in Path(".").glob("postgresql:*")]
        bogus_tmp = [str(p) for p in tmp_path.glob("postgresql:*")]
        assert len(bogus_cwd) == 0, f"Bogus directory created in CWD: {bogus_cwd}"
        assert len(bogus_tmp) == 0, f"Bogus directory created in tmp_path: {bogus_tmp}"
        
        fallback_dir = tmp_path / ".aqueduct/observability/postgres_store_test"
        assert fallback_dir.exists(), "Local scratch directory was not created under tmp_path/.aqueduct"


# ── Phase 34 CLI Integration ──────────────────────────────────────────────────

class TestPhase34CLI:
    def test_run_self_heal_invokes_on_attempt_callback(self, tmp_path):
        """Phase 34 Task 88: aqueduct run (self-heal) passes on_attempt hook to generator."""
        runner = CliRunner()
        bp_path = tmp_path / "bp.yml"
        bp_path.write_text(f"""
aqueduct: '1.0'
id: heal_hook
name: heal_hook
modules:
  - id: m1
    type: Ingress
    label: M1
    config: {{format: csv, path: /missing.csv}}
edges: []
agent:
  approval: human
""")
        config_path = tmp_path / "aq.yml"
        config_path.write_text(f"""
aqueduct_config: "1.0"
stores:
  observability:
    path: {tmp_path / 'obs.db'}
agent:
  provider: anthropic
  model: claude-3
  base_url: https://api.anthropic.example
""")
        import os
        os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
        
        from unittest.mock import patch
        
        with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
            # Need to fake result so CLI succeeds after heal
            from aqueduct.agent import AgentPatchResult
            from aqueduct.patch.grammar import PatchSpec
            mock_gap.return_value = AgentPatchResult(
                patch=PatchSpec(patch_id="p1", rationale="r", operations=[{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]),
                attempts=1, stop_reason=StopReason.SOLVED,
            )
            # Patch Executor to fail first, then succeed
            with patch("aqueduct.executor.get_executor") as mock_get_exec:
                from aqueduct.executor.models import ExecutionResult, ModuleResult
                mock_exec = MagicMock()
                mock_exec.side_effect = [
                    ExecutionResult(
                        blueprint_id="heal_hook", run_id="r1", status="error",
                        module_results=(ModuleResult("m1", "error", error="err", error_type="E"),),
                    ),
                    ExecutionResult(
                        blueprint_id="heal_hook", run_id="r1", status="success",
                        module_results=(ModuleResult("m1", "success"),),
                    ),
                ]
                mock_get_exec.return_value = mock_exec
                
                runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
                
                assert mock_gap.called
                agent_cfg = mock_gap.call_args[1]["agent_cfg"]
                assert agent_cfg.on_attempt is not None
                assert callable(agent_cfg.on_attempt)

    def test_heal_command_prints_stop_reason_and_usage(self, tmp_path):
        """Phase 34: aqueduct heal prints Phase 34 budget outputs."""
        runner = CliRunner()
        bp_path = tmp_path / "bp.yml"
        bp_path.write_text(f"""
aqueduct: '1.0'
id: heal_cli
name: heal_cli
modules: []
edges: []
""")
        # Surveyor writes to <store_dir>/observability.db. Config must point
        # at the SAME file (not a sibling) so `_resolve_obs_db` finds it.
        config_path = tmp_path / "aq.yml"
        config_path.write_text(
            f"agent: {{model: claude-3}}\n"
            f"stores: {{observability: {{path: {tmp_path / 'observability.db'}}}}}\n"
        )

        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        s = Surveyor(
            Manifest(
                blueprint_id="heal_cli", name="heal_cli",
                context={}, modules=(), edges=(), spark_config={},
            ),
            tmp_path,
        )
        s.start("run1")

        from aqueduct.executor.models import ExecutionResult, ModuleResult
        s.record(
            ExecutionResult(
                blueprint_id="heal_cli", run_id="run1", status="error",
                module_results=(ModuleResult("m1", "error", error="fail"),),
            ),
            exc=Exception("fail"),
        )

        from unittest.mock import patch
        with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
            from aqueduct.agent import AgentPatchResult
            mock_gap.return_value = AgentPatchResult(
                patch=None, attempts=2, stop_reason=StopReason.STUCK_SIGNATURE,
                tokens_in_total=100, tokens_out_total=200, escalated=True,
            )

            # heal takes a run_id positional; we recorded the failure under "run1"
            res = runner.invoke(cli, ["heal", "run1", "--config", str(config_path)])

            assert StopReason.STUCK_SIGNATURE in res.output
            assert "2 attempt" in res.output  # CLI prints "after N attempt(s)"

    def test_benchmark_command_wires_budget(self, tmp_path):
        """Phase 34: aqueduct benchmark parses budget from aqueduct.yml and passes it down."""
        runner = CliRunner()
        scen_path = tmp_path / "scen.yml"
        scen_path.write_text("id: s1\nengine_version: 1.0\nexpected_patch: {}\n")
        
        config_path = tmp_path / "aq.yml"
        config_path.write_text("""
agent:
  budget:
    max_reprompts: 9
    max_seconds: 300.0
stores:
  observability: {path: /tmp/obs}
""")
        
        from unittest.mock import patch
        with patch("aqueduct.surveyor.scenario.run_benchmark") as mock_rb:
            mock_rb.return_value = {}
            runner.invoke(cli, ["benchmark", str(scen_path), "--config", str(config_path)])

            assert mock_rb.called
            kwargs = mock_rb.call_args[1]
            assert "budget" in kwargs
            b = kwargs["budget"]
            assert b.max_reprompts == 9
            assert b.max_seconds == 300.0

    def test_benchmark_reads_agent_budget_from_config(self, tmp_path):
        """Phase 34: budget.max_reprompts from aqueduct.yml reaches run_benchmark."""
        from aqueduct.agent.budget import BudgetConfig

        runner = CliRunner()
        scen_path = tmp_path / "scen.yml"
        scen_path.write_text("id: s1\nengine_version: 1.0\nexpected_patch: {}\n")

        config_path = tmp_path / "aq.yml"
        config_path.write_text(
            "agent:\n"
            "  budget:\n"
            "    max_reprompts: 6\n"
            "stores:\n"
            "  observability: {path: /tmp/obs}\n"
        )

        from unittest.mock import patch
        with patch("aqueduct.surveyor.scenario.run_benchmark") as mock_rb:
            mock_rb.return_value = {}
            runner.invoke(
                cli,
                ["benchmark", str(scen_path),
                 "--model", "X", "--config", str(config_path)],
            )
            assert mock_rb.called
            kwargs = mock_rb.call_args[1]
            assert "budget" in kwargs
            b = kwargs["budget"]
            assert isinstance(b, BudgetConfig)
            assert b.max_reprompts == 6
