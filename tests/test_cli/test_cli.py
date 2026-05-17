# tests/test_cli.py
import json
import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
from pathlib import Path
from click.testing import CliRunner
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
    run_1_id = store.get("_last_run_id")
    assert run_1_id != ""
    store.close()
    
    # Run twice
    res2 = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
    assert res2.exit_code == 0
    
    store = DepotStore(tmp_path / "depot.db")
    run_2_id = store.get("_last_run_id")
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


def test_cli_run_postgres_observability_no_bogus_dir(tmp_path):
    """Verify that using postgres as the stores.observability.backend does NOT create a postgresql:/... directory.
    It should fall back to a safe per-pipeline local path (.aqueduct/observability/<blueprint_id>) for scratch work."""
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

