import pytest
from unittest.mock import MagicMock, patch
from click.testing import CliRunner
from pathlib import Path

from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.patch.grammar import PatchSpec
from aqueduct.patch.preview import Gate2Result, Gate3Result

pytestmark = [pytest.mark.spark, pytest.mark.integration]

@pytest.fixture
def base_blueprint(tmp_path):
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: test_bp
name: Test Blueprint
agent:
  approval_mode: auto
modules:
  - id: in
    type: Ingress
    label: Input
    config: { format: csv, path: input.csv }
edges: []
""")
    (tmp_path / "input.csv").write_text("a,b\n1,2")
    return bp_path

@pytest.fixture
def configs(tmp_path):
    # Sandbox config
    sb_path = tmp_path / "aqueduct_sandbox.yml"
    sb_path.write_text("""
agent: { patch_validation: sandbox }
stores:
  obs: { path: .aqueduct/obs.db }
  lineage: { path: .aqueduct/lineage.db }
  depot: { path: .aqueduct/depot.db }
deployment: { engine: spark, target: local }
""")
    # Full run config
    fr_path = tmp_path / "aqueduct_full.yml"
    fr_path.write_text("""
agent: { patch_validation: full_run }
stores:
  obs: { path: .aqueduct/obs.db }
  lineage: { path: .aqueduct/lineage.db }
  depot: { path: .aqueduct/depot.db }
deployment: { engine: spark, target: local }
""")
    return {"sandbox": str(sb_path), "full": str(fr_path)}

@pytest.fixture
def mock_llm_patch():
    def _make_patch(patch_id="fix-1"):
        return PatchSpec(
            patch_id=patch_id,
            rationale="test",
            confidence=0.9,
            category="other",
            root_cause="test",
            operations=[{"op": "replace_module_label", "module_id": "in", "label": "New Label"}]
        )
    return _make_patch

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._run_patch_gates_inline")
@patch("aqueduct.cli._llm_usable", return_value=True)
def test_auto_gate3_pass_full_run(
    mock_llm_ok, mock_gates, mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, configs, mock_llm_patch
):
    """auto mode + Gate 3 pass + patch_validation=full_run -> full Spark run is executed"""
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="Boom")]),
        ExecutionResult(blueprint_id="test_bp", run_id="r2", status="success", module_results=[]),
    ]
    
    mock_surveyor = mock_surveyor_cls.return_value
    mock_failure_ctx = MagicMock()
    mock_failure_ctx.failed_module = "in"
    mock_surveyor.record.return_value = mock_failure_ctx
    
    mock_res = MagicMock()
    mock_res.patch = mock_llm_patch()
    mock_gen_patch.return_value = mock_res
    
    mock_gates.return_value = (
        Gate2Result(status="pass", duration_ms=10, warnings=[]),
        Gate3Result(status="pass", duration_ms=50, sample_rows=1000, detail="ok"),
        True
    )
    
    # Setup record_patch_simulation mock
    mock_surveyor.record_patch_simulation = MagicMock()
    
    # Actually _run_patch_gates_inline is patched, so it WON'T call record_patch_simulation!
    # I need to un-patch _run_patch_gates_inline if I want to test simulation recording.
    # OR I just test _run_patch_gates_inline directly.
    
    with patch("aqueduct.cli._apply_patch_in_memory", return_value=MagicMock()) as mock_apply,          patch("aqueduct.cli._write_patch_to_blueprint") as mock_write:
        result = runner.invoke(cli, ["run", str(base_blueprint), "--config", configs["full"]])
    
    assert "✓ LLM patch validated and applied" in result.output
    assert mock_exec.call_count == 2
    assert mock_apply.call_count == 1

def test_run_patch_gates_inline_records_simulation():
    """Each gate evaluation writes one row to obs.patch_simulation"""
    from aqueduct.cli import _run_patch_gates_inline
    from aqueduct.patch.preview import Gate2Result, Gate3Result
    
    mock_patch = MagicMock(patch_id="P1")
    mock_surveyor = MagicMock()
    mock_bundle = MagicMock()
    
    with patch("aqueduct.patch.apply._yaml_load"),          patch("aqueduct.patch.apply.apply_patch_to_dict"),          patch("aqueduct.patch.preview.run_gate2_lineage") as mock_g2,          patch("aqueduct.patch.preview.run_gate3_sandbox") as mock_g3:
        
        mock_g2.return_value = Gate2Result(status="pass", duration_ms=10)
        mock_g3.return_value = Gate3Result(status="pass", duration_ms=50, sample_rows=100, detail="ok")
        
        _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=Path("bp.yml"),
            bundle=mock_bundle,
            surveyor=mock_surveyor,
            failed_module="in",
            current_run_id="r1",
            blueprint_id="b1"
        )
    
    assert mock_surveyor.record_patch_simulation.call_count == 2
    calls = mock_surveyor.record_patch_simulation.call_args_list
    assert calls[0].kwargs["gate"] == "gate2"
    assert calls[1].kwargs["gate"] == "gate3"
    assert calls[1].kwargs["status"] == "pass"

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._run_patch_gates_inline")
@patch("aqueduct.cli._llm_usable", return_value=True)
def test_auto_gate3_pass_sandbox_skips_full_run(
    mock_llm_ok, mock_gates, mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, configs, mock_llm_patch
):
    """auto mode + Gate 3 pass + patch_validation=sandbox -> full Spark run is SKIPPED"""
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="Boom")]),
    ]
    
    mock_surveyor = mock_surveyor_cls.return_value
    mock_failure_ctx = MagicMock()
    mock_failure_ctx.failed_module = "in"
    mock_surveyor.record.return_value = mock_failure_ctx
    
    mock_res = MagicMock()
    mock_res.patch = mock_llm_patch()
    mock_gen_patch.return_value = mock_res
    
    mock_gates.return_value = (
        Gate2Result(status="pass", duration_ms=10, warnings=[]),
        Gate3Result(status="pass", duration_ms=50, sample_rows=100, detail="ok"),
        True
    )
    
    with patch("aqueduct.cli._apply_patch_in_memory") as mock_apply,          patch("aqueduct.cli._write_patch_to_blueprint") as mock_write:
        result = runner.invoke(cli, ["run", str(base_blueprint), "--config", configs["sandbox"]])
    
    assert "✓ LLM patch validated via sandbox-only (100 rows)" in result.output
    assert mock_exec.call_count == 1
    assert mock_apply.call_count == 0

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._run_patch_gates_inline")
@patch("aqueduct.cli._llm_usable", return_value=True)
def test_auto_gate3_fail_stages(
    mock_llm_ok, mock_gates, mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, configs, mock_llm_patch
):
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    mock_exec.return_value = ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                                             module_results=[ModuleResult(module_id="in", status="error", error="Boom")])
    
    mock_surveyor = mock_surveyor_cls.return_value
    mock_failure_ctx = MagicMock()
    mock_failure_ctx.failed_module = "in"
    mock_surveyor.record.return_value = mock_failure_ctx
    
    mock_res = MagicMock()
    mock_res.patch = mock_llm_patch()
    mock_gen_patch.return_value = mock_res
    
    mock_gates.return_value = (
        Gate2Result(status="pass", duration_ms=10, warnings=[]),
        Gate3Result(status="fail", duration_ms=50, sample_rows=1000, detail="Sandbox fail"),
        False
    )
    
    with patch("aqueduct.cli._stage_failed_patch") as mock_stage:
        result = runner.invoke(cli, ["run", str(base_blueprint), "--config", configs["sandbox"]])
    
    assert "✗ LLM patch failed sandbox replay: Sandbox fail" in result.output
    assert mock_stage.call_count == 1

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._run_patch_gates_inline")
@patch("aqueduct.cli._llm_usable", return_value=True)
def test_aggressive_gate3_fail_continues(
    mock_llm_ok, mock_gates, mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, configs, mock_llm_patch
):
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    mock_exec.return_value = ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                                             module_results=[ModuleResult(module_id="in", status="error", error="Boom")])
    
    base_blueprint.write_text(base_blueprint.read_text().replace("approval_mode: auto", "approval_mode: aggressive\n  aggressive_max_patches: 2"))
    
    mock_surveyor = mock_surveyor_cls.return_value
    mock_failure_ctx = MagicMock()
    mock_failure_ctx.failed_module = "in"
    mock_surveyor.record.return_value = mock_failure_ctx
    
    mock_res1 = MagicMock()
    mock_res1.patch = mock_llm_patch("fix-1")
    mock_res2 = MagicMock()
    mock_res2.patch = None
    mock_gen_patch.side_effect = [mock_res1, mock_res2]
    
    mock_gates.return_value = (
        Gate2Result(status="pass", duration_ms=10, warnings=[]),
        Gate3Result(status="fail", duration_ms=50, sample_rows=1000, detail="Sandbox fail"),
        False
    )
    
    result = runner.invoke(cli, ["run", str(base_blueprint), "--config", configs["sandbox"], "--allow-aggressive"])
    
    assert "✗ aggressive: sandbox rejected patch — Sandbox fail" in result.output
    assert mock_gen_patch.call_count == 2

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._run_patch_gates_inline")
@patch("aqueduct.cli._llm_usable", return_value=True)
def test_aggressive_gate3_pass_sandbox_skips_full_run(
    mock_llm_ok, mock_gates, mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, configs, mock_llm_patch
):
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    mock_exec.return_value = ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                                             module_results=[ModuleResult(module_id="in", status="error", error="Boom")])
    
    base_blueprint.write_text(base_blueprint.read_text().replace("approval_mode: auto", "approval_mode: aggressive"))
    
    mock_surveyor = mock_surveyor_cls.return_value
    mock_failure_ctx = MagicMock()
    mock_failure_ctx.failed_module = "in"
    mock_surveyor.record.return_value = mock_failure_ctx
    
    mock_res = MagicMock()
    mock_res.patch = mock_llm_patch()
    mock_gen_patch.return_value = mock_res
    
    mock_gates.return_value = (
        Gate2Result(status="pass", duration_ms=10, warnings=[]),
        Gate3Result(status="pass", duration_ms=50, sample_rows=100, detail="ok"),
        True
    )
    
    with patch("aqueduct.cli._write_patch_to_blueprint") as mock_write:
        result = runner.invoke(cli, ["run", str(base_blueprint), "--config", configs["sandbox"], "--allow-aggressive"])
    
    assert "✓ aggressive: sandbox-only validated" in result.output
    assert mock_write.call_count == 1
