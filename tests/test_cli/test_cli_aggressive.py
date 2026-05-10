import json
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.patch.grammar import PatchSpec

pytestmark = [pytest.mark.spark, pytest.mark.integration]

@pytest.fixture
def base_blueprint(tmp_path):
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: test_bp
name: Test Blueprint
agent:
  approval_mode: aggressive
  aggressive_max_patches: 2
modules:
  - id: in
    type: Ingress
    label: Input
    config: { format: csv, path: input.csv }
edges: []
""")
    # Also create a dummy input file to avoid initial parse/compile failure
    (tmp_path / "input.csv").write_text("a,b\n1,2")
    return bp_path

@pytest.fixture
def mock_llm_patch():
    def _make_patch(patch_id="fix-1", rationale="test", ops=None):
        return PatchSpec(
            patch_id=patch_id,
            rationale=rationale,
            confidence=0.9,
            category="other",
            root_cause="test",
            operations=ops or [{"op": "replace_module_label", "module_id": "in", "label": "New Label"}]
        )
    return _make_patch

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_aggressive_mode_invalid_patch_stops_loop(
    mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, mock_llm_patch
):
    """
    Scenario: aggressive mode + patch produces invalid Blueprint (compile fail) -> loop stops.
    """
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    # 1. First run fails
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="Boom")]),
    ]
    
    # 2. LLM returns a patch that will fail compilation (e.g. duplicate module id)
    # Actually, we can just mock _apply_patch_in_memory to return None
    invalid_patch = mock_llm_patch(patch_id="invalid-fix")
    mock_gen_patch.return_value = MagicMock(patch=invalid_patch)
    
    with patch("aqueduct.cli._apply_patch_in_memory", return_value=None):
        result = runner.invoke(cli, ["run", str(base_blueprint), "--allow-aggressive"])
    
    assert "✗ LLM patch produces invalid Blueprint, discarding" in result.output
    # Loop should have stopped after the first invalid patch
    assert mock_gen_patch.call_count == 1
    # Blueprint should NOT have been updated (check its content)
    assert "New Label" not in base_blueprint.read_text()

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_aggressive_mode_fails_then_continues(
    mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, mock_llm_patch
):
    """
    Scenario: aggressive mode + patch valid but re-run fails -> on_heal_failure applied (staged), loop continues.
    """
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    # 1. Sequence of results: Error (Initial) -> Error (Patch 1) -> Error (Patch 2)
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="E1")]),
        ExecutionResult(blueprint_id="test_bp", run_id="r2", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="E2")]),
        ExecutionResult(blueprint_id="test_bp", run_id="r3", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="E3")]),
        ExecutionResult(blueprint_id="test_bp", run_id="r4", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="E4")]),
        ExecutionResult(blueprint_id="test_bp", run_id="r5", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="E5")]),
        ExecutionResult(blueprint_id="test_bp", run_id="r6", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="E6")]),
    ]
    
    # 2. LLM returns two different patches
    mock_gen_patch.side_effect = [
        MagicMock(patch=mock_llm_patch(patch_id="fix-1")),
        MagicMock(patch=mock_llm_patch(patch_id="fix-2")),
        MagicMock(patch=None),
        MagicMock(patch=None),
    ]
    
    # We need to mock _apply_patch_in_memory to return a valid manifest (mock it as MagicMock)
    # And we mock _stage_failed_patch to verify it's called
    with patch("aqueduct.cli._apply_patch_in_memory", return_value=MagicMock()), \
         patch("aqueduct.cli._stage_failed_patch") as mock_stage:
        result = runner.invoke(cli, ["run", str(base_blueprint), "--allow-aggressive"])
    
    assert "✗ LLM patch did not fix the issue (1/2)" in result.output
    assert "✗ LLM patch did not fix the issue (2/2)" in result.output
    assert "⚠  LLM: aggressive_max_patches=2 reached" in result.output
    
    assert mock_gen_patch.call_count == 2
    assert mock_stage.call_count == 2
    # Blueprint should NOT have been updated permanently
    assert "New Label" not in base_blueprint.read_text()

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_aggressive_mode_succeeds_stops_loop(
    mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, mock_llm_patch
):
    """
    Scenario: aggressive mode + patch valid + re-run succeeds -> Blueprint written to disk, loop stops.
    """
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    # 1. Error -> Success
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="Boom")]),
        ExecutionResult(blueprint_id="test_bp", run_id="r2", status="success", module_results=[]),
    ]
    
    patch_obj = mock_llm_patch(patch_id="good-fix")
    mock_gen_patch.return_value = MagicMock(patch=patch_obj)
    
    with patch("aqueduct.cli._apply_patch_in_memory", return_value=MagicMock()), \
         patch("aqueduct.cli._write_patch_to_blueprint") as mock_write:
        result = runner.invoke(cli, ["run", str(base_blueprint), "--allow-aggressive"])
    
    assert "✓ LLM patch validated and applied (1/2)" in result.output
    assert mock_gen_patch.call_count == 1
    assert mock_write.call_count == 1

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_trigger_agent_escalation(
    mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint, mock_llm_patch
):
    """
    Scenario: result.trigger_agent=True + approval_mode=disabled -> effective_mode set to "human".
    """
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    # Update blueprint to disabled
    base_blueprint.write_text(base_blueprint.read_text().replace("approval_mode: aggressive", "approval_mode: disabled"))
    
    # 1. First run fails but triggers agent
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=(ModuleResult(module_id="in", status="error", error="Boom"),),
                        trigger_agent=True),
    ]
    
    mock_gen_patch.return_value = MagicMock(patch=mock_llm_patch())
    
    with patch("aqueduct.surveyor.llm.stage_patch_for_human") as mock_stage:
        result = runner.invoke(cli, ["run", str(base_blueprint)])
    
    assert "LLM triggered by module rule (overriding approval_mode=disabled → staging patch for review)" in result.output
    assert "✎ LLM patch staged" in result.output
    assert mock_stage.call_count == 1

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_trigger_agent_false_disabled_breaks(
    mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint
):
    """
    Scenario: result.trigger_agent=False + approval_mode=disabled -> loop breaks immediately.
    """
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    base_blueprint.write_text(base_blueprint.read_text().replace("approval_mode: aggressive", "approval_mode: disabled"))
    
    mock_exec.return_value = ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", module_results=(), trigger_agent=False)
    
    result = runner.invoke(cli, ["run", str(base_blueprint)])
    
    assert "LLM self-healing" not in result.output
    assert mock_gen_patch.call_count == 0

@patch("aqueduct.config.load_config")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_block_full_actions_propagation(
    mock_surveyor_cls, mock_get_executor, mock_load_config, base_blueprint
):
    """
    Scenario: cfg.danger.allow_full_probe_actions=False -> block_full_actions=True passed to execute().
    """
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    from aqueduct.config import AqueductConfig, DangerConfig, DeploymentConfig, StoresConfig, StoreBackendConfig
    mock_cfg = AqueductConfig(
        danger=DangerConfig(
            allow_full_probe_actions=False,
            allow_aggressive_patching=True
        ),
        deployment=DeploymentConfig(engine="spark", master_url="local[*]"),
        stores=StoresConfig(
            obs=StoreBackendConfig(path=".aqueduct/obs.db"),
            lineage=StoreBackendConfig(path=".aqueduct/lineage.db"),
            depot=StoreBackendConfig(path=".aqueduct/depot.db")
        ),
        spark_config={}
    )
    
    mock_load_config.return_value = mock_cfg
    
    mock_exec.return_value = ExecutionResult(blueprint_id="test_bp", run_id="r1", status="success", module_results=())
    
    runner.invoke(cli, ["run", str(base_blueprint)])
    
    # Check that mock_exec (which is 'execute') was called with block_full_actions=True
    args, kwargs = mock_exec.call_args
    assert kwargs["block_full_actions"] is True
    # Verification of loop stopping is implicit in call counts
@patch("aqueduct.surveyor.llm.generate_llm_patch")
@patch("aqueduct.executor.get_executor")
def test_trigger_agent_stays_human(
    mock_get_executor, mock_gen_patch, base_blueprint, mock_llm_patch
):
    """trigger_agent=True + approval_mode=human -> stays human (no override message)"""
    runner = CliRunner()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    
    # Set to human
    base_blueprint.write_text(base_blueprint.read_text().replace("approval_mode: aggressive", "approval_mode: human"))
    
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=(ModuleResult(module_id="in", status="error", error="Boom"),),
                        trigger_agent=True),
        ExecutionResult(blueprint_id="test_bp", run_id="r2", status="success", module_results=()),
    ]
    
    from aqueduct.surveyor.llm import LLMPatchResult
    mock_gen_patch.return_value = LLMPatchResult(patch=mock_llm_patch(), attempts=1)
    
    with patch("aqueduct.surveyor.llm.stage_patch_for_human") as mock_stage:
        result = runner.invoke(cli, ["run", str(base_blueprint)])
    
    # Should NOT have the override message
    assert "overriding approval_mode=disabled" not in result.output
    assert "✎ LLM patch staged" in result.output
    assert mock_stage.call_count == 1
