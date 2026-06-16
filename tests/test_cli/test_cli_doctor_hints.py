import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.doctor import CheckResult

pytestmark = [pytest.mark.integration]

@pytest.fixture
def base_blueprint(tmp_path):
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: test_bp
name: Test Blueprint
agent:
  approval_mode: aggressive
  aggressive_max_patches: 1
modules:
  - id: in
    type: Ingress
    label: Input
    config: { format: csv, path: input.csv }
edges: []
""")
    (tmp_path / "input.csv").write_text("a,b\n1,2")
    return bp_path

@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.doctor.check_blueprint_sources_from_manifest")
def test_doctor_warn_adds_hints_before_agent(
    mock_check, mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint
):
    """blueprint has warn doctor result -> failure_ctx.doctor_hints non-empty before Agent call."""
    runner = CliRunner()
    patch("aqueduct.cli._agent_usable", return_value=True).start()
    
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="Boom")]),
    ]
    
    from aqueduct.surveyor.models import FailureContext
    mock_surveyor_instance = MagicMock()
    mock_surveyor_instance.record.return_value = FailureContext(
        run_id="r1",
        failed_module="in",
        error_message="Boom",
        blueprint_id="test_bp",
        stack_trace="",
        manifest_json="{}",
        started_at="2026-05-11T00:00:00Z",
        finished_at="2026-05-11T00:00:00Z",
        blueprint_source_yaml="id: test_bp",
        doctor_hints=[]
    )
    mock_surveyor_cls.return_value = mock_surveyor_instance
    # Prevent MagicMock from poisoning find_pending/find_replay_candidate
    mock_surveyor_instance.observability = None
    mock_surveyor_instance.patch_store.return_value = None
    
    # Mock doctor to return a warning
    mock_check.return_value = [
        CheckResult("ingress:in", "warn", "format='csv' but file extension suggests different format")
    ]
    
    mock_gen_patch.return_value = MagicMock(patch=None)  # stop loop
    
    result = runner.invoke(cli, ["run", str(base_blueprint), "--allow-aggressive"])
    
    # Verify doctor_hints in failure context
    assert mock_gen_patch.call_count == 1
    call_args = mock_gen_patch.call_args[0]
    failure_ctx = call_args[0]
    
    assert failure_ctx.doctor_hints is not None
    assert len(failure_ctx.doctor_hints) == 1
    assert "format='csv'" in failure_ctx.doctor_hints[0]


@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.doctor.check_blueprint_sources_from_manifest")
def test_doctor_exception_swallowed_hints_empty(
    mock_check, mock_surveyor_cls, mock_gen_patch, mock_get_executor, base_blueprint
):
    """doctor check throws exception -> exception swallowed; doctor_hints stays empty; self-healing continues."""
    runner = CliRunner()
    patch("aqueduct.cli._agent_usable", return_value=True).start()
    
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    mock_exec.side_effect = [
        ExecutionResult(blueprint_id="test_bp", run_id="r1", status="error", 
                        module_results=[ModuleResult(module_id="in", status="error", error="Boom")]),
    ]
    
    from aqueduct.surveyor.models import FailureContext
    mock_surveyor_instance = MagicMock()
    mock_surveyor_instance.record.return_value = FailureContext(
        run_id="r1",
        failed_module="in",
        error_message="Boom",
        blueprint_id="test_bp",
        stack_trace="",
        manifest_json="{}",
        started_at="2026-05-11T00:00:00Z",
        finished_at="2026-05-11T00:00:00Z",
        blueprint_source_yaml="id: test_bp",
        doctor_hints=[]
    )
    mock_surveyor_cls.return_value = mock_surveyor_instance
    mock_surveyor_instance.observability = None
    mock_surveyor_instance.patch_store.return_value = None
    
    # Mock doctor to throw exception
    mock_check.side_effect = Exception("Doctor crashed")
    
    mock_gen_patch.return_value = MagicMock(patch=None)  # stop loop
    
    result = runner.invoke(cli, ["run", str(base_blueprint), "--allow-aggressive"])
    
    # Verify doctor_hints in failure context
    assert mock_gen_patch.call_count == 1
    call_args = mock_gen_patch.call_args[0]
    failure_ctx = call_args[0]
    
    # Hints should be empty, but self-healing continued
    assert failure_ctx.doctor_hints == []
