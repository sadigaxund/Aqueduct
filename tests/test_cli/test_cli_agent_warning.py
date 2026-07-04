"""Test upfront agent-unreachable warning gates on agent.approval, not agent.model."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.surveyor.models import FailureContext

pytestmark = pytest.mark.unit


@pytest.fixture
def no_agent_bp(tmp_path):
    p = tmp_path / "blueprint.yml"
    p.write_text("""\
aqueduct: '1.0'
id: test_bp
name: Test Blueprint
modules:
  - id: in
    type: Ingress
    label: Input
    config: { format: csv, path: input.csv }
edges: []
""")
    (tmp_path / "input.csv").write_text("a,b\n1,2")
    return p


@pytest.fixture
def auto_agent_bp(tmp_path):
    p = tmp_path / "blueprint.yml"
    p.write_text("""\
aqueduct: '1.0'
id: test_bp
name: Test Blueprint
agent:
  approval: auto
modules:
  - id: in
    type: Ingress
    label: Input
    config: { format: csv, path: input.csv }
edges: []
""")
    (tmp_path / "input.csv").write_text("a,b\n1,2")
    return p


def _mock_run(mock_get_executor, mock_gen_patch, mock_surveyor_cls):
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    mock_exec.side_effect = [
        ExecutionResult(
            blueprint_id="test_bp", run_id="r1", status="error",
            module_results=[ModuleResult(module_id="in", status="error", error="Boom")],
        ),
    ]

    inst = MagicMock()
    inst.record.return_value = FailureContext(
        run_id="r1", failed_module="in", error_message="Boom",
        blueprint_id="test_bp", stack_trace="", manifest_json="{}",
        started_at="2026-05-11T00:00:00Z", finished_at="2026-05-11T00:00:00Z",
        blueprint_source_yaml="id: test_bp", doctor_hints=[],
    )
    inst.observability = None
    inst.patch_store.return_value = None
    mock_surveyor_cls.return_value = inst

    mock_gen_patch.return_value = MagicMock(patch=None)


@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_no_warning_when_agent_disabled(
    mock_surveyor_cls, mock_gen_patch, mock_get_executor, no_agent_bp
):
    """Blueprint with NO agent block (approval=disabled) + unreachable agent prints NO warning."""
    _mock_run(mock_get_executor, mock_gen_patch, mock_surveyor_cls)
    runner = CliRunner()

    with patch("aqueduct.cli._agent_usable", return_value=False):
        with patch("aqueduct.cli._agent_usable_with_cascade", return_value=False):
            result = runner.invoke(cli, ["run", str(no_agent_bp), "--allow-multi-patch"])

    assert "self-healing is enabled" not in result.output
    assert "agent is not reachable" not in result.output


@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
def test_warning_when_agent_auto_and_unreachable(
    mock_surveyor_cls, mock_gen_patch, mock_get_executor, auto_agent_bp
):
    """Blueprint with agent.approval=auto + unreachable agent prints expected warning."""
    _mock_run(mock_get_executor, mock_gen_patch, mock_surveyor_cls)
    runner = CliRunner()

    with patch("aqueduct.cli._agent_usable", return_value=False):
        with patch("aqueduct.cli._agent_usable_with_cascade", return_value=False):
            result = runner.invoke(cli, ["run", str(auto_agent_bp), "--allow-multi-patch"])

    assert "self-healing is enabled" in result.output
    assert "agent.approval=auto" in result.output
    assert "agent is not reachable" in result.output
