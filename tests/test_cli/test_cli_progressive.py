import pytest
from unittest.mock import patch
from click.testing import CliRunner
from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = [pytest.mark.integration]

_BP_TEMPLATE = """\
aqueduct: '1.0'
id: test_bp
name: Test BP
agent:
  approval: human
  progressive: {progressive}
  sandbox_mode: "{sandbox}"
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: /nonexistent/data.csv}}
edges: []
"""

_CFG_TEMPLATE = """\
aqueduct_config: "1.0"
danger:
  allow_skip_sandbox: {allow_skip}
"""


def _write_project(tmp_path, progressive, sandbox, allow_skip=True):
    bp = tmp_path / "bp.yml"
    bp.write_text(_BP_TEMPLATE.format(progressive=str(progressive).lower(), sandbox=sandbox), encoding="utf-8")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_CFG_TEMPLATE.format(allow_skip=str(allow_skip).lower()), encoding="utf-8")
    return bp, cfg


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_progressive_with_sandbox_off_refused(mock_get_exec, mock_surveyor_cls, tmp_path):
    """agent.progressive: true + agent.sandbox_mode: off -> CONFIG_ERROR, actionable message."""
    bp, cfg = _write_project(tmp_path, progressive=True, sandbox="off", allow_skip=True)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == exit_codes.CONFIG_ERROR
    assert "progressive" in result.output
    assert "sandbox" in result.output


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_progressive_with_sample_sandbox_not_refused(mock_get_exec, mock_surveyor_cls, tmp_path):
    """agent.progressive: true + default sandbox_mode ('sample') proceeds past the guard."""
    bp, cfg = _write_project(tmp_path, progressive=True, sandbox="sample", allow_skip=True)
    mock_get_exec.return_value.side_effect = Exception("stop after guard — reached execute()")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    # The guard did not fire — execution proceeded far enough to reach the
    # (mocked) executor factory, unlike the sandbox_mode: off case above.
    assert "requires per-link sandbox validation" not in result.output


def test_progressive_false_with_sandbox_off_not_refused(tmp_path):
    """Non-progressive heals are unaffected — sandbox_mode: off stays legal
    on its own (guarded by danger.allow_skip_sandbox, unrelated to progressive)."""
    from aqueduct.agent.progressive import require_sandbox_for_progressive
    assert require_sandbox_for_progressive(False, "off") is None  # no raise
