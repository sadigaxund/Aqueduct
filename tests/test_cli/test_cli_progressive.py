from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

# The `run` command imports the Spark executor at function entry
# (aqueduct/cli/run.py `from aqueduct.executor import ExecuteError`), BEFORE
# any config gate can fire — same dependency as every other run-command
# config-gate test in this directory (see test_cli_sandbox_mode.py and the
# coverage-job note in .github/workflows/test-suite.yml). ImportError happens
# at CLI-invoke time (not collection), so this must be an explicit
# importorskip, same pattern as test_report_trend.py.
pytest.importorskip("pyspark", reason="pyspark not installed — install aqueduct-core[spark]")

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.integration

_BP_TEMPLATE = """\
aqueduct: '1.0'
id: test_bp
name: Test BP
agent:
  approval: {approval}
  progressive: {progressive}
  sandbox_mode: "{sandbox}"
{agent_extra}modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: /nonexistent/data.csv}}
edges: []
"""

_CFG_TEMPLATE = """\
aqueduct_config: "1.0"
agent:
  provider: openai_compat
  base_url: "http://localhost:8000"
danger:
  allow_skip_sandbox: {allow_skip}
"""


def _write_project(tmp_path, progressive, sandbox, allow_skip=True,
                   approval="human", agent_extra=""):
    bp = tmp_path / "bp.yml"
    bp.write_text(
        _BP_TEMPLATE.format(
            approval=approval, progressive=str(progressive).lower(),
            sandbox=sandbox, agent_extra=agent_extra,
        ),
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_CFG_TEMPLATE.format(allow_skip=str(allow_skip).lower()), encoding="utf-8")
    return bp, cfg


def _failing_executor(mock_get_executor):
    """Wire the executor factory to a run that always fails at module 'src'."""
    mock_exec = MagicMock()
    mock_exec.return_value = MagicMock(
        status="error",
        trigger_agent=False,
        module_results=[
            MagicMock(module_id="src", status="error", error="Boom",
                      exception=ValueError("Boom"), warnings=[], notes=()),
        ],
    )
    mock_get_executor.return_value = mock_exec
    return mock_exec


def _no_patch_result():
    from aqueduct.agent import AgentPatchResult
    from aqueduct.agent.budget import StopReason
    return AgentPatchResult(
        patch=None, attempts=1, stop_reason=StopReason.EXHAUSTED_ATTEMPTS,
        tokens_in_total=0, tokens_out_total=0, attempt_records=[],
    )


# ── Sandbox requirement (config-load refusal) ──────────────────────────────

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


# ── Scope warning (no silent no-op on unsupported combos) ───────────────────

@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.executor.get_executor")
def test_progressive_with_approval_human_warns(mock_get_executor, mock_generate_patch, mock_surveyor_cls, tmp_path):
    """agent.progressive: true + approval: human must NOT silently fall through
    to the standard loop — one [agent_progressive_scope] warning names the
    disabling condition."""
    bp, cfg = _write_project(
        tmp_path, progressive=True, sandbox="sample", approval="human",
    )
    _failing_executor(mock_get_executor)
    mock_generate_patch.return_value = _no_patch_result()

    runner = CliRunner()
    with patch("aqueduct.agent.memory.find_pending", return_value=None), \
         patch("aqueduct.agent.memory.find_replay_candidate", return_value=None):
        result = runner.invoke(
            cli, ["run", str(bp), "--config", str(cfg), "--store-dir", str(tmp_path)],
        )

    assert "[agent_progressive_scope]" in result.output
    assert "requires agent.approval: auto" in result.output
    assert "'human'" in result.output
    # The standard (non-progressive) heal path still ran.
    assert mock_generate_patch.called


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.agent.cascade.generate_cascade_patch")
@patch("aqueduct.executor.get_executor")
def test_progressive_with_cascade_warns(mock_get_executor, mock_generate_cascade, mock_surveyor_cls, tmp_path):
    """agent.progressive: true + cascade tiers must warn (cascade chaining is
    deferred) and fall back to the standard cascade loop — visibly."""
    cascade_block = (
        "  cascade:\n"
        "    - model: small-model\n"
        "    - model: big-model\n"
    )
    bp, cfg = _write_project(
        tmp_path, progressive=True, sandbox="sample", approval="auto",
        agent_extra=cascade_block,
    )
    _failing_executor(mock_get_executor)
    mock_generate_cascade.return_value = _no_patch_result()

    runner = CliRunner()
    with patch("aqueduct.agent.memory.find_pending", return_value=None), \
         patch("aqueduct.agent.memory.find_replay_candidate", return_value=None):
        result = runner.invoke(
            cli, ["run", str(bp), "--config", str(cfg), "--store-dir", str(tmp_path)],
        )

    assert "[agent_progressive_scope]" in result.output
    assert "cascade" in result.output
    # The standard cascade heal path still ran.
    assert mock_generate_cascade.called


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.executor.get_executor")
def test_progressive_supported_combo_does_not_warn(mock_get_executor, mock_generate_patch, mock_surveyor_cls, tmp_path):
    """approval: auto + no cascade + no replay = the supported combo — the
    scope warning must NOT appear (the chain itself runs instead)."""
    bp, cfg = _write_project(
        tmp_path, progressive=True, sandbox="sample", approval="auto",
    )
    _failing_executor(mock_get_executor)
    mock_generate_patch.return_value = _no_patch_result()

    runner = CliRunner()
    with patch("aqueduct.agent.memory.find_pending", return_value=None), \
         patch("aqueduct.agent.memory.find_replay_candidate", return_value=None):
        result = runner.invoke(
            cli, ["run", str(bp), "--config", str(cfg), "--store-dir", str(tmp_path)],
        )

    assert "[agent_progressive_scope]" not in result.output
    assert "progressive self-healing" in result.output
