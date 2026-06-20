"""CLI exit-code contract tests — Phase 31 (1.0.1 fix).

Covers the 5 ⏳ items in TEST_MANIFEST.md § CLI — HEAL_PENDING exit code wiring:

  - human mode:    run fails → patch staged → exit 3 (HEAL_PENDING)
  - ci mode:       run fails → patch staged → exit 3 (HEAL_PENDING)
  - disabled mode: run fails → no staging   → exit 2 (DATA_OR_RUNTIME)
  - auto mode:     run fails → patch applied → re-run succeeds → exit 0 (SUCCESS)
  - parse error:   bad blueprint schema      → exit 1 (CONFIG_ERROR)

All tests use CliRunner (no network, no real LLM).
The Spark executor and Surveyor are mocked so no SparkSession is needed.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.models import FailureContext

pytestmark = [pytest.mark.integration]


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_patch(patch_id: str = "p-001") -> PatchSpec:
    return PatchSpec(
        patch_id=patch_id,
        rationale="test patch",
        confidence=0.9,
        category="other",
        root_cause="test",
        operations=[
            {"op": "replace_module_label", "module_id": "src", "label": "Fixed"}
        ],
    )


def _failed_exec_result(bp_id: str = "test_bp") -> ExecutionResult:
    return ExecutionResult(
        blueprint_id=bp_id,
        run_id=str(uuid.uuid4()),
        status="error",
        module_results=(
            ModuleResult(module_id="src", status="error", error="simulated failure"),
        ),
    )


def _ok_exec_result(bp_id: str = "test_bp") -> ExecutionResult:
    return ExecutionResult(
        blueprint_id=bp_id,
        run_id=str(uuid.uuid4()),
        status="success",
        module_results=(
            ModuleResult(module_id="src", status="success", error=None),
        ),
    )


_MINIMAL_BP = """\
aqueduct: '1.0'
id: test_bp
name: Test BP
agent:
  approval: {mode}
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: /nonexistent/data.csv}}
edges: []
"""

_MINIMAL_CFG = """\
aqueduct_config: "1.0"

stores:
  observability:
    backend: duckdb
    path: "{obs}"
  lineage:
    backend: duckdb
    path: "{lin}"
  depot:
    backend: duckdb
    path: "{dep}"
"""


def _write_project(tmp_path: Path, approval: str) -> tuple[Path, Path]:
    bp = tmp_path / "bp.yml"
    bp.write_text(_MINIMAL_BP.format(mode=approval), encoding="utf-8")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        _MINIMAL_CFG.format(
            obs=str(tmp_path / "obs.duckdb"),
            lin=str(tmp_path / "lin.duckdb"),
            dep=str(tmp_path / "dep.duckdb"),
        ),
        encoding="utf-8",
    )
    return bp, cfg


def _make_failure_context(run_id: str) -> FailureContext:
    return FailureContext(
        run_id=run_id,
        blueprint_id="test_bp",
        failed_module="src",
        error_message="simulated failure",
        stack_trace="",
        manifest_json='{"id": "test_bp", "modules": [{"id": "src", "type": "Ingress"}]}',
        started_at="2026-01-01T00:00:00Z",
        finished_at="2026-01-01T00:00:01Z",
    )


# ── tests ─────────────────────────────────────────────────────────────────────

@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
def test_run_human_mode_stages_patch_exits_3(
    mock_gen, mock_get_exec, mock_surveyor_cls, tmp_path
):
    """approval: human — runtime failure → patch staged → exit 3 (HEAL_PENDING)."""
    bp, cfg = _write_project(tmp_path, "human")

    exec_res = _failed_exec_result()
    mock_executor = MagicMock()
    mock_executor.return_value = exec_res
    mock_get_exec.return_value = mock_executor

    mock_surveyor = MagicMock()
    mock_surveyor.record.return_value = _make_failure_context(exec_res.run_id)
    mock_surveyor_cls.return_value = mock_surveyor
    # Prevent MagicMock from poisoning find_pending/find_replay_candidate
    mock_surveyor.observability = None
    mock_surveyor.patch_store.return_value = None

    mock_gen.return_value = MagicMock(patch=_make_patch("p-human-001"))

    runner = CliRunner()
    with patch("aqueduct.cli._agent_usable", return_value=True):
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert result.exit_code == 3, (
        f"Expected HEAL_PENDING (3), got {result.exit_code}\n{result.output}"
    )
    pending = list((tmp_path / "patches" / "pending").glob("*.json"))
    assert len(pending) >= 1, "Expected a patch under patches/pending/"


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
def test_run_ci_mode_stages_patch_exits_3(
    mock_gen, mock_get_exec, mock_surveyor_cls, tmp_path
):
    """approval: ci — runtime failure → patch staged → exit 3 (HEAL_PENDING)."""
    bp, cfg = _write_project(tmp_path, "ci")

    exec_res = _failed_exec_result()
    mock_executor = MagicMock()
    mock_executor.return_value = exec_res
    mock_get_exec.return_value = mock_executor

    mock_surveyor = MagicMock()
    mock_surveyor.record.return_value = _make_failure_context(exec_res.run_id)
    mock_surveyor_cls.return_value = mock_surveyor
    mock_surveyor.observability = None
    mock_surveyor.patch_store.return_value = None

    mock_gen.return_value = MagicMock(patch=_make_patch("p-ci-001"))

    runner = CliRunner()
    with patch("aqueduct.cli._agent_usable", return_value=True):
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert result.exit_code == 3, (
        f"Expected HEAL_PENDING (3), got {result.exit_code}\n{result.output}"
    )
    pending = list((tmp_path / "patches" / "pending").glob("*.json"))
    assert len(pending) >= 1, "Expected a patch under patches/pending/"


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_run_disabled_mode_no_patch_exits_2(
    mock_get_exec, mock_surveyor_cls, tmp_path
):
    """approval: disabled — runtime failure → no staging → exit 2 (DATA_OR_RUNTIME)."""
    bp, cfg = _write_project(tmp_path, "disabled")

    exec_res = _failed_exec_result()
    mock_executor = MagicMock()
    mock_executor.return_value = exec_res
    mock_get_exec.return_value = mock_executor

    mock_surveyor = MagicMock()
    mock_surveyor.record.return_value = _make_failure_context(exec_res.run_id)
    mock_surveyor_cls.return_value = mock_surveyor

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert result.exit_code == 2, (
        f"Expected DATA_OR_RUNTIME (2), got {result.exit_code}\n{result.output}"
    )
    pending_dir = tmp_path / "patches" / "pending"
    pending = list(pending_dir.glob("*.json")) if pending_dir.exists() else []
    assert len(pending) == 0, "No patch should be staged in disabled mode"


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
def test_run_auto_mode_patch_succeeds_exits_0(
    mock_gen, mock_get_exec, mock_surveyor_cls, tmp_path
):
    """approval: auto — failure → patch applied in-memory → re-run succeeds → exit 0."""
    bp, cfg = _write_project(tmp_path, "auto")

    # First execute fails, second succeeds
    fail_res = _failed_exec_result()
    ok_res = _ok_exec_result()
    mock_executor = MagicMock()
    mock_executor.side_effect = [fail_res, ok_res]
    mock_get_exec.return_value = mock_executor

    mock_surveyor = MagicMock()
    mock_surveyor.record.side_effect = [
        _make_failure_context(fail_res.run_id),
        None,   # second record after patch success
    ]
    mock_surveyor_cls.return_value = mock_surveyor
    mock_surveyor.observability = None
    mock_surveyor.patch_store.return_value = None

    # Recovered-patch downgrade policy (CHANGELOG [Unreleased] Added): when
    # ``recovery_applied`` is non-empty, auto-apply forfeits and the CLI
    # exits 3 (HEAL_PENDING). Set the list to empty so this test exercises
    # the clean-response auto-apply path it was written for.
    gen_result = MagicMock(patch=_make_patch("p-auto-001"))
    gen_result.recovery_applied = []
    mock_gen.return_value = gen_result

    runner = CliRunner()
    with patch("aqueduct.cli._agent_usable", return_value=True), \
         patch("aqueduct.cli._run_patch_gates_inline", return_value=(None, None, None, True)), \
         patch("aqueduct.cli._apply_patch_in_memory", return_value=MagicMock()):
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert result.exit_code == 0, (
        f"Expected SUCCESS (0), got {result.exit_code}\n{result.output}"
    )


def test_run_parse_error_exits_1(tmp_path):
    """A Blueprint with an invalid module type → ParseError → exit 1 (CONFIG_ERROR)."""
    bad_bp = tmp_path / "bad.yml"
    bad_bp.write_text(
        "aqueduct: '1.0'\nid: bad\nname: Bad\n"
        "modules:\n  - id: m\n    type: INVALID_TYPE_DOES_NOT_EXIST\n    label: M\n"
        "edges: []\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text('aqueduct_config: "1.0"\n', encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bad_bp), "--config", str(cfg)])

    assert result.exit_code == 1, (
        f"Expected CONFIG_ERROR (1), got {result.exit_code}\n{result.output}"
    )
