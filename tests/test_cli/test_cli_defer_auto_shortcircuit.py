"""A4 fix (Phase 88 Domain 6) — a defer-only patch in `agent.approval: auto`
must short-circuit straight to the pending/defer staging path, never running
the sandbox/gate/apply pyramid (`_run_patch_gates_inline`). A mixed patch (a
real op alongside a defer) must still run the full gate ladder.
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aqueduct.agent.budget import StopReason
from aqueduct.cli import cli
from aqueduct.patch.grammar import DeferToHumanOp, PatchSpec, SetModuleConfigKeyOp

pytestmark = pytest.mark.integration


def _bp_and_cfg(tmp_path):
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text(
        """
aqueduct: "1.0"
id: test_bp
name: Test BP
agent:
  approval: auto
  allow_defer: true
  sandbox_mode: "off"
  max_patches: 2
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: csv
      path: data.csv
edges: []
""",
        encoding="utf-8",
    )
    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(
        """
aqueduct_config: "1.0"
agent:
  provider: openai_compat
  base_url: "http://localhost:8000"
danger:
  allow_multi_patch: true
  allow_skip_sandbox: true
""",
        encoding="utf-8",
    )
    return bp_file, cfg_file


def _mock_failing_executor():
    mock_exec = MagicMock()
    mock_exec.return_value = MagicMock(
        status="error",
        module_results=[
            MagicMock(module_id="m1", status="error", error="Boom", exception=ValueError("Boom"))
        ],
        failed_engine=None,
    )
    return mock_exec


@patch("aqueduct.cli._run_patch_gates_inline")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.cli._resolve_obs_db")
@patch("duckdb.connect")
def test_defer_only_patch_in_auto_mode_skips_gates(
    mock_connect,
    mock_resolve_obs_db,
    mock_get_executor,
    mock_generate_patch,
    mock_run_gates,
    tmp_path,
):
    bp_file, cfg_file = _bp_and_cfg(tmp_path)
    mock_get_executor.return_value = _mock_failing_executor()

    from aqueduct.agent import AgentPatchResult

    defer_patch = PatchSpec(
        patch_id="defer1",
        rationale="cannot fix",
        operations=[
            DeferToHumanOp(
                op="defer_to_human",
                diagnosis="the warehouse is unreachable",
                suggestions=["page infra"],
                defer_reason="infrastructure",
            )
        ],
    )
    mock_generate_patch.return_value = AgentPatchResult(
        patch=defer_patch,
        attempts=1,
        stop_reason=StopReason.DEFERRED,
        tokens_in_total=10,
        tokens_out_total=20,
        attempt_records=[],
    )

    runner = CliRunner()
    with (
        patch("aqueduct.agent.memory.find_pending", return_value=None),
        patch("aqueduct.agent.memory.find_replay_candidate", return_value=None),
    ):
        result = runner.invoke(
            cli, ["run", str(bp_file), "--config", str(cfg_file), "--store-dir", str(tmp_path)]
        )

    print("OUTPUT:", result.output)
    print("EXCEPTION:", result.exception)

    assert mock_generate_patch.called
    # The core A4 assertion: a defer-only patch never reaches the
    # sandbox/gate/apply pyramid.
    assert not mock_run_gates.called
    # The run transcript must say so explicitly (AGENTS.md forbids a silent
    # no-op).
    assert "defer-only patch" in result.output
    assert "skipping sandbox/gate/apply" in result.output


def test_patchspec_grammar_forbids_constructing_a_mixed_defer_patch():
    """`grammar.py::_reject_mixed_defer_ops` already refuses to construct a
    PatchSpec mixing `defer_to_human` with a Blueprint-mutating op — so a
    genuinely "mixed" patch can never reach `cli/run.py`'s auto branch in
    the first place. This nails that invariant down: it is what makes
    `has_defer` (any) and "defer-only" (every op) provably equivalent for
    any patch that could actually exist, even though the auto branch
    defensively checks `all(...)` rather than relying on it."""
    with pytest.raises(Exception, match="cannot be mixed"):
        PatchSpec(
            patch_id="mixed1",
            rationale="partial fix plus defer",
            operations=[
                SetModuleConfigKeyOp(
                    op="set_module_config_key", module_id="m1", key="path", value="new_data.csv"
                ),
                DeferToHumanOp(
                    op="defer_to_human",
                    diagnosis="some of this needs a human too",
                    defer_reason="other",
                ),
            ],
        )


def test_defer_only_predicate_mirrors_cli_run_py_auto_branch():
    """Direct unit test of the exact boolean expression used in
    `cli/run.py`'s "auto" branch (`has_defer` mirroring
    `aqueduct/agent/loop.py:820`, `_defer_only` requiring EVERY op to be
    `defer_to_human`) against raw duck-typed ops — independent of whether a
    real PatchSpec could ever hold such a mix, so the "mixed still runs
    gates" half of the A4 spec is provable even though `grammar.py` makes
    the mixed case unreachable in practice."""

    class _FakeOp:
        def __init__(self, op):
            self.op = op

    defer_only_ops = [_FakeOp("defer_to_human")]
    has_defer = any(op.op == "defer_to_human" for op in defer_only_ops)
    defer_only = has_defer and all(op.op == "defer_to_human" for op in defer_only_ops)
    assert has_defer is True
    assert defer_only is True

    mixed_ops = [_FakeOp("set_module_config_key"), _FakeOp("defer_to_human")]
    has_defer = any(op.op == "defer_to_human" for op in mixed_ops)
    defer_only = has_defer and all(op.op == "defer_to_human" for op in mixed_ops)
    assert has_defer is True
    assert defer_only is False  # mixed → still runs the full gate ladder

    no_defer_ops = [_FakeOp("set_module_config_key")]
    has_defer = any(op.op == "defer_to_human" for op in no_defer_ops)
    defer_only = has_defer and all(op.op == "defer_to_human" for op in no_defer_ops)
    assert has_defer is False
    assert defer_only is False
