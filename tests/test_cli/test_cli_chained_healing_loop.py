"""Regression tests for the merged (chained) heal loop.

Three properties the fold has to keep, each of which a naive "just delete the
flag" implementation would break:

1. Two INDEPENDENT bugs heal in ONE staged patch — the loop carries a proven
   candidate forward instead of re-diagnosing the original failure.
2. A wrong patch against the SAME module retries, and `max_patches` is the
   single total-attempt cap for those retries.
3. Nothing is written to the Blueprint file mid-loop — the file is
   byte-identical until the full accumulated patch passes end to end.
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.patch.grammar import PatchSpec

pytestmark = [pytest.mark.spark, pytest.mark.integration]


@pytest.fixture
def two_module_blueprint(tmp_path):
    """Two modules, so a heal can fail at a DIFFERENT one on the re-run."""
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text(
        """
aqueduct: '1.0'
id: test_bp
name: Test Blueprint
agent:
  approval: auto
  max_patches: 3
  sandbox_mode: "sample"
modules:
  - id: in
    type: Ingress
    label: Input
    config: { format: csv, path: input.csv }
  - id: out
    type: Ingress
    label: Output
    config: { format: csv, path: input.csv }
edges: []
"""
    )
    (tmp_path / "input.csv").write_text("a,b\n1,2")
    return bp_path


def _patch_for(module_id, patch_id):
    return PatchSpec(
        patch_id=patch_id,
        rationale=f"fix {module_id}",
        confidence=0.9,
        category="other",
        root_cause="test",
        operations=[
            {"op": "replace_module_label", "module_id": module_id, "label": f"L-{patch_id}"}
        ],
    )


def _agent_result(patch):
    result = MagicMock(patch=patch)
    result.recovery_applied = []
    return result


def _failed(module_id, run_id="r"):
    return ExecutionResult(
        blueprint_id="test_bp",
        run_id=run_id,
        status="error",
        module_results=[
            ModuleResult(module_id=module_id, status="error", error=f"Boom {module_id}")
        ],
    )


def _ok(run_id="r"):
    return ExecutionResult(
        blueprint_id="test_bp",
        run_id=run_id,
        status="success",
        module_results=[ModuleResult(module_id="in", status="success")],
    )


def _ctx(module_id):
    """A FailureContext stand-in whose `failed_module` is a REAL string.

    The chain's advance/retry decision compares `failed_module` across
    iterations, so a bare MagicMock would make every failure look like the
    same module and silently disable chaining — which is exactly what these
    tests exist to catch.
    """
    ctx = MagicMock()
    ctx.failed_module = module_id
    ctx.engine = None
    return ctx


def _quiet_surveyor(mock_surveyor_cls, *failed_modules):
    """Keep the mocked Surveyor quiet, and give it real failed_module values."""
    inst = mock_surveyor_cls.return_value
    inst.observability = None
    inst.patch_store.return_value = None
    if failed_modules:
        # `record()` is also called on the terminal green run, so pad rather
        # than letting a StopIteration surface as "unexpected error".
        queue = [_ctx(m) for m in failed_modules]

        def _record(*_a, **_k):
            return queue.pop(0) if queue else None

        inst.record.side_effect = _record


@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._agent_usable", return_value=True)
def test_two_independent_bugs_heal_in_one_staged_patch(
    _usable, mock_surveyor_cls, mock_gen_patch, mock_get_executor, two_module_blueprint
):
    """Bug in `in`, then a DIFFERENT bug in `out` -> one combined patch.

    The first candidate is proven (the failure moved to another module), so it
    must be carried forward rather than thrown away; the write at the end must
    be a SINGLE PatchSpec carrying BOTH operations.
    """
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    _quiet_surveyor(mock_surveyor_cls, "in", "out")

    # baseline fails at `in`; after patch 1 fails at `out`; after patch 2 green.
    mock_exec.side_effect = [_failed("in"), _failed("out"), _ok()]
    mock_gen_patch.side_effect = [
        _agent_result(_patch_for("in", "fix-in")),
        _agent_result(_patch_for("out", "fix-out")),
    ]

    with (
        patch("aqueduct.cli._run_patch_gates_inline", return_value=(None, None, None, True)),
        patch("aqueduct.cli._apply_patch_in_memory", return_value=MagicMock()),
        patch("aqueduct.cli._write_patch_to_blueprint") as mock_write,
    ):
        result = CliRunner().invoke(cli, ["run", str(two_module_blueprint), "--allow-multi-patch"])

    assert mock_write.call_count == 1, result.output
    written = mock_write.call_args[0][0]
    ops = [o.op for o in written.operations]
    modules = {getattr(o, "module_id", None) for o in written.operations}
    assert len(ops) == 2, f"expected one combined 2-op patch, got {ops}\n{result.output}"
    assert modules == {"in", "out"}
    # The chain advanced rather than re-diagnosing the original failure.
    assert "chaining" in result.output


@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._agent_usable", return_value=True)
def test_wrong_patch_same_module_retries_and_respects_the_cap(
    _usable, mock_surveyor_cls, mock_gen_patch, mock_get_executor, two_module_blueprint
):
    """The SAME module failing again means the patch was wrong.

    Each retry spends one unit of `max_patches` (3 here), so the agent is
    called exactly 3 times and then the loop stops.
    """
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    _quiet_surveyor(mock_surveyor_cls, *["in"] * 6)

    # Always fails at the SAME module, no matter how many patches are applied.
    mock_exec.side_effect = [_failed("in")] * 6
    mock_gen_patch.side_effect = [_agent_result(_patch_for("in", f"fix-{i}")) for i in range(1, 5)]

    with (
        patch("aqueduct.cli._run_patch_gates_inline", return_value=(None, None, None, True)),
        patch("aqueduct.cli._apply_patch_in_memory", return_value=MagicMock()),
        patch("aqueduct.cli._write_patch_to_blueprint") as mock_write,
    ):
        result = CliRunner().invoke(cli, ["run", str(two_module_blueprint), "--allow-multi-patch"])

    assert mock_gen_patch.call_count == 3, (
        f"max_patches=3 is the total attempt cap; agent called "
        f"{mock_gen_patch.call_count}x\n{result.output}"
    )
    assert mock_write.call_count == 0, "a never-successful chain must not be written"
    assert "max_patches=3 reached" in result.output


@patch("aqueduct.executor.get_executor")
@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.cli._agent_usable", return_value=True)
def test_blueprint_file_is_untouched_mid_loop(
    _usable, mock_surveyor_cls, mock_gen_patch, mock_get_executor, two_module_blueprint
):
    """The disk invariant: nothing lands until the WHOLE chain passes.

    `_write_patch_to_blueprint` is left un-mocked here so a stray mid-loop
    write would really hit the file; the chain never succeeds, so the file
    must come out byte-identical.
    """
    before = two_module_blueprint.read_bytes()
    mock_exec = MagicMock()
    mock_get_executor.return_value = mock_exec
    _quiet_surveyor(mock_surveyor_cls, "in", "out", "out", "out")

    # Advance the chain once (in -> out), then keep failing at `out`.
    mock_exec.side_effect = [_failed("in"), _failed("out"), _failed("out"), _failed("out")]
    mock_gen_patch.side_effect = [
        _agent_result(_patch_for("in", "fix-in")),
        _agent_result(_patch_for("out", "fix-out-1")),
        _agent_result(_patch_for("out", "fix-out-2")),
    ]

    with (
        patch("aqueduct.cli._run_patch_gates_inline", return_value=(None, None, None, True)),
        patch("aqueduct.cli._apply_patch_in_memory", return_value=MagicMock()),
    ):
        result = CliRunner().invoke(cli, ["run", str(two_module_blueprint), "--allow-multi-patch"])

    assert two_module_blueprint.read_bytes() == before, (
        "Blueprint was modified mid-loop despite the chain never succeeding\n" + result.output
    )
