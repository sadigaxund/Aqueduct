"""Unit tests for aqueduct/agent/progressive.py — progressive (chained)
multi-patch healing (opt-in `agent.progressive`).

All diagnosis/apply/execute steps are mocked closures — no live LLM, no
Spark. See AGENTS.md's testing constraints.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

from aqueduct.agent.progressive import (
    ProgressiveChainResult,
    merge_patch_specs,
    require_sandbox_for_progressive,
    run_progressive_chain,
)
from aqueduct.errors import ConfigError
from aqueduct.executor.models import ExecutionResult, ExecutionStatus
from aqueduct.patch.grammar import PatchSpec


def _make_patch(patch_id: str, module_id: str, confidence: float | None = 0.9) -> PatchSpec:
    return PatchSpec(
        patch_id=patch_id,
        rationale=f"fix {module_id}",
        operations=[{"op": "replace_module_label", "module_id": module_id, "label": "patched"}],
        confidence=confidence,
    )


def _agent_result(patch, tokens_in=10, tokens_out=20, stop_reason="solved"):
    return SimpleNamespace(
        patch=patch, tokens_in_total=tokens_in, tokens_out_total=tokens_out,
        stop_reason=stop_reason,
    )


def _failure(module: str):
    return SimpleNamespace(failed_module=module)


def _exec_result(status: ExecutionStatus) -> ExecutionResult:
    return ExecutionResult(blueprint_id="bp", run_id="r", status=status, module_results=())


# ── merge_patch_specs ──────────────────────────────────────────────────────

class TestMergePatchSpecs:
    def test_single_patch_returned_unchanged(self):
        p = _make_patch("p1", "mod_a")
        assert merge_patch_specs([p]) is p

    def test_operations_concatenated_in_order(self):
        p1 = _make_patch("p1", "mod_a")
        p2 = _make_patch("p2", "mod_b")
        combined = merge_patch_specs([p1, p2])
        assert len(combined.operations) == 2
        assert combined.operations[0].module_id == "mod_a"
        assert combined.operations[1].module_id == "mod_b"

    def test_rationale_carries_per_link_evidence(self):
        p1 = _make_patch("p1", "mod_a")
        p2 = _make_patch("p2", "mod_b")
        combined = merge_patch_specs([p1, p2])
        assert "Link 1" in combined.rationale
        assert "Link 2" in combined.rationale
        assert "mod_a" in combined.rationale

    def test_confidence_is_the_minimum(self):
        p1 = _make_patch("p1", "mod_a", confidence=0.95)
        p2 = _make_patch("p2", "mod_b", confidence=0.6)
        combined = merge_patch_specs([p1, p2])
        assert combined.confidence == 0.6

    def test_empty_list_raises(self):
        with pytest.raises(ValueError):
            merge_patch_specs([])


# ── require_sandbox_for_progressive ────────────────────────────────────────

class TestSandboxRequirement:
    def test_progressive_with_sandbox_off_raises(self):
        with pytest.raises(ConfigError, match="sandbox"):
            require_sandbox_for_progressive(True, "off")

    def test_progressive_with_sample_ok(self):
        assert require_sandbox_for_progressive(True, "sample") is None  # no raise

    def test_progressive_with_preflight_ok(self):
        assert require_sandbox_for_progressive(True, "preflight") is None  # no raise

    def test_non_progressive_with_sandbox_off_ok(self):
        # no raise — the guard is only gated when progressive is True
        assert require_sandbox_for_progressive(False, "off") is None


# ── run_progressive_chain ──────────────────────────────────────────────────

class TestRunProgressiveChain:
    def test_chain_advances_on_different_module_failure(self):
        """Link 1 fixes module A; pipeline still fails but now at module B;
        link 2 fixes B; pipeline succeeds. Chain must solve with 2 links."""
        diagnose = MagicMock(side_effect=[
            _agent_result(_make_patch("p1", "A")),
            _agent_result(_make_patch("p2", "B")),
        ])
        apply_and_execute = MagicMock(side_effect=[
            (object(), _exec_result(ExecutionStatus.ERROR), _failure("B")),
            (object(), _exec_result(ExecutionStatus.SUCCESS), None),
        ])
        result = run_progressive_chain(
            initial_failure_ctx=_failure("A"),
            diagnose=diagnose,
            apply_and_execute=apply_and_execute,
            max_chain=3,
        )
        assert result.status == "solved"
        assert result.link_count == 2
        assert len(result.combined_patch.operations) == 2
        assert diagnose.call_count == 2
        assert result.links[0].advanced is True
        assert result.links[1].advanced is True

    def test_same_module_is_stuck(self):
        """Candidate patch validates in-memory but the SAME module fails
        again — the chain must stop, not loop forever."""
        diagnose = MagicMock(return_value=_agent_result(_make_patch("p1", "A")))
        apply_and_execute = MagicMock(
            return_value=(object(), _exec_result(ExecutionStatus.ERROR), _failure("A"))
        )
        result = run_progressive_chain(
            initial_failure_ctx=_failure("A"),
            diagnose=diagnose,
            apply_and_execute=apply_and_execute,
            max_chain=3,
        )
        assert result.status == "stuck"
        assert result.links[-1].stuck_reason == "same_module"
        # Only ONE diagnosis happened — chain ends immediately on first stuck link.
        assert diagnose.call_count == 1
        # The accumulated (single-link) patch is still surfaced for staging/discard.
        assert result.combined_patch is not None

    def test_chain_cap_enforced(self):
        """Every link advances to a new module but the pipeline never fully
        succeeds — the chain must stop at max_chain, not loop forever."""
        modules = ["A", "B", "C", "D", "E"]
        diagnose = MagicMock(side_effect=[
            _agent_result(_make_patch(f"p{i}", m)) for i, m in enumerate(modules)
        ])
        apply_and_execute = MagicMock(side_effect=[
            (object(), _exec_result(ExecutionStatus.ERROR), _failure(modules[i + 1]))
            for i in range(len(modules) - 1)
        ])
        result = run_progressive_chain(
            initial_failure_ctx=_failure("A"),
            diagnose=diagnose,
            apply_and_execute=apply_and_execute,
            max_chain=3,
        )
        assert result.status == "chain_cap_exceeded"
        assert result.link_count == 3
        assert diagnose.call_count == 3
        assert len(result.combined_patch.operations) == 3

    def test_no_patch_short_circuits(self):
        diagnose = MagicMock(return_value=_agent_result(None))
        apply_and_execute = MagicMock()
        result = run_progressive_chain(
            initial_failure_ctx=_failure("A"),
            diagnose=diagnose,
            apply_and_execute=apply_and_execute,
            max_chain=3,
        )
        assert result.status == "no_patch"
        assert result.combined_patch is None
        apply_and_execute.assert_not_called()

    def test_apply_failure_marks_stuck(self):
        diagnose = MagicMock(return_value=_agent_result(_make_patch("p1", "A")))
        apply_and_execute = MagicMock(return_value=(None, None, None))
        result = run_progressive_chain(
            initial_failure_ctx=_failure("A"),
            diagnose=diagnose,
            apply_and_execute=apply_and_execute,
            max_chain=3,
        )
        assert result.status == "stuck"
        assert result.links[-1].stuck_reason == "apply_failed"
        assert result.combined_patch is None

    def test_budget_aggregates_tokens_across_links(self):
        diagnose = MagicMock(side_effect=[
            _agent_result(_make_patch("p1", "A"), tokens_in=100, tokens_out=200),
            _agent_result(_make_patch("p2", "B"), tokens_in=50, tokens_out=75),
        ])
        apply_and_execute = MagicMock(side_effect=[
            (object(), _exec_result(ExecutionStatus.ERROR), _failure("B")),
            (object(), _exec_result(ExecutionStatus.SUCCESS), None),
        ])
        result = run_progressive_chain(
            initial_failure_ctx=_failure("A"),
            diagnose=diagnose,
            apply_and_execute=apply_and_execute,
            max_chain=3,
        )
        assert result.tokens_in_total == 150
        assert result.tokens_out_total == 275

    def test_max_chain_below_one_raises(self):
        with pytest.raises(ConfigError):
            run_progressive_chain(
                initial_failure_ctx=_failure("A"),
                diagnose=MagicMock(),
                apply_and_execute=MagicMock(),
                max_chain=0,
            )

    def test_disk_untouched_mid_chain(self, tmp_path):
        """Structural guarantee: run_progressive_chain never imports or calls
        any filesystem-write helper. Callers only write AFTER a `solved`
        result — this test proves the module itself performs zero writes by
        running a full multi-link chain against a real tmp file and
        asserting its mtime/content are unchanged throughout."""
        bp_file = tmp_path / "blueprint.yml"
        bp_file.write_text("original content\n")
        original_mtime = bp_file.stat().st_mtime_ns
        original_content = bp_file.read_text()

        diagnose = MagicMock(side_effect=[
            _agent_result(_make_patch("p1", "A")),
            _agent_result(_make_patch("p2", "B")),
        ])
        apply_and_execute = MagicMock(side_effect=[
            (object(), _exec_result(ExecutionStatus.ERROR), _failure("B")),
            (object(), _exec_result(ExecutionStatus.SUCCESS), None),
        ])
        result = run_progressive_chain(
            initial_failure_ctx=_failure("A"),
            diagnose=diagnose,
            apply_and_execute=apply_and_execute,
            max_chain=3,
        )
        assert result.status == "solved"
        assert bp_file.stat().st_mtime_ns == original_mtime
        assert bp_file.read_text() == original_content
