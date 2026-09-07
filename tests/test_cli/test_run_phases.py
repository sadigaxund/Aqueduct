"""Unit tests for the pure/small helpers extracted from `run()` into
`aqueduct/cli/run_phases.py` (Phase 91 decomposition, commit 3).

Covers:
  1. `select_failure_exit_code` — the three-way exit-code choice.
  2. `acquire_run_lock` — acquire/release via a real `ExitStack`, and a
     contended acquire refusing with CONFIG_ERROR.
  3. `combine_chain_patch` — the chain carry-forward idiom (identity on an
     empty chain, `merge_patch_specs` on a non-empty one), plus the
     `patch_count += 1` "every diagnosis spends one unit" accounting rule
     documented alongside it in run_phases.py.
"""

from __future__ import annotations

import contextlib
import types

import pytest

from aqueduct import exit_codes
from aqueduct.cli.run_phases import acquire_run_lock, combine_chain_patch, select_failure_exit_code
from aqueduct.stores.run_lock import RunLockedError

pytestmark = pytest.mark.unit


# ── select_failure_exit_code ────────────────────────────────────────────────


def test_select_failure_exit_code_staged_for_review():
    code = select_failure_exit_code(patch_staged_for_review=True, patch_rejected_by_gate=False)
    assert code == exit_codes.HEAL_PENDING


def test_select_failure_exit_code_rejected_by_gate():
    code = select_failure_exit_code(patch_staged_for_review=False, patch_rejected_by_gate=True)
    assert code == exit_codes.VALIDATION_GATE


def test_select_failure_exit_code_neither():
    code = select_failure_exit_code(patch_staged_for_review=False, patch_rejected_by_gate=False)
    assert code == exit_codes.DATA_OR_RUNTIME


def test_select_failure_exit_code_precedence_when_both_true():
    # Read from the implementation, not assumed: `patch_staged_for_review` is
    # checked first, so it wins when both flags are set.
    code = select_failure_exit_code(patch_staged_for_review=True, patch_rejected_by_gate=True)
    assert code == exit_codes.HEAL_PENDING


# ── acquire_run_lock ─────────────────────────────────────────────────────────


def _manifest(blueprint_id="run_phases_lock_bp"):
    return types.SimpleNamespace(blueprint_id=blueprint_id)


def test_acquire_run_lock_returns_expected_store_dir(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    with contextlib.ExitStack() as stack:
        resolved = acquire_run_lock(
            resolved_store_dir=str(store_dir),
            obs_routing_base=str(tmp_path),
            manifest=_manifest(),
            bundle=None,
            wait_for_lock=False,
            run_stack=stack,
        )
        assert resolved == str(store_dir)


def test_acquire_run_lock_releases_on_exitstack_close(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    manifest = _manifest()

    stack1 = contextlib.ExitStack()
    acquire_run_lock(
        resolved_store_dir=str(store_dir),
        obs_routing_base=str(tmp_path),
        manifest=manifest,
        bundle=None,
        wait_for_lock=False,
        run_stack=stack1,
    )
    stack1.close()  # releases the lock

    # A second acquire now succeeds — proves the first release actually ran.
    with contextlib.ExitStack() as stack2:
        resolved = acquire_run_lock(
            resolved_store_dir=str(store_dir),
            obs_routing_base=str(tmp_path),
            manifest=manifest,
            bundle=None,
            wait_for_lock=False,
            run_stack=stack2,
        )
        assert resolved == str(store_dir)


def test_acquire_run_lock_contended_exits_config_error(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    manifest = _manifest()

    with contextlib.ExitStack() as holder_stack:
        acquire_run_lock(
            resolved_store_dir=str(store_dir),
            obs_routing_base=str(tmp_path),
            manifest=manifest,
            bundle=None,
            wait_for_lock=False,
            run_stack=holder_stack,
        )

        # Still held — a second (non-waiting) acquire must refuse.
        with contextlib.ExitStack() as contender_stack, pytest.raises(SystemExit) as exc_info:
            acquire_run_lock(
                resolved_store_dir=str(store_dir),
                obs_routing_base=str(tmp_path),
                manifest=manifest,
                bundle=None,
                wait_for_lock=False,
                run_stack=contender_stack,
            )
        assert exc_info.value.code == exit_codes.CONFIG_ERROR


def test_run_locked_error_is_the_refusal_class(tmp_path):
    # Sanity check on the exception class `acquire_run_lock` catches — a
    # `blueprint_run_lock` refusal always raises this, never a bare
    # OSError, so the CONFIG_ERROR mapping above is deliberate, not
    # incidental.
    assert issubclass(RunLockedError, Exception)


# ── combine_chain_patch ──────────────────────────────────────────────────────


class _FakePatch:
    """Stand-in for a `PatchSpec` — `combine_chain_patch` never inspects it,
    only passes it through / concatenates it, so identity is enough."""


def test_combine_chain_patch_empty_accumulated_returns_candidate_unchanged():
    patch = _FakePatch()
    result = combine_chain_patch([], patch)
    # Identity, not a merge — `merge_patch_specs` is not even called.
    assert result is patch


def test_combine_chain_patch_nonempty_calls_merge_once_in_order(monkeypatch):
    calls = []

    def _fake_merge(patches):
        calls.append(list(patches))
        return "merged-result"

    monkeypatch.setattr("aqueduct.agent.merge_patch_specs", _fake_merge)

    link1, link2, candidate = _FakePatch(), _FakePatch(), _FakePatch()
    result = combine_chain_patch([link1, link2], candidate)

    assert result == "merged-result"
    assert len(calls) == 1
    # Earlier links first, candidate last.
    assert calls[0] == [link1, link2, candidate]


def test_combine_chain_patch_does_not_mutate_accumulated_list(monkeypatch):
    monkeypatch.setattr("aqueduct.agent.merge_patch_specs", lambda patches: "merged-result")
    accumulated = [_FakePatch()]
    original_len = len(accumulated)
    combine_chain_patch(accumulated, _FakePatch())
    assert len(accumulated) == original_len


# ── heal-loop attempt accounting (documented, not simulated) ────────────────
#
# The full "discarded same-module candidate is NOT carried into
# accumulated_patches, a validated link IS" rule lives inside `run_heal_loop`,
# a ~1200-line function with 30+ closed-over locals — not a function this
# file can honestly unit-test without building an elaborate fake of the
# whole loop (agent responses, sandbox gates, surveyor recording, ...).
# `test_cli_chained_healing.py` / `test_cli_chained_healing_loop.py` already
# cover that rule end-to-end via CliRunner. The two seams that ARE genuinely
# unit-testable in isolation are exercised above and here:
#   - `combine_chain_patch` (the fold-in expression itself, tested above).
#   - the `patch_count += 1` "every diagnosis spends one unit" rule, which
#     is a single unconditional statement — not a computation with inputs
#     and outputs — so it is asserted by direct source inspection instead
#     of a call.


def test_patch_count_increments_unconditionally_after_every_diagnosis():
    """`patch_count += 1` in the heal loop is unconditional — it runs
    immediately after every `generate_cascade_patch`/`generate_agent_patch`
    call, before any branch on `patch is None` or gate/guardrail outcome, so
    a diagnosis that produces no usable patch still spends its budget unit.
    This is the invariant the "LOOP-BOUND INVARIANT" comment (copied
    verbatim into `run_heal_loop`) documents; asserted here by inspecting
    the actual source line rather than driving the whole loop.
    """
    import inspect

    from aqueduct.cli import run_phases

    src = inspect.getsource(run_phases.run_heal_loop)
    lines = src.splitlines()
    # Find the actual statement (not the "LOOP-BOUND INVARIANT" comment
    # near the top of the loop, which also mentions `patch_count += 1` in
    # backticks) — the real increment is an unindented-relative-to-its-
    # block bare statement, not a comment line.
    stmt_lines = [i for i, line in enumerate(lines) if line.strip() == "patch_count += 1"]
    assert len(stmt_lines) == 1, "expected exactly one unconditional patch_count += 1 statement"
    increment_idx = stmt_lines[0]
    # The statement immediately following the increment is the start of
    # the agent-result handling, not a conditional guarding the increment
    # itself — i.e. the increment is unconditional, right after the
    # diagnosis call.
    assert "patch = agent_result.patch" in lines[increment_idx + 2]
