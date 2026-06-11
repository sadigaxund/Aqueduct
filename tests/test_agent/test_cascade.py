"""Unit tests for aqueduct/agent/cascade.py — Phase 44 multi-model healing cascade."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from dataclasses import FrozenInstanceError

pytestmark = pytest.mark.unit

from aqueduct.agent.cascade import generate_cascade_patch
from aqueduct.agent import AgentPatchResult
from aqueduct.parser.models import CascadeTierConfig


def _tier(model: str, **kwargs) -> CascadeTierConfig:
    return CascadeTierConfig(model=model, **kwargs)


def _fctx(**overrides):
    from aqueduct.surveyor.models import FailureContext

    base = dict(
        run_id="run1",
        blueprint_id="bp1",
        failed_module="m1",
        error_message="msg",
        stack_trace="",
        manifest_json="{}",
        started_at="2020-01-01T00:00:00Z",
        finished_at="2020-01-01T00:00:00Z",
    )
    base.update(overrides)
    return FailureContext(**base)


class TestGenerateCascadePatch:
    """Phase 44 — Multi-model healing cascade."""

    @pytest.fixture
    def fctx(self):
        return _fctx()

    @pytest.fixture
    def patches_dir(self, tmp_path: Path) -> Path:
        return tmp_path

    def _success_result(self, model_num: int = 1) -> AgentPatchResult:
        from aqueduct.patch.grammar import PatchSpec
        p = PatchSpec(
            patch_id=f"p{model_num}", rationale="fix",
            operations=[{"op": "set_module_config_key",
                         "module_id": "m1", "key": "k", "value": "v"}],
        )
        return AgentPatchResult(patch=p, attempts=1, stop_reason="solved")

    def _stuck_result(self) -> AgentPatchResult:
        return AgentPatchResult(patch=None, attempts=3, stop_reason="stuck_signature")

    def _api_error_result(self) -> AgentPatchResult:
        return AgentPatchResult(patch=None, attempts=1, stop_reason="api_error")

    def test_tier1_success_returns_immediately(self, fctx, patches_dir):
        """When tier1 returns a patch, tier2 is never called."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.return_value = self._success_result(model_num=1)

            result = generate_cascade_patch(
                tiers=[_tier("model-a"), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
            )

        assert result.patch is not None
        assert result.patch.patch_id == "p1"
        assert mock_gen.call_count == 1

    def test_tier1_stuck_escalates_to_tier2(self, fctx, patches_dir):
        """When tier1 returns stuck_signature, tier2 is called and its result returned."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.side_effect = [
                self._stuck_result(),
                self._success_result(model_num=2),
            ]

            result = generate_cascade_patch(
                tiers=[_tier("model-a"), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
            )

        assert result.patch is not None
        assert result.patch.patch_id == "p2"
        assert mock_gen.call_count == 2

    def test_tier1_api_error_aborts_no_escalation(self, fctx, patches_dir):
        """When tier1 returns api_error, cascade aborts without calling tier2."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.return_value = self._api_error_result()

            result = generate_cascade_patch(
                tiers=[_tier("model-a"), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
            )

        assert result.patch is None
        assert result.stop_reason == "api_error"
        assert mock_gen.call_count == 1

    def test_tier_receives_model_cascade_position(self, fctx, patches_dir):
        """Each tier's generate_agent_patch receives model_cascade_position matching its index."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.side_effect = [
                self._stuck_result(),
                self._success_result(model_num=2),
            ]

            generate_cascade_patch(
                tiers=[_tier("model-a"), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
            )

        # First call: model_cascade_position=0; second call: model_cascade_position=1
        assert mock_gen.call_args_list[0][1]["model_cascade_position"] == 0
        assert mock_gen.call_args_list[1][1]["model_cascade_position"] == 1

    def test_budget_per_tier_overrides_default(self, fctx, patches_dir):
        """tier.max_reprompts overrides cascade default; missing fields inherit."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.side_effect = [
                self._stuck_result(),
                self._success_result(model_num=2),
            ]

            generate_cascade_patch(
                tiers=[_tier("fast-model", max_reprompts=2), _tier("slow-model")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
                max_reprompts=5,
            )

        calls = mock_gen.call_args_list
        # Tier 1 uses its own max_reprompts=2
        assert calls[0][1]["max_reprompts"] == 2
        # Tier 2 falls back to cascade default max_reprompts=5
        assert calls[1][1]["max_reprompts"] == 5


    def test_tier1_deferred_escalates_to_tier2(self, fctx, patches_dir):
        """tier1 stop_reason='deferred' (patch non-None) with tier2 present → escalates to tier2."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            defer_result = AgentPatchResult(
                patch=None, attempts=2, stop_reason="deferred",
            )
            mock_gen.side_effect = [
                defer_result,
                self._success_result(model_num=2),
            ]

            result = generate_cascade_patch(
                tiers=[_tier("model-a"), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
            )

        assert result.patch is not None
        assert result.patch.patch_id == "p2"
        assert mock_gen.call_count == 2

    def test_final_tier_deferred_returned_to_caller(self, fctx, patches_dir):
        """Final-tier 'deferred' → defer result (patch + stop_reason) returned to caller for staging."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            defer_result = AgentPatchResult(
                patch=None, attempts=1, stop_reason="deferred",
            )
            mock_gen.return_value = defer_result

            result = generate_cascade_patch(
                tiers=[_tier("model-a")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
            )

        assert result.stop_reason == "deferred"
        assert result.patch is None
        assert mock_gen.call_count == 1

    def test_allow_defer_deep_loop_inherited_by_tiers(self, fctx, patches_dir):
        """Top-level allow_defer=True / deep_loop=True inherited by tiers whose own field is None."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.side_effect = [
                self._stuck_result(),
                self._success_result(model_num=2),
            ]

            generate_cascade_patch(
                tiers=[_tier("model-a"), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
                allow_defer=True,
                deep_loop=True,
            )

        calls = mock_gen.call_args_list
        assert calls[0][1]["allow_defer"] is True
        assert calls[0][1]["deep_loop"] is True
        assert calls[1][1]["allow_defer"] is True
        assert calls[1][1]["deep_loop"] is True

    def test_tier_allow_defer_overrides_default(self, fctx, patches_dir):
        """Tier's own allow_defer=False overrides top-level True."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.side_effect = [
                self._stuck_result(),
                self._success_result(model_num=2),
            ]

            generate_cascade_patch(
                tiers=[_tier("model-a", allow_defer=False), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
                allow_defer=True,
            )

        calls = mock_gen.call_args_list
        assert calls[0][1]["allow_defer"] is False
        assert calls[1][1]["allow_defer"] is True

    def test_budget_passed_to_cascade_preserves_non_overridden_axes(self, fctx, patches_dir):
        """budget=BudgetConfig(max_tokens_total=N) passed to cascade — every tier budget keeps max_tokens_total=N."""
        from aqueduct.agent.budget import BudgetConfig

        base_budget = BudgetConfig(max_tokens_total=9999, same_error_consecutive=2, same_signature_overall=3, progress_stalled_window=3)

        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.side_effect = [
                self._stuck_result(),
                self._success_result(model_num=2),
            ]

            generate_cascade_patch(
                tiers=[_tier("model-a", max_reprompts=2), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
                budget=base_budget,
            )

        calls = mock_gen.call_args_list
        assert calls[0][1]["budget"].max_tokens_total == 9999
        assert calls[0][1]["budget"].max_reprompts == 2  # tier override
        assert calls[1][1]["budget"].max_tokens_total == 9999
        assert calls[1][1]["budget"].max_reprompts == 5  # fallback

    def test_last_apply_error_forwarded_to_tiers(self, fctx, patches_dir):
        """last_apply_error forwarded to every tier's generate_agent_patch."""
        with patch("aqueduct.agent.cascade.generate_agent_patch") as mock_gen:
            mock_gen.side_effect = [
                self._stuck_result(),
                self._success_result(model_num=2),
            ]

            generate_cascade_patch(
                tiers=[_tier("model-a"), _tier("model-b")],
                failure_ctx=fctx,
                patches_dir=patches_dir,
                last_apply_error="previous patch failed: UNRESOLVED_COLUMN",
            )

        calls = mock_gen.call_args_list
        assert calls[0][1]["last_apply_error"] == "previous patch failed: UNRESOLVED_COLUMN"
        assert calls[1][1]["last_apply_error"] == "previous patch failed: UNRESOLVED_COLUMN"


class TestCascadeTierConfigImmutability:
    def test_frozen(self):
        t = CascadeTierConfig(model="m")
        with pytest.raises(FrozenInstanceError):
            t.model = "other"
