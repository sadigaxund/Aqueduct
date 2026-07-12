"""Progressive (chained) multi-patch healing — opt-in `agent.progressive`.

Motivation (recorded live failure): the pre-existing multi-patch loop
(`max_patches > 1`) re-diagnoses the SAME first failure on every attempt.
Its `full_run` validation path applies a candidate patch IN MEMORY, re-runs
the pipeline, and — when the re-run still fails — discards the candidate and
loops back to `execute(manifest, ...)` against the ORIGINAL unpatched
Manifest. With two independent bugs, the model can diagnose bug #1 correctly
on every attempt and it is thrown away every time, because the pipeline
still fails downstream at bug #2 and nothing carries bug #1's fix forward.

This module fixes that by chaining links: each link's candidate patch is
folded into an accumulated `PatchSpec` and re-validated end-to-end (in
memory, never on disk) against the ORIGINAL Blueprint file. When the new
failure surfaces at a DIFFERENT module than the one just patched, the chain
advances to a new link. When it recurs at the SAME module, the chain is
"stuck" and ends. A chain that fully resolves the pipeline produces exactly
ONE combined multi-op patch, which is what callers stage/apply/write —
mirroring the existing single-patch contract so the rest of the heal loop
(guardrails, gates, hooks, `healing_outcomes`) is unchanged.

Disk invariant: this module performs NO filesystem writes. It only calls
back into caller-supplied closures (`diagnose`, `apply_and_execute`), both
of which are expected to use the existing in-memory apply path
(`aqueduct.cli._apply_patch_in_memory`) — nothing here or in that path
touches the Blueprint file until the caller explicitly writes the final
combined patch after `run_progressive_chain` returns `status == "solved"`.

Public API
----------
``ProgressiveLinkResult``  — one link's outcome (module, candidate patch,
                              advanced/stuck).
``ProgressiveChainResult`` — the whole chain's outcome: status, the combined
                              patch (if any), per-link evidence, aggregated
                              token totals.
``merge_patch_specs(patches)`` — concatenate N single-link PatchSpecs into
                              one ordered multi-op PatchSpec.
``run_progressive_chain(...)`` — the orchestrator.
``require_sandbox_for_progressive(progressive, sandbox_mode)`` — refusal
                              guard: progressive mode needs per-link sandbox
                              validation, so `sandbox_mode: off` is rejected.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from aqueduct.errors import ConfigError
from aqueduct.patch.grammar import PatchSpec

__all__ = [
    "ProgressiveLinkResult",
    "ProgressiveChainResult",
    "merge_patch_specs",
    "run_progressive_chain",
    "require_sandbox_for_progressive",
]

ChainStatus = Literal["solved", "stuck", "chain_cap_exceeded", "no_patch"]


def require_sandbox_for_progressive(progressive: bool, sandbox_mode: str) -> None:
    """Refuse to run progressive healing with sandbox validation disabled.

    Each chain link's advancement test IS the in-memory-apply + sandbox
    gate — without a sandbox, a chain link has no way to validate a
    candidate before folding it into the accumulated patch. Called at
    heal-start once both `agent.progressive` and `agent.sandbox_mode` are
    resolved (blueprint override > engine default already applied by the
    caller); raises `ConfigError` rather than silently running unsafely.
    """
    if progressive and sandbox_mode == "off":
        raise ConfigError(
            "agent.progressive: true requires per-link sandbox validation, "
            "but agent.sandbox_mode: off disables sandbox replay entirely. "
            "Set sandbox_mode to 'sample' (default) or 'preflight', or turn "
            "off progressive."
        )


@dataclass
class ProgressiveLinkResult:
    """One chain link's outcome — the transcript/observability unit."""

    link_index: int  # 1-based
    failure_module: str  # module this link diagnosed
    patch: PatchSpec | None  # this link's candidate patch (None on no_patch)
    tokens_in: int = 0
    tokens_out: int = 0
    stop_reason: str | None = None  # the diagnosis loop's own stop_reason (Phase 34 vocabulary)
    advanced: bool = False  # True iff the next failure surfaced at a different module
    stuck_reason: str | None = (
        None  # None | "same_module" | "no_patch" | "apply_failed" | "no_failure_context"
    )
    next_failure_module: str | None = (
        None  # module the NEXT link would diagnose (None if solved/stuck)
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "link_index": self.link_index,
            "failure_module": self.failure_module,
            "patch_id": self.patch.patch_id if self.patch is not None else None,
            "tokens_in": self.tokens_in,
            "tokens_out": self.tokens_out,
            "stop_reason": self.stop_reason,
            "advanced": self.advanced,
            "stuck_reason": self.stuck_reason,
            "next_failure_module": self.next_failure_module,
        }


@dataclass
class ProgressiveChainResult:
    """Whole-chain outcome. `combined_patch` is the ONE patch callers stage/apply."""

    status: ChainStatus
    combined_patch: PatchSpec | None = None
    links: tuple[ProgressiveLinkResult, ...] = ()
    tokens_in_total: int = 0
    tokens_out_total: int = 0
    # Populated only on status == "solved" — the in-memory Manifest/result
    # from the final successful `apply_and_execute` call, so the caller can
    # reuse them exactly like the existing single-patch `full_run` path
    # (`result = result2; failure_ctx = failure_ctx2`) instead of re-running.
    final_manifest: Any = None
    final_result: Any = None
    final_failure_ctx: Any = None

    @property
    def link_count(self) -> int:
        return len(self.links)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "combined_patch_id": self.combined_patch.patch_id if self.combined_patch else None,
            "links": [link.to_dict() for link in self.links],
            "tokens_in_total": self.tokens_in_total,
            "tokens_out_total": self.tokens_out_total,
        }


def merge_patch_specs(patches: list[PatchSpec]) -> PatchSpec:
    """Concatenate N single-link PatchSpecs into one ordered multi-op PatchSpec.

    Operations are concatenated in link order (link 1's ops first) — PatchSpec
    already applies operations left-to-right, so later links' ops see the
    Blueprint state left by earlier links, which is exactly the chain's
    semantics. Rationale is a per-link bulleted trail (the "per-link evidence"
    the approval-composes-once design calls for) so a human/CI reviewer sees
    why each op exists without cross-referencing the transcript.
    """
    if not patches:
        raise ValueError("merge_patch_specs requires at least one PatchSpec")
    if len(patches) == 1:
        return patches[0]

    ops: list[Any] = []
    rationale_lines: list[str] = []
    confidences: list[float] = []
    categories: list[str] = []
    root_causes: list[str] = []
    for i, p in enumerate(patches, start=1):
        ops.extend(p.operations)
        rationale_lines.append(f"Link {i} ({p.patch_id}): {p.rationale}")
        if p.confidence is not None:
            confidences.append(p.confidence)
        if p.category:
            categories.append(p.category)
        if p.root_cause:
            root_causes.append(f"Link {i}: {p.root_cause}")

    import uuid as _uuid

    combined_id = f"progressive-{len(patches)}link-{_uuid.uuid4().hex[:8]}"
    return PatchSpec(
        patch_id=combined_id,
        run_id=patches[-1].run_id,
        rationale=(
            f"Progressive chain — {len(patches)} link(s) accumulated into one "
            "combined patch:\n" + "\n".join(rationale_lines)
        ),
        operations=ops,
        # Combined confidence is conservative — the weakest link, not the average.
        confidence=min(confidences) if confidences else None,
        category=categories[0] if categories else None,
        root_cause="; ".join(root_causes) if root_causes else None,
    )


def run_progressive_chain(
    *,
    initial_failure_ctx: Any,
    diagnose: Callable[[Any, int], Any],
    apply_and_execute: Callable[[PatchSpec], tuple[Any | None, Any | None, Any | None]],
    max_chain: int = 3,
) -> ProgressiveChainResult:
    """Drive the chain: diagnose → accumulate → validate → advance/stop.

    Args:
        initial_failure_ctx: The FailureContext for the pipeline's first
            (real) failure — the same object the non-progressive loop would
            hand to `generate_agent_patch`.
        diagnose: `(failure_ctx, link_index) -> AgentPatchResult`. Caller
            composes this closure so oneshot/agentic mode, cascade tiers,
            and heal-cache replay all apply per link exactly as they do for
            a normal single-patch heal — this module doesn't know or care
            which path produced the patch.
        apply_and_execute: `(combined_candidate_patch) -> (new_manifest,
            execution_result, new_failure_ctx)`. Caller wraps the existing
            in-memory apply path (`_apply_patch_in_memory`) + a full
            `execute()` run — i.e. the SAME gate the non-progressive
            `full_run` validation already performs, just invoked once per
            link with the growing accumulated patch instead of a single
            candidate. `new_manifest` is None when the combined patch fails
            to apply/compile at all (distinct from "applied but pipeline
            still fails").
        max_chain: Hard cap on the number of links (independent of each
            link's own reprompt/budget ceiling).

    Returns:
        ProgressiveChainResult. `status == "solved"` is the only case with
        `final_manifest`/`final_result` populated — those are what the
        caller reuses to skip a redundant re-run before writing to disk.
    """
    if max_chain < 1:
        raise ConfigError("agent.max_chain must be >= 1")

    patches: list[PatchSpec] = []
    links: list[ProgressiveLinkResult] = []
    tokens_in_total = 0
    tokens_out_total = 0
    current_failure = initial_failure_ctx

    for link_index in range(1, max_chain + 1):
        agent_result = diagnose(current_failure, link_index)
        _tokens_in = getattr(agent_result, "tokens_in_total", 0) or 0
        _tokens_out = getattr(agent_result, "tokens_out_total", 0) or 0
        tokens_in_total += _tokens_in
        tokens_out_total += _tokens_out

        candidate_patch = getattr(agent_result, "patch", None)
        _module = getattr(current_failure, "failed_module", None) or "<root>"

        if candidate_patch is None:
            links.append(
                ProgressiveLinkResult(
                    link_index=link_index,
                    failure_module=_module,
                    patch=None,
                    tokens_in=_tokens_in,
                    tokens_out=_tokens_out,
                    stop_reason=getattr(agent_result, "stop_reason", None),
                    advanced=False,
                    stuck_reason="no_patch",
                )
            )
            return ProgressiveChainResult(
                status="no_patch",
                combined_patch=None,
                links=tuple(links),
                tokens_in_total=tokens_in_total,
                tokens_out_total=tokens_out_total,
            )

        candidate_chain = patches + [candidate_patch]
        combined_candidate = merge_patch_specs(candidate_chain)
        new_manifest, result, new_failure = apply_and_execute(combined_candidate)

        if new_manifest is None:
            links.append(
                ProgressiveLinkResult(
                    link_index=link_index,
                    failure_module=_module,
                    patch=candidate_patch,
                    tokens_in=_tokens_in,
                    tokens_out=_tokens_out,
                    stop_reason=getattr(agent_result, "stop_reason", None),
                    advanced=False,
                    stuck_reason="apply_failed",
                )
            )
            return ProgressiveChainResult(
                status="stuck",
                combined_patch=None,
                links=tuple(links),
                tokens_in_total=tokens_in_total,
                tokens_out_total=tokens_out_total,
            )

        _succeeded = getattr(result, "status", None) is not None and str(result.status) == "success"
        if _succeeded:
            patches.append(candidate_patch)
            links.append(
                ProgressiveLinkResult(
                    link_index=link_index,
                    failure_module=_module,
                    patch=candidate_patch,
                    tokens_in=_tokens_in,
                    tokens_out=_tokens_out,
                    stop_reason=getattr(agent_result, "stop_reason", None),
                    advanced=True,
                    stuck_reason=None,
                    next_failure_module=None,
                )
            )
            return ProgressiveChainResult(
                status="solved",
                combined_patch=merge_patch_specs(patches),
                links=tuple(links),
                tokens_in_total=tokens_in_total,
                tokens_out_total=tokens_out_total,
                final_manifest=new_manifest,
                final_result=result,
                final_failure_ctx=new_failure,
            )

        if new_failure is None:
            links.append(
                ProgressiveLinkResult(
                    link_index=link_index,
                    failure_module=_module,
                    patch=candidate_patch,
                    tokens_in=_tokens_in,
                    tokens_out=_tokens_out,
                    stop_reason=getattr(agent_result, "stop_reason", None),
                    advanced=False,
                    stuck_reason="no_failure_context",
                )
            )
            return ProgressiveChainResult(
                status="stuck",
                combined_patch=merge_patch_specs(candidate_chain),
                links=tuple(links),
                tokens_in_total=tokens_in_total,
                tokens_out_total=tokens_out_total,
            )

        _next_module = getattr(new_failure, "failed_module", None) or "<root>"
        if _next_module == _module:
            # Same module again — stuck. The accumulated patch (including
            # THIS link's candidate) is returned so the caller can discard
            # (auto mode) or stage it for human/CI review with per-link
            # evidence, per the design's approval-composition rule.
            links.append(
                ProgressiveLinkResult(
                    link_index=link_index,
                    failure_module=_module,
                    patch=candidate_patch,
                    tokens_in=_tokens_in,
                    tokens_out=_tokens_out,
                    stop_reason=getattr(agent_result, "stop_reason", None),
                    advanced=False,
                    stuck_reason="same_module",
                    next_failure_module=_next_module,
                )
            )
            return ProgressiveChainResult(
                status="stuck",
                combined_patch=merge_patch_specs(candidate_chain),
                links=tuple(links),
                tokens_in_total=tokens_in_total,
                tokens_out_total=tokens_out_total,
            )

        # Advanced — different module. Fold this link's patch into the
        # accumulated chain and diagnose the new failure on the next link.
        patches.append(candidate_patch)
        links.append(
            ProgressiveLinkResult(
                link_index=link_index,
                failure_module=_module,
                patch=candidate_patch,
                tokens_in=_tokens_in,
                tokens_out=_tokens_out,
                stop_reason=getattr(agent_result, "stop_reason", None),
                advanced=True,
                stuck_reason=None,
                next_failure_module=_next_module,
            )
        )
        current_failure = new_failure

    # Hard cap reached — every link so far advanced (never stuck, never
    # solved). The accumulated patch is real progress; caller decides
    # whether to discard or stage it (same as "stuck").
    return ProgressiveChainResult(
        status="chain_cap_exceeded",
        combined_patch=merge_patch_specs(patches) if patches else None,
        links=tuple(links),
        tokens_in_total=tokens_in_total,
        tokens_out_total=tokens_out_total,
    )
