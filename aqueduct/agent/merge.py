"""Merge N single-link PatchSpecs into one ordered multi-op PatchSpec.

Extracted from the deleted ``aqueduct/agent/progressive.py`` (the fold that made chaining unconditional —
chained multi-patch healing is now the ONLY heal-loop behavior, so there is
no more "progressive, opt-in" module; the merge helper itself is generic and
still needed by the merged loop in ``aqueduct/cli/run.py``). Body copied
verbatim from that module's ``merge_patch_specs``.
"""

from __future__ import annotations

from typing import Any

from aqueduct.patch.grammar import PatchSpec

__all__ = ["merge_patch_specs"]


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

    combined_id = f"chained-{len(patches)}link-{_uuid.uuid4().hex[:8]}"
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
