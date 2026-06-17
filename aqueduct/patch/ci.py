"""Phase 54 — CI kit. The thin, serverless core for ``approval_mode: ci``.

The engine never runs a long-lived receiver and ships no versioned GitHub
Action. The flow is: a run heals on a cluster, stages the patch, and fires the
``on_patch_pending`` webhook (see ``surveyor/webhook.py``); a CI runner receives
that payload, obtains the patch body (artifact, or ``aqueduct patch pull``), and
calls ``aqueduct patch import`` to apply + commit it on a checkout. The user owns
a ~10-line workflow (see ``docs/templates/ci-heal-workflow.yml``); Aqueduct owns
only the pure helpers below.
"""
from __future__ import annotations

from aqueduct.patch.grammar import PATCH_META_KEY

from typing import Any

# ── CI webhook payload schema ────────────────────────────────────────────────
# Keys the ``on_patch_pending`` webhook always carries (approval_mode: ci). The
# webhook also includes diagnostic extras (root_cause, rationale, category,
# suggestions, patch_path) that workflows MAY use but are not required to wire.
CI_WEBHOOK_REQUIRED_KEYS: tuple[str, ...] = (
    "patch_id",
    "run_id",
    "blueprint_id",
    "failed_module",
    "source",
)


def validate_ci_payload(payload: Any) -> list[str]:
    """Return a list of schema violations for a CI webhook payload (empty = ok).

    Validates only the required envelope keys — the patch body itself is
    validated by ``load_patch_spec`` at import time. ``failed_module`` may be
    null (some failures have no single module), so its presence (not its value)
    is what is checked.
    """
    if not isinstance(payload, dict):
        return [f"payload must be a JSON object, got {type(payload).__name__}"]
    violations: list[str] = []
    for key in CI_WEBHOOK_REQUIRED_KEYS:
        if key not in payload:
            violations.append(f"missing required key: {key!r}")
    pid = payload.get("patch_id")
    if "patch_id" in payload and (not isinstance(pid, str) or not pid):
        violations.append("patch_id must be a non-empty string")
    return violations


# ── Structured commit message ────────────────────────────────────────────────

def build_commit_message(blueprint_id: str, patches: list[dict]) -> str:
    """Build the structured git commit message for applied Aqueduct patches.

    ``patches`` is a list of patch-body dicts (a PatchSpec plus optional
    ``_aq_meta``). The message carries a conventional-commit subject and a
    machine-parseable ``---aqueduct---`` trailer that ``aqueduct log`` and
    ``aqueduct rollback`` read back. Shared by ``patch commit`` (already-applied
    patches) and ``patch import`` (apply + commit in one shot).
    """
    patch_lines: list[str] = []
    all_ops: list[str] = []
    rationales: list[str] = []
    run_id: str | None = None

    for data in patches:
        pid = data.get("patch_id") or "(unknown)"
        rat = data.get("rationale", "")
        if rat:
            rationales.append(rat)
        all_ops.extend(op.get("op", "?") for op in data.get("operations", []))
        meta = data.get(PATCH_META_KEY, {}) or {}
        if not run_id:
            run_id = meta.get("run_id") or data.get("run_id")
        patch_lines.append(f"  - {pid}: {rat or '(no rationale)'}")

    n = len(patches)
    summary = rationales[0] if n == 1 and rationales else f"{n} patches applied"
    combined_rationale = "\n".join(rationales) if rationales else ""
    ops_str = ", ".join(dict.fromkeys(all_ops))  # deduplicated, order-preserving

    aqueduct_block = "---aqueduct---\npatches:\n" + "\n".join(patch_lines)
    if run_id:
        aqueduct_block += f"\nrun_id: {run_id}"
    if ops_str:
        aqueduct_block += f"\nops: {ops_str}"
    aqueduct_block += "\n---"

    commit_msg = f"fix(aqueduct/{blueprint_id}): {summary}"
    if combined_rationale:
        commit_msg += f"\n\n{combined_rationale}"
    commit_msg += f"\n\n{aqueduct_block}"
    return commit_msg
