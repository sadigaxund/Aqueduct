"""Phase 54 — heal-as-PR / patch-import kit, shared by ``aqueduct patch pr``,
``aqueduct patch commit`` and ``aqueduct patch import``.

The engine never runs a long-lived receiver and ships no versioned GitHub
Action. The flow is: a run heals on a cluster, stages the patch, and fires the
``on_patch_pending`` webhook (see ``surveyor/webhook.py``); a CI runner receives
that payload, obtains the patch body (artifact, or ``aqueduct patch pull``), and
calls ``aqueduct patch import`` to apply + commit it on a checkout, or
``aqueduct patch pr`` to open a PR directly. Aqueduct owns only the pure
helpers below — the user's CI workflow wires them together.
"""

from __future__ import annotations

from typing import Any

from aqueduct.patch.grammar import PATCH_META_KEY

# ── Heal-as-PR (Phase 87) ────────────────────────────────────────────────────
#
# Aqueduct-owned branch name convention for `aqueduct patch pr`. A constant,
# not a config knob (ratified: no registry, no per-project pattern override) —
# the user's CI workflow references it instead of hardcoding its own copy.
HEAL_BRANCH_PREFIX = "aqueduct/heal"


def heal_branch_name(patch_id: str) -> str:
    """Branch name `aqueduct patch pr` creates for *patch_id*."""
    return f"{HEAL_BRANCH_PREFIX}/{patch_id}"


# ── CI webhook payload schema ────────────────────────────────────────────────
# Keys the ``on_patch_pending`` webhook always carries. The webhook also
# includes diagnostic extras (root_cause, rationale, category,
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


# ── Repo-root guard (Phase 87) ───────────────────────────────────────────────


def resolve_repo_root_conflict(
    toplevel: str, project_root: str, expected_root: str | None
) -> str | None:
    """Pure comparison behind ``patch pr``'s pre-flight guard.

    *toplevel* is ``git rev-parse --show-toplevel`` resolved from the SAME
    cwd every git-writing command uses (the blueprint directory). *project_root*
    is the ``aqueduct.yml`` directory. Returns a refusal message (naming both
    paths and the config key) when they diverge, or ``None`` when the write is
    safe.

    Catches both footguns with one assertion (2026-08-24 design audit, item
    4): a parent `.git` above the project root resolves *toplevel* ABOVE
    *project_root* (mismatch); a stale child `.git` between the project root
    and the blueprint directory resolves *toplevel* to that child (also a
    mismatch, since it cannot be an ancestor-or-equal of *project_root*). A
    `.git` strictly BELOW the blueprint directory is unreachable by upward
    discovery and therefore never mis-targets a commit, so it is deliberately
    not scanned for.

    When ``expected_root`` (``git.expected_root``) is set, it — not
    *project_root* — is the required value: an explicit operator pin for a
    Blueprint that intentionally lives inside a larger repo.
    """
    from pathlib import Path

    resolved_toplevel = Path(toplevel).resolve()
    if expected_root is not None:
        pin = Path(expected_root).resolve()
        if resolved_toplevel != pin:
            return (
                f"git worktree root ({resolved_toplevel}) does not match the pinned "
                f"git.expected_root ({pin}) in aqueduct.yml — refusing to write. "
                "Fix git.expected_root if this is the wrong repo, or update it if "
                "the repo moved."
            )
        return None
    resolved_project_root = Path(project_root).resolve()
    if resolved_toplevel != resolved_project_root:
        return (
            f"git worktree root ({resolved_toplevel}) does not match the project "
            f"root ({resolved_project_root}, the aqueduct.yml directory) — refusing "
            "to write, to avoid committing into an unexpected repository. If this "
            "project intentionally lives inside a larger repo, pin "
            f'git.expected_root: "{resolved_toplevel}" in aqueduct.yml.'
        )
    return None


# ── PR title/body rendering (Phase 87) ───────────────────────────────────────
#
# Rendered PURELY from existing PatchSpec fields — no model-authored prose, no
# new prompt section, no PROMPT_VERSION bump (2026-08-24 design audit, item
# 5). The `---aqueduct---` trailer (build_commit_message above) is embedded
# verbatim so the PR body and the eventual commit message carry the same
# provenance.


def render_pr_title(template: str, *, patch_id: str, blueprint_id: str, module: str | None) -> str:
    """Render *template* (``pr.title_template``) against a patch's identity."""
    return template.format(
        patch_id=patch_id,
        blueprint_id=blueprint_id,
        module=module or "unknown",
    )


def render_pr_body(patch_body: dict, commit_message: str) -> str:
    """Render the PR body from a single patch's body dict + its commit message.

    Every line traces to a PatchSpec field or the machine-rendered
    `---aqueduct---` trailer already produced by `build_commit_message` — no
    field here is model-authored prose the agent did not already put in
    `rationale`/`root_cause`.
    """
    pid = patch_body.get("patch_id") or "(unknown)"
    rationale = patch_body.get("rationale") or "(no rationale)"
    root_cause = patch_body.get("root_cause")
    confidence = patch_body.get("confidence")
    category = patch_body.get("category")
    ops = [op.get("op", "?") for op in patch_body.get("operations", [])]

    lines = [f"Aqueduct heal `{pid}`", "", rationale]
    if root_cause:
        lines += ["", f"**Root cause:** {root_cause}"]
    details = []
    if confidence is not None:
        details.append(f"confidence={confidence}")
    if category:
        details.append(f"category={category}")
    if details:
        lines += ["", " · ".join(details)]
    if ops:
        lines += ["", f"**Operations:** {', '.join(dict.fromkeys(ops))}"]
    lines += ["", "```", commit_message, "```"]
    return "\n".join(lines)
