"""PatchSpec grammar — Pydantic v2 schema for structured Blueprint diffs.

A PatchSpec is a JSON document containing one or more typed operations.
Each operation maps 1:1 to a field or structural change in a Blueprint YAML.
The grammar is intentionally constrained: the LLM agent (and humans) operate
within these primitives rather than generating free-form YAML.

Usage:
    spec = PatchSpec.model_validate_json(raw_json)
    schema = PatchSpec.model_json_schema()   # expose to LLM in Phase 7
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

# JSON envelope key for CI-kit patch metadata — the key under which the
# structured `_aq_meta` block lives in webhook / patch-import payloads.
PATCH_META_KEY = "_aq_meta"


# ── Individual operation models ───────────────────────────────────────────────

class ReplaceModuleConfigOp(BaseModel, extra="forbid"):
    """Replace the entire config block of a named Module.

    Most common operation.  Used to fix bad SQL, wrong paths, incorrect params.
    """
    op: Literal["replace_module_config"]
    module_id: str = Field(..., description="ID of the module to patch")
    config: dict[str, Any] = Field(..., description="New config block (full replacement)")


class ReplaceModuleLabelOp(BaseModel, extra="forbid"):
    """Update the human-readable label of a Module."""
    op: Literal["replace_module_label"]
    module_id: str
    label: str


class InsertModuleOp(BaseModel, extra="forbid"):
    """Insert a new Module into the blueprint graph.

    The caller is responsible for providing edges that correctly wire the new
    module into the existing graph.  edges_to_remove lists existing edges that
    must be deleted to make room (e.g. a direct A→C edge when inserting B
    between A and C).
    """
    op: Literal["insert_module"]
    module: dict[str, Any] = Field(..., description="Full module definition dict")
    edges_to_add: list[dict[str, Any]] = Field(
        default_factory=list,
        description="New edges to add [{from, to, port?}]",
    )
    edges_to_remove: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Existing edges to remove [{from, to}]",
    )


class RemoveModuleOp(BaseModel, extra="forbid"):
    """Remove a Module and optionally rewire its edges.

    edges_to_add contains replacement edges that reconnect the graph after
    the module is removed (e.g. bypass edges).
    """
    op: Literal["remove_module"]
    module_id: str
    edges_to_add: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Replacement edges to restore connectivity",
    )


class ReplaceContextValueOp(BaseModel, extra="forbid"):
    """Update a Tier 0 or Tier 1 value in the Context Registry.

    key uses dot-notation for nested values (e.g. 'paths.input').
    """
    op: Literal["replace_context_value"]
    key: str = Field(..., description="Dot-notation context key, e.g. 'paths.input'")
    value: Any = Field(..., description="New value (string, number, or nested dict)")


class AddProbeOp(BaseModel, extra="forbid"):
    """Attach a new Probe module to a Module's output.

    The module dict must include type='Probe' and attach_to pointing to
    an existing Module.  edges_to_add wires the Probe into downstream signal
    consumers (e.g. Regulator signal ports).
    """
    op: Literal["add_probe"]
    module: dict[str, Any] = Field(
        ...,
        description="Full Probe module definition (must include attach_to)",
    )
    edges_to_add: list[dict[str, Any]] = Field(default_factory=list)


class ReplaceEdgeOp(BaseModel, extra="forbid"):
    """Rewire an existing edge.

    Identifies the edge by its current from_id + to_id.  Supply new_from_id
    and/or new_to_id to change endpoints; supply new_port to change the port.
    At least one of new_from_id, new_to_id, new_port must be provided.
    """
    op: Literal["replace_edge"]
    from_id: str = Field(..., description="Current source module ID")
    to_id: str = Field(..., description="Current target module ID")
    new_from_id: str | None = None
    new_to_id: str | None = None
    new_port: str | None = None


class SetModuleConfigKeyOp(BaseModel, extra="forbid"):
    """Set a single key inside a Module's config without touching other keys.

    Prefer over replace_module_config when fixing one field (path typo, bad
    format value, wrong option).  replace_module_config replaces the entire
    config block and risks silently dropping fields the LLM forgot to re-emit.

    key uses dot-notation for nested values (e.g. 'options.mergeSchema').
    """
    op: Literal["set_module_config_key"]
    module_id: str = Field(..., description="ID of the module to patch")
    key: str = Field(..., description="Dot-notation config key, e.g. 'path' or 'options.mergeSchema'")
    value: Any = Field(..., description="New value for the key")


class SetModuleOnFailureOp(BaseModel, extra="forbid"):
    """Change the on_failure policy for a specific Module."""
    op: Literal["set_module_on_failure"]
    module_id: str
    on_failure: dict[str, Any]


class ReplaceRetryPolicyOp(BaseModel, extra="forbid"):
    """Replace the blueprint-level retry_policy block entirely."""
    op: Literal["replace_retry_policy"]
    retry_policy: dict[str, Any]


class AddArcadeRefOp(BaseModel, extra="forbid"):
    """Reference a new or existing Arcade sub-Blueprint.

    The module dict must include type='Arcade' and ref pointing to the
    sub-Blueprint path (relative to the parent Blueprint file).
    """
    op: Literal["add_arcade_ref"]
    module: dict[str, Any] = Field(
        ...,
        description="Full Arcade module definition (must include ref)",
    )
    edges_to_add: list[dict[str, Any]] = Field(default_factory=list)
    edges_to_remove: list[dict[str, Any]] = Field(default_factory=list)


class DeferToHumanOp(BaseModel, extra="forbid"):
    """Signal that the failure cannot be patched at the Blueprint level.

    Infrastructure failures, upstream schema changes, or fundamental
    data-shape changes that Aqueduct's PatchSpec grammar cannot repair
    should use this op instead of hallucinating a patch.

    Makes zero Blueprint changes.  The loop terminates with
    ``stop_reason='deferred'`` and the full diagnosis is staged for
    human review.
    """
    op: Literal["defer_to_human"]
    diagnosis: str = Field(
        ...,
        description="Detailed explanation of why this cannot be patched automatically",
    )
    suggestions: list[str] = Field(
        default_factory=list,
        description="Actionable next steps for the human operator",
    )
    confidence_reason: str = Field(
        default="",
        description="Why the model is confident deferral is correct (vs uncertain)",
    )


class ReplaceMacroOp(BaseModel, extra="forbid"):
    """Replace the body of an existing SQL macro in the ``macros:`` block (Phase 47).

    Bad SQL often lives in a macro: the agent is told to preserve
    ``{{ macros.* }}`` references in module queries, so when the root cause
    is inside the macro body itself, this is the only op that can fix it.
    Replace-only — the macro name must already exist (a macro nothing
    references would be dead weight; unknown names are rejected at apply
    time, which also catches name hallucinations).

    Blast radius: a macro is shared — every module referencing it picks up
    the new body at re-expansion. The compile and lineage gates re-run on
    the patched Blueprint, so parameter mismatches and broken columns in ANY
    consumer are caught before the patch lands.

    Guardrail: recommended for ``guardrails.forbidden_ops`` (template
    default) so multi-module macro changes get human review.
    """
    op: Literal["replace_macro"]
    name: str = Field(..., description="Name of an EXISTING macro in the Blueprint macros: block")
    value: str = Field(..., description="New SQL body. Keep {{ param }} placeholders the macro's callers supply.")


class SetSparkConfigOp(BaseModel, extra="forbid"):
    """Set a single key in the Blueprint's ``spark_config`` block (Phase 42).

    Seven of the 20 most common Spark errors are fixed purely by changing
    spark config values: OOM, container kills, shuffle fetch failures,
    Kryo buffer overflow, dynamic allocation thrashing, GC/heartbeat
    issues, driver MaxResultSize.  This operation makes those healable.

    Auto-creates the ``spark_config`` block if absent.

    Guardrail: ``set_spark_config`` is **default-forbidden in auto mode**
    via ``guardrails.forbidden_ops``.  The LLM can always propose it —
    it just lands in ``patches/pending/`` unless the operator removes
    it from ``forbidden_ops``.
    """
    op: Literal["set_spark_config"]
    key: str = Field(
        ...,
        description="Dot-notation spark config key, e.g. 'spark.sql.shuffle.partitions'",
    )
    value: Any = Field(
        ...,
        description="New value (string, integer, float, or boolean)",
    )


# ── Discriminated union ───────────────────────────────────────────────────────

PatchOperation = Annotated[
    ReplaceModuleConfigOp | SetModuleConfigKeyOp | ReplaceModuleLabelOp | InsertModuleOp | RemoveModuleOp | ReplaceContextValueOp | AddProbeOp | ReplaceEdgeOp | SetModuleOnFailureOp | ReplaceRetryPolicyOp | AddArcadeRefOp | DeferToHumanOp | SetSparkConfigOp | ReplaceMacroOp,
    Field(discriminator="op"),
]

# Canonical list of valid PatchSpec operation discriminator values — the
# single source of truth consumed by the agent prompts so the LLM always
# sees the grammar's actual op set, not a hand-maintained copy.
VALID_PATCH_OPS = (
    "replace_module_config",
    "set_module_config_key",
    "replace_module_label",
    "insert_module",
    "remove_module",
    "replace_context_value",
    "add_probe",
    "replace_edge",
    "set_module_on_failure",
    "replace_retry_policy",
    "add_arcade_ref",
    "defer_to_human",
    "set_spark_config",
    "replace_macro",
)


# ── Top-level PatchSpec ───────────────────────────────────────────────────────

_OP_ALIASES: dict[str, str] = {
    # LLM frequently hallucinates this mashup of two real op names
    "replace_module_config_key": "set_module_config_key",
    # Other common confusions
    "set_module_config": "replace_module_config",
    "update_module_config": "replace_module_config",
    "patch_module_config": "replace_module_config",
    "update_module_config_key": "set_module_config_key",
    "patch_module_config_key": "set_module_config_key",
    # Phase 41: defer_to_human variants
    "defer": "defer_to_human",
    "defer_to_user": "defer_to_human",
    "human_review": "defer_to_human",
    # Phase 42: set_spark_config variants
    "set_spark_config_key": "set_spark_config",
    # Phase 47: replace_macro variants
    "set_macro": "replace_macro",
    "update_macro": "replace_macro",
    "replace_macro_body": "replace_macro",
}


# Casing / synonym aliases for top-level metadata fields. These are
# DESCRIPTIVE fields — they never mutate the blueprint, so we tolerate
# naming chaos. Operation-level fields (`op`, `module_id`, `key`, `value`)
# stay strict via `extra="forbid"` on each Op model.
_METADATA_ALIASES: dict[str, str] = {
    # rationale
    "description": "rationale",
    "summary": "rationale",
    "reason": "rationale",
    "explanation": "rationale",
    "reasoning": "rationale",
    # root_cause
    "rootCause": "root_cause",
    "rootcause": "root_cause",
    "cause": "root_cause",
    "rootCauseAnalysis": "root_cause",
    # confidence
    "Confidence": "confidence",
    "score": "confidence",
    # category
    "Category": "category",
    "failure_category": "category",
    "failureCategory": "category",
    # patch_id
    "patchId": "patch_id",
    "patchID": "patch_id",
    # run_id
    "runId": "run_id",
    "runID": "run_id",
}

# Top-level fields the PatchSpec recognises. Anything else gets bucketed
# into `misc` instead of bouncing the patch.
_PATCHSPEC_FIELDS: frozenset[str] = frozenset({
    "patch_id", "run_id", "rationale", "operations",
    "confidence", "category", "root_cause", "misc",
})


class PatchSpec(BaseModel, extra="allow"):
    """A structured diff to a Blueprint.

    Validated against this schema before application.  Applied atomically —
    all operations succeed or none are persisted.

    Fields:
        patch_id:   Stable identifier (slug + timestamp).  Used as filename in
                    patches/pending/, patches/applied/, patches/rejected/.
        run_id:     The run that triggered this patch (None for human-authored).
        rationale:  Human/LLM explanation of why this patch is needed.
        operations: Ordered list of operations to apply.  Applied left-to-right;
                    later operations see the Blueprint state left by earlier ones.
    """

    @model_validator(mode="after")
    def _reject_mixed_defer_ops(self) -> PatchSpec:
        """Phase 41: defer_to_human must be the ONLY operation.

        Mixing deferral with Blueprint-mutating ops is ambiguous — the
        model is hedging. Reject it explicitly so the reprompt can
        force a clear choice.
        """
        ops = self.operations
        has_defer = any(o.op == "defer_to_human" for o in ops)
        has_mutation = any(o.op != "defer_to_human" for o in ops)
        if has_defer and has_mutation:
            raise ValueError(
                "defer_to_human cannot be mixed with other operations. "
                "If you defer, defer completely — do NOT include any "
                "Blueprint-mutating ops in the same PatchSpec."
            )
        return self

    @model_validator(mode="before")
    @classmethod
    def _normalize_op_aliases(cls, data: Any) -> Any:
        """Silently fix common LLM field name and op name hallucinations."""
        if not isinstance(data, dict):
            return data

        # ── Top-level metadata field aliases (casing/synonym tolerance) ───────
        # Lenient on descriptive fields — they don't mutate the blueprint, so a
        # cosmetic typo (`rootCause` vs `root_cause`) burning a reprompt round
        # is pure dogma. Strict on operations[].* (each Op enforces `extra="forbid"`).
        for alias, canonical in _METADATA_ALIASES.items():
            if alias in data and canonical not in data:
                data[canonical] = data.pop(alias)
            elif alias in data and canonical in data:
                # Both present — canonical wins, alias drops.
                data.pop(alias)

        # All known aliases for "operations"
        _OPS_ALIASES = ("ops", "op_list", "patches", "steps", "fix", "changes",
                        "actions", "modifications", "updates", "patch_operations",
                        "module_updates", "module_changes", "edits", "diff")
        if "operations" not in data:
            for alias in _OPS_ALIASES:
                if alias in data:
                    data["operations"] = data.pop(alias)
                    break

        # Smart fallback: if still no "operations", look for any key whose value
        # is a non-empty list of dicts (likely the operations list under a novel name)
        if "operations" not in data:
            for key, val in list(data.items()):
                if (
                    key not in ("module_results",)
                    and isinstance(val, list)
                    and val
                    and isinstance(val[0], dict)
                    and ("op" in val[0] or "type" in val[0])
                ):
                    data["operations"] = data.pop(key)
                    break

        # ── Synthesise missing patch_id ───────────────────────────────────────
        # 1.1.0 — LLMs frequently omit `patch_id` even though it's required.
        # Re-prompting wastes a full attempt. Derive a stable slug from
        # rationale (or fall back to a short uuid) so the patch can apply
        # cleanly without bouncing the LLM.
        if not data.get("patch_id"):
            import re as _re
            import uuid as _uuid
            _rat = (data.get("rationale") or "").strip()
            if _rat:
                _slug = _re.sub(r"[^a-z0-9]+", "-", _rat.lower())[:48].strip("-")
                if _slug:
                    data["patch_id"] = f"auto-{_slug}"
            if not data.get("patch_id"):
                data["patch_id"] = f"auto-{_uuid.uuid4().hex[:12]}"

        # ── Strip well-known LLM-hallucinated meta fields ─────────────────────
        # Models sometimes add `id`, `name`, `applied_by`, `datetime_applied`,
        # etc. They're noise; drop them so they don't end up in `misc`.
        for _bad in ("id", "name", "applied_by", "datetime_applied", "timestamp",
                     "author", "version", "created_at", "updated_at"):
            data.pop(_bad, None)

        # ── Bucket unknown top-level fields into `misc` ───────────────────────
        # Any remaining unknown key is kept for human-eye visibility but does
        # not participate in mutation. Models trained on heterogeneous corpora
        # often emit fields like `examples`, `notes`, `references`, `verified_by`
        # — preserving them in `misc` is safer than dropping silently.
        existing_misc = data.get("misc")
        if not isinstance(existing_misc, dict):
            existing_misc = {}
        for _key in list(data.keys()):
            if _key not in _PATCHSPEC_FIELDS:
                existing_misc[_key] = data.pop(_key)
        if existing_misc:
            data["misc"] = existing_misc

        # ── Op name normalization inside each operation ───────────────────────
        _DISCRIMINATOR_ALIASES = ("type", "action", "operation", "method", "kind", "name")
        for op in data.get("operations") or []:
            if not isinstance(op, dict):
                continue
            # Rename wrong discriminator key → "op"
            if "op" not in op:
                for alias in _DISCRIMINATOR_ALIASES:
                    if alias in op:
                        op["op"] = op.pop(alias)
                        break
            # Normalize op name itself
            raw_op = op.get("op")
            if raw_op in _OP_ALIASES:
                op["op"] = _OP_ALIASES[raw_op]
        return data

    patch_id: str = Field(..., description="Unique patch identifier")
    run_id: str | None = Field(None, description="Run ID that triggered this patch")
    rationale: str = Field(..., description="Explanation of why this patch is needed")
    operations: list[PatchOperation] = Field(
        ...,
        min_length=1,
        description="Ordered list of patch operations",
    )
    confidence: float | None = Field(
        default=None,
        description="LLM-estimated fix confidence 0.0-1.0. Below 0.7 auto-escalates to human review.",
    )
    category: str | None = Field(
        default=None,
        description="Failure category (e.g. schema_drift, bad_path, oom_config, sql_column_not_found).",
    )
    root_cause: str | None = Field(
        default=None,
        description="LLM-identified root cause of the failure.",
    )
    misc: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Bucket for unknown top-level keys the LLM emitted. Never "
            "participates in blueprint mutation; preserved for human-eye "
            "review and post-mortem analytics. Common keys: `examples`, "
            "`notes`, `verified_by`, `references`."
        ),
    )
