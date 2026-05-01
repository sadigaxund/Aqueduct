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

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field


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


# ── Discriminated union ───────────────────────────────────────────────────────

PatchOperation = Annotated[
    Union[
        ReplaceModuleConfigOp,
        ReplaceModuleLabelOp,
        InsertModuleOp,
        RemoveModuleOp,
        ReplaceContextValueOp,
        AddProbeOp,
        ReplaceEdgeOp,
        SetModuleOnFailureOp,
        ReplaceRetryPolicyOp,
        AddArcadeRefOp,
    ],
    Field(discriminator="op"),
]


# ── Top-level PatchSpec ───────────────────────────────────────────────────────

class PatchSpec(BaseModel, extra="forbid"):
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

    patch_id: str = Field(..., description="Unique patch identifier")
    run_id: str | None = Field(None, description="Run ID that triggered this patch")
    rationale: str = Field(..., description="Explanation of why this patch is needed")
    operations: list[PatchOperation] = Field(
        ...,
        min_length=1,
        description="Ordered list of patch operations",
    )
