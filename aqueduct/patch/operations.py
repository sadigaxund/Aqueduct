"""Patch operation implementations — each mutates a raw Blueprint YAML dict.

Every function receives a deep copy of the Blueprint dict and returns the
modified dict.  If the operation cannot be applied (module not found, edge
not found, key missing) a PatchOperationError is raised immediately.

The caller (apply.py) is responsible for deep-copying before the first
operation and for rolling back (discarding the modified dict) if any
operation fails.

Blueprint dict shape (mirrors YAML structure):
    {
        "aqueduct": "1.0",
        "id": "...",
        "modules": [{"id": "...", "type": "...", "config": {...}}, ...],
        "edges":   [{"from": "...", "to": "...", "port": "main"}, ...],
        "context": {"key": "value", "nested": {"sub": "value"}},
        "retry_policy": {...},   # optional
        ...
    }
"""

from __future__ import annotations

from io import StringIO
from typing import Any

from ruamel.yaml import YAML as _YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString as _DQ

_ryaml = _YAML()
_ryaml.preserve_quotes = True
_ryaml.default_flow_style = False
_ryaml.width = 4096
_ryaml.indent(mapping=2, sequence=4, offset=2)


def _quote_strings(data: Any) -> Any:
    """Recursively wrap str values in DoubleQuotedScalarString.

    Ensures strings that look like YAML booleans/numbers ('true', 'false', '1')
    and template expressions ('${ctx.*}', '@aq.*') are always double-quoted in output.
    """
    if isinstance(data, dict):
        return {k: _quote_strings(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_quote_strings(v) for v in data]
    if isinstance(data, str):
        return _DQ(data)
    return data


def _to_ruamel(data: Any) -> Any:
    """Convert plain Python dict/list to ruamel CommentedMap/CommentedSeq with double-quoted strings."""
    buf = StringIO()
    _ryaml.dump(_quote_strings(data), buf)
    return _ryaml.load(buf.getvalue())

from aqueduct.patch.grammar import (
    AddArcadeRefOp,
    AddProbeOp,
    InsertModuleOp,
    RemoveModuleOp,
    ReplaceContextValueOp,
    ReplaceEdgeOp,
    ReplaceModuleConfigOp,
    ReplaceModuleLabelOp,
    ReplaceRetryPolicyOp,
    SetModuleConfigKeyOp,
    SetModuleOnFailureOp,
)


class PatchOperationError(Exception):
    """Raised when an operation cannot be applied to the Blueprint."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_module(bp: dict, module_id: str) -> dict:
    """Return the module dict with the given id.  Raises PatchOperationError if missing."""
    for m in bp.get("modules", []):
        if m.get("id") == module_id:
            return m
    raise PatchOperationError(
        f"Module {module_id!r} not found in Blueprint. "
        f"Available: {[m.get('id') for m in bp.get('modules', [])]}"
    )


def _find_edge_index(bp: dict, from_id: str, to_id: str) -> int:
    """Return the list index of the edge matching from_id → to_id.
    Raises PatchOperationError if not found.
    """
    for i, edge in enumerate(bp.get("edges", [])):
        if edge.get("from") == from_id and edge.get("to") == to_id:
            return i
    raise PatchOperationError(
        f"Edge {from_id!r} → {to_id!r} not found in Blueprint."
    )


def _set_nested(d: dict, dot_key: str, value: Any) -> None:
    """Set a nested dict value using dot-notation key.

    e.g. _set_nested(d, "paths.input", "/new/path") sets d["paths"]["input"] = "/new/path"
    """
    parts = dot_key.split(".")
    node = d
    for part in parts[:-1]:
        if part not in node or not isinstance(node[part], dict):
            raise PatchOperationError(
                f"Context key path {dot_key!r} invalid: "
                f"{part!r} not found or not a dict at this level."
            )
        node = node[part]
    node[parts[-1]] = value


def _remove_edges_matching(bp: dict, edge_specs: list[dict]) -> None:
    """Remove edges from bp["edges"] that match any spec in edge_specs.

    A spec matches if it shares the same "from" and "to" values.
    """
    for spec in edge_specs:
        from_id = spec.get("from")
        to_id = spec.get("to")
        original_len = len(bp.get("edges", []))
        bp["edges"] = [
            e for e in bp.get("edges", [])
            if not (e.get("from") == from_id and e.get("to") == to_id)
        ]
        if len(bp.get("edges", [])) == original_len:
            raise PatchOperationError(
                f"edges_to_remove specifies edge {from_id!r} → {to_id!r} "
                f"which does not exist in the Blueprint."
            )


def _add_edges(bp: dict, edge_specs: list[dict]) -> None:
    """Append edges from edge_specs to bp["edges"]."""
    bp.setdefault("edges", [])
    for spec in edge_specs:
        if "from" not in spec or "to" not in spec:
            raise PatchOperationError(
                f"Edge spec missing 'from' or 'to': {spec!r}"
            )
        bp["edges"].append(spec)


# ── Operation implementations ─────────────────────────────────────────────────

def apply_replace_module_config(bp: dict, op: ReplaceModuleConfigOp) -> dict:
    """Replace config block of a named Module."""
    module = _find_module(bp, op.module_id)
    module["config"] = _to_ruamel(op.config)
    return bp


def apply_replace_module_label(bp: dict, op: ReplaceModuleLabelOp) -> dict:
    """Update the label of a Module."""
    module = _find_module(bp, op.module_id)
    module["label"] = op.label
    return bp


def apply_insert_module(bp: dict, op: InsertModuleOp) -> dict:
    """Insert a new Module and rewire edges."""
    new_id = op.module.get("id")
    if not new_id:
        raise PatchOperationError("insert_module: 'module.id' is required")

    # Guard: module ID must not already exist
    existing_ids = [m.get("id") for m in bp.get("modules", [])]
    if new_id in existing_ids:
        raise PatchOperationError(
            f"insert_module: module {new_id!r} already exists in Blueprint."
        )

    bp.setdefault("modules", []).append(_to_ruamel(op.module))
    _remove_edges_matching(bp, op.edges_to_remove)
    _add_edges(bp, op.edges_to_add)
    return bp


def apply_remove_module(bp: dict, op: RemoveModuleOp) -> dict:
    """Remove a Module and all its edges; optionally add replacement edges."""
    _find_module(bp, op.module_id)  # asserts existence

    bp["modules"] = [m for m in bp.get("modules", []) if m.get("id") != op.module_id]
    # Remove all edges that reference this module
    bp["edges"] = [
        e for e in bp.get("edges", [])
        if e.get("from") != op.module_id and e.get("to") != op.module_id
    ]
    _add_edges(bp, op.edges_to_add)
    return bp


def apply_replace_context_value(bp: dict, op: ReplaceContextValueOp) -> dict:
    """Update a context value using dot-notation key."""
    context = bp.get("context")
    if context is None:
        raise PatchOperationError("Blueprint has no 'context' block.")
    _set_nested(context, op.key, op.value)
    return bp


def apply_add_probe(bp: dict, op: AddProbeOp) -> dict:
    """Add a new Probe module and optional signal-port edges."""
    probe_id = op.module.get("id")
    if not probe_id:
        raise PatchOperationError("add_probe: 'module.id' is required")

    if op.module.get("type") != "Probe":
        raise PatchOperationError(
            f"add_probe: module.type must be 'Probe', got {op.module.get('type')!r}"
        )

    attach_to = op.module.get("attach_to")
    if not attach_to:
        raise PatchOperationError("add_probe: 'module.attach_to' is required")

    # Verify attach_to target exists
    _find_module(bp, attach_to)

    bp.setdefault("modules", []).append(_to_ruamel(op.module))
    _add_edges(bp, op.edges_to_add)
    return bp


def apply_replace_edge(bp: dict, op: ReplaceEdgeOp) -> dict:
    """Rewire an existing edge."""
    if op.new_from_id is None and op.new_to_id is None and op.new_port is None:
        raise PatchOperationError(
            "replace_edge: at least one of new_from_id, new_to_id, new_port must be set"
        )

    idx = _find_edge_index(bp, op.from_id, op.to_id)
    edge = bp["edges"][idx]

    if op.new_from_id is not None:
        edge["from"] = op.new_from_id
    if op.new_to_id is not None:
        edge["to"] = op.new_to_id
    if op.new_port is not None:
        edge["port"] = op.new_port
    return bp


def apply_set_module_config_key(bp: dict, op: SetModuleConfigKeyOp) -> dict:
    """Set a single dot-notation key inside a Module's config, leaving other keys intact."""
    module = _find_module(bp, op.module_id)
    if "config" not in module or module["config"] is None:
        module["config"] = {}
    value = _to_ruamel(op.value) if isinstance(op.value, (dict, list)) else op.value
    if isinstance(op.value, str):
        from ruamel.yaml.scalarstring import DoubleQuotedScalarString
        value = DoubleQuotedScalarString(op.value)
    _set_nested(module["config"], op.key, value)
    return bp


def apply_set_module_on_failure(bp: dict, op: SetModuleOnFailureOp) -> dict:
    """Set (or replace) the on_failure block for a Module."""
    module = _find_module(bp, op.module_id)
    module["on_failure"] = op.on_failure
    return bp


def apply_replace_retry_policy(bp: dict, op: ReplaceRetryPolicyOp) -> dict:
    """Replace the blueprint-level retry_policy block."""
    bp["retry_policy"] = op.retry_policy
    return bp


def apply_add_arcade_ref(bp: dict, op: AddArcadeRefOp) -> dict:
    """Add a new Arcade module and rewire edges."""
    arcade_id = op.module.get("id")
    if not arcade_id:
        raise PatchOperationError("add_arcade_ref: 'module.id' is required")

    if op.module.get("type") != "Arcade":
        raise PatchOperationError(
            f"add_arcade_ref: module.type must be 'Arcade', got {op.module.get('type')!r}"
        )

    if not op.module.get("ref"):
        raise PatchOperationError("add_arcade_ref: 'module.ref' is required")

    existing_ids = [m.get("id") for m in bp.get("modules", [])]
    if arcade_id in existing_ids:
        raise PatchOperationError(
            f"add_arcade_ref: module {arcade_id!r} already exists in Blueprint."
        )

    bp.setdefault("modules", []).append(_to_ruamel(op.module))
    _remove_edges_matching(bp, op.edges_to_remove)
    _add_edges(bp, op.edges_to_add)
    return bp


# ── Dispatch table ────────────────────────────────────────────────────────────

_DISPATCH = {
    "replace_module_config":    apply_replace_module_config,
    "set_module_config_key":    apply_set_module_config_key,
    "replace_module_label":     apply_replace_module_label,
    "insert_module":          apply_insert_module,
    "remove_module":          apply_remove_module,
    "replace_context_value":  apply_replace_context_value,
    "add_probe":              apply_add_probe,
    "replace_edge":           apply_replace_edge,
    "set_module_on_failure":  apply_set_module_on_failure,
    "replace_retry_policy":   apply_replace_retry_policy,
    "add_arcade_ref":         apply_add_arcade_ref,
}


def apply_operation(bp: dict, op: Any) -> dict:
    """Dispatch a single PatchOperation to its implementation function."""
    handler = _DISPATCH.get(op.op)
    if handler is None:
        raise PatchOperationError(f"Unknown operation type {op.op!r}")
    return handler(bp, op)
