"""Provenance layer — tracks the source of every resolved config value in the Manifest.

Each resolved value carries metadata about whether it came from:
  - A literal YAML value
  - A ${ctx.key} context reference (and which key)
  - A ${ENV_VAR} environment variable reference
  - An @aq.* Tier 1 function call
  - An arcade context_override injection (inherited from a parent blueprint)

This is built during compilation and attached to the Manifest. The LLM receives
a slice of it in FailureContext so it can generate correct patch ops without
needing to understand Blueprint YAML structure or arcade expansion logic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal

_CTX_RE = re.compile(r"\$\{ctx\.([a-zA-Z0-9_.]+)\}")
_ENV_RE = re.compile(r"\$\{(?!ctx\.)([^}:\s]+?)(?::-(.*?))?\}")
_TIER1_RE = re.compile(r"@aq\.")


@dataclass(frozen=True)
class ValueProvenance:
    """Provenance for a single resolved scalar config value."""

    source_type: Literal["literal", "context_ref", "env_ref", "tier1", "arcade_inherited"]
    # For context_ref and arcade_inherited (when the arcade injects a ctx key)
    context_key: str | None = None
    # For arcade_inherited
    arcade_module_id: str | None = None       # e.g. "yellow_process" (parent arcade)
    arcade_sub_module_id: str | None = None   # original sub-module id, e.g. "ingress"
    sub_blueprint_path: str | None = None     # relative path, e.g. "arcades/taxi_processor.yml"
    # For env_ref
    env_var: str | None = None
    # Original unresolved expression from YAML source
    original_expression: str | None = None    # e.g. "${ctx.paths.yellow_path}"
    # Resolved (compiled) value for quick reference
    resolved_value: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in {
            "source_type": self.source_type,
            "context_key": self.context_key,
            "arcade_module_id": self.arcade_module_id,
            "arcade_sub_module_id": self.arcade_sub_module_id,
            "sub_blueprint_path": self.sub_blueprint_path,
            "env_var": self.env_var,
            "original_expression": self.original_expression,
            "resolved_value": self.resolved_value,
        }.items() if v is not None}


@dataclass(frozen=True)
class ModuleProvenance:
    """Provenance for all config keys of a single Manifest module."""

    module_id: str
    module_type: str
    # Set for arcade-expanded modules (ID contains __)
    arcade_module_id: str | None = None       # parent arcade module id in Blueprint
    sub_blueprint_path: str | None = None     # relative path to the arcade sub-blueprint
    original_module_id: str | None = None     # module id inside sub-blueprint pre-namespacing
    # Dot-notation config key → ValueProvenance for each leaf value
    config: dict[str, ValueProvenance] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "module_id": self.module_id,
            "module_type": self.module_type,
            "config": {k: v.to_dict() for k, v in self.config.items()},
        }
        if self.arcade_module_id:
            d["arcade_module_id"] = self.arcade_module_id
            d["sub_blueprint_path"] = self.sub_blueprint_path
            d["original_module_id"] = self.original_module_id
        return d


@dataclass(frozen=True)
class ProvenanceMap:
    """Full provenance index for a compiled Manifest."""

    blueprint_id: str
    blueprint_path: str  # absolute path to the Blueprint file
    modules: dict[str, ModuleProvenance]       # Manifest module_id → ModuleProvenance
    context: dict[str, ValueProvenance]        # context key → provenance for each context value

    def for_module(self, module_id: str) -> ModuleProvenance | None:
        return self.modules.get(module_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "blueprint_id": self.blueprint_id,
            "blueprint_path": self.blueprint_path,
            "modules": {mid: m.to_dict() for mid, m in self.modules.items()},
            "context": {k: v.to_dict() for k, v in self.context.items()},
        }


# ── Provenance inference ──────────────────────────────────────────────────────

def infer_value_provenance(
    original_expr: Any,
    resolved_value: Any,
    *,
    arcade_module_id: str | None = None,
    arcade_sub_module_id: str | None = None,
    sub_blueprint_path: str | None = None,
) -> ValueProvenance:
    """Infer ValueProvenance for a single resolved scalar.

    If arcade_* args are provided, wraps provenance as arcade_inherited,
    preserving the context_key if the original expression was a ${ctx.*} ref.
    """
    if not isinstance(original_expr, str):
        return ValueProvenance(
            source_type="literal",
            original_expression=repr(original_expr),
            resolved_value=resolved_value,
        )

    ctx_match = _CTX_RE.search(original_expr)
    env_match = _ENV_RE.search(original_expr)
    tier1_match = _TIER1_RE.search(original_expr)

    if arcade_module_id:
        return ValueProvenance(
            source_type="arcade_inherited",
            arcade_module_id=arcade_module_id,
            arcade_sub_module_id=arcade_sub_module_id,
            sub_blueprint_path=sub_blueprint_path,
            context_key=ctx_match.group(1) if ctx_match else None,
            env_var=env_match.group(1) if (env_match and not ctx_match) else None,
            original_expression=original_expr,
            resolved_value=resolved_value,
        )
    if ctx_match:
        return ValueProvenance(
            source_type="context_ref",
            context_key=ctx_match.group(1),
            original_expression=original_expr,
            resolved_value=resolved_value,
        )
    if env_match:
        return ValueProvenance(
            source_type="env_ref",
            env_var=env_match.group(1),
            original_expression=original_expr,
            resolved_value=resolved_value,
        )
    if tier1_match:
        return ValueProvenance(
            source_type="tier1",
            original_expression=original_expr,
            resolved_value=resolved_value,
        )
    return ValueProvenance(
        source_type="literal",
        original_expression=original_expr,
        resolved_value=resolved_value,
    )


def build_config_provenance(
    raw_config: dict[str, Any] | None,
    resolved_config: dict[str, Any] | None,
    *,
    arcade_module_id: str | None = None,
    arcade_sub_module_id: str | None = None,
    sub_blueprint_path: str | None = None,
    prefix: str = "",
) -> dict[str, ValueProvenance]:
    """Recursively build per-key provenance for a module config dict.

    Returns flat dot-notation keys → ValueProvenance.
    Dicts are recursed; lists and scalars are tracked at the key level.
    """
    result: dict[str, ValueProvenance] = {}
    for key, raw_val in (raw_config or {}).items():
        full_key = f"{prefix}.{key}" if prefix else key
        resolved_val = (resolved_config or {}).get(key) if resolved_config else raw_val
        if isinstance(raw_val, dict) and isinstance(resolved_val, dict):
            result.update(build_config_provenance(
                raw_val, resolved_val,
                arcade_module_id=arcade_module_id,
                arcade_sub_module_id=arcade_sub_module_id,
                sub_blueprint_path=sub_blueprint_path,
                prefix=full_key,
            ))
        else:
            result[full_key] = infer_value_provenance(
                raw_val, resolved_val,
                arcade_module_id=arcade_module_id,
                arcade_sub_module_id=arcade_sub_module_id,
                sub_blueprint_path=sub_blueprint_path,
            )
    return result
