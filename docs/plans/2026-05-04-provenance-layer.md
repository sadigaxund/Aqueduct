# Phase 19 — Provenance Layer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use executing-plans to implement this plan task-by-task.

**Goal:** Every resolved config value in the Manifest carries provenance metadata (where it came from in the Blueprint source), so the LLM, guardrails, and doctor all operate on compiled reality rather than raw Blueprint YAML structure.

**Architecture:** Add a `ValueProvenance` dataclass and a `ProvenanceMap` (per-module dict of config key → provenance) built during compilation. Pass it through `FailureContext` to the LLM as a flat, annotation-rich skeleton instead of raw Blueprint YAML. Guardrails check resolved values from the Manifest, not raw patch op strings. Patch applier uses provenance to validate ops against Manifest reality.

**Tech Stack:** Python 3.11+, dataclasses (frozen), existing compiler pipeline, existing Pydantic patch grammar. No new dependencies.

---

## Background: What Provenance Solves

Currently the LLM receives:
- Compiled module config (resolved values like `"data/yellow/*.parqut"`)
- Raw Blueprint YAML text (has `${ctx.paths.yellow_path}`)
- Must mentally reverse-engineer: "arcade-expanded module → arcade → context_override → context key"

This breaks when:
1. Arcade-expanded modules (`yellow_process__ingress`) don't exist in Blueprint YAML — LLM generates `set_module_config_key` targeting a non-existent module_id
2. Guardrails see `${ctx.paths.yellow_path}` in a patch op — string doesn't match `allowed_paths` patterns
3. Doctor parsed raw Blueprint, missed arcade sub-blueprints (fixed with workaround, but still fragile)

With provenance:
- LLM sees: `yellow_process__ingress.config.path = "data/yellow/*.parqut"` + annotation: `source: context_ref, context_key: paths.yellow_path, defined_in: blueprints/NYC_Taxi_Demo.yml`
- LLM generates `replace_context_value` with key `paths.yellow_path` — correct, directly applicable
- Guardrails check the Manifest's resolved `paths.yellow_path` value against `allowed_paths` — no `${ctx.*}` ambiguity
- Doctor runs against the Manifest (already arcade-expanded, already resolved) — no workarounds needed

---

## Data Model

### `ValueProvenance` (new, in `aqueduct/compiler/provenance.py`)

```python
@dataclass(frozen=True)
class ValueProvenance:
    source_type: Literal["literal", "context_ref", "env_ref", "tier1", "arcade_inherited"]
    # For context_ref: the dot-notation key in the parent blueprint context block
    context_key: str | None = None
    # For arcade_inherited: parent blueprint file and arcade module ID
    arcade_module_id: str | None = None       # e.g. "yellow_process"
    arcade_sub_module_id: str | None = None   # original sub-module id before namespacing, e.g. "ingress"
    sub_blueprint_path: str | None = None     # relative path, e.g. "arcades/taxi_processor.yml"
    # For env_ref: the env var name
    env_var: str | None = None
    # Original unresolved expression (always set)
    original_expression: str | None = None    # e.g. "${ctx.paths.yellow_path}" or "@aq.date.today()"
    # Resolved (compiled) value — for quick reference without re-lookup
    resolved_value: Any = None
```

### `ModuleProvenance` (new, in `aqueduct/compiler/provenance.py`)

```python
@dataclass(frozen=True)
class ModuleProvenance:
    module_id: str           # Manifest module ID (may be namespaced: "yellow_process__ingress")
    module_type: str
    # Arcade lineage — None for top-level modules
    arcade_module_id: str | None = None      # parent arcade module ID in Blueprint
    sub_blueprint_path: str | None = None    # relative path to the arcade sub-blueprint
    original_module_id: str | None = None    # module id inside the sub-blueprint (pre-namespacing)
    # Per-config-key provenance (dot-notation key → provenance)
    config: dict[str, ValueProvenance] = field(default_factory=dict)
```

### `ProvenanceMap` (new, in `aqueduct/compiler/provenance.py`)

```python
@dataclass(frozen=True)
class ProvenanceMap:
    blueprint_id: str
    blueprint_path: str  # absolute path
    modules: dict[str, ModuleProvenance]   # module_id → ModuleProvenance
    context: dict[str, ValueProvenance]    # context key → provenance for context values

    def for_module(self, module_id: str) -> ModuleProvenance | None:
        return self.modules.get(module_id)

    def to_dict(self) -> dict: ...  # JSON-serializable form for FailureContext
```

---

## Critical Files

| File | Change |
|---|---|
| `aqueduct/compiler/provenance.py` | **NEW** — `ValueProvenance`, `ModuleProvenance`, `ProvenanceMap` + builder helpers |
| `aqueduct/parser/resolver.py` | Extend `resolve_value()` to optionally return provenance alongside resolved value |
| `aqueduct/compiler/compiler.py` | Build `ProvenanceMap` during compile; return alongside Manifest |
| `aqueduct/compiler/expander.py` | Tag expanded modules with arcade lineage during `_expand_single()` |
| `aqueduct/compiler/models.py` | Add `provenance_map: ProvenanceMap | None = None` to `Manifest` |
| `aqueduct/surveyor/models.py` | Replace `blueprint_source_yaml` with `provenance_json` in `FailureContext` |
| `aqueduct/surveyor/llm.py` | Replace raw YAML section with provenance-annotated skeleton in user prompt |
| `aqueduct/patch/apply.py` | `_check_guardrails()`: resolve path values via ProvenanceMap before matching |
| `aqueduct/cli.py` | `_check_guardrails()`: same fix; pass provenance to FailureContext builder |
| `aqueduct/doctor.py` | Replace `check_blueprint_sources()` implementation to use Manifest (compile then check) |
| `.dev/TESTING.md` | Add provenance test checklist |

---

## Task 1: Create `aqueduct/compiler/provenance.py`

**Files:**
- Create: `aqueduct/compiler/provenance.py`

**What to build:**

```python
"""Provenance layer — tracks the source of every resolved config value in the Manifest.

Each resolved value carries metadata about whether it came from:
- A literal YAML value
- A ${ctx.key} context reference (and which key)
- A ${ENV_VAR} environment variable
- An @aq.* Tier 1 function call
- An arcade context_override injection (inherited from a parent blueprint)

This is built during compilation and passed to the LLM in FailureContext so the
LLM can generate correct patch ops without needing to understand Blueprint YAML
structure or arcade expansion.
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
    source_type: Literal["literal", "context_ref", "env_ref", "tier1", "arcade_inherited"]
    context_key: str | None = None
    arcade_module_id: str | None = None
    arcade_sub_module_id: str | None = None
    sub_blueprint_path: str | None = None
    env_var: str | None = None
    original_expression: str | None = None
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
    module_id: str
    module_type: str
    arcade_module_id: str | None = None
    sub_blueprint_path: str | None = None
    original_module_id: str | None = None
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
    blueprint_id: str
    blueprint_path: str
    modules: dict[str, ModuleProvenance]
    context: dict[str, ValueProvenance]

    def for_module(self, module_id: str) -> ModuleProvenance | None:
        return self.modules.get(module_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "blueprint_id": self.blueprint_id,
            "blueprint_path": self.blueprint_path,
            "modules": {mid: m.to_dict() for mid, m in self.modules.items()},
            "context": {k: v.to_dict() for k, v in self.context.items()},
        }


def infer_value_provenance(
    original_expr: Any,
    resolved_value: Any,
    *,
    arcade_module_id: str | None = None,
    arcade_sub_module_id: str | None = None,
    sub_blueprint_path: str | None = None,
) -> ValueProvenance:
    """Infer the provenance of a single resolved scalar value.

    If arcade_* args are set, wraps the base provenance as arcade_inherited.
    """
    if not isinstance(original_expr, str):
        # Non-string literal (int, bool, list, dict) — treat as literal
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
            env_var=env_match.group(1) if env_match else None,
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
    raw_config: dict[str, Any],
    resolved_config: dict[str, Any],
    *,
    arcade_module_id: str | None = None,
    arcade_sub_module_id: str | None = None,
    sub_blueprint_path: str | None = None,
    prefix: str = "",
) -> dict[str, ValueProvenance]:
    """Recursively build per-key provenance for a module config dict.

    Returns flat dot-notation keys → ValueProvenance.
    Only tracks leaf scalar values (string, int, bool, float).
    Dict values are recursed; list values are tracked at the list level.
    """
    result: dict[str, ValueProvenance] = {}
    for key, raw_val in (raw_config or {}).items():
        full_key = f"{prefix}.{key}" if prefix else key
        resolved_val = (resolved_config or {}).get(key)
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
```

**Validation:** Import in Python shell, construct a `ValueProvenance`, call `to_dict()`, verify JSON-serializable.

---

## Task 2: Extend `compiler.py` to build `ProvenanceMap`

**Files:**
- Modify: `aqueduct/compiler/compiler.py`
- Modify: `aqueduct/compiler/models.py`

### 2a — `models.py`: add `provenance_map` to `Manifest`

```python
# Add import at top:
from aqueduct.compiler.provenance import ProvenanceMap

# Add field to Manifest dataclass (after `checkpoint`):
provenance_map: "ProvenanceMap | None" = None
```

`to_dict()` — add:
```python
"provenance_map": self.provenance_map.to_dict() if self.provenance_map else None,
```

### 2b — `compiler.py`: build ProvenanceMap

After step 3 (Tier 1 module config resolution), before step 4 (arcade expansion):

```python
from aqueduct.compiler.provenance import (
    ModuleProvenance, ProvenanceMap, ValueProvenance, build_config_provenance,
    infer_value_provenance,
)

# Build context provenance (compare raw blueprint context vs resolved_ctx)
raw_context = dict(blueprint.context.values)  # pre-Tier1 context values (already Tier 0 resolved by parser)
context_provenance: dict[str, ValueProvenance] = {}
for key, resolved_val in resolved_ctx.items():
    raw_val = raw_context.get(key, resolved_val)
    context_provenance[key] = infer_value_provenance(raw_val, resolved_val)

# Build per-module provenance (pre-arcade-expansion, top-level modules only)
# Arcade-expanded provenance is added in expander (Task 3).
raw_modules_by_id = {m.id: m for m in blueprint.modules}
module_provenance: dict[str, ModuleProvenance] = {}
for m in modules:
    if m.type == "Arcade":
        continue  # arcade provenance built during expansion
    raw_m = raw_modules_by_id.get(m.id)
    raw_cfg = raw_m.config if raw_m else {}
    config_prov = build_config_provenance(raw_cfg, m.config)
    module_provenance[m.id] = ModuleProvenance(
        module_id=m.id,
        module_type=m.type,
        config=config_prov,
    )
```

After step 4 (arcade expansion), collect provenance injected by expander (see Task 3):

```python
# _expand_arcades now returns (modules, edges, arcade_provenance_entries)
# Merge arcade entries into module_provenance
```

At the end, construct and attach `ProvenanceMap`:

```python
prov_map = ProvenanceMap(
    blueprint_id=blueprint.id,
    blueprint_path=str(blueprint_path) if blueprint_path else "",
    modules=module_provenance,
    context=context_provenance,
)
return Manifest(
    ...
    provenance_map=prov_map,
)
```

**Note on arcade provenance:** The expander in Task 3 must return provenance alongside expanded modules. The simplest approach is a side-channel dict `{module_id: ModuleProvenance}` returned alongside `(modules, edges)`.

---

## Task 3: Tag arcade-expanded modules with provenance in `expander.py`

**Files:**
- Modify: `aqueduct/compiler/expander.py`

### Change `_expand_single()` signature and return type

```python
def _expand_single(
    arcade: Module,
    sub_bp: Blueprint,
    parent_edges: list[Edge],
    sub_blueprint_path: str,
) -> tuple[list[Module], list[Edge], dict[str, ModuleProvenance]]:
```

Inside `_expand_single`, after building `expanded_modules`, build provenance:

```python
from aqueduct.compiler.provenance import ModuleProvenance, build_config_provenance

arcade_provenance: dict[str, ModuleProvenance] = {}
for orig_m, expanded_m in zip(sub_bp.modules, expanded_modules):
    # orig_m has raw config (pre-arcade context_override resolution),
    # expanded_m has resolved config (after arcade context_override injected as cli_overrides)
    # We need the raw sub-blueprint config before context injection:
    # sub_bp was parsed WITH arcade context_override as cli_overrides, so
    # orig_m.config already has context refs resolved. We must track by comparing
    # the original expression stored in context_override keys.
    config_prov = build_config_provenance(
        orig_m.config,    # NOTE: this is already resolved — see caveat below
        expanded_m.config,
        arcade_module_id=arcade.id,
        arcade_sub_module_id=orig_m.id.split("__", 1)[-1] if "__" in orig_m.id else orig_m.id,
        sub_blueprint_path=sub_blueprint_path,
    )
    arcade_provenance[expanded_m.id] = ModuleProvenance(
        module_id=expanded_m.id,
        module_type=expanded_m.type,
        arcade_module_id=arcade.id,
        sub_blueprint_path=sub_blueprint_path,
        original_module_id=orig_m.id,
        config=config_prov,
    )
return expanded_modules, internal_edges + rewired, arcade_provenance
```

**Caveat on raw config:** `sub_bp` was parsed with `cli_overrides=arcade.context_override`, so context refs in sub-blueprint config are already resolved. To get the original expressions (e.g. `${ctx.input_path}`), we need the sub-blueprint's raw YAML config before parsing. Simplest approach: parse twice — once with overrides (for execution), once without (for provenance original expressions, catching `ParseError` on undefined refs and storing the raw YAML value). Actually simpler: read raw YAML with `yaml.safe_load` just to extract config strings, don't resolve — use those as `original_expression` in `build_config_provenance`.

Recommended: add a `_load_raw_module_configs(sub_path: Path) -> dict[str, dict]` helper that loads YAML with pyyaml (no resolution) and returns `{module_id: config_dict}`.

### Update `_expand_recursive()` to thread provenance through

```python
def _expand_recursive(
    modules, edges, base_dir, depth
) -> tuple[list[Module], list[Edge], dict[str, ModuleProvenance]]:
```

Collect all `arcade_provenance` dicts from each arcade expansion and merge.

### Update `expand_arcades()` public API

```python
def expand_arcades(
    modules, edges, base_dir
) -> tuple[list[Module], list[Edge], dict[str, ModuleProvenance]]:
```

Return the merged provenance dict alongside modules and edges.

### Update `compiler.py` call site

```python
modules, edges, arcade_prov = expand_arcades(modules, edges, blueprint_path.parent)
module_provenance.update(arcade_prov)
```

---

## Task 4: Replace `blueprint_source_yaml` with `provenance_json` in `FailureContext`

**Files:**
- Modify: `aqueduct/surveyor/models.py`
- Modify: `aqueduct/cli.py` (FailureContext construction site)

### `models.py`

```python
# Replace:
blueprint_source_yaml: str | None = None

# With:
provenance_json: str | None = None   # JSON-serialized ProvenanceMap.to_dict() for failed module + context
```

Update `to_dict()` and `to_json()` accordingly.

### `cli.py` — FailureContext construction

Find where `FailureContext` is built (around the `blueprint_source_yaml=...` line). Replace:

```python
# OLD
blueprint_source_yaml=blueprint_abs.read_text(encoding="utf-8") if blueprint_abs.exists() else None,

# NEW — only pass provenance for the failing module + context (not all modules)
_prov_json = None
if manifest.provenance_map:
    failed_mod_prov = manifest.provenance_map.for_module(failed_module)
    _prov_slice = {
        "blueprint_id": manifest.provenance_map.blueprint_id,
        "blueprint_path": manifest.provenance_map.blueprint_path,
        "failed_module": failed_mod_prov.to_dict() if failed_mod_prov else None,
        "context": {k: v.to_dict() for k, v in manifest.provenance_map.context.items()},
    }
    import json as _json
    _prov_json = _json.dumps(_prov_slice, indent=2)

failure_ctx = FailureContext(
    ...
    provenance_json=_prov_json,
    ...
)
```

---

## Task 5: Replace raw YAML section in LLM prompt with provenance skeleton

**Files:**
- Modify: `aqueduct/surveyor/llm.py`

### Remove `blueprint_source_section` logic

Delete the `if failure_ctx.blueprint_source_yaml:` block.

### Add `provenance_section` logic

```python
def _build_provenance_section(provenance_json: str | None) -> str:
    if not provenance_json:
        return ""
    try:
        prov = json.loads(provenance_json)
    except Exception:
        return ""

    lines = ["\n## Value provenance (where each config value comes from in the source Blueprint)"]
    lines.append(f"Blueprint: {prov.get('blueprint_path', '?')}")

    failed_mod = prov.get("failed_module")
    if failed_mod:
        lines.append(f"\n### Failed module: {failed_mod['module_id']} ({failed_mod['module_type']})")
        if failed_mod.get("arcade_module_id"):
            lines.append(
                f"  Arcade: expanded from arcade module `{failed_mod['arcade_module_id']}` "
                f"(sub-blueprint: {failed_mod.get('sub_blueprint_path', '?')})"
            )
            lines.append(
                f"  Original sub-module ID: `{failed_mod.get('original_module_id', '?')}`"
            )
            lines.append(
                "  NOTE: This module does NOT exist in the Blueprint YAML. "
                "Patch the arcade module or its context values instead."
            )
        for key, vp in (failed_mod.get("config") or {}).items():
            src = vp.get("source_type", "?")
            orig = vp.get("original_expression", "?")
            resolved = vp.get("resolved_value", "?")
            if src == "context_ref":
                ctx_key = vp.get("context_key", "?")
                lines.append(
                    f"  config.{key} = {resolved!r}  "
                    f"← context ref: use replace_context_value(key={ctx_key!r}, value=<fix>)"
                )
            elif src == "arcade_inherited":
                ctx_key = vp.get("context_key")
                if ctx_key:
                    lines.append(
                        f"  config.{key} = {resolved!r}  "
                        f"← arcade context_override from context key {ctx_key!r}: "
                        f"use replace_context_value(key={ctx_key!r}, value=<fix>)"
                    )
                else:
                    lines.append(f"  config.{key} = {resolved!r}  ← arcade_inherited (expr: {orig})")
            elif src == "env_ref":
                env_var = vp.get("env_var", "?")
                lines.append(f"  config.{key} = {resolved!r}  ← env var ${env_var}")
            elif src == "tier1":
                lines.append(f"  config.{key} = {resolved!r}  ← @aq.* expression: {orig}")
            else:
                lines.append(f"  config.{key} = {resolved!r}  ← literal")

    # Context block summary
    ctx_block = prov.get("context") or {}
    if ctx_block:
        lines.append("\n### Blueprint context values (editable via replace_context_value)")
        for key, vp in ctx_block.items():
            resolved = vp.get("resolved_value", "?")
            src = vp.get("source_type", "?")
            orig = vp.get("original_expression", "?")
            hint = f" (env: ${vp['env_var']})" if src == "env_ref" else ""
            lines.append(f"  {key} = {resolved!r}{hint}")

    lines.append("")
    return "\n".join(lines)
```

Update `_USER_PROMPT_TEMPLATE`:
- Replace `{blueprint_source_section}` with `{provenance_section}`
- Update `_build_user_prompt()` to call `_build_provenance_section(failure_ctx.provenance_json)`

### Update system prompt rules

Replace the rules about `${ctx.*}` in SQL and arcade modules with a single, clearer rule:

```
- The "Value provenance" section tells you exactly where each config value comes from. Always read it before generating a patch.
- For context_ref values: use replace_context_value with the dot-notation key shown.
- For arcade_inherited values: the module does not exist in Blueprint YAML. Use replace_context_value on the context key shown (which the arcade inherits via context_override).
- For literal values: use set_module_config_key directly on the module (it exists in Blueprint YAML).
- For env_ref values: the value comes from an environment variable — do not patch it; inform the user.
- NEVER use set_module_config_key on a module whose ID contains `__` — those are arcade-expanded and don't exist in the Blueprint YAML.
```

---

## Task 6: Fix `_check_guardrails()` in `apply.py` to use resolved values

**Files:**
- Modify: `aqueduct/patch/apply.py`

The current `_check_guardrails()` reads raw Blueprint YAML path values. The fix: for `set_module_config_key` ops with `key="path"`, check the Manifest's resolved value (from ProvenanceMap), not the raw patch op value which may be `${ctx.*}`.

```python
def _check_guardrails(
    patch_spec: PatchSpec,
    bp_raw: dict,
    provenance_map: "ProvenanceMap | None" = None,
) -> None:
    guardrails = (bp_raw.get("agent") or {}).get("guardrails") or {}
    forbidden_ops: list[str] = guardrails.get("forbidden_ops") or []
    allowed_paths: list[str] = guardrails.get("allowed_paths") or []

    for op in patch_spec.operations:
        op_name = op.op
        if op_name in forbidden_ops:
            raise PatchError(f"Operation {op_name!r} is forbidden by agent.guardrails.forbidden_ops.")

        if allowed_paths and op_name == "set_module_config_key":
            key = getattr(op, "key", None)
            if key in ("path", "output_path"):
                value = str(getattr(op, "value", "") or "")
                # If value is a context ref, resolve it via provenance map
                if value.startswith("${ctx.") and provenance_map:
                    ctx_key = value[len("${ctx."):-1]  # strip ${ctx. and }
                    ctx_prov = provenance_map.context.get(ctx_key)
                    if ctx_prov and ctx_prov.resolved_value is not None:
                        value = str(ctx_prov.resolved_value)
                if not any(fnmatch.fnmatch(value, pat) for pat in allowed_paths):
                    raise PatchError(
                        f"Path value {value!r} in op {op_name!r} (module {getattr(op, 'module_id', '?')!r}) "
                        f"does not match any agent.guardrails.allowed_paths pattern: {allowed_paths}"
                    )
```

Pass provenance_map from `apply_patch_file()` → requires the caller to supply it. Two options:
1. Accept `provenance_map` as an optional arg to `apply_patch_file()` (preferred)
2. Re-compile the Blueprint to get it (expensive, avoid)

Update `apply_patch_file()` signature:
```python
def apply_patch_file(
    blueprint_path: Path,
    patch_path: Path,
    patches_dir: Path = Path("patches"),
    provenance_map: "ProvenanceMap | None" = None,
) -> ApplyResult:
```

Update the `_check_guardrails(patch_spec, bp_raw)` call:
```python
_check_guardrails(patch_spec, bp_raw, provenance_map=provenance_map)
```

Update `cli.py` to pass the provenance_map (already in manifest) to `apply_patch_file()`.

Also remove the duplicate `_check_guardrails` in `cli.py` — consolidate to `apply.py` version. The `cli.py` version was a pre-LLM check; move that logic into the `apply.py` version which runs pre-apply anyway.

---

## Task 7: Simplify `doctor.py` to use the Manifest

**Files:**
- Modify: `aqueduct/doctor.py`

Currently `check_blueprint_sources()` calls `parse()` and manually recurses into arcades. Replace with a compile-and-inspect approach when possible.

```python
def check_blueprint_sources_from_manifest(manifest: "Manifest") -> list[CheckResult]:
    """Check all Ingress/Egress paths using the already-compiled Manifest.

    Advantages over parsing:
    - Arcade-expanded modules are already flat
    - All context refs are resolved — no ${ctx.*} placeholders
    - No workarounds for arcade context injection needed
    """
    import re
    import socket
    results = []

    for module in manifest.modules:
        if module.type not in ("Ingress", "Egress"):
            continue
        cfg = module.config if isinstance(module.config, dict) else {}
        fmt = cfg.get("format", "")
        path_val = cfg.get("path")
        # ... same logic as current check_blueprint_sources but path_val is already resolved
        # No ${ctx.*} refs will appear — values are fully resolved strings
        ...
    return results
```

Keep `check_blueprint_sources(blueprint_path, _context_override)` as a fallback for the standalone `aqueduct doctor --blueprint` case (where no Manifest is available). But in `run`, use `check_blueprint_sources_from_manifest(manifest)` after compilation.

Update `cli.py` doctor hints injection (before LLM call) to use `check_blueprint_sources_from_manifest(manifest)` instead of re-parsing the blueprint file.

---

## Task 8: Update TESTING.md

**Files:**
- Modify: `.dev/TESTING.md`

Add section:

```
## Provenance Layer

### ValueProvenance / build_config_provenance
⏳ literal value → source_type="literal"
⏳ ${ctx.paths.foo} → source_type="context_ref", context_key="paths.foo"
⏳ ${ENV_VAR} → source_type="env_ref", env_var="ENV_VAR"
⏳ @aq.date.today() → source_type="tier1"
⏳ nested config dict → flattened to dot-notation keys

### ProvenanceMap via compile()
⏳ top-level module with literal path → ModuleProvenance with literal provenance
⏳ top-level module with ${ctx.path} → context_ref provenance, context_key correct
⏳ arcade-expanded module → arcade_module_id, sub_blueprint_path, original_module_id set
⏳ arcade-expanded module with context_override key → arcade_inherited source_type
⏳ context values tracked with correct source_type

### FailureContext provenance_json
⏳ replaces blueprint_source_yaml
⏳ contains only failed module provenance + full context block
⏳ JSON-parseable, no pyspark types

### LLM prompt provenance section
⏳ arcade-expanded module: shows "does not exist in Blueprint YAML" warning
⏳ context_ref value: shows "use replace_context_value(key=...)"
⏳ literal value: shows "use set_module_config_key directly"
⏳ env_ref value: shows env var name, no patch suggestion

### Guardrails (apply.py)
⏳ set_module_config_key path=${ctx.foo}: resolves via provenance_map before matching
⏳ set_module_config_key path=literal: matches normally
⏳ replace_context_value is never path-checked (it's not a set_module_config_key op)

### Doctor with Manifest
⏳ check_blueprint_sources_from_manifest: arcade-expanded modules included
⏳ no ${ctx.*} refs in path_val (all resolved)
⏳ format mismatch detected on resolved path
```

---

## Implementation Order

Run in this order — each task builds on the previous:

1. **Task 1** — `provenance.py` (pure new file, no dependencies)
2. **Task 2a** — `Manifest.provenance_map` field (model change, backward-compatible: defaults None)
3. **Task 3** — expander returns arcade provenance (changes return type)
4. **Task 2b** — compiler builds ProvenanceMap (uses expander's new return type)
5. **Task 4** — FailureContext swap `blueprint_source_yaml` → `provenance_json`
6. **Task 5** — LLM prompt update (uses new FailureContext field)
7. **Task 6** — Guardrails fix in `apply.py`
8. **Task 7** — Doctor simplification
9. **Task 8** — TESTING.md

---

## Backward Compatibility Notes

- `Manifest.provenance_map` defaults to `None` — existing tests constructing minimal Manifests are unaffected
- `FailureContext.blueprint_source_yaml` removal is a breaking change to the `to_dict()` / `to_json()` schema — any existing stored FailureContext JSONs in obs.db will have the old field. This is acceptable (stored records are diagnostic, not load-bearing)
- `expand_arcades()` return type changes from `(list, list)` to `(list, list, dict)` — the compiler is the only caller; update it in Task 2b
- `apply_patch_file()` gains an optional `provenance_map` param — backward compatible

---

## Verification

```bash
# 1. Provenance unit test
python -c "
from aqueduct.compiler.provenance import infer_value_provenance
p = infer_value_provenance('\${ctx.paths.yellow_path}', 'data/yellow/*.parquet')
assert p.source_type == 'context_ref'
assert p.context_key == 'paths.yellow_path'
print('OK:', p)
"

# 2. Compile and inspect provenance
python -c "
from pathlib import Path
from aqueduct.parser.parser import parse
from aqueduct.compiler.compiler import compile
bp = parse('blueprints/NYC_Taxi_Demo.yml')
manifest = compile(bp, blueprint_path=Path('blueprints/NYC_Taxi_Demo.yml'))
pmap = manifest.provenance_map
print('Modules with provenance:', list(pmap.modules.keys()))
taxi_prov = pmap.for_module('yellow_process__ingress')
print('yellow_process__ingress provenance:', taxi_prov)
print('path provenance:', taxi_prov.config.get('path'))
"

# 3. Guardrails: ${ctx.*} path no longer blocked
# Apply a patch with path=${ctx.paths.yellow_path}  — should pass guardrails now

# 4. LLM prompt: verify provenance section appears
# Trigger a failure, check that provenance_section appears instead of raw YAML
```
