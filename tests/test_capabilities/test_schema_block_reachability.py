"""Every ``_SCHEMA_BLOCKS`` entry must be reachable from ``BlueprintSchema``.

``capability_leaves.py`` derives BLUEPRINT-grammar leaves: each nested schema
block it walks emits one ``<block>.field.<name>`` row that every registered
engine must give a per-engine verdict for. A model no Blueprint can reach has
no such question to answer — the rows govern a surface the grammar cannot
express, so every engine ends up declaring `supported` for something no engine
implements, or could implement differently.

The closure test cannot see this. It compares each engine's ``capabilities.yml``
against ``all_leaves()`` — i.e. against THIS generator — so a block that drifts
out of the grammar keeps matching perfectly, forever. That is how twelve
``agent_cascade_tier.field.*`` rows per engine outlived the change that made the
Blueprint ``agent:`` block policy-only and moved ``cascade:`` to ``aqueduct.yml``
(where it is core-owned and deliberately excluded from the framework by
``config_leaves.py``'s ``engine_scoped: False`` tag).

The check here is a SECOND, INDEPENDENT source: the pydantic field graph rooted
at ``BlueprintSchema``. It walks annotations rather than reading the registry it
is validating, so the two can genuinely disagree.
"""

from __future__ import annotations

import typing

import pytest
from pydantic import BaseModel

from aqueduct.executor.capability_leaves import _SCHEMA_BLOCKS, all_leaves
from aqueduct.executor.config_leaves import all_config_leaves, core_config_leaves
from aqueduct.parser.schema import (
    MODULE_NESTED_SCHEMA_BLOCKS,
    MODULE_TYPE_SCHEMAS,
    BlueprintSchema,
    DuckDBEngineBlockSchema,
    EngineBlockSchema,
    SparkEngineBlockSchema,
)

pytestmark = pytest.mark.unit

# ── Explicit exemptions for the REVERSE direction ───────────────────────────
# Every pydantic model reachable from BlueprintSchema must be governed by
# _SCHEMA_BLOCKS/MODULE_NESTED_SCHEMA_BLOCKS (the FORWARD direction, checked
# above) OR be named here, with a reason it deliberately emits no
# <block>.field.* leaves of its own.
_EXEMPT_MODELS: dict[type[BaseModel], str] = {
    # The walk root itself — not a "block", the whole grammar.
    BlueprintSchema: "root of the walk, not a nested block",
    # Standing decision: Blueprint engine-block fields (`engine:`) are
    # deliberately NOT capability leaves. The engine-config allowlist
    # (aqueduct/patch/apply.py:442) is the only gate on this healing
    # surface, not a per-engine capability verdict — see also
    # SparkEngineBlockSchema.conf's docstring.
    EngineBlockSchema: "engine-config allowlist is the only gate (patch/apply.py:442)",
    SparkEngineBlockSchema: "engine-config allowlist is the only gate (patch/apply.py:442)",
    DuckDBEngineBlockSchema: "engine-config allowlist is the only gate (patch/apply.py:442)",
    # Module-type root/config schemas: these ARE governed by the capability
    # framework, just via a different, independent mechanism —
    # capability_leaves.py's `_module_type_field_leaves()` walks
    # MODULE_TYPE_SCHEMAS directly (module.type.* / <type>.field.* leaves,
    # including one level into each type's `config:` sub-model). They are
    # not also routed through `_SCHEMA_BLOCKS`, which exists for blocks
    # SHARED across module types (agent, retry_policy, ...).
    **{
        model: "governed via MODULE_TYPE_SCHEMAS / _module_type_field_leaves(), not _SCHEMA_BLOCKS"
        for model in MODULE_TYPE_SCHEMAS.values()
    },
    **{
        model.model_fields["config"].annotation: (
            "governed via MODULE_TYPE_SCHEMAS / _module_type_field_leaves(), not _SCHEMA_BLOCKS"
        )
        for model in MODULE_TYPE_SCHEMAS.values()
        if "config" in model.model_fields
    },
}


def _annotation_types(annotation: object) -> list[object]:
    """Every type mentioned anywhere in an annotation, including inside
    ``list[...]`` / ``dict[str, ...]`` / ``X | None`` wrappers."""
    found: list[object] = [annotation]
    for arg in typing.get_args(annotation):
        found.extend(_annotation_types(arg))
    return found


def _reachable_models(root: type[BaseModel]) -> set[type[BaseModel]]:
    """Transitive closure of pydantic models reachable from ``root`` by
    following field annotations. Independent of ``capability_leaves.py``."""
    seen: set[type[BaseModel]] = set()
    stack: list[type[BaseModel]] = [root]
    while stack:
        model = stack.pop()
        if model in seen:
            continue
        seen.add(model)
        for field in model.model_fields.values():
            for candidate in _annotation_types(field.annotation):
                if (
                    isinstance(candidate, type)
                    and issubclass(candidate, BaseModel)
                    and candidate not in seen
                ):
                    stack.append(candidate)
    return seen


def test_every_schema_block_is_reachable_from_blueprint_schema():
    """The invariant. A block listed here that no Blueprint can reach emits
    per-engine rows for a surface the grammar does not have."""
    reachable = _reachable_models(BlueprintSchema)
    unreachable = sorted(
        f"{prefix} ({model.__name__})" for prefix, model in _SCHEMA_BLOCKS if model not in reachable
    )
    assert unreachable == [], (
        "_SCHEMA_BLOCKS entries not reachable from BlueprintSchema: "
        f"{unreachable}. Each emits <block>.field.* capability rows asking "
        "every engine about grammar no Blueprint can express. Remove the "
        "entry (and re-run `aqueduct dev capabilities sync`), or wire the "
        "model back into BlueprintSchema."
    )


def test_every_reachable_model_is_governed_or_exempt():
    """REVERSE direction of the invariant above: every pydantic model
    reachable from ``BlueprintSchema`` must either be walked for its own
    leaves (``_SCHEMA_BLOCKS`` / ``MODULE_NESTED_SCHEMA_BLOCKS``) or be on
    the explicit exempt list with a stated reason. A model missing from
    all three is Blueprint grammar with NO capability-leaf coverage at
    all — silently ungoverned, the mirror-image failure of the forward
    test (a leaf with no grammar behind it)."""
    reachable = _reachable_models(BlueprintSchema)
    governed = {model for _, model in _SCHEMA_BLOCKS} | {
        model for _, model in MODULE_NESTED_SCHEMA_BLOCKS
    }
    ungoverned = sorted(
        model.__name__
        for model in reachable
        if model not in governed and model not in _EXEMPT_MODELS
    )
    assert ungoverned == [], (
        "Models reachable from BlueprintSchema with NO capability-leaf "
        f"coverage: {ungoverned}. Add them to _SCHEMA_BLOCKS / "
        "MODULE_NESTED_SCHEMA_BLOCKS (capability_leaves.py / "
        "parser/schema.py), or to _EXEMPT_MODELS above with a reason."
    )


def test_reachability_walker_can_actually_fail():
    """Positive control — the walker above must REPORT an unreachable model,
    not silently classify everything as reachable.

    Without this, ``test_every_schema_block_is_reachable_from_blueprint_schema``
    would pass just as happily if ``_reachable_models`` returned every model in
    the process, or if the ``issubclass`` filter never matched.
    """

    class _NeverInABlueprint(BaseModel):
        knob: int = 0

    reachable = _reachable_models(BlueprintSchema)
    assert _NeverInABlueprint not in reachable
    # …and the real entries are not vacuously "unreachable" either: the walker
    # finds models that ARE wired in, so a green run above is evidence.
    assert BlueprintSchema in reachable
    assert len(reachable) > 1


def test_cascade_tier_emits_no_grammar_or_config_leaves():
    """``agent.cascade`` is core, engine-invariant config: the list of LLM
    tiers the healing loop escalates through on stuck/exhausted/deferred. It
    lives only in ``aqueduct.yml``, where it is tagged ``engine_scoped: False``
    — so it must produce NO leaf on either walker, grammar or config."""
    grammar = sorted(leaf for leaf in all_leaves() if leaf.startswith("agent_cascade_tier."))
    assert grammar == [], (
        f"cascade-tier grammar leaves reappeared: {grammar} — no engine has an "
        "opinion on which LLM tier the healing loop escalates to."
    )
    scoped = sorted(leaf for leaf in all_config_leaves() if "cascade" in leaf)
    assert scoped == [], f"cascade became an engine-scoped config leaf: {scoped}"
    # It IS still governed as core config — proving the assertions above are a
    # scoping statement, not evidence that the feature was deleted.
    assert "config.agent.cascade" in core_config_leaves()
