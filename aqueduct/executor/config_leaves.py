"""Config leaf walker — derives the canonical ``config.*`` capability-leaf-id set.

Phase 78 Step 3 — closes the gap left by ``capability_leaves.py``: that module
covers the BLUEPRINT grammar (``parser/schema.py``) only. ENGINE-CONFIG keys
(``aqueduct.yml`` -> ``aqueduct/config.py``) carry no capability verdict at
all today, so a key that means nothing on a given engine (e.g.
``deployment.master_url`` on a single-node engine) is a silent no-op instead
of a diagnosed one. This module is the same anti-drift walker applied to
``aqueduct.config.AqueductConfig`` instead of the Blueprint schema: every
``config.*`` leaf it derives must carry an explicit verdict in every
registered engine's ``EngineCapabilities`` table, exactly like a grammar leaf
(enforced by the SAME closure test, ``tests/test_capabilities/test_closure.py``).

Leaf-id naming scheme: ``config.<dotted-field-path>`` — one leaf per pydantic
field, recursed through nested ``BaseModel`` fields (``config.deployment.
target``, ``config.stores.observability.backend``, ``config.agent.
sandbox_master_url``, ...). A field typed ``list[Model]`` or ``dict[str,
Model]`` (e.g. ``stores.depots``, ``agent.cascade``) is a single atomic leaf
rather than enumerated per dynamic key/item — there is no fixed set of named
sub-fields to walk.

Q4 STEP 2 — CONFIG-LEAF SCOPING (this module's second anti-drift job).
Roughly 88 of the 105 ``config.*`` leaves are CORE-ONLY: ``webhooks.*``,
``secrets.*``, ``stores.*``, most of ``agent.*``, ``danger.*`` — they run in
core code paths that never dispatch through an engine, so asking every
registered engine for a verdict on webhook backoff is a category error, not
a governance win. Narrowing the CHECKLIST (not adding a fourth ``Support``
verdict — see ``docs/specs.md`` §10.9) is done via an explicit per-field tag
in ``aqueduct/config.py``::

    max_sample_rows: int = Field(..., json_schema_extra={"engine_scoped": True})
    api_key: str | None = Field(..., json_schema_extra={"engine_scoped": False})

**Every field must carry ONE of the two, explicitly. There is no implicit
default and no "untagged means core" fallback** — that shape was tried and
rejected: it let a brand-new field (or one that was in fact engine-scoped)
silently fall into the core bucket with nobody deciding, the exact
"all_leaves_default()" class of bug (see ``docs/specs.md`` §10.9 / AGENTS.md's
"Never make the break go away with a default") applied to classification
instead of verdicts. A field discovered with NEITHER key present raises
``CapabilityScopeError`` (``aqueduct/errors.py``) naming the field and both
legal resolutions. ``all_config_leaves()`` yields the ``True``-tagged fields
(the checklist); ``core_config_leaves()`` yields the ``False``-tagged
complement. Both derive from the SAME per-field tag read via
``model_fields[name].json_schema_extra`` so they cannot drift apart — there
is no second list anywhere (a hand-maintained "known core leaf ids" module
would be the identical bug in a new file), and no committed snapshot file
either (see ``docs/specs.md`` §10.9's enforcement table for why one isn't
needed: the invariant test in
``tests/test_capabilities/test_config_scope_invariant.py`` plus the existing
orphaned-row check in ``capability_tooling.check()`` cover the hazard twice
over — and reclassifying a key is now a one-word ``True``/``False`` diff at
the field itself, reviewable in a pull request, which is what the dropped
snapshot file was for).

``engine.<name>.*`` POSITIONAL SCOPING. A leaf living under a per-engine
namespaced block (``engine.spark.*``, a future ``engine.duckdb.*``) can only
ever mean something to that ONE engine — asking DuckDB for a verdict on
``engine.spark.master_url`` is the same category error as asking it about
webhook backoff, just spelled differently. ``all_config_leaves(engine=...)``
accepts an optional engine name and, when given, drops every OTHER engine's
``engine.<name>.*`` leaves from the returned set (a leaf under the CALLING
engine's own block, or not under any ``engine.<name>.*`` block at all, is
kept). The engine name is derived purely from the ``engine.<name>`` path
segment — never a hand-listed engine name. A field found there tagged
``engine_scoped: False`` is ALSO a contradiction (a field cannot be
namespaced to exactly one engine and also claim to be core) and raises the
same way an untagged one does.

``CapabilityScopeError`` (``aqueduct/errors.py`` — a SIBLING of
``CapabilityDeclarationError``, not a subclass) is raised the moment the
walker runs — which is at every registered engine's REGISTRATION time (each
engine's ``capabilities.py`` calls ``all_config_leaves(engine=<name>)`` at
import), never CI-only.

Two walkers share the same descent rule so their vocabularies line up:

  - ``all_config_leaves(engine=None)`` — CLASS-level walk of ``AqueductConfig``
    itself, filtered to ``engine_scoped: True`` fields: the checklist every
    engine must have a verdict for (closure test). ``engine=<name>`` further
    drops every OTHER engine's ``engine.<name>.*`` leaves.
  - ``core_config_leaves()`` — CLASS-level walk of ``AqueductConfig``,
    the ``engine_scoped: False``-tagged complement — leaves that leave
    the checklist entirely. Never engine-filtered: core leaves are, by
    construction, not namespaced to any one engine.
  - ``explicitly_set_config_leaves(cfg)`` — INSTANCE-level walk of an actual
    validated ``AqueductConfig``, using pydantic's ``model_fields_set`` at
    each nesting level to report only the leaves the USER explicitly wrote
    in their ``aqueduct.yml`` (untouched defaults never warn — the whole
    point is "you set a key that does nothing here", not "this key exists").
    Narrowed to the SAME ``engine_scoped`` tag as ``all_config_leaves()`` —
    once a core leaf leaves the checklist there is no capability row for it
    to warn against, so this walker must never report one.
    ``aqueduct/config.py::load_config()`` calls this at config-resolution
    time, once ``deployment.engine`` is known, and emits a suppressible
    ``engine_key_ignored`` warning (same rule id and `aqueduct.warnings.emit`
    machinery as the blueprint gate) for any explicitly-set leaf whose
    verdict isn't `SUPPORTED` — WARN, never ERROR (the same `aqueduct.yml`
    must stay valid across engines). Closes the gap left by Step 0's leaf
    walker (`capability_leaves.py` covers Blueprint grammar only, not
    `aqueduct.yml`). Imports `aqueduct.config` at module level; `config.py`
    imports this module back only lazily (inside `load_config()`), avoiding
    a circular import.
"""

from __future__ import annotations

import re
import types
import typing
from typing import Any

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from aqueduct.errors import CapabilityScopeError

LEAF_PREFIX = "config"

# Both spellings of a union origin. ``typing.Optional[X]`` / ``typing.Union[X,
# None]`` report origin ``typing.Union`` on every version, but a PEP 604
# annotation (``X | None`` — which is how ``aqueduct/config.py`` actually
# writes its optional sub-config fields) reports origin ``types.UnionType`` on
# Python 3.11-3.13 and only unifies to ``typing.Union`` on 3.14+ (where PEP 604
# unions became ``typing.Union`` instances).
#
# Matching only ``typing.Union`` made the DERIVED LEAF SET DEPEND ON THE PYTHON
# VERSION: on 3.11-3.13 — the entire supported range (``pyproject``'s
# ``requires-python`` is ``>=3.11``) — every ``Sub | None`` field failed to
# unwrap, so its children were never derived (e.g. ``budget: AgentBudgetConfig |
# None`` stayed one atomic ``config.agent.budget`` leaf instead of its six
# sub-leaves). On 3.14 they unwrapped. The engines' ``capabilities.yml`` files
# are generated by ``dev capabilities scaffold`` (run on 3.14), so they list the
# children — which on 3.11-3.13 are ORPHAN rows, and ``load_declaration()``
# refuses to register any engine whose YAML names a leaf the walker did not
# derive:
#
#     CapabilityDeclarationError: ... declares a verdict for
#     'config.agent.budget.max_reprompts', which is not a real capability leaf.
#
# That fired for the SHIPPED Spark engine, not just for a new one, and because
# ``deployment.engine`` validation is fail-closed it broke every ``aqueduct.yml``
# load on 3.11-3.13. Version-independent unwrapping is the fix; the derived leaf
# set is now identical on every supported interpreter.
_UNION_ORIGINS: tuple[Any, ...] = (typing.Union, types.UnionType)

# Matches a leaf id that IS an engine's own namespaced block root — exactly
# ``config.engine.<name>`` (two segments after the prefix), e.g.
# ``config.engine.spark``. A field discovered while recursing INTO that
# sub-model (``config.engine.spark.master_url``, ...) is positionally owned
# by that one engine. Deliberately does not match ``config.engine`` itself
# (the routing block) or a grandchild path directly — the walker computes
# "am I inside one of these" once, when it descends past this exact prefix.
_ENGINE_BLOCK_ROOT_RE = re.compile(r"^" + re.escape(LEAF_PREFIX) + r"\.engine\.[^.]+$")

# Matches ``config.engine.<name>.<...>`` and captures ``<name>`` — used only
# to filter an already-derived leaf set down to one engine's own leaves plus
# every non-``engine.*`` leaf. Never used to enumerate engine names (those
# come from the schema itself / the capabilities.yml discovery path, per
# AGENTS.md's "classify by exclusion" + "no hand-listed engine names" rules).
_ENGINE_BLOCK_LEAF_RE = re.compile(r"^" + re.escape(LEAF_PREFIX) + r"\.engine\.([^.]+)\.")


def _direct_basemodel(annotation: Any) -> type[BaseModel] | None:
    """Return the ``BaseModel`` subclass ``annotation`` directly wraps, if any.

    Handles ``Model``, ``Optional[Model]``, and the PEP 604 ``Model | None``
    (see ``_UNION_ORIGINS`` — the two spell the same type but report different
    origins below Python 3.14). Deliberately does NOT unwrap ``list[Model]`` or
    ``dict[str, Model]`` — those describe a dynamic/keyed collection of
    sub-configs (``stores.depots``, ``agent.cascade``), not a fixed set of named
    fields, so they are one atomic leaf rather than enumerated per possible
    key/item.
    """
    origin = typing.get_origin(annotation)
    if origin in _UNION_ORIGINS:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return _direct_basemodel(args[0])
        return None
    if origin is None and isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    return None


_UNTAGGED = object()  # sentinel: no `engine_scoped` key present at all


def _scope_tag(field_info: FieldInfo) -> Any:
    """Return the field's explicit ``engine_scoped`` tag: ``True``, ``False``,
    or the ``_UNTAGGED`` sentinel if the key is absent (or ``json_schema_extra``
    isn't a dict at all). Never guesses — the caller decides what "untagged"
    means (it means "raise", everywhere this module is concerned)."""
    extra = field_info.json_schema_extra
    if isinstance(extra, dict) and "engine_scoped" in extra:
        return bool(extra["engine_scoped"])
    return _UNTAGGED


def leaves_for_model(model_cls: type[BaseModel], prefix: str = LEAF_PREFIX) -> frozenset[str]:
    """Class-level walk: every leaf ``model_cls`` (recursively) declares.

    Untagged by scope — this is the full leaf universe (engine-scoped ∪
    core), used by ``core_config_leaves()`` and exposed so tests can prove
    the closure mechanism against a synthetic model without mutating the
    real ``AqueductConfig`` schema.
    """
    return frozenset(_walk_class(model_cls, prefix, (model_cls,)))


def _walk_class(model_cls: type[BaseModel], prefix: str, stack: tuple[type, ...]) -> set[str]:
    leaves: set[str] = set()
    for name, field_info in model_cls.model_fields.items():
        leaf_id = f"{prefix}.{name}"
        nested = _direct_basemodel(field_info.annotation)
        if nested is not None and nested not in stack:
            leaves |= _walk_class(nested, leaf_id, stack + (nested,))
        else:
            leaves.add(leaf_id)
    return leaves


def _walk_tagged(
    model_cls: type[BaseModel], prefix: str, stack: tuple[type, ...], in_engine_block: bool
) -> tuple[set[str], set[str]]:
    """Class-level walk requiring an EXPLICIT ``engine_scoped`` tag on every
    leaf field. Returns ``(true_leaves, false_leaves)``.

    ``in_engine_block`` tracks whether the CURRENT model is being walked as
    (or beneath) an ``engine.<name>`` sub-block — see
    ``_ENGINE_BLOCK_ROOT_RE``. A leaf found there tagged ``False`` is a
    positional impossibility (a field namespaced to one engine cannot also
    claim to be core), so it raises the same as an untagged one.

    A leaf with NO ``engine_scoped`` key at all — anywhere in the model,
    inside or outside an engine block — raises ``CapabilityScopeError``
    naming the field and both legal resolutions. There is no fallback: this
    is what makes "absent = core" impossible to reach by omission.
    """
    true_leaves: set[str] = set()
    false_leaves: set[str] = set()
    for name, field_info in model_cls.model_fields.items():
        leaf_id = f"{prefix}.{name}"
        nested = _direct_basemodel(field_info.annotation)
        child_in_engine_block = in_engine_block or bool(_ENGINE_BLOCK_ROOT_RE.match(leaf_id))
        if nested is not None and nested not in stack:
            t, f = _walk_tagged(nested, leaf_id, stack + (nested,), child_in_engine_block)
            true_leaves |= t
            false_leaves |= f
        else:
            tag = _scope_tag(field_info)
            if tag is _UNTAGGED:
                raise CapabilityScopeError(
                    f"{leaf_id!r} has no Field(..., json_schema_extra="
                    "{'engine_scoped': ...}) tag in aqueduct/config.py. Every config.* "
                    "leaf must carry an EXPLICIT True/False tag — there is no implicit "
                    "default and 'untagged' is never a valid state. Resolve it one of "
                    "two ways: tag it engine_scoped: True if it dispatches through an "
                    "engine (an engine module reads it, or it reaches ExecutorProtocol), "
                    "or engine_scoped: False if it only ever executes in core code paths."
                )
            if tag is False and child_in_engine_block:
                raise CapabilityScopeError(
                    f"{leaf_id!r} lives under an engine.<name>.* block — positionally "
                    "owned by exactly one engine — but is tagged engine_scoped: False. "
                    "That is a contradiction: there is no valid 'core' reading for a field "
                    "namespaced to one engine. Tag it engine_scoped: True."
                )
            if tag is True:
                true_leaves.add(leaf_id)
            else:
                false_leaves.add(leaf_id)
    return true_leaves, false_leaves


def _engine_owner(leaf_id: str) -> str | None:
    """Return the engine name if ``leaf_id`` is ``config.engine.<name>.*``, else None."""
    m = _ENGINE_BLOCK_LEAF_RE.match(leaf_id)
    return m.group(1) if m else None


def all_config_leaves(engine: str | None = None) -> frozenset[str]:
    """Return the ENGINE-SCOPED ``config.*`` checklist — the leaves every
    registered engine must have a verdict for.

    This is the single source of truth every ``EngineCapabilities`` table is
    checked against for config governance (folded into the closure test
    alongside ``aqueduct.executor.capability_leaves.all_leaves()``).

    Args:
        engine: When given, drops every OTHER engine's ``engine.<name>.*``
            leaves from the returned set (a leaf under ``engine.<engine>.*``,
            or not under any ``engine.<name>.*`` block at all, is kept). When
            None (the default), returns the full engine-scoped union across
            every engine — used for reference/spot-check purposes, never
            passed to ``load_declaration()`` for a specific engine's table.

    Raises:
        CapabilityScopeError: a field has no explicit ``engine_scoped`` tag,
            or a field under an ``engine.<name>.*`` block is tagged ``False``
            (see module docstring).
    """
    from aqueduct.config import AqueductConfig

    true_leaves, _ = _walk_tagged(AqueductConfig, LEAF_PREFIX, (AqueductConfig,), False)
    if engine is None:
        return frozenset(true_leaves)
    return frozenset(
        leaf for leaf in true_leaves
        if (owner := _engine_owner(leaf)) is None or owner == engine
    )


def core_config_leaves() -> frozenset[str]:
    """Return the CORE ``config.*`` complement — leaves that leave the
    per-engine checklist entirely (structurally universal: they run in core
    code paths with no per-engine implementation to diverge from).

    Derived from the SAME per-field tag as ``all_config_leaves()`` — the
    ``engine_scoped: False``-tagged set, from the same walk — so the two
    cannot drift apart and there is no second, hand-maintained list. Never
    engine-filtered: a core leaf is not namespaced to any one engine by
    definition.

    Raises:
        CapabilityScopeError: same conditions as ``all_config_leaves()`` —
            this walks the identical tree.
    """
    from aqueduct.config import AqueductConfig

    _, false_leaves = _walk_tagged(AqueductConfig, LEAF_PREFIX, (AqueductConfig,), False)
    return frozenset(false_leaves)


def _walk_instance(instance: BaseModel, prefix: str, *, scoped_only: bool) -> set[str]:
    leaves: set[str] = set()
    model_cls = type(instance)
    for name in instance.model_fields_set:
        leaf_id = f"{prefix}.{name}"
        value = getattr(instance, name)
        field_info = model_cls.model_fields[name]
        nested = _direct_basemodel(field_info.annotation)
        if nested is not None and isinstance(value, BaseModel):
            leaves |= _walk_instance(value, leaf_id, scoped_only=scoped_only)
        elif not scoped_only or _scope_tag(field_info) is True:
            leaves.add(leaf_id)
    return leaves


def explicitly_set_config_leaves(cfg: Any) -> frozenset[str]:
    """Instance-level walk: leaves the USER explicitly set in this config,
    narrowed to the SAME ``engine_scoped`` tag as ``all_config_leaves()``.

    Uses pydantic's ``model_fields_set`` at every nesting level, so a nested
    block the user never mentioned (left at its ``default_factory``) never
    contributes a leaf, even though ``all_config_leaves()`` still enumerates
    it as a possibility. Once a core leaf leaves the checklist there is no
    capability row for it — narrowing here is not optional, it is what keeps
    ``aqueduct/config.py::_warn_ignored_config_keys`` (which loops this and
    calls ``caps.verdict(leaf_id)``) from asking about a leaf no engine
    declares. ``cfg`` is typed ``Any`` to avoid importing ``aqueduct.config``
    at call time when the caller already has an instance in hand (the type is
    ``aqueduct.config.AqueductConfig`` in practice).
    """
    return frozenset(_walk_instance(cfg, LEAF_PREFIX, scoped_only=True))


__all__ = [
    "LEAF_PREFIX",
    "all_config_leaves",
    "core_config_leaves",
    "explicitly_set_config_leaves",
    "leaves_for_model",
]
