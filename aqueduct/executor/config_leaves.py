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
master_url``, ``config.stores.observability.backend``, ``config.agent.
sandbox_master_url``, ...). A field typed ``list[Model]`` or ``dict[str,
Model]`` (e.g. ``stores.depots``, ``agent.cascade``) is a single atomic leaf
rather than enumerated per dynamic key/item — there is no fixed set of named
sub-fields to walk.

Two walkers share the same descent rule so their vocabularies line up:

  - ``all_config_leaves()`` — CLASS-level walk of ``AqueductConfig`` itself:
    the exhaustive, deterministic set every engine must have a verdict for
    (closure test, and folded into Spark's default-ALLOW leaf set so the
    reference engine's capability table stays a total function over the
    union of grammar + config leaves with zero per-key overrides needed).
  - ``explicitly_set_config_leaves(cfg)`` — INSTANCE-level walk of an actual
    validated ``AqueductConfig``, using pydantic's ``model_fields_set`` at
    each nesting level to report only the leaves the USER explicitly wrote
    in their ``aqueduct.yml`` (untouched defaults never warn — the whole
    point is "you set a key that does nothing here", not "this key exists").
    ``aqueduct/config.py::load_config()`` calls this at config-resolution
    time, after the engine is known (``cfg.deployment.engine``, itself
    already validated as a registered engine), and emits a suppressible
    ``engine_key_ignored`` warning — the SAME rule id the compile-time
    blueprint gate uses (``aqueduct/compiler/capability_check.py``) — for
    every explicitly-set leaf whose verdict on that engine is not
    ``SUPPORTED``. This is a deliberate WARN, never ERROR: unlike the
    blueprint gate (where UNSUPPORTED is a hard CompileError), a config key
    that means nothing on the current engine must not break loading
    ``aqueduct.yml`` — the same file has to stay valid across engines (a
    user's test overrides, or a shared config used by multiple deployment
    profiles, must not hard-fail just because one engine ignores one key).

This module imports ``aqueduct.config`` at module level to introspect
``AqueductConfig`` (there is nothing else it could walk). ``aqueduct/
config.py`` therefore imports THIS module lazily (inside ``load_config()``,
not at module top) to avoid a circular import — same lazy-import precedent as
``DeploymentConfig._validate_target_master_url`` importing
``aqueduct.executor.capabilities``. Pure introspection, **pyspark-free**.
"""

from __future__ import annotations

import typing
from typing import Any

from pydantic import BaseModel

LEAF_PREFIX = "config"


def _direct_basemodel(annotation: Any) -> type[BaseModel] | None:
    """Return the ``BaseModel`` subclass ``annotation`` directly wraps, if any.

    Handles ``Model`` and ``Model | None`` (``Optional[Model]``). Deliberately
    does NOT unwrap ``list[Model]`` or ``dict[str, Model]`` — those describe a
    dynamic/keyed collection of sub-configs (``stores.depots``, ``agent.
    cascade``), not a fixed set of named fields, so they are one atomic leaf
    rather than enumerated per possible key/item.
    """
    origin = typing.get_origin(annotation)
    if origin is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return _direct_basemodel(args[0])
        return None
    if origin is None and isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    return None


def leaves_for_model(model_cls: type[BaseModel], prefix: str = LEAF_PREFIX) -> frozenset[str]:
    """Class-level walk: every leaf ``model_cls`` (recursively) declares.

    Exposed (not just used internally by ``all_config_leaves()``) so tests can
    prove the closure mechanism against a synthetic model — e.g. a fixture
    subclass with an extra field — without needing to mutate the real
    ``AqueductConfig`` schema.
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


def all_config_leaves() -> frozenset[str]:
    """Return the full, deterministic set of canonical ``config.*`` leaf ids.

    This is the single source of truth every ``EngineCapabilities`` table is
    checked against for config governance (folded into the closure test
    alongside ``aqueduct.executor.capability_leaves.all_leaves()``).
    """
    from aqueduct.config import AqueductConfig

    return leaves_for_model(AqueductConfig)


def _walk_instance(instance: BaseModel, prefix: str) -> set[str]:
    leaves: set[str] = set()
    model_cls = type(instance)
    for name in instance.model_fields_set:
        leaf_id = f"{prefix}.{name}"
        value = getattr(instance, name)
        field_info = model_cls.model_fields[name]
        nested = _direct_basemodel(field_info.annotation)
        if nested is not None and isinstance(value, BaseModel):
            leaves |= _walk_instance(value, leaf_id)
        else:
            leaves.add(leaf_id)
    return leaves


def explicitly_set_config_leaves(cfg: Any) -> frozenset[str]:
    """Instance-level walk: leaves the USER explicitly set in this config.

    Uses pydantic's ``model_fields_set`` at every nesting level, so a nested
    block the user never mentioned (left at its ``default_factory``) never
    contributes a leaf, even though ``all_config_leaves()`` still enumerates
    it as a possibility. ``cfg`` is typed ``Any`` to avoid importing
    ``aqueduct.config`` at call time when the caller already has an instance
    in hand (the type is ``aqueduct.config.AqueductConfig`` in practice).
    """
    return frozenset(_walk_instance(cfg, LEAF_PREFIX))


__all__ = [
    "LEAF_PREFIX",
    "all_config_leaves",
    "explicitly_set_config_leaves",
    "leaves_for_model",
]
