"""Grammar leaf walker — derives the canonical capability-leaf-id set.

Phase 78 — the anti-drift core of the capability framework. A "leaf" is one
atomic point of engine-visible grammar: a module type, a pydantic schema
field, a Channel op, an Egress write mode, a Junction/Funnel fan mode, or a
hand-curated engine feature flag. Every leaf returned by ``all_leaves()``
must carry an explicit verdict in every registered ``EngineCapabilities``
table (enforced by ``tests/test_capabilities/test_closure.py``) — a new
schema field or op landing without a capability verdict fails the build.

Leaf-id naming scheme (stable — do not rename without a deprecation note,
same as compiler warning ``rule_id``s):
  module.type.<ModuleType>              — one per ModuleType enum value
  module.field.<name>                   — ModuleSchema's own fields
  <block>.field.<name>                  — one entry per nested schema block
                                           (agent, agent_guardrails,
                                           agent_cascade_tier, retry_policy,
                                           module_retry, backoff, edge, udf,
                                           warnings, hooks, hook_entry)
  channel.op.<op>                       — Channel op names
  egress.mode.<mode>                    — Egress write modes
  egress.on_new_columns.<policy>        — Egress schema-drift policy
  egress.format.<fmt>                   — Egress formats with dedicated code
                                           paths (maintenance ops) — curated,
                                           NOT the full open format list
                                           (any Spark format string works)
  ingress.format.<fmt>                  — Ingress formats with dedicated
                                           code paths — curated, same caveat
  junction.mode.<mode>                  — Junction fan-out modes
  funnel.mode.<mode>                    — Funnel fan-in modes
  feature.<name>                        — hand-curated engine feature flags
                                           (NOT schema-derivable — see
                                           FEATURE_FLAGS docstring below)

Derivation sources, by leaf category:
  - module.type.*, module.field.*, and every <block>.field.* leaf are derived
    by introspecting ``aqueduct/parser/schema.py`` pydantic models
    (``model_fields``) — this IS schema-derived and drifts automatically as
    fields are added/removed.
  - channel.op.*, egress.mode.*, egress.on_new_columns.*, junction.mode.*,
    funnel.mode.* are derived from named frozenset constants that live next
    to the op/mode dispatch code (``executor/channel_ops.py``,
    ``executor/spark/egress.py``, ``executor/spark/junction.py``,
    ``executor/spark/funnel.py``) — these are NOT pydantic schemas (Module
    ``config:`` is a freeform ``dict[str, Any]``, validated per-op at
    runtime, not by ``schema.py``), but the constants are still the single
    source of truth the dispatch code itself reads, so adding an op without
    updating the constant is impossible by construction.
  - ingress.format.* / egress.format.* are a SMALL CURATED subset: Ingress
    and Egress accept "any format string the active SparkSession supports"
    (see their module docstrings) — there is no closed enumerable set. The
    leaves here cover only the formats with dedicated engine code paths
    (schema_hint/time_travel, delta/iceberg/hudi maintenance, custom
    DataSource, depot pseudo-format) where a capability verdict is
    meaningful. A brand-new pass-through format string needs no capability
    verdict — Spark forwards it to ``DataFrameReader``/``DataFrameWriter``
    unconditionally.
  - feature.* is hand-curated (documented in FEATURE_FLAGS below) — engine
    feature flags aren't declared anywhere in the schema; they're behavioral
    capabilities (java_udf support, Delta time travel, …).

This module must stay importable without ``pyspark`` — it is read by the
compile-time capability gate (``aqueduct/compiler/capability_check.py``,
which runs inside `compile()`, before any Spark session exists) and by the
closure test.
"""

from __future__ import annotations

from pydantic import BaseModel

from aqueduct.executor.channel_ops import ALL_OPS as _CHANNEL_OPS
from aqueduct.executor.spark.egress import ON_NEW_COLUMNS_POLICIES as _EGRESS_ON_NEW_COLUMNS
from aqueduct.executor.spark.egress import SUPPORTED_MODES as _EGRESS_MODES
from aqueduct.executor.spark.funnel import VALID_MODES as _FUNNEL_MODES
from aqueduct.executor.spark.junction import VALID_MODES as _JUNCTION_MODES
from aqueduct.parser.models import ModuleType
from aqueduct.parser.schema import (
    AgentSchema,
    BackoffSchema,
    CascadeTierSchema,
    EdgeSchema,
    GuardrailsSchema,
    HookEntrySchema,
    HooksSchema,
    ModuleRetrySchema,
    ModuleSchema,
    RetryPolicySchema,
    UdfSchema,
    WarningsSchema,
)

# ── Curated: format strings with a dedicated engine code path ──────────────
# See module docstring — NOT the full open format list.
INGRESS_FORMATS: frozenset[str] = frozenset({"custom", "delta", "csv", "jdbc", "kafka", "depot"})
EGRESS_FORMATS: frozenset[str] = frozenset({"custom", "delta", "iceberg", "hudi", "depot"})

# ── Curated: engine feature flags (not schema-derivable) ───────────────────
# One entry per behavioral capability that isn't a config key or op name —
# whether the engine can run a given kind of UDF, write format, or runtime
# behavior at all. Hand-maintained; add a line here when a new cross-cutting
# engine capability is introduced.
FEATURE_FLAGS: frozenset[str] = frozenset({
    "java_udf",
    "python_udf",
    "delta_write",
    "delta_time_travel",
    "spillway",
    "metrics_boundary",
    "broadcast_junction",
    "custom_datasource",
    "checkpoint",
    "parallel_mode",
})

# Nested schema blocks walked for their own field-name leaves. Each entry is
# (leaf-prefix, pydantic model). Keep in sync with schema.py's actual nested
# models — a new nested config block should be added here.
_SCHEMA_BLOCKS: tuple[tuple[str, type[BaseModel]], ...] = (
    ("agent", AgentSchema),
    ("agent_guardrails", GuardrailsSchema),
    ("agent_cascade_tier", CascadeTierSchema),
    ("retry_policy", RetryPolicySchema),
    ("module_retry", ModuleRetrySchema),
    ("backoff", BackoffSchema),
    ("edge", EdgeSchema),
    ("udf", UdfSchema),
    ("warnings", WarningsSchema),
    ("hooks", HooksSchema),
    ("hook_entry", HookEntrySchema),
)


def _module_field_leaves() -> set[str]:
    return {f"module.field.{name}" for name in ModuleSchema.model_fields}


def _module_type_leaves() -> set[str]:
    return {f"module.type.{mt.value}" for mt in ModuleType}


def _schema_block_leaves() -> set[str]:
    leaves: set[str] = set()
    for prefix, model in _SCHEMA_BLOCKS:
        for name in model.model_fields:
            leaves.add(f"{prefix}.field.{name}")
    return leaves


def _channel_op_leaves() -> set[str]:
    return {f"channel.op.{op}" for op in _CHANNEL_OPS}


def _egress_leaves() -> set[str]:
    leaves = {f"egress.mode.{m}" for m in _EGRESS_MODES}
    leaves |= {f"egress.on_new_columns.{p}" for p in _EGRESS_ON_NEW_COLUMNS}
    leaves |= {f"egress.format.{f}" for f in EGRESS_FORMATS}
    return leaves


def _ingress_leaves() -> set[str]:
    return {f"ingress.format.{f}" for f in INGRESS_FORMATS}


def _junction_leaves() -> set[str]:
    return {f"junction.mode.{m}" for m in _JUNCTION_MODES}


def _funnel_leaves() -> set[str]:
    return {f"funnel.mode.{m}" for m in _FUNNEL_MODES}


def _feature_leaves() -> set[str]:
    return {f"feature.{name}" for name in FEATURE_FLAGS}


def all_leaves() -> frozenset[str]:
    """Return the full, deterministic set of canonical capability leaf ids.

    This is the single source of truth every ``EngineCapabilities`` table is
    checked against (both directions — see the closure test).
    """
    leaves: set[str] = set()
    leaves |= _module_type_leaves()
    leaves |= _module_field_leaves()
    leaves |= _schema_block_leaves()
    leaves |= _channel_op_leaves()
    leaves |= _egress_leaves()
    leaves |= _ingress_leaves()
    leaves |= _junction_leaves()
    leaves |= _funnel_leaves()
    leaves |= _feature_leaves()
    return frozenset(leaves)


__all__ = [
    "EGRESS_FORMATS",
    "FEATURE_FLAGS",
    "INGRESS_FORMATS",
    "all_leaves",
]
