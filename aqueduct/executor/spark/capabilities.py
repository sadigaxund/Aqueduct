"""Spark engine capability declaration (Phase 78) — declaration only.

This file makes NO changes to any existing Spark executor module; it is a
pure data declaration that must stay importable without ``pyspark``
installed (it is read by the compile-time capability gate, which runs
before any Spark session exists, and by the closure test).

Spark implements almost the entire grammar today, so it declares
default-ALLOW (``all_leaves_default(..., Support.SUPPORTED)``) and layers a
small number of targeted overrides on top — leaves that are genuinely
UNSUPPORTED, or SUPPORTED-but-version-gated. Every ``requires`` constraint
below is mined from currently-documented claims (``docs/compatibility.md``,
``aqueduct/executor/spark/session.py``, ``aqueduct/executor/spark/udf.py``)
— this file does not invent new version claims. A future engine (e.g. a
DuckDB engine) starts from ``all_leaves_default(..., Support.UNSUPPORTED)``
instead and earns leaves one at a time — the opposite default posture.
"""

from __future__ import annotations

from aqueduct.executor.capabilities import (
    CAPABILITY_REGISTRY,  # noqa: F401 — re-exported for convenience
    Capability,
    EngineCapabilities,
    Support,
    all_leaves_default,
    register,
)
from aqueduct.executor.capability_leaves import all_leaves

_ALL_LEAVES = all_leaves()

_TABLE: dict[str, Capability] = all_leaves_default(_ALL_LEAVES, Support.SUPPORTED)

# ── Targeted overrides — version-gated SUPPORTED leaves ────────────────────
# Compile cannot fail these (it doesn't know the runtime environment); the
# `requires` constraint is checked at doctor time (aqueduct/doctor/).

# format: custom (Python DataSource, `spark.dataSource` registry) needs
# Spark 4.0+ — see docs/compatibility.md "Custom Python DataSource" note and
# pyproject.toml's `pyspark>=4.0,<5.0` pin. On the unsupported Legacy 3.5
# lane the engine raises a clear RuntimeError at runtime (per-feature gate,
# not a hard requirement bump) — that is exactly what this Capability models.
_TABLE["ingress.format.custom"] = Capability(
    support=Support.SUPPORTED,
    requires={"pyspark": ">=4.0"},
    hint="format: custom requires pyspark>=4.0 (the spark.dataSource registry). "
    "See docs/compatibility.md.",
)
_TABLE["egress.format.custom"] = Capability(
    support=Support.SUPPORTED,
    requires={"pyspark": ">=4.0"},
    hint="format: custom requires pyspark>=4.0 (the spark.dataSource registry). "
    "See docs/compatibility.md.",
)
_TABLE["feature.custom_datasource"] = Capability(
    support=Support.SUPPORTED,
    requires={"pyspark": ">=4.0"},
    hint="Custom Python DataSource requires pyspark>=4.0. See docs/compatibility.md.",
)

# Delta-backed features need the delta-spark package installed
# (pyproject.toml `spark` extra: `delta-spark>=4.0,<5.0`).
for _leaf in (
    "ingress.format.delta",
    "egress.format.delta",
    "feature.delta_write",
    "feature.delta_time_travel",
):
    _TABLE[_leaf] = Capability(
        support=Support.SUPPORTED,
        requires={"delta-spark": ">=4.0"},
        hint="Delta Lake features require the delta-spark package "
        "(aqueduct-core[spark] extra). See docs/compatibility.md.",
    )

# java_udf — lang: java/scala UDFs load a JAR; nothing pyspark-version-gated,
# but they need a JVM classpath entry, not a pip-installable dependency, so
# no `requires` (doctor cannot check a JAR the way it checks a pip package).
# Left as plain SUPPORTED (the default) — no override needed.

SPARK = EngineCapabilities(engine="spark", table=_TABLE)

register(SPARK)

__all__ = ["SPARK"]
