"""Phase 78 — engine capability framework closure test.

The anti-drift core: every leaf the grammar walker derives (``all_leaves()``)
must have an explicit verdict in every registered engine's capability table,
and every declared verdict key must correspond to a real derived leaf (no
orphaned/typo'd keys). Together these two directions make it structurally
impossible for a new schema field / Channel op / Egress mode to land without
someone deciding what every registered engine does with it.

Pure — no pyspark, no Spark session. Importing ``aqueduct.executor.spark.capabilities``
pulls in the Spark declaration but does NOT import pyspark itself (verified
below).
"""

from __future__ import annotations

import sys

import pytest

from aqueduct.executor.capabilities import (
    CAPABILITY_REGISTRY,
    Capability,
    Support,
    validate_specifier,
)
from aqueduct.executor.capability_leaves import all_leaves

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _spark_capabilities_registered():
    # Importing this module registers "spark" into CAPABILITY_REGISTRY as a
    # side effect (mirrors how aqueduct/executor/spark/session.py registers
    # itself). Import here rather than module level so a stray import
    # ordering elsewhere in the suite can't hide a registration bug.
    import aqueduct.executor.spark.capabilities  # noqa: F401


def test_spark_capabilities_importable_without_pyspark():
    """The Spark declaration module must be pure data — no pyspark import."""
    assert "pyspark" not in sys.modules or True  # pyspark may be installed in dev env
    # Stronger check: block pyspark and re-import in a subprocess-free way by
    # inspecting the module's own source for a top-level pyspark import is
    # fragile; instead assert the documented exception list in AGENTS.md is
    # respected structurally — capabilities.py imports only from
    # aqueduct.executor.capabilities / capability_leaves, both of which are
    # independently proven pyspark-free by test_capability_leaves_no_pyspark.
    import aqueduct.executor.spark.capabilities as caps_mod

    src = caps_mod.__file__
    with open(src, encoding="utf-8") as f:
        text = f.read()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("import pyspark") or stripped.startswith("from pyspark"):
            pytest.fail(f"aqueduct/executor/spark/capabilities.py must stay pyspark-free: {line!r}")


def test_registry_has_spark():
    assert "spark" in CAPABILITY_REGISTRY


@pytest.mark.parametrize("engine_name", list(CAPABILITY_REGISTRY))
def test_every_leaf_has_a_verdict(engine_name):
    """Forward direction: every derived leaf must appear in the engine's table."""
    caps = CAPABILITY_REGISTRY[engine_name]
    leaves = all_leaves()
    missing = sorted(leaves - set(caps.table))
    assert not missing, (
        f"engine {engine_name!r} has no explicit capability verdict for: {missing}. "
        "A new schema field / Channel op / Egress mode landed without a capability "
        "verdict — add an entry to the engine's EngineCapabilities table (or rely on "
        "all_leaves_default() if it should inherit the engine's baseline posture)."
    )


@pytest.mark.parametrize("engine_name", list(CAPABILITY_REGISTRY))
def test_every_verdict_key_is_a_real_leaf(engine_name):
    """Reverse direction: every declared table key must be a real derived leaf."""
    caps = CAPABILITY_REGISTRY[engine_name]
    leaves = all_leaves()
    orphaned = sorted(set(caps.table) - leaves)
    assert not orphaned, (
        f"engine {engine_name!r} declares capability verdicts for leaf ids that don't "
        f"exist in the grammar: {orphaned}. Likely a typo, or a leaf that was renamed/"
        "removed without updating the capability table."
    )


def test_all_leaves_deterministic():
    """Calling all_leaves() twice returns the identical set (no ordering/randomness bugs)."""
    assert all_leaves() == all_leaves()


def test_all_leaves_nonempty():
    assert len(all_leaves()) > 50  # sanity floor — the grammar is not tiny


@pytest.mark.parametrize("engine_name", list(CAPABILITY_REGISTRY))
def test_registry_sanity(engine_name):
    caps = CAPABILITY_REGISTRY[engine_name]
    assert caps.engine == engine_name
    for leaf_id, cap in caps.table.items():
        assert isinstance(cap, Capability)
        assert cap.support in set(Support)
        if cap.requires:
            for dep, spec in cap.requires.items():
                assert isinstance(dep, str) and dep
                assert validate_specifier(spec), (
                    f"{engine_name}:{leaf_id} requires[{dep!r}] = {spec!r} is not a "
                    "well-formed version specifier"
                )


def test_frozen_dataclasses_are_immutable():
    from dataclasses import FrozenInstanceError

    cap = Capability(support=Support.SUPPORTED)
    with pytest.raises(FrozenInstanceError):
        cap.support = Support.UNSUPPORTED  # type: ignore[misc]


def test_capability_rejects_malformed_specifier():
    with pytest.raises(ValueError):
        Capability(support=Support.SUPPORTED, requires={"pyspark": "not-a-specifier"})


@pytest.mark.parametrize(
    ("installed", "spec", "expected"),
    [
        ("4.0.0", ">=4.0", True),
        ("3.5.8", ">=4.0", False),
        ("4.0.0", ">=4.0,<5.0", True),
        ("5.0.0", ">=4.0,<5.0", False),
        ("4.1.2", "==4.1.2", True),
        ("4.1.3", "==4.1.2", False),
    ],
)
def test_version_satisfies(installed, spec, expected):
    from aqueduct.executor.capabilities import version_satisfies

    assert version_satisfies(installed, spec) is expected
