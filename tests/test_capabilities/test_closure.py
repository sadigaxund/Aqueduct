"""Phase 78 — engine capability framework closure test.

The anti-drift core: every leaf the grammar walker derives (``all_leaves()``)
PLUS every engine-config leaf the config walker derives (``all_config_leaves()``,
Phase 78 Step 3) must have an explicit verdict in every registered engine's
capability table, and every declared verdict key must correspond to a real
derived leaf (no orphaned/typo'd keys). Together these two directions make it
structurally impossible for a new schema field / Channel op / Egress mode /
``aqueduct.yml`` config key to land without someone deciding what every
registered engine does with it.

Pure — no pyspark, no Spark session. The registry is populated via
``load_engines()`` (the ``aqueduct.engines`` entry-point group), NOT a direct
import of ``aqueduct.executor.spark.capabilities`` — this is the same path
the real compile/config/doctor code uses, so registering a future engine
(e.g. DuckDB) automatically drags it into this closure proof with no test
edit. Neither the entry-point module (``aqueduct/executor/spark/engine.py``)
nor the declaration it imports pulls in pyspark itself (verified below).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from aqueduct.executor.capabilities import (
    CAPABILITY_REGISTRY,
    Capability,
    Support,
    validate_specifier,
)
from aqueduct.executor.capability_leaves import all_leaves
from aqueduct.executor.config_leaves import all_config_leaves


def _all_governed_leaves() -> frozenset[str]:
    """Blueprint grammar leaves ∪ engine-config leaves — the combined set
    every registered engine's declaration must be a total function over."""
    return all_leaves() | all_config_leaves()


_REPO = Path(__file__).resolve().parents[2]

# Every engine's YAML declaration, discovered on disk. Parametrizing over THESE
# (not over CAPABILITY_REGISTRY) keeps the closure comparison independent of the
# registry — and keeps these tests collectable even when a missing verdict makes
# engine registration itself raise.
_DECLARATIONS = sorted((_REPO / "aqueduct" / "executor").glob("*/capabilities.yml"))


def _declared_rows(path: Path) -> dict:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return raw.get("leaves") or {}


def _verdict_of(row) -> str:
    """A row is either a bare verdict string or a mapping with `support:`."""
    return row if isinstance(row, str) else (row or {}).get("support", "")


pytestmark = pytest.mark.unit


def test_declarations_discovered():
    """Guard the discovery glob itself — if it silently matched nothing, every
    parametrized closure test below would vacuously pass with zero cases."""
    assert _DECLARATIONS, "no engine capabilities.yml found — the closure tests would be vacuous"


@pytest.fixture(autouse=True)
def _engines_registered():
    # Goes through the real registration seam — the aqueduct.engines
    # entry-point group — rather than importing
    # aqueduct.executor.spark.capabilities directly. This is the regression
    # proof for "the registry is empty at runtime because nothing imports
    # the Spark declaration outside tests": if load_engines() ever stops
    # resolving the entry point, the registry-dependent tests below fail
    # instead of silently passing via a test-only import.
    #
    # EnginePluginError is caught rather than allowed to error every test in
    # the file: an incomplete declaration (a leaf with no verdict) makes
    # registration raise, and if that escaped here it would bury the ONE thing
    # a developer needs to see — the clean "leaf X has no verdict, run sync"
    # diff from the YAML-vs-walker closure tests, which read the declaration
    # straight from disk and do not need the registry at all.
    # `test_engine_registration_succeeds` below still asserts the failure
    # loudly, so a broken registration is never silently tolerated.
    from aqueduct.errors import EnginePluginError
    from aqueduct.executor.capabilities import load_engines

    try:
        load_engines()
    except EnginePluginError:
        pass


def test_engine_registration_succeeds():
    """Registration must actually work — the counterpart to the fixture's
    tolerant catch above. An incomplete declaration makes `load_engines()`
    raise `EnginePluginError` (naming the leaf + the sync command), so this is
    where a missing verdict surfaces as a hard, loud failure."""
    from aqueduct.executor.capabilities import load_engines

    load_engines()
    assert CAPABILITY_REGISTRY, "no engine registered — declaration failed to load"


def test_spark_capabilities_importable_without_pyspark():
    """The Spark declaration + its entry-point module must be pure data —
    no pyspark import in either."""
    assert "pyspark" not in sys.modules or True  # pyspark may be installed in dev env
    # Stronger check: block pyspark and re-import in a subprocess-free way by
    # inspecting the module's own source for a top-level pyspark import is
    # fragile; instead assert the documented exception list in AGENTS.md is
    # respected structurally — capabilities.py imports only from
    # aqueduct.executor.capabilities / capability_leaves, both of which are
    # independently proven pyspark-free by test_capability_leaves_no_pyspark.
    import aqueduct.executor.spark.capabilities as caps_mod
    import aqueduct.executor.spark.engine as engine_mod

    for mod in (caps_mod, engine_mod):
        src = mod.__file__
        with open(src, encoding="utf-8") as f:
            text = f.read()
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("import pyspark") or stripped.startswith("from pyspark"):
                pytest.fail(f"{mod.__name__} must stay pyspark-free: {line!r}")


def test_registry_has_spark():
    assert "spark" in CAPABILITY_REGISTRY


@pytest.mark.parametrize("decl_path", _DECLARATIONS, ids=[p.parent.name for p in _DECLARATIONS])
def test_every_leaf_has_a_verdict(decl_path):
    """Forward direction: every derived leaf (grammar ∪ config) must have an
    explicit row in the engine's YAML declaration.

    This reads the YAML STRAIGHT FROM DISK rather than going through
    ``CAPABILITY_REGISTRY``, on purpose. Two properties follow:

      1. The comparison is between two INDEPENDENT sources — the leaf walker
         (code) and the declaration (data). The old version compared the
         registry against the same walker the registry's table was BUILT from
         (`all_leaves_default(all_leaves(), SUPPORTED)`), so it was a tautology
         that could never fail; a planted schema key was silently swept into
         SUPPORTED and the test still passed.
      2. It still collects and reports a clean diff even when the planted leaf
         makes engine REGISTRATION itself blow up (load_declaration raises
         EnginePluginError on an incomplete table) — so the failure a developer
         sees names the offending leaf instead of an import error.
    """
    rows = _declared_rows(decl_path)
    leaves = _all_governed_leaves()
    missing = sorted(leaves - set(rows))
    undeclared = sorted(k for k, v in rows.items() if _verdict_of(v) == Support.UNDECLARED.value)
    assert not missing and not undeclared, (
        f"{decl_path.name}: {len(missing)} leaf/leaves have NO verdict {missing[:10]} and "
        f"{len(undeclared)} are still UNDECLARED {undeclared[:10]}.\n"
        "A new schema field / Channel op / Egress mode / aqueduct.yml config key landed "
        "without a per-engine capability verdict. Run "
        "`python scripts/capabilities.py sync` to append the missing leaves as "
        "`undeclared`, then replace each with a real verdict "
        "(supported | unsupported | ignored_with_warning)."
    )


@pytest.mark.parametrize("decl_path", _DECLARATIONS, ids=[p.parent.name for p in _DECLARATIONS])
def test_every_verdict_key_is_a_real_leaf(decl_path):
    """Reverse direction: every declared row must be a real derived leaf."""
    rows = _declared_rows(decl_path)
    leaves = _all_governed_leaves()
    orphaned = sorted(set(rows) - leaves)
    assert not orphaned, (
        f"{decl_path.name} declares capability verdicts for leaf ids that don't exist in "
        f"the grammar or config schema: {orphaned}. Likely a typo, or a leaf that was "
        "renamed/removed without updating the declaration."
    )


def test_declaration_has_no_default_sweep():
    """The regression guard for the tautology itself.

    `all_leaves_default()` (or any equivalent "fill every leaf with one default
    verdict" helper) is what made the closure test unable to fail. If it comes
    back — in the capability model or in any engine's declaration module — the
    guarantee silently dies again, so fail the build on its mere presence.
    """
    import aqueduct.executor.capabilities as caps_mod
    import aqueduct.executor.spark.capabilities as spark_caps_mod

    assert not hasattr(caps_mod, "all_leaves_default"), (
        "all_leaves_default() is back in the capability model. It derives an engine's "
        "table from the SAME walker the closure test compares it against, which makes "
        "the closure guarantee a tautology. Engines declare explicit YAML rows instead."
    )
    # Look for a CALL, not the bare name — both modules' docstrings deliberately
    # name the old helper to explain why it must not come back.
    for mod in (caps_mod, spark_caps_mod):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        assert "all_leaves_default(" not in src.replace("``all_leaves_default(", ""), (
            f"{mod.__name__} calls all_leaves_default — no default-verdict sweep "
            "may exist anywhere in the capability framework."
        )


def test_spark_declares_every_leaf_explicitly():
    """Spark's table is ~261 explicit rows, not a swept default. Guards the
    row count against silently collapsing back to a generated table."""
    rows = _declared_rows(_REPO / "aqueduct" / "executor" / "spark" / "capabilities.yml")
    assert len(rows) == len(_all_governed_leaves())
    assert len(rows) > 200  # sanity floor — grammar + config is not tiny


def test_all_leaves_deterministic():
    """Calling all_leaves() twice returns the identical set (no ordering/randomness bugs)."""
    assert all_leaves() == all_leaves()


def test_all_leaves_nonempty():
    assert len(all_leaves()) > 50  # sanity floor — the grammar is not tiny


# ── Phase 78 Step 3 — config-leaf governance ────────────────────────────────


def test_all_config_leaves_deterministic():
    assert all_config_leaves() == all_config_leaves()


def test_all_config_leaves_nonempty():
    assert len(all_config_leaves()) > 50  # sanity floor — aqueduct.yml is not tiny


def test_all_config_leaves_contains_known_examples():
    """Spot-check leaf ids the brief calls out by name, so a naming-scheme
    refactor is a deliberate, visible test update rather than a silent drift."""
    leaves = all_config_leaves()
    for expected in (
        "config.deployment.master_url",
        "config.deployment.target",
        "config.deployment.engine",
        "config.agent.sandbox_master_url",
        "config.stores.observability.backend",
        "config.checkpoint_root",
        "config.spark_config",
    ):
        assert expected in leaves, f"expected config leaf {expected!r} not derived"


def test_config_leaves_do_not_collide_with_grammar_leaves():
    """The 'config.' prefix must stay disjoint from every grammar leaf
    category (module.*, channel.*, egress.*, ingress.*, junction.*,
    funnel.*, feature.*, agent.*, ...) so the union in a single table is
    unambiguous."""
    assert all_leaves() & all_config_leaves() == frozenset()


# ── The closure guard must be able to FAIL ─────────────────────────────────
#
# These are the tests that would have caught the original tautology. Each one
# plants a leaf into the REAL walker (monkeypatching the walker function that
# `parser/schema.py` / `config.py` feed, which is exactly what adding a schema
# key does to the leaf set) and asserts that both arms of the framework reject
# it: the closure comparison reports it missing, AND engine registration
# (`load_declaration`) hard-fails. A synthetic fixture model would prove
# nothing here — it never touches the path the real declaration is checked on.


def _plant(monkeypatch, leaf_id: str) -> None:
    """Make the real governed-leaf set contain one extra leaf, as if a
    developer had just added a field to parser/schema.py or config.py."""
    import aqueduct.executor.capability_leaves as cl
    import aqueduct.executor.config_leaves as cfgl

    if leaf_id.startswith("config."):
        real = cfgl.all_config_leaves()
        monkeypatch.setattr(cfgl, "all_config_leaves", lambda: real | {leaf_id})
    else:
        real = cl.all_leaves()
        monkeypatch.setattr(cl, "all_leaves", lambda: real | {leaf_id})


@pytest.mark.parametrize(
    "planted",
    [
        "module.field.planted_grammar_leaf",   # as if added to parser/schema.py
        "config.planted_config_key",           # as if added to config.py
    ],
)
def test_closure_fails_on_a_planted_leaf_with_no_verdict(monkeypatch, planted):
    """A real leaf with no verdict in an engine's YAML must be reported missing.

    This is the assertion the old closure test could not make: because Spark's
    table was `all_leaves_default(all_leaves(), SUPPORTED)`, the planted leaf
    was swept into the table by the very walker it was compared against, and
    the diff was always empty.
    """
    _plant(monkeypatch, planted)

    import aqueduct.executor.capability_leaves as cl
    import aqueduct.executor.config_leaves as cfgl

    governed = cl.all_leaves() | cfgl.all_config_leaves()
    assert planted in governed  # the plant took effect

    for decl_path in _DECLARATIONS:
        rows = _declared_rows(decl_path)
        missing = sorted(governed - set(rows))
        assert planted in missing, (
            f"{decl_path.name} silently accepted a brand-new leaf {planted!r} with no "
            "declared verdict — the closure guard is a tautology again."
        )


@pytest.mark.parametrize(
    "planted",
    [
        "module.field.planted_grammar_leaf",
        "config.planted_config_key",
    ],
)
def test_registration_hard_fails_on_a_planted_leaf(planted):
    """The other arm: loading an engine declaration that is missing a verdict
    for a real leaf raises EnginePluginError (an AqueductError), naming the
    leaf. Registration cannot succeed with a half-declared engine."""
    from aqueduct.errors import EnginePluginError
    from aqueduct.executor.capabilities import load_declaration
    from aqueduct.executor.spark.capabilities import DECLARATION_PATH

    governed = _all_governed_leaves() | {planted}
    with pytest.raises(EnginePluginError) as exc:
        load_declaration(DECLARATION_PATH, governed)
    assert planted in str(exc.value)
    assert "scripts/capabilities.py sync" in str(exc.value)


def test_registration_rejects_an_undeclared_row(tmp_path):
    """The UNDECLARED sentinel is a build failure, not a synonym for
    UNSUPPORTED. `sync` parks new leaves there precisely so the build stays
    red until a human decides."""
    from aqueduct.errors import EnginePluginError
    from aqueduct.executor.capabilities import load_declaration

    leaves = frozenset({"feature.a", "feature.b"})
    decl = tmp_path / "capabilities.yml"
    decl.write_text(
        "engine: toy\nleaves:\n  feature.a: supported\n  feature.b: undeclared\n",
        encoding="utf-8",
    )
    with pytest.raises(EnginePluginError, match="UNDECLARED|undeclared"):
        load_declaration(decl, leaves)


def test_registration_rejects_unknown_leaf_and_bad_verdict(tmp_path):
    from aqueduct.errors import EnginePluginError
    from aqueduct.executor.capabilities import load_declaration

    leaves = frozenset({"feature.a"})

    orphan = tmp_path / "orphan.yml"
    orphan.write_text(
        "engine: toy\nleaves:\n  feature.a: supported\n  feature.ghost: supported\n",
        encoding="utf-8",
    )
    with pytest.raises(EnginePluginError, match="not a real capability leaf"):
        load_declaration(orphan, leaves)

    bad = tmp_path / "bad.yml"
    bad.write_text("engine: toy\nleaves:\n  feature.a: sorta_supported\n", encoding="utf-8")
    with pytest.raises(EnginePluginError, match="invalid verdict"):
        load_declaration(bad, leaves)


def test_scaffold_generates_a_complete_all_undeclared_table(tmp_path):
    """`scripts/capabilities.py scaffold --engine X` is the entry point for a NEW
    engine: a COMPLETE table generated from the walker, every row `undeclared`.

    The two properties that matter, both asserted here:
      - it contains EVERY governed leaf (it cannot go stale like a static
        template would), and
      - not one row is `supported` — a new engine must not inherit a default,
        which is exactly what copying Spark's table would do (~261 `supported`
        rows = a silent claim to support the whole grammar).
    """
    import subprocess

    out = tmp_path / "capabilities.yml"
    proc = subprocess.run(
        [sys.executable, str(_REPO / "scripts" / "capabilities.py"),
         "scaffold", "--engine", "toyengine", "--out", str(out)],
        cwd=str(_REPO), capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stderr

    rows = _declared_rows(out)
    governed = _all_governed_leaves()
    assert set(rows) == set(governed), "scaffold must emit every governed leaf"
    verdicts = {_verdict_of(v) for v in rows.values()}
    assert verdicts == {Support.UNDECLARED.value}, (
        f"scaffold must emit ONLY `undeclared` rows, got {verdicts} — a generated "
        "default verdict would let a new engine silently claim capabilities."
    )

    # And that scaffold is unregisterable until a human decides each row.
    from aqueduct.errors import EnginePluginError
    from aqueduct.executor.capabilities import load_declaration

    with pytest.raises(EnginePluginError, match="UNDECLARED|undeclared"):
        load_declaration(out, governed)

    header = out.read_text(encoding="utf-8")
    assert "NOT A FILE TO COPY" in header  # the anti-copy-Spark warning is present


def test_declaration_preserves_requires_and_hint(tmp_path):
    """Capability metadata must survive the move to YAML — a version-gated
    leaf keeps its `requires` specifier and its actionable `hint`."""
    from aqueduct.executor.capabilities import load_declaration

    leaves = frozenset({"ingress.format.custom"})
    decl = tmp_path / "capabilities.yml"
    decl.write_text(
        "engine: toy\n"
        "leaves:\n"
        "  ingress.format.custom:\n"
        "    support: supported\n"
        '    requires:\n      pyspark: ">=4.0"\n'
        '    hint: "needs pyspark 4"\n',
        encoding="utf-8",
    )
    caps = load_declaration(decl, leaves)
    cap = caps.verdict("ingress.format.custom")
    assert cap.support is Support.SUPPORTED
    assert cap.requires == {"pyspark": ">=4.0"}
    assert cap.hint == "needs pyspark 4"


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
