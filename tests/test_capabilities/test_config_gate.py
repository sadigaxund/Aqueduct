"""Phase 78 Step 3 — the config-leaf governance gate.

Closes the gap left by Step 0's leaf walker: it covers the BLUEPRINT grammar
only (`parser/schema.py`), so an ENGINE-CONFIG key (`aqueduct.yml` ->
`aqueduct/config.py`) that means nothing on the target engine (e.g.
`engine.spark.master_url` on a single-node engine) was a silent no-op. This
file proves:

  1. The gate is a genuine NO-OP for Spark today — every real `aqueduct.yml`
     in the repo, and the default `AqueductConfig()`, resolve every
     explicitly-set config leaf to `SUPPORTED` on `engine="spark"`, and the
     actual `warnings.warn` call path never fires for a representative file.
  2. The mechanism works for a FUTURE engine: a fake engine that declares
     `config.engine.spark.master_url` as `IGNORED_WITH_WARNING` gets a
     suppressible `engine_key_ignored` warning at config-resolution time
     when a user's `aqueduct.yml` sets that key — the exact DuckDB-day proof
     the brief asks for, without registering anything DuckDB-specific.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

import aqueduct.executor.spark.capabilities  # noqa: F401 — registers "spark"
from aqueduct.config import AqueductConfig, _warn_ignored_config_keys, load_config
from aqueduct.executor.capabilities import (
    CAPABILITY_REGISTRY,
    Capability,
    EngineCapabilities,
    Support,
    get_capabilities,
    load_engines,
    register,
)
from aqueduct.executor.capability_leaves import all_leaves as bp_leaves
from aqueduct.executor.config_leaves import all_config_leaves, explicitly_set_config_leaves
from aqueduct.warnings import AqueductWarning

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_GALLERY_CONFIGS = sorted((_REPO / "gallery" / "snippets").glob("*/aqueduct.yml"))

_ENGINE_NS_PREFIX = "config.engine."


def _namespaced_engine(leaf_id: str) -> str | None:
    """Engine name a `config.engine.<name>.*` leaf belongs to, else None.

    Derived from the leaf id itself, never a hand-kept list of engine names —
    the same reason `all_config_leaves()` derives its namespace filter from the
    `engine_scoped` tag instead of enumerating engines.
    """
    if not leaf_id.startswith(_ENGINE_NS_PREFIX):
        return None
    rest = leaf_id[len(_ENGINE_NS_PREFIX) :]
    return rest.split(".", 1)[0] if "." in rest else None


# ── 1. No-op proof (Spark) ──────────────────────────────────────────────────


def test_default_config_is_noop_on_spark():
    cfg = AqueductConfig()
    leaves = explicitly_set_config_leaves(cfg)
    caps = get_capabilities("spark")
    non_supported = [
        leaf_id for leaf_id in leaves if caps.verdict(leaf_id).support != Support.SUPPORTED
    ]
    assert non_supported == []


@pytest.mark.parametrize("cfg_path", _GALLERY_CONFIGS, ids=[p.parent.name for p in _GALLERY_CONFIGS])
def test_gallery_configs_set_no_dead_config_leaves(cfg_path):
    """Every leaf a real gallery aqueduct.yml explicitly sets must be honoured
    by SOME registered engine — checked against the capability tables directly,
    so the result never depends on a given file's own `warnings.suppress`.

    Two rules, because `engine.<name>.*` namespacing (Q4) made a single
    spark-only rule wrong:

    * An engine-NEUTRAL leaf must be SUPPORTED on spark. Unchanged, and it is
      what catches a genuinely dead knob.
    * A namespaced `config.engine.<name>.*` leaf must name a REGISTERED engine
      and be SUPPORTED on THAT engine.

    Why the second rule exists: every engine declares every OTHER engine's
    namespaced keys UNSUPPORTED, which is Q4's design working as intended (a
    foreign engine key is accepted and ignored with a suppressible
    `engine_key_ignored` warning, never an error). Under the old
    must-be-SUPPORTED-on-spark rule that made it impossible for any gallery
    snippet to configure DuckDB at all — `23_table_first` needs
    `engine.duckdb.database_path` because an unset path means `:memory:`, and
    nothing survives between its populate script and the run.

    This is not a weakening: dead config still fails, because a namespaced leaf
    must be SUPPORTED on the engine that owns it.

    On typos specifically, measured rather than assumed: an unknown engine name
    never reaches this test. `engine.dukdb.database_path` is rejected at config
    LOAD by pydantic's `extra="forbid"` on the `engine:` block
    ("engine.dukdb — Extra inputs are not permitted"), so the
    unregistered-engine branch below is defence in depth against a leaf id that
    arrives some other way, not the primary typo guard.
    """
    cfg = load_config(cfg_path)
    leaves = explicitly_set_config_leaves(cfg)
    # Resolve engines through the entry-point registry, the same seam
    # `get_capabilities()` uses on the real compile path — so a third engine
    # registered tomorrow is covered here with no edit to this test.
    load_engines()
    registered = set(CAPABILITY_REGISTRY)

    dead: list[str] = []
    for leaf_id in leaves:
        owner = _namespaced_engine(leaf_id)
        if owner is None:
            if get_capabilities("spark").verdict(leaf_id).support != Support.SUPPORTED:
                dead.append(f"{leaf_id} (engine-neutral, not SUPPORTED on spark)")
        elif owner not in registered:
            dead.append(f"{leaf_id} (names unregistered engine {owner!r})")
        elif get_capabilities(owner).verdict(leaf_id).support != Support.SUPPORTED:
            dead.append(f"{leaf_id} (not SUPPORTED on its own engine {owner!r})")

    assert dead == [], f"{cfg_path} sets config leaves no engine honours: {dead}"


def test_representative_config_emits_zero_engine_key_ignored_warnings():
    """A real gallery aqueduct.yml, loaded through the ACTUAL warnings.warn
    path (not the leaf-table shortcut above), with its own suppress list
    overridden to empty so a real regression would be observable here."""
    cfg_path = _GALLERY_CONFIGS[0]
    cfg = load_config(cfg_path)
    # Override to an empty suppress set — every gallery aqueduct.yml ships
    # `warnings.suppress: ["*"]`, which would hide a real regression here.
    cfg_no_suppress = cfg.model_copy(update={"warnings": cfg.warnings.model_copy(update={"suppress": []})})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_ignored_config_keys(cfg_no_suppress)
    ignored = [w for w in caught if "engine_key_ignored" in str(w.message)]
    assert ignored == []


def test_load_config_default_missing_file_is_noop(tmp_path, monkeypatch):
    """`load_config()` with no file present (the documented default-behavior
    path) must not emit any engine_key_ignored warning."""
    monkeypatch.chdir(tmp_path)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        load_config()
    ignored = [w for w in caught if "engine_key_ignored" in str(w.message)]
    assert ignored == []


# ── 2. The mechanism works for a future engine (fake-engine proof) ─────────


@pytest.fixture
def _fake_engine():
    """Register a throwaway engine, default-ALLOW like Spark, except
    `config.engine.spark.master_url` is IGNORED_WITH_WARNING — exactly the
    DuckDB-day scenario the brief describes, with zero DuckDB-specific code."""
    all_governed = bp_leaves() | all_config_leaves()
    # Built explicitly rather than via a default sweep — the sweep is exactly the
    # tautology this phase removed. This is a TEST double standing in for an
    # engine's YAML declaration; a real engine ships capabilities.yml instead.
    table = {leaf: Capability(support=Support.SUPPORTED) for leaf in all_governed}
    table["config.engine.spark.master_url"] = Capability(
        support=Support.IGNORED_WITH_WARNING,
        hint="fake_single_node is single-node; master_url is meaningless here.",
    )
    register(EngineCapabilities(engine="fake_single_node", table=table))
    yield "fake_single_node"
    from aqueduct.executor.capabilities import CAPABILITY_REGISTRY

    CAPABILITY_REGISTRY.pop("fake_single_node", None)


def _cfg_for_fake_engine(**overrides) -> AqueductConfig:
    data = {
        "deployment": {"engine": "fake_single_node", "target": "local"},
        "engine": {"spark": {"master_url": "local[2]"}},
    }
    data.update(overrides)
    return AqueductConfig.model_validate(data)


def test_fake_engine_ignored_config_key_warns(_fake_engine):
    cfg = _cfg_for_fake_engine()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_ignored_config_keys(cfg)
    msgs = [str(w.message) for w in caught if issubclass(w.category, AqueductWarning)]
    matching = [m for m in msgs if "engine_key_ignored" in m and "config.engine.spark.master_url" in m]
    assert len(matching) == 1, f"expected exactly one engine_key_ignored warning for master_url, got: {msgs}"
    assert "fake_single_node" in matching[0]
    assert "master_url is meaningless" in matching[0]  # the hint is actionable, not just the rule id


def test_fake_engine_warning_is_suppressible(_fake_engine):
    cfg = _cfg_for_fake_engine(warnings={"suppress": ["engine_key_ignored"]})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_ignored_config_keys(cfg)
    assert caught == []


def test_fake_engine_suppress_all_wildcard(_fake_engine):
    cfg = _cfg_for_fake_engine(warnings={"suppress": ["*"]})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_ignored_config_keys(cfg)
    assert caught == []


def test_fake_engine_never_hard_errors(_fake_engine):
    """The core asymmetry vs the blueprint gate: a config leaf that means
    nothing on the target engine must never raise — only warn. Loading the
    config itself must succeed even with the "ignored" key set."""
    cfg = _cfg_for_fake_engine()  # must not raise
    assert cfg.deployment.engine == "fake_single_node"


def test_config_leaf_verdict_reused_rule_id_matches_blueprint_gate():
    """The config gate must reuse the exact same rule_id the compile-time
    blueprint gate uses — one suppression vocabulary, not two."""
    from aqueduct.compiler.capability_check import RULE_ID_IGNORED

    assert RULE_ID_IGNORED == "engine_key_ignored"
