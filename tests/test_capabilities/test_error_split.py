"""Two states, two error types — `EnginePluginError` vs `CapabilityDeclarationError`.

They were conflated: an incomplete/invalid `capabilities.yml` raised
`EnginePluginError`, whose message ends "Reinstall or uninstall the package
providing it." That advice is correct for what `EnginePluginError` was FOR (an
`aqueduct.engines` entry point that failed to IMPORT — a broken/half-installed
plugin) and useless for what it had also come to cover (a first-party developer
added a schema key, so every engine's table is now missing a row — reinstalling
fixes nothing). These tests pin the split, and pin that callers branch by TYPE
rather than by matching message substrings (the invariant AGENTS.md records, and
the one that was already violated once this phase).
"""

from __future__ import annotations

import pytest

from aqueduct.errors import (
    AqueductError,
    CapabilityDeclarationError,
    CapabilityScopeError,
    EnginePluginError,
)
from aqueduct.executor.capabilities import load_declaration

pytestmark = pytest.mark.unit


class _BoomEntryPoint:
    """An `aqueduct.engines` entry point whose module does not import."""

    name = "boom"
    value = "not_a_real_module.engine"
    group = "aqueduct.engines"

    def load(self):
        raise ImportError("No module named 'not_a_real_module'")


@pytest.fixture()
def _reset_engine_load_cache():
    import aqueduct.executor.capabilities as caps

    prev_loaded = caps._engines_loaded
    prev_registry = dict(caps.CAPABILITY_REGISTRY)
    caps._engines_loaded = False
    yield caps
    caps._engines_loaded = prev_loaded
    caps.CAPABILITY_REGISTRY.clear()
    caps.CAPABILITY_REGISTRY.update(prev_registry)


def test_both_are_aqueduct_errors_but_distinct_types():
    assert issubclass(EnginePluginError, AqueductError)
    assert issubclass(CapabilityDeclarationError, AqueductError)
    assert not issubclass(CapabilityDeclarationError, EnginePluginError)
    assert not issubclass(EnginePluginError, CapabilityDeclarationError)


def test_capability_scope_error_is_a_sibling_not_a_subclass():
    """Q4 step 2's THIRD error type (a config.* leaf's engine-scoping is
    undecided — see aqueduct/errors.py::CapabilityScopeError). It must be a
    direct AqueductError subclass, deliberately NOT derived from
    CapabilityDeclarationError, so a `except CapabilityDeclarationError:`
    site (aqueduct/doctor/checks_io.py) cannot swallow it and re-conflate
    the two — the same mistake this file's docstring records for the
    EnginePluginError/CapabilityDeclarationError split."""
    assert issubclass(CapabilityScopeError, AqueductError)
    assert not issubclass(CapabilityScopeError, CapabilityDeclarationError)
    assert not issubclass(CapabilityDeclarationError, CapabilityScopeError)
    assert not issubclass(CapabilityScopeError, EnginePluginError)
    assert not issubclass(EnginePluginError, CapabilityScopeError)


def test_failed_entry_point_import_is_an_engine_plugin_error(monkeypatch, _reset_engine_load_cache):
    """State 1 — the plugin is broken/half-installed. Reinstall advice is RIGHT."""
    import importlib.metadata

    caps = _reset_engine_load_cache
    monkeypatch.setattr(importlib.metadata, "entry_points", lambda **kw: [_BoomEntryPoint()])

    with pytest.raises(EnginePluginError) as exc:
        caps.load_engines()

    msg = str(exc.value)
    assert "boom" in msg  # names the entry point
    assert "not_a_real_module.engine" in msg  # names its target
    assert "ImportError" in msg  # names the underlying cause
    assert "Reinstall or uninstall" in msg  # the fix for THIS state
    assert not isinstance(exc.value, CapabilityDeclarationError)


def test_incomplete_declaration_is_a_capability_declaration_error(tmp_path):
    """State 2 — a leaf has no verdict. Reinstalling fixes NOTHING; the fix is to
    run sync and declare a verdict, and the message must say so."""
    decl = tmp_path / "capabilities.yml"
    decl.write_text("engine: toy\nleaves:\n  feature.a: supported\n", encoding="utf-8")

    with pytest.raises(CapabilityDeclarationError) as exc:
        load_declaration(decl, frozenset({"feature.a", "feature.b"}))

    msg = str(exc.value)
    assert "feature.b" in msg  # names the offending leaf
    assert "aqueduct dev capabilities sync" in msg  # the ACTUAL fix
    assert "supported | unsupported | ignored_with_warning" in msg
    assert "einstall" not in msg  # no reinstall advice — it cannot help here
    assert exc.value.leaves == ["feature.b"]  # structured, not prose-only
    assert exc.value.engine == "toy"
    assert not isinstance(exc.value, EnginePluginError)


def test_undeclared_row_is_a_declaration_error_not_a_plugin_error(tmp_path):
    decl = tmp_path / "capabilities.yml"
    decl.write_text(
        "engine: toy\nleaves:\n  feature.a: supported\n  feature.b: undeclared\n",
        encoding="utf-8",
    )
    with pytest.raises(CapabilityDeclarationError) as exc:
        load_declaration(decl, frozenset({"feature.a", "feature.b"}))
    assert "einstall" not in str(exc.value)
    assert exc.value.leaves == ["feature.b"]


@pytest.mark.parametrize(
    "body, needle",
    [
        ("engine: toy\nleaves:\n  feature.ghost: supported\n", "not a real capability leaf"),
        ("engine: toy\nleaves:\n  feature.a: sorta\n", "invalid verdict"),
        (
            "engine: toy\nleaves:\n  feature.a:\n    support: supported\n"
            '    requires:\n      dep: "not a specifier"\n',
            "specifier",
        ),
    ],
)
def test_invalid_declarations_are_declaration_errors(tmp_path, body, needle):
    decl = tmp_path / "capabilities.yml"
    decl.write_text(body, encoding="utf-8")
    with pytest.raises(CapabilityDeclarationError, match=needle):
        load_declaration(decl, frozenset({"feature.a"}))


def test_declaration_error_survives_load_engines_unwrapped(
    monkeypatch, tmp_path, _reset_engine_load_cache
):
    """The regression guard for the conflation itself: `load_engines()` wraps a
    failing entry point in `EnginePluginError`, and its `except Exception` is
    deliberately broad. A CapabilityDeclarationError raised while the engine
    module imports must NOT be swallowed by it — re-wrapping would replace the
    leaf names with "reinstall the package", which is exactly the useless advice
    this split removes."""
    import importlib.metadata

    caps = _reset_engine_load_cache

    class _HalfDeclaredEngine:
        name = "toy"
        value = "toy_engine.module"
        group = "aqueduct.engines"

        def load(self):
            raise CapabilityDeclarationError(
                "capability declaration /x/capabilities.yml (engine 'toy') is incomplete "
                "— 1 leaf/leaves are still UNDECLARED: ['feature.b']. Run "
                "`aqueduct dev capabilities sync`",
                engine="toy",
                leaves=["feature.b"],
            )

    monkeypatch.setattr(importlib.metadata, "entry_points", lambda **kw: [_HalfDeclaredEngine()])

    with pytest.raises(CapabilityDeclarationError) as exc:
        caps.load_engines()
    assert exc.value.leaves == ["feature.b"]
    assert "einstall" not in str(exc.value)


def test_doctor_reports_a_declaration_error_as_fail_not_a_blueprint_skip(monkeypatch, tmp_path):
    """A caller branching by TYPE: doctor must not misfile a declaration error as
    'blueprint did not parse/compile' (a `skip`) — that sends the user hunting
    through their YAML for a bug in the engine's capability table."""
    from aqueduct.doctor import checks_io

    bp = tmp_path / "blueprint.yml"
    bp.write_text("aqueduct: '1.0'\npipeline:\n  name: p\nmodules: []\n", encoding="utf-8")

    def _boom(*a, **kw):
        raise CapabilityDeclarationError(
            "capability declaration x is incomplete — 1 leaf/leaves are still "
            "UNDECLARED: ['feature.b']. Run `aqueduct dev capabilities sync`",
            engine="toy",
            leaves=["feature.b"],
        )

    monkeypatch.setattr("aqueduct.parser.parser.parse", _boom)

    results = checks_io.check_capabilities(bp, engine="spark")
    assert [r.status for r in results] == ["fail"]
    assert "feature.b" in results[0].detail


# ── Q4 step 2 — CapabilityScopeError (the third state) ─────────────────────


def test_capability_scope_error_raised_for_any_untagged_field():
    """The general form (mandatory, not just the engine.<name>.* case): ANY
    config.* field with no explicit engine_scoped tag raises — there is no
    'untagged means core' fallback anywhere. A field with neither True nor
    False is nobody's decision, and that must be loud everywhere in the
    model, not only inside a per-engine block."""
    from pydantic import BaseModel, ConfigDict, Field

    from aqueduct.executor import config_leaves as cfgl

    class _UntaggedTopLevel(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        knob: int = 1  # no engine_scoped tag at all, and NOT under engine.<name>.*

    with pytest.raises(CapabilityScopeError) as exc:
        cfgl._walk_tagged(_UntaggedTopLevel, "config", (_UntaggedTopLevel,), False)
    assert "config.knob" in str(exc.value)
    assert "engine_scoped" in str(exc.value)
    assert "True" in str(exc.value) and "False" in str(exc.value)  # both resolutions named


def test_capability_scope_error_raised_for_false_tagged_engine_block_field():
    """The contradiction form: a field under engine.<name>.* CANNOT be
    engine_scoped: False either — there is no valid 'core' reading for
    something namespaced to exactly one engine."""
    from pydantic import BaseModel, ConfigDict, Field

    from aqueduct.executor import config_leaves as cfgl

    class _FalseTaggedEngineBlock(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        memory_limit: str = Field(
            default="2GB", json_schema_extra={"engine_scoped": False},
        )

    class _EngineRouting(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        duckdb: _FalseTaggedEngineBlock = Field(default_factory=_FalseTaggedEngineBlock)

    class _FakeConfig(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        engine: _EngineRouting = Field(default_factory=_EngineRouting)

    with pytest.raises(CapabilityScopeError) as exc:
        cfgl._walk_tagged(_FakeConfig, "config", (_FakeConfig,), False)
    assert "config.engine.duckdb.memory_limit" in str(exc.value)
    assert "engine_scoped: False" in str(exc.value)
    assert "contradiction" in str(exc.value)


def test_capability_scope_error_raised_for_untagged_engine_block_field():
    """The walker-level guard (aqueduct/executor/config_leaves.py): a field
    discovered under an `engine.<name>.*` block with no
    `engine_scoped: True` tag has no valid 'core' reading (it is namespaced
    to exactly one engine), so it raises at the WALKER — never CI-only.
    Fires at real registration time via `all_config_leaves(engine=...)`,
    which every engine's `capabilities.py` calls at import."""
    from pydantic import BaseModel, ConfigDict, Field

    from aqueduct.executor import config_leaves as cfgl

    class _UntaggedEngineBlock(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        memory_limit: str = Field(default="2GB")  # no engine_scoped tag — the bug

    class _EngineRouting(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        duckdb: _UntaggedEngineBlock = Field(default_factory=_UntaggedEngineBlock)

    class _FakeConfig(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        engine: _EngineRouting = Field(default_factory=_EngineRouting)

    with pytest.raises(CapabilityScopeError) as exc:
        cfgl._walk_tagged(_FakeConfig, "config", (_FakeConfig,), False)
    assert "config.engine.duckdb.memory_limit" in str(exc.value)
    assert "engine_scoped" in str(exc.value)


def test_capability_scope_error_names_a_valid_resolution():
    """Both legal fixes are named, per the hard constraint on this error."""
    from pydantic import BaseModel, ConfigDict, Field

    from aqueduct.executor import config_leaves as cfgl

    class _Untagged(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        knob: int = 1

    class _Routing(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        toy: _Untagged = Field(default_factory=_Untagged)

    class _Cfg(BaseModel):
        model_config = ConfigDict(frozen=True, extra="forbid")
        engine: _Routing = Field(default_factory=_Routing)

    with pytest.raises(CapabilityScopeError) as exc:
        cfgl._walk_tagged(_Cfg, "config", (_Cfg,), False)
    msg = str(exc.value)
    assert "config.engine.toy.knob" in msg
    assert "engine_scoped: True" in msg  # resolution 1: tag it


def test_doctor_reports_a_scope_error_as_fail_not_a_blueprint_skip(monkeypatch, tmp_path):
    """Mirrors ``test_doctor_reports_a_declaration_error_as_fail_not_a_blueprint_skip``
    for the third error type — a shared handler is fine (same response
    shape), silent conflation into the broad `except Exception` catch-all
    (which files it as 'blueprint did not parse/compile', a `skip`) is not."""
    from aqueduct.doctor import checks_io

    bp = tmp_path / "blueprint.yml"
    bp.write_text("aqueduct: '1.0'\npipeline:\n  name: p\nmodules: []\n", encoding="utf-8")

    def _boom(*a, **kw):
        raise CapabilityScopeError(
            "'config.engine.duckdb.memory_limit' lives under an engine.<name>.* "
            "block but carries no engine_scoped tag. Tag it engine_scoped: True."
        )

    monkeypatch.setattr("aqueduct.parser.parser.parse", _boom)

    results = checks_io.check_capabilities(bp, engine="spark")
    assert [r.status for r in results] == ["fail"]
    assert "memory_limit" in results[0].detail
    assert "engine_scoped" in results[0].detail
