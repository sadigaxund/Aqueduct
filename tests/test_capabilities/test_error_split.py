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

from aqueduct.errors import AqueductError, CapabilityDeclarationError, EnginePluginError
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
