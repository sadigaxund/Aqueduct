"""Tests for aqueduct/infra/module_loading.py — the shared user-code loader
behind custom Assert rules, Probe pointers, python UDFs, custom DataSources,
and the secrets resolver (Phase: custom-callable-import-not-standardized)."""

from __future__ import annotations

import sys

import pytest

from aqueduct.infra.module_loading import load_callable, load_module

pytestmark = [pytest.mark.unit]


def test_load_module_from_base_dir_file(tmp_path):
    (tmp_path / "mymod.py").write_text("VALUE = 42\n")
    mod = load_module("mymod", str(tmp_path))
    assert mod.VALUE == 42


def test_load_module_falls_back_to_import_when_no_file(tmp_path):
    mod = load_module("json", str(tmp_path))
    assert mod is sys.modules["json"]


def test_load_module_falls_back_when_base_dir_none():
    mod = load_module("json", None)
    assert mod is sys.modules["json"]


def test_load_module_missing_both_raises():
    with pytest.raises(ImportError):
        load_module("definitely_not_a_real_module_xyz", None)


def test_load_module_survives_stdlib_name_collision(tmp_path):
    """A user module named e.g. ``os`` (colliding with a heavily-used stdlib
    module) must load from base_dir without touching sys.modules["os"]."""
    pkg_dir = tmp_path / "os"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    (pkg_dir / "extra.py").write_text("VALUE = 7\n")

    sentinel = sys.modules["os"]
    try:
        mod = load_module("os.extra", str(tmp_path))
        assert mod.VALUE == 7
        assert sys.modules["os"] is sentinel
        assert sys.modules["os"].getcwd  # stdlib API still intact
    finally:
        sys.modules.pop("os.extra", None)


def test_load_module_dotted_path_maps_to_nested_file(tmp_path):
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    (pkg_dir / "sub.py").write_text("VALUE = 99\n")
    mod = load_module("pkg.sub", str(tmp_path))
    assert mod.VALUE == 99


def test_load_callable_resolves_module_and_attr(tmp_path):
    (tmp_path / "rules.py").write_text("def check(x):\n    return x > 0\n")
    fn = load_callable("rules.check", str(tmp_path))
    assert fn(1) is True


def test_load_callable_missing_module_raises():
    with pytest.raises(ImportError):
        load_callable("nope_mod_xyz.fn", None)


def test_load_callable_no_dot_raises_value_error():
    """A dotted path with no dot (e.g. a user typo'd ``fn:`` with just a bare
    name) can't be split into module + attribute — rsplit('.', 1) leaves one
    element, so unpacking raises ValueError (load_secret's docstring names
    this explicitly as one of the three exception types callers must wrap)."""
    with pytest.raises(ValueError):
        load_callable("no_dot_at_all", None)


def test_load_module_reloads_from_file_on_every_call(tmp_path):
    """No caching / no sys.modules registration for a base_dir file load — two
    calls after editing the file on disk both see the CURRENT contents."""
    mod_file = tmp_path / "reloadme.py"
    mod_file.write_text("VALUE = 1\n")
    first = load_module("reloadme", str(tmp_path))
    assert first.VALUE == 1

    mod_file.write_text("VALUE = 2\n")
    second = load_module("reloadme", str(tmp_path))
    assert second.VALUE == 2
    assert "reloadme" not in sys.modules


def test_load_module_raises_import_error_when_spec_is_none(tmp_path, monkeypatch):
    """A base_dir file exists but spec_from_file_location fails to produce a
    usable spec (e.g. a loader-less/corrupt location) — surfaces as
    ImportError, not a silent None-returning path or an AttributeError."""
    (tmp_path / "brokenmod.py").write_text("VALUE = 1\n")
    import importlib.util as _ilu

    monkeypatch.setattr(_ilu, "spec_from_file_location", lambda *a, **k: None)
    with pytest.raises(ImportError, match="could not load spec"):
        load_module("brokenmod", str(tmp_path))
