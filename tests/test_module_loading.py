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
