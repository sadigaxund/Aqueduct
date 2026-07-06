"""Phase 60 — custom probe signal resolver (engine-agnostic, no Spark)."""
from __future__ import annotations

import pytest

from aqueduct.errors import ConfigError
from aqueduct.executor.probe_plugins import (
    AQ_PROBE_ENTRYPOINT_GROUP,
    custom_signal_source,
    resolve_callable,
)

pytestmark = [pytest.mark.unit]


def _demo_signal(df, sig_cfg):  # referenced by the pointer-resolution test
    return {"estimate": 42, "metadata": {}, "passed": True}


# ── custom_signal_source classification ─────────────────────────────────────

def test_source_sql():
    assert custom_signal_source({"sql": "MAX(x)"}) == "sql"
    assert custom_signal_source({"passed_when": "MAX(x) > 0"}) == "sql"


def test_source_pointer():
    assert custom_signal_source({"module": "m", "entry": "e"}) == "pointer"


def test_source_plugin():
    assert custom_signal_source({"plugin": "p"}) == "plugin"


def test_source_none_raises():
    with pytest.raises(ConfigError, match="exactly one source"):
        custom_signal_source({})


def test_source_conflict_raises():
    with pytest.raises(ConfigError, match="conflicting sources"):
        custom_signal_source({"sql": "MAX(x)", "plugin": "p"})


def test_source_partial_pointer_raises():
    with pytest.raises(ConfigError, match="BOTH 'module' and 'entry'"):
        custom_signal_source({"module": "m"})


# ── resolve_callable ────────────────────────────────────────────────────────

def test_resolve_pointer_imports_callable():
    fn = resolve_callable(
        {"module": "tests.test_executor.test_probe_plugins", "entry": "_demo_signal"}
    )
    assert fn(None, {}) == {"estimate": 42, "metadata": {}, "passed": True}


def test_resolve_pointer_not_callable_raises():
    with pytest.raises(ConfigError, match="is not a callable"):
        resolve_callable(
            {"module": "tests.test_executor.test_probe_plugins", "entry": "AQ_PROBE_ENTRYPOINT_GROUP"}
        )


def test_resolve_missing_plugin_raises():
    with pytest.raises(ConfigError, match="not found in entry-point group"):
        resolve_callable({"plugin": "definitely_not_registered_xyz"})


def test_resolve_sql_form_raises():
    with pytest.raises(ConfigError, match="inline-SQL"):
        resolve_callable({"sql": "MAX(x)"})


def test_entrypoint_group_name_stable():
    # The group name is a public contract for plugin authors — guard it.
    assert AQ_PROBE_ENTRYPOINT_GROUP == "aqueduct.probe_signals"


# ── resolve_callable base_dir (Manifest.base_dir sibling-file resolution) ───

def test_resolve_pointer_survives_stdlib_name_collision(tmp_path):
    """A probe module named e.g. ``types`` (stdlib collision) must still load
    correctly, and must never touch the colliding sys.modules entry — same
    bug class as the secrets resolver, see tests/test_secrets.py."""
    import sys

    pkg_dir = tmp_path / "types"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    (pkg_dir / "probe.py").write_text(
        "def check(df, sig_cfg):\n    return {'estimate': 1, 'metadata': {}, 'passed': True}\n"
    )

    sentinel = sys.modules["types"]
    try:
        fn = resolve_callable(
            {"module": "types.probe", "entry": "check"}, base_dir=str(tmp_path)
        )
        assert fn(None, {}) == {"estimate": 1, "metadata": {}, "passed": True}
        assert sys.modules["types"] is sentinel
    finally:
        sys.modules.pop("types.probe", None)


def test_resolve_pointer_falls_back_to_import_when_no_file(tmp_path):
    fn = resolve_callable(
        {"module": "tests.test_executor.test_probe_plugins", "entry": "_demo_signal"},
        base_dir=str(tmp_path),
    )
    assert fn(None, {}) == {"estimate": 42, "metadata": {}, "passed": True}


def test_resolve_pointer_missing_both_raises():
    with pytest.raises(ImportError):
        resolve_callable({"module": "nope_mod_xyz", "entry": "fn"}, base_dir=None)
