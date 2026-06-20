"""Phase 60 — custom probe signal resolver (engine-agnostic, no Spark)."""
from __future__ import annotations

import pytest

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
    with pytest.raises(ValueError, match="exactly one source"):
        custom_signal_source({})


def test_source_conflict_raises():
    with pytest.raises(ValueError, match="conflicting sources"):
        custom_signal_source({"sql": "MAX(x)", "plugin": "p"})


def test_source_partial_pointer_raises():
    with pytest.raises(ValueError, match="BOTH 'module' and 'entry'"):
        custom_signal_source({"module": "m"})


# ── resolve_callable ────────────────────────────────────────────────────────

def test_resolve_pointer_imports_callable():
    fn = resolve_callable(
        {"module": "tests.test_executor.test_probe_plugins", "entry": "_demo_signal"}
    )
    assert fn(None, {}) == {"estimate": 42, "metadata": {}, "passed": True}


def test_resolve_pointer_not_callable_raises():
    with pytest.raises(ValueError, match="is not a callable"):
        resolve_callable(
            {"module": "tests.test_executor.test_probe_plugins", "entry": "AQ_PROBE_ENTRYPOINT_GROUP"}
        )


def test_resolve_missing_plugin_raises():
    with pytest.raises(ValueError, match="not found in entry-point group"):
        resolve_callable({"plugin": "definitely_not_registered_xyz"})


def test_resolve_sql_form_raises():
    with pytest.raises(ValueError, match="inline-SQL"):
        resolve_callable({"sql": "MAX(x)"})


def test_entrypoint_group_name_stable():
    # The group name is a public contract for plugin authors — guard it.
    assert AQ_PROBE_ENTRYPOINT_GROUP == "aqueduct.probe_signals"
