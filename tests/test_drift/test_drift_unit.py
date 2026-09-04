"""Phase 58 (aqueduct drift) — classifier + store (report-only).

Pure / store-only unit tests (no Spark). The live source read and full
`aqueduct drift` command are covered by a stub in tests/test_backlog.py.
"""

from __future__ import annotations

import pytest

from aqueduct.drift.classifier import SchemaChange, diff_schemas

pytestmark = pytest.mark.unit


# ── classifier ──────────────────────────────────────────────────────────────


def test_no_drift_when_identical():
    r = diff_schemas({"a": "int", "b": "string"}, {"a": "int", "b": "string"})
    assert not r.has_drift and r.status == "no_drift"


def test_dropped_column_is_breaking():
    r = diff_schemas({"a": "int", "b": "string"}, {"a": "int"})
    assert r.has_breaking and r.status == "drift_breaking"
    assert r.dropped_columns == ("b",)
    assert [c.kind for c in r.breaking] == ["dropped"]


def test_type_change_is_breaking():
    r = diff_schemas({"amount": "double"}, {"amount": "string"})
    assert r.has_breaking
    (c,) = r.breaking
    assert c.kind == "type_changed" and c.baseline_type == "double" and c.live_type == "string"


def test_added_column_is_benign():
    r = diff_schemas({"a": "int"}, {"a": "int", "b": "string"})
    assert r.has_drift and not r.has_breaking
    assert r.status == "drift_benign" and r.added_columns == ("b",)


def test_rename_surfaces_as_drop_plus_add():
    # rename amount → amount_usd: drop (breaking) + add (benign)
    r = diff_schemas({"amount": "double", "id": "int"}, {"amount_usd": "double", "id": "int"})
    assert r.has_breaking
    assert r.dropped_columns == ("amount",) and r.added_columns == ("amount_usd",)


def test_schemachange_breaking_property():
    assert SchemaChange("x", "dropped").breaking
    assert SchemaChange("x", "type_changed").breaking
    assert not SchemaChange("x", "added").breaking


# ── store baseline round-trip ───────────────────────────────────────────────


def test_baseline_roundtrip(tmp_path):
    from aqueduct.drift import store as ds
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    obs = DuckDBObservabilityStore(str(tmp_path / "o.db"))
    ds.ensure_schema(obs)
    assert ds.get_baseline(obs, "bp.x", "load") is None

    ds.record_check(
        obs,
        blueprint_id="bp.x",
        module_id="load",
        baseline_schema=None,
        live_schema={"a": "int"},
        status="baseline_set",
    )
    assert ds.get_baseline(obs, "bp.x", "load") == {"a": "int"}

    # newest live_schema becomes the baseline
    ds.record_check(
        obs,
        blueprint_id="bp.x",
        module_id="load",
        baseline_schema={"a": "int"},
        live_schema={"a": "int", "b": "string"},
        status="drift_benign",
    )
    assert ds.get_baseline(obs, "bp.x", "load") == {"a": "int", "b": "string"}
