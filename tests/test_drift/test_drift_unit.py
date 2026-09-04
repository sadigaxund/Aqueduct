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


# ── batched writer: connection-per-write churn guard ───────────────────────


def test_batched_helpers_use_one_connection_for_200_modules(tmp_path, monkeypatch):
    """`get_baselines`/`record_checks` are the batched siblings of
    `get_baseline`/`record_check` that `aqueduct drift` (aqueduct/cli/drift.py)
    now calls once per command invocation instead of once per Ingress module.

    Before this fix, `aqueduct drift` on a 200-Ingress-module Blueprint opened
    200 connections for `get_baseline` + 200 for `record_check` = 400 DuckDB
    `connect()`/`close()` cycles for one command run (measured directly: the
    old per-module loop called `ds.get_baseline` then `ds.record_check` inside
    `for mod in ingress`, each opening its own `with observability_store.
    connect()`). This test proves the batched replacement opens exactly 2 —
    one connect() for all 200 baseline reads, one for all 200 check writes —
    regardless of module count, so it stays a real regression guard and not a
    tautology.
    """
    import aqueduct.stores.duckdb_ as duckdb_store_mod
    from aqueduct.drift import store as ds
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    obs = DuckDBObservabilityStore(str(tmp_path / "o.db"))
    ds.ensure_schema(obs)

    connect_calls = 0
    real_connect_with_retry = duckdb_store_mod._connect_with_retry

    def _counting_connect(path):
        nonlocal connect_calls
        connect_calls += 1
        return real_connect_with_retry(path)

    monkeypatch.setattr(duckdb_store_mod, "_connect_with_retry", _counting_connect)

    module_ids = [f"module_{i}" for i in range(200)]

    # No baselines recorded yet — one connect() covering all 200 lookups.
    connect_calls = 0
    baselines = ds.get_baselines(obs, "bp.batch", module_ids)
    assert baselines == {}
    assert connect_calls == 1

    # One connect() covering all 200 writes (an executemany, not 200 inserts).
    connect_calls = 0
    checks = [
        {
            "blueprint_id": "bp.batch",
            "module_id": module_id,
            "baseline_schema": None,
            "live_schema": {"a": "int"},
            "status": "baseline_set",
        }
        for module_id in module_ids
    ]
    ids = ds.record_checks(obs, checks)
    assert len(ids) == 200
    assert connect_calls == 1

    # And the second connect() actually persisted all 200 rows, readable back
    # through the same batched lookup in a THIRD single connect().
    connect_calls = 0
    baselines = ds.get_baselines(obs, "bp.batch", module_ids)
    assert len(baselines) == 200
    assert connect_calls == 1
