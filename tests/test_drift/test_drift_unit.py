"""Phase 58 (aqueduct drift) — classifier + synthetic context + store.

Pure / store-only unit tests (no Spark). The live source read and full
`aqueduct drift` command are covered by a stub in tests/test_backlog.py.
"""
from __future__ import annotations

import pytest

from aqueduct.drift.classifier import SchemaChange, diff_schemas
from aqueduct.drift.context import PREDICTED_DRIFT_ERROR_CLASS, build_synthetic_failure_context

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


# ── synthetic FailureContext ────────────────────────────────────────────────

def test_synthetic_fc_surfaces_rename_candidates():
    r = diff_schemas({"amount": "double", "id": "int"}, {"amount_usd": "double", "id": "int"})
    fc = build_synthetic_failure_context("bp.x", "load", r, "{}", engine="duckdb")
    assert fc.error_class == PREDICTED_DRIFT_ERROR_CLASS
    assert fc.failed_module == "load"
    assert fc.object_name == "amount"           # the missing column
    assert fc.suggested_columns == ("amount_usd",)  # rename candidate for the agent
    assert fc.run_id.startswith("drift-")
    assert "Predicted schema drift" in fc.error_message
    # the predicted context must carry the deployment's real engine, not a
    # silently-inherited "spark" default (that was the exact bug this task fixes)
    assert fc.engine == "duckdb"


# ── store baseline round-trip ───────────────────────────────────────────────

def test_baseline_roundtrip(tmp_path):
    from aqueduct.drift import store as ds
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    obs = DuckDBObservabilityStore(str(tmp_path / "o.db"))
    ds.ensure_schema(obs)
    assert ds.get_baseline(obs, "bp.x", "load") is None

    ds.record_check(obs, blueprint_id="bp.x", module_id="load",
                    baseline_schema=None, live_schema={"a": "int"}, status="baseline_set")
    assert ds.get_baseline(obs, "bp.x", "load") == {"a": "int"}

    # newest live_schema becomes the baseline
    ds.record_check(obs, blueprint_id="bp.x", module_id="load",
                    baseline_schema={"a": "int"}, live_schema={"a": "int", "b": "string"},
                    status="drift_benign")
    assert ds.get_baseline(obs, "bp.x", "load") == {"a": "int", "b": "string"}


def test_heal_drift_stages_to_configured_backend(monkeypatch, tmp_path):
    """B1 regression: drift auto-heal must stage the patch to the configured
    `stores.blob` backend (s3/gcs/…), not a hardcoded local dir — otherwise on a
    remote backend the body lands on local FS and `patch list`/`apply` miss it."""
    pytest.importorskip("fsspec")
    import importlib
    from types import SimpleNamespace
    import aqueduct.agent as A
    import aqueduct.drift.context as DC
    D = importlib.import_module("aqueduct.cli.drift")  # the module, not the click Command

    captured = {}

    def _fake_stage(patch, patches_dir, fc, *, patch_store=None, **kw):
        captured["patch_store"] = patch_store

    monkeypatch.setattr(A, "stage_patch_for_human", _fake_stage)
    monkeypatch.setattr(A, "generate_agent_patch",
                        lambda **k: SimpleNamespace(patch=SimpleNamespace(patch_id="p1")))
    monkeypatch.setattr(DC, "build_synthetic_failure_context", lambda *a, **k: object())

    eng = SimpleNamespace(model="m", max_reprompts=1, budget=None, provider="anthropic",
                          base_url=None, api_key=None, provider_options=None, timeout=30,
                          prompt_context=None)
    cfg = SimpleNamespace(
        agent=eng,
        stores=SimpleNamespace(blob=SimpleNamespace(backend="s3", path="s3://bucket/prefix")),
        deployment=SimpleNamespace(engine="spark"),
    )

    pid = D._heal_drift(cfg, "bp", "mod", object(), "{}", tmp_path / "patches")
    assert pid == "p1"
    assert captured["patch_store"] is not None
    assert "s3://bucket/prefix" in captured["patch_store"].location_label  # configured backend, not local
