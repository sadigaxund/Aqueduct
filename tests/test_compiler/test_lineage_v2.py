"""Phase 56 (Lineage v2) — Channel SQL fingerprints + type-tracked chains.

Pure-compiler unit tests (no Spark, no store). CLI-level rendering and the
store changelog round-trip are covered by stubs in tests/test_backlog.py.
"""
from __future__ import annotations

import hashlib

import pytest

from aqueduct.compiler.chain import compute_type_chain
from aqueduct.compiler.fingerprint import _canonicalize, compute_channel_fingerprints
from aqueduct.parser.models import Edge, Module, ModuleType

pytestmark = pytest.mark.unit


def _ch(mid: str, query: str) -> Module:
    return Module(id=mid, type=ModuleType.Channel, label=mid, config={"op": "sql", "query": query})


def _ingress(mid: str) -> Module:
    return Module(id=mid, type=ModuleType.Ingress, label=mid, config={})


def _h(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# ── fingerprints ────────────────────────────────────────────────────────────

def test_fingerprint_ignores_formatting_and_comments():
    a = _canonicalize("SELECT  a, b  FROM t WHERE x = 1  -- note")
    b = _canonicalize("select a,b from t where x=1")
    assert _h(a) == _h(b)


def test_fingerprint_changes_on_semantic_edit():
    a = _canonicalize("select a from t where x = 1")
    c = _canonicalize("select a from t where x = 2")
    assert _h(a) != _h(c)


def test_compute_fingerprints_only_sql_channels():
    mods = (
        _ingress("load"),
        _ch("clean", "SELECT a FROM load"),
        Module(id="dedup", type=ModuleType.Channel, label="d", config={"op": "deduplicate", "key": "a"}),
    )
    rows = compute_channel_fingerprints(mods)
    assert {r["channel_id"] for r in rows} == {"clean"}
    assert len(rows[0]["fingerprint"]) == 64  # sha256 hex


def test_fingerprint_unparseable_sql_still_hashes():
    # Garbage SQL falls back to whitespace-normalised raw — still produces a hash.
    rows = compute_channel_fingerprints((_ch("x", ">>> not sql <<<"),))
    assert len(rows) == 1 and len(rows[0]["fingerprint"]) == 64


# ── type-tracked chains ─────────────────────────────────────────────────────

def test_chain_tracks_cast_type_and_passthrough():
    mods = (
        _ingress("load"),
        _ch("clean", "SELECT CAST(amount AS DOUBLE) AS amount, id FROM load"),
        _ch("final", "SELECT amount, id FROM clean"),
    )
    edges = (Edge("load", "clean"), Edge("clean", "final"))
    hops = compute_type_chain(mods, edges, "amount")

    # source → output order
    assert [h.channel_id for h in hops] == ["clean", "final"]
    clean, final = hops
    assert clean.transform_op == "CAST" and clean.output_type == "DOUBLE"
    assert clean.source_table == "load" and clean.source_column == "amount"
    assert final.transform_op == "passthrough"


def test_chain_rename_classified():
    mods = (_ingress("load"), _ch("c", "SELECT total AS amount FROM load"))
    hops = compute_type_chain(mods, edges=(Edge("load", "c"),), column="amount")
    assert len(hops) == 1
    assert hops[0].transform_op == "rename"
    assert hops[0].source_column == "total"


def test_chain_missing_column_returns_empty():
    mods = (_ingress("load"), _ch("c", "SELECT a FROM load"))
    assert compute_type_chain(mods, (Edge("load", "c"),), "nonexistent") == []
