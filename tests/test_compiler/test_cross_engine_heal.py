"""Tests for the cross-engine heal-patch provenance gate (Phase 79).

`healed_by:` records — see parser/schema.py::HealedByRecordSchema — are
compile-time provenance metadata. An `engine_shaped` record healed on a
different engine than the compile target, not yet validated_on that engine,
fires a suppressible `cross_engine_heal` warning; `warnings.strict` escalates
it to a hard CompileError. dialect_neutral-only records never warn.
"""

from __future__ import annotations

import hashlib
import json
import warnings as _pywarnings
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from aqueduct.compiler.capability_check import (
    RULE_ID_CROSS_ENGINE_HEAL,
    check_cross_engine_heal,
)
from aqueduct.compiler.compiler import CompileError
from aqueduct.compiler.compiler import compile as ccompile
from aqueduct.parser.parser import parse_dict

_BASE_BP: dict = {
    "aqueduct": "1.0",
    "id": "test_bp",
    "name": "test",
    "modules": [
        {"id": "in", "label": "in", "type": "Ingress", "config": {"format": "csv", "path": "/tmp/x.csv"}},
        {"id": "out", "label": "out", "type": "Egress", "config": {"format": "parquet", "path": "/tmp/y", "mode": "overwrite"}},
    ],
    "edges": [{"from": "in", "to": "out"}],
}


def _bp_with_healed_by(records: list[dict]) -> dict:
    bp = json.loads(json.dumps(_BASE_BP))  # cheap deep copy
    bp["healed_by"] = records
    return bp


def _record(engine, classification, validated_on=None, patch_id="p1"):
    return {
        "patch_id": patch_id,
        "engine": engine,
        "classification": classification,
        "applied_at": "2026-07-18T00:00:00Z",
        "validated_on": validated_on or [],
    }


def test_no_healed_by_records_no_problems():
    bp = parse_dict(_BASE_BP, base_dir=Path("/tmp"))
    assert check_cross_engine_heal(bp, "spark") == []


def test_engine_shaped_cross_engine_fires():
    bp = parse_dict(_bp_with_healed_by([_record("duckdb", "engine_shaped")]), base_dir=Path("/tmp"))
    problems = check_cross_engine_heal(bp, "spark")
    assert len(problems) == 1
    assert problems[0].patch_id == "p1"
    assert problems[0].origin_engine == "duckdb"
    assert problems[0].target_engine == "spark"


def test_same_engine_does_not_fire():
    bp = parse_dict(_bp_with_healed_by([_record("spark", "engine_shaped")]), base_dir=Path("/tmp"))
    assert check_cross_engine_heal(bp, "spark") == []


def test_dialect_neutral_never_fires():
    bp = parse_dict(_bp_with_healed_by([_record("duckdb", "dialect_neutral")]), base_dir=Path("/tmp"))
    assert check_cross_engine_heal(bp, "spark") == []


def test_validated_on_clears_the_warning():
    bp = parse_dict(
        _bp_with_healed_by([_record("duckdb", "engine_shaped", validated_on=["spark"])]),
        base_dir=Path("/tmp"),
    )
    assert check_cross_engine_heal(bp, "spark") == []


def test_compile_emits_suppressible_warning():
    bp = parse_dict(_bp_with_healed_by([_record("duckdb", "engine_shaped")]), base_dir=Path("/tmp"))
    with _pywarnings.catch_warnings(record=True) as caught:
        _pywarnings.simplefilter("always")
        ccompile(bp, engine="spark")
    messages = [str(w.message) for w in caught]
    assert any(f"[aqueduct:{RULE_ID_CROSS_ENGINE_HEAL}]" in m for m in messages)


def test_compile_warning_suppressed():
    bp = parse_dict(_bp_with_healed_by([_record("duckdb", "engine_shaped")]), base_dir=Path("/tmp"))
    with _pywarnings.catch_warnings(record=True) as caught:
        _pywarnings.simplefilter("always")
        ccompile(bp, engine="spark", warnings_suppress={RULE_ID_CROSS_ENGINE_HEAL})
    messages = [str(w.message) for w in caught]
    assert not any(RULE_ID_CROSS_ENGINE_HEAL in m for m in messages)


def test_compile_strict_escalates_to_compile_error():
    bp = parse_dict(_bp_with_healed_by([_record("duckdb", "engine_shaped")]), base_dir=Path("/tmp"))
    with pytest.raises(CompileError, match="Cross-engine heal-patch gate failed"):
        ccompile(bp, engine="spark", warnings_strict={RULE_ID_CROSS_ENGINE_HEAL})


def test_compile_strict_does_not_escalate_dialect_neutral():
    bp = parse_dict(_bp_with_healed_by([_record("duckdb", "dialect_neutral")]), base_dir=Path("/tmp"))
    # Should compile cleanly — no engine_shaped problem to escalate.
    manifest = ccompile(bp, engine="spark", warnings_strict={RULE_ID_CROSS_ENGINE_HEAL})
    assert manifest.blueprint_id == "test_bp"


def test_manifest_hash_invariant_with_and_without_healed_by():
    """The `healed_by:` block is compile-time-gate metadata, excluded from
    Manifest assembly entirely — proves it never perturbs the compiled
    Manifest's semantic content/hash (checkpoint `_manifest_hash` safety)."""
    bp_without = parse_dict(_BASE_BP, base_dir=Path("/tmp"))
    bp_with = parse_dict(
        _bp_with_healed_by([_record("duckdb", "engine_shaped", validated_on=["spark"])]),
        base_dir=Path("/tmp"),
    )
    m_without = ccompile(bp_without, engine="spark")
    m_with = ccompile(bp_with, engine="spark")

    h_without = hashlib.sha256(json.dumps(m_without.to_dict(), sort_keys=True).encode()).hexdigest()
    h_with = hashlib.sha256(json.dumps(m_with.to_dict(), sort_keys=True).encode()).hexdigest()
    assert h_without == h_with
    # Also confirms Manifest itself carries no healed_by attribute.
    assert not hasattr(m_without, "healed_by")
