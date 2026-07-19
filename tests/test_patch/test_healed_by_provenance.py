"""Tests for the `healed_by:` blueprint provenance block (Phase 79):
apply-time write and the self-clearing validated_on stamp."""

from __future__ import annotations

import json

import pytest
import yaml

pytestmark = pytest.mark.unit

from aqueduct.patch.apply import apply_patch_file, stamp_validated_engine


def _write_bp(path, healed_by=None):
    bp = {
        "aqueduct": "1.0",
        "id": "test.bp",
        "name": "Test Blueprint",
        "modules": [
            {"id": "in", "label": "in", "type": "Ingress", "config": {"format": "parquet", "path": "p1"}},
        ],
        "edges": [],
    }
    if healed_by is not None:
        bp["healed_by"] = healed_by
    path.write_text(yaml.dump(bp), encoding="utf-8")
    return path


def _write_patch(path, *, patch_id="p1", meta=None, key="path", value="p2"):
    data = {
        "patch_id": patch_id,
        "rationale": "fix",
        "operations": [
            {"op": "set_module_config_key", "module_id": "in", "key": key, "value": value},
        ],
    }
    if meta is not None:
        data["_aq_meta"] = meta
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_apply_writes_healed_by_block(tmp_path):
    bp_path = _write_bp(tmp_path / "bp.yml")
    patch_path = _write_patch(
        tmp_path / "patch.json",
        meta={"engine": "duckdb", "engine_version": "1.5.4", "run_id": "r1"},
    )
    apply_patch_file(
        blueprint_path=bp_path, patch_path=patch_path,
        patches_dir=tmp_path / "patches",
    )
    written = yaml.safe_load(bp_path.read_text())
    assert "healed_by" in written
    assert len(written["healed_by"]) == 1
    rec = written["healed_by"][0]
    assert rec["patch_id"] == "p1"
    assert rec["engine"] == "duckdb"
    assert rec["engine_version"] == "1.5.4"
    assert rec["run_id"] == "r1"
    assert rec["classification"] == "dialect_neutral"  # key="path"
    assert rec["validated_on"] == []
    assert "applied_at" in rec


def test_apply_appends_to_existing_healed_by(tmp_path):
    existing = [{
        "patch_id": "p0", "engine": "spark", "classification": "dialect_neutral",
        "applied_at": "2026-01-01T00:00:00Z", "validated_on": ["spark"],
    }]
    bp_path = _write_bp(tmp_path / "bp.yml", healed_by=existing)
    patch_path = _write_patch(
        tmp_path / "patch.json",
        patch_id="p1", meta={"engine": "duckdb"},
    )
    apply_patch_file(
        blueprint_path=bp_path, patch_path=patch_path,
        patches_dir=tmp_path / "patches",
    )
    written = yaml.safe_load(bp_path.read_text())
    assert len(written["healed_by"]) == 2
    assert written["healed_by"][0]["patch_id"] == "p0"
    assert written["healed_by"][1]["patch_id"] == "p1"


def test_apply_no_healed_by_record_without_engine_meta(tmp_path):
    """A hand-authored patch with no _aq_meta.engine does not get stamped."""
    bp_path = _write_bp(tmp_path / "bp.yml")
    patch_path = _write_patch(tmp_path / "patch.json", meta=None)
    apply_patch_file(
        blueprint_path=bp_path, patch_path=patch_path,
        patches_dir=tmp_path / "patches",
    )
    written = yaml.safe_load(bp_path.read_text())
    assert not written.get("healed_by")


def test_apply_engine_shaped_classification_from_ops(tmp_path):
    bp_path = _write_bp(tmp_path / "bp.yml")
    patch_path = _write_patch(
        tmp_path / "patch.json",
        meta={"engine": "duckdb"}, key="format", value="csv",
    )
    apply_patch_file(
        blueprint_path=bp_path, patch_path=patch_path,
        patches_dir=tmp_path / "patches",
    )
    written = yaml.safe_load(bp_path.read_text())
    assert written["healed_by"][0]["classification"] == "engine_shaped"


# ── stamp_validated_engine ──────────────────────────────────────────────────

def test_stamp_validated_engine_noop_without_healed_by(tmp_path):
    bp_path = _write_bp(tmp_path / "bp.yml")
    changed = stamp_validated_engine(bp_path, "duckdb")
    assert changed is False
    # untouched — no healed_by key added
    assert "healed_by" not in yaml.safe_load(bp_path.read_text())


def test_stamp_validated_engine_appends_and_is_idempotent(tmp_path):
    existing = [{
        "patch_id": "p1", "engine": "duckdb", "classification": "engine_shaped",
        "applied_at": "2026-01-01T00:00:00Z", "validated_on": [],
    }]
    bp_path = _write_bp(tmp_path / "bp.yml", healed_by=existing)

    changed = stamp_validated_engine(bp_path, "spark")
    assert changed is True
    written = yaml.safe_load(bp_path.read_text())
    assert written["healed_by"][0]["validated_on"] == ["spark"]

    # Second stamp with the same engine is a no-op.
    changed2 = stamp_validated_engine(bp_path, "spark")
    assert changed2 is False
    written2 = yaml.safe_load(bp_path.read_text())
    assert written2["healed_by"][0]["validated_on"] == ["spark"]


def test_stamp_validated_engine_multiple_records(tmp_path):
    existing = [
        {"patch_id": "p1", "engine": "duckdb", "classification": "engine_shaped",
         "applied_at": "2026-01-01T00:00:00Z", "validated_on": []},
        {"patch_id": "p2", "engine": "spark", "classification": "dialect_neutral",
         "applied_at": "2026-01-01T00:00:00Z", "validated_on": ["spark"]},
    ]
    bp_path = _write_bp(tmp_path / "bp.yml", healed_by=existing)
    changed = stamp_validated_engine(bp_path, "spark")
    assert changed is True
    written = yaml.safe_load(bp_path.read_text())
    assert written["healed_by"][0]["validated_on"] == ["spark"]
    # p2 already had spark — untouched but re-serialized identically.
    assert written["healed_by"][1]["validated_on"] == ["spark"]


def test_stamp_validated_engine_never_raises_on_missing_file(tmp_path):
    missing = tmp_path / "does_not_exist.yml"
    assert stamp_validated_engine(missing, "spark") is False
