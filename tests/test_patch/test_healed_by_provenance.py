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
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "config": {"format": "parquet", "path": "p1"},
            },
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
    from aqueduct.patch import index as _ix
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
    from aqueduct.surveyor.ddl import _DDL

    obs_store = DuckDBObservabilityStore(tmp_path / "obs.db")
    with obs_store.connect() as cur:
        cur.execute(_DDL)

    bp_path = _write_bp(tmp_path / "bp.yml")
    patch_path = _write_patch(
        tmp_path / "patch.json",
        meta={"engine": "duckdb", "engine_version": "1.5.4", "run_id": "r1"},
    )
    apply_patch_file(
        blueprint_path=bp_path,
        patch_path=patch_path,
        patches_dir=tmp_path / "patches",
        obs_store=obs_store,
    )
    written = yaml.safe_load(bp_path.read_text())
    assert "healed_by" in written
    assert len(written["healed_by"]) == 1
    rec = written["healed_by"][0]
    assert rec["patch_id"] == "p1"
    assert rec["engine"] == "duckdb"
    assert rec["classification"] == "dialect_neutral"  # key="path"
    assert rec["validated_on"] == []
    assert "applied_at" in rec
    # engine_version/run_id moved out of the Blueprint record and into the
    # patch index — see aqueduct/parser/schema.py::MOVED_HEALED_BY_FIELDS.
    assert "engine_version" not in rec
    assert "run_id" not in rec

    with obs_store.connect() as cur:
        facts = _ix.heal_provenance(cur, "p1")
    assert facts["engine_version"] == "1.5.4"
    assert facts["run_id"] == "r1"


def test_apply_appends_to_existing_healed_by(tmp_path):
    existing = [
        {
            "patch_id": "p0",
            "engine": "spark",
            "classification": "dialect_neutral",
            "applied_at": "2026-01-01T00:00:00Z",
            "validated_on": ["spark"],
        }
    ]
    bp_path = _write_bp(tmp_path / "bp.yml", healed_by=existing)
    patch_path = _write_patch(
        tmp_path / "patch.json",
        patch_id="p1",
        meta={"engine": "duckdb"},
    )
    apply_patch_file(
        blueprint_path=bp_path,
        patch_path=patch_path,
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
        blueprint_path=bp_path,
        patch_path=patch_path,
        patches_dir=tmp_path / "patches",
    )
    written = yaml.safe_load(bp_path.read_text())
    assert not written.get("healed_by")


def test_apply_engine_shaped_classification_from_ops(tmp_path):
    bp_path = _write_bp(tmp_path / "bp.yml")
    patch_path = _write_patch(
        tmp_path / "patch.json",
        meta={"engine": "duckdb"},
        key="format",
        value="csv",
    )
    apply_patch_file(
        blueprint_path=bp_path,
        patch_path=patch_path,
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
    existing = [
        {
            "patch_id": "p1",
            "engine": "duckdb",
            "classification": "engine_shaped",
            "applied_at": "2026-01-01T00:00:00Z",
            "validated_on": [],
        }
    ]
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
        {
            "patch_id": "p1",
            "engine": "duckdb",
            "classification": "engine_shaped",
            "applied_at": "2026-01-01T00:00:00Z",
            "validated_on": [],
        },
        {
            "patch_id": "p2",
            "engine": "spark",
            "classification": "dialect_neutral",
            "applied_at": "2026-01-01T00:00:00Z",
            "validated_on": ["spark"],
        },
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


# ── old-shape (pre-2.x) healed_by records are rejected by name ─────────────
#
# The five fields `HealedByRecordSchema` used to carry now live in the
# `patch_index` table (aqueduct/patch/index.py) instead — see
# aqueduct/parser/schema.py::MOVED_HEALED_BY_FIELDS. A Blueprint still
# carrying one of them fails parsing with the field named, not a generic
# "extra inputs are not permitted".


def _old_shape_record(**extra):
    return {
        "patch_id": "p1",
        "engine": "spark",
        "classification": "dialect_neutral",
        "applied_at": "2026-01-01T00:00:00+00:00",
        "validated_on": [],
        **extra,
    }


def test_engine_config_delta_in_healed_by_is_rejected_by_name(tmp_path):
    from aqueduct.parser.parser import parse

    bp_path = _write_bp(
        tmp_path / "bp.yml",
        healed_by=[
            _old_shape_record(
                engine_config_delta={
                    "spark": {"spark.sql.shuffle.partitions": {"before": "200", "after": "800"}}
                }
            )
        ],
    )
    with pytest.raises(Exception) as exc:
        parse(str(bp_path))
    msg = str(exc.value)
    assert "engine_config_delta" in msg
    assert "patch index" in msg


def test_perf_observations_in_healed_by_is_rejected_by_name(tmp_path):
    from aqueduct.parser.parser import parse

    bp_path = _write_bp(
        tmp_path / "bp.yml",
        healed_by=[
            _old_shape_record(
                perf_observations=[{"engine": "spark", "status": "observed"}],
            )
        ],
    )
    with pytest.raises(Exception) as exc:
        parse(str(bp_path))
    msg = str(exc.value)
    assert "perf_observations" in msg
    assert "patch index" in msg


def test_parse_dict_also_rejects_the_old_shape(tmp_path):
    """Same rejection on the in-memory entrypoint patch flows use, not just
    the file-based `parse`."""
    from aqueduct.parser.parser import parse_dict

    raw = {
        "aqueduct": "1.0",
        "id": "test.bp",
        "name": "Test Blueprint",
        "modules": [],
        "edges": [],
        "healed_by": [_old_shape_record(engine_version="1.5.4")],
    }
    with pytest.raises(Exception) as exc:
        parse_dict(raw, base_dir=tmp_path)
    msg = str(exc.value)
    assert "engine_version" in msg
    assert "patch index" in msg
