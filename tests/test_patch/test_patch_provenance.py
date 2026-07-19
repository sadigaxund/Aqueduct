"""Tests for aqueduct.patch.provenance — PatchSpec op classification (Phase 79)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from aqueduct.patch.grammar import VALID_PATCH_OPS
from aqueduct.patch.provenance import (
    DIALECT_NEUTRAL,
    ENGINE_SHAPED,
    _FIELD_SENSITIVE_OPS,
    _STATIC_OP_CLASSIFICATION,
    build_healed_by_record,
    classify_op,
    classify_ops,
    detect_engine_version,
)


class _FakeOp:
    def __init__(self, op: str, **fields):
        self.op = op
        for k, v in fields.items():
            setattr(self, k, v)


def test_every_grammar_op_is_classified_closure():
    """Closure guard: every VALID_PATCH_OPS entry must be covered by the
    static table or the field-sensitive set. A 15th op added to the grammar
    without updating this module fails this test."""
    covered = set(_STATIC_OP_CLASSIFICATION) | set(_FIELD_SENSITIVE_OPS)
    assert covered == set(VALID_PATCH_OPS)


@pytest.mark.parametrize("op_name,classification", [
    ("replace_module_config", ENGINE_SHAPED),
    ("replace_module_label", DIALECT_NEUTRAL),
    ("insert_module", ENGINE_SHAPED),
    ("remove_module", DIALECT_NEUTRAL),
    ("replace_context_value", DIALECT_NEUTRAL),
    ("add_probe", DIALECT_NEUTRAL),
    ("replace_edge", DIALECT_NEUTRAL),
    ("set_module_on_failure", DIALECT_NEUTRAL),
    ("replace_retry_policy", DIALECT_NEUTRAL),
    ("add_arcade_ref", DIALECT_NEUTRAL),
    ("defer_to_human", DIALECT_NEUTRAL),
    ("set_spark_config", ENGINE_SHAPED),
    ("replace_macro", ENGINE_SHAPED),
])
def test_static_op_classification(op_name, classification):
    assert classify_op(_FakeOp(op_name)) == classification


@pytest.mark.parametrize("key,expected", [
    ("path", DIALECT_NEUTRAL),
    ("output_path", DIALECT_NEUTRAL),
    ("max_attempts", DIALECT_NEUTRAL),
    ("query", ENGINE_SHAPED),
    ("sql", ENGINE_SHAPED),
    ("format", ENGINE_SHAPED),
    ("mode", ENGINE_SHAPED),
    ("options.mergeSchema", ENGINE_SHAPED),
    (None, ENGINE_SHAPED),  # missing key — conservative
])
def test_set_module_config_key_field_sensitive(key, expected):
    assert classify_op(_FakeOp("set_module_config_key", key=key)) == expected


def test_classify_ops_is_max_over_ops():
    ops = [_FakeOp("replace_module_label"), _FakeOp("set_spark_config")]
    assert classify_ops(ops) == ENGINE_SHAPED

    ops2 = [_FakeOp("replace_module_label"), _FakeOp("remove_module")]
    assert classify_ops(ops2) == DIALECT_NEUTRAL

    assert classify_ops([]) == DIALECT_NEUTRAL


def test_classify_op_dict_form():
    """classify_op also accepts a plain dict (op discriminator + fields)."""
    assert classify_op({"op": "replace_macro"}) == ENGINE_SHAPED
    assert classify_op({"op": "set_module_config_key", "key": "path"}) == DIALECT_NEUTRAL


def test_detect_engine_version_unknown_engine_returns_none():
    assert detect_engine_version("not-a-real-engine") is None


def test_detect_engine_version_duckdb_installed():
    # duckdb is a base dependency (see AGENTS.md) — must resolve on any dev env.
    version = detect_engine_version("duckdb")
    assert version is not None
    assert isinstance(version, str)


def test_build_healed_by_record_none_without_engine_meta():
    """A hand-authored patch with no _aq_meta.engine gets no healed_by record."""
    rec = build_healed_by_record(
        patch_id="p1", operations=[_FakeOp("replace_module_label")],
        meta={}, applied_at="2026-01-01T00:00:00Z",
    )
    assert rec is None

    rec_none_meta = build_healed_by_record(
        patch_id="p1", operations=[_FakeOp("replace_module_label")],
        meta=None, applied_at="2026-01-01T00:00:00Z",
    )
    assert rec_none_meta is None


def test_build_healed_by_record_with_engine_meta():
    rec = build_healed_by_record(
        patch_id="p1",
        operations=[_FakeOp("set_spark_config")],
        meta={"engine": "duckdb", "engine_version": "1.5.4", "run_id": "r1"},
        applied_at="2026-01-01T00:00:00Z",
    )
    assert rec == {
        "patch_id": "p1",
        "engine": "duckdb",
        "engine_version": "1.5.4",
        "run_id": "r1",
        "classification": ENGINE_SHAPED,
        "applied_at": "2026-01-01T00:00:00Z",
        "validated_on": [],
    }


def test_build_healed_by_record_fallback_run_id():
    rec = build_healed_by_record(
        patch_id="p1",
        operations=[_FakeOp("remove_module")],
        meta={"engine": "duckdb"},
        applied_at="2026-01-01T00:00:00Z",
        fallback_run_id="fallback-run",
    )
    assert rec["run_id"] == "fallback-run"
    assert rec["classification"] == DIALECT_NEUTRAL
