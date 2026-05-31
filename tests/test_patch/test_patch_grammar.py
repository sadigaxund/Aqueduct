"""Unit tests for PatchSpec Pydantic grammar."""

from __future__ import annotations

import json
import pytest
pytestmark = pytest.mark.unit
from pydantic import ValidationError
from aqueduct.patch.grammar import PatchSpec


def test_valid_patch_spec_parsing():
    raw_json = {
        "patch_id": "patch_123",
        "rationale": "Fixing SQL query",
        "operations": [
            {
                "op": "replace_module_config",
                "module_id": "m1",
                "config": {"query": "SELECT 1"}
            }
        ]
    }
    spec = PatchSpec.model_validate(raw_json)
    assert spec.patch_id == "patch_123"
    assert len(spec.operations) == 1
    assert spec.operations[0].op == "replace_module_config"


def test_patch_spec_empty_operations():
    # min_length=1 should prevent empty operations list
    raw_json = {
        "patch_id": "patch_123",
        "rationale": "Empty patch",
        "operations": []
    }
    with pytest.raises(ValidationError, match="at least 1 item"):
        PatchSpec.model_validate(raw_json)


def test_patch_spec_unknown_top_level_keys_land_in_misc():
    # PatchSpec now allows extra top-level keys (extra="allow") and buckets
    # them into ``misc`` via a pre-validator. Operation-level fields stay
    # strict — a typo in ``key:`` or ``module_id:`` still bounces.
    raw_json = {
        "patch_id": "patch_123",
        "rationale": "Unknown top-level keys collected",
        "confidence": 0.9,
        "root_cause": "test",
        "hacker_field": "exploit",
        "rootCause": "ignored alias bucket",
        "operations": [
            {"op": "replace_module_label", "module_id": "m1", "label": "L"}
        ],
    }
    spec = PatchSpec.model_validate(raw_json)
    # The unknown top-level key lands in misc; canonical fields stay clean.
    assert "hacker_field" in spec.misc
    assert spec.misc["hacker_field"] == "exploit"


def test_patch_operation_discriminator_mismatch():
    # discriminator="op" is used
    raw_json = {
        "patch_id": "p1",
        "rationale": "Wrong op",
        "operations": [
            {"op": "invalid_op_name", "module_id": "m1"}
        ]
    }
    # Adjusting for Pydantic v2 error message format
    with pytest.raises(ValidationError, match="invalid_op_name"):
        PatchSpec.model_validate(raw_json)


def test_patch_operation_missing_required_field():
    raw_json = {
        "patch_id": "p1",
        "rationale": "Missing field",
        "operations": [
            {"op": "replace_module_config", "module_id": "m1"}  # config is missing
        ]
    }
    with pytest.raises(ValidationError, match="Field required"):
        PatchSpec.model_validate(raw_json)


def test_patch_operation_extra_field_forbidden():
    # extra="forbid" on operation level
    raw_json = {
        "patch_id": "p1",
        "rationale": "Extra op field",
        "operations": [
            {
                "op": "replace_module_label",
                "module_id": "m1",
                "label": "L",
                "unknown_key": "val"
            }
        ]
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PatchSpec.model_validate(raw_json)


def test_patch_spec_json_schema():
    schema = PatchSpec.model_json_schema()
    assert schema["title"] == "PatchSpec"
    assert "operations" in schema["properties"]
    # Check that ops are a list of discriminated items
    ops_schema = schema["$defs"]
    assert "ReplaceModuleConfigOp" in ops_schema
    assert "ReplaceModuleLabelOp" in ops_schema


# ── PatchSpec resilience (1.1.0) ──────────────────────────────────────────────

class TestPatchSpecResilience:
    """LLM responses often omit or add fields. The normalizer in
    PatchSpec._normalize_op_aliases handles these gracefully."""

    def test_missing_patch_id_synthesised_from_rationale(self):
        """Missing patch_id → normalizer creates auto-<slug> from rationale."""
        raw = {
            "rationale": "Fixing column reference in SQL query",
            "operations": [
                {"op": "replace_module_config", "module_id": "m1",
                 "config": {"query": "SELECT id FROM t"}}
            ],
        }
        spec = PatchSpec.model_validate(raw)
        assert spec.patch_id.startswith("auto-")
        assert "fixing" in spec.patch_id
        assert spec.rationale == "Fixing column reference in SQL query"

    def test_missing_patch_id_and_rationale_falls_back_to_uuid(self):
        """Missing both patch_id and rationale → normalizer generates patch_id
        but the overall model still fails because rationale is required.

        The generated patch_id is visible in the error's input_value — confirms
        the pre-validator ran before pydantic field validation rejected it.
        """
        raw = {
            "operations": [
                {"op": "replace_module_label", "module_id": "m1", "label": "L"}
            ],
        }
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="rationale") as excinfo:
            PatchSpec.model_validate(raw)
        # The normalizer still ran: error input_value shows synthesized patch_id
        error_input = str(excinfo.value)
        assert "auto-" in error_input

    def test_empty_rationale_gets_uuid_patch_id(self):
        """Rationale present but empty → no slug possible; uuid fallback."""
        raw = {
            "rationale": "",
            "operations": [
                {"op": "replace_module_label", "module_id": "m1", "label": "L"}
            ],
        }
        spec = PatchSpec.model_validate(raw)
        assert spec.patch_id.startswith("auto-")
        assert len(spec.patch_id) == 17

    def test_hallucinated_meta_fields_silently_stripped(self):
        """Extra fields like id, name, applied_by, datetime_applied removed."""
        raw = {
            "patch_id": "p1",
            "rationale": "Fix",
            "id": "some-uuid",
            "name": "My Patch",
            "applied_by": "claude",
            "datetime_applied": "2026-05-30T12:00:00Z",
            "operations": [
                {"op": "replace_module_config", "module_id": "m1",
                 "config": {"query": "SELECT 1"}}
            ],
        }
        spec = PatchSpec.model_validate(raw)
        # The hallucinated fields should not appear in misc or anywhere
        assert spec.misc == {}
        assert spec.patch_id == "p1"

    def test_hallucinated_timestamp_author_version_stripped(self):
        """timestamp, author, version, created_at, updated_at also stripped."""
        raw = {
            "rationale": "Fix",
            "timestamp": "2026-01-01T00:00:00Z",
            "author": "llm",
            "version": "1.0",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "operations": [
                {"op": "replace_module_label", "module_id": "m1", "label": "L"}
            ],
        }
        spec = PatchSpec.model_validate(raw)
        assert spec.misc == {}
        # patch_id should be auto-generated since we didn't provide one
        assert "auto-" in spec.patch_id

    def test_existing_patch_id_preserved(self):
        """When patch_id is already provided, normalizer is a no-op."""
        raw = {
            "patch_id": "my-custom-patch-001",
            "rationale": "Manual fix",
            "operations": [
                {"op": "replace_module_label", "module_id": "m1", "label": "L"}
            ],
        }
        spec = PatchSpec.model_validate(raw)
        assert spec.patch_id == "my-custom-patch-001"

    def test_rationale_with_special_chars_produces_clean_slug(self):
        """Special characters in rationale are sanitised."""
        raw = {
            "rationale": "Fix $PECIAL ch@rs & numbers 123!",
            "operations": [
                {"op": "replace_module_label", "module_id": "m1", "label": "L"}
            ],
        }
        spec = PatchSpec.model_validate(raw)
        assert spec.patch_id.startswith("auto-")
        # Only lowercase alphanumeric + hyphens
        slug = spec.patch_id[len("auto-"):]
        import re
        assert re.fullmatch(r"[a-z0-9-]+", slug), f"slug contains bad chars: {slug}"

    def test_rationale_long_text_truncated_at_48_chars(self):
        """Long rationale slug is truncated to 48 chars max."""
        raw = {
            "rationale": "This is a very long rationale that should be truncated to forty eight characters or less for the patch id slug which helps keep filenames reasonable",
            "operations": [
                {"op": "replace_module_label", "module_id": "m1", "label": "L"}
            ],
        }
        spec = PatchSpec.model_validate(raw)
        slug = spec.patch_id[len("auto-"):]
        assert len(slug) <= 48
        # Ensure no trailing hyphen
        assert not slug.endswith("-")
