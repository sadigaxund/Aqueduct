"""Tests for agent.approval rename (aliased) — Phase 51.

Source: aqueduct/parser/schema.py  (AgentSchema.approval_mode with AliasChoices)
        aqueduct/parser/parser.py  (deprecation-warning block)

Behavior:
  - `agent: {approval: auto}`         → approval_mode == 'auto',  NO stderr warning
  - `agent: {approval_mode: human}`   → approval_mode == 'human', '[deprecated] agent.approval_mode' on stderr
  - Both keys present                 → canonical `approval` wins, warning still emitted, no ValidationError
  - `approval: aggressive`            → still parses + existing "aggressive" deprecation warning
  - absent                            → default 'disabled', no warning

TEST_MANIFEST section: ### `agent.approval` rename (aliased) — `aqueduct/parser/schema.py` + `parser.py`
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Any

import pytest

from aqueduct.parser.parser import parse_dict

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SINGLE_MODULE = [
    {
        "id": "src",
        "type": "Ingress",
        "label": "Source",
        "config": {
            "format": "parquet",
            "path": "s3://bucket/in.parquet",
        },
    }
]

BASE_DIR = Path(".")


def _make_raw(agent: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a minimal valid blueprint dict with an optional `agent` block."""
    raw: dict[str, Any] = {
        "aqueduct": "1.0",
        "id": "test_approval",
        "name": "Approval Rename Test",
        "modules": _SINGLE_MODULE,
    }
    if agent is not None:
        raw["agent"] = agent
    return raw


def _parse_capture_stderr(raw: dict[str, Any]) -> tuple[Any, str]:
    """Run parse_dict and return (blueprint, stderr_text)."""
    buf = io.StringIO()
    old_stderr = sys.stderr
    try:
        sys.stderr = buf
        bp = parse_dict(raw, base_dir=BASE_DIR)
    finally:
        sys.stderr = old_stderr
    return bp, buf.getvalue()


# ---------------------------------------------------------------------------
# Canonical `approval` key
# ---------------------------------------------------------------------------


class TestApprovalCanonicalKey:
    def test_approval_auto_parses(self):
        """agent: {approval: auto} → approval_mode == 'auto'."""
        raw = _make_raw(agent={"approval": "auto"})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "auto"

    def test_approval_canonical_no_stderr_warning(self):
        """Canonical `approval` key → NO '[deprecated] agent.approval_mode' on stderr."""
        raw = _make_raw(agent={"approval": "auto"})
        _, stderr = _parse_capture_stderr(raw)
        assert "[deprecated] agent.approval_mode" not in stderr

    def test_approval_human_parses(self):
        """agent: {approval: human} → approval_mode == 'human', no warning."""
        raw = _make_raw(agent={"approval": "human"})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "human"
        assert "[deprecated] agent.approval_mode" not in stderr

    def test_approval_disabled_parses(self):
        """agent: {approval: disabled} → approval_mode == 'disabled', no warning."""
        raw = _make_raw(agent={"approval": "disabled"})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "disabled"
        assert "[deprecated] agent.approval_mode" not in stderr

    def test_approval_ci_parses(self):
        """agent: {approval: ci} → approval_mode == 'ci', no warning."""
        raw = _make_raw(agent={"approval": "ci"})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "ci"
        assert "[deprecated] agent.approval_mode" not in stderr


# ---------------------------------------------------------------------------
# Deprecated `approval_mode` key
# ---------------------------------------------------------------------------


class TestApprovalModeDeprecatedKey:
    def test_approval_mode_human_parses(self):
        """agent: {approval_mode: human} → approval_mode == 'human'."""
        raw = _make_raw(agent={"approval_mode": "human"})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "human"

    def test_approval_mode_emits_deprecation_warning(self):
        """agent: {approval_mode: human} → '[deprecated] agent.approval_mode' line on stderr."""
        raw = _make_raw(agent={"approval_mode": "human"})
        _, stderr = _parse_capture_stderr(raw)
        assert "[deprecated] agent.approval_mode" in stderr

    def test_approval_mode_warning_mentions_use_approval(self):
        """Deprecation warning body mentions 'use agent.approval'."""
        raw = _make_raw(agent={"approval_mode": "human"})
        _, stderr = _parse_capture_stderr(raw)
        assert "use agent.approval" in stderr

    def test_approval_mode_auto_parses(self):
        """agent: {approval_mode: auto} → approval_mode == 'auto' with warning."""
        raw = _make_raw(agent={"approval_mode": "auto"})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "auto"
        assert "[deprecated] agent.approval_mode" in stderr


# ---------------------------------------------------------------------------
# Both keys present
# ---------------------------------------------------------------------------


class TestBothKeysPresent:
    def test_canonical_approval_wins(self):
        """Both keys: canonical `approval` value wins over `approval_mode`."""
        raw = _make_raw(agent={"approval": "auto", "approval_mode": "human"})
        bp, _ = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "auto"

    def test_both_keys_no_validation_error(self):
        """Both keys present → no extra='forbid' ValidationError (alias dropped before validate)."""
        from aqueduct.parser.parser import ParseError
        raw = _make_raw(agent={"approval": "auto", "approval_mode": "human"})
        # Should NOT raise
        bp, _ = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "auto"

    def test_both_keys_warning_still_emitted(self):
        """Even when canonical key wins, the deprecation warning is still emitted."""
        raw = _make_raw(agent={"approval": "auto", "approval_mode": "human"})
        _, stderr = _parse_capture_stderr(raw)
        assert "[deprecated] agent.approval_mode" in stderr


# ---------------------------------------------------------------------------
# `aggressive` value deprecation (still parses)
# ---------------------------------------------------------------------------


class TestAggressiveValueDeprecation:
    def test_approval_aggressive_still_parses(self):
        """approval: aggressive → still parses to 'aggressive'."""
        raw = _make_raw(agent={"approval": "aggressive"})
        bp, _ = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "aggressive"

    def test_approval_aggressive_emits_aggressive_warning(self):
        """approval: aggressive → existing 'aggressive → use approval: auto' warning."""
        raw = _make_raw(agent={"approval": "aggressive"})
        _, stderr = _parse_capture_stderr(raw)
        assert "aggressive" in stderr

    def test_approval_mode_aggressive_via_deprecated_key(self):
        """approval_mode: aggressive → parses + both kinds of deprecation warning."""
        raw = _make_raw(agent={"approval_mode": "aggressive"})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "aggressive"
        # The approval_mode → use agent.approval warning
        assert "[deprecated] agent.approval_mode" in stderr
        # The aggressive value warning
        assert "aggressive" in stderr


# ---------------------------------------------------------------------------
# Absent agent block (default)
# ---------------------------------------------------------------------------


class TestApprovalAbsent:
    def test_absent_agent_defaults_disabled(self):
        """No `agent:` block at all → approval_mode == 'disabled'."""
        raw = _make_raw(agent=None)
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "disabled"

    def test_absent_agent_no_deprecation_warning(self):
        """No `agent:` block → no '[deprecated] agent.approval_mode' on stderr."""
        raw = _make_raw(agent=None)
        _, stderr = _parse_capture_stderr(raw)
        assert "[deprecated] agent.approval_mode" not in stderr

    def test_empty_agent_block_defaults_disabled(self):
        """Empty `agent: {}` → approval_mode == 'disabled', no warning."""
        raw = _make_raw(agent={})
        bp, stderr = _parse_capture_stderr(raw)
        assert bp.agent.approval_mode == "disabled"
        assert "[deprecated] agent.approval_mode" not in stderr


# ---------------------------------------------------------------------------
# Schema exposure — approval alias in JSON Schema
# ---------------------------------------------------------------------------


class TestApprovalSchema:
    def test_schema_contains_approval_alias(self):
        """`aqueduct schema --target blueprint` JSON exposes the approval alias."""
        from aqueduct.parser.schema import BlueprintSchema
        schema = BlueprintSchema.model_json_schema()
        # Walk to agent block
        props = schema.get("$defs", schema)
        # Find AgentSchema
        def _find_def(name):
            for key, val in schema.get("$defs", {}).items():
                if name.lower() in key.lower():
                    return val
            return None
        agent_def = _find_def("Agent") or _find_def("AgentSchema")
        if agent_def:
            agent_props = agent_def.get("properties", {})
            approval = agent_props.get("approval_mode", agent_props.get("approval"))
            # At least one of them should reference the alias
            assert approval is not None


# ---------------------------------------------------------------------------
# --set routing — approval alias through overrides
# ---------------------------------------------------------------------------


class TestApprovalSetRouting:
    def test_set_approval_routes_to_blueprint(self):
        """--set agent.approval=auto routes to BlueprintSchema."""
        from aqueduct.overrides import route_overrides
        cfg, bp = route_overrides(["agent.approval=auto"], allow_blueprint=True)
        assert bp == {"agent": {"approval": "auto"}}
        assert cfg == {}

    def test_set_approval_mode_routes_to_blueprint(self):
        """--set agent.approval_mode=auto routes to BlueprintSchema."""
        from aqueduct.overrides import route_overrides
        cfg, bp = route_overrides(["agent.approval_mode=auto"], allow_blueprint=True)
        assert bp == {"agent": {"approval_mode": "auto"}}
        assert cfg == {}
