"""agent.approval — the canonical key (2.0).

The former `approval_mode` YAML key was removed (no populate_by_name on
AgentSchema → the field name is not accepted as a key). The Python attribute is
still `approval_mode`, keyed from YAML via the `approval` alias.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Any

import pytest

from aqueduct.parser.parser import ParseError, parse_dict

pytestmark = pytest.mark.unit

_SINGLE_MODULE = [
    {"id": "src", "type": "Ingress", "label": "Source",
     "config": {"format": "parquet", "path": "s3://bucket/in.parquet"}}
]
BASE_DIR = Path(".")


def _make_raw(agent: dict[str, Any] | None = None) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "aqueduct": "1.0", "id": "test_approval", "name": "Approval Test",
        "modules": _SINGLE_MODULE,
    }
    if agent is not None:
        raw["agent"] = agent
    return raw


def _parse_capture_stderr(raw: dict[str, Any]) -> tuple[Any, str]:
    buf = io.StringIO()
    old = sys.stderr
    try:
        sys.stderr = buf
        bp = parse_dict(raw, base_dir=BASE_DIR)
    finally:
        sys.stderr = old
    return bp, buf.getvalue()


class TestApprovalCanonicalKey:
    @pytest.mark.parametrize("value", ["auto", "human", "disabled", "ci"])
    def test_approval_value_parses_no_warning(self, value):
        bp, stderr = _parse_capture_stderr(_make_raw(agent={"approval": value}))
        assert bp.agent.approval_mode == value
        assert "[deprecated]" not in stderr


class TestApprovalModeKeyRemoved:
    def test_approval_mode_key_rejected(self):
        """The former `approval_mode` YAML key is no longer accepted (extra=forbid)."""
        with pytest.raises(ParseError):
            parse_dict(_make_raw(agent={"approval_mode": "human"}), base_dir=BASE_DIR)


class TestApprovalAbsent:
    def test_absent_agent_defaults_disabled(self):
        bp, stderr = _parse_capture_stderr(_make_raw(agent=None))
        assert bp.agent.approval_mode == "disabled"
        assert "[deprecated]" not in stderr

    def test_empty_agent_block_defaults_disabled(self):
        bp, _ = _parse_capture_stderr(_make_raw(agent={}))
        assert bp.agent.approval_mode == "disabled"


class TestApprovalSetRouting:
    def test_set_approval_routes_to_blueprint(self):
        from aqueduct.overrides import route_overrides
        cfg, bp = route_overrides(["agent.approval=auto"], allow_blueprint=True)
        assert bp == {"agent": {"approval": "auto"}}
        assert cfg == {}
