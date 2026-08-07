"""Tests for aqueduct/utils.py — cross-layer helpers with no aqueduct.* imports.

Neither `format_error_loc` nor `utcnow_iso` had a direct unit test before this
file (both are exercised only indirectly through consumers — pydantic error
formatting for the former, `agent/loop.py` for the latter — so a breaking
change to either helper's contract could pass CI without ever being pinned
here).
"""

from __future__ import annotations

import re
from datetime import datetime

import pytest

pytestmark = pytest.mark.unit

from aqueduct.utils import format_error_loc, utcnow_iso


class TestFormatErrorLoc:
    def test_empty_loc_returns_root_placeholder(self):
        assert format_error_loc(()) == "<root>"

    def test_single_field_name_has_no_leading_dot(self):
        assert format_error_loc(("blueprint_id",)) == "blueprint_id"

    def test_nested_field_names_join_with_dots(self):
        assert format_error_loc(("operations", "op")) == "operations.op"

    def test_list_index_uses_bracket_notation(self):
        assert format_error_loc(("operations", 0, "op")) == "operations[0].op"

    def test_leading_index_has_no_leading_dot(self):
        """An index as the FIRST loc element still renders as `[N]`, not `.{N}` or `[N]` prefixed with a dot."""
        assert format_error_loc((0, "op")) == "[0].op"

    def test_multiple_consecutive_indices(self):
        assert format_error_loc(("modules", 2, "config", 1)) == "modules[2].config[1]"


class TestUtcnowIso:
    def test_returns_iso_8601_string_with_utc_offset(self):
        ts = utcnow_iso()
        assert isinstance(ts, str)
        # +00:00 offset (UTC) — not a naive/local-time isoformat string.
        assert ts.endswith("+00:00")
        # Round-trips through datetime.fromisoformat unchanged.
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None

    def test_matches_iso_8601_shape(self):
        ts = utcnow_iso()
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+00:00$", ts)

    def test_successive_calls_are_monotonically_non_decreasing(self):
        first = datetime.fromisoformat(utcnow_iso())
        second = datetime.fromisoformat(utcnow_iso())
        assert second >= first
