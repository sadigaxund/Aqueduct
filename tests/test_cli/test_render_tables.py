"""Tests for aqueduct.cli.render.tables — the shared width-aware table
helper (Phase 85 Wave 1). The pipe-safety test is the important one: piped/
CI output must never carry an ANSI escape, a rich box-drawing border, or a
truncated cell — that's the regression the survey called out."""

from __future__ import annotations

import re

import pytest

from aqueduct.cli.render.tables import Column, render_table_str

pytestmark = pytest.mark.unit

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_BOX_CHARS = "┌┐└┘├┤┬┴┼│═╔╗╚╝╠╣╦╩╬"

_LONG_CELL = (
    'replace unknown column "total" with "total_amt" in enrich SQL '
    "before the join step runs, otherwise downstream aggregation breaks"
)

_COLUMNS = [
    Column("patch"),
    Column("status"),
    Column("module"),
    Column("rationale", flex=True),
]

_ROWS = [
    ["a3f2c1", "pending", "enrich", _LONG_CELL],
    ["99d0e4", "applied", "output", "short one"],
]


class TestPipeSafety:
    def test_no_ansi_escape_in_piped_output(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        text = render_table_str(_COLUMNS, _ROWS)
        assert "\x1b" not in text
        assert not _ANSI_RE.search(text)

    def test_no_box_drawing_borders_in_piped_output(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        text = render_table_str(_COLUMNS, _ROWS)
        assert not any(ch in text for ch in _BOX_CHARS)

    def test_full_untruncated_text_in_piped_output(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        text = render_table_str(_COLUMNS, _ROWS)
        assert _LONG_CELL in text
        assert "…" not in text

    def test_one_line_per_record(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        text = render_table_str(_COLUMNS, _ROWS)
        lines = text.split("\n")
        # header + rule + one line per row, nothing more
        assert len(lines) == 2 + len(_ROWS)


class TestTtyWidthFitting:
    @pytest.mark.parametrize("width", [68, 100])
    def test_no_line_exceeds_width(self, monkeypatch, width):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", str(width))
        text = render_table_str(_COLUMNS, _ROWS, width=width)
        for line in text.split("\n"):
            assert len(line) <= width, repr(line)

    def test_fixed_columns_keep_width_across_terminal_sizes(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        text_68 = render_table_str(_COLUMNS, _ROWS, width=68).split("\n")
        text_100 = render_table_str(_COLUMNS, _ROWS, width=100).split("\n")
        # First row: the "patch"/"status"/"module" cell boundaries land at
        # the same offsets regardless of terminal width — only the flex
        # ("rationale") column grows.
        row_68 = text_68[2]
        row_100 = text_100[2]
        assert row_68[:24] == row_100[:24]

    def test_flex_column_truncates_on_narrow_tty(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        text = render_table_str(_COLUMNS, _ROWS, width=68)
        assert "…" in text
        assert _LONG_CELL not in text

    def test_flex_column_absorbs_width_on_wide_tty(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        text = render_table_str(_COLUMNS, _ROWS, width=160)
        assert _LONG_CELL in text
        assert "…" not in text


class TestVerbose:
    def test_verbose_lifts_truncation_on_tty(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        text = render_table_str(_COLUMNS, _ROWS, width=68, verbose=True)
        assert _LONG_CELL in text
        assert "…" not in text


class TestAlignment:
    def test_right_aligned_numeric_column_stays_right_aligned(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        cols = [Column("Module"), Column("Duration", align="right")]
        rows = [["mod_a", "5ms"], ["mod_bbbbbbbbbb", "12345ms"]]
        text = render_table_str(cols, rows, width=60)
        lines = text.split("\n")
        # Duration values should end at the same column offset (right edge).
        row1_end = lines[2].rstrip().rfind("5ms") + len("5ms")
        row2_end = lines[3].rstrip().rfind("12345ms") + len("12345ms")
        assert row1_end == row2_end

    def test_right_alignment_holds_in_piped_output_too(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        cols = [Column("Module"), Column("Duration", align="right")]
        rows = [["mod_a", "5ms"], ["mod_bbbbbbbbbb", "12345ms"]]
        text = render_table_str(cols, rows)
        lines = text.split("\n")
        assert lines[2].endswith("5ms")
        assert lines[3].endswith("12345ms")


class TestValidation:
    def test_more_than_one_flex_column_raises(self):
        cols = [Column("a", flex=True), Column("b", flex=True)]
        with pytest.raises(ValueError):
            render_table_str(cols, [["x", "y"]])

    def test_no_flex_column_is_allowed(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        cols = [Column("a"), Column("b")]
        text = render_table_str(cols, [["x", "y"]])
        assert "x" in text and "y" in text
