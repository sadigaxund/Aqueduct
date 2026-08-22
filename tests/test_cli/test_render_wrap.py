"""Tests for aqueduct.cli.render — width/wrap primitives + funnel stream defaults
(Phase 85 Wave 1 — the render funnel foundation)."""

from __future__ import annotations

import click
import pytest

from aqueduct.cli.render import width as _width
from aqueduct.cli.render.funnel import echo, emit
from aqueduct.cli.render.wrap import truncate, wrap_line

pytestmark = pytest.mark.unit

GLYPHS = "✓ ✗ ⚠ ⓘ ◆ · │ ├ └ ┆ ▶ ↳ ⊘ ⇄".split()


class TestDisplayWidth:
    @pytest.mark.parametrize("glyph", GLYPHS)
    def test_aqueduct_glyph_is_width_one(self, glyph):
        assert _width.display_width(glyph) == 1

    def test_cjk_char_is_width_two(self):
        assert _width.display_width("日") == 2

    def test_ansi_styled_string_measures_plain_width(self):
        plain = "hello"
        styled = click.style(plain, fg="red", bold=True)
        assert styled != plain  # sanity: styling actually added escapes
        assert _width.display_width(styled) == _width.display_width(plain) == len(plain)


class TestTerminalWidthAndTty:
    def test_terminal_width_reads_columns_env(self, monkeypatch):
        monkeypatch.setenv("COLUMNS", "42")
        assert _width.terminal_width() == 42

    def test_terminal_width_has_a_floor(self, monkeypatch):
        monkeypatch.setenv("COLUMNS", "1")
        assert _width.terminal_width() >= 20

    def test_force_tty_env_pins_true(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        assert _width.is_tty() is True
        assert _width.is_tty(err=True) is True

    def test_force_tty_env_pins_false(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        assert _width.is_tty() is False
        assert _width.is_tty(err=True) is False


LONG_TEXT = (
    'SQL binder error — Referenced column "total" not found in FROM clause '
    "— candidates: total_amt, total_qty"
)


class TestWrapLineTty:
    def _wrapped(self, monkeypatch, width, **kw):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", str(width))
        return wrap_line(LONG_TEXT, width=width, err=False, **kw)

    @pytest.mark.parametrize("width", [40, 60, 80, 100])
    def test_no_line_exceeds_width(self, monkeypatch, width):
        lines = self._wrapped(monkeypatch, width, gutter="  ")
        for line in lines:
            assert _width.display_width(line) <= width

    def test_every_line_starts_with_gutter(self, monkeypatch):
        lines = self._wrapped(monkeypatch, 50, gutter="│ ")
        for line in lines:
            assert line.startswith("│ ")

    def test_continuation_lines_carry_gutter_and_hang(self, monkeypatch):
        lines = self._wrapped(monkeypatch, 50, gutter="│ ", hang=4)
        assert len(lines) > 1
        for line in lines[1:]:
            assert line.startswith("│ " + " " * 4)

    def test_wraps_into_multiple_lines_when_narrow(self, monkeypatch):
        lines = self._wrapped(monkeypatch, 40, gutter="")
        assert len(lines) > 1


class TestWrapLinePiped:
    def test_piped_returns_one_unwrapped_gutter_prefixed_line(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        lines = wrap_line(LONG_TEXT, gutter="│ ", err=False)
        assert len(lines) == 1
        assert lines[0] == "│ " + LONG_TEXT

    def test_piped_ignores_max_lines(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        multi = "line one\nline two\nline three\nline four"
        lines = wrap_line(multi, gutter="", err=False, max_lines=1)
        # one logical record per input line, uncapped
        assert len(lines) == 4

    def test_piped_never_adds_hint(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        lines = wrap_line(LONG_TEXT, gutter="", err=False, max_lines=1, hint="full text")
        assert not any("-v" in line for line in lines)


class TestMaxLinesHeadCap:
    def test_tty_head_cap_adds_tail_line(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "20")
        text = "word " * 40  # forces many wrapped lines at width 20
        lines = wrap_line(text, gutter="", err=False, max_lines=3)
        assert len(lines) == 4  # 3 head lines + tail
        assert "more lines" in lines[-1]

    def test_no_cap_when_uncapped(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "20")
        text = "word " * 40
        lines = wrap_line(text, gutter="", err=False, max_lines=None)
        assert "more lines" not in "".join(lines)


class TestVerboseLiftsTruncation:
    def test_verbose_ignores_max_lines_and_hint(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "20")
        text = "word " * 40
        lines = wrap_line(text, gutter="", err=False, max_lines=3, hint="full text", verbose=True)
        assert "more lines" not in "".join(lines)
        assert not any("-v" in line for line in lines)
        # still wraps on a TTY
        assert len(lines) > 1

    def test_verbose_still_wraps_on_tty(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "30")
        lines = wrap_line(LONG_TEXT, gutter="", err=False, verbose=True)
        for line in lines:
            assert _width.display_width(line) <= 30


class TestHintMarker:
    def test_hint_appears_only_when_actually_shortened(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "20")
        text = "word " * 40
        capped = wrap_line(text, gutter="", err=False, max_lines=3, hint="full text")
        assert any("full text" in line and "-v" in line for line in capped)

        uncapped = wrap_line(text, gutter="", err=False, max_lines=None, hint="full text")
        assert not any("full text" in line for line in uncapped)


class TestHardBreak:
    def test_overlong_single_token_hard_breaks(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "10")
        token = "x" * 55
        lines = wrap_line(token, gutter="", width=10, err=False)
        assert len(lines) > 1
        for line in lines:
            assert _width.display_width(line) <= 10
        assert "".join(lines) == token


class TestTruncate:
    def test_noop_when_piped(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        text = "x" * 500
        assert truncate(text, 10) == text

    def test_noop_when_verbose(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        text = "x" * 500
        assert truncate(text, 10, verbose=True) == text

    def test_truncates_on_tty_with_ellipsis(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        text = "x" * 500
        out = truncate(text, 10)
        assert out.endswith("…")
        assert _width.display_width(out) <= 10

    def test_short_text_unchanged(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        assert truncate("hi", 10) == "hi"


class TestFunnelStreamDefaults:
    """`echo`/friends default to stderr; `result`/`emit` default to stdout."""

    def test_echo_defaults_to_stderr(self, capsys, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        echo("narrative line")
        captured = capsys.readouterr()
        assert "narrative line" in captured.err
        assert captured.out == ""

    def test_emit_defaults_to_stdout(self, capsys):
        emit("a result", redact=False)
        captured = capsys.readouterr()
        assert "a result" in captured.out
        assert captured.err == ""

    def test_echo_err_false_writes_stdout(self, capsys, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        echo("stdout line", err=False)
        captured = capsys.readouterr()
        assert "stdout line" in captured.out
        assert captured.err == ""
