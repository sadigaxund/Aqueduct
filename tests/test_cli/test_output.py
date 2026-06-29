"""Tests for aqueduct.cli.output (Phase 1 — T24 foundation)."""

from __future__ import annotations

import json

import click
import pytest

from aqueduct.cli.output import emit, warn
from aqueduct import redaction

pytestmark = pytest.mark.unit


# Tests that register secrets need to guard against the click.echo wrapper
# installed by _install_secret_redaction_hooks() (run from CLI tests that
# execute first). Save/restore the original to keep tests hermetic.
_ORIG_ECHO = click.echo


@pytest.fixture(autouse=True)
def _guard_echo():
    """Restore click.echo and clear redaction registry after each test."""
    yield
    click.echo = _ORIG_ECHO
    redaction.clear()


class TestEmit:
    def test_emit_json_outputs_valid_json(self, capsys):
        data = {"a": 1, "nested": [2, 3]}
        emit(data, fmt="json", redact=False)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed == data

    def test_emit_text_outputs_human_line(self, capsys):
        data = {"a": 1}
        emit(data, fmt="text", redact=False)
        captured = capsys.readouterr()
        assert "{'a': 1}" in captured.out

    def test_emit_string_passthrough(self, capsys):
        emit("hello world", fmt="text", redact=False)
        captured = capsys.readouterr()
        assert captured.out.strip() == "hello world"

    def test_redact_scrubs_registered_secret(self, capsys):
        secret = "s3cr3t-k3y-l0ng-3n0ugh"
        redaction.register(secret)
        emit({"key": secret}, fmt="json", redact=True)
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.out
        assert secret not in captured.out

    def test_redact_false_bypasses_scrubbing(self, capsys):
        secret = "s3cr3t-k3y-l0ng-3n0ugh"
        redaction.register(secret)
        emit({"key": secret}, fmt="json", redact=False)
        captured = capsys.readouterr()
        assert secret in captured.out
        assert "[REDACTED]" not in captured.out


class TestEmitRedact:
    """Phase 5 — redaction at output for risky echo sites."""

    def test_emit_redacts_config_like_error(self, capsys):
        """A config-path error message that includes a registered secret is scrubbed."""
        secret = "sk-s3cr3t-k3y-l0ng"
        redaction.register(secret)
        emit(f"config error: master_url=http://user:{secret}@host:8080", fmt="text", redact=True)
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.out
        assert secret not in captured.out

    def test_emit_redacts_store_path_with_secret(self, capsys):
        """A store path that embeds a registered secret is scrubbed."""
        secret = "sk-s3cr3t-k3y-l0ng"
        redaction.register(secret)
        emit(f"stores: observability=s3://bucket/{secret}/obs.db", fmt="text", redact=True)
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.out
        assert secret not in captured.out

    def test_emit_redacts_dsn_credentials(self, capsys):
        """A DSN-style connection string is scrubbed when the password is registered."""
        secret = "p4ssw0rd-l0ng-3n0ugh"
        redaction.register(secret)
        emit(
            {
                "error": "connection failed",
                "detail": f"DSN postgresql://user:{secret}@dbhost/dbname",
            },
            fmt="json",
            redact=True,
        )
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.out
        assert secret not in captured.out

    def test_emit_redacts_nested_data(self, capsys):
        """Nested dicts/lists are recursively redacted."""
        secret = "s3cr3t-v4lu3-l0ng"
        redaction.register(secret)
        emit(
            {
                "agent": {
                    "provider": "openai",
                    "api_key": secret,
                },
                "stores": [
                    {"backend": "postgres", "password": secret},
                ],
            },
            fmt="json",
            redact=True,
        )
        captured = capsys.readouterr()
        assert "[REDACTED]" in captured.out
        assert secret not in captured.out


class TestWarn:
    def test_warn_produces_styled_output(self, capsys):
        warn("test_rule", "something happened", module="test_module")
        captured = capsys.readouterr()
        assert "⚠" in captured.err
        assert "[test_rule]" in captured.err
        assert "something happened" in captured.err


class TestRuntimeWarningRollup:
    """T26 — the end-of-run runtime-warning roll-up renderer (emit_warning_pairs),
    the shared shape behind compile/session/runtime blocks."""

    def _pairs(self):
        return [
            ("runtime_assert", "quality_gate: min_rows got 360, expected >= 100000"),
            ("runtime_probe_blocked", "distinct_scan: distinct_count skipped (block_full_actions)"),
        ]

    def test_collapsed_header_and_module_ids(self, capsys, monkeypatch):
        from aqueduct.cli import style
        monkeypatch.setattr(style, "_color_enabled", lambda: False)
        style.emit_warning_pairs(self._pairs(), label="runtime:", verbose=False, err=False)
        out = capsys.readouterr().out
        assert "⚠ runtime: 2 warnings" in out
        assert "-v for full text" in out           # collapsed affordance
        assert "[runtime_assert]" in out
        assert "quality_gate" in out and "distinct_scan" in out   # module ids present

    def test_verbose_shows_full_message(self, capsys, monkeypatch):
        from aqueduct.cli import style
        monkeypatch.setattr(style, "_color_enabled", lambda: False)
        style.emit_warning_pairs(self._pairs(), label="runtime:", verbose=True, err=False)
        out = capsys.readouterr().out
        assert "expected >= 100000" in out          # full body, not truncated
        assert "-v for full text" not in out        # no collapse hint when verbose

    def test_empty_prints_nothing(self, capsys):
        from aqueduct.cli import style
        style.emit_warning_pairs([], label="runtime:", err=False)
        assert capsys.readouterr().out == ""

    def test_run_aggregation_shape(self, capsys, monkeypatch):
        """The run.py aggregation: (rule_id, '<module>: <msg>') across module_results."""
        from aqueduct.cli import style
        from aqueduct.executor.models import ModuleResult
        monkeypatch.setattr(style, "_color_enabled", lambda: False)
        module_results = (
            ModuleResult(module_id="m1", status="success",
                         warnings=(("runtime_assert", "min_rows got 1"),)),
            ModuleResult(module_id="m2", status="success",
                         warnings=(("runtime_probe_signal_error", "signal failed"),)),
        )
        pairs = [(rid, f"{mr.module_id}: {msg}")
                 for mr in module_results for rid, msg in mr.warnings]
        style.emit_warning_pairs(pairs, label="runtime:", verbose=True, err=False)
        out = capsys.readouterr().out
        assert "⚠ runtime: 2 warnings" in out
        assert "m1: min_rows got 1" in out and "m2: signal failed" in out
