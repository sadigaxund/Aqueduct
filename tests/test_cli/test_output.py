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
