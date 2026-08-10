"""Tests for `aqueduct patch policy` — the discovery command for Gate 1's
`set_engine_config` healing-config allowlist (cross-engine remediation
follow-up: a rejection that names a rule nobody can look up is half a gate).

Pure CLI-rendering tests: they read the two SHIPPED
`engine_config_allowlist.yml` files (spark, duckdb) through the real
loader — no mocking of the allowlist data — so a typo in either shipped
file, or a mismatch between the two render paths (text vs json), breaks
here rather than being noticed for the first time by a user.
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit


def _run(*args):
    return CliRunner().invoke(cli, ["patch", "policy", *args])


class TestPatchPolicyCommandRegistered:
    def test_policy_is_a_real_patch_subcommand(self):
        """Guards the exact command name Gate 1's rejection message points
        at (`aqueduct patch policy`) — if this command is ever renamed
        without updating the pointer string in `apply.py`, this fails
        alongside the string-match guard in
        tests/test_patch/test_guardrails_rollback.py."""
        assert "policy" in cli.commands["patch"].commands


class TestPatchPolicyText:
    def test_lists_allowed_keys_and_denied_families_for_both_engines(self):
        result = _run()
        assert result.exit_code == 0, result.output

        assert "Engine: spark" in result.output
        assert "Engine: duckdb" in result.output

        # Spark — a representative allowed key and a representative denied
        # family, with its reason (the whole point of this command).
        assert "spark.sql.shuffle.partitions" in result.output
        assert "spark.master" in result.output
        assert "redirects where work runs; a heal may not move the job" in result.output

        # DuckDB — same shape, typed-field engine.
        assert "memory_limit" in result.output
        assert "database_path" in result.output
        assert "the on-disk database file location" in result.output

        # Explicit "this is the whole policy" statement — no configurability
        # implied.
        assert "not yet implemented" in result.output

    def test_narrows_to_one_engine(self):
        result = _run("--engine", "duckdb")
        assert result.exit_code == 0, result.output
        assert "Engine: duckdb" in result.output
        assert "Engine: spark" not in result.output

    def test_unregistered_engine_fails_with_typed_error_and_usage_exit_code(self):
        from aqueduct import exit_codes

        result = _run("--engine", "flink")
        assert result.exit_code == exit_codes.USAGE_ERROR
        assert "flink" in result.output
        assert "not registered" in result.output
        # Registered engines are still named, so the failure is actionable.
        assert "spark" in result.output
        assert "duckdb" in result.output


class TestPatchPolicyJson:
    def test_json_is_parseable_and_carries_the_same_facts_as_text(self):
        result = _run("--format", "json")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)

        assert "narrowing" in payload
        assert "not yet implemented" in payload["narrowing"]

        engines = {e["engine"]: e for e in payload["engines"]}
        assert set(engines) == {"spark", "duckdb"}

        spark = engines["spark"]
        assert spark["shape"] == "free_form_conf_bag"
        spark_allow = {e["pattern"]: e for e in spark["allow"]}
        assert spark_allow["spark.sql.shuffle.partitions"]["type"] == "int"
        # enum constraint round-trips
        assert spark_allow["spark.serializer"]["enum"] == [
            "org.apache.spark.serializer.KryoSerializer",
            "org.apache.spark.serializer.JavaSerializer",
        ]
        spark_deny = {d["pattern"]: d for d in spark["deny"]}
        assert (
            spark_deny["spark.master"]["reason"]
            == "redirects where work runs; a heal may not move the job"
        )
        # scoped value-ban round-trips its deny_values
        assert spark_deny["spark.driver.maxResultSize"]["deny_values"] == ["0", 0]

        duckdb = engines["duckdb"]
        assert duckdb["shape"] == "typed_fields"
        duckdb_allow = {e["pattern"]: e for e in duckdb["allow"]}
        assert duckdb_allow["memory_limit"]["type"] == "size"
        assert duckdb_allow["threads"]["type"] == "int"
        duckdb_deny = {d["pattern"]: d for d in duckdb["deny"]}
        assert "database_path" in duckdb_deny

    def test_json_has_no_ansi_or_icon_styling(self):
        """`--format json` is structured data only — no colour, no icons
        (AGENTS.md: "CLI output speaks ONE vocabulary")."""
        result = _run("--format", "json")
        assert "\x1b[" not in result.output
        for icon in ("✓", "✗", "⚠"):
            assert icon not in result.output

    def test_json_narrows_to_one_engine(self):
        result = _run("--engine", "spark", "--format", "json")
        payload = json.loads(result.output)
        assert [e["engine"] for e in payload["engines"]] == ["spark"]
