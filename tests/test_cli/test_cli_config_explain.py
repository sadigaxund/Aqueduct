"""`aqueduct config explain` must say where every resolved value came from.

The command exists because nothing else answers "why is this setting this
value". Its whole contract is the source label, so these tests pin the
attribution for each of the five sources rather than the formatting.
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.integration


def _write_config(tmp_path, body: str):
    path = tmp_path / "aqueduct.yml"
    path.write_text(body)
    return path


def _explain(config_path, *extra, env=None) -> dict[str, dict]:
    res = CliRunner().invoke(
        cli,
        ["config", "explain", "--config", str(config_path), "--format", "json", *extra],
        env=env or {},
    )
    assert res.exit_code == 0, (res.exit_code, res.output)
    brace = res.output.find("[")
    assert brace != -1, f"no JSON body: {res.output!r}"
    rows = json.loads(res.output[brace:].strip())
    return {r["path"]: r for r in rows}


def test_a_literal_in_the_config_file_is_labelled_file(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n  model: m1\n")
    rows = _explain(cfg)
    assert rows["agent.provider"]["source"] == "file"
    assert rows["agent.provider"]["value"] == "openai_compat"
    assert str(cfg) in rows["agent.provider"]["detail"]


def test_an_undeclared_value_is_labelled_default(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n")
    rows = _explain(cfg)
    assert rows["agent.max_reprompts"]["source"] == "default"
    assert rows["agent.max_reprompts"]["detail"] == "schema default"


def test_a_dollar_brace_value_is_labelled_env_and_names_the_variable(tmp_path):
    cfg = _write_config(
        tmp_path,
        'agent:\n  provider: openai_compat\n  base_url: "${AQ_TEST_BASE_URL}"\n',
    )
    rows = _explain(cfg, env={"AQ_TEST_BASE_URL": "http://h:1/v1"})
    row = rows["agent.base_url"]
    assert row["source"] == "env", "an expanded ${VAR} must not be reported as a file literal"
    assert row["value"] == "http://h:1/v1", "the RESOLVED value is reported, not the raw text"
    assert row["detail"] == "${AQ_TEST_BASE_URL}"


def test_a_set_override_is_labelled_override_and_wins_over_the_file(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n  timeout: 120\n")
    rows = _explain(cfg, "-s", "agent.timeout=600")
    row = rows["agent.timeout"]
    assert row["source"] == "override"
    assert row["detail"] == "-s/--set"
    assert float(row["value"]) == 600.0


def test_a_set_override_beats_an_env_value_too(tmp_path):
    cfg = _write_config(
        tmp_path,
        'agent:\n  provider: openai_compat\n  base_url: "${AQ_TEST_BASE_URL}"\n',
    )
    rows = _explain(
        cfg,
        "-s",
        "agent.base_url=http://flag:9/v1",
        env={"AQ_TEST_BASE_URL": "http://h:1/v1"},
    )
    assert rows["agent.base_url"]["source"] == "override"
    assert rows["agent.base_url"]["value"] == "http://flag:9/v1"


def test_blueprint_agent_overrides_are_reported_as_their_own_source(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n")
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\nid: bp1\nagent:\n  approval: auto\n  max_patches: 3\nmodules: []\n"
    )
    rows = _explain(cfg, "--blueprint", str(bp))
    assert rows["agent.approval"]["source"] == "blueprint"
    assert rows["agent.approval"]["value"] == "auto"
    assert str(bp) in rows["agent.approval"]["detail"]


def test_no_blueprint_means_no_blueprint_sourced_rows(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n")
    rows = _explain(cfg)
    assert not [r for r in rows.values() if r["source"] == "blueprint"]


def test_source_filter_narrows_the_output(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n  model: m1\n")
    rows = _explain(cfg, "--source", "file")
    assert set(rows) == {"agent.provider", "agent.model"}


def test_the_command_writes_nothing_back(tmp_path):
    """Read-only: an override must not be persisted into aqueduct.yml."""
    body = "agent:\n  provider: openai_compat\n  timeout: 120\n"
    cfg = _write_config(tmp_path, body)
    _explain(cfg, "-s", "agent.timeout=600")
    assert cfg.read_text() == body


def test_a_bad_override_path_exits_config_error(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n")
    res = CliRunner().invoke(cli, ["config", "explain", "--config", str(cfg), "-s", "agent.nope=1"])
    assert res.exit_code == exit_codes.CONFIG_ERROR, (res.exit_code, res.output)


def test_table_output_carries_a_source_column(tmp_path):
    cfg = _write_config(tmp_path, "agent:\n  provider: openai_compat\n")
    res = CliRunner().invoke(cli, ["config", "explain", "--config", str(cfg)])
    assert res.exit_code == 0, res.output
    assert "source" in res.output
    assert "agent.provider" in res.output
