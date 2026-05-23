"""Tier-0 resolution in the Blueprint `agent:` block (1.0.1 fix).

Covers ⏳ items in TEST_MANIFEST.md § Parser — Tier-0 resolution in agent block:

  - agent.base_url with ${ENV_VAR} resolves
  - agent.model with ${ENV_VAR} resolves; missing var raises ParseError
  - agent.prompt_context with ${ctx.*} resolves from blueprint context
  - agent.provider_options dict with nested ${ENV} resolves
  - None / unset agent fields pass through without errors
"""
from __future__ import annotations

import pytest

from aqueduct.parser.parser import ParseError, parse

pytestmark = pytest.mark.unit


def _write_bp(tmp_path, agent_block: str, context_block: str = "") -> str:
    """Build a minimal blueprint with the given agent + optional context block."""
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_agent_resolution\n"
        "name: Test\n"
        f"{context_block}"
        f"agent:\n{agent_block}\n"
        "modules:\n"
        "  - id: src\n"
        "    type: Ingress\n"
        "    label: Src\n"
        "    config:\n"
        "      format: csv\n"
        "      path: /tmp/nonexistent.csv\n"
        "edges: []\n",
        encoding="utf-8",
    )
    return str(bp)


class TestAgentBlockEnvVarResolution:
    def test_base_url_env_var_resolves(self, tmp_path, monkeypatch):
        monkeypatch.setenv("AQ_OLLAMA_URL", "http://10.0.0.39:11434")
        bp = _write_bp(
            tmp_path,
            agent_block='  provider: openai_compat\n  base_url: "${AQ_OLLAMA_URL}/v1"\n  model: m\n',
        )
        result = parse(bp)
        assert result.agent.base_url == "http://10.0.0.39:11434/v1"

    def test_model_env_var_resolves(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_MODEL", "qwen2.5-coder:7b")
        bp = _write_bp(
            tmp_path,
            agent_block='  model: "${MY_MODEL}"\n',
        )
        result = parse(bp)
        assert result.agent.model == "qwen2.5-coder:7b"

    def test_missing_env_var_raises_parse_error(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEFINITELY_NOT_SET_VAR", raising=False)
        bp = _write_bp(
            tmp_path,
            agent_block='  model: "${DEFINITELY_NOT_SET_VAR}"\n',
        )
        with pytest.raises(ParseError, match="agent config resolution failed"):
            parse(bp)

    def test_env_var_with_default_resolves_when_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv("UNSET_WITH_DEFAULT", raising=False)
        bp = _write_bp(
            tmp_path,
            agent_block='  model: "${UNSET_WITH_DEFAULT:-default-model}"\n',
        )
        result = parse(bp)
        assert result.agent.model == "default-model"


class TestAgentBlockCtxResolution:
    def test_prompt_context_resolves_from_context_block(self, tmp_path):
        bp = _write_bp(
            tmp_path,
            agent_block='  prompt_context: "Pipeline runs in ${ctx.team}"\n',
            context_block="context:\n  team: data-eng\n",
        )
        result = parse(bp)
        assert result.agent.prompt_context == "Pipeline runs in data-eng"

    def test_provider_options_nested_env_resolves(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPENAI_API_VERSION", "2024-02-01")
        bp = _write_bp(
            tmp_path,
            agent_block=(
                "  provider: openai_compat\n"
                "  model: m\n"
                "  provider_options:\n"
                '    api_version: "${OPENAI_API_VERSION}"\n'
            ),
        )
        result = parse(bp)
        assert result.agent.provider_options["api_version"] == "2024-02-01"


class TestAgentBlockPassThrough:
    def test_no_agent_block_is_valid(self, tmp_path):
        """Blueprint without agent block parses cleanly (self-healing disabled)."""
        bp = tmp_path / "bp.yml"
        bp.write_text(
            "aqueduct: '1.0'\n"
            "id: no_agent\n"
            "name: No agent\n"
            "modules:\n"
            "  - id: src\n"
            "    type: Ingress\n"
            "    label: Src\n"
            "    config: {format: csv, path: /tmp/x.csv}\n"
            "edges: []\n",
            encoding="utf-8",
        )
        result = parse(str(bp))
        # Default agent config has provider=anthropic but no model/base_url; passes parse.
        assert result.agent is not None

    def test_literal_strings_passthrough_unchanged(self, tmp_path):
        bp = _write_bp(
            tmp_path,
            agent_block=(
                "  provider: openai_compat\n"
                "  model: literal-model-name\n"
                "  base_url: https://api.example.com/v1\n"
            ),
        )
        result = parse(bp)
        assert result.agent.model == "literal-model-name"
        assert result.agent.base_url == "https://api.example.com/v1"
