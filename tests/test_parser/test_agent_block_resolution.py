"""Tier-0 resolution in the Blueprint `agent:` block (1.0.1 fix), and
CONNECTION-field rejection (2.59 security fix).

Covers ⏳ items in TEST_MANIFEST.md § Parser — Tier-0 resolution in agent block:

  - agent.prompt_context with ${ctx.*} resolves from blueprint context
  - None / unset agent fields pass through without errors

2.59 — `base_url`, `model`, `provider_options`, `api_key`, `timeout`,
`cascade` are CONNECTION fields, removed from the Blueprint `agent:` block
entirely (a security fix — see `AgentSchema`'s docstring in
`aqueduct/parser/schema.py`). They are no longer Tier-0-resolved here at
all; a Blueprint that sets one is REJECTED at parse time, with pydantic
naming the offending key. Tier-0 (`${ENV}`/`${ctx.*}`) resolution for these
fields now only happens for `aqueduct.yml`'s `agent:` block
(`AgentConnectionConfig`), covered in `tests/test_parser/test_config.py`.
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


class TestAgentBlockConnectionFieldsRejected:
    """2.59 — every CONNECTION field is a named pydantic rejection at
    Blueprint parse time, never a silent inherit. Replaces the pre-2.59
    TestAgentBlockEnvVarResolution class, which asserted these fields
    resolved `${ENV}` templates in the Blueprint — that resolution now only
    happens at the aqueduct.yml level (`AgentConnectionConfig`)."""

    @pytest.mark.parametrize(
        "agent_block",
        [
            '  base_url: "http://ollama.internal:11434/v1"\n',
            '  model: "qwen2.5-coder:7b"\n',
            "  provider: openai_compat\n",
            '  api_key: "sk-literal"\n',
            "  timeout: 120.0\n",
            "  provider_options:\n    api_version: '2024-02-01'\n",
            "  cascade:\n    - model: claude\n",
        ],
    )
    def test_connection_field_rejected(self, tmp_path, agent_block):
        bp = _write_bp(tmp_path, agent_block=agent_block)
        with pytest.raises(ParseError, match="1 validation error"):
            parse(bp)

    def test_missing_env_var_in_policy_field_still_raises_parse_error(self, tmp_path, monkeypatch):
        # prompt_context (a POLICY field) is still Tier-0 resolved, so a
        # missing ${ENV} reference there is still a real parse-time failure.
        monkeypatch.delenv("DEFINITELY_NOT_SET_VAR", raising=False)
        bp = _write_bp(
            tmp_path,
            agent_block='  prompt_context: "${DEFINITELY_NOT_SET_VAR}"\n',
        )
        with pytest.raises(ParseError, match="agent config resolution failed"):
            parse(bp)


class TestAgentBlockCtxResolution:
    def test_prompt_context_resolves_from_context_block(self, tmp_path):
        bp = _write_bp(
            tmp_path,
            agent_block='  prompt_context: "Pipeline runs in ${ctx.team}"\n',
            context_block="context:\n  team: data-eng\n",
        )
        result = parse(bp)
        assert result.agent.prompt_context == "Pipeline runs in data-eng"


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
        # Default agent config has no connection fields at all (Blueprint-level).
        assert result.agent is not None

    def test_policy_fields_passthrough_unchanged(self, tmp_path):
        bp = _write_bp(
            tmp_path,
            agent_block=(
                "  approval: human\n"
                "  sandbox_mode: preflight\n"
            ),
        )
        result = parse(bp)
        assert result.agent.approval_mode == "human"
        assert result.agent.sandbox_mode == "preflight"
