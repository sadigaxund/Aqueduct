"""Unit tests for aqueduct/config.py."""

from __future__ import annotations

import yaml
from pathlib import Path

import pytest
pytestmark = pytest.mark.unit

from aqueduct.config import AqueductConfig, AgentConnectionConfig, load_config


class TestAgentConnectionConfig:
    def test_llm_timeout_default_and_custom(self):
        # Default is 120.0
        cfg = AgentConnectionConfig()
        assert cfg.llm_timeout == 120.0

        # Custom is respected
        cfg_custom = AgentConnectionConfig(llm_timeout=600.0)
        assert cfg_custom.llm_timeout == 600.0

    def test_llm_max_reprompts_default_and_custom(self):
        # Default is 3
        cfg = AgentConnectionConfig()
        assert cfg.llm_max_reprompts == 3

        # Custom is respected
        cfg_custom = AgentConnectionConfig(llm_max_reprompts=10)
        assert cfg_custom.llm_max_reprompts == 10

    def test_load_config_respects_custom_agent_values(self, tmp_path):
        cfg_path = tmp_path / "aqueduct.yml"
        cfg_data = {
            "agent": {
                "llm_timeout": 300.5,
                "llm_max_reprompts": 5
            }
        }
        cfg_path.write_text(yaml.dump(cfg_data))
        
        config = load_config(cfg_path)
        assert config.agent.llm_timeout == 300.5
        assert config.agent.llm_max_reprompts == 5
