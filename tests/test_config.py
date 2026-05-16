"""Unit tests for aqueduct/config.py."""

from __future__ import annotations

import yaml
from pathlib import Path

import pytest
pytestmark = pytest.mark.unit

from aqueduct.config import AqueductConfig, AgentConnectionConfig, load_config


class TestAgentConnectionConfig:
    def test_agent_timeout_default_and_custom(self):
        # Default is 120.0
        cfg = AgentConnectionConfig()
        assert cfg.timeout == 120.0

        # Custom is respected
        cfg_custom = AgentConnectionConfig(timeout=600.0)
        assert cfg_custom.timeout == 600.0

    def test_agent_max_reprompts_default_and_custom(self):
        # Default is 3
        cfg = AgentConnectionConfig()
        assert cfg.max_reprompts == 3

        # Custom is respected
        cfg_custom = AgentConnectionConfig(max_reprompts=10)
        assert cfg_custom.max_reprompts == 10

    def test_load_config_respects_custom_agent_values(self, tmp_path):
        cfg_path = tmp_path / "aqueduct.yml"
        cfg_data = {
            "agent": {
                "timeout": 300.5,
                "max_reprompts": 5
            }
        }
        cfg_path.write_text(yaml.dump(cfg_data))
        
        config = load_config(cfg_path)
        assert config.agent.timeout == 300.5
        assert config.agent.max_reprompts == 5
