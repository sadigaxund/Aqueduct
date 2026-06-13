"""Unit tests for aqueduct/config.py."""

from __future__ import annotations

import yaml
from pathlib import Path

import pytest
pytestmark = pytest.mark.unit

from aqueduct.config import AqueductConfig, AgentConnectionConfig, load_config


class TestAgentConnectionConfig:
    def test_agent_timeout_default_and_custom(self):
        # Default is 300.0
        cfg = AgentConnectionConfig()
        assert cfg.timeout == 300.0

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


class TestAgentMemoryConfig:
    def test_defaults_replay_coaching_true(self):
        from aqueduct.config import AgentMemoryConfig
        cfg = AgentMemoryConfig()
        assert cfg.replay is True
        assert cfg.coaching is True

    def test_frozen_pydantic(self):
        from aqueduct.config import AgentMemoryConfig
        cfg = AgentMemoryConfig()
        with pytest.raises(Exception):
            cfg.replay = False

    def test_extra_forbid_raises(self):
        from pydantic import ValidationError
        from aqueduct.config import AgentMemoryConfig
        with pytest.raises(ValidationError):
            AgentMemoryConfig(**{"replay": True, "unknown_key": 1})

    def test_replay_false_round_trips(self, tmp_path):
        import yaml
        from aqueduct.config import AgentMemoryConfig
        data = yaml.safe_load("memory:\n  replay: false\n  coaching: true\n")
        cfg = AgentMemoryConfig(**data["memory"])
        assert cfg.replay is False
        assert cfg.coaching is True

    def test_memory_in_agent_connection_config(self):
        from aqueduct.config import AgentConnectionConfig
        cfg = AgentConnectionConfig()
        assert cfg.memory.replay is True
        assert cfg.memory.coaching is True
