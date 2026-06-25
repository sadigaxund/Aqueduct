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


class TestBlobLeakGuardrail:
    """Phase: storage integrity — warn on implicitly-local blobs under remote obs."""

    def _warns(self, stores_dict):
        import warnings
        from aqueduct.config import StoresConfig
        from aqueduct import AqueductWarning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            StoresConfig(**stores_dict)
            return [x for x in w if issubclass(x.category, AqueductWarning)
                    and "blob" in str(x.message)]

    def test_remote_obs_implicit_local_blob_warns(self):
        assert self._warns({"observability": {"backend": "postgres", "path": "postgresql://x/y"}})

    def test_explicit_local_blob_is_silent(self):
        assert not self._warns({
            "observability": {"backend": "postgres", "path": "postgresql://x/y"},
            "blob": {"backend": "local"},
        })

    def test_duckdb_default_is_silent(self):
        assert not self._warns({})

    def test_remote_obs_remote_blob_is_silent(self):
        assert not self._warns({
            "observability": {"backend": "postgres", "path": "postgresql://x/y"},
            "blob": {"backend": "s3", "path": "s3://b/k"},
        })


class TestConfigAqGuard:
    """aqueduct.yml resolves only ${ENV} + @aq.secret(); other @aq.* is rejected."""

    def _write(self, tmp_path, body):
        p = tmp_path / "aqueduct.yml"
        p.write_text(body, encoding="utf-8")
        return p

    def test_non_secret_aq_in_config_rejected(self, tmp_path):
        from aqueduct.config import ConfigError
        p = self._write(tmp_path,
            'stores:\n  depot:\n    path: ".aqueduct/@aq.blueprint.id().db"\n')
        with pytest.raises(ConfigError, match=r"@aq\.blueprint\.id cannot be used in aqueduct\.yml"):
            load_config(p)

    def test_run_scope_in_config_rejected(self, tmp_path):
        from aqueduct.config import ConfigError
        p = self._write(tmp_path, 'deployment:\n  master_url: "@aq.run.id()"\n')
        with pytest.raises(ConfigError, match=r"@aq\.run\.id"):
            load_config(p)

    def test_engine_flink_rejected(self, tmp_path):
        from aqueduct.config import ConfigError
        p = self._write(tmp_path, 'deployment:\n  engine: flink\n')
        with pytest.raises(ConfigError, match=r"flink is not yet supported"):
            load_config(p)

    def test_env_and_plain_config_ok(self, tmp_path, monkeypatch):
        monkeypatch.setenv("AQ_CFG_ENV", "cluster")
        p = self._write(tmp_path, 'deployment:\n  env: ${AQ_CFG_ENV}\n')
        cfg = load_config(p)
        assert cfg.deployment.env == "cluster"
