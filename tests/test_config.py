"""Tests for engine configuration loader."""

from __future__ import annotations

import pytest
import yaml
from pathlib import Path
from pydantic import ValidationError

from aqueduct.config import AqueductConfig, ConfigError, load_config


def test_load_config_implicit_missing(monkeypatch, tmp_path):
    """no file present (implicit lookup) -> returns AqueductConfig with all defaults"""
    monkeypatch.chdir(tmp_path)
    config = load_config()
    assert isinstance(config, AqueductConfig)
    assert config.deployment.target == "local"


def test_load_config_explicit_missing(tmp_path):
    """explicit path that does not exist -> ConfigError"""
    with pytest.raises(ConfigError, match="Config file not found"):
        load_config(tmp_path / "ghost.yml")


def test_load_config_empty_file(tmp_path):
    """empty YAML file -> returns AqueductConfig with all defaults"""
    path = tmp_path / "empty.yml"
    path.write_text("")
    config = load_config(path)
    assert config.deployment.target == "local"


def test_load_config_valid_file(tmp_path):
    """valid aqueduct.yml -> returns correctly populated AqueductConfig"""
    path = tmp_path / "valid.yml"
    data = {
        "aqueduct_config": "1.0",
        "deployment": {"target": "yarn", "master_url": "yarn"},
        "spark_config": {"spark.executor.memory": "4g"}
    }
    path.write_text(yaml.dump(data))
    config = load_config(path)
    assert config.deployment.target == "yarn"
    assert config.deployment.master_url == "yarn"
    assert config.spark_config == {"spark.executor.memory": "4g"}


def test_load_config_invalid_yaml(tmp_path):
    """invalid YAML syntax -> ConfigError"""
    path = tmp_path / "invalid.yml"
    path.write_text("what: is\n    this\n- file:")
    with pytest.raises(ConfigError, match="Invalid YAML"):
        load_config(path)


def test_load_config_unknown_top_level_key(tmp_path):
    """unknown top-level key -> ConfigError (extra='forbid')"""
    path = tmp_path / "extra.yml"
    path.write_text("unknown_field: 123")
    with pytest.raises(ConfigError, match="Config validation failed"):
        load_config(path)


def test_load_config_unknown_nested_key(tmp_path):
    """unknown nested key in deployment -> ConfigError"""
    path = tmp_path / "nested.yml"
    path.write_text("deployment:\n  alien_tech: true")
    with pytest.raises(ConfigError, match="Config validation failed"):
        load_config(path)


def test_config_defaults():
    config = AqueductConfig()
    assert config.deployment.target == "local"
    assert config.deployment.master_url == "local[*]"
    assert config.stores.obs.path == ".aqueduct/obs.db"
    assert config.stores.lineage.path == ".aqueduct/lineage.db"
    assert config.stores.depot.path == ".aqueduct/depot.db"
    assert config.agent.model == "claude-sonnet-4-6"
    assert config.probes.max_sample_rows == 100
    assert config.secrets.provider == "env"
    assert config.webhooks.on_failure is None


def test_config_frozen():
    """AqueductConfig is frozen; mutation raises ValidationError"""
    config = AqueductConfig()
    with pytest.raises(ValidationError):
        config.aqueduct_config = "2.0"


def test_config_overrides(tmp_path):
    """custom master_url in config read back correctly
    partial config (only deployment section) -> other sections use defaults
    spark_config dict entries preserved in returned config
    """
    path = tmp_path / "override.yml"
    data = {
        "deployment": {"master_url": "spark://host:7077"},
        "spark_config": {"spark.driver.memory": "2g"}
    }
    path.write_text(yaml.dump(data))
    config = load_config(path)
    
    # Custom read back
    assert config.deployment.master_url == "spark://host:7077"
    
    # Partial fallback
    assert config.deployment.target == "local"
    assert config.stores.depot.backend == "duckdb"
    
    # Dict preserved
    assert config.spark_config == {"spark.driver.memory": "2g"}
