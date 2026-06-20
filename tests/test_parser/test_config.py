"""Tests for engine configuration loader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
pytestmark = pytest.mark.unit
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
    with pytest.raises(ConfigError, match="validation error"):
        load_config(path)


def test_load_config_unknown_nested_key(tmp_path):
    """unknown nested key in deployment -> ConfigError"""
    path = tmp_path / "nested.yml"
    path.write_text("deployment:\n  alien_tech: true")
    with pytest.raises(ConfigError, match="validation error"):
        load_config(path)

def test_load_config_unknown_stores_key(tmp_path):
    """unknown key in stores -> ConfigError"""
    path = tmp_path / "stores_extra.yml"
    path.write_text("stores:\n  ghost: {path: ./obs.db}")
    with pytest.raises(ConfigError, match="validation error"):
        load_config(path)

def test_config_defaults():
    config = AqueductConfig()
    assert config.deployment.target == "local"
    assert config.deployment.master_url == "local[*]"
    assert config.stores.observability.path == ".aqueduct/observability.db"
    assert not hasattr(config.stores, "lineage")  # removed — merged into observability
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


def test_webhook_config_defaults():
    config = AqueductConfig()
    assert config.webhooks.on_success is None
    assert config.webhooks.on_failure is None


def test_webhook_config_coercion(tmp_path):
    path = tmp_path / "webhooks.yml"
    data = {
        "webhooks": {
            "on_success": "http://api.test/success"
        }
    }
    path.write_text(yaml.dump(data))
    config = load_config(path)
    assert config.webhooks.on_success.url == "http://api.test/success"
    assert config.webhooks.on_success.method == "POST"

def test_load_config_postgres_missing_driver(tmp_path, monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    monkeypatch.setitem(sys.modules, "psycopg2.pool", None)
    
    path = tmp_path / "aq_pg.yml"
    path.write_text("stores:\n  observability: {backend: postgres, path: postgresql://localhost/aq}")
    
    from aqueduct.config import ConfigError
    with pytest.raises(ConfigError, match="psycopg2"):
        load_config(path)

def test_load_config_redis_missing_driver(tmp_path, monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, "redis", None)
    
    path = tmp_path / "aq_redis.yml"
    path.write_text("stores:\n  depot: {backend: redis, path: redis://localhost}")
    
    from aqueduct.config import ConfigError
    with pytest.raises(ConfigError, match="redis"):
        load_config(path)

def test_load_config_duckdb_lazy_imports(tmp_path, monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    monkeypatch.setitem(sys.modules, "redis", None)
    
    path = tmp_path / "aq_duck.yml"
    path.write_text("stores:\n  observability: {backend: duckdb, path: obs.db}")
    
    cfg = load_config(path)
    assert cfg.stores.observability.backend == "duckdb"

def test_metrics_config_parsing(tmp_path):
    """MetricsConfig parses use_observe: true and use_observe: false without error"""
    path = tmp_path / "metrics.yml"
    
    # Test true
    path.write_text("metrics:\n  use_observe: true")
    cfg = load_config(path)
    assert cfg.metrics.use_observe is True
    
    # Test false
    path.write_text("metrics:\n  use_observe: false")
    cfg = load_config(path)
    assert cfg.metrics.use_observe is False

def test_metrics_config_extra_keys_forbidden(tmp_path):
    """MetricsConfig rejects extra keys (extra="forbid" raises ValidationError via ConfigError)"""
    path = tmp_path / "metrics_extra.yml"
    path.write_text("metrics:\n  use_observe: true\n  unknown: 1")
    with pytest.raises(ConfigError, match="validation error"):
        load_config(path)

def test_deployment_config_literal_validation(tmp_path):
    """DeploymentConfig fields (engine, target, env) reject invalid Literal values"""
    path = tmp_path / "invalid_lit.yml"
    
    # Invalid engine
    path.write_text("deployment:\n  engine: turbo-pascal")
    with pytest.raises(ConfigError, match="validation error"):
        load_config(path)
        
    # Invalid target
    path.write_text("deployment:\n  target: the-moon")
    with pytest.raises(ConfigError, match="validation error"):
        load_config(path)
        
    # Invalid env
    path.write_text("deployment:\n  env: void")
    with pytest.raises(ConfigError, match="validation error"):
        load_config(path)


# ── Two-pass Secrets Loading tests ───────────────────────────────────────────

def test_load_config_no_secrets(tmp_path):
    """no @aq.secret() tokens -> single-pass load (one YAML parse, one validation); secrets.provider: env default applies"""
    path = tmp_path / "no_secrets.yml"
    path.write_text("deployment:\n  target: local")
    cfg = load_config(path)
    assert cfg.secrets.provider == "env"


def test_load_config_secret_resolved_env(monkeypatch, tmp_path):
    """@aq.secret('KEY') with provider: env, env var set -> resolved to env value; appears in final cfg"""
    monkeypatch.setenv("MY_SECRET_KEY", "super-secret-value-12345")
    from aqueduct import redaction
    redaction.clear()

    path = tmp_path / "secret_env.yml"
    path.write_text("spark_config:\n  spark.password: \"@aq.secret('MY_SECRET_KEY')\"")
    cfg = load_config(path)
    assert cfg.spark_config["spark.password"] == "super-secret-value-12345"
    assert redaction.is_registered("super-secret-value-12345")


def test_load_config_secret_unresolved_env(tmp_path):
    """@aq.secret('KEY') with provider: env, env var unset -> ConfigError listing @aq.secret('KEY') as unresolved"""
    import os
    if "MY_UNSET_KEY" in os.environ:
        del os.environ["MY_UNSET_KEY"]

    path = tmp_path / "secret_env_unset.yml"
    path.write_text("spark_config:\n  spark.password: \"@aq.secret('MY_UNSET_KEY')\"")
    with pytest.raises(ConfigError, match=r"Unresolved secrets.*MY_UNSET_KEY"):
        load_config(path)


def test_load_config_secret_resolved_aws(tmp_path):
    """@aq.secret('KEY') with provider: aws (mocked boto3) -> calls _fetch_aws, resolved value lands in config"""
    import importlib.util

    path = tmp_path / "secret_aws.yml"
    path.write_text(
        "secrets:\n"
        "  provider: aws\n"
        "spark_config:\n"
        "  spark.password: \"@aq.secret('MY_AWS_KEY')\""
    )

    # importlib.util is imported inline inside _validate_secrets_backend, so
    # patch it at its canonical location.
    real_find_spec = importlib.util.find_spec
    def _fake_find_spec(name, *args, **kwargs):
        if name == "boto3":
            return MagicMock()  # truthy → SDK "present"
        return real_find_spec(name, *args, **kwargs)

    with patch("importlib.util.find_spec", side_effect=_fake_find_spec), \
         patch("aqueduct.secrets.resolve_secret", return_value="aws-secret-value-12345"):
        cfg = load_config(path)
    assert cfg.spark_config["spark.password"] == "aws-secret-value-12345"


def test_load_config_secret_aws_sdk_missing(tmp_path, monkeypatch):
    """@aq.secret('KEY') with provider: aws and boto3 NOT installed -> ConfigError at pass-1 before pass-2 dispatch"""
    import sys
    monkeypatch.setitem(sys.modules, "boto3", None)

    path = tmp_path / "secret_aws_no_sdk.yml"
    path.write_text(
        "secrets:\n"
        "  provider: aws\n"
        "spark_config:\n"
        "  spark.password: \"@aq.secret('MY_AWS_KEY')\""
    )

    with pytest.raises(ConfigError, match="boto3"):
        load_config(path)


def test_load_config_env_provider_resolution(monkeypatch, tmp_path):
    """${VAR} in secrets.provider: ${PROVIDER} resolves first; pass 2 then uses the resolved provider"""
    monkeypatch.setenv("CHOSEN_PROVIDER", "aws")

    import importlib.util

    path = tmp_path / "secret_provider_env.yml"
    path.write_text(
        "secrets:\n"
        "  provider: ${CHOSEN_PROVIDER}\n"
        "spark_config:\n"
        "  spark.password: \"@aq.secret('MY_AWS_KEY')\""
    )

    real_find_spec = importlib.util.find_spec
    def _fake_find_spec(name, *args, **kwargs):
        if name == "boto3":
            return MagicMock()  # truthy → SDK "present"
        return real_find_spec(name, *args, **kwargs)

    with patch("importlib.util.find_spec", side_effect=_fake_find_spec), \
         patch("aqueduct.secrets.resolve_secret", return_value="aws-resolved-value"):
        cfg = load_config(path)
    assert cfg.secrets.provider == "aws"
    assert cfg.spark_config["spark.password"] == "aws-resolved-value"


def test_load_config_pass2_invalid_yaml(monkeypatch, tmp_path):
    """pass-2 YAML re-validation runs after secret expansion — invalid YAML produced by an exotic resolved value raises ConfigError"""
    # An exotic resolved value that results in invalid YAML (e.g. producing unindented mapping or syntax error)
    monkeypatch.setenv("BAD_YAML_SECRET", "\n  invalid: - : : oops")

    path = tmp_path / "secret_bad_yaml.yml"
    path.write_text("spark_config:\n  spark.password: \"@aq.secret('BAD_YAML_SECRET')\"")
    with pytest.raises(ConfigError, match="after secret expansion"):
        load_config(path)


def test_load_config_pass2_registers_redaction(monkeypatch, tmp_path):
    """resolved @aq.secret() values are registered with aqueduct.redaction.register() after pass 2"""
    monkeypatch.setenv("SECRET_TO_REGISTER", "reg-secret-999999")
    from aqueduct import redaction
    redaction.clear()

    path = tmp_path / "secret_register.yml"
    path.write_text("spark_config:\n  spark.password: \"@aq.secret('SECRET_TO_REGISTER')\"")
    
    load_config(path)
    assert redaction.is_registered("reg-secret-999999")

