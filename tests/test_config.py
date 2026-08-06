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
            'stores:\n  depots:\n    default:\n      path: ".aqueduct/@aq.blueprint.id().db"\n')
        with pytest.raises(ConfigError, match=r"@aq\.blueprint\.id cannot be used in aqueduct\.yml"):
            load_config(p)

    def test_run_scope_in_config_rejected(self, tmp_path):
        from aqueduct.config import ConfigError
        p = self._write(tmp_path, 'engine:\n  spark:\n    master_url: "@aq.run.id()"\n')
        with pytest.raises(ConfigError, match=r"@aq\.run\.id"):
            load_config(p)

    def test_engine_flink_rejected(self, tmp_path):
        """flink is not a registered engine (Phase 78 Step 1 — engine portfolio is
        spark + duckdb; flink is out of scope, not a special-cased literal)."""
        from aqueduct.config import ConfigError
        p = self._write(tmp_path, 'deployment:\n  engine: flink\n')
        with pytest.raises(ConfigError, match=r"not a registered engine"):
            load_config(p)

    def test_env_and_plain_config_ok(self, tmp_path, monkeypatch):
        monkeypatch.setenv("AQ_CFG_ENV", "cluster")
        p = self._write(tmp_path, 'deployment:\n  env: ${AQ_CFG_ENV}\n')
        cfg = load_config(p)
        assert cfg.deployment.env == "cluster"


class TestCheckpointRoot:
    """checkpoint_root — local-path-only engine-config override (Phase 70)."""

    def test_default_is_none(self):
        assert AqueductConfig().checkpoint_root is None

    def test_local_path_accepted(self):
        cfg = AqueductConfig(checkpoint_root="/mnt/fast/checkpoints")
        assert cfg.checkpoint_root == "/mnt/fast/checkpoints"

    def test_relative_local_path_accepted(self):
        cfg = AqueductConfig(checkpoint_root="my/checkpoints")
        assert cfg.checkpoint_root == "my/checkpoints"

    @pytest.mark.parametrize("uri", [
        "s3://bucket/checkpoints",
        "s3a://bucket/checkpoints",
        "gs://bucket/checkpoints",
        "hdfs://namenode/checkpoints",
        "abfss://container@acct.dfs.core.windows.net/checkpoints",
    ])
    def test_remote_uri_scheme_rejected(self, uri):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="checkpoint_root"):
            AqueductConfig(checkpoint_root=uri)

    def test_remote_uri_error_points_at_roadmap(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="roadmap"):
            AqueductConfig(checkpoint_root="s3a://bucket/checkpoints")

    def test_load_config_yaml_key(self, tmp_path):
        p = tmp_path / "aqueduct.yml"
        p.write_text('checkpoint_root: "/mnt/shared/ckpts"\n', encoding="utf-8")
        cfg = load_config(p)
        assert cfg.checkpoint_root == "/mnt/shared/ckpts"

    def test_load_config_yaml_remote_uri_rejected(self, tmp_path):
        from aqueduct.config import ConfigError
        p = tmp_path / "aqueduct.yml"
        p.write_text('checkpoint_root: "s3a://bucket/ckpts"\n', encoding="utf-8")
        with pytest.raises(ConfigError, match="checkpoint_root"):
            load_config(p)


class TestDuckDBEngineConfig:
    """``engine.duckdb.*`` — memory_limit/threads/database_path/
    extension_repository/s3_* (the DuckDB config-surface + httpfs task).
    Every field carries a real consumer in
    ``duckdb_/engine.py::_make_session`` — see
    ``tests/test_executor_duckdb/test_engine_config.py`` for the
    live-DuckDB wiring proofs; this class covers pure pydantic validation."""

    def test_defaults_are_all_none(self):
        from aqueduct.config import DuckDBEngineConfig
        cfg = DuckDBEngineConfig()
        assert cfg.memory_limit is None
        assert cfg.threads is None
        assert cfg.database_path is None
        assert cfg.extension_repository is None
        assert cfg.s3_key_id_secret is None
        assert cfg.s3_secret_access_key_secret is None
        assert cfg.s3_region is None

    def test_threads_must_be_positive(self):
        from pydantic import ValidationError

        from aqueduct.config import DuckDBEngineConfig
        with pytest.raises(ValidationError):
            DuckDBEngineConfig(threads=0)

    @pytest.mark.parametrize("uri", [
        "s3://bucket/db.duckdb",
        "gs://bucket/db.duckdb",
        "abfss://container@acct.dfs.core.windows.net/db.duckdb",
    ])
    def test_database_path_rejects_remote_uri_scheme(self, uri):
        from pydantic import ValidationError

        from aqueduct.config import DuckDBEngineConfig
        with pytest.raises(ValidationError, match="database_path"):
            DuckDBEngineConfig(database_path=uri)

    def test_database_path_accepts_local_path(self):
        from aqueduct.config import DuckDBEngineConfig
        cfg = DuckDBEngineConfig(database_path="/mnt/fast/db.duckdb")
        assert cfg.database_path == "/mnt/fast/db.duckdb"

    def test_s3_credential_pair_required_together(self):
        from pydantic import ValidationError

        from aqueduct.config import DuckDBEngineConfig
        with pytest.raises(ValidationError, match="TOGETHER"):
            DuckDBEngineConfig(s3_key_id_secret="AWS_ACCESS_KEY_ID")
        with pytest.raises(ValidationError, match="TOGETHER"):
            DuckDBEngineConfig(s3_secret_access_key_secret="AWS_SECRET_ACCESS_KEY")

    def test_s3_credential_pair_accepted_together(self):
        from aqueduct.config import DuckDBEngineConfig
        cfg = DuckDBEngineConfig(
            s3_key_id_secret="AWS_ACCESS_KEY_ID",
            s3_secret_access_key_secret="AWS_SECRET_ACCESS_KEY",
            s3_region="us-east-1",
        )
        assert cfg.s3_key_id_secret == "AWS_ACCESS_KEY_ID"

    def test_load_config_yaml_engine_duckdb_block(self, tmp_path):
        p = tmp_path / "aqueduct.yml"
        p.write_text(
            "deployment:\n"
            "  engine: duckdb\n"
            "engine:\n"
            "  duckdb:\n"
            "    memory_limit: '4GB'\n"
            "    threads: 4\n"
            "    database_path: '/mnt/fast/db.duckdb'\n",
            encoding="utf-8",
        )
        cfg = load_config(p)
        assert cfg.engine.duckdb.memory_limit == "4GB"
        assert cfg.engine.duckdb.threads == 4
        assert cfg.engine.duckdb.database_path == "/mnt/fast/db.duckdb"

    def test_engine_duckdb_leaves_engine_scoped_to_duckdb_only(self):
        """``config.engine.duckdb.*`` leaves must appear ONLY in DuckDB's
        own capability checklist, never Spark's (Q4 step 2 positional
        scoping)."""
        from aqueduct.executor.config_leaves import all_config_leaves

        duckdb_leaves = all_config_leaves(engine="duckdb")
        spark_leaves = all_config_leaves(engine="spark")
        new_leaves = {
            "config.engine.duckdb.memory_limit",
            "config.engine.duckdb.threads",
            "config.engine.duckdb.database_path",
            "config.engine.duckdb.extension_repository",
            "config.engine.duckdb.s3_key_id_secret",
            "config.engine.duckdb.s3_secret_access_key_secret",
            "config.engine.duckdb.s3_region",
        }
        assert new_leaves <= duckdb_leaves
        assert not (new_leaves & spark_leaves)


class TestValidateStoreBackendsCoverage:
    """Audit triage (2026-08): `_validate_store_backends` fail-fast only
    checked observability + the DEFAULT depot mount — an extra named depot
    mount (`depots.<name>`) or the benchmark store's own backend loaded
    cleanly with a missing SDK and only died with a bare ImportError at
    first real use, mid-run, instead of the ConfigError every other store
    backend gets at load. Covers both previously-unchecked surfaces."""

    def test_extra_depot_mount_missing_backend_sdk_raises_config_error(self, monkeypatch):
        import importlib.util as importlib_util

        from aqueduct.config import DepotMountConfig, StoresConfig, _validate_store_backends

        monkeypatch.setattr(
            importlib_util, "find_spec", lambda name: None if name == "psycopg2" else object()
        )
        stores_cfg = StoresConfig(
            depots={
                "default": DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db"),
                "fleet": DepotMountConfig(backend="postgres", path="postgresql://x/y"),
            }
        )
        with pytest.raises(Exception, match=r"depots\.fleet.*psycopg2"):
            _validate_store_backends(stores_cfg)

    def test_benchmark_store_missing_backend_sdk_raises_config_error(self, monkeypatch):
        import importlib.util as importlib_util

        from aqueduct.config import BenchmarkStoreConfig, StoresConfig, _validate_store_backends

        monkeypatch.setattr(
            importlib_util, "find_spec", lambda name: None if name == "psycopg2" else object()
        )
        stores_cfg = StoresConfig(benchmark=BenchmarkStoreConfig(backend="postgres"))
        with pytest.raises(Exception, match=r"benchmark.*psycopg2"):
            _validate_store_backends(stores_cfg)

    def test_default_depot_only_still_passes_with_no_sdk_needed(self, monkeypatch):
        """Regression guard: the common case (duckdb-only, no extra
        depots/benchmark backend) must still load with ZERO SDK checks.

        Asserts the claim the docstring actually makes — that no third-party
        SDK is probed at all — rather than only that nothing raised. A future
        change that started probing (say) psycopg2 for a duckdb-only config
        would still "not raise" here while adding a real import cost to the
        common path, so `find_spec` is recorded and required to stay unused.
        """
        import importlib.util as importlib_util

        from aqueduct.config import StoresConfig, _validate_store_backends

        probed: list[str] = []
        monkeypatch.setattr(
            importlib_util, "find_spec", lambda name: probed.append(name) or object()
        )
        _validate_store_backends(StoresConfig())
        assert probed == []
