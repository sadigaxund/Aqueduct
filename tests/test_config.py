"""Unit tests for aqueduct/config.py."""

from __future__ import annotations

import pytest
import yaml

pytestmark = pytest.mark.unit

# Imports deliberately follow the `pytestmark` assignment above so the marker is
# established before the module under test is imported.
from aqueduct.config import (  # noqa: E402
    AgentConnectionConfig,
    AqueductConfig,
    GitConfig,
    ObservabilityConfig,
    ObservabilityRetentionConfig,
    PrConfig,
    WebhookEndpointConfig,
    WebhooksConfig,
    load_config,
)


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
        cfg_data = {"agent": {"timeout": 300.5, "max_reprompts": 5}}
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

        from aqueduct import AqueductWarning
        from aqueduct.config import StoresConfig

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            StoresConfig(**stores_dict)
            return [
                x for x in w if issubclass(x.category, AqueductWarning) and "blob" in str(x.message)
            ]

    def test_remote_obs_implicit_local_blob_warns(self):
        assert self._warns({"observability": {"backend": "postgres", "path": "postgresql://x/y"}})

    def test_explicit_local_blob_is_silent(self):
        assert not self._warns(
            {
                "observability": {"backend": "postgres", "path": "postgresql://x/y"},
                "blob": {"backend": "local"},
            }
        )

    def test_duckdb_default_is_silent(self):
        assert not self._warns({})

    def test_remote_obs_remote_blob_is_silent(self):
        assert not self._warns(
            {
                "observability": {"backend": "postgres", "path": "postgresql://x/y"},
                "blob": {"backend": "s3", "path": "s3://b/k"},
            }
        )


class TestConfigAqGuard:
    """aqueduct.yml resolves only ${ENV} + @aq.secret(); other @aq.* is rejected."""

    def _write(self, tmp_path, body):
        p = tmp_path / "aqueduct.yml"
        p.write_text(body, encoding="utf-8")
        return p

    def test_non_secret_aq_in_config_rejected(self, tmp_path):
        from aqueduct.config import ConfigError

        p = self._write(
            tmp_path,
            'stores:\n  depots:\n    default:\n      path: ".aqueduct/@aq.blueprint.id().db"\n',
        )
        with pytest.raises(
            ConfigError, match=r"@aq\.blueprint\.id cannot be used in aqueduct\.yml"
        ):
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

        p = self._write(tmp_path, "deployment:\n  engine: flink\n")
        with pytest.raises(ConfigError, match=r"not a registered engine"):
            load_config(p)

    def test_env_and_plain_config_ok(self, tmp_path, monkeypatch):
        monkeypatch.setenv("AQ_CFG_ENV", "cluster")
        p = self._write(tmp_path, "deployment:\n  env: ${AQ_CFG_ENV}\n")
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

    @pytest.mark.parametrize(
        "uri",
        [
            "s3://bucket/checkpoints",
            "s3a://bucket/checkpoints",
            "gs://bucket/checkpoints",
            "hdfs://namenode/checkpoints",
            "abfss://container@acct.dfs.core.windows.net/checkpoints",
        ],
    )
    def test_remote_uri_scheme_rejected(self, uri):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="checkpoint_root"):
            AqueductConfig(checkpoint_root=uri)

    def test_remote_uri_error_names_local_fallback(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="local path"):
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

    @pytest.mark.parametrize(
        "uri",
        [
            "s3://bucket/db.duckdb",
            "gs://bucket/db.duckdb",
            "abfss://container@acct.dfs.core.windows.net/db.duckdb",
        ],
    )
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


class TestAqueductConfigEqHash:
    """Phase 84 item 3 — ``_cli_engine_overrides`` (a PrivateAttr) must not
    leak into ``AqueductConfig.__eq__``/``__hash__``. Pydantic v2's
    generated ``__eq__`` compares ``__pydantic_private__`` before fields, so
    two otherwise-identical configs differing only by their per-invocation
    ``-s/--set`` layer used to compare unequal. Closing a latent trap — no
    shipped call site compares AqueductConfig instances today."""

    def test_cli_engine_overrides_does_not_break_equality(self):
        cfg = AqueductConfig()
        overridden = cfg.with_cli_engine_overrides(
            {"spark": {"spark.sql.shuffle.partitions": "10"}}
        )
        assert overridden.cli_engine_overrides != cfg.cli_engine_overrides
        assert overridden == cfg

    def test_field_differences_still_compare_unequal(self):
        assert AqueductConfig() != AqueductConfig(timezone="UTC")

    def test_hash_matches_across_differing_cli_overrides(self):
        cfg = AqueductConfig()
        overridden = cfg.with_cli_engine_overrides({"duckdb": {"threads": "4"}})
        assert hash(cfg) == hash(overridden)

    def test_hash_differs_for_field_differences(self):
        assert hash(AqueductConfig()) != hash(AqueductConfig(timezone="UTC"))

    def test_config_is_usable_as_a_set_or_dict_key(self):
        cfg = AqueductConfig()
        overridden = cfg.with_cli_engine_overrides({"spark": {"k": "v"}})
        seen = {cfg, overridden}
        assert len(seen) == 1  # equal + same hash → collapse to one entry


class TestObservabilityRetentionConfig:
    """Phase 85 B1 — observability.retention: config block."""

    def test_defaults_match_documented_windows(self):
        cfg = ObservabilityRetentionConfig()
        assert cfg.run_records_days == 90
        assert cfg.failure_contexts_days == 90
        assert cfg.healing_outcomes_days == 180
        assert cfg.heal_attempts_days == 180
        assert cfg.patch_simulation_days == 90
        assert cfg.column_lineage_days == 90
        assert cfg.probe_signals_days == 90
        assert cfg.sample_rows_keep_last_n == 20

    def test_aqueduct_config_wires_observability_block_with_defaults(self):
        cfg = AqueductConfig()
        assert isinstance(cfg.observability, ObservabilityConfig)
        assert cfg.observability.retention == ObservabilityRetentionConfig()

    def test_load_config_respects_custom_retention_values(self, tmp_path):
        cfg_path = tmp_path / "aqueduct.yml"
        cfg_data = {
            "observability": {
                "retention": {
                    "run_records_days": 30,
                    "sample_rows_keep_last_n": 5,
                }
            }
        }
        cfg_path.write_text(yaml.dump(cfg_data))

        config = load_config(cfg_path)
        assert config.observability.retention.run_records_days == 30
        assert config.observability.retention.sample_rows_keep_last_n == 5
        # untouched fields keep their defaults
        assert config.observability.retention.heal_attempts_days == 180

    def test_rejects_zero_or_negative_days(self):
        with pytest.raises(Exception):
            ObservabilityRetentionConfig(run_records_days=0)
        with pytest.raises(Exception):
            ObservabilityRetentionConfig(sample_rows_keep_last_n=-1)

    def test_rejects_unknown_key(self):
        with pytest.raises(Exception):
            ObservabilityRetentionConfig(not_a_real_field=1)

    def test_frozen(self):
        cfg = ObservabilityRetentionConfig()
        with pytest.raises(Exception):
            cfg.run_records_days = 5  # type: ignore[misc]


class TestWebhooksOnDefer:
    """Phase 88 Domain 6 — dedicated on_defer webhook event."""

    def test_on_defer_defaults_to_none(self):
        cfg = WebhooksConfig()
        assert cfg.on_defer is None

    def test_on_defer_accepts_string_url_shorthand(self):
        # Same coerce_string_url validator as on_failure/on_success/etc.
        cfg = WebhooksConfig(on_defer="https://hooks.example.com/defer")
        assert isinstance(cfg.on_defer, WebhookEndpointConfig)
        assert cfg.on_defer.url == "https://hooks.example.com/defer"

    def test_on_defer_accepts_full_config(self):
        cfg = WebhooksConfig(on_defer={"url": "https://hooks.example.com/defer", "max_retries": 3})
        assert cfg.on_defer.url == "https://hooks.example.com/defer"
        assert cfg.on_defer.max_retries == 3

    def test_load_config_respects_on_defer(self, tmp_path):
        cfg_path = tmp_path / "aqueduct.yml"
        cfg_data = {"webhooks": {"on_defer": "https://hooks.example.com/defer"}}
        cfg_path.write_text(yaml.dump(cfg_data))

        config = load_config(cfg_path)
        assert config.webhooks.on_defer.url == "https://hooks.example.com/defer"

    def test_config_leaves_walker_accepts_on_defer_without_capability_scope_error(self):
        """`on_defer: WebhookEndpointConfig | None` is a nested-BaseModel field
        — the walker recurses INTO `WebhookEndpointConfig`'s own tagged
        fields rather than requiring a tag on `on_defer` itself (same as its
        three siblings on_failure/on_success/on_patch_pending).
        This must not raise CapabilityScopeError."""
        from aqueduct.executor.config_leaves import all_config_leaves

        leaves = all_config_leaves()
        # WebhookEndpointConfig's own leaves (e.g. url/timeout) appear once,
        # not once per parent field — proving on_defer recursed cleanly
        # rather than becoming its own untagged leaf.
        assert "config.webhooks.on_defer" not in leaves
        assert "config.webhooks.on_defer.url" not in leaves


class TestGitAndPrConfig:
    """Phase 87 — `git:`/`pr:` blocks backing `aqueduct patch pr`."""

    def test_git_config_defaults(self):
        cfg = GitConfig()
        assert cfg.expected_root is None
        assert cfg.remote == "origin"

    def test_pr_config_defaults(self):
        cfg = PrConfig()
        assert cfg.base_branch == "main"
        assert cfg.draft is False
        assert "{patch_id}" in cfg.title_template
        assert "{blueprint_id}" in cfg.title_template

    def test_aqueduct_config_carries_git_and_pr_blocks(self):
        cfg = AqueductConfig()
        assert isinstance(cfg.git, GitConfig)
        assert isinstance(cfg.pr, PrConfig)

    def test_git_config_extra_forbid(self):
        with pytest.raises(Exception):
            GitConfig(nonexistent_field="x")

    def test_pr_config_has_no_labels_or_reviewers_fields(self):
        """Ratified 2026-08-24 design audit item 3: reviewer routing belongs
        to CODEOWNERS/branch protection, never to a healed pipeline's own
        config. A `labels`/`reviewers` field here would let a Blueprint (or
        an upstream-data-steered heal) influence who reviews its own fix."""
        assert "labels" not in PrConfig.model_fields
        assert "reviewers" not in PrConfig.model_fields

    def test_load_config_respects_git_and_pr_blocks(self, tmp_path):
        cfg_path = tmp_path / "aqueduct.yml"
        cfg_data = {
            "git": {"expected_root": str(tmp_path), "remote": "upstream"},
            "pr": {"base_branch": "develop", "draft": True, "title_template": "heal {patch_id}"},
        }
        cfg_path.write_text(yaml.dump(cfg_data))

        config = load_config(cfg_path)
        assert config.git.expected_root == str(tmp_path)
        assert config.git.remote == "upstream"
        assert config.pr.base_branch == "develop"
        assert config.pr.draft is True
        assert config.pr.title_template == "heal {patch_id}"

    def test_config_leaves_walker_accepts_git_and_pr_blocks(self):
        """Every field carries an explicit `engine_scoped` tag (connection-
        level, not engine-scoped) — must not raise CapabilityScopeError."""
        from aqueduct.executor.config_leaves import all_config_leaves

        leaves = all_config_leaves()
        assert "config.git.remote" not in leaves  # engine_scoped=False, excluded
        assert "config.pr.base_branch" not in leaves
