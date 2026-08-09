"""Unit tests for ``aqueduct/executor/session_config.py``.

Phase 82 remediation (`engine:` config generalization): before this fix,
`resolve_session_engine_config` only ever folded a Blueprint-level override
into the session for Spark (`{**cfg.engine.spark.conf,
**manifest.spark_config}`) — every OTHER registered engine got ONLY its
`aqueduct.yml`-level config, with no way for a Blueprint's `engine.<name>:`
block to override it. `Manifest.engine_config: dict[str, dict[str, Any]]`
(the renamed, now per-engine `spark_config` carrier) fixes that: every
engine's Blueprint-level entry now layers over its `aqueduct.yml` config,
Blueprint wins, the same way Spark's always did. These tests pin that
precedence for BOTH registered engines and the "no Blueprint override"
fallback case.
"""

from __future__ import annotations

import pytest

from aqueduct.compiler.models import Manifest
from aqueduct.config import AqueductConfig, DuckDBEngineConfig, EngineConfig, SparkEngineConfig
from aqueduct.executor.session_config import (
    resolve_session_engine_config,
    session_secrets_options,
)

pytestmark = pytest.mark.unit


def _manifest(engine_config: dict[str, dict] | None = None, base_dir: str = "") -> Manifest:
    return Manifest(
        blueprint_id="bp",
        context={},
        modules=(),
        edges=(),
        engine_config=engine_config or {},
        base_dir=base_dir,
    )


class TestSparkPrecedence:
    """Spark's own precedent — must keep working exactly as before."""

    def test_blueprint_wins_over_aqueduct_yml(self):
        cfg = AqueductConfig(
            engine=EngineConfig(
                spark=SparkEngineConfig(conf={"spark.sql.shuffle.partitions": "100"})
            )
        )
        manifest = _manifest({"spark": {"spark.sql.shuffle.partitions": "200"}})
        result = resolve_session_engine_config(cfg, "spark", manifest)
        assert result["spark.sql.shuffle.partitions"] == "200"

    def test_aqueduct_yml_used_when_no_blueprint_override(self):
        cfg = AqueductConfig(
            engine=EngineConfig(
                spark=SparkEngineConfig(conf={"spark.sql.shuffle.partitions": "100"})
            )
        )
        manifest = _manifest({})
        result = resolve_session_engine_config(cfg, "spark", manifest)
        assert result["spark.sql.shuffle.partitions"] == "100"

    def test_disjoint_keys_merge_from_both_sources(self):
        cfg = AqueductConfig(engine=EngineConfig(spark=SparkEngineConfig(conf={"a": "1"})))
        manifest = _manifest({"spark": {"b": "2"}})
        result = resolve_session_engine_config(cfg, "spark", manifest)
        assert result == {"a": "1", "b": "2"}


class TestDuckDBPrecedence:
    """The defect this phase fixes: DuckDB used to get NO Blueprint-level
    override at all — only its `aqueduct.yml`-level `engine.duckdb:` config
    ever reached a session."""

    def test_blueprint_wins_over_aqueduct_yml(self):
        cfg = AqueductConfig(
            engine=EngineConfig(duckdb=DuckDBEngineConfig(memory_limit="4GB", threads=2))
        )
        manifest = _manifest({"duckdb": {"memory_limit": "8GB"}})
        result = resolve_session_engine_config(cfg, "duckdb", manifest)
        assert result["memory_limit"] == "8GB"
        # The untouched aqueduct.yml-level field survives the merge.
        assert result["threads"] == 2

    def test_no_blueprint_override_still_gets_aqueduct_yml_config(self):
        """An engine with no Blueprint-level `engine.duckdb:` block must
        still resolve its `aqueduct.yml` config — the pre-fix behavior for
        non-Spark engines, which must not regress."""
        cfg = AqueductConfig(
            engine=EngineConfig(duckdb=DuckDBEngineConfig(memory_limit="4GB", threads=2))
        )
        manifest = _manifest({})
        result = resolve_session_engine_config(cfg, "duckdb", manifest)
        assert result["memory_limit"] == "4GB"
        assert result["threads"] == 2

    def test_empty_blueprint_duckdb_block_is_a_no_op_override(self):
        """DuckDB's Blueprint-level schema block carries no fields yet
        (`DuckDBEngineBlockSchema`) — the parser always resolves it to `{}`.
        An empty override must not erase the aqueduct.yml-level config."""
        cfg = AqueductConfig(engine=EngineConfig(duckdb=DuckDBEngineConfig(memory_limit="4GB")))
        manifest = _manifest({"duckdb": {}})
        result = resolve_session_engine_config(cfg, "duckdb", manifest)
        assert result["memory_limit"] == "4GB"


class TestUnregisteredEngineOnConfig:
    def test_engine_absent_from_cfg_engine_returns_empty_dict(self):
        cfg = AqueductConfig()
        manifest = _manifest({"flink": {"some.key": "value"}})
        result = resolve_session_engine_config(cfg, "flink", manifest)
        assert result == {}


class TestSessionSecretsOptions:
    def test_carries_secrets_block_and_manifest_base_dir(self):
        cfg = AqueductConfig()
        manifest = _manifest(base_dir="/blueprints/x")
        opts = session_secrets_options(cfg, manifest)
        assert opts["secrets"]["provider"] == cfg.secrets.provider
        assert opts["secrets"]["region"] == cfg.secrets.region
        assert opts["secrets"]["resolver"] == cfg.secrets.resolver
        assert opts["secrets"]["base_dir"] == "/blueprints/x"
