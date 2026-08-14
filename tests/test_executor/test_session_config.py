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
    session_config_fingerprint,
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


class TestSessionConfigFingerprint:
    """Unit coverage for the cross-engine-remediation fingerprint: the
    ``aqueduct/cli/run.py`` session-rebuild-on-mismatch check (see
    ``tests/test_cli/test_cli_run_heal_session_rebuild.py`` for the
    CLI-level seam tests) is only correct if this function is (a)
    deterministic for an unchanged input and (b) actually sensitive to
    every way ``resolve_session_engine_config``'s output can change."""

    def test_deterministic_for_the_same_input(self):
        cfg = AqueductConfig(
            engine=EngineConfig(
                spark=SparkEngineConfig(conf={"spark.sql.shuffle.partitions": "100"})
            )
        )
        manifest = _manifest({"spark": {"a": "1"}})
        fp1 = session_config_fingerprint(cfg, "spark", manifest)
        fp2 = session_config_fingerprint(cfg, "spark", manifest)
        assert fp1 == fp2

    def test_key_order_does_not_affect_the_fingerprint(self):
        """A dict built with keys in a different order must still fingerprint
        identically — the whole point of a canonical (sorted-key) encoding."""
        cfg = AqueductConfig()
        manifest_a = _manifest({"spark": {"a": "1", "b": "2"}})
        manifest_b = _manifest({"spark": {"b": "2", "a": "1"}})
        assert session_config_fingerprint(cfg, "spark", manifest_a) == session_config_fingerprint(
            cfg, "spark", manifest_b
        )

    def test_changed_blueprint_engine_config_changes_the_fingerprint(self):
        """The property the rebuild-on-mismatch check depends on: a
        ``set_engine_config``-shaped change to ``Manifest.engine_config``
        (the only thing that can differ between two manifests compiled from
        the same ``aqueduct.yml`` within one run) must change the
        fingerprint."""
        cfg = AqueductConfig(
            engine=EngineConfig(
                spark=SparkEngineConfig(conf={"spark.sql.shuffle.partitions": "100"})
            )
        )
        original = _manifest({})
        patched = _manifest({"spark": {"spark.sql.shuffle.partitions": "999"}})
        assert session_config_fingerprint(cfg, "spark", original) != session_config_fingerprint(
            cfg, "spark", patched
        )

    def test_unchanged_blueprint_engine_config_reproduces_the_original_fingerprint(self):
        """The "free when unchanged" property: a manifest with NO
        Blueprint-level override reproduces the exact fingerprint of another
        manifest with no override, even if they are different Manifest
        objects (e.g. the original vs. a re-parsed copy of the same
        blueprint) — the case that must NOT trigger a session rebuild."""
        cfg = AqueductConfig(
            engine=EngineConfig(
                spark=SparkEngineConfig(conf={"spark.sql.shuffle.partitions": "100"})
            )
        )
        manifest_1 = _manifest({})
        manifest_2 = _manifest({})
        assert session_config_fingerprint(cfg, "spark", manifest_1) == session_config_fingerprint(
            cfg, "spark", manifest_2
        )

    def test_changed_aqueduct_yml_engine_conf_changes_the_fingerprint(self):
        cfg_a = AqueductConfig(engine=EngineConfig(spark=SparkEngineConfig(conf={"a": "1"})))
        cfg_b = AqueductConfig(engine=EngineConfig(spark=SparkEngineConfig(conf={"a": "2"})))
        manifest = _manifest({})
        assert session_config_fingerprint(cfg_a, "spark", manifest) != session_config_fingerprint(
            cfg_b, "spark", manifest
        )


class TestSessionSecretsOptions:
    def test_carries_secrets_block_and_manifest_base_dir(self):
        cfg = AqueductConfig()
        manifest = _manifest(base_dir="/blueprints/x")
        opts = session_secrets_options(cfg, manifest)
        assert opts["secrets"]["provider"] == cfg.secrets.provider
        assert opts["secrets"]["region"] == cfg.secrets.region
        assert opts["secrets"]["resolver"] == cfg.secrets.resolver
        assert opts["secrets"]["base_dir"] == "/blueprints/x"
