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
    """F-5: an engine with no ``cfg.engine.<name>:`` block (a third-party BYO
    engine that is not a field on the closed ``EngineConfig`` model) must
    still get its Blueprint and ``--set`` layers — only layer 1
    (``aqueduct.yml``) is unavailable to it."""

    def test_engine_absent_from_cfg_engine_still_gets_blueprint_and_set_layers(self):
        manifest = _manifest({"flink": {"some.key": "blueprint_val", "other.key": "kept"}})
        cfg = AqueductConfig().with_cli_engine_overrides({"flink": {"some.key": "cli_val"}})
        result = resolve_session_engine_config(cfg, "flink", manifest)
        # --set wins over the Blueprint value for the key it names...
        assert result["some.key"] == "cli_val"
        # ...and an untouched Blueprint-only key survives the merge.
        assert result["other.key"] == "kept"

    def test_engine_absent_from_cfg_engine_and_no_overrides_returns_empty_dict(self):
        cfg = AqueductConfig()
        manifest = _manifest({})
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


# ── `-s/--set` is the TOP layer, above the Blueprint ─────────────────────────
#
# Before this, `--set` overlaid the `aqueduct.yml` layer only, so a value a
# heal had written into the Blueprint's `engine.<name>:` block months earlier
# beat the flag the user typed thirty seconds ago. A CLI flag is the most
# explicit statement a user can make about one run and was silently the
# weakest input in the merge. It is now a genuine third layer, expressed once
# in `resolve_session_engine_config` — never by mutating a lower one.


def _cfg_with_set(*items: str) -> AqueductConfig:
    """The real routing + overlay path a CLI invocation takes."""
    from aqueduct.overrides import apply_to_model, route_overrides

    nested, _ = route_overrides(items, allow_blueprint=False)
    return apply_to_model(AqueductConfig(), nested)


def test_set_beats_a_healed_blueprint_value_for_spark():
    manifest = _manifest({"spark": {"spark.sql.shuffle.partitions": "200"}})
    cfg_plain = AqueductConfig()
    cfg_set = _cfg_with_set("engine.spark.conf.spark.sql.shuffle.partitions=800")

    # Control: without the flag the Blueprint value still wins over
    # aqueduct.yml, exactly as before — this change does not weaken layer 2.
    assert resolve_session_engine_config(cfg_plain, "spark", manifest) == {
        "spark.sql.shuffle.partitions": "200"
    }
    assert resolve_session_engine_config(cfg_set, "spark", manifest) == {
        "spark.sql.shuffle.partitions": 800
    }


def test_set_beats_a_blueprint_value_for_a_typed_engine_block():
    """The rule is per-engine and structural, not a Spark special case: a
    typed engine block (DuckDB) layers identically."""
    manifest = _manifest({"duckdb": {"memory_limit": "1GB"}})
    cfg_set = _cfg_with_set("engine.duckdb.memory_limit=4GB")

    assert resolve_session_engine_config(AqueductConfig(), "duckdb", manifest)["memory_limit"] == (
        "1GB"
    )
    assert resolve_session_engine_config(cfg_set, "duckdb", manifest)["memory_limit"] == "4GB"


def test_set_does_not_leak_into_an_engine_it_did_not_name():
    """Positive control on scope: pinning a Spark key must not touch DuckDB's
    resolved config, and must not disturb OTHER Spark keys the Blueprint set."""
    manifest = _manifest(
        {
            "spark": {"spark.sql.shuffle.partitions": "200", "spark.sql.adaptive.enabled": "false"},
            "duckdb": {"memory_limit": "1GB"},
        }
    )
    cfg_set = _cfg_with_set("engine.spark.conf.spark.sql.shuffle.partitions=800")

    spark_cfg = resolve_session_engine_config(cfg_set, "spark", manifest)
    assert spark_cfg["spark.sql.shuffle.partitions"] == 800
    assert spark_cfg["spark.sql.adaptive.enabled"] == "false"  # untouched
    assert resolve_session_engine_config(cfg_set, "duckdb", manifest)["memory_limit"] == "1GB"


def test_a_dotted_engine_conf_key_stays_ONE_key():
    """`engine.spark.conf` is `dict[str, Any]` and Spark's own key names are
    dotted, so splitting the whole `--set` path on every dot built a DEEP
    nested dict (`conf["spark"]["sql"][...]`) that pydantic accepted without
    complaint — the session was configured with a key literally named
    `spark` whose value was a dict. Silent wrong answer, fixed at the router."""
    from aqueduct.overrides import route_overrides

    nested, _ = route_overrides(
        ["engine.spark.conf.spark.sql.shuffle.partitions=800"], allow_blueprint=False
    )
    assert nested == {"engine": {"spark": {"conf": {"spark.sql.shuffle.partitions": 800}}}}


def test_a_model_valued_dict_path_still_nests_normally():
    """Positive control for the collapse above: `stores.depots` is
    `dict[str, DepotMountConfig]`, so the segments after it really are a key
    followed by that model's fields. Collapsing there would break paths that
    work today."""
    from aqueduct.overrides import route_overrides

    nested, _ = route_overrides(["stores.depots.mydepot.backend=redis"], allow_blueprint=False)
    assert nested == {"stores": {"depots": {"mydepot": {"backend": "redis"}}}}


def test_fingerprint_separates_a_session_built_with_set_from_one_without():
    """The invariant closed in the session-rebuild work: a session built with
    `--set` must never be reused for a Manifest resolved without it. Nothing
    was added to `session_config_fingerprint` for this — the CLI layer lives
    INSIDE `resolve_session_engine_config`, whose output is what gets hashed,
    so the separation holds by construction."""
    manifest = _manifest({"spark": {"spark.sql.shuffle.partitions": "200"}})
    plain = session_config_fingerprint(AqueductConfig(), "spark", manifest)
    pinned = session_config_fingerprint(
        _cfg_with_set("engine.spark.conf.spark.sql.shuffle.partitions=800"), "spark", manifest
    )
    assert plain != pinned


def test_fingerprint_is_stable_when_a_heal_writes_a_key_that_set_pins():
    """Consequence of layer 3 that the fingerprint gets right for free: a
    heal writing a Blueprint value the user's `--set` shadows does NOT change
    the resolved session config, so no session rebuild is triggered. The
    control below proves the fingerprint still moves for an UNPINNED key —
    without it this assertion would also hold for a fingerprint that ignored
    the Manifest entirely."""
    cfg_set = _cfg_with_set("engine.spark.conf.spark.sql.shuffle.partitions=800")
    before = _manifest({"spark": {"spark.sql.shuffle.partitions": "200"}})
    healed = _manifest({"spark": {"spark.sql.shuffle.partitions": "400"}})
    assert session_config_fingerprint(cfg_set, "spark", before) == session_config_fingerprint(
        cfg_set, "spark", healed
    )

    healed_elsewhere = _manifest(
        {"spark": {"spark.sql.shuffle.partitions": "200", "spark.sql.adaptive.enabled": "false"}}
    )
    assert session_config_fingerprint(cfg_set, "spark", before) != session_config_fingerprint(
        cfg_set, "spark", healed_elsewhere
    )


def test_no_set_leaves_the_cli_layer_empty():
    assert AqueductConfig().cli_engine_overrides == {}
    assert _cfg_with_set("timezone=UTC").cli_engine_overrides == {}
