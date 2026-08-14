"""`aqueduct/patch/revert.py` — undo a healed engine-config write, or refuse.

The property under test throughout: a revert either reproduces a
configuration this Blueprint actually had, or it does not happen at all.
Every refusal below is a state where the naive "put the recorded `before`
back" would leave a Blueprint matching no prior state.
"""

from __future__ import annotations

import pytest

from aqueduct.config import (
    AqueductConfig,
    DuckDBEngineConfig,
    EngineConfig,
    SparkEngineConfig,
)
from aqueduct.errors import AqueductError
from aqueduct.patch.revert import RevertError, apply_revert, plan_revert

pytestmark = pytest.mark.unit


def _cfg(spark_conf=None, duckdb=None):
    return AqueductConfig(
        engine=EngineConfig(
            spark=SparkEngineConfig(conf=dict(spark_conf or {})),
            duckdb=duckdb or DuckDBEngineConfig(),
        )
    )


def _record(**over):
    rec = {
        "patch_id": "p1",
        "engine": "spark",
        "classification": "engine_shaped",
        "applied_at": "2026-01-01T00:00:00+00:00",
        "validated_on": [],
        "engine_config_delta": {
            "spark": {"spark.sql.shuffle.partitions": {"before": "200", "after": "800"}}
        },
    }
    rec.update(over)
    return rec


def _bp(records=None, conf=None):
    bp: dict = {
        "aqueduct": "1.0",
        "id": "bp",
        "name": "bp",
        "modules": [],
    }
    if conf is not None:
        bp["engine"] = {"spark": {"conf": dict(conf)}}
    if records is not None:
        bp["healed_by"] = list(records)
    return bp


_CONFIG_OP = [
    {
        "op": "set_engine_config",
        "engine": "spark",
        "key": "spark.sql.shuffle.partitions",
        "value": "800",
    }
]


# ── the safe case ─────────────────────────────────────────────────────────────


def test_removes_the_key_when_the_prior_value_came_from_aqueduct_yml():
    """`aqueduct.yml` already carries 200, so deleting the Blueprint's own
    key is the correct undo — writing 200 back explicitly would leave the
    Blueprint claiming a value it never claimed before the patch."""
    bp = _bp([_record()], conf={"spark.sql.shuffle.partitions": "800"})
    plan = plan_revert(
        cfg=_cfg({"spark.sql.shuffle.partitions": "200"}),
        blueprint=bp,
        patch_id="p1",
        operations=_CONFIG_OP,
    )
    assert [(r.engine, r.key, r.action) for r in plan.restores] == [
        ("spark", "spark.sql.shuffle.partitions", "remove")
    ]
    out = apply_revert(bp, plan, reverted_at="2026-02-02T00:00:00+00:00")
    assert out["engine"]["spark"]["conf"] == {}
    assert out["healed_by"][0]["reverted_at"] == "2026-02-02T00:00:00+00:00"
    # The record is KEPT, not deleted — history survives the undo.
    assert out["healed_by"][0]["patch_id"] == "p1"


def test_writes_the_prior_value_back_when_it_was_the_blueprints_own():
    """No `aqueduct.yml` layer for this key, and the pre-patch value came
    from an earlier Blueprint write — removal would resolve to nothing, so
    the value has to be written back explicitly."""
    rec = _record(
        engine_config_delta={
            "spark": {"spark.sql.shuffle.partitions": {"before": "400", "after": "800"}}
        }
    )
    bp = _bp([rec], conf={"spark.sql.shuffle.partitions": "800"})
    plan = plan_revert(cfg=_cfg(), blueprint=bp, patch_id="p1", operations=_CONFIG_OP)
    assert [(r.action, r.value) for r in plan.restores] == [("set", "400")]
    out = apply_revert(bp, plan, reverted_at="t")
    assert out["engine"]["spark"]["conf"]["spark.sql.shuffle.partitions"] == "400"


def test_reverting_in_reverse_order_works():
    """Patch 2 then patch 1: a reverted record is skipped as a later writer,
    which is exactly what makes LIFO order the safe order."""
    first = _record(
        patch_id="p1",
        applied_at="2026-01-01T00:00:00+00:00",
        engine_config_delta={
            "spark": {"spark.sql.shuffle.partitions": {"before": "200", "after": "800"}}
        },
    )
    second = _record(
        patch_id="p2",
        applied_at="2026-01-02T00:00:00+00:00",
        engine_config_delta={
            "spark": {"spark.sql.shuffle.partitions": {"before": "800", "after": "1600"}}
        },
    )
    cfg = _cfg({"spark.sql.shuffle.partitions": "200"})
    bp = _bp([first, second], conf={"spark.sql.shuffle.partitions": "1600"})

    plan2 = plan_revert(
        cfg=cfg,
        blueprint=bp,
        patch_id="p2",
        operations=[{**_CONFIG_OP[0], "value": "1600"}],
    )
    bp = apply_revert(bp, plan2, reverted_at="t2")
    assert bp["engine"]["spark"]["conf"]["spark.sql.shuffle.partitions"] == "800"

    plan1 = plan_revert(cfg=cfg, blueprint=bp, patch_id="p1", operations=_CONFIG_OP)
    bp = apply_revert(bp, plan1, reverted_at="t1")
    assert bp["engine"]["spark"]["conf"] == {}
    assert [r["reverted_at"] for r in bp["healed_by"]] == ["t1", "t2"]


# ── the refusals ──────────────────────────────────────────────────────────────


def test_refuses_when_a_later_patch_wrote_the_same_key():
    first = _record(patch_id="p1")
    second = _record(
        patch_id="p2",
        engine_config_delta={
            "spark": {"spark.sql.shuffle.partitions": {"before": "800", "after": "1600"}}
        },
    )
    bp = _bp([first, second], conf={"spark.sql.shuffle.partitions": "1600"})
    with pytest.raises(RevertError) as exc:
        plan_revert(
            cfg=_cfg({"spark.sql.shuffle.partitions": "200"}),
            blueprint=bp,
            patch_id="p1",
            operations=_CONFIG_OP,
        )
    assert "'p2'" in str(exc.value)
    assert "spark.sql.shuffle.partitions" in str(exc.value)


def test_refuses_a_patch_carrying_a_non_config_operation():
    """A mixed patch's module half has no recorded prior state, so undoing
    only its config half would leave a hybrid Blueprint."""
    bp = _bp([_record()], conf={"spark.sql.shuffle.partitions": "800"})
    ops = _CONFIG_OP + [
        {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "x"}
    ]
    with pytest.raises(RevertError, match="set_module_config_key"):
        plan_revert(
            cfg=_cfg({"spark.sql.shuffle.partitions": "200"}),
            blueprint=bp,
            patch_id="p1",
            operations=ops,
        )


def test_refuses_when_the_applied_patch_body_is_unavailable():
    """Without the body there is no proof the patch was config-only."""
    bp = _bp([_record()], conf={"spark.sql.shuffle.partitions": "800"})
    with pytest.raises(RevertError, match="could not be read"):
        plan_revert(
            cfg=_cfg({"spark.sql.shuffle.partitions": "200"}),
            blueprint=bp,
            patch_id="p1",
            operations=None,
        )


def test_refuses_when_the_value_was_edited_since_the_patch():
    """A hand edit since the heal: reverting would silently overwrite it."""
    bp = _bp([_record()], conf={"spark.sql.shuffle.partitions": "999"})
    with pytest.raises(RevertError) as exc:
        plan_revert(
            cfg=_cfg({"spark.sql.shuffle.partitions": "200"}),
            blueprint=bp,
            patch_id="p1",
            operations=_CONFIG_OP,
        )
    assert "'999'" in str(exc.value)


def test_refuses_a_record_with_no_engine_config_delta():
    """A pipeline-only heal records no prior value for anything."""
    bp = _bp([_record(engine_config_delta={})])
    with pytest.raises(RevertError, match="no engine_config_delta"):
        plan_revert(cfg=_cfg(), blueprint=bp, patch_id="p1", operations=_CONFIG_OP)


def test_refuses_an_unknown_patch_id_and_names_what_is_recorded():
    bp = _bp([_record(patch_id="p1")], conf={"spark.sql.shuffle.partitions": "800"})
    with pytest.raises(RevertError) as exc:
        plan_revert(cfg=_cfg(), blueprint=bp, patch_id="nope", operations=_CONFIG_OP)
    assert "'p1'" in str(exc.value)


def test_refuses_a_duplicated_patch_id_rather_than_guessing():
    bp = _bp(
        [_record(patch_id="p1"), _record(patch_id="p1")],
        conf={"spark.sql.shuffle.partitions": "800"},
    )
    with pytest.raises(RevertError, match="2 healed_by records"):
        plan_revert(
            cfg=_cfg({"spark.sql.shuffle.partitions": "200"}),
            blueprint=bp,
            patch_id="p1",
            operations=_CONFIG_OP,
        )


def test_refuses_an_already_reverted_record():
    bp = _bp([_record(reverted_at="2026-02-02T00:00:00+00:00")], conf={})
    with pytest.raises(RevertError, match="already reverted"):
        plan_revert(cfg=_cfg(), blueprint=bp, patch_id="p1", operations=_CONFIG_OP)


def test_refuses_when_the_pre_patch_state_is_no_longer_reachable():
    """The record says the key did not resolve at all before the patch, but
    `aqueduct.yml` has gained it since — removing the Blueprint's key would
    unmask a value that was never the pre-patch state."""
    rec = _record(
        engine_config_delta={
            "spark": {"spark.sql.shuffle.partitions": {"before": None, "after": "800"}}
        }
    )
    bp = _bp([rec], conf={"spark.sql.shuffle.partitions": "800"})
    with pytest.raises(RevertError, match="no longer reachable"):
        plan_revert(
            cfg=_cfg({"spark.sql.shuffle.partitions": "300"}),
            blueprint=bp,
            patch_id="p1",
            operations=_CONFIG_OP,
        )


def test_revert_error_is_an_aqueduct_error_but_not_a_patch_error():
    """Type, not message: 'this applied patch cannot be undone' and 'this
    patch cannot be applied' are different states with different fixes, so a
    handler for one must not swallow the other."""
    from aqueduct.patch.apply import PatchError

    assert issubclass(RevertError, AqueductError)
    assert not issubclass(RevertError, PatchError)
    assert not issubclass(PatchError, RevertError)


def test_a_typed_engine_field_reverts_through_its_own_addressing():
    """DuckDB's block declares typed fields, not a `conf` bag — the restore
    must address it the same way `set_engine_config` did."""
    rec = _record(
        engine="duckdb",
        engine_config_delta={"duckdb": {"threads": {"before": 4, "after": 16}}},
    )
    bp: dict = {
        "aqueduct": "1.0",
        "id": "bp",
        "name": "bp",
        "modules": [],
        "engine": {"duckdb": {"threads": 16}},
        "healed_by": [rec],
    }
    plan = plan_revert(
        cfg=_cfg(duckdb=DuckDBEngineConfig(threads=4)),
        blueprint=bp,
        patch_id="p1",
        operations=[{"op": "set_engine_config", "engine": "duckdb", "key": "threads", "value": 16}],
    )
    out = apply_revert(bp, plan, reverted_at="t")
    assert "threads" not in out["engine"]["duckdb"]


# ── the consumers of `reverted_at` ────────────────────────────────────────────


def test_cross_engine_gate_ignores_a_reverted_record():
    """A reverted record's dialect-bearing change is out of the Blueprint,
    so warning about it would report a risk the file no longer carries."""
    from types import SimpleNamespace

    from aqueduct.compiler.capability_check import check_cross_engine_heal

    live = SimpleNamespace(
        patch_id="p1",
        classification="engine_shaped",
        engine="duckdb",
        validated_on=(),
        reverted_at=None,
    )
    reverted = SimpleNamespace(
        patch_id="p2",
        classification="engine_shaped",
        engine="duckdb",
        validated_on=(),
        reverted_at="2026-02-02T00:00:00+00:00",
    )
    bp = SimpleNamespace(healed_by=(live, reverted))
    problems = check_cross_engine_heal(bp, "spark")
    assert [p.patch_id for p in problems] == ["p1"]


def test_green_run_stamps_skip_a_reverted_record(tmp_path):
    """`validated_on` must not gain an engine for a patch whose change is
    not in the Blueprint the green run executed."""
    from aqueduct.patch.apply import _yaml_load, stamp_validated_engine

    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(
        "aqueduct: '1.0'\n"
        "id: bp\n"
        "name: bp\n"
        "modules: []\n"
        "healed_by:\n"
        "  - patch_id: p1\n"
        "    engine: spark\n"
        "    classification: engine_shaped\n"
        "    applied_at: t\n"
        "    validated_on: []\n"
        "    reverted_at: '2026-02-02T00:00:00+00:00'\n"
        "  - patch_id: p2\n"
        "    engine: spark\n"
        "    classification: engine_shaped\n"
        "    applied_at: t\n"
        "    validated_on: []\n",
        encoding="utf-8",
    )
    assert stamp_validated_engine(bp_path, "spark") is True
    records = _yaml_load(bp_path)["healed_by"]
    assert list(records[0]["validated_on"]) == []
    assert list(records[1]["validated_on"]) == ["spark"]

    # Same trigger, same rule: a run's duration says nothing about a patch
    # whose change was reverted out before the run started.
    from aqueduct.patch.apply import stamp_perf_observation

    written = stamp_perf_observation(bp_path, "spark", obs_store=None, run_id="r1")
    assert len(written) == 1
    records = _yaml_load(bp_path)["healed_by"]
    assert "perf_observations" not in records[0]
    assert len(records[1]["perf_observations"]) == 1
