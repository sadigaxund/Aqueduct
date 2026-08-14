"""The engine-config delta gate writes its verdict to `patch_simulation`.

Property under test: every outcome of the gate
(`aqueduct/patch/config_delta.py`) reaches the audit trail the fleet metric
reads (`stores/queries.py::gate_rejection_rates`) — including the refusal,
which raises rather than returning a result and was therefore the outcome
most at risk of being invisible. The gate stays non-blocking here: the
returned gate tuple is unaffected by what this records.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from aqueduct.cli import _run_patch_gates_inline
from aqueduct.config import AqueductConfig, EngineConfig, SparkEngineConfig
from aqueduct.patch.grammar import PatchSpec, SetEngineConfigOp, SetModuleConfigKeyOp

pytestmark = pytest.mark.unit


_BLUEPRINT = """
aqueduct: "1.0"
id: test_bp
name: Test BP
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: csv
      path: data.csv
edges: []
"""


def _cfg(conf: dict | None = None) -> AqueductConfig:
    return AqueductConfig(engine=EngineConfig(spark=SparkEngineConfig(conf=dict(conf or {}))))


def _run(tmp_path, spec, cfg):
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text(_BLUEPRINT, encoding="utf-8")
    surveyor = MagicMock()
    bundle = MagicMock()
    bundle.observability = None
    result = _run_patch_gates_inline(
        patch=spec,
        blueprint_path=bp_file,
        bundle=bundle,
        surveyor=surveyor,
        failed_module="m1",
        iteration_run_id="iter-1",
        blueprint_id="test_bp",
        engine="spark",
        cfg=cfg,
        sandbox_mode="off",
    )
    calls = surveyor.record_patch_simulation.call_args_list
    row = next(c.kwargs for c in calls if c.kwargs["gate"] == "engine_config")
    return result, row


def _engine_config_spec(value: str) -> PatchSpec:
    return PatchSpec(
        patch_id="p-cfg",
        rationale="test",
        operations=[
            SetEngineConfigOp(
                op="set_engine_config",
                engine="spark",
                key="spark.sql.shuffle.partitions",
                value=value,
            )
        ],
    )


def test_effective_change_records_a_pass_row(tmp_path):
    _result, row = _run(
        tmp_path, _engine_config_spec("400"), _cfg({"spark.sql.shuffle.partitions": "200"})
    )
    assert row["status"] == "pass"
    assert "1 effective engine-config key(s) change" in row["detail"]
    assert row["run_id"] == "iter-1"
    assert row["blueprint_id"] == "test_bp"
    assert isinstance(row["duration_ms"], int)


def test_inert_write_records_a_fail_row_and_does_not_block(tmp_path):
    """The refusal is enforced at apply time; here it must be COUNTABLE.

    `gate_rejection_rates` counts `status = 'fail'` only, so a gate whose
    only failing outcome is a raised exception contributes nothing to the
    fleet metric unless the row is written explicitly.
    """
    cfg = _cfg({"spark.sql.shuffle.partitions": "400"})
    result, row = _run(tmp_path, _engine_config_spec("400"), cfg)
    assert row["status"] == "fail"
    assert "has no effect" in row["detail"]
    # Non-blocking: the three-gate tuple is untouched by this recording.
    assert len(result) == 4
    assert result[3] is True


def test_pipeline_only_patch_records_not_applicable(tmp_path):
    spec = PatchSpec(
        patch_id="p-mod",
        rationale="test",
        operations=[
            SetModuleConfigKeyOp(
                op="set_module_config_key", module_id="m1", key="path", value="new.csv"
            )
        ],
    )
    _result, row = _run(tmp_path, spec, _cfg())
    assert row["status"] == "not_applicable"
    assert row["detail"] == "patch writes no engine/session config"


def test_a_broken_store_never_breaks_the_gate_run(tmp_path):
    """The recording is best-effort — a surveyor that raises must not take
    the gate run down with it (the three existing gates all behave this
    way; a fourth that aborts the loop would be a regression)."""
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text(_BLUEPRINT, encoding="utf-8")
    surveyor = MagicMock()
    surveyor.record_patch_simulation.side_effect = RuntimeError("store down")
    bundle = MagicMock()
    bundle.observability = None
    result = _run_patch_gates_inline(
        patch=_engine_config_spec("400"),
        blueprint_path=bp_file,
        bundle=bundle,
        surveyor=surveyor,
        failed_module="m1",
        iteration_run_id="iter-1",
        blueprint_id="test_bp",
        engine="spark",
        cfg=_cfg({"spark.sql.shuffle.partitions": "200"}),
        sandbox_mode="off",
    )
    assert len(result) == 4
    assert result[3] is True
