"""`doctor`'s healed-engine-config check — facts only, no invented threshold.

The check must never render a verdict on a perf number (Aqueduct sets no
regression threshold), and must escalate exactly one condition: the recorded
value is no longer what the effective config resolves to. That is an
equality, not a threshold.
"""

from __future__ import annotations

import pytest

from aqueduct.config import AqueductConfig, EngineConfig, SparkEngineConfig
from aqueduct.doctor import check_healed_engine_config

pytestmark = pytest.mark.unit


_HEAD = """
aqueduct: "1.0"
id: demo_bp
name: Demo
modules: []
edges: []
"""


def _cfg(conf=None):
    return AqueductConfig(engine=EngineConfig(spark=SparkEngineConfig(conf=dict(conf or {}))))


def _write(tmp_path, body: str):
    p = tmp_path / "blueprint.yml"
    p.write_text(_HEAD + body, encoding="utf-8")
    return p


_LIVE_RECORD = """
engine:
  spark:
    conf:
      spark.sql.shuffle.partitions: 800
healed_by:
  - patch_id: fix-shuffle
    engine: spark
    classification: engine_shaped
    applied_at: '2026-01-01T00:00:00+00:00'
    validated_on: [spark]
    engine_config_delta:
      spark:
        spark.sql.shuffle.partitions:
          before: 200
          after: 800
"""


def test_no_healed_config_produces_no_rows(tmp_path):
    bp = _write(tmp_path, "")
    assert check_healed_engine_config(bp, _cfg()) == []


def test_a_live_healed_key_reports_facts_and_the_undo_command(tmp_path):
    bp = _write(tmp_path, _LIVE_RECORD)
    (result,) = check_healed_engine_config(bp, _cfg({"spark.sql.shuffle.partitions": 200}))
    assert result.name == "healed-config:fix-shuffle"
    assert result.status == "ok"
    assert "spark.sql.shuffle.partitions 200→800" in result.detail
    assert "green-run validated on ['spark']" in result.detail
    assert "aqueduct patch revert fix-shuffle" in result.detail


def test_a_perf_observation_is_reported_as_a_number_never_as_a_verdict(tmp_path):
    bp = _write(
        tmp_path,
        _LIVE_RECORD + """    perf_observations:
      - status: observed
        engine: spark
        observed_at: '2026-01-02T00:00:00+00:00'
        duration_ratio: 3.2
""",
    )
    (result,) = check_healed_engine_config(bp, _cfg({"spark.sql.shuffle.partitions": 200}))
    assert "3.2x the pre-patch baseline duration" in result.detail
    assert "Aqueduct sets no threshold" in result.detail
    # A ratio must not move the status — no threshold exists to move it.
    assert result.status == "ok"
    lowered = result.detail.lower()
    assert "regression" not in lowered
    assert "stale" not in lowered


def test_a_superseded_key_warns_because_the_record_no_longer_describes_reality(tmp_path):
    """Not a threshold: the recorded `after` and the resolved value differ."""
    bp = _write(
        tmp_path,
        _LIVE_RECORD.replace(
            "      spark.sql.shuffle.partitions: 800",
            "      spark.sql.shuffle.partitions: 1600",
        ),
    )
    (result,) = check_healed_engine_config(bp, _cfg({"spark.sql.shuffle.partitions": 200}))
    assert result.status == "warn"
    assert "now resolves to 1600" in result.detail
    assert "patch revert" in result.detail


def test_a_reverted_record_is_reported_as_history_not_as_a_live_key(tmp_path):
    bp = _write(
        tmp_path,
        _LIVE_RECORD.replace("      spark.sql.shuffle.partitions: 800\n", "").replace(
            "    conf:\n", "    conf: {}\n"
        )
        + "    reverted_at: '2026-02-02T00:00:00+00:00'\n",
    )
    (result,) = check_healed_engine_config(bp, _cfg({"spark.sql.shuffle.partitions": 200}))
    assert result.status == "ok"
    assert "reverted at 2026-02-02T00:00:00+00:00" in result.detail
    assert "no longer in this Blueprint" in result.detail
