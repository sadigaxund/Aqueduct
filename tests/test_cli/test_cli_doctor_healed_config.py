"""`doctor`'s healed-engine-config check — facts only, no invented threshold.

The check must never render a verdict on a perf number (Aqueduct sets no
regression threshold), and must escalate exactly one condition: the recorded
value is no longer what the effective config resolves to. That is an
equality, not a threshold.

The apply-time facts a `healed_by:` record used to carry inline
(`engine_config_delta`, `perf_baseline`, `perf_observations`, `engine_version`,
`run_id`) now live in the `patch_index` table of the observability store,
keyed by `patch_id` (see `aqueduct/patch/index.py::HealedByRecordSchema`'s
docstring). A Blueprint fixture here carries only the BOUNDED record; every
test seeds the moved facts into a real DuckDB observability store at the path
`aqueduct.stores.read.open_obs_read` resolves for the blueprint's `id`
(`<obs_path>/<blueprint_id>/observability.db`).
"""

from __future__ import annotations

import pytest

from aqueduct.config import (
    AqueductConfig,
    EngineConfig,
    RelationalStoreConfig,
    SparkEngineConfig,
    StoresConfig,
)
from aqueduct.doctor import check_healed_engine_config
from aqueduct.patch import index as _ix
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

pytestmark = pytest.mark.unit

_BLUEPRINT_ID = "demo_bp"
_PATCH_ID = "fix-shuffle"

_HEAD = """
aqueduct: "1.0"
id: demo_bp
name: Demo
modules: []
edges: []
"""


def _cfg(conf=None, obs_path=None):
    return AqueductConfig(
        engine=EngineConfig(spark=SparkEngineConfig(conf=dict(conf or {}))),
        stores=StoresConfig(
            observability=RelationalStoreConfig(path=str(obs_path) if obs_path else None)
        ),
    )


def _write(tmp_path, body: str):
    p = tmp_path / "blueprint.yml"
    p.write_text(_HEAD + body, encoding="utf-8")
    return p


def _obs_path(tmp_path):
    return tmp_path / "obs"


def _seed_index(
    tmp_path,
    *,
    blueprint_id: str = _BLUEPRINT_ID,
    patch_id: str = _PATCH_ID,
    engine: str = "spark",
    engine_config_delta: dict | None = None,
    perf_baseline: dict | None = None,
    perf_observation: dict | None = None,
):
    """Seed one patch's heal-provenance facts into a real DuckDB store.

    Writes to the exact path `open_obs_read` resolves for *blueprint_id*
    under `<tmp_path>/obs` — the same routing base `_cfg(..., obs_path=...)`
    points the check's config at.
    """
    store = DuckDBObservabilityStore(_obs_path(tmp_path) / blueprint_id / "observability.db")
    with store.connect() as cur:
        _ix.ensure_schema(cur)
        _ix.record_heal_provenance(
            cur,
            patch_id,
            engine=engine,
            engine_config_delta=engine_config_delta,
            perf_baseline=perf_baseline,
        )
        if perf_observation is not None:
            _ix.append_perf_observation(cur, patch_id, perf_observation)
    return store


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
"""

_LIVE_DELTA = {
    "spark": {
        "spark.sql.shuffle.partitions": {"before": 200, "after": 800},
    }
}


def test_no_healed_config_produces_no_rows(tmp_path):
    bp = _write(tmp_path, "")
    assert check_healed_engine_config(bp, _cfg(obs_path=_obs_path(tmp_path))) == []


def test_a_live_healed_key_reports_facts_and_the_undo_command(tmp_path):
    bp = _write(tmp_path, _LIVE_RECORD)
    _seed_index(tmp_path, engine_config_delta=_LIVE_DELTA)
    (result,) = check_healed_engine_config(
        bp, _cfg({"spark.sql.shuffle.partitions": 200}, obs_path=_obs_path(tmp_path))
    )
    assert result.name == "healed-config:fix-shuffle"
    assert result.status == "ok"
    assert "spark.sql.shuffle.partitions 200→800" in result.detail
    assert "green-run validated on ['spark']" in result.detail
    assert "aqueduct patch revert fix-shuffle" in result.detail


def test_a_perf_observation_is_reported_as_a_number_never_as_a_verdict(tmp_path):
    bp = _write(tmp_path, _LIVE_RECORD)
    _seed_index(
        tmp_path,
        engine_config_delta=_LIVE_DELTA,
        perf_observation={
            "status": "observed",
            "engine": "spark",
            "observed_at": "2026-01-02T00:00:00+00:00",
            "duration_ratio": 3.2,
        },
    )
    (result,) = check_healed_engine_config(
        bp, _cfg({"spark.sql.shuffle.partitions": 200}, obs_path=_obs_path(tmp_path))
    )
    assert "3.2x the pre-patch baseline duration" in result.detail
    assert "Aqueduct sets no threshold" in result.detail
    # A ratio must not move the status — no threshold exists to move it.
    assert result.status == "ok"
    lowered = result.detail.lower()
    assert "regression" not in lowered
    assert "stale" not in lowered


def test_a_superseded_key_warns_because_the_record_no_longer_describes_reality(tmp_path):
    """Not a threshold: the recorded `after` and the resolved value differ."""
    bp = _write(tmp_path, _LIVE_RECORD)
    _seed_index(tmp_path, engine_config_delta=_LIVE_DELTA)
    bp.write_text(
        bp.read_text(encoding="utf-8").replace(
            "      spark.sql.shuffle.partitions: 800",
            "      spark.sql.shuffle.partitions: 1600",
        ),
        encoding="utf-8",
    )
    (result,) = check_healed_engine_config(
        bp, _cfg({"spark.sql.shuffle.partitions": 200}, obs_path=_obs_path(tmp_path))
    )
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
    _seed_index(tmp_path, engine_config_delta=_LIVE_DELTA)
    (result,) = check_healed_engine_config(
        bp, _cfg({"spark.sql.shuffle.partitions": 200}, obs_path=_obs_path(tmp_path))
    )
    assert result.status == "ok"
    assert "reverted at 2026-02-02T00:00:00+00:00" in result.detail
    assert "no longer in this Blueprint" in result.detail


def test_the_delta_and_perf_ratio_come_from_the_index_when_the_blueprint_only_carries_the_bounded_record(
    tmp_path,
):
    """The Blueprint carries only the bounded `healed_by:` fields (patch_id,
    engine, classification, applied_at, validated_on) — no
    `engine_config_delta` and no `perf_observations` anywhere in the YAML.
    `doctor` must still report both, reading them purely from `patch_index`.
    """
    bp = _write(tmp_path, _LIVE_RECORD)
    assert "engine_config_delta" not in bp.read_text(encoding="utf-8")
    assert "perf_observations" not in bp.read_text(encoding="utf-8")
    _seed_index(
        tmp_path,
        engine_config_delta=_LIVE_DELTA,
        perf_observation={
            "status": "observed",
            "engine": "spark",
            "observed_at": "2026-01-02T00:00:00+00:00",
            "duration_ratio": 1.1,
        },
    )
    (result,) = check_healed_engine_config(
        bp, _cfg({"spark.sql.shuffle.partitions": 200}, obs_path=_obs_path(tmp_path))
    )
    assert result.status == "ok"
    assert "spark.sql.shuffle.partitions 200→800" in result.detail
    assert "1.1x the pre-patch baseline duration" in result.detail


def test_an_unreadable_index_for_a_blueprint_with_healed_by_records_warns_instead_of_going_silent(
    tmp_path,
):
    """No `observability.db` file exists yet for this blueprint at all — the
    check must return the single `healed-config` warn row naming that,
    never `[]` (silence would be indistinguishable from "nothing healed
    here", which is false: this Blueprint DOES carry a healed_by record)."""
    bp = _write(tmp_path, _LIVE_RECORD)
    # Deliberately not seeding any store — `<obs_path>/demo_bp/observability.db`
    # does not exist.
    (result,) = check_healed_engine_config(
        bp, _cfg({"spark.sql.shuffle.partitions": 200}, obs_path=_obs_path(tmp_path))
    )
    assert result.name == "healed-config"
    assert result.status == "warn"
    assert "could not be read" in result.detail
