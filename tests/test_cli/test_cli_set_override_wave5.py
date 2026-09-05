"""`-s/--set` on `patch preview`, `patch revert`, and `doctor` (Phase 85 Wave 5).

Three commands that resolve engine config but previously had no way to
preview/revert/diagnose under the same override a user ran their pipeline
with. The dangerous half is `patch revert`'s prior-values equality check:
it MUST always compare against the UNPINNED resolution, never the
`--set`-pinned one — see `aqueduct/patch/revert.py`'s module docstring and
`aqueduct/cli/patch.py::patch_revert`. These tests cover both directions:
a `--set` must not make a legitimate revert abort, and must not let a
genuinely diverged revert falsely pass.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli
from aqueduct.doctor import run_doctor

pytestmark = [pytest.mark.spark, pytest.mark.integration]


_AQUEDUCT_YML = """
engine:
  spark:
    conf:
      spark.sql.shuffle.partitions: 200
"""

_BLUEPRINT_ID = "demo_bp"


def _touch_obs_store(tmp_path, blueprint_id: str = _BLUEPRINT_ID) -> None:
    """Pre-create the (empty) DuckDB observability store `patch apply`'s
    heal-provenance write and `patch revert`'s read both resolve to.

    See `tests/test_cli/test_cli_patch_revert.py::_touch_obs_store` for the
    full rationale: `_patch_index_obs_store` (`aqueduct/cli/patch.py`)
    resolves through `open_obs_read`, which returns None — a best-effort
    skip, not an error — when no `observability.db` file exists yet at the
    routed path. These tests never `aqueduct run`, so nothing else would
    create it first. Requires `monkeypatch.chdir(tmp_path)`.
    """
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    store = DuckDBObservabilityStore(
        Path(tmp_path) / ".aqueduct" / blueprint_id / "observability.db"
    )
    with store.connect():
        pass


_BLUEPRINT = """
aqueduct: "1.0"
id: demo_bp
name: Demo Blueprint
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: csv
      path: data.csv
edges: []
"""


def _project(tmp_path, operations, patch_id="fix-shuffle"):
    (tmp_path / "aqueduct.yml").write_text(_AQUEDUCT_YML, encoding="utf-8")
    (tmp_path / "blueprint.yml").write_text(_BLUEPRINT, encoding="utf-8")
    pending = tmp_path / "patches" / "pending"
    pending.mkdir(parents=True)
    (pending / "00001_patch.json").write_text(
        json.dumps(
            {
                "patch_id": patch_id,
                "run_id": "r1",
                "rationale": "test",
                "operations": operations,
                "_aq_meta": {"engine": "spark", "run_id": "r1", "blueprint_id": "demo_bp"},
            }
        ),
        encoding="utf-8",
    )
    return pending / "00001_patch.json"


def _apply(runner, tmp_path, patch_file):
    return runner.invoke(
        cli,
        [
            "patch",
            "apply",
            str(patch_file),
            "--blueprint",
            str(tmp_path / "blueprint.yml"),
            "--config",
            str(tmp_path / "aqueduct.yml"),
        ],
    )


def _revert(runner, tmp_path, patch_id, *extra):
    return runner.invoke(
        cli,
        [
            "patch",
            "revert",
            patch_id,
            "--blueprint",
            str(tmp_path / "blueprint.yml"),
            "--config",
            str(tmp_path / "aqueduct.yml"),
            *extra,
        ],
    )


_SET_CONF = {
    "op": "set_engine_config",
    "engine": "spark",
    "key": "spark.sql.shuffle.partitions",
    "value": 800,
}


# ── patch preview: --set reaches the resolved config ───────────────────────────


def test_patch_preview_without_set_reports_the_engine_config_delta(tmp_path):
    """Baseline (no --set): the patch's config write shows up as a delta."""
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    result = runner.invoke(
        cli,
        [
            "patch",
            "preview",
            str(patch_file),
            "--blueprint",
            str(tmp_path / "blueprint.yml"),
            "--config",
            str(tmp_path / "aqueduct.yml"),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == exit_codes.SUCCESS, result.output
    report = json.loads(result.output)
    assert report["engine_config"]["status"] == "pass"
    assert report["engine_config"]["delta"]["spark"]["spark.sql.shuffle.partitions"] == {
        "before": 200,
        "after": 800,
    }
    assert report["engine_config"]["cli_pinned"] == {}


def test_patch_preview_set_pins_the_same_key_the_patch_writes(tmp_path):
    """`--set` on the SAME key the patch writes reaches the resolved config:
    the effective session config no longer moves (the CLI override wins at
    every layer), so the engine-config gate refuses the patch as inert
    rather than reporting a delta nothing can exhibit."""
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    result = runner.invoke(
        cli,
        [
            "patch",
            "preview",
            str(patch_file),
            "--blueprint",
            str(tmp_path / "blueprint.yml"),
            "--config",
            str(tmp_path / "aqueduct.yml"),
            "-s",
            "engine.spark.conf.spark.sql.shuffle.partitions=999",
        ],
    )
    assert result.exit_code == exit_codes.DATA_OR_RUNTIME
    assert "guardrails gate blocked" in result.output
    assert "pinned by a -s/--set override" in result.output


# ── patch revert: --set reaches the resolved config (patch-store lookup) ───────


def test_patch_revert_set_reroutes_the_patch_store_used_to_read_the_applied_body(
    tmp_path, monkeypatch
):
    """`--set stores.blob.path=...` changes WHICH patch store `patch revert`
    reads the applied patch body from — proof `--set` reaches this
    command's resolved config. The equality check itself is untouched by
    this (see the two tests below); this test only exercises the lookup
    `_applied_patch_operations`/`_patch_store_from` perform.
    """
    monkeypatch.chdir(tmp_path)
    _touch_obs_store(tmp_path)
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    assert _apply(runner, tmp_path, patch_file).exit_code == exit_codes.SUCCESS

    applied = tmp_path / "patches" / "applied" / "00001_patch.json"
    assert applied.exists()
    body = applied.read_text(encoding="utf-8")

    alt_root = tmp_path / "alt_blob_root"
    alt_applied_dir = alt_root / "patches" / "applied"
    alt_applied_dir.mkdir(parents=True)
    (alt_applied_dir / "00001_patch.json").write_text(body, encoding="utf-8")
    applied.unlink()  # break the DEFAULT store's lookup

    # Without --set: the default store's applied/ dir is now empty.
    without = _revert(runner, tmp_path, "fix-shuffle")
    assert without.exit_code == exit_codes.DATA_OR_RUNTIME
    assert "applied patch body could not be read" in without.output

    # With --set pointed at the alt root: the body is found and reverted.
    with_set = _revert(
        runner,
        tmp_path,
        "fix-shuffle",
        "-s",
        "stores.blob.backend=local",
        "-s",
        f"stores.blob.path={alt_root}",
    )
    assert with_set.exit_code == exit_codes.SUCCESS, with_set.output
    after = yaml.safe_load((tmp_path / "blueprint.yml").read_text(encoding="utf-8"))
    assert after["engine"]["spark"]["conf"] == {}


# ── patch revert: the prior-values equality check stays UNPINNED ───────────────


def test_set_override_does_not_make_a_legitimate_revert_abort(tmp_path, monkeypatch):
    """The dangerous direction (1/2): a `--set` on the SAME key the patch
    recorded must NOT make an otherwise-clean revert abort. If the
    equality check used the --set-PINNED resolution, the pinned value
    (555) would differ from what the patch recorded (800) and the revert
    would incorrectly refuse."""
    monkeypatch.chdir(tmp_path)
    _touch_obs_store(tmp_path)
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    assert _apply(runner, tmp_path, patch_file).exit_code == exit_codes.SUCCESS

    result = _revert(
        runner,
        tmp_path,
        "fix-shuffle",
        "-s",
        "engine.spark.conf.spark.sql.shuffle.partitions=555",
    )
    assert result.exit_code == exit_codes.SUCCESS, result.output
    after = yaml.safe_load((tmp_path / "blueprint.yml").read_text(encoding="utf-8"))
    assert after["engine"]["spark"]["conf"] == {}
    assert after["healed_by"][0]["reverted_at"]


def test_set_override_does_not_let_a_diverged_revert_falsely_pass(tmp_path, monkeypatch):
    """The dangerous direction (2/2): a genuinely diverged Blueprint (hand-
    edited to 123 after the patch recorded 800) must still abort even when
    `--set` happens to name the SAME key with the value the patch recorded
    (800) — the coincidence that would mask real drift if the equality
    check were fed the pinned resolution instead of the unpinned one."""
    monkeypatch.chdir(tmp_path)
    _touch_obs_store(tmp_path)
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    assert _apply(runner, tmp_path, patch_file).exit_code == exit_codes.SUCCESS

    bp_path = tmp_path / "blueprint.yml"
    bp = yaml.safe_load(bp_path.read_text(encoding="utf-8"))
    bp["engine"]["spark"]["conf"]["spark.sql.shuffle.partitions"] = 123  # simulated hand edit
    bp_path.write_text(yaml.safe_dump(bp), encoding="utf-8")

    # Sanity: an ordinary (no --set) revert already refuses this drift.
    plain = _revert(runner, tmp_path, "fix-shuffle")
    assert plain.exit_code == exit_codes.DATA_OR_RUNTIME
    assert "now resolves to 123" in plain.output

    # The dangerous case: --set coincidentally names the RECORDED value.
    masked = _revert(
        runner,
        tmp_path,
        "fix-shuffle",
        "-s",
        "engine.spark.conf.spark.sql.shuffle.partitions=800",
    )
    assert masked.exit_code == exit_codes.DATA_OR_RUNTIME
    assert "now resolves to 123" in masked.output
    still_drifted = yaml.safe_load(bp_path.read_text(encoding="utf-8"))
    assert still_drifted["engine"]["spark"]["conf"]["spark.sql.shuffle.partitions"] == 123


# ── doctor: --set reaches the resolved config ───────────────────────────────────


_DOCTOR_YML = """
aqueduct_config: "1.0"
deployment:
  engine: spark
  target: local
agent:
  provider: anthropic
"""


def test_doctor_set_reaches_the_agent_check(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-key-not-real")
    config = tmp_path / "aqueduct.yml"
    config.write_text(_DOCTOR_YML, encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "doctor",
            str(config),
            "--skip-spark",
            "--format",
            "json",
            "-s",
            "agent.model=distinctive-test-model-xyz",
        ],
    )
    assert result.exit_code == exit_codes.SUCCESS, result.output
    payload = json.loads(result.output)
    agent_row = next(r for r in payload["checks"] if r["name"] == "agent")
    assert "distinctive-test-model-xyz" in agent_row["detail"]


def test_doctor_set_does_not_reach_the_healed_config_drift_check(tmp_path):
    """Same equality danger as `patch revert`'s: `check_healed_engine_config`
    compares a `healed_by` record's recorded value against the effective
    session config. A `--set` on that same key must not manufacture false
    drift (or hide real drift) for this diagnostic either.

    `engine_config_delta` no longer lives inline in the Blueprint's
    `healed_by:` record (see `aqueduct/patch/index.py::HealedByRecordSchema`)
    — the Blueprint carries only the bounded record, and the delta is seeded
    into the `patch_index` table of a real DuckDB observability store at the
    path `open_obs_read` resolves for this blueprint's `id`.
    """
    obs_path = tmp_path / "obs"
    config = tmp_path / "aqueduct.yml"
    config.write_text(
        _AQUEDUCT_YML + f"stores:\n  observability:\n    path: {obs_path}\n",
        encoding="utf-8",
    )
    bp = tmp_path / "blueprint.yml"
    bp.write_text(
        _BLUEPRINT.rstrip()
        + """
engine:
  spark:
    conf:
      spark.sql.shuffle.partitions: 800
healed_by:
  - patch_id: fix-shuffle
    engine: spark
    classification: engine_shaped
    applied_at: '2026-01-01T00:00:00+00:00'
""",
        encoding="utf-8",
    )
    from aqueduct.patch import index as _ix
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    store = DuckDBObservabilityStore(obs_path / _BLUEPRINT_ID / "observability.db")
    with store.connect() as cur:
        _ix.ensure_schema(cur)
        _ix.record_heal_provenance(
            cur,
            "fix-shuffle",
            engine="spark",
            engine_config_delta={
                "spark": {"spark.sql.shuffle.partitions": {"before": 200, "after": 800}}
            },
        )

    baseline = run_doctor(config_path=config, skip_spark=True, blueprint_path=bp)
    baseline_row = next(r for r in baseline if r.name.startswith("healed-config"))
    assert baseline_row.status == "ok"

    pinned = run_doctor(
        config_path=config,
        skip_spark=True,
        blueprint_path=bp,
        set_items=("engine.spark.conf.spark.sql.shuffle.partitions=999",),
    )
    pinned_row = next(r for r in pinned if r.name.startswith("healed-config"))
    # If --set had reached this check, the pinned resolution (999) would
    # no longer match the recorded `after` (800) and this would flip to
    # "warn" purely because of the flag — it must not.
    assert pinned_row.status == "ok"
    assert pinned_row.detail == baseline_row.detail
