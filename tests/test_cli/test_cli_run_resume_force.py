"""P4 — ``aqueduct run --resume`` must fail closed on a manifest-hash
mismatch, with a ``--force`` override.

Investigation (see ``aqueduct/cli/run.py``, the "P4: --resume fails closed"
block right after the Manifest compiles): two independent checkpoint
mechanisms exist, both keyed off ``aqueduct.executor.models.manifest_hash``.

  - Module checkpoints (``checkpoint_root``/``store_dir/checkpoints``) store
    the ORIGINAL run's manifest hash in ``<base>/<run_id>/_manifest_hash``.
    Both engines' ``execute()`` already compare it on resume, but only ever
    WARN (``runtime_resume_hash_changed``) and proceed — a deliberate,
    separately-tested permissive contract at the engine layer (see
    ``tests/test_executor_duckdb/test_executor.py::
    test_resume_mismatched_manifest_warns_and_continues``) left untouched
    here. This module tests the NEW hard-refusal check one layer up, in the
    CLI, before any engine session is built.

  - Handoff spill (polyglot Blueprints) lays out
    ``<handoff.root>/<manifest_hash>/<run_id>/`` keyed STRICTLY by the
    CURRENT hash — a mismatch there was previously unreachable/undetectable
    (it just silently finds nothing and re-executes fresh). Covered
    separately at the unit level for ``find_run_under_other_hash`` in
    ``tests/test_executor/test_spill.py`` if present; this module only
    exercises the single-engine (module-checkpoint) path end-to-end via the
    CLI, since that is the common case and the one with a real detectable
    stored hash.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = [pytest.mark.integration, pytest.mark.duckdb]

_BP = """\
aqueduct: '1.0'
id: resume_force_bp
name: Resume Force BP
agent:
  approval: disabled
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: {src_path}}}
    checkpoint: true
  - id: sink
    type: Egress
    label: Sink
    config: {{format: parquet, path: {out_path}, mode: overwrite}}
edges:
  - from: src
    to: sink
"""

_CFG = """\
aqueduct_config: "1.0"

deployment:
  engine: duckdb

stores:
  observability:
    backend: duckdb
    path: "{obs}"
  depots:
    default:
      backend: duckdb
      path: "{dep}"
"""


def _write_project(tmp_path: Path, *, out_name: str = "out.parquet") -> tuple[Path, Path]:
    src_path = tmp_path / "src.csv"
    src_path.write_text("a\n1\n2\n3\n", encoding="utf-8")
    out_path = tmp_path / out_name

    bp = tmp_path / "bp.yml"
    bp.write_text(_BP.format(src_path=src_path, out_path=out_path), encoding="utf-8")

    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        _CFG.format(obs=str(tmp_path / "obs"), dep=str(tmp_path / "dep.duckdb")),
        encoding="utf-8",
    )
    return bp, cfg


def _run(runner: CliRunner, *args: str) -> object:
    return runner.invoke(cli, list(args))


def test_force_without_resume_is_usage_error(tmp_path):
    bp, cfg = _write_project(tmp_path)
    runner = CliRunner()
    result = _run(runner, "run", str(bp), "--config", str(cfg), "--force")
    assert result.exit_code == 64, result.output  # USAGE_ERROR (sysexits EX_USAGE)
    assert "--force" in result.output
    assert "--resume" in result.output


def test_resume_matching_hash_resumes_normally(tmp_path):
    bp, cfg = _write_project(tmp_path)
    store_dir = tmp_path / "store"
    runner = CliRunner()

    r1 = _run(
        runner,
        "run",
        str(bp),
        "--config",
        str(cfg),
        "--store-dir",
        str(store_dir),
        "--run-id",
        "r1",
    )
    assert r1.exit_code == exit_codes.SUCCESS, r1.output

    # Same blueprint recompiled — same manifest hash — resume must proceed
    # exactly as before this change.
    r2 = _run(
        runner,
        "run",
        str(bp),
        "--config",
        str(cfg),
        "--store-dir",
        str(store_dir),
        "--run-id",
        "r2",
        "--resume",
        "r1",
    )
    assert r2.exit_code == exit_codes.SUCCESS, r2.output


def test_resume_mismatched_hash_refused_without_force(tmp_path):
    bp, cfg = _write_project(tmp_path)
    store_dir = tmp_path / "store"
    runner = CliRunner()

    r1 = _run(
        runner,
        "run",
        str(bp),
        "--config",
        str(cfg),
        "--store-dir",
        str(store_dir),
        "--run-id",
        "r1",
    )
    assert r1.exit_code == exit_codes.SUCCESS, r1.output

    # Recompile a DIFFERENT blueprint (different Egress path) against the
    # SAME store — changes the manifest hash without touching the checkpoint
    # layout.
    bp2, _ = _write_project(tmp_path, out_name="out2.parquet")

    stored_hash = (
        (store_dir / "checkpoints" / "r1" / "_manifest_hash").read_text(encoding="utf-8").strip()
    )

    r2 = _run(
        runner,
        "run",
        str(bp2),
        "--config",
        str(cfg),
        "--store-dir",
        str(store_dir),
        "--run-id",
        "r2",
        "--resume",
        "r1",
    )
    assert r2.exit_code == exit_codes.CONFIG_ERROR, r2.output
    assert stored_hash in r2.output
    assert "r1" in r2.output
    # New Egress output must NOT have been written — the run was refused
    # before any execution happened.
    assert not (tmp_path / "out2.parquet").exists()


def test_resume_mismatched_hash_with_force_proceeds(tmp_path):
    bp, cfg = _write_project(tmp_path)
    store_dir = tmp_path / "store"
    runner = CliRunner()

    r1 = _run(
        runner,
        "run",
        str(bp),
        "--config",
        str(cfg),
        "--store-dir",
        str(store_dir),
        "--run-id",
        "r1",
    )
    assert r1.exit_code == exit_codes.SUCCESS, r1.output

    bp2, _ = _write_project(tmp_path, out_name="out2.parquet")

    r2 = _run(
        runner,
        "run",
        str(bp2),
        "--config",
        str(cfg),
        "--store-dir",
        str(store_dir),
        "--run-id",
        "r2",
        "--resume",
        "r1",
        "--force",
    )
    assert r2.exit_code == exit_codes.SUCCESS, r2.output
    assert (tmp_path / "out2.parquet").exists()
