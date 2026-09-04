"""`aqueduct run` refuses a second concurrent run of the same Blueprint.

Unit coverage of the lock itself is in `tests/test_stores/test_run_lock.py`;
this file pins the CLI wiring: that `run` takes the lock at all, that the
refusal exits CONFIG_ERROR with a message the user can act on, and that a
completed run leaves the lock free.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli
from aqueduct.stores.run_lock import LOCK_FILENAME, blueprint_run_lock

pytestmark = pytest.mark.integration

_BP = """\
aqueduct: '1.0'
id: run_lock_bp
name: Run Lock BP
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: {in_path}}}
  - id: sink
    type: Egress
    label: Sink
    config: {{format: csv, path: {out_path}, mode: overwrite}}
edges:
  - from: src
    to: sink
"""

_CFG = 'aqueduct_config: "1.0"\n\ndeployment:\n  engine: duckdb\n'


@pytest.fixture
def project(tmp_path):
    (tmp_path / "in.csv").write_text("a,b\n1,2\n")
    bp = tmp_path / "bp.yml"
    bp.write_text(
        _BP.format(in_path=tmp_path / "in.csv", out_path=tmp_path / "out.csv"),
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_CFG, encoding="utf-8")
    store = tmp_path / "store"
    store.mkdir()
    return bp, cfg, store


def _invoke(bp, cfg, store, *extra):
    return CliRunner().invoke(
        cli,
        ["run", str(bp), "--config", str(cfg), "--store-dir", str(store), *extra],
    )


def test_a_run_succeeds_and_leaves_the_lock_free(project):
    bp, cfg, store = project
    res = _invoke(bp, cfg, store)
    assert res.exit_code == 0, (res.exit_code, res.output, res.exception)
    # The lock file stays on disk (flock is advisory, the file is the token),
    # but nothing holds it, so a fresh acquisition succeeds.
    with blueprint_run_lock(store, "run_lock_bp") as label:
        assert label == str(Path(store) / LOCK_FILENAME)


def test_a_second_run_is_refused_while_the_lock_is_held(project):
    bp, cfg, store = project
    with blueprint_run_lock(store, "run_lock_bp"):
        res = _invoke(bp, cfg, store)
    assert res.exit_code == exit_codes.CONFIG_ERROR, (res.exit_code, res.output)
    assert "run_lock_bp" in res.output
    assert "--wait-for-lock" in res.output


def test_the_refused_run_executed_nothing(project):
    """A refusal must happen before any module runs, so the Egress target
    stays untouched."""
    bp, cfg, store = project
    out = Path(str(bp.parent / "out.csv"))
    with blueprint_run_lock(store, "run_lock_bp"):
        res = _invoke(bp, cfg, store)
    assert res.exit_code == exit_codes.CONFIG_ERROR
    assert not out.exists(), "the refused run wrote output before refusing"


def test_a_failed_run_still_releases_the_lock(project):
    """The lock is released in `run`'s outer finally, so a run that exits
    non-zero does not strand it for every later invocation."""
    bp, cfg, store = project
    bad = bp.parent / "bad.yml"
    bad.write_text(
        _BP.format(in_path=bp.parent / "missing.csv", out_path=bp.parent / "o2.csv"),
        encoding="utf-8",
    )
    res = _invoke(bad, cfg, store)
    assert res.exit_code != 0, "the fixture blueprint was supposed to fail"
    with blueprint_run_lock(store, "run_lock_bp") as label:
        assert label is not None, "a failed run stranded the lock"
