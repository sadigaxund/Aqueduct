"""Guard: ``aqueduct run`` on a duckdb-engine Blueprint must succeed on a
base install with NO pyspark — the exact bug this test is named for.

Root cause (fixed alongside this test): ``aqueduct/cli/run.py``'s ``run()``
command unconditionally did ``from aqueduct.executor import ExecuteError`` —
regardless of ``deployment.engine``. That name was resolved lazily via
``aqueduct.executor.__init__.__getattr__``, which (before the fix) imported
it from ``aqueduct.executor.spark.executor`` — dragging ``pyspark`` into
EVERY ``aqueduct run``, including a pure-DuckDB one, and crashing with a bare
``ImportError: No module named pyspark`` (exit code 1, zero output) on any
install that took only the BASE dependencies (``duckdb`` — Phase 78 Stage A —
is a base dep; ``pyspark`` is the optional ``[spark]`` extra).
``ExecuteError`` now lives in ``aqueduct.errors`` (engine-agnostic, no
pyspark anywhere in its import chain) and both engines' executors raise
that SAME type — see ``aqueduct/errors.py::ExecuteError``.

pyspark is very likely already importED (module cached in ``sys.modules``)
by the time any test in this suite runs, so an in-process
``sys.meta_path`` blocker would prove nothing — Python checks
``sys.modules`` before consulting meta_path finders. This test spawns a
FRESH subprocess, installs the blocker as the very first statement (before
``aqueduct`` — or anything else — is imported), and only then imports
``aqueduct.cli`` and drives ``aqueduct run`` in-process via Click's
``CliRunner`` inside that subprocess — the same shape used by
``tests/test_capabilities/test_executor_protocol.py``'s
``test_protocol_module_importable_without_pyspark`` /
``test_spark_engine_module_importable_without_pyspark``.

Falsifiability (verified by hand while writing this guard, not re-checked by
CI on every run): temporarily reverting
``aqueduct/executor/__init__.py``'s ``ExecuteError`` branch back to
``from aqueduct.executor.spark.executor import ExecuteError`` makes this
test fail with the reported ``ImportError: No module named pyspark
(simulated)`` and exit_code 1; restoring the fix makes it pass again.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.duckdb]

_REPO = Path(__file__).resolve().parents[2]

_BP = """\
aqueduct: '1.0'
id: no_pyspark_bp
name: No Pyspark BP
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

_CFG = """\
aqueduct_config: "1.0"

deployment:
  engine: duckdb
"""


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=str(_REPO),
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_run_duckdb_blueprint_succeeds_without_pyspark(tmp_path):
    """``aqueduct run`` on a duckdb-engine Blueprint must succeed even when
    pyspark cannot be imported at all — the base ``aqueduct-core`` (no
    ``[spark]`` extra) install path. Uses a real DuckDB run (no mocking of
    ``execute``) so a regression that reintroduces a pyspark import ANYWHERE
    on this path is caught, not just the one line that broke it originally.
    """
    in_path = tmp_path / "in.csv"
    in_path.write_text("a,b\n1,2\n3,4\n")
    out_path = tmp_path / "out.csv"
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(_BP.format(in_path=in_path, out_path=out_path), encoding="utf-8")
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text(_CFG, encoding="utf-8")

    proc = _run(f"""
        import sys

        class _BlockPyspark:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError("No module named pyspark (simulated)")
                return None

        _blocker = _BlockPyspark()
        sys.meta_path.insert(0, _blocker)
        try:
            from click.testing import CliRunner
            from aqueduct.cli import cli

            runner = CliRunner()
            result = runner.invoke(cli, ["run", {str(bp_path)!r}, "--config", {str(cfg_path)!r}])
            assert "pyspark" not in sys.modules, (
                "pyspark got imported despite the block — the run path is "
                "not actually pyspark-free"
            )
            assert result.exit_code == 0, (
                f"exit_code={{result.exit_code}} output={{result.output!r}} "
                f"exc={{result.exception!r}}"
            )
        finally:
            sys.meta_path.remove(_blocker)
        print("OK")
    """)
    assert proc.returncode == 0, (
        f"aqueduct run on duckdb crashed with pyspark blocked\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    assert "OK" in proc.stdout, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert out_path.exists(), "duckdb run did not actually write the Egress output"
