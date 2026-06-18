"""Phase 58 — `aqueduct drift` CLI flow.

Integration of the command end to end with the Spark boundary stubbed
(`make_spark_session` + `read_source_schema`) and the agent mocked — so the
real compile → baseline → classify → heal → exit-code path is exercised without
a cluster or a live LLM.

The ``project`` fixture pre-seeds ``sys.modules`` for the spark submodules so
the test works even when ``pyspark`` is not installed (the CI drift job runs
with ``-m "not spark"``). The real spark modules are never loaded — only the
functions needed by the drift CLI flow are mocked via ``monkeypatch.setattr`` on
the pre-seeded stubs.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.unit

_BP = """aqueduct: "1.0"
id: drift.demo
name: D
modules:
  - id: load
    type: Ingress
    label: L
    config: { format: parquet, path: data/in }
  - id: c
    type: Channel
    label: C
    config: { op: sql, query: "SELECT a, b FROM load" }
edges:
  - { from: load, to: c }
"""

_AQ = "deployment:\n  engine: spark\n  master_url: local[1]\nagent:\n  model: test-model\n"


def _stub_spark_modules():
    """Pre-seed ``sys.modules`` with minimal stubs for spark submodules.

    The drift CLI lazily imports ``make_spark_session`` from
    ``aqueduct.executor.spark.session`` and ``read_source_schema`` from
    ``aqueduct.executor.spark.ingress`` at call time.  Without ``pyspark``
    installed these imports fail.  Pre-seeding stubs (which are then
    monkeypatched by the tests) lets the test exercise the drift flow without
    ever loading the real spark modules.

    When pyspark IS installed these stubs are never used (the real spark
    submodules are already in ``sys.modules``).
    """
    mods: dict[str, types.ModuleType] = {
        "aqueduct.executor.spark": types.ModuleType("aqueduct.executor.spark"),
        "aqueduct.executor.spark.session": types.ModuleType("aqueduct.executor.spark.session"),
        "aqueduct.executor.spark.ingress": types.ModuleType("aqueduct.executor.spark.ingress"),
    }
    for name, mod in mods.items():
        if name not in sys.modules:
            sys.modules[name] = mod
    # Wire parent-package bindings so dotted-import resolution works.
    import aqueduct.executor
    for name, mod in mods.items():
        parts = name.split(".")
        parent = ".".join(parts[:-1])
        attr = parts[-1]
        if parent in sys.modules:
            setattr(sys.modules[parent], attr, mod)
    # Default mocks — overridden per-test via monkeypatch.
    sys.modules["aqueduct.executor.spark.session"].make_spark_session = MagicMock()
    sys.modules["aqueduct.executor.spark.ingress"].read_source_schema = MagicMock(
        return_value={"a": "int", "b": "string"}
    )


# Seed stubs at module load time so collection doesn't trigger real spark imports.
_stub_spark_modules()


@pytest.fixture
def project(tmp_path):
    (tmp_path / "bp.yml").write_text(_BP)
    (tmp_path / "aqueduct.yml").write_text(_AQ)
    store = tmp_path / "store"
    store.mkdir()
    return tmp_path, store


def _invoke(tmp_path, store):
    return CliRunner().invoke(
        cli, ["drift", str(tmp_path / "bp.yml"),
              "--config", str(tmp_path / "aqueduct.yml"), "--store-dir", str(store)],
    )


def test_first_run_sets_baseline(project, monkeypatch):
    tmp_path, store = project
    import aqueduct.executor.spark.ingress as ing
    monkeypatch.setattr(ing, "read_source_schema", lambda mod, spark: {"a": "int", "b": "string"})

    res = _invoke(tmp_path, store)
    assert res.exit_code == 0, res.output
    assert "baseline established" in res.output


def test_breaking_drift_stages_patch_and_exits_heal_pending(project, monkeypatch):
    tmp_path, store = project
    import aqueduct.agent as agent
    import aqueduct.executor.spark.ingress as ing

    # Run 1 → baseline with both columns.
    monkeypatch.setattr(ing, "read_source_schema", lambda mod, spark: {"a": "int", "b": "string"})
    assert _invoke(tmp_path, store).exit_code == 0

    # Run 2 → column b dropped (breaking); mock the agent staging a patch.
    monkeypatch.setattr(ing, "read_source_schema", lambda mod, spark: {"a": "int"})
    fake = MagicMock()
    fake.patch = MagicMock(patch_id="pid-123")
    monkeypatch.setattr(agent, "generate_agent_patch", lambda *a, **k: fake)
    monkeypatch.setattr(agent, "stage_patch_for_human", lambda *a, **k: None)

    res = _invoke(tmp_path, store)
    assert res.exit_code == exit_codes.HEAL_PENDING, res.output
    assert "breaking drift" in res.output
    assert "column 'b' dropped" in res.output
    assert "pid-123" in res.output


def test_benign_addition_does_not_heal(project, monkeypatch):
    tmp_path, store = project
    import aqueduct.executor.spark.ingress as ing

    monkeypatch.setattr(ing, "read_source_schema", lambda mod, spark: {"a": "int"})
    assert _invoke(tmp_path, store).exit_code == 0  # baseline

    # add a column → benign, no heal, exit 0
    monkeypatch.setattr(ing, "read_source_schema", lambda mod, spark: {"a": "int", "z": "string"})
    res = _invoke(tmp_path, store)
    assert res.exit_code == 0, res.output
    assert "benign" in res.output
