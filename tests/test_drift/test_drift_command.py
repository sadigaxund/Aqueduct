"""Phase 58 — `aqueduct drift` CLI flow.

Integration of the command end to end with the Spark boundary stubbed
(`make_spark_session` + `read_source_schema`) and the agent mocked — so the
real compile → baseline → classify → heal → exit-code path is exercised without
a cluster or a live LLM.

Why stub via ``monkeypatch.setitem(sys.modules, …)``: the drift CLI ends with
``session.stop()``. ``make_spark_session`` uses ``getOrCreate``, so a real call
here would return the **session-scoped ``spark`` fixture** and stopping it would
break every later Spark test (``sc is None``). Stubbing ``make_spark_session`` to
a ``MagicMock`` makes ``.stop()`` a no-op and never touches a real session — and
``setitem`` auto-restores ``sys.modules`` after each test, so nothing leaks (and
it works whether or not ``pyspark`` is installed, since the drift CI job runs
``-m "not spark"``).
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

_AQ = "deployment:\n  engine: spark\nengine:\n  spark:\n    master_url: local[1]\nagent:\n  model: test-model\n"


@pytest.fixture
def project(tmp_path, monkeypatch):
    """Write a tiny project and stub the Spark boundary (never a real session)."""
    (tmp_path / "bp.yml").write_text(_BP)
    (tmp_path / "aqueduct.yml").write_text(_AQ)
    store = tmp_path / "store"
    store.mkdir()

    # Stub the two spark submodules the drift CLI lazily imports. A mutable
    # holder lets a test change the "live" schema between drift runs.
    schema_holder: dict[str, dict[str, str]] = {"schema": {"a": "int", "b": "string"}}

    session_stub = types.ModuleType("aqueduct.executor.spark.session")
    session_stub.make_spark_session = lambda *a, **k: MagicMock()
    ingress_stub = types.ModuleType("aqueduct.executor.spark.ingress")
    ingress_stub.read_source_schema = lambda mod, spark: schema_holder["schema"]

    # When pyspark is absent the parent package never imported — stub it too so
    # `from aqueduct.executor.spark.session import …` resolves. When present,
    # leave the real parent in place and only override the submodules.
    if "aqueduct.executor.spark" not in sys.modules:
        parent = types.ModuleType("aqueduct.executor.spark")
        monkeypatch.setitem(sys.modules, "aqueduct.executor.spark", parent)
    else:
        parent = sys.modules["aqueduct.executor.spark"]
    # Seed sys.modules BEFORE the setattr calls below: monkeypatch.setattr's
    # save-original step does a plain getattr(parent, "ingress"), which
    # triggers this package's PEP 562 __getattr__("ingress") ->
    # importlib.import_module("aqueduct.executor.spark.ingress"). With the
    # stub already cached in sys.modules, that import returns the stub
    # instead of importing the real submodule (`from pyspark.sql import
    # SparkSession` at module level) — the fix for the pyspark-less
    # ModuleNotFoundError this fixture used to raise when ordered the other
    # way round.
    monkeypatch.setitem(sys.modules, "aqueduct.executor.spark.session", session_stub)
    monkeypatch.setitem(sys.modules, "aqueduct.executor.spark.ingress", ingress_stub)
    monkeypatch.setattr(parent, "session", session_stub, raising=False)
    monkeypatch.setattr(parent, "ingress", ingress_stub, raising=False)

    return tmp_path, store, schema_holder


def _invoke(tmp_path, store):
    return CliRunner().invoke(
        cli,
        [
            "drift",
            str(tmp_path / "bp.yml"),
            "--config",
            str(tmp_path / "aqueduct.yml"),
            "--store-dir",
            str(store),
        ],
    )


def test_first_run_sets_baseline(project):
    tmp_path, store, schema = project
    schema["schema"] = {"a": "int", "b": "string"}

    res = _invoke(tmp_path, store)
    assert res.exit_code == 0, res.output
    assert "baseline established" in res.output


def test_breaking_drift_stages_patch_and_exits_heal_pending(project, monkeypatch):
    tmp_path, store, schema = project
    import aqueduct.agent as agent

    # Run 1 → baseline with both columns.
    schema["schema"] = {"a": "int", "b": "string"}
    assert _invoke(tmp_path, store).exit_code == 0

    # Run 2 → column b dropped (breaking); mock the agent staging a patch.
    schema["schema"] = {"a": "int"}
    fake = MagicMock()
    fake.patch = MagicMock(patch_id="pid-123")
    monkeypatch.setattr(agent, "generate_agent_patch", lambda *a, **k: fake)
    monkeypatch.setattr(agent, "stage_patch_for_human", lambda *a, **k: None)

    res = _invoke(tmp_path, store)
    assert res.exit_code == exit_codes.HEAL_PENDING, res.output
    assert "breaking drift" in res.output
    assert "column 'b' dropped" in res.output
    assert "pid-123" in res.output


def test_benign_addition_does_not_heal(project):
    tmp_path, store, schema = project

    schema["schema"] = {"a": "int"}
    assert _invoke(tmp_path, store).exit_code == 0  # baseline

    # add a column → benign, no heal, exit 0
    schema["schema"] = {"a": "int", "z": "string"}
    res = _invoke(tmp_path, store)
    assert res.exit_code == 0, res.output
    assert "benign" in res.output


def test_drift_compile_resolves_real_depot_value_with_run_namespacing(project, monkeypatch):
    """`aqueduct drift` compiles with a real depot wired in (Item 1) — a
    Blueprint using `@aq.depot.get()` in an Ingress path resolves the value a
    real `aqueduct run` (`aqueduct.stores.get_stores`) would have written for
    this SAME blueprint_id, not the literal default."""
    tmp_path, store, schema = project

    (tmp_path / "aqueduct.yml").write_text(
        _AQ
        + f"stores:\n  depots:\n    default:\n      backend: duckdb\n      path: {tmp_path}/depot.db\n"
    )
    bp_with_depot = _BP.replace(
        "config: { format: parquet, path: data/in }",
        "config: { format: parquet, path: \"data/@aq.depot.get('watermark','2020-01-01')/in\" }",
    )
    (tmp_path / "bp.yml").write_text(bp_with_depot)

    # Write the depot value the SAME way a real `aqueduct run` would — through
    # `get_stores(cfg, blueprint_id=...)`, which namespaces the key.
    from aqueduct.config import load_config
    from aqueduct.depot.depot import DepotStore
    from aqueduct.stores import get_stores

    cfg = load_config(tmp_path / "aqueduct.yml")
    bundle = get_stores(cfg, blueprint_id="drift.demo")  # bp.yml's `id:`
    DepotStore(backend=bundle.depot).put("watermark", "2099-12-31")

    captured: dict[str, str] = {}
    ingress_mod = sys.modules["aqueduct.executor.spark.ingress"]
    orig_read = ingress_mod.read_source_schema

    def _spy(mod, spark):
        captured["path"] = mod.config.get("path")
        return orig_read(mod, spark)

    monkeypatch.setattr(ingress_mod, "read_source_schema", _spy)

    schema["schema"] = {"a": "int", "b": "string"}
    res = _invoke(tmp_path, store)
    assert res.exit_code == 0, res.output
    assert captured["path"] == str(tmp_path / "data" / "2099-12-31" / "in")


def test_drift_unconfigured_depot_raises_when_config_load_fails(project, monkeypatch):
    """When even loading config fails / a depot cannot be built, the preview
    depot wiring falls back gracefully and the pre-existing loud
    `_depot_get_or_raise` CompileError backstop still fires — Blueprint
    compilation must never crash with an unrelated traceback."""
    tmp_path, store, schema = project

    bp_with_depot = _BP.replace(
        "config: { format: parquet, path: data/in }",
        "config: { format: parquet, path: \"data/@aq.depot.get('watermark')/in\" }",
    )
    (tmp_path / "bp.yml").write_text(bp_with_depot)

    # Force the shared preview-depot helper to fail, simulating a depot
    # backend that cannot be constructed (e.g. unreachable) — the CLI must
    # fall back to depot=None rather than propagate the exception.
    import aqueduct.depot.depot as depot_mod

    def _boom(cfg, blueprint_id):
        raise RuntimeError("simulated depot construction failure")

    monkeypatch.setattr(depot_mod, "preview_depots", _boom)

    res = _invoke(tmp_path, store)
    assert res.exit_code == exit_codes.CONFIG_ERROR, res.output
    assert "no depot backend is configured" in res.output
