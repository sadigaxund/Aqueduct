"""`aqueduct drift` must route each Ingress module's schema read through its
OWN resolved engine, not a hardcoded ``"spark"`` (Phase 85 Wave 4 P2).

``tooling.drift_schema_read`` is declared ``supported`` for BOTH engines
(``aqueduct/executor/{spark,duckdb_}/capabilities.yml``) — this is a routing
fix, not a capability gate: a DuckDB-resolved Ingress module must read via
``aqueduct.executor.duckdb_.schema_reader.read_source_schema``
(``ExecutorProtocol.read_source_schema``), and Spark's reader must never be
imported/called for it. No real Spark session is built on this path, so this
stays a pure unit test (no ``pytest.mark.spark`` needed) — the drift CI job
already runs ``-m "not spark"``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.unit


@pytest.fixture
def duckdb_project(tmp_path):
    """A pure-DuckDB project with one real CSV Ingress source — real enough
    that DuckDB's schema reader (metadata-only) can answer for real, so this
    test proves the DUCKDB reader actually ran, not merely that Spark's
    didn't."""
    src_csv = tmp_path / "orders.csv"
    src_csv.write_text("a,b\n1,x\n2,y\n", encoding="utf-8")

    (tmp_path / "bp.yml").write_text(
        f"""
aqueduct: "1.0"
id: drift.duckdb.demo
name: D
modules:
  - id: load
    type: Ingress
    label: L
    config: {{ format: csv, path: {src_csv.name} }}
  - id: c
    type: Channel
    label: C
    config: {{ op: sql, query: "SELECT a, b FROM load" }}
edges:
  - {{ from: load, to: c }}
"""
    )
    (tmp_path / "aqueduct.yml").write_text(
        'aqueduct_config: "2.0"\ndeployment:\n  engine: duckdb\nagent:\n  model: test-model\n'
    )
    store = tmp_path / "store"
    store.mkdir()
    return tmp_path, store


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


def test_duckdb_module_reads_via_duckdb_schema_reader_not_spark(duckdb_project, monkeypatch):
    tmp_path, store = duckdb_project

    # Spy on the REAL DuckDB reader to prove it actually ran ...
    from aqueduct.executor.duckdb_ import schema_reader as duckdb_schema_reader

    real_read = duckdb_schema_reader.read_source_schema
    spy = MagicMock(side_effect=real_read)
    monkeypatch.setattr(duckdb_schema_reader, "read_source_schema", spy)

    # ... and poison Spark's reader/session so calling either fails the test
    # loudly instead of silently succeeding via the wrong engine.
    def _boom(*a, **kw):
        raise AssertionError("drift must not read a duckdb module's schema via Spark")

    # Only installable when pyspark is importable. `raising=False` does NOT
    # help here: it suppresses AttributeError on a missing attribute, not the
    # ImportError `monkeypatch.setattr` raises while importing the dotted
    # target — and `aqueduct.executor.spark.session` imports pyspark at module
    # level. The `drift-tests` CI lane installs no pyspark, so poisoning there
    # would fail the test for the very reason it is asserting against.
    #
    # Skipping the poison without pyspark loses nothing: with pyspark absent,
    # ANY attempt to route a duckdb module through Spark raises
    # ModuleNotFoundError on its own, which is a strictly louder failure than
    # the AssertionError above. The `spy.assert_called_once()` below is the
    # positive half of the proof and runs in both environments.
    try:
        import pyspark  # noqa: F401
    except ImportError:
        pass
    else:
        monkeypatch.setattr(
            "aqueduct.executor.spark.ingress.read_source_schema", _boom, raising=False
        )
        monkeypatch.setattr(
            "aqueduct.executor.spark.session.make_spark_session", _boom, raising=False
        )

    res = _invoke(tmp_path, store)
    assert res.exit_code == exit_codes.SUCCESS, res.output
    assert "baseline established" in res.output
    spy.assert_called_once()
