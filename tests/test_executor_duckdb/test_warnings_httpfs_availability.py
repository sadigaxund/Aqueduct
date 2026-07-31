"""DuckDB session-startup warning: httpfs availability for remote paths.

Mirrors the shape of ``tests/test_executor/test_warnings/`` (Spark's
jar_availability rule tests) applied to DuckDB's own lifecycle — see
``aqueduct/executor/duckdb_/warnings/httpfs_availability.py``'s module
docstring for why the mechanisms differ but the diagnostic shape doesn't.
"""

from __future__ import annotations

import pytest

from aqueduct.executor.duckdb_.warnings import RULES, run_all
from aqueduct.executor.duckdb_.warnings.httpfs_availability import RULE_ID, check
from aqueduct.models import Manifest, Module

pytestmark = pytest.mark.duckdb


def _manifest(*modules: Module) -> Manifest:
    return Manifest(blueprint_id="bp", context={}, modules=tuple(modules), edges=(), spark_config={})


def _ingress(path: str) -> Module:
    return Module(id="i", type="Ingress", label="i", config={"format": "parquet", "path": path})


class TestCheck:
    def test_no_remote_paths_is_silent(self, duckdb_con):
        assert check(_manifest(_ingress("/local/data.parquet")), duckdb_con) == []

    def test_remote_path_warns_when_httpfs_not_loaded(self, duckdb_con):
        msgs = check(_manifest(_ingress("s3://bucket/data.parquet")), duckdb_con)
        assert len(msgs) == 1
        assert "'i'" in msgs[0]
        assert "network" in msgs[0]
        assert "extension_repository" in msgs[0]

    def test_remote_path_silent_once_httpfs_loaded(self, duckdb_con):
        """Once httpfs is actually loaded (e.g. an earlier query already
        triggered autoload), the warning no longer fires — it is advisory
        for the not-yet-loaded case only."""
        try:
            duckdb_con.execute("INSTALL httpfs")
            duckdb_con.execute("LOAD httpfs")
        except Exception:
            pytest.skip("httpfs not installable in this environment (no network)")
        assert check(_manifest(_ingress("s3://bucket/data.parquet")), duckdb_con) == []

    def test_egress_remote_path_also_detected(self, duckdb_con):
        m = _manifest(
            Module(id="e", type="Egress", label="e", config={"format": "parquet", "path": "gs://bucket/out.parquet", "mode": "overwrite"})
        )
        msgs = check(m, duckdb_con)
        assert len(msgs) == 1
        assert "'e'" in msgs[0]


class TestRunAll:
    def test_registered_in_rules(self):
        rule_ids = [rid for rid, _ in RULES]
        assert RULE_ID in rule_ids

    def test_run_all_respects_suppress(self, duckdb_con):
        m = _manifest(_ingress("s3://bucket/data.parquet"))
        assert run_all(m, duckdb_con, suppress=None) != []
        assert run_all(m, duckdb_con, suppress={RULE_ID}) == []

    def test_run_all_swallows_rule_exceptions(self, duckdb_con, monkeypatch):
        def _boom(_manifest, _con):
            raise RuntimeError("boom")

        monkeypatch.setattr(
            "aqueduct.executor.duckdb_.warnings.RULES", [("broken_rule", _boom)],
        )
        assert run_all(_manifest(_ingress("/x.parquet")), duckdb_con) == []


class TestWiredThroughExecute:
    """`duckdb_/executor.py::execute()` accepted `warnings_suppress`/
    `warnings_silence_all` in its signature but never read either — a
    silent no-op fixed as part of this task (the fix gave the DuckDB
    session-startup warning pass, above, a real caller). These prove the
    wiring end to end via a monkeypatched rule set, avoiding any dependency
    on real remote storage or httpfs install."""

    def test_execute_emits_session_startup_warning(self, monkeypatch):
        import warnings as _w

        from aqueduct.executor.duckdb_.executor import execute

        monkeypatch.setattr(
            "aqueduct.executor.duckdb_.warnings.run_all",
            lambda manifest, con, suppress=None: [(RULE_ID, "sentinel warning text")],
        )
        con_local = __import__("duckdb").connect(":memory:")
        manifest = Manifest(blueprint_id="bp", context={}, modules=(), edges=(), spark_config={})
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            execute(manifest, con_local)
        con_local.close()
        assert any("sentinel warning text" in str(w.message) for w in caught)

    def test_execute_silences_all_when_flag_set(self, monkeypatch):
        import warnings as _w

        from aqueduct.executor.duckdb_.executor import execute

        monkeypatch.setattr(
            "aqueduct.executor.duckdb_.warnings.run_all",
            lambda manifest, con, suppress=None: [(RULE_ID, "sentinel warning text")],
        )
        con_local = __import__("duckdb").connect(":memory:")
        manifest = Manifest(blueprint_id="bp", context={}, modules=(), edges=(), spark_config={})
        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            execute(manifest, con_local, warnings_silence_all=True)
        con_local.close()
        assert not any("sentinel warning text" in str(w.message) for w in caught)
