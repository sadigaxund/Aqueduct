"""Tests for the two cross-engine handoff doctor checks (Phase 81/82
remediation): free disk space at ``handoff.root``
(``aqueduct.doctor.checks_io.check_handoff_free_space``) and per-engine
round-trip access (``aqueduct.doctor.check_handoff_engine_access``).

Both are pure/unit — no live Spark session in the default path (the Spark
half of the access check only runs a real SparkSession under
``--preflight``, exercised in ``tests/test_cli/test_cli_doctor_preflight.py``
style elsewhere; this file sticks to what is truthfully unit-testable
without a JVM: the free-space heuristic, the remote-URI skip, the DuckDB
round-trip (duckdb is a base dependency, not gated by the ``duckdb`` marker
which is reserved for the DuckDB execution engine), the default-skip for
Spark, and the unknown-engine fallback).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.doctor import check_handoff_engine_access
from aqueduct.doctor.checks_io import check_handoff_free_space
from aqueduct.executor.capabilities import CAPABILITY_REGISTRY, EngineCapabilities, register

pytestmark = pytest.mark.unit


# ── check_handoff_free_space ────────────────────────────────────────────────


class TestCheckHandoffFreeSpace:
    def test_remote_root_is_skipped(self, tmp_path):
        r = check_handoff_free_space("s3://bucket/handoff", tmp_path)
        assert r.status == "skip"
        assert "remote" in r.detail.lower()

    def test_local_existing_root_reports_ok(self, tmp_path):
        root = tmp_path / "handoff"
        root.mkdir()
        r = check_handoff_free_space(str(root), tmp_path)
        assert r.status in ("ok", "warn")  # depends on the test host's real free space
        assert str(root) in r.detail
        assert "GiB free" in r.detail

    def test_root_not_yet_created_walks_up_to_existing_ancestor(self, tmp_path):
        # `.aqueduct/handoff` does not exist yet — the check must not fail,
        # it should walk up to `tmp_path` (which does exist) and report that.
        r = check_handoff_free_space(".aqueduct/handoff", tmp_path)
        assert r.status in ("ok", "warn")
        assert "not created yet" in r.detail
        assert str(tmp_path) in r.detail

    def test_relative_root_resolves_against_project_root(self, tmp_path):
        (tmp_path / "existing").mkdir()
        r = check_handoff_free_space("existing", tmp_path)
        assert str(tmp_path / "existing") in r.detail

    def test_low_free_space_warns(self, tmp_path, monkeypatch):
        import shutil as _shutil

        root = tmp_path / "handoff"
        root.mkdir()

        class _Usage:
            total = 100 * 1024**3
            used = 99 * 1024**3
            free = 1 * 1024**3  # 1 GiB — below the 5 GiB warn threshold

        monkeypatch.setattr(_shutil, "disk_usage", lambda _p: _Usage())
        r = check_handoff_free_space(str(root), tmp_path)
        assert r.status == "warn"
        assert "1.0 GiB free" in r.detail

    def test_ample_free_space_ok(self, tmp_path, monkeypatch):
        import shutil as _shutil

        root = tmp_path / "handoff"
        root.mkdir()

        class _Usage:
            total = 500 * 1024**3
            used = 100 * 1024**3
            free = 400 * 1024**3

        monkeypatch.setattr(_shutil, "disk_usage", lambda _p: _Usage())
        r = check_handoff_free_space(str(root), tmp_path)
        assert r.status == "ok"

    def test_disk_usage_error_warns_not_fails(self, tmp_path, monkeypatch):
        import shutil as _shutil

        root = tmp_path / "handoff"
        root.mkdir()

        def _raise(_p):
            raise OSError("boom")

        monkeypatch.setattr(_shutil, "disk_usage", _raise)
        r = check_handoff_free_space(str(root), tmp_path)
        assert r.status == "warn"
        assert "boom" in r.detail


# ── check_handoff_engine_access ─────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _registered_engines():
    """Ensure spark + duckdb are registered (import side effect), and restore
    the registry afterwards so a test-added fake engine never leaks."""
    import aqueduct.executor.duckdb_.engine  # noqa: F401
    import aqueduct.executor.spark.engine  # noqa: F401

    snapshot = dict(CAPABILITY_REGISTRY)
    yield
    CAPABILITY_REGISTRY.clear()
    CAPABILITY_REGISTRY.update(snapshot)


class TestCheckHandoffEngineAccess:
    def test_duckdb_round_trip_ok_and_cleans_up(self, tmp_path):
        root = tmp_path / "handoff"
        results = check_handoff_engine_access(str(root), tmp_path, preflight=False)
        by_name = {r.name: r for r in results}
        assert by_name["handoff-access:duckdb"].status == "ok"
        # No leftover probe files under the root.
        assert list(root.glob("*")) == []

    def test_duckdb_remote_root_attempts_round_trip_not_blanket_skip(self, tmp_path, monkeypatch):
        """Remote roots are no longer unconditionally skipped with a claim
        that DuckDB "has no httpfs wiring" — httpfs autoloads on first
        s3:// touch (measured 2026-07-31: `autoinstall_known_extensions`/
        `autoload_known_extensions` default True), so a remote root now
        genuinely ATTEMPTS the round trip through the SAME code path as a
        local one. Network-free: stubs `duckdb.connect` so this asserts
        the ROUTING change (no more blanket skip), not a live S3 round
        trip — see `tests/test_executor_duckdb/test_engine_config.py` for
        the real-DuckDB proofs (secret creation, extension error wrapping)
        this task's report documents as network-dependent."""
        import duckdb as _duckdb

        class _FakeConn:
            def sql(self, _q):
                raise RuntimeError("simulated: no route to remote storage")

            def execute(self, *a, **k):
                return self

            def close(self):
                pass

        monkeypatch.setattr(_duckdb, "connect", lambda *_a, **_k: _FakeConn())
        results = check_handoff_engine_access("s3://bucket/handoff", tmp_path, preflight=False)
        by_name = {r.name: r for r in results}
        result = by_name["handoff-access:duckdb"]
        assert result.status == "fail"
        assert "no httpfs wiring" not in result.detail.lower()
        assert "simulated" in result.detail

    def test_spark_default_skips_pending_preflight(self, tmp_path):
        root = tmp_path / "handoff"
        results = check_handoff_engine_access(str(root), tmp_path, preflight=False)
        by_name = {r.name: r for r in results}
        assert by_name["handoff-access:spark"].status == "skip"
        assert "--preflight" in by_name["handoff-access:spark"].detail

    def test_unrecognised_engine_skips_cleanly_not_silently_dropped(self, tmp_path):
        register(EngineCapabilities(engine="flink", table={}))
        root = tmp_path / "handoff"
        results = check_handoff_engine_access(str(root), tmp_path, preflight=False)
        by_name = {r.name: r for r in results}
        assert "handoff-access:flink" in by_name
        assert by_name["handoff-access:flink"].status == "skip"
        assert "flink" in by_name["handoff-access:flink"].detail

    def test_no_registered_engines_skips_cleanly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "aqueduct.executor.capabilities.load_engines",
            lambda: None,
        )
        CAPABILITY_REGISTRY.clear()
        results = check_handoff_engine_access(str(tmp_path / "handoff"), tmp_path, preflight=False)
        assert len(results) == 1
        assert results[0].status == "skip"

    def test_duckdb_missing_dependency_skips_gracefully(self, tmp_path, monkeypatch):
        import sys as _sys
        from unittest.mock import patch

        with patch.dict(_sys.modules, {"duckdb": None}):
            results = check_handoff_engine_access(
                str(tmp_path / "handoff"), tmp_path, preflight=False
            )
        by_name = {r.name: r for r in results}
        assert by_name["handoff-access:duckdb"].status == "skip"
        assert "not installed" in by_name["handoff-access:duckdb"].detail
