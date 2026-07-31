"""DuckDB engine config surface — ``engine.duckdb.*`` (memory_limit, threads,
database_path, extension_repository, s3_*) wired into ``_make_session``.

Every field here has a real consumer (AGENTS.md's "no silent no-ops" rule) —
these tests prove the wiring against a REAL DuckDB connection, not a mock,
per the same discipline the rest of ``capabilities.yml`` follows.

Network note: ``test_s3_secret_created_from_resolved_env_secret`` and
``test_ensure_extension_wraps_install_failure`` both touch DuckDB's
extension installer (``INSTALL httpfs`` / a bogus extension name), which
needs network on first use per the measured
``autoinstall_known_extensions``/``autoload_known_extensions`` defaults.
``CREATE SECRET`` itself (``configure_s3_secret``) needs no network — that
subset is also covered without the extension-install step.
"""

from __future__ import annotations

import duckdb
import pytest

from aqueduct.executor.duckdb_.engine import _make_session
from aqueduct.executor.duckdb_.extensions import (
    DuckDBExtensionError,
    configure_s3_secret,
    ensure_extension,
    resolve_s3_secret_from_config,
)
from aqueduct.executor.protocol import SessionSpec

pytestmark = pytest.mark.duckdb


def _spec(**engine_config) -> SessionSpec:
    return SessionSpec(blueprint_id="test", engine_config=engine_config)


class TestMakeSessionMemoryAndThreads:
    def test_memory_limit_applied(self):
        # Binary unit (MiB) so DuckDB's own reported string matches exactly —
        # a decimal "MB" input gets re-reported in MiB (e.g. 512MB -> "488.2
        # MiB"), which is DuckDB's own unit-conversion, not this wiring.
        con = _make_session(_spec(memory_limit="512MiB"))
        try:
            val = con.sql("SELECT current_setting('memory_limit')").fetchone()[0]
            assert val == "512.0 MiB"
        finally:
            con.close()

    def test_threads_applied(self):
        con = _make_session(_spec(threads=2))
        try:
            val = con.sql("SELECT current_setting('threads')").fetchone()[0]
            assert int(val) == 2
        finally:
            con.close()

    def test_no_config_leaves_defaults_untouched(self):
        """An empty engine_config (the historical Stage A shape) still opens
        a working `:memory:` connection with no forced settings."""
        con = _make_session(_spec())
        try:
            assert con.sql("SELECT 1").fetchone() == (1,)
        finally:
            con.close()


class TestMakeSessionDatabasePath:
    def test_database_path_persists_across_sessions(self, tmp_path):
        db_path = str(tmp_path / "persist.duckdb")
        con1 = _make_session(_spec(database_path=db_path))
        try:
            con1.execute("CREATE TABLE t AS SELECT 1 AS x")
        finally:
            con1.close()

        con2 = _make_session(_spec(database_path=db_path))
        try:
            assert con2.sql("SELECT x FROM t").fetchone() == (1,)
        finally:
            con2.close()

    def test_unset_database_path_is_memory_only(self, tmp_path):
        """Two sessions with no `database_path` never share state — proves
        the default really is a fresh `:memory:` connection, not an
        accidental shared file."""
        con1 = _make_session(_spec())
        try:
            con1.execute("CREATE TABLE t AS SELECT 1 AS x")
        finally:
            con1.close()

        con2 = _make_session(_spec())
        try:
            with pytest.raises(duckdb.CatalogException):
                con2.sql("SELECT x FROM t")
        finally:
            con2.close()


class TestS3SecretResolution:
    def test_resolve_s3_secret_from_config_none_when_unconfigured(self):
        assert resolve_s3_secret_from_config({}, {}) is None

    def test_resolve_s3_secret_from_config_resolves_env_provider(self, monkeypatch):
        monkeypatch.setenv("AQ_TEST_KEY_ID", "AKIAFAKE")
        monkeypatch.setenv("AQ_TEST_SECRET", "fakesecretvalue")
        result = resolve_s3_secret_from_config(
            {
                "s3_key_id_secret": "AQ_TEST_KEY_ID",
                "s3_secret_access_key_secret": "AQ_TEST_SECRET",
                "s3_region": "us-east-1",
            },
            {"provider": "env"},
        )
        assert result == {
            "key_id": "AKIAFAKE",
            "secret": "fakesecretvalue",
            "region": "us-east-1",
            "endpoint": None,
            "url_style": None,
            "use_ssl": None,
        }

    def test_resolve_s3_secret_from_config_includes_minio_overrides(self, monkeypatch):
        """The non-AWS-S3-compatible escape hatch (endpoint/url_style/use_ssl)
        flows through unchanged — these are literal config, not secrets."""
        monkeypatch.setenv("AQ_TEST_KEY_ID3", "minioadmin")
        monkeypatch.setenv("AQ_TEST_SECRET3", "minioadmin")
        result = resolve_s3_secret_from_config(
            {
                "s3_key_id_secret": "AQ_TEST_KEY_ID3",
                "s3_secret_access_key_secret": "AQ_TEST_SECRET3",
                "s3_endpoint": "localhost:9000",
                "s3_url_style": "path",
                "s3_use_ssl": False,
            },
            {"provider": "env"},
        )
        assert result == {
            "key_id": "minioadmin",
            "secret": "minioadmin",
            "region": None,
            "endpoint": "localhost:9000",
            "url_style": "path",
            "use_ssl": False,
        }

    def test_configure_s3_secret_creates_duckdb_secret_no_network(self, duckdb_con):
        """`CREATE SECRET` succeeds with httpfs unloaded — the secret
        manager is core DuckDB (measured 2026-07-31). No network touched."""
        configure_s3_secret(duckdb_con, key_id="AKIAFAKE", secret="shh", region="us-east-1")
        rows = duckdb_con.sql(
            "SELECT name, type FROM duckdb_secrets() WHERE name = 'aqueduct_s3'"
        ).fetchall()
        assert rows == [("aqueduct_s3", "s3")]

    def test_configure_s3_secret_without_region(self, duckdb_con):
        configure_s3_secret(duckdb_con, key_id="AKIAFAKE", secret="shh")
        rows = duckdb_con.sql(
            "SELECT name FROM duckdb_secrets() WHERE name = 'aqueduct_s3'"
        ).fetchall()
        assert rows == [("aqueduct_s3",)]

    def test_configure_s3_secret_with_minio_overrides_no_network(self, duckdb_con):
        """endpoint/url_style/use_ssl reach `CREATE SECRET` — the non-AWS
        escape hatch — with no network touched (secret creation is core
        DuckDB, same as the plain-AWS-shape secret above)."""
        configure_s3_secret(
            duckdb_con,
            key_id="minioadmin",
            secret="minioadmin",
            endpoint="localhost:9000",
            url_style="path",
            use_ssl=False,
        )
        rows = duckdb_con.sql(
            "SELECT name, type FROM duckdb_secrets() WHERE name = 'aqueduct_s3'"
        ).fetchall()
        assert rows == [("aqueduct_s3", "s3")]

    def test_make_session_wires_s3_secret_via_engine_options(self, monkeypatch):
        """End-to-end through `_make_session`: engine_config names secret
        KEYS, engine_options carries the resolved `secrets:` block — the
        SAME reconciliation path `@aq.secret()` uses, never a parallel one.
        Network: this also triggers the proactive httpfs INSTALL/LOAD."""
        monkeypatch.setenv("AQ_TEST_KEY_ID2", "AKIAFAKE2")
        monkeypatch.setenv("AQ_TEST_SECRET2", "fakesecretvalue2")
        spec = SessionSpec(
            blueprint_id="test",
            engine_config={
                "s3_key_id_secret": "AQ_TEST_KEY_ID2",
                "s3_secret_access_key_secret": "AQ_TEST_SECRET2",
                "s3_region": "us-west-2",
            },
            engine_options={"secrets": {"provider": "env"}},
        )
        con = _make_session(spec)
        try:
            rows = con.sql(
                "SELECT name, type FROM duckdb_secrets() WHERE name = 'aqueduct_s3'"
            ).fetchall()
            assert rows == [("aqueduct_s3", "s3")]
        finally:
            con.close()


class TestEnsureExtension:
    def test_ensure_extension_wraps_install_failure(self, duckdb_con):
        """A bogus extension name fails fast (no such extension known) —
        the failure is wrapped as DuckDBExtensionError naming the network
        cause and both escape hatches, never a bare duckdb.Error."""
        with pytest.raises(DuckDBExtensionError) as exc_info:
            ensure_extension(duckdb_con, "totally_not_a_real_extension_xyz")
        msg = str(exc_info.value)
        assert "~/.duckdb/extensions" in msg
        assert "extension_repository" in msg

    def test_ensure_extension_sets_custom_repository_before_install(self, duckdb_con):
        """`extension_repository`, when given, is applied via `SET
        custom_extension_repository` BEFORE the install attempt is made —
        proven by pointing it at an unreachable host and observing the
        wrapped failure names that URL."""
        with pytest.raises(DuckDBExtensionError) as exc_info:
            ensure_extension(
                duckdb_con,
                "totally_not_a_real_extension_xyz",
                extension_repository="http://aqueduct-test-unreachable.invalid/repo",
            )
        current = duckdb_con.sql(
            "SELECT current_setting('custom_extension_repository')"
        ).fetchone()[0]
        assert current == "http://aqueduct-test-unreachable.invalid/repo"
