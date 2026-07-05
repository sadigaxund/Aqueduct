"""Phase 69 — canonical backend-aware observability read resolver."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest

from aqueduct.stores.read import (
    open_obs_read,
    open_obs_write,
    resolve_duckdb_obs_path,
    resolve_obs_store_dir,
)

pytestmark = pytest.mark.unit


def _cfg(path=None, backend="duckdb"):
    return SimpleNamespace(
        stores=SimpleNamespace(observability=SimpleNamespace(path=path, backend=backend))
    )


def _make_db(p: Path, with_run: str | None = None):
    p.parent.mkdir(parents=True, exist_ok=True)
    c = duckdb.connect(str(p))
    c.execute("CREATE TABLE run_records (run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results VARCHAR)")
    if with_run:
        c.execute("INSERT INTO run_records VALUES (?, 'bp', 'success', now(), now(), '[]')", [with_run])
    c.close()


# ── resolve_duckdb_obs_path ──────────────────────────────────────────────────

def test_resolve_store_dir(tmp_path):
    _make_db(tmp_path / "observability.db")
    assert resolve_duckdb_obs_path(_cfg(), store_dir=str(tmp_path)) == tmp_path / "observability.db"


def test_resolve_store_dir_missing(tmp_path):
    assert resolve_duckdb_obs_path(_cfg(), store_dir=str(tmp_path)) is None


def test_resolve_nondefault_dir(tmp_path):
    _make_db(tmp_path / "observability.db")
    assert resolve_duckdb_obs_path(_cfg(path=str(tmp_path))) == tmp_path / "observability.db"


def test_resolve_flat_default(tmp_path, monkeypatch):
    """Legacy flat .aqueduct/observability.db — no longer default path; None means per-blueprint routing."""
    monkeypatch.chdir(tmp_path)
    _make_db(tmp_path / ".aqueduct" / "observability.db")
    # path=None → per-blueprint routing, not flat file
    assert resolve_duckdb_obs_path(_cfg()) is None


def test_resolve_per_pipeline_by_run_id(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _make_db(tmp_path / ".aqueduct/observability/alpha/observability.db", with_run="r9")
    got = resolve_duckdb_obs_path(_cfg(), run_id="r9")
    assert got == Path(".aqueduct/observability/alpha/observability.db")


def test_resolve_per_pipeline_by_blueprint_id(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _make_db(tmp_path / ".aqueduct/observability/beta/observability.db")
    got = resolve_duckdb_obs_path(_cfg(), blueprint_id="beta")
    assert got == Path(".aqueduct/observability/beta/observability.db")


def test_resolve_location_only_dir_routes_per_blueprint(tmp_path):
    """A suffix-less custom path is a base DIR → route <dir>/<blueprint_id>/."""
    base = tmp_path / "custom_obs"
    _make_db(base / "beta" / "observability.db")
    got = resolve_duckdb_obs_path(_cfg(path=str(base)), blueprint_id="beta")
    assert got == base / "beta" / "observability.db"


def test_resolve_location_only_dir_by_run_id(tmp_path):
    base = tmp_path / "custom_obs"
    _make_db(base / "alpha" / "observability.db", with_run="rZ")
    got = resolve_duckdb_obs_path(_cfg(path=str(base)), run_id="rZ")
    assert got == base / "alpha" / "observability.db"


# (2.0: the explicit-single-file mode was removed — a `.db` duckdb path is
# rejected at config load; see tests/test_parser/test_config.py.)


# ── open_obs_read ────────────────────────────────────────────────────────────

def test_open_duckdb_returns_store(tmp_path):
    base = tmp_path / "obs"
    _make_db(base / "beta" / "observability.db")
    store = open_obs_read(_cfg(path=str(base)), blueprint_id="beta")
    assert store is not None and store.backend == "duckdb"
    with store.connect() as cur:
        cur.execute("SELECT count(*) FROM run_records")
        assert cur.fetchone()[0] == 0


def test_open_duckdb_missing_is_none(tmp_path):
    assert open_obs_read(_cfg(path=str(tmp_path / "nope.db"))) is None


def test_open_postgres_uses_get_stores():
    sentinel = object()
    bundle = SimpleNamespace(observability=sentinel)
    with patch("aqueduct.stores.base.get_stores", return_value=bundle) as gs:
        got = open_obs_read(_cfg(backend="postgres"))
    assert got is sentinel
    gs.assert_called_once()


# ── resolve_obs_store_dir / open_obs_write (ISSUE-036: per-blueprint write) ────

def test_write_dir_default_routes_per_blueprint():
    assert resolve_obs_store_dir(_cfg(), "beta") == Path(".aqueduct/observability/beta")


def test_write_dir_location_only_dir(tmp_path):
    base = tmp_path / "obs"
    assert resolve_obs_store_dir(_cfg(path=str(base)), "beta") == base / "beta"


def test_write_dir_cli_store_dir_wins(tmp_path):
    assert resolve_obs_store_dir(_cfg(path="anything"), "beta", store_dir=str(tmp_path)) == tmp_path


def test_open_obs_write_creates_per_blueprint_file(tmp_path):
    """A location-only DIRECTORY config must NOT be opened as a file (the drift bug)."""
    base = tmp_path / "obs"
    base.mkdir()
    store = open_obs_write(_cfg(path=str(base)), "beta")
    # writes land in <base>/beta/observability.db, not by opening <base> as a file
    with store.connect() as cur:
        cur.execute("CREATE TABLE t (x INT); INSERT INTO t VALUES (1)")
        cur.execute("SELECT COUNT(*) FROM t")
        assert cur.fetchone()[0] == 1
    assert (base / "beta" / "observability.db").exists()
