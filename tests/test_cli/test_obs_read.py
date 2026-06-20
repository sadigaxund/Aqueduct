"""Phase 69 — canonical backend-aware observability read resolver."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest

from aqueduct.stores.read import open_obs_read, resolve_duckdb_obs_path

pytestmark = pytest.mark.unit


def _cfg(path=".aqueduct/observability.db", backend="duckdb"):
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


def test_resolve_nondefault_file(tmp_path):
    f = tmp_path / "custom.db"
    _make_db(f)
    assert resolve_duckdb_obs_path(_cfg(path=str(f))) == f


def test_resolve_nondefault_dir(tmp_path):
    _make_db(tmp_path / "observability.db")
    assert resolve_duckdb_obs_path(_cfg(path=str(tmp_path))) == tmp_path / "observability.db"


def test_resolve_flat_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _make_db(tmp_path / ".aqueduct" / "observability.db")
    assert resolve_duckdb_obs_path(_cfg()) == Path(".aqueduct/observability.db")


def test_resolve_per_pipeline_by_run_id(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _make_db(tmp_path / ".aqueduct/observability/alpha/observability.db", with_run="r9")
    got = resolve_duckdb_obs_path(_cfg(), run_id="r9")
    assert got == Path(".aqueduct/observability/alpha/observability.db")


# ── open_obs_read ────────────────────────────────────────────────────────────

def test_open_duckdb_returns_store(tmp_path):
    f = tmp_path / "custom.db"
    _make_db(f)
    store = open_obs_read(_cfg(path=str(f)))
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
