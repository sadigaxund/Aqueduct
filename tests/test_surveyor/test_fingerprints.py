"""Phase 56 — channel_fingerprints changelog round-trip against a real store."""
from __future__ import annotations

import pytest

from aqueduct.compiler.fingerprint import write_fingerprints
from aqueduct.parser.models import Module, ModuleType
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.surveyor.surveyor import _DDL

pytestmark = pytest.mark.unit


def _store(tmp_path):
    obs = DuckDBObservabilityStore(str(tmp_path / "o.db"))
    with obs.connect() as cur:
        cur.execute(_DDL)
    return obs


def _ch(query: str) -> Module:
    return Module(id="clean", type=ModuleType.Channel, label="c", config={"op": "sql", "query": query})


def _rows(obs):
    with obs.connect() as cur:
        return cur.execute(
            "SELECT fingerprint, first_run_id, last_run_id FROM channel_fingerprints "
            "WHERE blueprint_id='bp.x' AND channel_id='clean' ORDER BY first_seen"
        ).fetchall()


def test_unchanged_sql_keeps_one_row_and_bumps_last_run(tmp_path):
    obs = _store(tmp_path)
    mods = (_ch("SELECT a FROM t WHERE x = 1"),)
    write_fingerprints("bp.x", "run1", mods, observability_store=obs)
    # reformatted but semantically identical → same fingerprint → still 1 row
    write_fingerprints("bp.x", "run2", (_ch("select a from t where x=1  -- c"),), observability_store=obs)

    rows = _rows(obs)
    assert len(rows) == 1
    assert rows[0][1] == "run1"   # first_run_id preserved
    assert rows[0][2] == "run2"   # last_run_id bumped


def test_semantic_change_appends_a_row(tmp_path):
    obs = _store(tmp_path)
    write_fingerprints("bp.x", "run1", (_ch("SELECT a FROM t WHERE x = 1"),), observability_store=obs)
    write_fingerprints("bp.x", "run2", (_ch("SELECT a FROM t WHERE x = 2"),), observability_store=obs)

    rows = _rows(obs)
    assert len(rows) == 2
    assert {r[0] for r in rows}.__len__() == 2  # two distinct fingerprints
