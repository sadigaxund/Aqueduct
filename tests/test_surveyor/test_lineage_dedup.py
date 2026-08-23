"""Phase 85 B2 — column_lineage dedup against channel_fingerprints + scoped read."""

from __future__ import annotations

import pytest

from aqueduct.compiler.fingerprint import write_fingerprints
from aqueduct.compiler.lineage import write_lineage
from aqueduct.parser.models import Module, ModuleType
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.stores.queries import lineage
from aqueduct.surveyor.surveyor import _DDL

pytestmark = pytest.mark.unit


def _store(tmp_path):
    obs = DuckDBObservabilityStore(str(tmp_path / "o.db"))
    with obs.connect() as cur:
        cur.execute(_DDL)
    return obs


def _ch(query: str) -> Module:
    return Module(
        id="clean", type=ModuleType.Channel, label="c", config={"op": "sql", "query": query}
    )


def _lineage_rows(obs, run_id: str | None = None):
    with obs.connect() as cur:
        clause = " AND run_id = ?" if run_id else ""
        params = [run_id] if run_id else []
        return cur.execute(
            f"SELECT run_id FROM column_lineage WHERE blueprint_id='bp.x'{clause}",
            params,
        ).fetchall()


def test_unchanged_fingerprint_writes_nothing(tmp_path):
    obs = _store(tmp_path)
    mods = (_ch("SELECT a FROM t WHERE x = 1"),)

    # First run: no channel_fingerprints row exists yet -> lineage IS written.
    write_lineage("bp.x", "run1", mods, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run1", mods, observability_store=obs)
    assert len(_lineage_rows(obs)) == 1

    # Second run, semantically-identical SQL (just reformatted) -> the
    # fingerprint is unchanged, so write_lineage must write NOTHING new.
    mods2 = (_ch("select a from t where x=1  -- comment"),)
    write_lineage("bp.x", "run2", mods2, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run2", mods2, observability_store=obs)

    rows = _lineage_rows(obs)
    assert len(rows) == 1, "unchanged fingerprint must not produce a second lineage row"
    assert rows[0][0] == "run1"


def test_changed_fingerprint_writes_a_new_row(tmp_path):
    obs = _store(tmp_path)
    mods1 = (_ch("SELECT a FROM t WHERE x = 1"),)
    write_lineage("bp.x", "run1", mods1, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run1", mods1, observability_store=obs)

    mods2 = (_ch("SELECT a, b FROM t WHERE x = 2"),)  # real predicate/column change
    write_lineage("bp.x", "run2", mods2, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run2", mods2, observability_store=obs)

    rows = _lineage_rows(obs)
    run_ids = {r[0] for r in rows}
    assert "run1" in run_ids
    assert "run2" in run_ids


def test_lineage_query_scopes_to_latest_run_only(tmp_path):
    obs = _store(tmp_path)
    mods1 = (_ch("SELECT a FROM t WHERE x = 1"),)
    write_lineage("bp.x", "run1", mods1, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run1", mods1, observability_store=obs)

    mods2 = (_ch("SELECT a, b FROM t WHERE x = 2"),)
    write_lineage("bp.x", "run2", mods2, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run2", mods2, observability_store=obs)

    # No run_id given -> lineage() must scope to the LATEST run only (run2),
    # not mix rows from run1 and run2 up to the LIMIT cap.
    rows = lineage(obs, blueprint_id="bp.x")
    assert rows, "expected the latest run's lineage rows"
    # run2's SQL selects a, b — both output columns should show up, and none
    # of run1's stale row should leak in.
    out_cols = {r.output_column for r in rows}
    assert out_cols == {"a", "b"}


def test_lineage_query_explicit_run_id_unaffected(tmp_path):
    obs = _store(tmp_path)
    mods1 = (_ch("SELECT a FROM t WHERE x = 1"),)
    write_lineage("bp.x", "run1", mods1, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run1", mods1, observability_store=obs)

    mods2 = (_ch("SELECT a, b FROM t WHERE x = 2"),)
    write_lineage("bp.x", "run2", mods2, edges=(), observability_store=obs)
    write_fingerprints("bp.x", "run2", mods2, observability_store=obs)

    rows = lineage(obs, blueprint_id="bp.x", run_id="run1")
    assert {r.output_column for r in rows} == {"a"}
