"""Phase 85 C1/D7 — `cascade_position_outcomes()` and `heal_costs()`.

Backend-agnostic read-time aggregates, same pattern as
``tests/test_stores/test_queries.py``: seeds per-blueprint DuckDB files
directly with the real DDL (``aqueduct.surveyor.ddl``) under a routing root
and checks the read-time merge.
"""

from __future__ import annotations

import datetime as dt
import uuid
from types import SimpleNamespace

import duckdb
import pytest

from aqueduct.stores import queries as q
from aqueduct.surveyor import ddl as _ddl

pytestmark = pytest.mark.unit


def _cfg(path=None, backend="duckdb"):
    return SimpleNamespace(
        stores=SimpleNamespace(observability=SimpleNamespace(path=path, backend=backend))
    )


def _init(path):
    c = duckdb.connect(str(path))
    c.execute(_ddl._DDL)
    c.execute(_ddl._HEAL_ATTEMPTS_DDL)
    for m in _ddl._HEAL_ATTEMPTS_MIGRATIONS:
        c.execute(m)
    return c


def _iso(days_ago=0):
    return (dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=days_ago)).isoformat()


# ── cascade_position_outcomes ───────────────────────────────────────────────


def _healing_outcome(c, run_id, position, resolution, success):
    c.execute(
        "INSERT INTO healing_outcomes (id, run_id, parent_run_id, failed_module, "
        "failure_category, model, patch_id, confidence, patch_applied, "
        "run_success_after_patch, applied_at, prompt_version, failure_signature, "
        "failure_signature_coarse, resolution, model_cascade_position, engine) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            str(uuid.uuid4()),
            run_id,
            None,
            "mod",
            "cat",
            "modelA",
            "p1",
            0.9,
            True,
            success,
            _iso(),
            "v1",
            "sig",
            "sig",
            resolution,
            position,
            "duckdb",
        ],
    )


def test_cascade_position_outcomes_reports_tier_vs_outcome(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".aqueduct" / "observability" / "bp1"
    root.mkdir(parents=True)
    c = _init(root / "observability.db")
    _healing_outcome(c, "r1", 1, "llm", True)
    _healing_outcome(c, "r2", 1, "llm", True)
    _healing_outcome(c, "r3", 2, "llm", False)
    c.close()

    rows = q.cascade_position_outcomes(_cfg())
    by_tier = {
        (r["model_cascade_position"], r["run_success_after_patch"]): r["count"] for r in rows
    }
    assert by_tier[(1, "success")] == 2
    assert by_tier[(2, "failed")] == 1


def test_cascade_position_outcomes_ignores_null_position(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".aqueduct" / "observability" / "bp1"
    root.mkdir(parents=True)
    c = _init(root / "observability.db")
    _healing_outcome(c, "r1", None, "cached", True)
    c.close()

    rows = q.cascade_position_outcomes(_cfg())
    assert rows == []


# ── heal_costs ───────────────────────────────────────────────────────────────


def _run_and_attempt(c, run_id, blueprint_id, tokens_in, tokens_out, days_ago=0):
    ts = _iso(days_ago)
    c.execute(
        "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at, "
        "module_results) VALUES (?,?,?,?,?,?)",
        [run_id, blueprint_id, "success", ts, ts, "[]"],
    )
    c.execute(
        "INSERT INTO heal_attempts (id, run_id, attempt_num, error_class, where_field, "
        "normalized_message, tokens_in, tokens_out, latency_ms, gate_that_rejected, "
        "stop_reason, prompt_version, recorded_at, chain_link, engine) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            str(uuid.uuid4()),
            run_id,
            1,
            "ValueError",
            "mod",
            "msg",
            tokens_in,
            tokens_out,
            50,
            None,
            "solved",
            "v1",
            ts,
            None,
            "duckdb",
        ],
    )


def test_heal_costs_aggregates_per_blueprint_per_month_across_runs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".aqueduct" / "observability"
    (root / "alpha").mkdir(parents=True)
    (root / "beta").mkdir(parents=True)

    ca = _init(root / "alpha" / "observability.db")
    _run_and_attempt(ca, "a1", "alpha", 100, 50)
    _run_and_attempt(ca, "a2", "alpha", 200, 75)
    ca.close()

    cb = _init(root / "beta" / "observability.db")
    _run_and_attempt(cb, "b1", "beta", 10, 5)
    cb.close()

    rows = {(r["blueprint_id"], r["month"]): r for r in q.heal_costs(_cfg())}
    month = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m")

    alpha = rows[("alpha", month)]
    assert alpha["tokens_in"] == 300
    assert alpha["tokens_out"] == 125
    assert alpha["tokens_total"] == 425
    assert alpha["attempts"] == 2

    beta = rows[("beta", month)]
    assert beta["tokens_in"] == 10
    assert beta["tokens_out"] == 5
    assert beta["attempts"] == 1


def test_heal_costs_empty_store_returns_empty_list(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".aqueduct" / "observability" / "empty"
    root.mkdir(parents=True)
    _init(root / "observability.db").close()

    assert q.heal_costs(_cfg()) == []
