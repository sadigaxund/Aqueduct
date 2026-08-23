"""Phase 85 W9 — `aqueduct report-prune` (B1 CLI half) and `aqueduct
report-costs` (D7).

Pyspark-free: seeds the observability store directly with the real DDL
(``aqueduct.surveyor.ddl``) so this runs in the unit lane (no Spark, no
executor import) — same approach as ``test_report_profile.py``.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid

import duckdb
import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.surveyor import ddl as _ddl

pytestmark = pytest.mark.unit


def _conn(store_dir):
    c = duckdb.connect(str(store_dir / "observability.db"))
    c.execute(_ddl._DDL)
    c.execute(_ddl._HEAL_ATTEMPTS_DDL)
    for m in _ddl._HEAL_ATTEMPTS_MIGRATIONS:
        c.execute(m)
    return c


def _iso(days_ago: int) -> str:
    return (dt.datetime.now(tz=dt.UTC) - dt.timedelta(days=days_ago)).isoformat()


def _run_record(c, run_id, blueprint_id, days_ago):
    ts = _iso(days_ago)
    c.execute(
        "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at, "
        "module_results) VALUES (?,?,?,?,?,?)",
        [run_id, blueprint_id, "success", ts, ts, "[]"],
    )


def _heal_attempt(c, run_id, days_ago, tokens_in=0, tokens_out=0, tool_calls=None):
    ts = _iso(days_ago)
    c.execute(
        "INSERT INTO heal_attempts (id, run_id, attempt_num, error_class, where_field, "
        "normalized_message, tokens_in, tokens_out, latency_ms, gate_that_rejected, "
        "stop_reason, prompt_version, recorded_at, tool_calls_json, chain_link, engine) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
            json.dumps(tool_calls) if tool_calls else None,
            None,
            "duckdb",
        ],
    )


# ── report-prune ─────────────────────────────────────────────────────────────


def test_report_prune_deletes_old_rows_and_reports_counts(tmp_path):
    c = _conn(tmp_path)
    _run_record(c, "run-old", "bp1", days_ago=400)  # older than default 90d window
    _run_record(c, "run-new", "bp1", days_ago=1)
    c.close()

    res = CliRunner().invoke(cli, ["report-prune", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "run_records" in res.output
    assert "1" in res.output

    c = duckdb.connect(str(tmp_path / "observability.db"))
    remaining = [r[0] for r in c.execute("SELECT run_id FROM run_records").fetchall()]
    c.close()
    assert remaining == ["run-new"]


def test_report_prune_json_format_reports_per_table_counts(tmp_path):
    c = _conn(tmp_path)
    _run_record(c, "run-old", "bp1", days_ago=400)
    _run_record(c, "run-new", "bp1", days_ago=1)
    c.close()

    res = CliRunner().invoke(
        cli, ["report-prune", "--store-dir", str(tmp_path), "--format", "json"]
    )
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert data["vacuum"] is False
    assert data["totals"]["run_records"] == 1


def test_report_prune_without_vacuum_flag_never_calls_vacuum(tmp_path, monkeypatch):
    c = _conn(tmp_path)
    _run_record(c, "run-old", "bp1", days_ago=400)
    c.close()

    calls: list = []
    monkeypatch.setattr(
        "aqueduct.surveyor.retention.vacuum_store", lambda store: calls.append(store)
    )

    res = CliRunner().invoke(cli, ["report-prune", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert calls == [], "vacuum_store must NEVER be called without --vacuum"


def test_report_prune_with_vacuum_flag_calls_vacuum(tmp_path, monkeypatch):
    c = _conn(tmp_path)
    _run_record(c, "run-old", "bp1", days_ago=400)
    c.close()

    calls: list = []
    monkeypatch.setattr(
        "aqueduct.surveyor.retention.vacuum_store", lambda store: calls.append(store)
    )

    res = CliRunner().invoke(cli, ["report-prune", "--store-dir", str(tmp_path), "--vacuum"])
    assert res.exit_code == 0, res.output
    assert len(calls) == 1, "vacuum_store must be called exactly once with --vacuum"


# ── report-costs ─────────────────────────────────────────────────────────────


def test_report_costs_aggregates_tokens_per_blueprint_per_month(tmp_path):
    c = _conn(tmp_path)
    _run_record(c, "run-a1", "bp1", days_ago=1)
    _run_record(c, "run-a2", "bp1", days_ago=2)
    _run_record(c, "run-b1", "bp2", days_ago=1)
    # bp1 gets two attempts this month (summed together)
    _heal_attempt(c, "run-a1", days_ago=1, tokens_in=100, tokens_out=50)
    _heal_attempt(c, "run-a2", days_ago=2, tokens_in=200, tokens_out=75)
    # bp2 gets one attempt this month, in a separate blueprint bucket
    _heal_attempt(c, "run-b1", days_ago=1, tokens_in=10, tokens_out=5)
    c.close()

    res = CliRunner().invoke(
        cli, ["report-costs", "--store-dir", str(tmp_path), "--format", "json"]
    )
    assert res.exit_code == 0, res.output
    rows = {(r["blueprint_id"], r["month"]): r for r in json.loads(res.output)}

    month = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m")
    bp1 = rows[("bp1", month)]
    assert bp1["tokens_in"] == 300
    assert bp1["tokens_out"] == 125
    assert bp1["tokens_total"] == 425
    assert bp1["attempts"] == 2

    bp2 = rows[("bp2", month)]
    assert bp2["tokens_in"] == 10
    assert bp2["tokens_out"] == 5
    assert bp2["attempts"] == 1


def test_report_costs_table_format_has_no_ansi_when_piped(tmp_path):
    c = _conn(tmp_path)
    _run_record(c, "run-a1", "bp1", days_ago=1)
    _heal_attempt(c, "run-a1", days_ago=1, tokens_in=10, tokens_out=5)
    c.close()

    res = CliRunner().invoke(cli, ["report-costs", "--store-dir", str(tmp_path)])
    assert res.exit_code == 0, res.output
    assert "\x1b[" not in res.output
    assert "bp1" in res.output
