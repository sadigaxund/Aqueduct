"""Phase 56 — `aqueduct report --trend <column>` cross-run quality trend."""
from __future__ import annotations

import json

import duckdb
import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

# aqueduct.executor.spark.* pulls in pyspark at package-import time
# (aqueduct/executor/spark/__init__.py) regardless of which submodule you
# want — a hard ModuleNotFoundError here is a fatal collection error that
# aborts the WHOLE pytest run (not a graceful per-file skip), so this must
# be an explicit importorskip before the import, not just a marker.
pytest.importorskip("pyspark", reason="pyspark not installed — install aqueduct-core[spark]")
from aqueduct.executor.spark.probe import _DDL as _PROBE_DDL  # noqa: E402
from aqueduct.surveyor.surveyor import _DDL as _OBS_DDL  # noqa: E402

pytestmark = [pytest.mark.unit, pytest.mark.spark]


def _seed(store_dir, seed_ts):
    db = store_dir / "observability.db"
    c = duckdb.connect(str(db))
    c.execute(_OBS_DDL)
    c.execute(_PROBE_DDL)

    def ins(run, sig, payload, ts):
        c.execute(
            "INSERT INTO probe_signals (run_id, probe_id, signal_type, payload, captured_at) "
            "VALUES (?,?,?,?,?)",
            [run, "p", sig, json.dumps(payload), ts],
        )

    ts1, ts2 = seed_ts(days=-2), seed_ts(days=-1)
    ins("r1", "null_rates", {"null_rates": {"amount": 0.1}}, ts1)
    ins("r1", "schema_snapshot", {"fields": [{"name": "amount", "type": "double"}]}, ts1)
    ins("r2", "null_rates", {"null_rates": {"amount": 0.4}}, ts2)
    ins("r2", "schema_snapshot", {"fields": [{"name": "amount", "type": "string"}]}, ts2)
    c.close()


def test_trend_table_shows_history_and_type_drift(tmp_path, seed_ts):
    _seed(tmp_path, seed_ts)
    res = CliRunner().invoke(
        cli, ["report", "--trend", "amount", "--store-dir", str(tmp_path),
              "--since", seed_ts(days=-365)],
    )
    assert res.exit_code == 0, res.output
    assert "null-rate:" in res.output
    assert "0.1000" in res.output and "0.4000" in res.output
    assert "type drift" in res.output  # double → string flagged


def test_trend_json_shape(tmp_path, seed_ts):
    _seed(tmp_path, seed_ts)
    res = CliRunner().invoke(
        cli, ["report", "--trend", "amount", "--store-dir", str(tmp_path),
              "--since", seed_ts(days=-365), "--format", "json"],
    )
    assert res.exit_code == 0, res.output
    payload = json.loads(res.output)
    assert payload["column"] == "amount"
    assert [r["null_rate"] for r in payload["null_rate"]] == [0.1, 0.4]
    assert [r["type"] for r in payload["type"]] == ["double", "string"]


def test_trend_empty_when_column_absent(tmp_path, seed_ts):
    _seed(tmp_path, seed_ts)
    res = CliRunner().invoke(
        cli, ["report", "--trend", "ghost", "--store-dir", str(tmp_path),
              "--since", seed_ts(days=-365)],
    )
    assert res.exit_code == 0, res.output
    assert "No probe signals for column 'ghost'" in res.output
