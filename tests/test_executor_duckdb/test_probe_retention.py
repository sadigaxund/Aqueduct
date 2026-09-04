"""Phase 85 A1 — sample_rows redaction + per-probe retention cap (DuckDB engine)."""

from __future__ import annotations

import json

import duckdb
import pytest

from aqueduct import redaction
from aqueduct.executor.duckdb_.probe import ProbeSampling, execute_probe
from aqueduct.models import Module

pytestmark = pytest.mark.duckdb


def _probe_module(signals, attach_to="src", id_="probe1", **kw):
    return Module(
        id=id_, type="Probe", label=id_, config={"signals": signals}, attach_to=attach_to, **kw
    )


def _signal_rows(store_dir, probe_id, signal_type):
    obs = duckdb.connect(str(store_dir / "observability.db"))
    try:
        return obs.execute(
            "SELECT run_id, payload FROM probe_signals WHERE probe_id=? AND signal_type=? "
            "ORDER BY captured_at",
            [probe_id, signal_type],
        ).fetchall()
    finally:
        obs.close()


@pytest.fixture(autouse=True)
def _clean_redaction_registry():
    redaction.clear()
    yield
    redaction.clear()


def test_sample_rows_payload_is_redacted(duckdb_con, tmp_path):
    secret_value = "S3cr3tTok3n1234567890"  # long+high-entropy enough to register
    assert redaction.register(secret_value, key_hint="TEST_SECRET")

    rel = duckdb_con.sql(f"SELECT 1 AS id, '{secret_value}' AS token")
    mod = _probe_module([{"type": "sample_rows", "n": 5}])
    execute_probe(mod, rel, duckdb_con, "r1", tmp_path)

    rows = _signal_rows(tmp_path, "probe1", "sample_rows")
    assert len(rows) == 1
    payload = json.loads(rows[0][1])
    serialized = json.dumps(payload)
    assert secret_value not in serialized
    assert redaction.REDACTED_PLACEHOLDER in serialized


def test_sample_rows_retention_cap_keeps_only_last_n(duckdb_con, tmp_path):
    rel = duckdb_con.sql("SELECT 1 AS id")
    mod = _probe_module([{"type": "sample_rows", "n": 1}])
    sampling = ProbeSampling(sample_rows_keep_last_n=3)

    for i in range(5):
        execute_probe(mod, rel, duckdb_con, f"r{i}", tmp_path, sampling=sampling)

    rows = _signal_rows(tmp_path, "probe1", "sample_rows")
    assert len(rows) == 3, "retention cap must keep only the last N sample_rows rows"
    kept_run_ids = {r[0] for r in rows}
    assert kept_run_ids == {"r2", "r3", "r4"}, "must keep the MOST RECENT N, not the first N"


def test_sample_rows_retention_cap_defaults_to_20(duckdb_con, tmp_path):
    """No `observability.retention:` config knob exists any more — the cap
    is `ProbeSampling`'s own fixed default (20), applied even when the
    caller passes no `sampling=` argument at all."""
    rel = duckdb_con.sql("SELECT 1 AS id")
    mod = _probe_module([{"type": "sample_rows", "n": 1}])
    assert ProbeSampling().sample_rows_keep_last_n == 20

    for i in range(25):
        execute_probe(mod, rel, duckdb_con, f"r{i}", tmp_path)

    rows = _signal_rows(tmp_path, "probe1", "sample_rows")
    assert len(rows) == 20, "default cap must keep exactly the last 20 sample_rows rows"
    kept_run_ids = {r[0] for r in rows}
    assert kept_run_ids == {f"r{i}" for i in range(5, 25)}


def test_non_sample_rows_signals_are_not_redacted_or_capped(duckdb_con, tmp_path):
    """schema_snapshot/null_rates/etc. are aggregate/statistical — they carry
    no comparable sensitivity, so they must NOT be routed through redact()
    or the retention cap (only sample_rows gets both)."""
    rel = duckdb_con.sql("SELECT 1 AS id")
    mod = _probe_module([{"type": "schema_snapshot"}])
    sampling = ProbeSampling(sample_rows_keep_last_n=1)

    for i in range(3):
        execute_probe(mod, rel, duckdb_con, f"r{i}", tmp_path, sampling=sampling)

    rows = _signal_rows(tmp_path, "probe1", "schema_snapshot")
    assert len(rows) == 3, "schema_snapshot must not be pruned by the sample_rows cap"
