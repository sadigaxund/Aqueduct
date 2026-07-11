"""CLI tests for `aqueduct blueprint history` (Phase 73).

Read-only timeline merging patch_index/healing_outcomes (via
stores/queries.py) with the blueprint file's git commit history.
"""

from __future__ import annotations

import duckdb
import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.integration


def _seed_store(store_dir):
    conn = duckdb.connect(str(store_dir / "observability.db"))
    from aqueduct.patch.index import PATCH_INDEX_DDL
    from aqueduct.surveyor.ddl import _DDL, _HEAL_ATTEMPTS_DDL

    conn.execute(_DDL)
    conn.execute(_HEAL_ATTEMPTS_DDL)
    conn.execute(PATCH_INDEX_DDL)
    conn.execute(
        "INSERT INTO run_records VALUES "
        "('run1','bp1','error','2026-01-01T00:00:00','2026-01-01T00:01:00', '[]', NULL)"
    )
    conn.execute(
        "INSERT INTO heal_attempts (id, run_id, attempt_num, recorded_at) "
        "VALUES ('a1','run1',1,'2026-01-01T00:00:30')"
    )
    conn.execute(
        "INSERT INTO patch_index (patch_id, blueprint_id, run_id, status, object_key, "
        "rationale, ops, source, prompt_version, created_at, updated_at) VALUES "
        "('p1','bp1','run1','applied','k1','fix null check','[]','llm','v1',"
        "'2026-01-01T00:02:00','2026-01-01T00:02:00')"
    )
    conn.close()


def test_blueprint_history_text_output(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_store(store_dir)

    runner = CliRunner()
    result = runner.invoke(cli, ["blueprint", "history", "bp1", "--store-dir", str(store_dir)])
    assert result.exit_code == 0, result.output
    assert "heal_run_started" in result.output
    assert "patch_apply" in result.output
    assert "fix null check" in result.output


def test_blueprint_history_json_output(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_store(store_dir)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["blueprint", "history", "bp1", "--store-dir", str(store_dir), "--format", "json"],
    )
    assert result.exit_code == 0, result.output
    import json

    data = json.loads(result.output)
    assert data["blueprint_id"] == "bp1"
    types = [e["event_type"] for e in data["events"]]
    assert "heal_run_started" in types
    assert "patch_apply" in types


def test_blueprint_history_no_activity_prints_message(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_store(store_dir)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["blueprint", "history", "no-such-bp", "--store-dir", str(store_dir)]
    )
    assert result.exit_code == 0
    assert "No remediation history" in result.output
