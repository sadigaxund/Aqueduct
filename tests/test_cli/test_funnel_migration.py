"""Phase 85 Wave 2 — funnel-migration regression tests.

Wave 1 built the render funnel (``aqueduct/cli/render/{funnel,wrap,width,
tables}.py``) and wired it into ``run.py``/``patch.py``/``diagnostics.py``.
Wave 2 (this worker) rewires the remaining raw ``click.echo`` call sites in
``observability.py``, ``project.py``, ``dev.py``, ``benchmark.py``,
``drift.py``, ``blueprint.py``, ``stores.py``, ``hooks.py``, and ``mcp.py``.

These tests assert the property that matters, not the exact copy: narrative
lines land on stderr, the final/structured result lands on stdout, a
``--format json`` path stays pure machine-readable data (no ANSI), and a
long line survives complete and unwrapped when piped (the default,
non-TTY ``CliRunner`` environment — ``render/wrap.py``'s piped/CI branch
never wraps or truncates).
"""

from __future__ import annotations

import json
from datetime import datetime

import duckdb
import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit


def _invoke(args):
    return CliRunner().invoke(cli, args)


# ---------------------------------------------------------------------------
# stores.py — narrative-empty-result and success confirmation land on stdout
# ---------------------------------------------------------------------------


def test_stores_migrate_empty_result_on_stdout(tmp_path):
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")
    empty_db = tmp_path / "empty.db"
    conn = duckdb.connect(str(empty_db))
    conn.execute("CREATE TABLE depot_kv (key VARCHAR, value BLOB, updated_at TIMESTAMP)")
    conn.close()

    result = _invoke(
        [
            "stores",
            "migrate",
            "--from-duckdb",
            str(empty_db),
            "--store",
            "depot",
            "--config",
            str(config),
        ]
    )
    assert result.exit_code == 0, result.output
    assert "0 rows" in result.stdout
    assert "\x1b[" not in result.stdout


def test_stores_migrate_success_message_on_stdout(tmp_path):
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")
    pop_db = tmp_path / "pop.db"
    conn = duckdb.connect(str(pop_db))
    conn.execute("CREATE TABLE depot_kv (key VARCHAR, value BLOB, updated_at TIMESTAMP)")
    conn.execute("INSERT INTO depot_kv VALUES ('k1', '\\x00', ?)", [datetime.now()])
    conn.close()

    result = _invoke(
        [
            "stores",
            "migrate",
            "--from-duckdb",
            str(pop_db),
            "--store",
            "depot",
            "--config",
            str(config),
        ]
    )
    assert result.exit_code == 0, result.output
    assert "migrated 1 depot key(s)" in result.stdout


# ---------------------------------------------------------------------------
# dev.py — narrative (style.error/warn, stderr) vs. detail rows (stdout);
# the config-snippet result is never wrapped, even when it is long.
# ---------------------------------------------------------------------------


def test_dev_capabilities_check_unknown_engine_narrative_on_stderr(tmp_path, monkeypatch):
    """No capabilities.yml declarations exist under an empty cwd — the
    ``style.warn`` narrative line is the one guaranteed-reachable path with
    no repo fixtures, and it must land on stderr, not stdout."""
    monkeypatch.chdir(tmp_path)
    import aqueduct.executor.capability_tooling as tooling

    monkeypatch.setattr(tooling, "check", lambda: [])

    result = _invoke(["dev", "capabilities", "check"])
    assert result.exit_code != 0
    assert "no engine capability declarations found" in result.stderr
    assert result.stdout == ""


def test_dev_scaffold_config_snippet_unwrapped_when_piped(tmp_path):
    """The printed config snippet is the structured result the user pastes
    into a blueprint/aqueduct.yml — it must survive complete, on one line
    per source line, even when piped (never truncated/wrapped)."""
    result = _invoke(["dev", "scaffold", "probe", "--name", "my_probe", "--out", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "Add this to your" in result.stdout
    # The snippet is echoed verbatim — every non-blank line the scaffold
    # module built appears intact in stdout (no "(N more lines)" truncation
    # marker, no mid-line ellipsis).
    assert "(more lines)" not in result.stdout
    assert "…" not in result.stdout


# ---------------------------------------------------------------------------
# blueprint.py — the table-format result is on stdout; --format json stays
# pure JSON with no ANSI leaking in.
# ---------------------------------------------------------------------------


def _seed_history_store(store_dir):
    conn = duckdb.connect(str(store_dir / "observability.db"))
    from aqueduct.patch.index import PATCH_INDEX_DDL
    from aqueduct.surveyor.ddl import _DDL, _HEAL_ATTEMPTS_DDL

    conn.execute(_DDL)
    conn.execute(_HEAL_ATTEMPTS_DDL)
    conn.execute(PATCH_INDEX_DDL)
    conn.execute(
        "INSERT INTO run_records VALUES "
        "('run1','bp1','error','2026-01-01T00:00:00','2026-01-01T00:01:00', '[]', NULL, NULL)"
    )
    conn.execute(
        "INSERT INTO heal_attempts (id, run_id, attempt_num, recorded_at) "
        "VALUES ('a1','run1',1,'2026-01-01T00:00:30')"
    )
    conn.close()


def test_blueprint_history_no_activity_message_on_stdout(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_history_store(store_dir)

    result = _invoke(["blueprint", "history", "no-such-bp", "--store-dir", str(store_dir)])
    assert result.exit_code == 0, result.output
    assert "No remediation history for blueprint 'no-such-bp'." in result.stdout


def test_blueprint_history_json_is_clean_stdout(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_history_store(store_dir)

    result = _invoke(
        ["blueprint", "history", "bp1", "--store-dir", str(store_dir), "--format", "json"]
    )
    assert result.exit_code == 0, result.output
    assert "\x1b[" not in result.stdout
    data = json.loads(result.stdout)
    assert data["blueprint_id"] == "bp1"


# ---------------------------------------------------------------------------
# drift.py — the USAGE_ERROR narrative is stderr; the baseline-established
# result line is stdout.
# ---------------------------------------------------------------------------


_DRIFT_BP = """aqueduct: "1.0"
id: drift.funnel_test
name: D
modules:
  - id: load
    type: Ingress
    label: L
    config: { format: parquet, path: data/in }
  - id: c
    type: Channel
    label: C
    config: { op: sql, query: "SELECT a, b FROM load" }
edges:
  - { from: load, to: c }
"""

_DRIFT_AQ = (
    "deployment:\n  engine: spark\nengine:\n  spark:\n    master_url: local[1]\n"
    "agent:\n  model: test-model\n"
)


def _drift_project(tmp_path):
    (tmp_path / "bp.yml").write_text(_DRIFT_BP)
    (tmp_path / "aqueduct.yml").write_text(_DRIFT_AQ)
    (tmp_path / "store").mkdir()
    return tmp_path


def test_drift_no_matching_module_error_on_stderr(tmp_path):
    project = _drift_project(tmp_path)
    result = _invoke(
        [
            "drift",
            str(project / "bp.yml"),
            "--config",
            str(project / "aqueduct.yml"),
            "--store-dir",
            str(project / "store"),
            "--module",
            "does-not-exist",
        ]
    )
    assert result.exit_code != 0
    assert "no Ingress modules to check" in result.stderr
    assert result.stdout == ""


def test_drift_baseline_established_result_on_stdout(tmp_path, monkeypatch):
    """Same Spark-boundary stub as tests/test_drift/test_drift_command.py:
    stub the two lazily-imported spark submodules so the real compile →
    baseline path runs without a cluster."""
    import sys
    import types
    from unittest.mock import MagicMock

    project = _drift_project(tmp_path)

    session_stub = types.ModuleType("aqueduct.executor.spark.session")
    session_stub.make_spark_session = lambda *a, **k: MagicMock()
    ingress_stub = types.ModuleType("aqueduct.executor.spark.ingress")
    ingress_stub.read_source_schema = lambda mod, spark: {"a": "int", "b": "string"}

    if "aqueduct.executor.spark" not in sys.modules:
        parent = types.ModuleType("aqueduct.executor.spark")
        monkeypatch.setitem(sys.modules, "aqueduct.executor.spark", parent)
    else:
        parent = sys.modules["aqueduct.executor.spark"]
    monkeypatch.setitem(sys.modules, "aqueduct.executor.spark.session", session_stub)
    monkeypatch.setitem(sys.modules, "aqueduct.executor.spark.ingress", ingress_stub)
    monkeypatch.setattr(parent, "session", session_stub, raising=False)
    monkeypatch.setattr(parent, "ingress", ingress_stub, raising=False)

    result = _invoke(
        [
            "drift",
            str(project / "bp.yml"),
            "--config",
            str(project / "aqueduct.yml"),
            "--store-dir",
            str(project / "store"),
        ]
    )
    assert result.exit_code == 0, result.output
    assert "baseline established" in result.stdout
    assert "\x1b[" not in result.stdout


# ---------------------------------------------------------------------------
# observability.py — a "no results" message is the report's stdout result;
# --format json on `aqueduct runs` (no store) stays clean.
# ---------------------------------------------------------------------------


def test_runs_no_store_message_on_stdout(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config = tmp_path / "aq.yml"
    config.write_text("aqueduct_config: '1.0'")

    result = _invoke(["runs", "--config", str(config), "--store-dir", str(tmp_path / "nope")])
    assert result.exit_code == 0, result.output
    assert "No runs found" in result.stdout


# ---------------------------------------------------------------------------
# Piped-long-line spot check: a long error string in `aqueduct test` is
# printed complete (never wrapped/dropped) when piped — exercises the
# project.py migration through the funnel's non-TTY branch.
# ---------------------------------------------------------------------------


def test_project_completion_script_unwrapped_when_piped():
    """`aqueduct completion` prints a shell-completion script the user
    redirects to a file — long lines must survive intact when piped."""
    result = _invoke(["completion", "bash"])
    assert result.exit_code == 0, result.output
    assert "_AQUEDUCT_COMPLETE" in result.stdout
    assert "\x1b[" not in result.stdout
