"""Phase 9 tests: sub-DAG selectors, logical execution date, LLM guardrails, patch rollback."""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from aqueduct.cli import _check_guardrails, cli
from aqueduct.compiler.runtime import AqFunctions
from aqueduct.executor.spark.executor import (
    ExecuteError,
    _reachable_backward,
    _reachable_forward,
    _selector_included,
)
from aqueduct.parser.models import AgentConfig, Edge, Module


# ── Helpers ───────────────────────────────────────────────────────────────────

def _edge(from_id: str, to_id: str, port: str = "main") -> Edge:
    return Edge(from_id=from_id, to_id=to_id, port=port)


def _module(id: str, type: str = "Channel") -> Module:
    return Module(id=id, type=type, label=id, config={})


def _edges(*pairs) -> tuple[Edge, ...]:
    return tuple(_edge(f, t) for f, t in pairs)


# ── Sub-DAG selectors: _reachable_forward ─────────────────────────────────────

def test_reachable_forward_linear_from_start():
    edges = _edges(("A", "B"), ("B", "C"))
    assert _reachable_forward("A", edges) == {"A", "B", "C"}


def test_reachable_forward_linear_from_middle():
    edges = _edges(("A", "B"), ("B", "C"))
    assert _reachable_forward("B", edges) == {"B", "C"}


def test_reachable_forward_fanout():
    edges = _edges(("A", "B"), ("A", "C"))
    assert _reachable_forward("A", edges) == {"A", "B", "C"}


def test_reachable_forward_leaf_node():
    edges = _edges(("A", "B"), ("B", "C"))
    assert _reachable_forward("C", edges) == {"C"}


def test_reachable_forward_ignores_signal_edges():
    edges = (_edge("A", "B", port="main"), _edge("A", "R", port="signal"))
    # signal edges are not data edges — R should not be reachable
    result = _reachable_forward("A", edges)
    assert "R" not in result
    assert "A" in result
    assert "B" in result


# ── Sub-DAG selectors: _reachable_backward ────────────────────────────────────

def test_reachable_backward_linear_from_end():
    edges = _edges(("A", "B"), ("B", "C"))
    assert _reachable_backward("C", edges) == {"A", "B", "C"}


def test_reachable_backward_linear_from_middle():
    edges = _edges(("A", "B"), ("B", "C"))
    assert _reachable_backward("B", edges) == {"A", "B"}


def test_reachable_backward_fanin():
    edges = _edges(("A", "C"), ("B", "C"))
    assert _reachable_backward("C", edges) == {"A", "B", "C"}


def test_reachable_backward_root_node():
    edges = _edges(("A", "B"), ("B", "C"))
    assert _reachable_backward("A", edges) == {"A"}


# ── Sub-DAG selectors: _selector_included ────────────────────────────────────

def _modules(*ids) -> tuple[Module, ...]:
    return tuple(_module(id_) for id_ in ids)


def test_selector_included_none_when_both_none():
    modules = _modules("A", "B", "C")
    edges = _edges(("A", "B"), ("B", "C"))
    assert _selector_included(modules, edges, None, None) is None


def test_selector_included_from_only():
    modules = _modules("A", "B", "C")
    edges = _edges(("A", "B"), ("B", "C"))
    result = _selector_included(modules, edges, "B", None)
    assert result == {"B", "C"}
    assert "A" not in result


def test_selector_included_to_only():
    modules = _modules("A", "B", "C")
    edges = _edges(("A", "B"), ("B", "C"))
    result = _selector_included(modules, edges, None, "B")
    assert result == {"A", "B"}
    assert "C" not in result


def test_selector_included_from_and_to_intersection():
    modules = _modules("A", "B", "C", "D")
    edges = _edges(("A", "B"), ("B", "C"), ("C", "D"))
    result = _selector_included(modules, edges, "B", "C")
    assert result == {"B", "C"}
    assert "A" not in result
    assert "D" not in result


def test_selector_included_from_not_found_raises():
    modules = _modules("A", "B")
    edges = _edges(("A", "B"),)
    with pytest.raises(ExecuteError, match="--from module 'X' not found"):
        _selector_included(modules, edges, "X", None)


def test_selector_included_to_not_found_raises():
    modules = _modules("A", "B")
    edges = _edges(("A", "B"),)
    with pytest.raises(ExecuteError, match="--to module 'Z' not found"):
        _selector_included(modules, edges, None, "Z")


def test_selector_included_probe_auto_included_when_attach_to_included():
    modules = (
        _module("A"),
        _module("B"),
        Module(id="P", type="Probe", label="P", config={}, attach_to="B"),
    )
    edges = _edges(("A", "B"),)
    result = _selector_included(modules, edges, "B", None)
    assert "P" in result


def test_selector_included_probe_excluded_when_attach_to_not_included():
    modules = (
        _module("A"),
        _module("B"),
        Module(id="P", type="Probe", label="P", config={}, attach_to="A"),
    )
    edges = _edges(("A", "B"),)
    result = _selector_included(modules, edges, "B", None)
    assert "P" not in result


# ── Executor sub-DAG end-to-end (Spark) ──────────────────────────────────────

@pytest.mark.usefixtures("spark")
def test_executor_skips_module_not_in_included_set(spark, tmp_path):
    from aqueduct.compiler.models import Manifest
    from aqueduct.executor.spark.executor import execute

    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out.parquet")
    spark.range(5).write.parquet(in_path)

    manifest = Manifest(
        blueprint_id="test.subdag",
        modules=(
            Module(id="ing", type="Ingress", label="Ingress", config={"format": "parquet", "path": in_path}),
            Module(id="ch", type="Channel", label="Ch", config={"op": "sql", "query": "SELECT * FROM ing"}),
            Module(id="eg", type="Egress", label="Egress", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="ing", to_id="ch"),
            Edge(from_id="ch", to_id="eg"),
        ),
        context={},
        spark_config={},
    )

    # --to ch: backward from ch = {ing, ch}; eg is excluded → skipped
    result = execute(manifest, spark, to_module="ch")

    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["ing"] == "success"
    assert statuses["ch"] == "success"
    assert statuses["eg"] == "skipped"
    assert result.status == "success"


@pytest.mark.usefixtures("spark")
def test_executor_from_to_skips_downstream(spark, tmp_path):
    from aqueduct.compiler.models import Manifest
    from aqueduct.executor.spark.executor import execute

    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out.parquet")  # should be skipped
    spark.range(3).write.parquet(in_path)

    manifest = Manifest(
        blueprint_id="test.subdag.to",
        modules=(
            Module(id="ing", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="eg", type="Egress", label="Eg", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="ing", to_id="eg"),
        ),
        context={},
        spark_config={},
    )

    result = execute(manifest, spark, to_module="ing")

    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["ing"] == "success"
    assert statuses["eg"] == "skipped"


# ── Logical execution date ────────────────────────────────────────────────────

def test_base_date_returns_execution_date_when_set():
    d = date(2026, 1, 15)
    aq = AqFunctions(execution_date=d)
    assert aq._base_date() == d


def test_base_date_returns_today_when_not_set():
    aq = AqFunctions()
    assert aq._base_date() == date.today()


def test_date_today_with_execution_date():
    aq = AqFunctions(execution_date=date(2026, 1, 15))
    assert aq.date_today() == "2026-01-15"


def test_date_today_with_execution_date_custom_format():
    aq = AqFunctions(execution_date=date(2026, 1, 15))
    assert aq.date_today(format="MM/dd/yyyy") == "01/15/2026"


def test_date_yesterday_with_execution_date():
    aq = AqFunctions(execution_date=date(2026, 1, 15))
    assert aq.date_yesterday() == "2026-01-14"


def test_date_month_start_with_execution_date():
    aq = AqFunctions(execution_date=date(2026, 1, 15))
    assert aq.date_month_start() == "2026-01-01"


def test_runtime_timestamp_with_execution_date_is_midnight_utc():
    aq = AqFunctions(execution_date=date(2026, 1, 15))
    ts = aq.runtime_timestamp()
    parsed = datetime.fromisoformat(ts)
    assert parsed.year == 2026
    assert parsed.month == 1
    assert parsed.day == 15
    assert parsed.hour == 0
    assert parsed.minute == 0
    assert parsed.second == 0
    assert parsed.tzinfo is not None


def test_runtime_timestamp_without_execution_date_is_not_midnight():
    aq = AqFunctions()
    ts = aq.runtime_timestamp()
    parsed = datetime.fromisoformat(ts)
    # Verify it's approximately now (within 5 seconds)
    now = datetime.now(tz=timezone.utc)
    assert abs((now - parsed).total_seconds()) < 5


def test_execution_date_pinned_through_compile():
    from pathlib import Path

    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.parser.parser import parse

    bp_path = Path(__file__).parent / "fixtures" / "valid_minimal.yml"
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, execution_date=date(2026, 1, 15))
    assert manifest is not None


# ── Guardrails: _check_guardrails ────────────────────────────────────────────

def _make_agent(allowed_paths=(), forbidden_ops=()):
    from aqueduct.parser.models import GuardrailsConfig
    return AgentConfig(guardrails=GuardrailsConfig(allowed_paths=allowed_paths, forbidden_ops=forbidden_ops))


def _make_patch(*ops):
    """Build a minimal mock PatchSpec with operations as list of dicts."""
    patch = MagicMock()
    patch.operations = list(ops)
    return patch


def test_guardrails_no_restrictions_always_pass():
    agent = _make_agent()
    patch = _make_patch({"op": "remove_module", "module_id": "x"})
    assert _check_guardrails(patch, agent) is None


def test_guardrails_forbidden_op_blocks():
    agent = _make_agent(forbidden_ops=("remove_module",))
    patch = _make_patch({"op": "remove_module", "module_id": "x"})
    err = _check_guardrails(patch, agent)
    assert err is not None
    assert "remove_module" in err


def test_guardrails_non_forbidden_op_passes():
    agent = _make_agent(forbidden_ops=("remove_module",))
    patch = _make_patch({"op": "replace_module_config", "module_id": "x", "config": {}})
    assert _check_guardrails(patch, agent) is None


def test_guardrails_allowed_paths_pass_matching():
    agent = _make_agent(allowed_paths=("s3a://prod/*",))
    patch = _make_patch({"op": "replace_module_config", "module_id": "x", "config": {"path": "s3a://prod/orders/"}})
    assert _check_guardrails(patch, agent) is None


def test_guardrails_allowed_paths_block_non_matching():
    agent = _make_agent(allowed_paths=("s3a://prod/*",))
    patch = _make_patch({"op": "replace_module_config", "module_id": "x", "config": {"path": "s3a://staging/orders/"}})
    err = _check_guardrails(patch, agent)
    assert err is not None
    assert "s3a://staging/orders/" in err


def test_guardrails_allowed_paths_empty_means_unrestricted():
    agent = _make_agent(allowed_paths=())
    patch = _make_patch({"op": "replace_module_config", "module_id": "x", "config": {"path": "s3a://anywhere/"}})
    assert _check_guardrails(patch, agent) is None


def test_guardrails_op_without_config_path_skips_path_check():
    agent = _make_agent(allowed_paths=("s3a://prod/*",))
    patch = _make_patch({"op": "replace_module_label", "module_id": "x", "label": "New Label"})
    assert _check_guardrails(patch, agent) is None


def test_guardrails_empty_operations_list():
    agent = _make_agent(allowed_paths=("s3a://prod/*",), forbidden_ops=("remove_module",))
    patch = _make_patch()
    assert _check_guardrails(patch, agent) is None


# ── AgentConfig schema round-trip ─────────────────────────────────────────────

def test_agent_config_allowed_paths_default_empty():
    a = AgentConfig()
    assert a.guardrails.allowed_paths == ()


def test_agent_config_forbidden_ops_default_empty():
    a = AgentConfig()
    assert a.guardrails.forbidden_ops == ()


def test_agent_config_fields_set():
    from aqueduct.parser.models import GuardrailsConfig
    a = AgentConfig(guardrails=GuardrailsConfig(allowed_paths=("s3a://prod/*",), forbidden_ops=("remove_module",)))
    assert "s3a://prod/*" in a.guardrails.allowed_paths
    assert "remove_module" in a.guardrails.forbidden_ops


def test_agent_config_schema_parses_allowed_paths(tmp_path):
    from aqueduct.parser.parser import parse

    bp_text = """
aqueduct: "1.0"
id: test.guardrails
name: Test
modules:
  - id: m
    type: Ingress
    label: M
    config:
      format: parquet
      path: /tmp/x
edges: []
agent:
  approval_mode: auto
  allowed_paths:
    - "s3a://prod/*"
    - "s3a://staging/*"
  forbidden_ops:
    - remove_module
"""
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(bp_text)
    bp = parse(str(bp_path))
    assert "s3a://prod/*" in bp.agent.guardrails.allowed_paths
    assert "s3a://staging/*" in bp.agent.guardrails.allowed_paths
    assert "remove_module" in bp.agent.guardrails.forbidden_ops


# ── Patch Rollback CLI ────────────────────────────────────────────────────────

_MINIMAL_BP = """aqueduct: "1.0"
id: test.rollback
name: Test
modules:
  - id: m
    type: Ingress
    label: M
    config:
      format: parquet
      path: /tmp/x
edges: []
"""

_ORIGINAL_BP = """aqueduct: "1.0"
id: test.rollback
name: Original
modules:
  - id: m
    type: Ingress
    label: M
    config:
      format: parquet
      path: /tmp/original
edges: []
"""


def _setup_patch_env(tmp_path: Path, patch_id: str = "abc123") -> dict:
    """Create minimal patch lifecycle dirs + files for rollback tests."""
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text(_MINIMAL_BP)

    patches_root = tmp_path / "patches"
    backup_dir = patches_root / "backups"
    applied_dir = patches_root / "applied"
    backup_dir.mkdir(parents=True)
    applied_dir.mkdir(parents=True)

    backup_file = backup_dir / f"{patch_id}_20260101T000000_blueprint.yml"
    backup_file.write_text(_ORIGINAL_BP)

    applied_record = applied_dir / f"{patch_id}.json"
    applied_record.write_text(json.dumps({
        "_aq_meta": {"run_id": "run-001", "blueprint_id": "test.rollback"},
        "operations": [],
    }, indent=2))

    return {
        "bp_path": bp_path,
        "patches_root": patches_root,
        "backup_file": backup_file,
        "applied_record": applied_record,
    }


def test_patch_rollback_restores_blueprint(tmp_path):
    env = _setup_patch_env(tmp_path)
    runner = CliRunner()

    result = runner.invoke(cli, [
        "patch", "rollback", "abc123",
        "--blueprint", str(env["bp_path"]),
        "--patches-dir", str(env["patches_root"]),
    ])

    assert result.exit_code == 0, result.output
    restored = env["bp_path"].read_text()
    assert "Original" in restored
    assert "/tmp/original" in restored


def test_patch_rollback_is_atomic(tmp_path):
    env = _setup_patch_env(tmp_path)
    runner = CliRunner()

    original_content = env["bp_path"].read_text()
    runner.invoke(cli, [
        "patch", "rollback", "abc123",
        "--blueprint", str(env["bp_path"]),
        "--patches-dir", str(env["patches_root"]),
    ])

    # No .rollback.tmp.yml file left behind
    tmp_files = list(tmp_path.glob("*.rollback.tmp.yml"))
    assert len(tmp_files) == 0


def test_patch_rollback_moves_applied_to_rolled_back(tmp_path):
    env = _setup_patch_env(tmp_path)
    runner = CliRunner()

    runner.invoke(cli, [
        "patch", "rollback", "abc123",
        "--blueprint", str(env["bp_path"]),
        "--patches-dir", str(env["patches_root"]),
    ])

    assert not env["applied_record"].exists()
    rolled_back_path = env["patches_root"] / "rolled_back" / "abc123.json"
    assert rolled_back_path.exists()


def test_patch_rollback_record_contains_rolled_back_at(tmp_path):
    env = _setup_patch_env(tmp_path)
    runner = CliRunner()

    runner.invoke(cli, [
        "patch", "rollback", "abc123",
        "--blueprint", str(env["bp_path"]),
        "--patches-dir", str(env["patches_root"]),
    ])

    rolled_back_path = env["patches_root"] / "rolled_back" / "abc123.json"
    data = json.loads(rolled_back_path.read_text())
    assert "rolled_back_at" in data["_aq_meta"]
    ts = data["_aq_meta"]["rolled_back_at"]
    datetime.fromisoformat(ts)  # must be valid ISO timestamp


def test_patch_rollback_no_backup_exits_nonzero(tmp_path):
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text(_MINIMAL_BP)
    patches_root = tmp_path / "patches"
    patches_root.mkdir()

    runner = CliRunner()
    result = runner.invoke(cli, [
        "patch", "rollback", "nonexistent_patch",
        "--blueprint", str(bp_path),
        "--patches-dir", str(patches_root),
    ])

    assert result.exit_code != 0


def test_patch_rollback_no_applied_record_still_restores_blueprint(tmp_path):
    env = _setup_patch_env(tmp_path)
    # Remove the applied record — patch was staged-only
    env["applied_record"].unlink()

    runner = CliRunner()
    result = runner.invoke(cli, [
        "patch", "rollback", "abc123",
        "--blueprint", str(env["bp_path"]),
        "--patches-dir", str(env["patches_root"]),
    ])

    assert result.exit_code == 0
    restored = env["bp_path"].read_text()
    assert "Original" in restored
