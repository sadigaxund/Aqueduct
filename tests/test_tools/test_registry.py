"""Phase 73 — internal ToolRegistry (`aqueduct/tools/`).

Read-only enforcement is structural, not convention: no handler's source
references a write path (`open_obs_write` or a store's mutating methods).
Every result must pass through redaction — verified by registering a fake
secret and confirming it never survives a `call_tool()` round trip.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import duckdb
import pytest

from aqueduct import redaction
from aqueduct.tools import registry as tool_registry
from aqueduct.tools.registry import REGISTRY, Tool, call_tool, get_tools

pytestmark = pytest.mark.unit

_WRITE_MARKERS = (
    "open_obs_write(",
    ".put_json(",
    ".write_pending(",
    ".write_applied(",
    ".write_rejected(",
    "upsert(",
    "set_status(",
)


@pytest.fixture(autouse=True)
def _clean_redaction_registry():
    yield
    redaction.clear()


def test_registry_has_expected_built_ins():
    names = {t.name for t in get_tools()}
    assert names == {
        "list_runs",
        "run_detail",
        "lineage",
        "patch_list",
        "patch_show",
        "probe_signals",
        "doctor",
        "blueprint_history",
    }


def test_every_tool_is_read_only_and_frozen():
    for tool in get_tools():
        assert tool.read_only is True
        assert isinstance(tool, Tool)
        with pytest.raises(Exception):  # frozen dataclass — FrozenInstanceError
            tool.name = "mutated"  # type: ignore[misc]


def test_no_handler_source_touches_a_write_path():
    """Structural read-only check: grep every handler's source for a known
    write-side call. A handler that ever needs a write path must not exist
    in this registry (v1 has no write tool)."""
    src = inspect.getsource(tool_registry)
    for marker in _WRITE_MARKERS:
        assert marker not in src, f"tools/registry.py references a write path: {marker}"


def test_entrypoint_group_constant_is_reserved_not_resolved():
    assert tool_registry.AQ_TOOLS_ENTRYPOINT_GROUP == "aqueduct.tools"
    # Not resolved anywhere in the module — no entry_points() call.
    assert "entry_points(" not in inspect.getsource(tool_registry)


def test_register_rejects_non_read_only_tool():
    with pytest.raises(ValueError):
        tool_registry.register(
            Tool(
                name="bogus_write_tool",
                description="",
                params_schema={},
                handler=lambda: None,
                read_only=False,
            )
        )
    assert "bogus_write_tool" not in REGISTRY


def test_call_tool_unknown_name_raises():
    with pytest.raises(KeyError):
        call_tool("does_not_exist")


def _cfg(store_dir):
    return SimpleNamespace(
        stores=SimpleNamespace(observability=SimpleNamespace(path=None, backend="duckdb"))
    )


def test_call_tool_redacts_secret_in_run_detail(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    conn = duckdb.connect(str(store_dir / "observability.db"))
    from aqueduct.surveyor.ddl import _DDL

    conn.execute(_DDL)
    conn.execute(
        "INSERT INTO run_records VALUES ('run1','bp1','error',"
        "'2026-01-01T00:00:00','2026-01-01T00:01:00',"
        '\'[{"module_id":"m1","status":"error",'
        '"error":"connect failed with token supersecrettoken123456"}]\', NULL)'
    )
    conn.close()

    redaction.register("supersecrettoken123456", key_hint="test")

    result = call_tool(
        "run_detail",
        run_id="run1",
        store_dir=str(store_dir),
        config_path=None,
    )
    assert "supersecrettoken123456" not in str(result)
    assert redaction.REDACTED_PLACEHOLDER in str(result)


def test_list_runs_tool_returns_json_serializable_rows(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    conn = duckdb.connect(str(store_dir / "observability.db"))
    from aqueduct.surveyor.ddl import _DDL

    conn.execute(_DDL)
    conn.execute(
        "INSERT INTO run_records VALUES ('run1','bp1','success',"
        "'2026-01-01T00:00:00','2026-01-01T00:01:00','[]', NULL)"
    )
    conn.close()

    rows = call_tool("list_runs", store_dir=str(store_dir), config_path=None)
    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == "run1"
    assert row["blueprint_id"] == "bp1"
    assert row["status"] == "success"
    assert row["started_at"].startswith("2026-01-01")
    assert row["finished_at"].startswith("2026-01-01")


def test_doctor_tool_returns_structured_checks():
    result = call_tool("doctor", skip_spark=True)
    assert "passed" in result
    assert isinstance(result["checks"], list)
    assert all({"name", "status", "detail"} <= set(c) for c in result["checks"])
