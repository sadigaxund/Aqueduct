"""MCP stdio server over the ToolRegistry (`aqueduct/mcp/`).

Two layers:

- **Pure declaration tests** — the registry → MCP tool-declaration round
  trip must not require the `mcp` SDK (`build_tool_declarations` is
  SDK-free by contract; a structural test enforces the module top level
  stays SDK-free).
- **In-memory session tests** — gated on `pytest.importorskip("mcp")`, they
  drive a real client/server pair through the SDK's
  `create_connected_server_and_client_session` harness: enumeration,
  schema pass-through, redaction of results AND of handler exception
  messages, structured errors that never kill the server loop, and the
  `--config` injection path.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import duckdb
import pytest

from aqueduct import redaction
from aqueduct.mcp.server import build_tool_declarations
from aqueduct.tools.registry import REGISTRY

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _clean_redaction_registry():
    yield
    redaction.clear()


# ── Pure declaration tests (no SDK required) ────────────────────────────────


def test_declarations_cover_every_registry_tool():
    decls = {d["name"]: d for d in build_tool_declarations()}
    assert set(decls) == set(REGISTRY)


def test_declarations_pass_schema_through_verbatim():
    """name/description/params_schema survive the conversion untouched —
    no translation layer (the registry's params_schema is the MCP
    inputSchema, by contract)."""
    for d in build_tool_declarations():
        tool = REGISTRY[d["name"]]
        assert d["description"] == tool.description
        assert d["inputSchema"] is tool.params_schema  # verbatim, not a copy


def test_declarations_are_json_schema_shaped():
    import json

    for d in build_tool_declarations():
        schema = d["inputSchema"]
        assert schema.get("type") == "object"
        assert isinstance(schema.get("properties", {}), dict)
        json.dumps(d)  # must be JSON-serializable as-is


def test_server_module_top_level_is_sdk_free():
    """`import aqueduct.mcp` must work on a base install — the SDK import
    lives inside serve()/_build_server() only (the studio/textual pattern)."""
    import aqueduct.mcp.server as srv

    for line in inspect.getsource(srv).splitlines():
        stripped = line.strip()
        if stripped.startswith(("import mcp", "from mcp")):
            # only allowed inside a function body (indented)
            assert line != stripped, f"top-level SDK import: {stripped!r}"


def test_server_module_has_no_write_api_references():
    """Read-only stays structural: the server imports only the registry."""
    import aqueduct.mcp.server as srv

    src = inspect.getsource(srv)
    for marker in (
        "open_obs_write(",
        ".write_pending(",
        ".write_applied(",
        ".write_rejected(",
        "upsert(",
        "set_status(",
    ):
        assert marker not in src


# ── In-memory session tests (SDK required — skip when absent) ───────────────


def _seed_store(store_dir: Path, error_text: str = "boom") -> None:
    conn = duckdb.connect(str(store_dir / "observability.db"))
    from aqueduct.surveyor.ddl import _DDL

    conn.execute(_DDL)
    conn.execute(
        "INSERT INTO run_records VALUES ('run1','bp1','error',"
        "'2026-01-01T00:00:00','2026-01-01T00:01:00',"
        f'\'[{{"module_id":"m1","status":"error","error":"{error_text}"}}]\', NULL)'
    )
    conn.close()


def _run_session(server, script):
    """Drive an async client-session script from a sync test (no
    pytest-asyncio in the suite — anyio comes with the mcp SDK)."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    async def _main():
        async with create_connected_server_and_client_session(server) as session:
            return await script(session)

    return anyio.run(_main)


def test_session_lists_every_tool_with_schema():
    pytest.importorskip("mcp")
    from aqueduct.mcp.server import _build_server

    async def script(session):
        return await session.list_tools()

    result = _run_session(_build_server(), script)
    by_name = {t.name: t for t in result.tools}
    assert set(by_name) == set(REGISTRY)
    for name, t in by_name.items():
        assert t.inputSchema == REGISTRY[name].params_schema


def test_session_call_redacts_result(tmp_path):
    pytest.importorskip("mcp")
    from aqueduct.mcp.server import _build_server

    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_store(store_dir, error_text="token supersecrettoken123456 leaked")
    redaction.register("supersecrettoken123456", key_hint="t")

    async def script(session):
        return await session.call_tool(
            "run_detail", {"run_id": "run1", "store_dir": str(store_dir)}
        )

    res = _run_session(_build_server(), script)
    assert res.isError is False
    text = res.content[0].text
    assert "supersecrettoken123456" not in text
    assert redaction.REDACTED_PLACEHOLDER in text


def test_session_wraps_non_dict_results(tmp_path):
    pytest.importorskip("mcp")
    from aqueduct.mcp.server import _build_server

    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_store(store_dir)

    async def script(session):
        return await session.call_tool("list_runs", {"store_dir": str(store_dir)})

    res = _run_session(_build_server(), script)
    assert res.isError is False
    # list results are wrapped so MCP doesn't misread them as content blocks
    assert list(res.structuredContent) == ["result"]
    assert res.structuredContent["result"][0]["run_id"] == "run1"


def test_session_validation_error_is_structured_not_fatal(tmp_path):
    pytest.importorskip("mcp")
    from aqueduct.mcp.server import _build_server

    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_store(store_dir)

    async def script(session):
        missing = await session.call_tool("run_detail", {})  # run_id required
        bad_type = await session.call_tool("list_runs", {"limit": "not_an_int"})
        alive = await session.call_tool("list_runs", {"store_dir": str(store_dir)})
        return missing, bad_type, alive

    missing, bad_type, alive = _run_session(_build_server(), script)
    assert missing.isError is True
    assert "run_id" in missing.content[0].text
    assert bad_type.isError is True
    assert alive.isError is False  # errors never kill the server loop


def test_session_handler_exception_is_redacted_structured_error(tmp_path):
    pytest.importorskip("mcp")
    from aqueduct.mcp.server import _build_server

    # A malformed aqueduct.yml raises ConfigError inside the handler; the
    # server must surface it as a structured isError result (redacted),
    # then keep serving.
    bad_cfg = tmp_path / "aqueduct.yml"
    bad_cfg.write_text("aqueduct_config: '1.0'\nstores: [not, a, dict]\n")

    async def script(session):
        err = await session.call_tool("list_runs", {})
        alive = await session.call_tool("doctor", {"skip_spark": True})
        return err, alive

    # config_path injection: server-level --config reaches the handler
    err, alive = _run_session(_build_server(config_path=str(bad_cfg)), script)
    assert err.isError is True
    assert "ConfigError" in err.content[0].text
    assert alive.isError is False
    assert alive.structuredContent["passed"] in (True, False)


def test_session_explicit_config_path_beats_server_flag(tmp_path):
    pytest.importorskip("mcp")
    from aqueduct.mcp.server import _build_server

    bad_cfg = tmp_path / "bad.yml"
    bad_cfg.write_text("aqueduct_config: '1.0'\nstores: [not, a, dict]\n")
    good_cfg = tmp_path / "good.yml"
    good_cfg.write_text("aqueduct_config: '1.0'\n")

    async def script(session):
        return await session.call_tool("list_runs", {"config_path": str(good_cfg)})

    res = _run_session(_build_server(config_path=str(bad_cfg)), script)
    assert res.isError is False  # client's explicit config_path won
