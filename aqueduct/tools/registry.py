"""Internal, read-only diagnostics tool registry (Phase 73).

The single enumeration surface a future MCP server and an agentic ToolBox
will both read from — today it backs `call_tool()` / `get_tools()` (used by
tests) and the `blueprint_history` tool, which dogfoods the registry from
`aqueduct/cli/blueprint.py`.

Design:
- Every ``Tool`` is a thin wrapper over an EXISTING read function — never
  inline SQL. Data comes from ``aqueduct/stores/queries.py`` (the one
  read-time query layer), ``aqueduct/patch/index.py`` (via ``queries.py``),
  or ``aqueduct/doctor`` (the health-check runner). No handler receives a
  write handle: they only ever call ``open_obs_read`` / ``discover_stores`` /
  read-side ``patch_index`` functions — never ``open_obs_write`` or a store's
  mutating methods. A test asserts no handler's source references a write
  path.
- Every tool's result passes through ``aqueduct.redaction.redact()`` before
  it is returned — observability rows can embed error blobs (stack traces,
  connection strings) that may carry a resolved secret. ``call_tool()`` is
  the single call path that applies this, so a handler cannot forget it.
- ``params_schema`` is a JSON-schema-shaped dict (``{"type": "object",
  "properties": {...}, "required": [...]}``) — descriptive only in v1 (no
  validator is wired in); it exists so a future MCP server can hand it
  straight to a client without re-deriving parameter shapes.

RESERVED EXTENSIBILITY SEAM — do NOT implement loading yet:
``AQ_TOOLS_ENTRYPOINT_GROUP`` names the setuptools entry-point group a
custom-tool package would register under (mirrors
``aqueduct.executor.probe_plugins.AQ_PROBE_ENTRYPOINT_GROUP``). No code here
resolves it. When custom-tool loading ships, it must require explicit config
allowlisting (a tool is CODE that runs on the driver, same trust model as a
probe plugin or UDF) — deferring that now keeps v1's tool surface closed to
exactly the built-ins below.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# setuptools entry-point group reserved for future custom-tool distribution.
# NOT resolved anywhere in this module — see module docstring.
AQ_TOOLS_ENTRYPOINT_GROUP = "aqueduct.tools"


@dataclass(frozen=True)
class Tool:
    """One registry entry. ``read_only`` is structurally always True in v1 —
    there is no write tool and no handler receives a store write handle."""

    name: str
    description: str
    params_schema: dict[str, Any]
    handler: Callable[..., Any]
    read_only: bool = True


REGISTRY: dict[str, Tool] = {}


def register(tool: Tool) -> Tool:
    """Add a tool to the registry (used by this module's built-ins only —
    the entry-point seam above is reserved, not wired to this yet)."""
    if not tool.read_only:
        raise ValueError(f"tool {tool.name!r}: read_only=False is not supported in v1")
    REGISTRY[tool.name] = tool
    return tool


def get_tools() -> list[Tool]:
    """All registered tools, name-sorted (stable enumeration order)."""
    return [REGISTRY[name] for name in sorted(REGISTRY)]


def call_tool(name: str, **kwargs: Any) -> Any:
    """Invoke a registered tool by name and redact its result.

    The ONE call path — every result is run through
    ``aqueduct.redaction.redact()`` here, so an individual handler cannot
    forget to scrub a secret-bearing string out of its output.
    """
    from aqueduct import redaction as _redaction

    tool = REGISTRY.get(name)
    if tool is None:
        raise KeyError(f"unknown tool {name!r} — known: {sorted(REGISTRY)}")
    result = tool.handler(**kwargs)
    return _redaction.redact(result)


# ── shared helpers ───────────────────────────────────────────────────────────


def _load_cfg(config_path: str | None = None) -> Any:
    from aqueduct.config import load_config

    return load_config(Path(config_path) if config_path else None)


def _asdict(obj: Any) -> Any:
    """``dataclasses.asdict`` for a dataclass (incl. nested), passthrough
    for anything else (a plain dict/list a handler already built)."""
    import dataclasses

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    return obj


# ── built-in tool handlers ──────────────────────────────────────────────────
# Every handler param list ends with (config_path, store_dir) — the same
# `--config`/`--store-dir` pair every read CLI command already accepts. No
# handler accepts a store object, a writable cursor, or anything named
# `*_write` — that is what keeps the read-only contract structural.


def _list_runs(
    limit: int = 50,
    blueprint_id: str | None = None,
    config_path: str | None = None,
    store_dir: str | None = None,
) -> list[dict]:
    from aqueduct.stores.queries import discover_stores
    from aqueduct.stores.queries import list_runs as _q_list_runs

    cfg = _load_cfg(config_path)
    rows = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            rows.extend(_q_list_runs(h.store, limit=limit, blueprint_id=blueprint_id))
        except Exception:
            continue
    rows.sort(key=lambda r: r.started_at or "", reverse=True)
    return [_asdict(r) for r in rows[:limit]]


def _run_detail(
    run_id: str,
    config_path: str | None = None,
    store_dir: str | None = None,
) -> dict | None:
    from aqueduct.stores.queries import run_detail as _q_run_detail
    from aqueduct.stores.read import open_obs_read

    cfg = _load_cfg(config_path)
    store = open_obs_read(cfg, store_dir, run_id=run_id)
    if store is None:
        return None
    detail = _q_run_detail(store, run_id)
    return _asdict(detail) if detail is not None else None


def _lineage(
    blueprint_id: str,
    run_id: str | None = None,
    limit: int = 500,
    config_path: str | None = None,
    store_dir: str | None = None,
) -> list[dict]:
    from aqueduct.stores.queries import lineage as _q_lineage
    from aqueduct.stores.read import open_obs_read

    cfg = _load_cfg(config_path)
    store = open_obs_read(cfg, store_dir, blueprint_id=blueprint_id)
    if store is None:
        return []
    return [
        _asdict(r)
        for r in _q_lineage(
            store,
            blueprint_id=blueprint_id,
            run_id=run_id,
            limit=limit,
        )
    ]


def _patch_list(
    status: str | None = None,
    blueprint_id: str | None = None,
    config_path: str | None = None,
    store_dir: str | None = None,
) -> list[dict]:
    from aqueduct.stores.queries import patch_list as _q_patch_list

    cfg = _load_cfg(config_path)
    rows = _q_patch_list(cfg, store_dir=store_dir)
    if status:
        rows = [r for r in rows if r.status == status]
    if blueprint_id:
        rows = [r for r in rows if r.blueprint_id == blueprint_id]
    return [_asdict(r) for r in rows]


def _patch_show(
    patch_id: str,
    config_path: str | None = None,
    store_dir: str | None = None,
) -> dict | None:
    from aqueduct.stores.queries import patch_show as _q_patch_show

    cfg = _load_cfg(config_path)
    return _q_patch_show(cfg, patch_id, store_dir=store_dir)


def _probe_signals(
    blueprint_id: str,
    signal_type: str,
    run_id: str | None = None,
    limit: int = 20,
    config_path: str | None = None,
    store_dir: str | None = None,
) -> list[dict]:
    from aqueduct.stores.queries import probe_signals as _q_probe_signals
    from aqueduct.stores.read import open_obs_read

    cfg = _load_cfg(config_path)
    store = open_obs_read(cfg, store_dir, blueprint_id=blueprint_id)
    if store is None:
        return []
    return [
        _asdict(r)
        for r in _q_probe_signals(
            store,
            blueprint_id=blueprint_id,
            signal_type=signal_type,
            limit=limit,
            run_id=run_id,
        )
    ]


def _doctor(
    config_path: str | None = None,
    blueprint_path: str | None = None,
    skip_spark: bool = False,
    preflight: bool = False,
) -> dict:
    from aqueduct.doctor import run_doctor

    results = run_doctor(
        config_path=Path(config_path) if config_path else None,
        skip_spark=skip_spark,
        blueprint_path=Path(blueprint_path) if blueprint_path else None,
        preflight=preflight,
    )
    checks = [_asdict(r) for r in results]
    return {"passed": not any(c["status"] == "fail" for c in checks), "checks": checks}


def _blueprint_history(
    blueprint_id: str,
    blueprint_path: str | None = None,
    config_path: str | None = None,
    store_dir: str | None = None,
) -> list[dict]:
    # Reuses the CLI's merge helper (store events + git commits) so the
    # registry tool and `aqueduct blueprint history` never drift apart.
    from aqueduct.cli.blueprint import blueprint_history_events

    cfg = _load_cfg(config_path)
    path = Path(blueprint_path) if blueprint_path else None
    return blueprint_history_events(cfg, blueprint_id, path, store_dir=store_dir)


# ── registration ─────────────────────────────────────────────────────────────

register(
    Tool(
        name="list_runs",
        description="Recent pipeline runs, most recent first.",
        params_schema={
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 50},
                "blueprint_id": {"type": ["string", "null"], "default": None},
                "config_path": {"type": ["string", "null"], "default": None},
                "store_dir": {"type": ["string", "null"], "default": None},
            },
        },
        handler=_list_runs,
    )
)

register(
    Tool(
        name="run_detail",
        description="Module results + resource profile for one run.",
        params_schema={
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "config_path": {"type": ["string", "null"], "default": None},
                "store_dir": {"type": ["string", "null"], "default": None},
            },
            "required": ["run_id"],
        },
        handler=_run_detail,
    )
)

register(
    Tool(
        name="lineage",
        description="Column-level lineage rows for a blueprint (optionally one run).",
        params_schema={
            "type": "object",
            "properties": {
                "blueprint_id": {"type": "string"},
                "run_id": {"type": ["string", "null"], "default": None},
                "limit": {"type": "integer", "default": 500},
                "config_path": {"type": ["string", "null"], "default": None},
                "store_dir": {"type": ["string", "null"], "default": None},
            },
            "required": ["blueprint_id"],
        },
        handler=_lineage,
    )
)

register(
    Tool(
        name="patch_list",
        description="Patches across the fleet (optionally filtered by status/blueprint).",
        params_schema={
            "type": "object",
            "properties": {
                "status": {
                    "type": ["string", "null"],
                    "default": None,
                    "enum": [None, "pending", "applied", "rejected"],
                },
                "blueprint_id": {"type": ["string", "null"], "default": None},
                "config_path": {"type": ["string", "null"], "default": None},
                "store_dir": {"type": ["string", "null"], "default": None},
            },
        },
        handler=_patch_list,
    )
)

register(
    Tool(
        name="patch_show",
        description="One patch's patch_index metadata (+ body, best-effort).",
        params_schema={
            "type": "object",
            "properties": {
                "patch_id": {"type": "string"},
                "config_path": {"type": ["string", "null"], "default": None},
                "store_dir": {"type": ["string", "null"], "default": None},
            },
            "required": ["patch_id"],
        },
        handler=_patch_show,
    )
)

register(
    Tool(
        name="probe_signals",
        description="Probe signal payloads across recent runs of a blueprint.",
        params_schema={
            "type": "object",
            "properties": {
                "blueprint_id": {"type": "string"},
                "signal_type": {"type": "string"},
                "run_id": {"type": ["string", "null"], "default": None},
                "limit": {"type": "integer", "default": 20},
                "config_path": {"type": ["string", "null"], "default": None},
                "store_dir": {"type": ["string", "null"], "default": None},
            },
            "required": ["blueprint_id", "signal_type"],
        },
        handler=_probe_signals,
    )
)

register(
    Tool(
        name="doctor",
        description="Run aqueduct doctor's connectivity/health checks; structured CheckResults.",
        params_schema={
            "type": "object",
            "properties": {
                "config_path": {"type": ["string", "null"], "default": None},
                "blueprint_path": {"type": ["string", "null"], "default": None},
                "skip_spark": {"type": "boolean", "default": False},
                "preflight": {"type": "boolean", "default": False},
            },
        },
        handler=_doctor,
    )
)

register(
    Tool(
        name="blueprint_history",
        description="Chronological remediation timeline for one blueprint "
        "(heal runs, patch apply/reject, outcomes, manual git edits).",
        params_schema={
            "type": "object",
            "properties": {
                "blueprint_id": {"type": "string"},
                "blueprint_path": {"type": ["string", "null"], "default": None},
                "config_path": {"type": ["string", "null"], "default": None},
                "store_dir": {"type": ["string", "null"], "default": None},
            },
            "required": ["blueprint_id"],
        },
        handler=_blueprint_history,
    )
)
