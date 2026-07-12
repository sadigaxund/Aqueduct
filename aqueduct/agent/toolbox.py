"""Per-heal ToolBox — read-only diagnostic tools for agentic-mode healing (Phase 75).

Bridges two tool surfaces into one provider-neutral declaration/dispatch pair
that ``loop.py``'s agentic-mode conversation drives:

- **Registry diagnostics** (Phase 73's ``aqueduct/tools/registry.py``) —
  ``list_runs``, ``run_detail``, ``lineage``, ``patch_list``, ``patch_show``,
  ``probe_signals``, ``blueprint_history``. Invoked ONLY via
  ``aqueduct.tools.call_tool()`` — the redaction chokepoint — never a handler
  directly. ``doctor`` is deliberately excluded here: it is a health-probe,
  not a failure-diagnosis tool, and its Spark/network checks are out of place
  mid-heal.
- **Heal-context tools** this module defines itself — ``read_blueprint``,
  ``get_source_schema``, ``sample_rows`` — all read-only, all pass through
  ``redaction.redact()`` themselves (the registry chokepoint doesn't cover
  them, so ToolBox is where that discipline lives for its own tools).

A ``ToolBox`` is built fresh per heal call (not shared across attempts) —
it closes over the failure's ``manifest``/``failure_ctx`` and an optional
live ``spark_session`` (session-bound tools degrade to a structured
"unavailable" result when no session is present, e.g. scenario/benchmark
runs which never start Spark).

Every handler is read-only — nothing here writes a store, applies a patch,
or mutates the Blueprint. ``max_tool_calls`` enforcement (the hard per-attempt
cap) lives in the caller (``loop.py``'s agentic turn driver), not here — the
ToolBox only executes what it's asked to.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from aqueduct.redaction import redact as _redact

logger = logging.getLogger(__name__)

# Registry tools surfaced to the model. `doctor` is intentionally excluded
# (health-probe, not failure-diagnosis — see module docstring); every other
# Phase 73 built-in is diagnostic and safe to expose.
_REGISTRY_TOOL_NAMES: tuple[str, ...] = (
    "list_runs",
    "run_detail",
    "lineage",
    "patch_list",
    "patch_show",
    "probe_signals",
    "blueprint_history",
)

# Size cap for read_blueprint — a pathological Blueprint YAML shouldn't blow
# the context window.
_MAX_BLUEPRINT_CHARS = 20_000

# Hard row cap for sample_rows — never negotiable via the `n` argument.
_MAX_SAMPLE_ROWS = 20

_UNAVAILABLE_NO_SESSION = "no Spark session available in this heal context"


def _own_tool_declarations() -> list[dict[str, Any]]:
    """ToolBox-defined (non-registry) tool declarations, registry `Tool` shape."""
    return [
        {
            "name": "read_blueprint",
            "description": (
                "The failing Blueprint's YAML source text (size-capped). Use "
                "this to see the full authored config, not just the failed "
                "module's resolved config already in the failure report."
            ),
            "params_schema": {"type": "object", "properties": {}},
        },
        {
            "name": "get_source_schema",
            "description": (
                "Live schema of an Ingress module's source, read directly from "
                "the data (not the Blueprint's schema_hint). Metadata-only — "
                "zero Spark actions. Unavailable when this heal has no live "
                "Spark session (e.g. benchmark/scenario runs)."
            ),
            "params_schema": {
                "type": "object",
                "properties": {"module_id": {"type": "string"}},
                "required": ["module_id"],
            },
        },
        {
            "name": "sample_rows",
            "description": (
                "Up to 20 real rows from an Ingress module's source, via "
                "limit(n).collect() (bounded — never a full scan). Supports "
                "Ingress modules only in v1. Unavailable without a live Spark "
                "session."
            ),
            "params_schema": {
                "type": "object",
                "properties": {
                    "module_id": {"type": "string"},
                    "n": {"type": "integer", "default": 10, "maximum": _MAX_SAMPLE_ROWS},
                },
                "required": ["module_id"],
            },
        },
    ]


@dataclass
class ToolBox:
    """Per-heal read-only tool surface handed to the agentic-mode LLM loop.

    Args:
        manifest: The compiled Manifest for the failing Blueprint (module
            list — used by ``get_source_schema``/``sample_rows`` to resolve
            ``module_id`` to a compiled Module).
        failure_ctx: The FailureContext for this heal — ``read_blueprint``
            serves its ``blueprint_source_yaml`` verbatim (already read once
            at failure time; no re-read from disk).
        obs_store: Present for interface symmetry with the registry tools'
            call shape; the registry handlers re-derive their own store from
            ``config_path``/``store_dir`` (see those fields below) rather
            than accepting a store object directly, so this is not read
            by ToolBox itself today. Kept on the dataclass because a future
            registry tool may accept a store handle directly.
        patch_store: Same note as ``obs_store`` — kept for interface
            symmetry; not read directly by ToolBox today.
        base_dir: The Blueprint's base directory (path anchoring for any
            future tool that needs it — unused by the v1 tool set, which
            resolves everything through ``manifest``/``failure_ctx``).
        spark_session: Live SparkSession, or None. Session-bound tools
            (``get_source_schema``, ``sample_rows``) return a structured
            ``{"available": False, "reason": ...}`` result instead of
            raising when this is None.
        config_path: Forwarded to registry tools whose ``params_schema``
            accepts it, so ``list_runs``/``run_detail``/etc. resolve the
            SAME ``aqueduct.yml`` the heal itself is running under.
        store_dir: Same forwarding as ``config_path``, for the observability
            store directory.
    """

    manifest: Any
    failure_ctx: Any
    obs_store: Any = None
    patch_store: Any = None
    base_dir: str = ""
    spark_session: Any = None
    config_path: str | None = None
    store_dir: str | None = None

    # ── declarations ─────────────────────────────────────────────────────

    def declarations(self) -> list[dict[str, Any]]:
        """Provider-neutral tool list: name / description / params_schema.

        Same shape as ``aqueduct.tools.registry.Tool`` (minus ``handler``/
        ``read_only`` — those are implementation details the provider layer
        doesn't need). Providers translate this into their own tool-use
        wire format (Anthropic ``input_schema``, OpenAI ``function``).
        """
        from aqueduct.tools.registry import REGISTRY

        decls: list[dict[str, Any]] = []
        for name in _REGISTRY_TOOL_NAMES:
            tool = REGISTRY.get(name)
            if tool is None:
                continue  # defensive — registry composition changed underneath us
            decls.append({
                "name": tool.name,
                "description": tool.description,
                "params_schema": tool.params_schema,
            })
        decls.extend(_own_tool_declarations())
        return decls

    # ── dispatch ─────────────────────────────────────────────────────────

    def call(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        """Invoke one tool by name; result is always ``redaction.redact()``-clean.

        Never raises on a bad/unknown call from the model — returns a
        structured error dict instead, so a hallucinated tool name or bad
        argument becomes a tool_result the model can recover from, not a
        crashed heal attempt.
        """
        arguments = dict(arguments or {})
        try:
            if name in _REGISTRY_TOOL_NAMES:
                return self._call_registry_tool(name, arguments)
            if name == "read_blueprint":
                return _redact(self._read_blueprint())
            if name == "get_source_schema":
                return _redact(self._get_source_schema(arguments.get("module_id", "")))
            if name == "sample_rows":
                return _redact(self._sample_rows(
                    arguments.get("module_id", ""), arguments.get("n", 10),
                ))
            return {"error": f"unknown tool {name!r} — known: {self._known_names()}"}
        except Exception as exc:
            # A tool call failing must not crash the heal attempt — surface
            # the (redacted) error as the tool_result so the model can adapt.
            logger.debug("ToolBox tool %r raised", name, exc_info=True)
            return {"error": _redact(f"{type(exc).__name__}: {exc}")}

    def _known_names(self) -> list[str]:
        return sorted(_REGISTRY_TOOL_NAMES) + ["read_blueprint", "get_source_schema", "sample_rows"]

    def _call_registry_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        from aqueduct.tools.registry import REGISTRY, call_tool

        tool = REGISTRY.get(name)
        kwargs = dict(arguments)
        if tool is not None:
            props = tool.params_schema.get("properties", {})
            if "config_path" in props and kwargs.get("config_path") is None:
                kwargs["config_path"] = self.config_path
            if "store_dir" in props and kwargs.get("store_dir") is None:
                kwargs["store_dir"] = self.store_dir
        # call_tool() already applies redaction — do not double-redact.
        return call_tool(name, **kwargs)

    # ── own read-only tools ──────────────────────────────────────────────

    def _read_blueprint(self) -> dict[str, Any]:
        text = getattr(self.failure_ctx, "blueprint_source_yaml", None)
        if not text:
            return {"available": False, "reason": "no blueprint source captured for this failure"}
        truncated = len(text) > _MAX_BLUEPRINT_CHARS
        if truncated:
            text = text[:_MAX_BLUEPRINT_CHARS] + f"\n… (truncated at {_MAX_BLUEPRINT_CHARS} chars)"
        return {"available": True, "yaml": text, "truncated": truncated}

    def _find_module(self, module_id: str) -> Any | None:
        modules = getattr(self.manifest, "modules", None) or ()
        for m in modules:
            if getattr(m, "id", None) == module_id:
                return m
        return None

    def _get_source_schema(self, module_id: str) -> dict[str, Any]:
        if not module_id:
            return {"available": False, "reason": "module_id is required"}
        if self.spark_session is None:
            return {"available": False, "reason": _UNAVAILABLE_NO_SESSION}
        module = self._find_module(module_id)
        if module is None:
            return {"available": False, "reason": f"unknown module_id {module_id!r}"}
        from aqueduct.parser.models import ModuleType

        if getattr(module, "type", None) != ModuleType.Ingress:
            return {"available": False, "reason": f"{module_id!r} is not an Ingress module"}
        # Lazy pyspark import — the agent package top level stays pyspark-free
        # (only fires when a live spark_session was supplied).
        from aqueduct.executor.spark.ingress import read_source_schema

        schema = read_source_schema(module, self.spark_session)
        return {"available": True, "module_id": module_id, "schema": schema}

    def _sample_rows(self, module_id: str, n: Any) -> dict[str, Any]:
        if not module_id:
            return {"available": False, "reason": "module_id is required"}
        if self.spark_session is None:
            return {"available": False, "reason": _UNAVAILABLE_NO_SESSION}
        try:
            n_int = int(n)
        except (TypeError, ValueError):
            n_int = 10
        n_int = max(1, min(n_int, _MAX_SAMPLE_ROWS))
        module = self._find_module(module_id)
        if module is None:
            return {"available": False, "reason": f"unknown module_id {module_id!r}"}
        from aqueduct.parser.models import ModuleType

        if getattr(module, "type", None) != ModuleType.Ingress:
            return {
                "available": False,
                "reason": (
                    f"{module_id!r} is not an Ingress module — sample_rows "
                    "supports source reads only in v1, not intermediate "
                    "transform output"
                ),
            }
        # Lazy pyspark import — same governance as the executor Probe's
        # sample_rows signal: bounded via limit(n).collect() so this is
        # exempt from the block_full_actions gate (a LIMIT push-down never
        # scans the full dataset, unlike row_count_estimate/null_rates'
        # fraction-of-whole sampling — see executor/spark/probe.py::_sample_rows).
        from aqueduct.executor.spark.ingress import read_ingress

        _base_dir = self.base_dir or getattr(self.manifest, "base_dir", None)
        df = read_ingress(module, self.spark_session, base_dir=_base_dir)
        rows = df.limit(n_int).collect()
        return {
            "available": True,
            "module_id": module_id,
            "n": n_int,
            "rows": [row.asDict(recursive=True) for row in rows],
        }
