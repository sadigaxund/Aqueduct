"""Surveyor data models — frozen dataclasses for run records and failure context.

FailureContext is the primary artifact consumed by the LLM agent in later phases.
It is serialised to JSON and embedded in the DuckDB observability store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RunRecord:
    """Persisted record of a single blueprint run."""

    run_id: str
    blueprint_id: str
    status: str                     # "running" | "success" | "error"
    started_at: str                 # ISO-8601 UTC
    finished_at: str | None         # None while running
    module_results: tuple[dict, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "blueprint_id": self.blueprint_id,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "module_results": list(self.module_results),
        }


@dataclass(frozen=True)
class FailureContext:
    """Structured failure payload — logged to DuckDB and sent to webhook / LLM.

    All fields required by the LLM in later phases are included here so that
    the context document is self-contained and can be passed without additional
    Manifest lookups.
    """

    run_id: str
    blueprint_id: str
    failed_module: str          # module_id of the first failing module, or "_executor"
    error_message: str
    stack_trace: str | None
    manifest_json: str          # JSON-serialised Manifest.to_dict()
    started_at: str             # ISO-8601 UTC
    finished_at: str            # ISO-8601 UTC

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "blueprint_id": self.blueprint_id,
            "failed_module": self.failed_module,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "manifest_json": self.manifest_json,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)
