"""Execution result models — frozen dataclasses returned by the Executor."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModuleResult:
    """Outcome of executing a single module."""

    module_id: str
    status: str          # "success" | "error" | "skipped"
    error: str | None = None


@dataclass(frozen=True)
class ExecutionResult:
    """Aggregate outcome of a full pipeline run."""

    pipeline_id: str
    run_id: str
    status: str                          # "success" | "error"
    module_results: tuple[ModuleResult, ...]

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "status": self.status,
            "module_results": [
                {
                    "module_id": r.module_id,
                    "status": r.status,
                    "error": r.error,
                }
                for r in self.module_results
            ],
        }
