"""Manifest — the fully-compiled, execution-ready form of a Blueprint.

Differences from Blueprint:
  - All Tier 1 (@aq.*) tokens resolved to concrete values.
  - Arcade modules expanded; all module IDs are flat and namespaced.
  - Passive Regulators (no wired signal port) compiled away.
  - Probes validated against their attach_to targets.

The Manifest is the single document given to the Executor and embedded
verbatim in FailureContext packages sent to the LLM agent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from aqueduct.parser.models import AgentConfig, Edge, Module, RetryPolicy

if TYPE_CHECKING:
    from aqueduct.compiler.provenance import ProvenanceMap


@dataclass(frozen=True, kw_only=True)
class Manifest:
    # Required — no defaults
    blueprint_id: str
    context: dict[str, str]
    modules: tuple[Module, ...]
    edges: tuple[Edge, ...]
    spark_config: dict[str, Any]
    # Optional — defaulted so tests can construct minimal Manifests
    name: str = ""
    description: str = ""
    aqueduct_version: str = "1.0"
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    agent: AgentConfig = field(default_factory=AgentConfig)
    udf_registry: tuple[dict[str, Any], ...] = ()
    macros: dict[str, str] = field(default_factory=dict)
    checkpoint: bool = False
    provenance_map: "ProvenanceMap | None" = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict for writing to disk."""
        return {
            "blueprint_id": self.blueprint_id,
            "name": self.name,
            "description": self.description,
            "aqueduct_version": self.aqueduct_version,
            "context": self.context,
            "modules": [
                {
                    "id": m.id,
                    "type": m.type,
                    "label": m.label,
                    "description": m.description,
                    "tags": list(m.tags),
                    "config": m.config,
                    "attach_to": m.attach_to,
                    "spillway": m.spillway,
                    "depends_on": list(m.depends_on),
                }
                for m in self.modules
            ],
            "edges": [
                {
                    "from": e.from_id,
                    "to": e.to_id,
                    "port": e.port,
                    "error_types": list(e.error_types),
                }
                for e in self.edges
            ],
            "spark_config": self.spark_config,
            "retry_policy": {
                "max_attempts": self.retry_policy.max_attempts,
                "backoff_strategy": self.retry_policy.backoff_strategy,
                "backoff_base_seconds": self.retry_policy.backoff_base_seconds,
                "backoff_max_seconds": self.retry_policy.backoff_max_seconds,
                "jitter": self.retry_policy.jitter,
                "on_exhaustion": self.retry_policy.on_exhaustion,
                "transient_errors": list(self.retry_policy.transient_errors),
                "non_transient_errors": list(self.retry_policy.non_transient_errors),
                "deadline_seconds": self.retry_policy.deadline_seconds,
            },
            "agent": {
                "approval_mode": self.agent.approval_mode,
                "model": self.agent.model,
                "max_patches_per_run": self.agent.max_patches_per_run,
                "provider": self.agent.provider,
                "base_url": self.agent.base_url,
                "validate_patch": self.agent.validate_patch,
                "prompt_context": self.agent.prompt_context,
                "guardrails": {
                    "forbidden_ops": list(self.agent.guardrails.forbidden_ops),
                    "allowed_paths": list(self.agent.guardrails.allowed_paths),
                },
            },
            "udf_registry": list(self.udf_registry),
            "macros": self.macros,
            "checkpoint": self.checkpoint,
            "provenance_map": self.provenance_map.to_dict() if self.provenance_map else None,
        }
