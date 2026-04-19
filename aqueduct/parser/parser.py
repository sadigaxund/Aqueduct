"""Main Parser entrypoint.

Orchestrates: YAML load → Pydantic validation → context resolution →
graph validation → AST construction.

Output is a frozen Blueprint dataclass — the single input to the Compiler.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from aqueduct.parser.graph import detect_cycles, validate_spillway_targets
from aqueduct.parser.models import (
    AgentConfig,
    Blueprint,
    ContextRegistry,
    Edge,
    Module,
    RetryPolicy,
)
from aqueduct.parser.resolver import build_context_map, resolve_value
from aqueduct.parser.schema import BlueprintSchema


class ParseError(Exception):
    """Raised for any Blueprint parse, validation, or resolution failure."""


def parse(
    path: str | Path,
    profile: str | None = None,
    cli_overrides: dict[str, str] | None = None,
) -> Blueprint:
    """Parse a Blueprint YAML file and return a validated, resolved AST.

    Args:
        path:          Path to the Blueprint YAML file.
        profile:       Active context profile name (--profile flag).
        cli_overrides: Key=value overrides from --ctx CLI flags.

    Returns:
        A frozen Blueprint dataclass with all Tier 0 context resolved.

    Raises:
        ParseError: On any YAML, schema, context, or graph validation failure.
    """
    path = Path(path)

    # ── 1. Load YAML ──────────────────────────────────────────────────────────
    try:
        raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ParseError(f"Blueprint file not found: {path}")
    except yaml.YAMLError as exc:
        raise ParseError(f"Invalid YAML in {path.name}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ParseError(f"Blueprint must be a YAML mapping, got {type(raw).__name__}")

    # ── 2. Pydantic schema validation ─────────────────────────────────────────
    try:
        validated = BlueprintSchema.model_validate(raw)
    except ValidationError as exc:
        raise ParseError(f"Blueprint schema validation failed:\n{exc}") from exc

    # ── 3. Tier 0 context resolution ──────────────────────────────────────────
    try:
        ctx_map = build_context_map(
            context_dict=validated.context,
            profile=profile,
            profiles=validated.context_profiles,
            cli_overrides=cli_overrides,
        )
    except ValueError as exc:
        raise ParseError(f"Context resolution failed: {exc}") from exc

    # ── 4. Build Module dataclasses (with config substitution) ────────────────
    try:
        modules = tuple(
            Module(
                id=m.id,
                type=m.type,
                label=m.label,
                description=m.description,
                tags=tuple(m.tags),
                config=resolve_value(m.config, ctx_map),
                on_failure=m.on_failure,
                spillway=m.spillway,
                depends_on=tuple(m.depends_on),
                attach_to=m.attach_to,
                ref=m.ref,
                context_override=m.context_override,
            )
            for m in validated.modules
        )
    except ValueError as exc:
        raise ParseError(f"Module config resolution failed: {exc}") from exc

    # ── 5. Build Edge dataclasses ─────────────────────────────────────────────
    edges = tuple(
        Edge(
            from_id=e.from_id,
            to_id=e.to,
            port=e.port,
            error_types=tuple(e.error_types),
        )
        for e in validated.edges
    )

    # ── 6. Graph validation ───────────────────────────────────────────────────
    try:
        cycle_nodes = detect_cycles(list(modules), list(edges))
        if cycle_nodes:
            raise ParseError(
                f"Cycle detected in module graph. Involved modules: {cycle_nodes}"
            )
        validate_spillway_targets(list(modules))
    except ValueError as exc:
        raise ParseError(str(exc)) from exc

    # ── 7. Assemble final Blueprint AST ───────────────────────────────────────
    rp = validated.retry_policy
    retry_policy = RetryPolicy(
        max_attempts=rp.max_attempts,
        backoff_strategy=rp.backoff.strategy,
        backoff_base_seconds=rp.backoff.base_seconds,
        backoff_max_seconds=rp.backoff.max_seconds,
        jitter=rp.backoff.jitter,
        on_exhaustion=rp.on_exhaustion,
        transient_errors=tuple(rp.transient_errors),
        non_transient_errors=tuple(rp.non_transient_errors),
        deadline_seconds=rp.deadline_seconds,
    )

    agent = AgentConfig(
        approval_mode=validated.agent.approval_mode,
        on_pending_patches=validated.agent.on_pending_patches,
        model=validated.agent.model,
        max_patches_per_run=validated.agent.max_patches_per_run,
        provider=validated.agent.provider,
        base_url=validated.agent.base_url,
    )

    return Blueprint(
        aqueduct_version=validated.aqueduct,
        id=validated.id,
        name=validated.name,
        description=validated.description,
        context=ContextRegistry(values=ctx_map),
        modules=modules,
        edges=edges,
        spark_config=dict(validated.spark_config),
        retry_policy=retry_policy,
        agent=agent,
        udf_registry=tuple(validated.udf_registry),
        required_context=tuple(validated.required_context),
    )
