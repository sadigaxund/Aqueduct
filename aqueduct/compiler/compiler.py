"""Main Compiler orchestrator.

Pipeline:
  Blueprint (AST)
    → [1] Tier 1 resolution (@aq.*)
    → [2] Arcade expansion (flat module list)
    → [3] Probe / Spillway validation
    → [4] Passive Regulator compile-away
    → Manifest (JSON-ready)

The Compiler does NOT:
  - Initialize a SparkSession.
  - Write to disk (the CLI/Executor does that).
  - Make LLM calls.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

from aqueduct.compiler.expander import ExpandError, expand_arcades
from aqueduct.compiler.macros import MacroError, resolve_macros_in_config
from aqueduct.compiler.models import Manifest
from aqueduct.compiler.runtime import AqFunctions, resolve_tier1
from aqueduct.compiler.wirer import (
    WireError,
    compile_away_regulators,
    validate_probes,
    validate_spillway_edges,
)
from aqueduct.parser.models import Blueprint, Module
from aqueduct.parser.resolver import _CTX_RE, _sub_ctx  # Tier 0 re-pass after Tier 1


class CompileError(Exception):
    """Raised for any compilation failure."""


def _resolve_module_tier1(m: Module, registry: AqFunctions) -> Module:
    """Return a copy of Module with all @aq.* tokens resolved in its config."""
    resolved_config: Any = resolve_tier1(m.config, registry)
    if resolved_config is m.config:
        return m
    return dataclasses.replace(m, config=resolved_config)


def compile(  # noqa: A001
    blueprint: Blueprint,
    blueprint_path: Path | None = None,
    run_id: str | None = None,
    depot: Any = None,
    execution_date: Any = None,
) -> Manifest:
    """Compile a parsed Blueprint into a fully-resolved Manifest.

    Args:
        blueprint:      Parsed, validated Blueprint AST from the Parser.
        blueprint_path: Path to the Blueprint file on disk. Required for Arcade
                        expansion (sub-Blueprint paths are relative to this file).
        run_id:         Optional run UUID. Auto-generated if not provided.
        depot:          Optional Depot connection for @aq.depot.get() resolution.

    Returns:
        A frozen Manifest ready for the Executor.

    Raises:
        CompileError: On any Tier 1 resolution, expansion, or wiring failure.
    """
    registry = AqFunctions(run_id=run_id, depot=depot, execution_date=execution_date)

    # ── 1. Resolve Tier 1 in context values ───────────────────────────────────
    try:
        resolved_ctx: dict[str, str] = {
            k: resolve_tier1(v, registry)
            for k, v in blueprint.context.values.items()
        }
    except (ValueError, RuntimeError) as exc:
        raise CompileError(f"Tier 1 context resolution failed: {exc}") from exc

    # ── 2. Re-run ${ctx.*} substitution with the now-fully-resolved context ───
    # Needed for context values that referenced other Tier 1 context entries.
    # e.g. context.path = "data/${ctx.today}/out" where ctx.today was @aq.date.today()
    try:
        for key in list(resolved_ctx):
            if _CTX_RE.search(resolved_ctx[key]):
                resolved_ctx[key] = _sub_ctx(resolved_ctx[key], resolved_ctx)
    except ValueError as exc:
        raise CompileError(f"Post-Tier-1 context re-resolution failed: {exc}") from exc

    # ── 3. Resolve Tier 1 in module configs ───────────────────────────────────
    try:
        modules: list[Module] = [
            _resolve_module_tier1(m, registry) for m in blueprint.modules
        ]
    except (ValueError, RuntimeError) as exc:
        raise CompileError(f"Tier 1 module config resolution failed: {exc}") from exc

    # ── 3.5. Resolve SQL macros in module configs ─────────────────────────────
    if blueprint.macros:
        try:
            modules = [
                dataclasses.replace(m, config=resolve_macros_in_config(m.config, blueprint.macros))
                for m in modules
            ]
        except MacroError as exc:
            raise CompileError(f"SQL macro resolution failed: {exc}") from exc

    # ── 4. Expand Arcades ─────────────────────────────────────────────────────
    edges = list(blueprint.edges)
    if any(m.type == "Arcade" for m in modules):
        if blueprint_path is None:
            raise CompileError(
                "Blueprint contains Arcade modules but blueprint_path was not provided. "
                "Pass the Blueprint file path to compile() so Arcade refs can be resolved."
            )
        try:
            modules, edges = expand_arcades(modules, edges, blueprint_path.parent)
        except ExpandError as exc:
            raise CompileError(f"Arcade expansion failed: {exc}") from exc

    # ── 5. Validate Probes and Spillways ──────────────────────────────────────
    try:
        validate_probes(modules)
        validate_spillway_edges(modules, edges)
    except WireError as exc:
        raise CompileError(f"Wiring validation failed: {exc}") from exc

    # ── 6. Compile away passive Regulators ────────────────────────────────────
    modules, edges = compile_away_regulators(modules, edges)

    # ── 7. Delivery semantics warning ─────────────────────────────────────────
    if blueprint.retry_policy.max_attempts > 1:
        import warnings
        for m in modules:
            if m.type == "Egress" and m.config.get("mode") == "append":
                warnings.warn(
                    f"Egress '{m.id}' uses mode=append with "
                    f"max_attempts={blueprint.retry_policy.max_attempts} — "
                    "retries may produce duplicate rows. "
                    "Use mode=overwrite for idempotent writes, or set max_attempts=1.",
                    stacklevel=2,
                )

    return Manifest(
        blueprint_id=blueprint.id,
        name=blueprint.name,
        description=blueprint.description,
        aqueduct_version=blueprint.aqueduct_version,
        context=resolved_ctx,
        modules=tuple(modules),
        edges=tuple(edges),
        spark_config=dict(blueprint.spark_config),
        retry_policy=blueprint.retry_policy,
        agent=blueprint.agent,
        udf_registry=blueprint.udf_registry,
        macros=dict(blueprint.macros),
        checkpoint=blueprint.checkpoint,
    )
