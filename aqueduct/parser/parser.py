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

from aqueduct.errors import ParseError
from aqueduct.parser.graph import (
    detect_cycles,
    validate_edge_error_types,
    validate_spillway_targets,
)
from aqueduct.parser.models import (
    AgentConfig,
    Blueprint,
    CascadeTierConfig,
    ContextRegistry,
    Edge,
    GuardrailsConfig,
    Module,
    RetryPolicy,
)
from aqueduct.parser.resolver import build_context_map, resolve_value
from aqueduct.parser.schema import BlueprintSchema


def _build_cascade(raw: list | None, ctx_map: dict | None = None) -> tuple | None:
    """Phase 44 — Convert Pydantic ``CascadeTierSchema`` list to frozen configs.

    ``ctx_map`` resolves ``${ENV:-default}`` template literals on the tier's
    string fields, exactly as the flat ``agent.*`` fields are resolved (ISSUE-047
    #1 — without this a cascade ``base_url`` reaches httpx as the literal
    ``${AQ_CASCADE_0_URL:-…}`` and fails "missing http:// protocol"). When
    ``ctx_map`` is None the raw values pass through unchanged.
    """
    if not raw:
        return None
    _rv = (lambda v: resolve_value(v, ctx_map)) if ctx_map is not None else (lambda v: v)
    tiers: list[CascadeTierConfig] = []
    for t in raw:
        tiers.append(CascadeTierConfig(
            model=_rv(t.model),
            provider=_rv(t.provider),
            base_url=_rv(t.base_url),
            api_key=_rv(t.api_key),
            provider_options=_rv(t.provider_options),
            timeout=t.timeout,
            max_tokens=t.max_tokens,
            max_reprompts=t.max_reprompts,
            max_seconds=float(t.max_seconds) if t.max_seconds is not None else None,
            deep_loop=t.deep_loop,
            allow_defer=t.allow_defer,
        ))
    return tuple(tiers)


def _format_validation_error(exc: ValidationError, raw: dict | None = None) -> str:
    """Format Pydantic ValidationError into a readable message."""
    errors = exc.errors(include_url=False)
    n = len(errors)
    header = f"{n} validation error{'s' if n != 1 else ''} in Blueprint:"
    lines = [header]
    modules_raw = raw.get("modules", []) if raw else []
    for e in errors:
        loc = e["loc"]
        loc_parts: list[str] = []
        for i, part in enumerate(loc):
            if part == "modules" and i == 0:
                loc_parts.append("module")
            elif isinstance(part, int) and i == 1 and loc[0] == "modules":
                mod = modules_raw[part] if part < len(modules_raw) else {}
                name = mod.get("id") or mod.get("label") or f"#{part}"
                loc_parts.append(f' "{name}"')
            elif isinstance(part, int):
                loc_parts.append(f"[{part}]")
            elif loc_parts:
                loc_parts.append(f" → {part}")
            else:
                loc_parts.append(str(part))
        lines.append(f"  • {''.join(loc_parts)} — {e['msg']}")
    return "\n".join(lines)


def parse(
    path: str | Path,
    profile: str | None = None,
    cli_overrides: dict[str, str] | None = None,
) -> Blueprint:
    """Parse a Blueprint YAML file and return a validated, resolved AST.

    Thin wrapper around :func:`parse_dict` — loads the YAML, then anchors
    path resolution to ``path.parent``. All in-memory / patched-in-place
    flows (sandbox replay, scenario apply, in-memory patch test) should
    call :func:`parse_dict` directly with the original Blueprint's
    ``base_dir``; routing through tempfiles breaks path anchoring because
    the tempfile sits in ``/tmp`` and relative paths resolve there.

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
    try:
        raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ParseError(f"Blueprint file not found: {path}")
    except yaml.YAMLError as exc:
        raise ParseError(f"Invalid YAML in {path.name}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ParseError(f"Blueprint must be a YAML mapping, got {type(raw).__name__}")
    return parse_dict(
        raw,
        base_dir=path.parent.resolve(),
        profile=profile,
        cli_overrides=cli_overrides,
    )


def parse_dict(
    raw: dict[str, Any],
    base_dir: Path,
    profile: str | None = None,
    cli_overrides: dict[str, str] | None = None,
) -> Blueprint:
    """Validate + resolve an already-loaded Blueprint dict.

    Canonical entrypoint for in-memory patch flows (sandbox replay,
    scenario apply, ad-hoc tests) that previously round-tripped through
    ``tempfile.NamedTemporaryFile``. The tempfile dance broke path
    anchoring because it placed the YAML in ``/tmp`` — relative paths
    like ``../data/in.csv`` then resolved against ``/tmp`` and produced
    absurd values.

    ``base_dir`` is the explicit anchor for relative path fields
    (``path``, ``data_dir``, ``input_dir``, ``output_dir``, ``jar``) in
    each module config. Pass the original Blueprint's parent directory
    so anchoring matches the on-disk behaviour. Same anchoring rule as
    1.1.0: ``s3://`` / ``gs://`` / ``file://`` and absolute paths pass
    through untouched.

    Args:
        raw:           Already-loaded Blueprint dict (e.g. from
                       ``yaml.safe_load`` or an in-memory patched copy).
        base_dir:      Directory used to anchor relative path fields.
        profile:       Active context profile name (--profile flag).
        cli_overrides: Key=value overrides from --ctx CLI flags.

    Returns:
        A frozen Blueprint dataclass with all Tier 0 context resolved.

    Raises:
        ParseError: On any schema, context, or graph validation failure.
    """
    if not isinstance(raw, dict):
        raise ParseError(f"Blueprint must be a mapping, got {type(raw).__name__}")

    # ── 2. Pydantic schema validation ─────────────────────────────────────────
    try:
        validated = BlueprintSchema.model_validate(raw)
    except ValidationError as exc:
        raise ParseError(_format_validation_error(exc, raw)) from exc

    # ── 3. Tier 0 context resolution ──────────────────────────────────────────
    try:
        ctx_map = build_context_map(
            context_dict=validated.context,
            profile=profile,
            profiles=validated.context_profiles,
            cli_overrides=cli_overrides,
        )
    except (ValueError, ParseError) as exc:
        raise ParseError(f"Context resolution failed: {exc}") from exc

    # ── 4. Build Module dataclasses (with config substitution) ────────────────
    #
    # 1.1.0 — Anchor relative path fields to ``base_dir`` so
    # `ingress.path`, `egress.path`, etc. resolve consistently regardless of
    # the CWD `aqueduct run` was invoked from. Matches industry norm
    # (Compose, k8s, Terraform): paths inside a YAML resolve to that YAML's
    # parent. For ``parse(path)`` ``base_dir`` is ``path.parent``; for
    # in-memory patch flows the caller passes the original Blueprint's
    # parent directory so the patched dict behaves identically to the
    # on-disk version.
    _bp_dir = Path(base_dir).resolve()

    def _anchor_path_value(val: Any) -> Any:
        if not isinstance(val, str) or not val:
            return val
        if "://" in val:  # s3://, gs://, file://, etc. — leave untouched
            return val
        p = Path(val)
        if p.is_absolute():
            return val
        return str((_bp_dir / p).resolve())

    # Keys to anchor come from ``aqueduct.executor.path_keys`` (per-type
    # registry), not a hardcoded tuple. Unknown types fall back to a
    # blanket union for backward compatibility — shrink the fallback to
    # ``()`` once every executor module type has an explicit entry there.
    from aqueduct.executor.path_keys import get_path_keys as _get_path_keys

    def _anchor_paths(cfg: Any, module_type: str) -> Any:
        if not isinstance(cfg, dict):
            return cfg
        out = dict(cfg)
        for k in _get_path_keys(module_type):
            if k in out:
                out[k] = _anchor_path_value(out[k])
        return out

    try:
        modules = tuple(
            Module(
                id=m.id,
                type=m.type,
                label=m.label,
                description=m.description,
                tags=tuple(m.tags),
                config=_anchor_paths(resolve_value(m.config, ctx_map), m.type),
                on_failure=m.on_failure,
                on_failure_webhook=m.on_failure_webhook,
                checkpoint=m.checkpoint,
                spillway=m.spillway,
                depends_on=tuple(m.depends_on),
                attach_to=m.attach_to,
                ref=m.ref,
                context_override=resolve_value(m.context_override, ctx_map),
            )
            for m in validated.modules
        )
    except (ValueError, ParseError) as exc:
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
        validate_edge_error_types(list(edges))
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

    try:
        agent = AgentConfig(
            approval_mode=validated.agent.approval_mode,
            on_pending_patches=validated.agent.on_pending_patches,
            model=resolve_value(validated.agent.model, ctx_map),
            max_patches=validated.agent.max_patches,
            provider=validated.agent.provider,
            base_url=resolve_value(validated.agent.base_url, ctx_map),
            api_key=resolve_value(validated.agent.api_key, ctx_map),
            provider_options=resolve_value(validated.agent.provider_options, ctx_map),
            guardrails=GuardrailsConfig(
                forbidden_ops=tuple(validated.agent.guardrails.forbidden_ops),
                allowed_paths=tuple(validated.agent.guardrails.allowed_paths),
                heal_on_errors=tuple(validated.agent.guardrails.heal_on_errors),
                never_heal_errors=tuple(validated.agent.guardrails.never_heal_errors),
            ),
            prompt_context=resolve_value(validated.agent.prompt_context, ctx_map),
            timeout=validated.agent.timeout,
            max_reprompts=validated.agent.max_reprompts,
            confidence_threshold=validated.agent.confidence_threshold,
            on_heal_failure=validated.agent.on_heal_failure,
            allow_defer=validated.agent.allow_defer,
            deep_loop=validated.agent.deep_loop,
            cascade=_build_cascade(validated.agent.cascade, ctx_map),
            max_heal_attempts_per_hour=validated.agent.max_heal_attempts_per_hour,
            patch_validation=validated.agent.patch_validation,
            block_on_explain_regression=validated.agent.block_on_explain_regression,
            sandbox_mode=validated.agent.sandbox_mode,
        )
    except (ValueError, ParseError) as exc:
        raise ParseError(f"agent config resolution failed: {exc}") from exc

    # Tier-0 resolution applies here too (parity with module config /
    # context_override) — ${ENV:-default} / ${ctx.*} in spark_config and
    # macros must be substituted, Spark/macros do no var expansion (ISSUE-027).
    # Wrapped so a bad ${ctx.*} surfaces as ParseError, not raw ValueError.
    try:
        resolved_spark_config = resolve_value(dict(validated.spark_config), ctx_map)
        resolved_macros = resolve_value(dict(validated.macros), ctx_map)
    except (ValueError, ParseError) as exc:
        raise ParseError(f"spark_config / macros resolution failed: {exc}") from exc

    return Blueprint(
        aqueduct_version=validated.aqueduct,
        id=validated.id,
        name=validated.name,
        description=validated.description,
        context=ContextRegistry(values=ctx_map),
        modules=modules,
        edges=edges,
        spark_config=resolved_spark_config,
        retry_policy=retry_policy,
        agent=agent,
        # UdfSchema → plain dicts (the compiler/executor consume dicts via .get()).
        # by_alias dumps `class_name` back to `class`; exclude_none keeps the dict
        # shape the executor expects (it applies its own field defaults).
        udf_registry=tuple(
            resolve_value(u.model_dump(by_alias=True, exclude_none=True), ctx_map)
            for u in validated.udf_registry
        ),
        macros=resolved_macros,
        required_context=tuple(validated.required_context),
        checkpoint=validated.checkpoint,
        warning_suppress=tuple(validated.warnings.suppress),
    )
