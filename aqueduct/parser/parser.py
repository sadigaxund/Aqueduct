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
    validate_edge_aliases,
    validate_edge_error_types,
    validate_spillway_targets,
    validate_watermark_keys,
)
from aqueduct.parser.models import (
    AgentConfig,
    Blueprint,
    CascadeTierConfig,
    ContextRegistry,
    Edge,
    GuardrailsConfig,
    HealedByRecord,
    HookEntry,
    Hooks,
    Module,
    RetryPolicy,
)
from aqueduct.parser.resolver import build_context_map, resolve_value
from aqueduct.parser.schema import BlueprintSchema


def _resolve_engine_block_raw(engine_block: Any) -> dict[str, Any]:
    """One engine's Blueprint-level `engine.<name>:` block → the raw dict
    that becomes `Blueprint.engine_config[<name>]`, BEFORE Tier-0
    (`${ENV}`/`${ctx.*}`) resolution.

    Spark's block nests its free-form session properties under a `conf:`
    sub-field (arbitrary `spark.*` keys) — that dict already contains only
    what the author typed, so it is returned as-is. Every other engine's
    block declares its session-affecting fields directly on the model
    (`DuckDBEngineBlockSchema` carries `memory_limit`/`threads`, 2.54), so
    `model_dump()` is the generic per-engine bag — but it MUST be
    `exclude_unset=True`.

    Plain `model_dump()` (no args) emits EVERY declared field, including
    ones the author never wrote in the Blueprint, each carrying its
    pydantic default (typically `None`). A Blueprint that sets only ONE
    field on a non-Spark engine block (e.g. just `engine.duckdb.memory_limit`)
    would otherwise produce `{"memory_limit": "8GB", "threads": None}`, and
    `aqueduct.executor.session_config.resolve_session_engine_config`'s merge
    (Blueprint wins) would let that `None` silently clobber a real
    `aqueduct.yml`-level `threads` value the Blueprint never touched — the
    exact falsy/None trap AGENTS.md warns about. `exclude_unset=True`
    keeps only the fields pydantic's own `model_fields_set` says were
    actually provided — the same primitive
    `aqueduct.executor.config_leaves.explicitly_set_config_leaves` uses for
    exactly this explicit-vs-default distinction.
    """
    if "conf" in type(engine_block).model_fields:
        return dict(engine_block.conf)
    return engine_block.model_dump(exclude_unset=True)


def _hook_entry(h) -> HookEntry:
    """HookEntrySchema → HookEntry (kind + verbatim value)."""
    for kind in ("blueprint", "webhook", "command"):
        v = getattr(h, kind)
        if v is not None:
            return HookEntry(
                kind=kind,
                value=v,
                timeout=h.timeout,
                when_error=tuple(h.when_error),
                in_process=h.in_process,
            )
    raise ParseError("hook entry has no action")  # unreachable — schema enforces exactly-one


def _healed_by_entry(r) -> HealedByRecord:
    """HealedByRecordSchema → HealedByRecord, verbatim (machine-written data,
    no Tier 0/1 resolution — see HealedByRecord docstring)."""
    return HealedByRecord(
        patch_id=r.patch_id,
        engine=r.engine,
        classification=r.classification,
        applied_at=r.applied_at,
        validated_on=tuple(r.validated_on),
        reverted_at=r.reverted_at,
    )


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
        tiers.append(
            CascadeTierConfig(
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
            )
        )
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
    except IsADirectoryError:
        raise ParseError(f"Blueprint path is a directory, not a file: {path}")
    except PermissionError as exc:
        raise ParseError(f"Permission denied reading Blueprint file {path}: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ParseError(f"Blueprint file {path} is not valid UTF-8 text: {exc}") from exc
    except OSError as exc:
        # Any other filesystem-level failure (e.g. a broken symlink, an I/O
        # error from a network mount) — user-reachable via a bad --blueprint
        # path, so it must surface as ParseError rather than escape raw
        # (AGENTS.md: "User-reachable errors raise an AqueductError
        # subclass, never a bare builtin"). FileNotFoundError/
        # IsADirectoryError/PermissionError are caught more specifically
        # above for a clearer message; this is the catch-all sibling.
        raise ParseError(f"Cannot read Blueprint file {path}: {exc}") from exc
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
    # registry), not a hardcoded tuple. Every module type now has an explicit
    # row there (Pass C, 2026-08); an unknown type's fallback is already
    # ``()`` — a structural safety net, not a source of real anchoring
    # behaviour. See ``path_keys.py``'s own comment on ``_LEGACY_FALLBACK``.
    from aqueduct.executor.path_keys import get_path_keys as _get_path_keys

    def _anchor_paths(cfg: Any, module_type: str) -> Any:
        if not isinstance(cfg, dict):
            return cfg
        out = dict(cfg)
        for k in _get_path_keys(module_type):
            if k in out:
                out[k] = _anchor_path_value(out[k])
        return out

    def _anchor_udf_entry(entry: dict[str, Any]) -> dict[str, Any]:
        """Anchor a udf_registry entry's `jar`/`path` keys to base_dir.

        udf_registry is a top-level list (not a module's `config:` block),
        so `_anchor_paths` above never walks it — this is the "UDF" row's
        own anchoring call site.
        """
        out = dict(entry)
        for k in _get_path_keys("UDF"):
            if k in out:
                out[k] = _anchor_path_value(out[k])
        return out

    def _to_retry_policy(rp: Any) -> RetryPolicy:
        """Convert a RetryPolicySchema (blueprint-level, all fields required
        with defaults) into a RetryPolicy dataclass."""
        return RetryPolicy(
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

    # Computed early (before the module loop) so per-module `retry:` overrides
    # can be merged against it field-by-field — same per-field inheritance
    # shape as agent cascade tiers (aqueduct/agent/cascade.py): override on
    # the module, inherit from the blueprint when unset.
    retry_policy = _to_retry_policy(validated.retry_policy)

    def _merge_module_retry(override: Any) -> RetryPolicy | None:
        """Merge a ModuleRetrySchema against the blueprint-level retry_policy.
        Returns None when the module declares no `retry:` block at all."""
        if override is None:
            return None
        return RetryPolicy(
            max_attempts=(
                override.max_attempts
                if override.max_attempts is not None
                else retry_policy.max_attempts
            ),
            backoff_strategy=(
                override.backoff.strategy
                if override.backoff is not None
                else retry_policy.backoff_strategy
            ),
            backoff_base_seconds=(
                override.backoff.base_seconds
                if override.backoff is not None
                else retry_policy.backoff_base_seconds
            ),
            backoff_max_seconds=(
                override.backoff.max_seconds
                if override.backoff is not None
                else retry_policy.backoff_max_seconds
            ),
            jitter=override.backoff.jitter if override.backoff is not None else retry_policy.jitter,
            on_exhaustion=(
                override.on_exhaustion
                if override.on_exhaustion is not None
                else retry_policy.on_exhaustion
            ),
            transient_errors=(
                tuple(override.transient_errors)
                if override.transient_errors is not None
                else retry_policy.transient_errors
            ),
            non_transient_errors=(
                tuple(override.non_transient_errors)
                if override.non_transient_errors is not None
                else retry_policy.non_transient_errors
            ),
            deadline_seconds=(
                override.deadline_seconds
                if override.deadline_seconds is not None
                else retry_policy.deadline_seconds
            ),
        )

    def _coerce_enabled(raw: Any, module_id: str) -> bool:
        """`enabled:` — resolve ${ctx.*}/${ENV} then coerce to bool."""
        v = resolve_value(raw, ctx_map) if isinstance(raw, str) else raw
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("true", "1", "yes", "on"):
                return True
            if s in ("false", "0", "no", "off"):
                return False
        raise ParseError(
            f"Module {module_id!r}: `enabled` must resolve to a boolean "
            f"(true/false/1/0/yes/no/on/off), got {v!r}"
        )

    # Pass C (2026-08) — each module's `config:` is now a typed per-type
    # pydantic sub-model (`IngressConfigSchema`, `ChannelConfigSchema`, ...),
    # not a freeform dict. `Module.config` (the dataclass field the
    # Compiler/Executor read via `.get("...")`) stays a plain dict — dumping
    # the validated sub-model back to a dict here means every downstream
    # consumer is unaffected; only the PARSE-TIME validation got stricter.
    # `by_alias=True` restores `class_` -> `"class"` etc.; `exclude_none=True`
    # keeps the pre-Pass-C shape where an unset key is simply absent (never
    # an explicit `None` a `.get(k, default)` caller would fail to default).
    def _config_dict(m: Any) -> dict[str, Any]:
        return m.config.model_dump(by_alias=True, exclude_none=True)

    try:
        modules = tuple(
            Module(
                id=m.id,
                type=m.type,
                label=m.label,
                description=m.description,
                tags=tuple(m.tags),
                config=_anchor_paths(resolve_value(_config_dict(m), ctx_map), m.type),
                engine=m.engine,
                on_failure=m.on_failure,
                on_failure_webhook=m.on_failure_webhook,
                retry=_merge_module_retry(m.retry),
                checkpoint=m.checkpoint,
                enabled=_coerce_enabled(m.enabled, m.id),
                spillway=m.spillway,
                depends_on=tuple(m.depends_on),
                attach_to=getattr(m, "attach_to", None),
                ref=getattr(m, "ref", None),
                context_override=resolve_value(getattr(m, "context_override", None), ctx_map),
                materialize=getattr(m, "materialize", None),
                watermark_column=getattr(m, "watermark_column", None),
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
            alias=e.alias,
        )
        for e in validated.edges
    )

    # ── 6. Graph validation ───────────────────────────────────────────────────
    try:
        cycle_nodes = detect_cycles(list(modules), list(edges))
        if cycle_nodes:
            raise ParseError(f"Cycle detected in module graph. Involved modules: {cycle_nodes}")
        validate_spillway_targets(list(modules))
        validate_edge_error_types(list(edges))
        validate_edge_aliases(list(modules), list(edges))
        validate_watermark_keys(list(modules), list(edges))
    except ValueError as exc:
        raise ParseError(str(exc)) from exc

    # ── 7. Assemble final Blueprint AST ───────────────────────────────────────
    # (retry_policy already computed above, before the module loop, so
    # per-module `retry:` overrides could be merged against it.)

    try:
        agent = AgentConfig(
            approval_mode=validated.agent.approval_mode,
            on_pending_patches=validated.agent.on_pending_patches,
            max_patches=validated.agent.max_patches,
            guardrails=GuardrailsConfig(
                forbidden_ops=tuple(validated.agent.guardrails.forbidden_ops),
                allowed_paths=tuple(validated.agent.guardrails.allowed_paths),
                deny_patterns=tuple(validated.agent.guardrails.deny_patterns),
                heal_on_errors=tuple(validated.agent.guardrails.heal_on_errors),
                never_heal_errors=tuple(validated.agent.guardrails.never_heal_errors),
            ),
            prompt_context=resolve_value(validated.agent.prompt_context, ctx_map),
            max_reprompts=validated.agent.max_reprompts,
            confidence_threshold=validated.agent.confidence_threshold,
            on_heal_failure=validated.agent.on_heal_failure,
            allow_defer=validated.agent.allow_defer,
            deep_loop=validated.agent.deep_loop,
            max_heal_attempts_per_hour=validated.agent.max_heal_attempts_per_hour,
            patch_validation=validated.agent.patch_validation,
            sandbox_mode=validated.agent.sandbox_mode,
        )
    except (ValueError, ParseError) as exc:
        raise ParseError(f"agent config resolution failed: {exc}") from exc

    # Tier-0 resolution applies here too (parity with module config /
    # context_override) — ${ENV:-default} / ${ctx.*} in every engine's
    # `engine.<name>:` block and in macros must be substituted, Spark/macros
    # do no var expansion (ISSUE-027). Wrapped so a bad ${ctx.*} surfaces as
    # ParseError, not raw ValueError.
    #
    # Built GENERICALLY off `EngineBlockSchema`'s own fields, one entry per
    # registered engine block — never a hardcoded `validated.engine.spark`
    # read. See `_resolve_engine_block_raw` for why the non-`conf` branch
    # must use `exclude_unset=True` (explicit-vs-default fields), not a
    # plain `model_dump()`.
    try:
        resolved_engine_config: dict[str, dict[str, Any]] = {}
        for _engine_name in type(validated.engine).model_fields:
            _engine_block = getattr(validated.engine, _engine_name)
            _raw = _resolve_engine_block_raw(_engine_block)
            resolved_engine_config[_engine_name] = resolve_value(_raw, ctx_map)
        resolved_macros = resolve_value(dict(validated.macros), ctx_map)
    except (ValueError, ParseError) as exc:
        raise ParseError(f"engine.<name> / macros resolution failed: {exc}") from exc

    return Blueprint(
        aqueduct_version=validated.aqueduct,
        id=validated.id,
        name=validated.name,
        description=validated.description,
        context=ContextRegistry(values=ctx_map),
        modules=modules,
        edges=edges,
        engine_config=resolved_engine_config,
        retry_policy=retry_policy,
        agent=agent,
        # UdfSchema → plain dicts (the compiler/executor consume dicts via .get()).
        # by_alias dumps `class_name` back to `class`; exclude_none keeps the dict
        # shape the executor expects (it applies its own field defaults).
        # `jar`/`path` are anchored to base_dir here — udf_registry entries are
        # a top-level list, not a `config:` block, so `_anchor_paths` (which
        # only ever walks a module's `config:` dict) never reached them
        # despite the "UDF" row already existing in path_keys.py; specs.md
        # documents relative `jar:` paths as anchoring to the Blueprint dir,
        # which was false at HEAD before this fix (a blueprint run from any
        # CWD other than its own directory got "JAR not found").
        udf_registry=tuple(
            _anchor_udf_entry(
                resolve_value(u.model_dump(by_alias=True, exclude_none=True), ctx_map)
            )
            for u in validated.udf_registry
        ),
        macros=resolved_macros,
        # Verbatim — a PEP 508 requirement string is not a Blueprint value
        # expression, so no `${ctx.*}`/@aq.* resolution applies (same
        # treatment as `required_context` below).
        dependencies=tuple(validated.dependencies),
        required_context=tuple(validated.required_context),
        checkpoint=validated.checkpoint,
        warning_suppress=tuple(validated.warnings.suppress),
        # Hook values pass through VERBATIM — ${run.id}/${run.status}/
        # ${blueprint.id} are runtime variables the CLI hook runner
        # interpolates at fire time (resolve_value here would reject them).
        hooks=Hooks(
            on_success=tuple(_hook_entry(h) for h in validated.hooks.on_success),
            on_failure=tuple(_hook_entry(h) for h in validated.hooks.on_failure),
            on_patch_pending=tuple(_hook_entry(h) for h in validated.hooks.on_patch_pending),
            on_healed=tuple(_hook_entry(h) for h in validated.hooks.on_healed),
        ),
        healed_by=tuple(_healed_by_entry(r) for r in validated.healed_by),
        base_dir=str(_bp_dir),
    )
