"""`patch` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    _apply_warnings_from_cfg,
    _env_options,
    _patches_root_from_blueprint,
    _resolve_and_load_env,
    _uncommitted_applied_patches,
    cli,
    style,
)
from aqueduct.cli.render.funnel import emit
from aqueduct.cli.render.tables import Column, render_table


def _patch_index_obs_store(blueprint_path: Path | None = None):
    """Best-effort observability store for patch_index status updates (Phase 53).

    Postgres → the shared DSN. DuckDB → the per-blueprint store when a blueprint
    is known, else the configured default. Returns None on any failure — the
    index update is best-effort and never blocks a local patch command."""
    try:
        from aqueduct.cli import _load_config_with_env
        from aqueduct.stores.read import open_obs_read

        # Auto-discover aqueduct.yml (walk up from CWD) AND load .env — the same
        # resolution every other command uses. `load_config(None)` did neither, so
        # a config/blueprint needing env or @aq.secret() (Postgres DSN, s3
        # endpoint) failed to open → the index update was silently skipped.
        cfg = _load_config_with_env(None, quiet=True)
        bp_id = None
        if blueprint_path is not None:
            from aqueduct.parser.parser import parse as _parse

            bp_id = _parse(str(blueprint_path)).id
        # Backend-aware (Phase 69): postgres → shared store; duckdb → per-blueprint
        # file when known, else the configured/flat default.
        return open_obs_read(cfg, blueprint_id=bp_id)
    except Exception:
        return None


def _row_from_body(st: str, key: str, payload: dict) -> dict:
    """Build a `_list_from_store` row from a full patch body (the shape both
    the index-backed and full-scan paths converge on)."""
    from aqueduct.patch.grammar import PATCH_META_KEY

    meta = payload.get(PATCH_META_KEY) or {}
    return {
        "status": st,
        "file": key.rsplit("/", 1)[-1],  # unique surrogate key (timestamped)
        "patch_id": payload.get("patch_id", ""),
        "rationale": payload.get("rationale"),
        "confidence": payload.get("confidence"),
        "blueprint_id": meta.get("blueprint_id"),
        "run_id": meta.get("run_id"),
        "failed_module": meta.get("failed_module"),
    }


def _list_rows_full_scan(ps, statuses: tuple[str, ...]) -> list[dict]:
    """The pre-Phase-84 path: one full body read per patch via
    ``PatchStore.iter_payloads`` — O(n) in patch count. Kept as the
    fallback when the index is unavailable or its query fails, and as the
    JSON-format path (JSON needs `confidence`/`failed_module`, which live
    only in the body, so there is nothing to save by consulting the index
    first)."""
    rows: list[dict] = []
    for st in statuses:
        for _key, _mtime, payload in ps.iter_payloads(st):
            rows.append(_row_from_body(st, _key, payload))
    return rows


def _list_rows_via_index(ps, obs_store: Any, statuses: tuple[str, ...]) -> list[dict] | None:
    """Fast path for TEXT output: `patch_index` metadata only — zero body
    reads for any patch the index already knows about.

    Returns None (→ caller falls back to `_list_rows_full_scan`) when no
    obs store is available or the query errors; the index is a derived
    cache, `ps` (the patch store) stays the source of truth. Cross-checked
    against `PatchStore.list_keys` (a cheap key listing, no body reads) so
    stale index rows (pointing at a moved/deleted body) are dropped and
    pre-index patches (written before Phase 53, or by a path that skipped
    the index write) still list — read individually, since only THOSE
    bodies are actually missing metadata.

    Table output never renders `confidence`/`failed_module` (body-only
    fields), so those come back None here — correct for this caller,
    which is exactly why this fast path exists only for TEXT format.
    """
    if obs_store is None:
        return None
    from aqueduct.patch import index as _ix

    try:
        with obs_store.connect() as cur:
            index_rows: list[dict] = []
            for st in statuses:
                index_rows.extend(_ix.list_by_status(cur, status=st, limit=10_000))
    except Exception:
        return None

    try:
        store_keys: dict[str, list[str]] = {
            st: sorted((k for k in ps.list_keys(st) if k.endswith(".json")), reverse=True)
            for st in statuses
        }
    except Exception:
        return None

    all_store_keys = {k for keys in store_keys.values() for k in keys}
    indexed = {r["object_key"]: r for r in index_rows if r["object_key"] in all_store_keys}
    missing = all_store_keys - set(indexed)

    rows: list[dict] = []
    for st in statuses:
        for key in store_keys[st]:  # filename-desc ≈ newest-first (ts-prefixed names)
            r = indexed.get(key)
            if r is not None:
                rows.append(
                    {
                        "status": st,
                        "file": key.rsplit("/", 1)[-1],
                        "patch_id": r.get("patch_id", ""),
                        "rationale": r.get("rationale"),
                        "confidence": None,
                        "blueprint_id": r.get("blueprint_id") or None,
                        "run_id": r.get("run_id") or None,
                        "failed_module": None,
                    }
                )
            elif key in missing:
                try:
                    payload = ps.get_json(key)
                except Exception:
                    continue
                rows.append(_row_from_body(st, key, payload))
    return rows


def _list_from_store(ps, filter_status: str, out_format: str, obs_store: Any = None) -> None:
    """Render patches read straight from the patch store (the source of truth).

    TEXT output (the common interactive case) is served from `patch_index`
    when available — O(1) metadata query, no per-patch body read. JSON
    output and the no-index/query-error case fall back to the full
    `PatchStore.iter_payloads` scan (`_list_rows_full_scan`)."""
    statuses = ("pending", "applied", "rejected") if filter_status == "all" else (filter_status,)
    rows: list[dict] | None = None
    if out_format.lower() != "json":
        rows = _list_rows_via_index(ps, obs_store, statuses)
    if rows is None:
        rows = _list_rows_full_scan(ps, statuses)
    if out_format.lower() == "json":
        emit(rows, fmt="json")
        return
    if not rows:
        click.echo(f"No {filter_status} patches found in the patch store ({ps.location_label}).")
        return
    # `file` is the UNIQUE key (`<ts>_<patch_id>.json`) — shown in FULL so it can
    # be copied verbatim into `apply`/`reject` (the embedded slug is the model's
    # non-unique patch_id, so no separate column). `rationale` is the ONE flex
    # column (highest length variance) — it absorbs remaining terminal width
    # and truncates with `…` on a narrow TTY; -v/piping always print it whole.
    from aqueduct.cli.verbosity import resolve_verbosity

    click.echo("")
    render_table(
        [
            Column("file"),
            Column("status"),
            Column("blueprint"),
            Column("rationale", flex=True),
        ],
        [
            [
                r.get("file") or "",
                r["status"],
                r.get("blueprint_id") or "",
                (r.get("rationale") or "").replace("\n", " "),
            ]
            for r in rows
        ],
        verbose=resolve_verbosity() >= 1,
    )
    has_pending = any(r["status"] == "pending" for r in rows)
    if has_pending:
        click.echo("\n  Apply:  aqueduct patch apply <patch_id|file> --blueprint <blueprint.yml>")
        click.echo("  Reject: aqueduct patch reject <patch_id|file> --reason '<reason>'")


# ── patch command group ───────────────────────────────────────────────────────


@cli.group()
def patch() -> None:
    """Manage Blueprint patches."""


@patch.command("preview")
@click.argument("patch_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--blueprint",
    "blueprint_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML the patch will be applied to.",
)
@click.option(
    "--sandbox",
    is_flag=True,
    default=False,
    help="Also run the sandbox gate — replay the patched Blueprint on a sampled DataFrame.",
)
@click.option(
    "--sample",
    "sample_rows",
    type=int,
    default=1000,
    show_default=True,
    help="Per-Ingress row limit during the sandbox gate. 0 = unbounded (full data).",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@click.option(
    "--format",
    "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. `text` (default) renders diff + gate findings. `json` emits a machine-readable report.",
)
@click.option(
    "-s",
    "--set",
    "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Override an aqueduct.yml value for this preview only (repeatable, "
    "in-memory, never persisted). Dotted path — e.g. "
    "--set engine.spark.master_url=spark://h:7077. Same precedence as "
    "`aqueduct run`'s --set: pins the effective session config the "
    "engine-config gate and --sandbox replay measure against.",
)
@_env_options
def patch_preview(
    patch_file: str,
    blueprint_path: str,
    sandbox: bool,
    sample_rows: int,
    config_path: str | None,
    out_format: str,
    set_items: tuple[str, ...],
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Validation pyramid preview for a pending patch.

    Always runs the guardrails gate (schema + post-apply Parser re-check),
    the lineage gate (live lineage impact), and the resolvability gate
    (Gate 5, Phase 88 — PyPI check for every `declare_dependency` op;
    `not_applicable` for a patch that declares none). With `--sandbox`, also
    runs the sandbox gate (replay the patched Blueprint on a per-Ingress
    LIMIT N, Egress modules skipped and listed in the report) and the
    explain gate (post-patch `explain()` regression
    check against the most recent baseline in `observability.explain_snapshot`).
    """
    from pathlib import Path as _Path

    from aqueduct.config import ConfigError, load_config
    from aqueduct.patch.apply import (
        PatchError,
        _check_guardrails,
        _yaml_load,
        apply_patch_to_dict,
        load_patch_spec,
    )
    from aqueduct.patch.explain_gate import run_explain_gate
    from aqueduct.patch.gate_status import GateStatus, sandbox_gate_blocks_preview
    from aqueduct.patch.preview import (
        SandboxGateResult,
        render_unified_diff,
        run_lineage_gate,
        run_sandbox_gate,
    )
    from aqueduct.patch.resolvability_gate import run_resolvability_gate

    bp_raw = _yaml_load(_Path(blueprint_path))
    try:
        spec = load_patch_spec(_Path(patch_file))
    except PatchError as exc:
        click.echo(f"✗ patch schema error: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Config is loaded here, not only under `--sandbox`, because Gate 1's
    # effective-engine-config check needs the `aqueduct.yml` layer to answer
    # whether a config write changes anything — `patch preview` has to run
    # the SAME gate `patch apply` runs, or a reviewer is shown a verdict the
    # apply will not honour.
    cfg = None
    try:
        _resolve_and_load_env(
            env_file,
            _Path(config_path) if config_path else _Path(blueprint_path),
            cli_env=cli_env,
        )
        cfg = load_config(_Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    # ── -s/--set overrides (config-only; the patch itself is not re-routed) ────
    if set_items:
        from aqueduct.overrides import OverrideError, apply_to_model, route_overrides

        try:
            _cfg_set_nested, _ = route_overrides(set_items, allow_blueprint=False)
            cfg = apply_to_model(cfg, _cfg_set_nested)
        except OverrideError as exc:
            click.echo(f"✗ {exc}", err=True)
            sys.exit(exit_codes.CONFIG_ERROR)

    # Guardrails gate — deterministic. Identical enforcement used by
    # `patch apply`; surfaced here so reviewers see violations up front.
    try:
        config_delta_res = _check_guardrails(spec, bp_raw, provenance_map=None, cfg=cfg)
    except PatchError as exc:
        from aqueduct.cli.render.style import error as _style_error

        _style_error(f"guardrails gate blocked: {exc}")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    try:
        bp_after = apply_patch_to_dict(bp_raw, spec)
    except PatchError as exc:
        click.echo(f"✗ patch could not be applied in memory: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    diff = render_unified_diff(bp_raw, bp_after)
    lineage_res = run_lineage_gate(bp_raw, bp_after, spec)
    # Resolvability gate (Gate 5, Phase 88) — unconditional, like the
    # lineage gate: cheap and network-free unless the patch actually
    # declares a dependency (a declare_dependency-free patch short-circuits
    # to `not_applicable` before any PyPI call is made).
    resolvability_res = run_resolvability_gate(spec)

    # No `--sandbox` means the gate was never asked to run — a caller-level
    # fact, not a verdict, and (unlike the old bare `None`) it now reports
    # as an explicit status so `sandbox_gate_permits_auto_apply` fails
    # closed instead of silently permitting a patch nothing replayed.
    sandbox_res = SandboxGateResult(
        status=GateStatus.NOT_REQUESTED,
        detail="sandbox replay was not requested — pass --sandbox to replay this patch",
    )
    explain_res = None
    if sandbox:
        from aqueduct.stores import get_stores

        bundle = get_stores(cfg)
        failed_module = None
        explain_after: dict[str, dict] = {}
        # patch_id is used both for run-tagging and tempfile naming
        sandbox_res = run_sandbox_gate(
            bp_after,
            blueprint_path=_Path(blueprint_path),
            patch_id=spec.patch_id,
            failed_module=failed_module,
            engine=cfg.deployment.engine,
            cfg=cfg,
            sample_rows=int(sample_rows),
            observability_store=bundle.observability,
            explain_capture=explain_after,
            sandbox_master_url=cfg.agent.sandbox_master_url,
            warnings_suppress=cfg.warnings.suppress,
            timezone=cfg.timezone,
            patch_spec=spec,
        )
        # Explain gate — baseline read directly from the observability store.
        try:
            from aqueduct.stores import get_stores as _gs  # noqa
            from aqueduct.surveyor.surveyor import Surveyor

            # Compile to retrieve blueprint_id without full run.
            from aqueduct.parser.parser import parse as _parse
            from aqueduct.compiler.compiler import compile as _compile

            _bp = _parse(blueprint_path)
            _mf = _compile(_bp, blueprint_path=_Path(blueprint_path))
            _surv = Surveyor(
                manifest=_mf,
                store_dir=cfg.store_dir,
                stores=bundle,
                engine=cfg.deployment.engine,
            )
            _baseline = _surv.latest_explain_snapshots(blueprint_id=_mf.blueprint_id)
        except Exception:
            _baseline = {}
        explain_res = run_explain_gate(
            _baseline, explain_after, touched_modules=lineage_res.touched_modules
        )

    if out_format.lower() == "json":
        report = {
            "patch_id": spec.patch_id,
            "blueprint_path": str(blueprint_path),
            "diff": diff,
            "lineage": {
                "status": lineage_res.status,
                "touched_modules": lineage_res.touched_modules,
                "warnings": [w.__dict__ for w in lineage_res.warnings],
                "detail": lineage_res.detail or None,
                "duration_ms": lineage_res.duration_ms,
            },
            "engine_config": {
                "status": config_delta_res.status,
                "detail": config_delta_res.detail,
                "delta": config_delta_res.delta,
                "write_targets": {k: list(v) for k, v in config_delta_res.write_targets.items()},
                # Always present, normally `{}` — non-empty only when this
                # invocation passed `-s/--set engine.<name>.<key>=...`.
                # Emitting the key unconditionally is what lets a consumer
                # tell "measured without pins" apart from "the field does
                # not exist in this version".
                "cli_pinned": {k: list(v) for k, v in config_delta_res.cli_pinned.items()},
            },
            "resolvability": {
                "status": resolvability_res.status,
                "detail": resolvability_res.detail or None,
                "requirements": resolvability_res.requirements,
                "duration_ms": resolvability_res.duration_ms,
            },
        }
        if sandbox_res is not None:
            report["sandbox"] = {
                "status": sandbox_res.status,
                "detail": sandbox_res.detail,
                "sample_rows": sandbox_res.sample_rows,
                "duration_ms": sandbox_res.duration_ms,
                "egress_targets": sandbox_res.egress_targets,
            }
        if explain_res is not None:
            report["explain"] = {
                "status": explain_res.status,
                "detail": explain_res.detail,
                "duration_ms": explain_res.duration_ms,
                "baseline_run_id": explain_res.baseline_run_id,
                "regressions": [r.__dict__ for r in explain_res.regressions],
            }
        emit(report, fmt="json")
        # `sandbox_gate_blocks_preview`, NOT the auto-apply predicate
        # (`patch/gate_status.py` documents why they differ): this exit code
        # answers "did a gate that ran object to this patch". An
        # `unavailable` sandbox result still exits non-zero — the review this
        # command exists to support could not be given a replay — but a gate
        # that was never asked to run (no `--sandbox`, the documented default
        # invocation) has objected to nothing and must not fail the command.
        sys.exit(
            exit_codes.SUCCESS
            if lineage_res.status != "fail"
            and resolvability_res.status != GateStatus.FAIL
            and not sandbox_gate_blocks_preview(sandbox_res)
            else exit_codes.DATA_OR_RUNTIME
        )

    # Text report — headers dim (structural), gate status lines use the
    # shared ✓/✗/⚠/· vocabulary (style.py) so `patch preview`'s gate pyramid
    # reads consistently with the rest of the CLI's output.
    from aqueduct.cli.render.style import dim as _dim

    def _gate_status_line(status: str) -> None:
        from aqueduct.cli.render.style import error as _e
        from aqueduct.cli.render.style import info as _i
        from aqueduct.cli.render.style import success as _s
        from aqueduct.cli.render.style import warn as _w

        label = f"status: {status}"
        if status == GateStatus.PASS:
            _s(f"  {label}")
        elif status == GateStatus.FAIL:
            _e(f"  {label}", err=False)
        elif status == GateStatus.WARN:
            _w(f"  {label}", err=False)
        elif status == GateStatus.UNAVAILABLE:
            # A check was owed and did not happen — nothing is known to be
            # wrong with the patch, but nothing is known to be right
            # either. `warn` (whole-line ⚠), not `info`: it blocks
            # auto-apply, so it must not read like the dim, purely
            # informational `not_applicable` line one gate above it.
            _w(f"  {label}", err=False)
        elif status == GateStatus.NOT_APPLICABLE:
            # Informational only — nothing was owed, nothing blocks.
            _i(f"  {label}")
        elif status == GateStatus.NOT_REQUESTED:
            # Sandbox-only: the gate was deliberately never asked to run
            # (`--sandbox` omitted), distinct from `not_applicable` (asked,
            # nothing to check). Informational text, but it still fails
            # closed on auto-apply — see `sandbox_gate_permits_auto_apply`.
            _i("  sandbox: not requested")
        else:
            _i(f"  {label}")

    click.echo(f"Patch {spec.patch_id}")
    click.echo(f"  rationale: {spec.rationale}")
    if spec.confidence is not None:
        click.echo(f"  confidence: {spec.confidence:.0%}")
    click.echo()
    click.echo(_dim("── Blueprint diff ────────────────────────────────────────────"))
    click.echo(diff if diff.strip() else "  (no textual change)")

    click.echo()
    click.echo(_dim("── Lineage gate (live sqlglot) ───────────────────────────────"))
    _gate_status_line(lineage_res.status)
    click.echo(f"  touched modules: {', '.join(lineage_res.touched_modules) or '(none)'}")
    if lineage_res.status == "not_applicable":
        click.echo(f"  · {lineage_res.detail}")
    elif lineage_res.warnings:
        for w in lineage_res.warnings:
            click.echo(f"  ⚠ {w.detail}")
    else:
        click.echo("  no downstream column-consumption regressions detected")
    click.echo(f"  duration:        {lineage_res.duration_ms} ms")

    click.echo()
    click.echo(_dim("── Engine-config gate (effective session config) ─────────────"))
    _gate_status_line(config_delta_res.status)
    click.echo(f"  detail: {config_delta_res.detail}")
    for _eng, _keys in sorted(config_delta_res.delta.items()):
        for _key, _ba in sorted(_keys.items()):
            click.echo(f"    {_eng}.{_key}: {_ba['before']!r} → {_ba['after']!r}")
    for _eng, _pinned in sorted(config_delta_res.cli_pinned.items()):
        for _key in _pinned:
            click.echo(f"    {_eng}.{_key}: pinned by --set — this patch cannot move it")

    click.echo()
    click.echo(_dim("── Resolvability gate (declared dependencies) ─────────────────"))
    _gate_status_line(resolvability_res.status)
    click.echo(f"  requirements: {', '.join(resolvability_res.requirements) or '(none)'}")
    if resolvability_res.status == GateStatus.NOT_APPLICABLE:
        click.echo(f"  · {resolvability_res.detail}")
    elif resolvability_res.detail:
        click.echo(f"  detail: {resolvability_res.detail}")
    if resolvability_res.status in (GateStatus.WARN, GateStatus.UNAVAILABLE):
        click.echo(
            "  effect:      this patch is NOT eligible for automatic "
            "application — a human must decide"
        )
    click.echo(f"  duration:    {resolvability_res.duration_ms} ms")

    if sandbox_res is not None:
        click.echo()
        click.echo(_dim("── Sandbox gate (replay) ─────────────────────────────────────"))
        _gate_status_line(sandbox_res.status)
        click.echo(f"  detail:      {sandbox_res.detail}")
        if sandbox_res.status in (GateStatus.UNAVAILABLE, GateStatus.NOT_REQUESTED):
            # State the consequence, not only the fact: this is the status
            # that stops the patch, and a reviewer reading a gate block
            # should not have to infer that from the word.
            click.echo(
                "  effect:      this patch is NOT eligible for automatic "
                "application — a human must decide"
            )
        if sandbox_res.sample_rows is not None:
            click.echo(f"  sample_rows: {sandbox_res.sample_rows}")
        click.echo(f"  duration:    {sandbox_res.duration_ms} ms")
        if sandbox_res.egress_targets:
            click.echo("  Egress operations (sandbox skipped):")
            for t in sandbox_res.egress_targets:
                click.echo(
                    f"    {t.get('id')}  → {t.get('format')}  {t.get('path')}"
                    + (f"  (mode={t.get('mode')})" if t.get("mode") else "")
                )

    if explain_res is not None:
        click.echo()
        click.echo(_dim("── Explain gate (plan regression) ────────────────────────────"))
        _gate_status_line(explain_res.status)
        click.echo(f"  detail:   {explain_res.detail}")
        if explain_res.baseline_run_id:
            click.echo(f"  baseline: run {explain_res.baseline_run_id}")
        if explain_res.regressions:
            for r in explain_res.regressions:
                click.echo(f"  ⚠ {r.detail}")
        click.echo(f"  duration: {explain_res.duration_ms} ms")

    exit_code = exit_codes.SUCCESS
    if lineage_res.status == GateStatus.FAIL:
        exit_code = exit_codes.DATA_OR_RUNTIME
    # WARN is a defer, not a rejection — it must NOT make preview exit
    # non-zero, only FAIL does (mirrors the lineage gate's line above).
    if resolvability_res.status == GateStatus.FAIL:
        exit_code = exit_codes.DATA_OR_RUNTIME
    # The same predicate as the `--format json` branch above, so text and
    # json modes cannot disagree about whether this patch is reviewable as
    # validated.
    if sandbox_gate_blocks_preview(sandbox_res):
        exit_code = exit_codes.DATA_OR_RUNTIME
    sys.exit(exit_code)


POLICY_NARROWING_NOTE = (
    "Operator extension and narrowing of this policy are not yet "
    "implemented — this is the whole policy, not a customized view."
)


def _policy_entry_dict(entry) -> dict:
    """One allow entry, JSON-serializable — shared by text and json rendering."""
    from aqueduct.executor.engine_config_allowlist import EnumConstraint, RangeConstraint

    out: dict = {"pattern": entry.pattern, "type": entry.value_type}
    if isinstance(entry.constraint, EnumConstraint):
        out["enum"] = list(entry.constraint.values)
    elif isinstance(entry.constraint, RangeConstraint):
        out["range"] = [entry.constraint.minimum, entry.constraint.maximum]
    return out


def _policy_deny_dict(entry) -> dict:
    """One deny entry, JSON-serializable."""
    out: dict = {"pattern": entry.pattern, "reason": entry.reason}
    if entry.deny_values is not None:
        out["deny_values"] = list(entry.deny_values)
    return out


def _policy_report(engine: str, allowlist) -> dict:
    """One engine's effective ``set_engine_config`` policy — the SAME
    ``EngineConfigAllowlist`` Gate 1 evaluates a candidate write against
    (``aqueduct.executor.engine_config_allowlist.load_allowlist`` /
    ``evaluate_set_engine_config``), reshaped for display. No matching
    logic is reimplemented here."""
    return {
        "engine": engine,
        "shape": "free_form_conf_bag" if allowlist.is_free_form else "typed_fields",
        "allow": [_policy_entry_dict(e) for e in allowlist.entries],
        "deny": [_policy_deny_dict(d) for d in allowlist.deny_entries],
    }


@patch.command("policy")
@click.option(
    "--engine",
    "engine_name",
    default=None,
    help="Show one engine's policy only (default: every registered engine).",
)
@click.option(
    "--format",
    "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. `text` (default) renders the allow/deny tables. `json` emits structured data only.",
)
def patch_policy(engine_name: str | None, out_format: str) -> None:
    """Print the effective `set_engine_config` healing policy, per engine.

    Answers "what may the healing agent write on this engine?" — the allowed
    `engine.<name>` config keys (with value type and any enum/range
    constraint) and the denied key families (with their `reason`), read
    straight from each registered engine's core
    `engine_config_allowlist.yml` — the same file and evaluation logic Gate
    1 enforces against a `set_engine_config` patch op (see
    `docs/specs.md` §8.5 "Permission model" /
    `aqueduct/executor/engine_config_allowlist.py`). Operator extension and
    narrowing of this policy are not yet implemented, so what this command
    prints is the complete policy — not a preview of a configurable subset.
    """
    from aqueduct.executor.engine_config_allowlist import (
        DECLARATION_FILENAME,
        discover_registered_engines,
        load_allowlist,
    )

    registered = discover_registered_engines()
    if not registered:
        style.warn("no registered engines found")
        sys.exit(exit_codes.CONFIG_ERROR)

    if engine_name is not None and engine_name not in registered:
        style.error(
            f"engine {engine_name!r} is not registered. Registered engines: "
            f"{sorted(registered)}"
        )
        sys.exit(exit_codes.USAGE_ERROR)

    targets = [engine_name] if engine_name is not None else sorted(registered)
    reports = []
    for eng in targets:
        allowlist_path = registered[eng] / DECLARATION_FILENAME
        allowlist = load_allowlist(allowlist_path, eng)
        reports.append(_policy_report(eng, allowlist))

    if out_format.lower() == "json":
        emit({"narrowing": POLICY_NARROWING_NOTE, "engines": reports}, fmt="json")
        return

    for r in reports:
        shape_label = "free-form conf bag" if r["shape"] == "free_form_conf_bag" else "typed fields"
        click.echo(f"Engine: {r['engine']}  ({shape_label})")

        click.echo("  Allowed keys:")
        if not r["allow"]:
            click.echo("    (none)")
        for e in r["allow"]:
            constraint = ""
            if "enum" in e:
                constraint = f"  enum={e['enum']}"
            elif "range" in e:
                constraint = f"  range={e['range']}"
            click.echo(f"    {e['pattern']:<48} type={e['type']}{constraint}")

        click.echo("  Denied families:")
        if not r["deny"]:
            click.echo("    (none)")
        for d in r["deny"]:
            scope = f"  (values={d['deny_values']})" if "deny_values" in d else ""
            click.echo(f"    {d['pattern']:<48} {d['reason']}{scope}")
        click.echo()

    style.info(POLICY_NARROWING_NOTE)


def _patch_store_from(patches_root, config_path, env_file, cli_env, set_items=()):
    """Build the configured PatchStore (local OR object backend), or None.

    Resolves aqueduct.yml (CWD walk-up / --config) + .env, so the body lifecycle
    (apply/reject move) acts on the same store `patch list` shows.

    *set_items* (``-s/--set``, config-only) overlays on top — e.g.
    `--set stores.blob.backend=s3` — so a caller that resolved its engine
    config under an override finds the SAME patch store. Never touches
    anything but this internally-loaded ``cfg``; a caller with its own
    ``cfg`` (e.g. `patch revert`'s prior-values equality check) is
    unaffected by this pin.
    """
    from pathlib import Path

    try:
        from aqueduct.cli import _load_config_with_env
        from aqueduct.stores.object_store import make_patch_store

        cfg = _load_config_with_env(
            Path(config_path) if config_path else None,
            env_file=env_file,
            cli_env=cli_env or (),
        )
        if set_items:
            from aqueduct.overrides import apply_to_model as _apply_to_model
            from aqueduct.overrides import route_overrides

            _cfg_set_nested, _ = route_overrides(set_items, allow_blueprint=False)
            cfg = _apply_to_model(cfg, _cfg_set_nested)
        return make_patch_store(cfg.stores.blob.backend, cfg.stores.blob.path, Path(patches_root))
    except Exception:
        return None


def _resolve_pending_key(ps, ref: str):
    """Resolve PATCH_REF to a unique pending store key.

    A full filename (``00003_…_slug.json``) wins outright — that is the unique
    surrogate key. A bare ``patch_id`` is accepted only when it matches exactly
    one pending body; if several share the id (same fix re-staged across runs)
    the caller must use the full filename. Returns ``(key, [])`` on success,
    ``(None, [ambiguous keys])`` for >1 match, ``(None, [])`` for none."""
    from pathlib import Path

    by_id: list[str] = []
    for key, _mt, payload in ps.iter_payloads("pending"):
        name = key.rsplit("/", 1)[-1]
        if name == ref or name == f"{ref}.json" or key == ref or Path(ref).name == name:
            return key, []
        if payload.get("patch_id") == ref:
            by_id.append(key)
    if len(by_id) == 1:
        return by_id[0], []
    return None, by_id


def _fetch_pending_to_temp(ps, key: str):
    """Materialise a pending body to a scratch temp file (the operations source
    `apply_patch_file` reads; the store still owns the canonical body)."""
    import tempfile
    from pathlib import Path

    fd, tmp = tempfile.mkstemp(suffix="_" + key.rsplit("/", 1)[-1])
    import os as _os

    _os.close(fd)
    Path(tmp).write_text(ps.get_text(key), encoding="utf-8")
    return Path(tmp)


def _resolve_patch_source(ref, patches_root, config_path, env_file, cli_env, *, need_local: bool):
    """PATCH_REF → ``(ops_path, patch_store, pending_key)``.

    External local file → ``(that path, store, None)`` (applied straight to the
    store, no pending to move). Otherwise a unique pending key in the configured
    store → ``(temp body | None, store, key)``. Exits with a clear message on
    ambiguous / not-found."""
    from pathlib import Path

    ps = _patch_store_from(patches_root, config_path, env_file, cli_env)
    p = Path(ref)
    if p.exists() and p.parent.name != "pending":
        return p, ps, None  # genuinely external file (CI download) → applied copy
    if p.exists() and p.parent.name == "pending":
        ref = p.name  # a pending file path → resolve to its store key below (move)
    if ps is None:
        click.echo(
            f"✗ {ref!r} is not a local file and no patch store could be resolved "
            f"(need aqueduct.yml / --config + env)",
            err=True,
        )
        sys.exit(exit_codes.USAGE_ERROR)
    key = _require_pending_key(ps, ref)
    ops_path = _fetch_pending_to_temp(ps, key) if need_local else None
    return ops_path, ps, key


def _require_pending_key(ps, ref: str) -> str:
    """Resolve PATCH_REF to a unique pending key or exit with a clear message
    (ambiguous → list the full filenames; none → not-found)."""
    key, ambiguous = _resolve_pending_key(ps, ref)
    if key is None:
        if ambiguous:
            click.echo(
                f"✗ {len(ambiguous)} pending patches share id {ref!r} — re-run with the "
                f"full filename:",
                err=True,
            )
            for k in ambiguous:
                click.echo(f"    {k.rsplit('/', 1)[-1]}", err=True)
        else:
            click.echo(
                f"✗ no pending patch matching {ref!r} in the store ({ps.location_label})", err=True
            )
        sys.exit(exit_codes.USAGE_ERROR)
    return key


@patch.command("apply")
@click.argument("patch_ref")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to patch",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml — resolves the patch store when PATCH_REF is a patch_id.",
)
@_env_options
def patch_apply(
    patch_ref: str,
    blueprint: str,
    patches_dir: str | None,
    config_path: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Validate and apply a patch to a Blueprint YAML.

    PATCH_REF is either a local PatchSpec JSON file, or a bare ``patch_id`` —
    in which case the body is fetched from the configured patch store (local or
    object store) automatically (no manual ``patch pull`` needed). Backs up the
    original Blueprint, applies all operations atomically, verifies the result
    parses cleanly, then archives the patch.
    """
    from pathlib import Path

    from aqueduct.config import ConfigError, load_config
    from aqueduct.patch.apply import PatchError, apply_patch_file

    blueprint_path = Path(blueprint)
    patches_root = (
        Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)
    )

    ops_path, ps, pending_key = _resolve_patch_source(
        patch_ref,
        patches_root,
        config_path,
        env_file,
        cli_env,
        need_local=True,
    )
    # `--config` is honoured explicitly: Gate 1's effective-engine-config
    # check compares against `aqueduct.yml`'s `engine.<name>` layer, so
    # letting `apply_patch_file` fall back to ambient discovery would answer
    # against a different config than the one the user named.
    try:
        _cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"\u2717 config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)
    try:
        result = apply_patch_file(
            blueprint_path=blueprint_path,
            patch_path=ops_path,
            patches_dir=patches_root,
            obs_store=_patch_index_obs_store(blueprint_path),
            patch_store=ps,
            pending_key=pending_key,
            cfg=_cfg,
        )
    except PatchError as exc:
        click.echo(f"✗ patch failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    click.echo(f"✓ patch applied  id={result.patch_id}")
    click.echo(f"  blueprint  → {result.blueprint_path}")
    click.echo(f"  archived   → {result.archive_path}")
    click.echo(f"  operations   {result.operations_applied} applied")
    click.echo(f"  commit with: aqueduct patch commit --blueprint {blueprint}")


def _applied_patch_operations(
    patches_root, config_path, env_file, cli_env, patch_id: str, set_items=()
) -> list | None:
    """Operations of the APPLIED patch body carrying *patch_id*, or None.

    The body is what proves a patch wrote nothing but engine config
    (``aqueduct/patch/revert.py::_require_config_only``); the ``healed_by``
    record alone cannot tell a config-only patch from a mixed one. None means
    "no such applied body", which the planner turns into its own refusal.

    *set_items* is forwarded to ``_patch_store_from`` only (which patch
    store to read the applied body from) — it never touches the caller's
    own ``cfg``, so `patch revert`'s prior-values equality check stays
    unpinned regardless of what this resolves.
    """
    from aqueduct.patch.revert import RevertError

    ps = _patch_store_from(patches_root, config_path, env_file, cli_env, set_items=set_items)
    if ps is None:
        return None
    matches = [
        payload
        for _key, _mtime, payload in ps.iter_payloads("applied")
        if str(payload.get("patch_id") or "") == patch_id
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise RevertError(
            f"cannot revert patch {patch_id!r}: {len(matches)} applied patch "
            f"bodies in {ps.location_label} carry that id, so which operations "
            "were applied is ambiguous. Aqueduct will not guess."
        )
    return list(matches[0].get("operations") or [])


@patch.command("revert")
@click.argument("patch_id")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML carrying the healed_by record to revert",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml — the engine.<name> layer the effective config resolves against.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Plan and verify the revert, print what it would change, write nothing.",
)
@click.option(
    "--format",
    "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. `text` (default) renders the restore list. `json` emits the plan.",
)
@click.option(
    "-s",
    "--set",
    "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Override an aqueduct.yml value for this invocation only (repeatable, "
    "in-memory, never persisted) — e.g. --set stores.blob.backend=s3 to read "
    "the applied patch body from a different patch store. Does NOT affect the "
    "prior-values safety check: that check always compares against the "
    "UNPINNED effective config, exactly as `aqueduct run` (no --set) would "
    "resolve it, so a --set can never make a legitimate revert abort nor let "
    "a genuinely diverged one falsely pass.",
)
@_env_options
def patch_revert(
    patch_id: str,
    blueprint: str,
    patches_dir: str | None,
    config_path: str | None,
    dry_run: bool,
    out_format: str,
    set_items: tuple[str, ...],
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Undo an applied heal patch's engine-config writes.

    Restores every `engine.<name>` key the patch wrote to the value the
    `healed_by:` record captured before it was applied, and stamps that
    record `reverted_at:` — the record is kept, so the history of the heal
    survives its undo.

    Only engine-config writes can be reverted: they are the only change for
    which Aqueduct records a prior value. A patch that also touched a module,
    an edge or a SQL body is REFUSED by name rather than half-undone; so is
    one whose keys a later patch overwrote, or whose values have been edited
    since. For those, `aqueduct patch rollback --to <patch_id>` restores the
    whole file from git history.
    """
    from datetime import UTC, datetime
    from pathlib import Path as _Path

    from aqueduct.config import ConfigError, load_config
    from aqueduct.parser.parser import ParseError, parse
    from aqueduct.patch.apply import _yaml_dump, _yaml_load
    from aqueduct.patch.revert import RevertError, apply_revert, plan_revert

    blueprint_path = _Path(blueprint)
    patches_root = (
        _Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)
    )

    try:
        _resolve_and_load_env(
            env_file,
            _Path(config_path) if config_path else blueprint_path,
            cli_env=cli_env,
        )
        # `cfg` here is what feeds `plan_revert`'s prior-values equality
        # check (`resolve_effective_engine_configs(cfg, ...)`), and MUST
        # stay the UNPINNED resolution — see the `-s/--set` help text above
        # and `aqueduct/patch/revert.py`'s module docstring. `--set` still
        # reaches this command (via `_applied_patch_operations`'s patch-store
        # resolution below), it just never touches this `cfg`.
        cfg = load_config(_Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        style.error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    bp_raw = _yaml_load(blueprint_path)
    try:
        operations = _applied_patch_operations(
            patches_root, config_path, env_file, cli_env, patch_id, set_items=set_items
        )
        plan = plan_revert(cfg=cfg, blueprint=bp_raw, patch_id=patch_id, operations=operations)
    except RevertError as exc:
        style.error(str(exc))
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    if dry_run:
        if out_format.lower() == "json":
            emit({**plan.to_dict(), "dry_run": True}, fmt="json")
            return
        click.echo(f"Revert plan for patch {plan.patch_id}  (nothing written)")
        for r in plan.restores:
            click.echo(f"  {r.action:<6} {r.engine}  {r.key}: {r.current!r} → {r.target!r}")
        return

    reverted_at = datetime.now(tz=UTC).isoformat()
    reverted = apply_revert(bp_raw, plan, reverted_at=reverted_at)

    # Same order as `patch apply`: verify the document parses BEFORE it
    # replaces the live file, then back the original up, then swap.
    tmp_verify = blueprint_path.with_suffix(".revert_verify.tmp.yml")
    try:
        _yaml_dump(reverted, tmp_verify)
        parse(str(tmp_verify))
    except ParseError as exc:
        tmp_verify.unlink(missing_ok=True)
        style.error(f"reverted Blueprint does not parse — nothing written:\n{exc}")
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    except Exception as exc:
        tmp_verify.unlink(missing_ok=True)
        style.error(f"unexpected error verifying the reverted Blueprint: {exc}")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    backup_dir = patches_root / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_path = backup_dir / f"revert_{plan.patch_id}_{ts}_{blueprint_path.name}"
    import shutil

    shutil.copy2(blueprint_path, backup_path)
    os.replace(tmp_verify, blueprint_path)

    if out_format.lower() == "json":
        emit(
            {
                **plan.to_dict(),
                "reverted_at": reverted_at,
                "blueprint_path": str(blueprint_path),
                "backup_path": str(backup_path),
            },
            fmt="json",
        )
        return

    style.success(f"patch reverted  id={plan.patch_id}")
    for r in plan.restores:
        click.echo(f"  {r.action:<6} {r.engine}  {r.key}: {r.current!r} → {r.target!r}")
    click.echo(f"  blueprint  → {blueprint_path}")
    click.echo(f"  backup     → {backup_path}")
    style.info(f"  healed_by record kept, stamped reverted_at: {reverted_at}")


@patch.command("import")
@click.argument("patch_ref")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to patch",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
@click.option(
    "--no-commit",
    is_flag=True,
    default=False,
    help="Apply only, skip the git commit (leave the change staged for review).",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml — resolves the patch store when PATCH_REF is a remote patch_id.",
)
@_env_options
def patch_import(
    patch_ref: str,
    blueprint: str,
    patches_dir: str | None,
    no_commit: bool,
    config_path: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Apply a received patch and commit it — the CI entry point (Phase 54).

    PATCH_REF is a local patch JSON file (a bare PatchSpec or a CI webhook
    envelope), or a bare ``patch_id`` fetched from the configured store.
    `approval_mode: ci` reference flow: a cluster run heals, stages the patch,
    and fires the on_patch_pending webhook; a CI runner obtains the patch body
    and calls this to apply + commit it on a fresh checkout, then opens a PR
    (see docs/templates/ci-heal-workflow.yml). Equivalent to
    `patch apply` + `patch commit` in one atomic step.
    """
    import subprocess
    import tempfile
    from pathlib import Path

    from aqueduct.config import ConfigError, load_config
    from aqueduct.patch.apply import PatchError, apply_patch_file
    from aqueduct.patch.ci import build_commit_message, validate_ci_payload

    blueprint_path = Path(blueprint)
    patches_root = (
        Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)
    )

    # PATCH_REF may be a local file (CI download) or a pending patch_id/filename
    # in the configured store. Resolve to an operations source + the store/key so
    # the body lifecycle (move pending → applied) runs in the store.
    _ops_path, _ps, _pending_key = _resolve_patch_source(
        patch_ref,
        patches_root,
        config_path,
        env_file,
        cli_env,
        need_local=True,
    )
    patch_file = str(_ops_path)

    # Pre-flight: if we are going to commit, fail BEFORE mutating the Blueprint
    # when we are not inside a git work tree (so a non-repo checkout doesn't end
    # up with an applied-but-uncommittable change).
    if not no_commit:
        _check = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            cwd=blueprint_path.parent or None,
        )
        if _check.returncode != 0 or _check.stdout.strip() != "true":
            click.echo(
                "✗ not inside a git work tree — `patch import` commits the change. "
                "Run inside the repo, or pass --no-commit to stage only.",
                err=True,
            )
            sys.exit(exit_codes.DATA_OR_RUNTIME)

    # The input may be a bare PatchSpec OR a CI webhook envelope that wraps the
    # body under a `patch` key (patch + `_aq_meta` + envelope fields). When it is
    # an envelope, validate the envelope schema and unwrap the body to a tempfile.
    _apply_path = Path(patch_file)
    _tmp_unwrapped: Path | None = None
    try:
        _raw = json.loads(Path(patch_file).read_text(encoding="utf-8"))
    except Exception:
        _raw = None
    if isinstance(_raw, dict) and isinstance(_raw.get("patch"), dict):
        violations = validate_ci_payload(_raw)
        if violations:
            click.echo("✗ invalid CI webhook payload:\n  - " + "\n  - ".join(violations), err=True)
            sys.exit(exit_codes.DATA_OR_RUNTIME)
        fd, _tmp = tempfile.mkstemp(suffix=".json", prefix="aq_ci_patch_")
        import os as _os

        with _os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(_raw["patch"], fh)
        _tmp_unwrapped = Path(_tmp)
        _apply_path = _tmp_unwrapped

    try:
        _cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"\u2717 config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)
    try:
        result = apply_patch_file(
            blueprint_path=blueprint_path,
            patch_path=_apply_path,
            patches_dir=patches_root,
            obs_store=_patch_index_obs_store(blueprint_path),
            patch_store=_ps,
            pending_key=_pending_key,
            cfg=_cfg,
        )
    except PatchError as exc:
        click.echo(f"✗ patch failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    finally:
        if _tmp_unwrapped is not None and _tmp_unwrapped.exists():
            _tmp_unwrapped.unlink()

    click.echo(f"✓ patch imported  id={result.patch_id}")
    click.echo(f"  blueprint  → {result.blueprint_path}")
    click.echo(f"  operations   {result.operations_applied} applied")

    if no_commit:
        click.echo(
            "  (--no-commit) staged only — commit with: "
            f"aqueduct patch commit --blueprint {blueprint}"
        )
        return

    # Resolve blueprint_id for the structured commit message.
    try:
        from aqueduct.parser.parser import parse as _parse

        blueprint_id = _parse(blueprint).id
    except Exception:
        blueprint_id = blueprint_path.stem

    # The applied body is archived under patches/applied/ — read it back for the
    # commit trailer (rationale, operations, run_id).
    try:
        body = json.loads(result.archive_path.read_text(encoding="utf-8"))
    except Exception:
        body = {"patch_id": result.patch_id}
    commit_msg = build_commit_message(blueprint_id, [body])

    add = subprocess.run(
        ["git", "add", blueprint_path.name],
        capture_output=True,
        text=True,
        cwd=blueprint_path.parent or None,
    )
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True,
        text=True,
        cwd=blueprint_path.parent or None,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    short_hash = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        cwd=blueprint_path.parent or None,
    ).stdout.strip()
    click.echo(f"  committed  [{short_hash}]  {blueprint_id}")


@patch.command("reject")
@click.argument("patch_ref")
@click.option("--reason", required=True, help="Rejection reason (recorded in patch file)")
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: derived from patch file path or CWD/patches)",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml — resolves the patch store when PATCH_REF is a remote patch_id.",
)
@_env_options
def patch_reject(
    patch_ref: str,
    reason: str,
    patches_dir: str | None,
    config_path: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Reject a pending patch and record the reason.

    PATCH_REF is a bare ``patch_id`` (when unique) or the full pending filename
    (``00001_…_slug.json``) when several share an id. The body is moved
    ``pending/`` → ``rejected/`` **in the configured store** (local or object
    backend) with a rejection_reason annotation, and the index status/object_key
    is updated.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, reject_patch

    p = Path(patch_ref)
    if p.parent.name == "pending":
        patches_root = p.parent.parent  # derive from the given pending path
    elif patches_dir:
        patches_root = Path(patches_dir)
    else:
        patches_root = _patches_root_from_blueprint(Path.cwd() / "_sentinel")

    ps = _patch_store_from(patches_root, config_path, env_file, cli_env)
    pending_key, ambiguous = _resolve_pending_key(ps, patch_ref) if ps is not None else (None, [])
    if ambiguous:
        click.echo(
            f"✗ {len(ambiguous)} pending patches share id {patch_ref!r} — re-run with the "
            f"full filename:",
            err=True,
        )
        for k in ambiguous:
            click.echo(f"    {k.rsplit('/', 1)[-1]}", err=True)
        sys.exit(exit_codes.USAGE_ERROR)

    if pending_key:
        try:
            patch_id = (
                json.loads(ps.get_text(pending_key)).get("patch_id") or Path(pending_key).stem
            )
        except Exception:
            patch_id = Path(pending_key).stem
    else:
        patch_id = p.stem  # not in store → legacy reject_patch raises the not-found error

    try:
        rejected_path = reject_patch(
            patch_id=patch_id,
            reason=reason,
            patches_dir=patches_root,
            obs_store=_patch_index_obs_store(),
            patch_store=ps if pending_key else None,
            pending_key=pending_key,
        )
    except PatchError as exc:
        click.echo(f"✗ reject failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    click.echo(f"✓ patch rejected  id={patch_id}")
    click.echo(f"  archived → {rejected_path}")
    click.echo(f"  reason: {reason}")


@patch.command("pull")
@click.argument("patch_id")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint the patch belongs to (locates the index + patches dir)",
)
@click.option(
    "--out",
    default=None,
    type=click.Path(file_okay=False),
    help="Output directory (default: <blueprint-dir>/patches/pending)",
)
def patch_pull(patch_id: str, blueprint: str, out: str | None) -> None:
    """Fetch a patch body from the object store into a local checkout for review.

    Profile C — the pipeline heals on a cluster and stages the patch to an
    object store (s3/gcs/adls); this pulls the body down so you can `git diff`
    and apply it locally. With a local object store this just copies the file.
    """
    from pathlib import Path

    from aqueduct.cli import _load_config_with_env
    from aqueduct.patch import index as _ix
    from aqueduct.stores.object_store import make_patch_store

    blueprint_path = Path(blueprint)
    patches_root = _patches_root_from_blueprint(blueprint_path)
    # Auto-discover aqueduct.yml (CWD walk-up) + load .env so a remote store
    # backend resolves (was load_config(None) — no discovery, no env).
    cfg = _load_config_with_env(None, quiet=True)

    obs = _patch_index_obs_store(blueprint_path)
    if obs is None:
        click.echo("✗ no observability store found — cannot resolve the patch index", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    try:
        with obs.connect() as cur:
            row = _ix.get(cur, patch_id)
    except Exception as exc:
        click.echo(f"✗ index query failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    if row is None:
        click.echo(
            f"✗ patch {patch_id!r} not found in the index — `aqueduct patch list --blueprint <bp>` shows known patches",
            err=True,
        )
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    ps = make_patch_store(cfg.stores.blob.backend, cfg.stores.blob.path, patches_root)
    try:
        body = ps.get_text(str(row["object_key"]))
    except Exception as exc:
        click.echo(f"✗ could not read patch body at {row['object_key']!r}: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    out_dir = Path(out) if out else patches_root / "pending"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{patch_id}.json"
    out_path.write_text(body, encoding="utf-8")

    click.echo(f"✓ patch pulled  id={patch_id}  status={row['status']}")
    click.echo(f"  → {out_path}")
    click.echo(
        f"  review: git diff  •  apply: aqueduct patch apply {out_path} --blueprint {blueprint}"
    )


@patch.command("commit")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to commit",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_commit(blueprint: str, patches_dir: str | None) -> None:
    """Commit applied patches to git with a structured commit message.

    Finds applied patches newer than the last git commit for this Blueprint,
    then runs: git add <blueprint> && git commit.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = (
        Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)
    )

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)
    if not uncommitted:
        click.echo("Nothing to commit — no applied patches since last git commit.")
        return

    # Parse blueprint_id
    try:
        from aqueduct.parser.parser import parse as _parse

        bp = _parse(blueprint)
        blueprint_id = bp.id
    except Exception:
        blueprint_id = blueprint_path.stem

    # Build commit message — label each patch line by its filename stem so
    # `aqueduct log`/`rollback` can match either the patch_id or the file name.
    from aqueduct.patch.ci import build_commit_message

    patch_bodies: list[dict] = []
    n = len(uncommitted)
    for p in uncommitted:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        patch_bodies.append({**data, "patch_id": p.stem})

    commit_msg = build_commit_message(blueprint_id, patch_bodies)

    add = subprocess.run(
        ["git", "add", blueprint_path.name], capture_output=True, cwd=blueprint_path.parent or None
    )
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.decode().strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True,
        text=True,
        cwd=blueprint_path.parent or None,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    short_hash = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        cwd=blueprint_path.parent or None,
    ).stdout.strip()

    click.echo(f"✓ committed {n} patch(es)  [{short_hash}]  {blueprint_id}")
    for p in uncommitted:
        click.echo(f"  {p.name}")


@patch.command("discard")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to restore from git HEAD",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_discard(blueprint: str, patches_dir: str | None) -> None:
    """Discard applied patches — restore Blueprint to last git commit.

    Runs: git checkout HEAD -- <blueprint>
    Moves uncommitted applied patches back to patches/pending/.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = (
        Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)
    )

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)

    restore = subprocess.run(
        ["git", "checkout", "HEAD", "--", blueprint_path.name],
        capture_output=True,
        text=True,
        cwd=blueprint_path.parent or None,
    )
    if restore.returncode != 0:
        click.echo(f"✗ git checkout failed: {restore.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    click.echo(f"✓ blueprint restored to HEAD: {blueprint_path}")

    pending_dir = patches_root / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    moved = 0
    failed: list[str] = []
    for patch_file in uncommitted:
        dest = pending_dir / patch_file.name
        try:
            patch_file.rename(dest)
            moved += 1
        except OSError as exc:
            # Cross-device rename, permission issue, etc. — do not abort the
            # rollback (the blueprint restore above already succeeded); surface
            # the leftover file instead of silently under-reporting `moved`.
            failed.append(f"{patch_file.name} ({exc})")

    if moved:
        click.echo(f"  moved {moved} applied patch(es) back to patches/pending/")
        click.echo(
            f"  re-apply with: aqueduct patch apply patches/pending/<file> --blueprint {blueprint}"
        )
    if failed:
        click.echo(
            f"⚠ {len(failed)} applied patch(es) could not be moved back to patches/pending/:",
            err=True,
        )
        for item in failed:
            click.echo(f"  · {item}", err=True)


@patch.command("list")
@click.option(
    "--blueprint",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file (used to locate patches/ dir)",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches or CWD/patches)",
)
@click.option(
    "--status",
    "filter_status",
    default="pending",
    type=click.Choice(["pending", "applied", "rejected", "all"]),
    show_default=True,
    help="Which lifecycle directory to list",
)
@click.option(
    "--format",
    "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. `json` for machine-readable consumption (Phase 30b).",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml — resolves the configured patch store backend (local / s3 / …).",
)
@_env_options
def patch_list(
    blueprint: str | None,
    patches_dir: str | None,
    filter_status: str,
    out_format: str,
    config_path: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """List patches from the configured patch store (backend-blind).

    Lists the patch store directly — local ``patches/`` **or** the configured
    object store (s3/gcs/…) per ``stores.blob`` — which is the source of truth.
    Defaults to pending; use ``--status`` for applied/rejected/all. Resolves the
    store from ``aqueduct.yml`` (``--config`` / CWD) + env, so it sees remote
    backends instead of silently scanning the local dir.
    """
    from pathlib import Path

    # Primary path: resolve the configured patch store (local OR object store)
    # from aqueduct.yml + env and list it directly. `--patches-dir` forces the
    # legacy local scan; any resolution failure falls back to it too.
    if not patches_dir:
        try:
            from aqueduct.cli import _load_config_with_env
            from aqueduct.stores.object_store import make_patch_store

            # Auto-discovers aqueduct.yml (CWD walk-up) when no --config + loads .env.
            cfg = _load_config_with_env(
                Path(config_path) if config_path else None,
                env_file=env_file,
                cli_env=cli_env,
            )
            _bp_path = Path(blueprint) if blueprint else None
            # No blueprint → walk up from CWD to the project root (where
            # aqueduct.yml is) for patches/, not raw CWD/patches.
            _patches_root = _patches_root_from_blueprint(
                _bp_path if _bp_path else (Path.cwd() / "_sentinel")
            )
            ps = make_patch_store(cfg.stores.blob.backend, cfg.stores.blob.path, _patches_root)
            _obs = _patch_index_obs_store(_bp_path)
            _list_from_store(ps, filter_status, out_format, obs_store=_obs)
            return
        except Exception as exc:
            # A missing aqueduct.yml resolves to defaults above and never reaches
            # here (load_config's own contract) — anything caught here is a real
            # resolution problem (malformed config, missing store SDK, unreachable
            # backend). Warn rather than silently showing the (possibly wrong)
            # local-dir scan the docstring promises this path avoids.
            click.echo(
                f"⚠ could not resolve the configured patch store ({exc}); "
                "falling back to a local directory scan",
                err=True,
            )

    if patches_dir:
        patches_root = Path(patches_dir)
    elif blueprint:
        patches_root = _patches_root_from_blueprint(Path(blueprint))
    else:
        patches_root = _patches_root_from_blueprint(Path.cwd() / "_sentinel")

    dirs_to_show: list[tuple[str, Path]] = []
    if filter_status == "all":
        for sub in ("pending", "applied", "rejected"):
            d = patches_root / sub
            if d.exists():
                dirs_to_show.append((sub, d))
    else:
        d = patches_root / filter_status
        if d.exists():
            dirs_to_show.append((filter_status, d))

    if out_format.lower() == "json":
        from aqueduct.patch.grammar import PATCH_META_KEY

        payload: list[dict] = []
        for status_label, d in dirs_to_show:
            for f in sorted(d.glob("*.json"), key=lambda x: x.name):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                except Exception:
                    data = {}
                meta = data.get(PATCH_META_KEY) or {}
                payload.append(
                    {
                        "status": status_label,
                        "file": str(f),
                        "patch_id": data.get("patch_id", f.stem),
                        "rationale": data.get("rationale"),
                        "confidence": data.get("confidence"),
                        "category": data.get("category"),
                        "run_id": meta.get("run_id"),
                        "blueprint_id": meta.get("blueprint_id"),
                        "failed_module": meta.get("failed_module"),
                    }
                )
        emit(payload, fmt="json")
        return

    total = 0
    for status_label, d in dirs_to_show:
        files = sorted(d.glob("*.json"), key=lambda f: f.name)
        if not files:
            continue

        click.echo(f"\n  [{status_label}]  {d}")
        group_rows = []
        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                data = {}
            pid = data.get("patch_id", f.stem)
            rationale = (data.get("rationale") or "").replace("\n", " ")
            group_rows.append([f.name, pid, rationale])
            total += 1
        # `rationale` is the flex column — it has by far the highest length
        # variance of the three; `file`/`patch_id` are fixed-format tokens.
        from aqueduct.cli.verbosity import resolve_verbosity

        render_table(
            [Column("file"), Column("patch_id"), Column("rationale", flex=True)],
            group_rows,
            verbose=resolve_verbosity() >= 1,
        )

    if total == 0:
        click.echo(f"No {filter_status} patches found in {patches_root}")
        return

    if filter_status == "pending":
        click.echo(
            "\n  Apply: aqueduct patch apply patches/pending/<file> --blueprint <blueprint.yml>"
        )
        click.echo("  Reject: aqueduct patch reject patches/pending/<file> --reason '<reason>'")


# ── aqueduct log ─────────────────────────────────────────────────────────────


@patch.command("log")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
def log_cmd(blueprint: str, fmt: str) -> None:
    """Show git commit history for a Blueprint with Aqueduct patch metadata.

    Parses ---aqueduct--- blocks from commit messages.  Manual commits (no
    block) are shown as '(manual change)'.
    """
    import re
    import subprocess

    blueprint_path = Path(blueprint)

    result = subprocess.run(
        [
            "git",
            "log",
            "--follow",
            "--format=%H\x1f%ci\x1f%s\x1f%B\x1eENDCOMMIT",
            "--",
            str(blueprint_path),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    raw = result.stdout.strip()
    if not raw:
        click.echo("No git history for this blueprint.")
        return

    _AQ_BLOCK_RE = re.compile(r"---aqueduct---(.*?)---", re.DOTALL)
    _PATCH_LINE_RE = re.compile(r"^\s*-\s+(\S+):\s*(.*)", re.MULTILINE)

    entries = []
    for commit_raw in raw.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        # First line is the \x1f-separated header
        header_line, _, body = commit_raw.partition("\n")
        parts = header_line.split("\x1f")
        if len(parts) < 3:
            continue
        commit_hash = parts[0].strip()
        commit_date = parts[1].strip()
        subject = parts[2].strip()

        aq_match = _AQ_BLOCK_RE.search(body)
        if aq_match:
            block = aq_match.group(1)
            patch_ids = [m.group(1) for m in _PATCH_LINE_RE.finditer(block)]
            ops_match = re.search(r"^ops:\s*(.+)", block, re.MULTILINE)
            ops = ops_match.group(1).strip() if ops_match else ""
            run_match = re.search(r"^run_id:\s*(\S+)", block, re.MULTILINE)
            run_id = run_match.group(1) if run_match else ""
        else:
            patch_ids = []
            ops = ""
            run_id = ""

        entries.append(
            {
                "hash": commit_hash[:8],
                "date": commit_date[:19],
                "subject": subject,
                "patches": ", ".join(patch_ids) if patch_ids else "(manual change)",
                "ops": ops,
                "run_id": run_id,
            }
        )

    if fmt == "json":
        emit(entries, fmt="json")
        return

    if not entries:
        click.echo("No commits found.")
        return

    # `patches` is the flex column — a comma-joined list of patch_ids (or the
    # "(manual change)" fallback) with far more length variance than `ops`,
    # which is a single short commit-trailer descriptor.
    from aqueduct.cli.verbosity import resolve_verbosity

    render_table(
        [
            Column("hash"),
            Column("date"),
            Column("patches", flex=True),
            Column("ops"),
        ],
        [[e["hash"], e["date"], e["patches"], e["ops"]] for e in entries],
        verbose=resolve_verbosity() >= 1,
    )


# ── aqueduct rollback ─────────────────────────────────────────────────────────


@patch.command("rollback")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--to", "patch_id", required=True, help="Revert the git commit containing this patch_id"
)
def rollback_cmd(blueprint: str, patch_id: str) -> None:
    """Revert a Blueprint file to its state before a specific patch was applied.

    Restores only the blueprint file (and any arcade blueprints touched in the
    same commit) by checking out the pre-patch file content from git history,
    then creates a new forward commit. Never rewrites history or touches other
    files in the repository.
    """
    import subprocess

    blueprint_path = Path(blueprint)
    cwd = blueprint_path.parent or Path.cwd()

    # Walk git log scoped to this blueprint file to find the target commit
    result = subprocess.run(
        ["git", "log", "--follow", "--format=%H\x1f%B\x1eENDCOMMIT", "--", str(blueprint_path)],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    target_hash: str | None = None
    for commit_raw in result.stdout.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        header_line, _, body = commit_raw.partition("\n")
        commit_hash = header_line.split("\x1f")[0].strip()
        if patch_id in body:
            target_hash = commit_hash
            break

    if not target_hash:
        click.echo(
            f"✗ patch_id {patch_id!r} not found in git history for {blueprint}\n"
            "  Use 'aqueduct log <blueprint>' to list available patch_ids.",
            err=True,
        )
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Resolve the commit immediately before the patch
    parent = subprocess.run(
        ["git", "rev-parse", f"{target_hash}~1"],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if parent.returncode != 0:
        click.echo(f"✗ could not resolve parent commit: {parent.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    parent_hash = parent.stdout.strip()

    # Discover all blueprint files touched by the patch commit (handles arcades)
    diff_files = subprocess.run(
        ["git", "diff-tree", "--no-commit-id", "-r", "--name-only", target_hash],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if diff_files.returncode != 0:
        click.echo(f"✗ could not list files in commit: {diff_files.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    touched_files = [f.strip() for f in diff_files.stdout.splitlines() if f.strip()]
    if not touched_files:
        click.echo(f"✗ commit {target_hash[:8]} has no file changes", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Restore each file to its pre-patch state (file-scoped, non-destructive)
    for rel_path in touched_files:
        restore = subprocess.run(
            ["git", "checkout", parent_hash, "--", rel_path],
            capture_output=True,
            text=True,
            cwd=cwd,
        )
        if restore.returncode != 0:
            click.echo(f"✗ git checkout {rel_path} failed: {restore.stderr.strip()}", err=True)
            sys.exit(exit_codes.DATA_OR_RUNTIME)

    # Stage restored files and create a forward revert commit
    add = subprocess.run(
        ["git", "add", "--"] + touched_files,
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    commit_msg = (
        f"revert(aqueduct): roll back patch {patch_id!r}\n\n"
        f"Restores {', '.join(touched_files)} to state before commit {target_hash[:8]}."
    )
    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    short = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        cwd=cwd,
    ).stdout.strip()

    click.echo(f"✓ rolled back patch {patch_id!r}  [{short}]")
    for f in touched_files:
        click.echo(f"  restored  {f}  (from {parent_hash[:8]})")
