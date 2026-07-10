"""Blueprint lifecycle hooks — fire `hooks.on_success` / `hooks.on_failure` /
`hooks.on_patch_pending` / `hooks.on_healed`.

`on_success` / `on_failure` fire after a run reaches its terminal state.
`on_patch_pending` / `on_healed` fire mid-run at heal milestones — the
former when a heal stages a patch for human/CI review, the latter right
after a heal's re-run succeeds (patch applied AND the pipeline is green
again) — mirroring the engine-level `webhooks:` vocabulary
(`on_patch_pending`/`on_ci_patch`) one level up, at the Blueprint.

Entry types (parser guarantees exactly one per entry):
  blueprint: <path>   Chain another Blueprint. By default a fresh
                      `aqueduct run` subprocess — own session, run_id, and
                      report (loose coupling by design; tight coupling
                      belongs in ONE blueprint via Arcades / --parallel /
                      `enabled:`). `in_process: true` opts into parsing +
                      compiling + executing the target in THIS process,
                      reusing the live SparkSession (session reuse — no
                      self-healing loop for the chained target; falls back
                      to the subprocess path when the target's
                      `spark_config` is non-empty, since merging two
                      Blueprints' Spark configs into one live session isn't
                      generally safe).
  webhook: <url|map>  Fire-and-forget POST via the same endpoint model as
                      the engine-level `webhooks:` block (payload templating
                      included) — `aqueduct.surveyor.webhook.fire_webhook`.
  command: <string>   Arbitrary subprocess. Requires
                      `danger.allow_command_hooks: true` in aqueduct.yml —
                      the gate is operator-owned engine config, so a
                      Blueprint (or an LLM patch) cannot self-authorize.

`when_error:` (optional, on events with a failure context — on_failure /
on_patch_pending / on_healed) filters entries to fire only when the run's
error_type / stack-trace exception class matches one of the listed names —
same exact-match candidate set as `GuardrailsConfig.heal_on_errors`. A
non-matching entry is silently skipped (not a `[hook_failed]`) and does not
stop the remaining entries of that event.

Security model: fixed argv (shlex, never `shell=True`); interpolation is
limited to ${run.id} / ${run.status} / ${blueprint.id}; per-entry timeout;
exit codes logged. Hook outcomes NEVER change the pipeline's exit code.
A failing hook warns and skips the event's remaining hooks. The patch
grammar has no operation that can address `hooks:`, so the self-healer
cannot inject or alter hooks.

Cycle guard: AQUEDUCT_HOOK_CHAIN carries the resolved ancestor blueprint
paths (os.pathsep-joined) through `blueprint:` subprocesses. In-process
chained hooks carry the same ancestry via an explicit in-memory chain (the
env var is process-scoped and does not propagate across an in-process
call). A hook whose target is already in the chain is refused
([hook_cycle]); chain depth is capped at 8 ([hook_depth]). `aqueduct doctor`
runs the same walk statically via `static_hook_check`.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

if TYPE_CHECKING:
    from collections.abc import Sequence

    from aqueduct.parser.models import HookEntry

_CHAIN_ENV = "AQUEDUCT_HOOK_CHAIN"
_MAX_DEPTH = 8
# Same-interpreter relaunch — immune to PATH pointing at a different (stale)
# aqueduct install than the one currently running.
_RELAUNCH = "from aqueduct.cli import cli; cli()"


def _interpolate(text: str, run_vars: dict[str, str]) -> str:
    out = text
    for k, v in run_vars.items():
        out = out.replace("${" + k + "}", v)
    return out


def _fmt_dur(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.1f} s"
    return f"{int(seconds // 60)}m {seconds % 60:02.0f}s"


def _chain_paths() -> list[str]:
    raw = os.environ.get(_CHAIN_ENV, "")
    return [p for p in raw.split(os.pathsep) if p]


def _resolve_target(value: str, anchor: Path) -> Path:
    p = Path(value)
    return p.resolve() if p.is_absolute() else (anchor.parent / p).resolve()


def _error_candidates(failure_ctx: Any) -> set[str]:
    """Same candidate set GuardrailsConfig.heal_on_errors matches against:
    the Assert-rule error_type label, plus the exception class name
    extracted from the stack trace for infra errors."""
    from aqueduct.cli import _extract_stack_class

    candidates: set[str] = set()
    error_type = getattr(failure_ctx, "error_type", None)
    if error_type:
        candidates.add(error_type)
    stack_class = _extract_stack_class(getattr(failure_ctx, "stack_trace", None))
    if stack_class:
        candidates.add(stack_class)
    return candidates


def _hook_matches_error(entry: HookEntry, failure_ctx: Any | None) -> bool:
    """True when `entry` should fire given the run's failure context.

    Unset `when_error` always fires (backward-compatible default). A set
    `when_error` with no failure_ctx available fires too — the schema
    blocks when_error on events without a failure context (on_success), so
    this branch is a defensive fallback, not the expected path.
    """
    if not entry.when_error:
        return True
    if failure_ctx is None:
        return True
    return bool(_error_candidates(failure_ctx) & set(entry.when_error))


def run_hooks(
    entries: Sequence[HookEntry],
    event: str,
    *,
    run_id: str,
    status: str,
    blueprint_id: str,
    blueprint_path: str,
    allow_command_hooks: bool,
    failure_ctx: Any | None = None,
    session: Any | None = None,
    _chain: list[str] | None = None,
) -> bool:
    """Execute one event's hook entries sequentially.

    Args:
        failure_ctx: The run's FailureContext, when the event carries one
            (on_failure / on_patch_pending / on_healed) — feeds `when_error`
            matching. None for on_success (no failure context to filter on).
        session: Live SparkSession, when the caller has one available —
            enables `in_process: true` blueprint entries. None falls back to
            the subprocess path for every blueprint entry regardless of
            `in_process`.
        _chain: Internal — explicit ancestor chain for recursive in-process
            calls (the env-var chain is process-scoped and doesn't see
            in-process calls). None reads the env var as before.

    Returns True when a hooks section was rendered (entries present) so the
    caller can close with a `run complete` footer. Never raises; never
    changes the pipeline's exit code.
    """
    if not entries:
        return False
    from aqueduct.cli.style import info as _info
    from aqueduct.cli.style import warn as _warn

    matching = [h for h in entries if _hook_matches_error(h, failure_ctx)]
    if not matching:
        return False
    _info(f"· hooks  ·  {event} ({len(matching)})")
    run_vars = {"run.id": run_id, "run.status": status, "blueprint.id": blueprint_id}
    bp_path = Path(blueprint_path).resolve()
    chain = _chain if _chain is not None else _chain_paths()

    def _ok(label: str, dur: float) -> None:
        icon = click.style("✓", fg="green")
        click.echo(f"  {icon} {label}    " + click.style(_fmt_dur(dur), dim=True))

    entries = matching
    for i, h in enumerate(entries):
        argv: list[str] | None = None
        env = None
        label = ""

        if h.kind == "command":
            # Interpolated form in the label — shows what actually ran.
            label = _interpolate(str(h.value), run_vars)
            if not allow_command_hooks:
                _warn(
                    f"[hook_command_disabled] command hook skipped — set "
                    f"danger.allow_command_hooks: true in aqueduct.yml: {label}"
                )
                continue  # gating skips the entry, not the event
            try:
                argv = shlex.split(label)
            except ValueError as exc:
                _warn(f"[hook_failed] {label} — bad command syntax: {exc}")
                break

        elif h.kind == "blueprint":
            target = _resolve_target(str(h.value), bp_path)
            label = f"aqueduct run {h.value}"
            if str(target) == str(bp_path) or str(target) in chain:
                _warn(
                    f"[hook_cycle] blueprint hook skipped — {h.value} is already "
                    "in the hook chain (would loop forever)"
                )
                continue
            if len(chain) + 1 >= _MAX_DEPTH:
                _warn(f"[hook_depth] blueprint hook skipped — chain depth cap ({_MAX_DEPTH}) reached")
                continue
            if not target.exists():
                _warn(f"[hook_failed] {label} — blueprint not found: {target}")
                break

            if h.in_process and session is not None:
                handled = _run_in_process_blueprint_hook(
                    target=target, label=f"in-process {h.value}", session=session,
                    chain=chain, bp_path=bp_path, warn=_warn, ok=_ok,
                    allow_command_hooks=allow_command_hooks,
                )
                if handled:
                    continue
                # Fell back to subprocess (target had a non-empty
                # spark_config) — build the same argv as the default path.

            argv = [sys.executable, "-c", _RELAUNCH, "run", str(target)]
            env = {**os.environ, _CHAIN_ENV: os.pathsep.join([*chain, str(bp_path)])}

        elif h.kind == "webhook":
            raw: Any = h.value
            url = raw.get("url", "") if isinstance(raw, dict) else str(raw)
            label = f"webhook {url}"
            try:
                from aqueduct.config import WebhookEndpointConfig
                from aqueduct.surveyor.webhook import fire_webhook
                endpoint = WebhookEndpointConfig.model_validate(
                    raw if isinstance(raw, dict) else {"url": raw}
                )
                payload = {"run_id": run_id, "blueprint_id": blueprint_id, "status": status}
                fire_webhook(endpoint, full_payload=payload, template_vars=payload, event=event)
                icon = click.style("✓", fg="green")
                click.echo(f"  {icon} {label}    " + click.style("fired (async)", dim=True))
            except Exception as exc:  # noqa: BLE001 — webhooks are best-effort by contract
                _warn(f"[hook_failed] {label} — {exc}")
                if i + 1 < len(entries):
                    _warn(f"[hook_aborted] {len(entries) - i - 1} remaining hook(s) skipped")
                break
            continue

        # blueprint / command — synchronous subprocess with timeout
        t0 = time.monotonic()
        try:
            rc = subprocess.run(argv, env=env, timeout=h.timeout, check=False).returncode  # noqa: S603 — argv is fixed (shlex, no shell); command entries are danger-gated
        except subprocess.TimeoutExpired:
            _warn(f"[hook_failed] {label} — timeout after {h.timeout}s")
            rc = -1
        except OSError as exc:
            _warn(f"[hook_failed] {label} — {exc}")
            rc = -1
        dur = time.monotonic() - t0
        if rc == 0:
            _ok(label, dur)
        else:
            if rc > 0:
                _warn(f"[hook_failed] {label} — exit {rc}  ·  {_fmt_dur(dur)}")
            if i + 1 < len(entries):
                _warn(f"[hook_aborted] {len(entries) - i - 1} remaining hook(s) skipped")
            break
    return True


def _run_in_process_blueprint_hook(
    *,
    target: Path,
    label: str,
    session: Any,
    chain: list[str],
    bp_path: Path,
    warn: Any,
    ok: Any,
    allow_command_hooks: bool,
) -> bool:
    """Parse + compile + execute `target` in this process, reusing `session`.

    No self-healing loop for the chained target — the heal loop lives in
    `aqueduct run`'s CLI orchestration and is not (yet) reusable as a bare
    library call; an in-process hook target that fails just reports
    `[hook_failed]` like any other hook, same as a subprocess `aqueduct run`
    would report a non-zero exit on failure. The target's OWN
    `on_success`/`on_failure` hooks (not `on_patch_pending`/`on_healed` — no
    heal loop ran) fire recursively afterwards, same-session, chain-guarded.

    Returns True when the in-process path handled the entry (success or
    failure — both terminal for this entry); False to signal "fall back to
    the subprocess path" (only when the target's spark_config is non-empty).
    """
    from aqueduct.parser.parser import parse as _parse

    try:
        t_bp = _parse(str(target))
    except Exception as exc:  # noqa: BLE001 — parse errors are reported as hook failures, not raised
        warn(f"[hook_failed] {label} — parse error: {exc}")
        return True

    if t_bp.spark_config:
        # Safe subset per design: two Blueprints' spark_config may conflict
        # in a shared live session (e.g. differing shuffle partitions) —
        # fall back to the isolated subprocess path instead of guessing at
        # a merge policy. (No pyspark import needed on this branch — keeps
        # the fallback usable on a [spark]-less install too.)
        from aqueduct.cli.style import info as _info
        _info(
            f"[hook_inprocess_fallback] {label} — target sets spark_config, "
            "falling back to subprocess (session config would conflict)"
        )
        return False

    from aqueduct.compiler.compiler import compile as _compiler_compile
    from aqueduct.executor import ExecuteError, get_executor
    from aqueduct.executor.models import ExecutionStatus

    try:
        t_manifest = _compiler_compile(t_bp, blueprint_path=target)
    except Exception as exc:  # noqa: BLE001 — compile errors are reported as hook failures, not raised
        warn(f"[hook_failed] {label} — compile error: {exc}")
        return True

    execute_fn = get_executor("spark")
    t_run_id = str(uuid.uuid4())
    t0 = time.monotonic()
    try:
        t_result = execute_fn(t_manifest, session, run_id=t_run_id, store_dir=None, surveyor=None, depot=None)
    except ExecuteError as exc:
        warn(f"[hook_failed] {label} — {exc}")
        return True
    dur = time.monotonic() - t0

    success = t_result.status == ExecutionStatus.SUCCESS
    if success:
        ok(label, dur)
    else:
        failing = next((r for r in t_result.module_results if r.status == ExecutionStatus.ERROR), None)
        detail = f" — {failing.module_id}: {failing.error}" if failing is not None else ""
        warn(f"[hook_failed] {label}{detail}  ·  {_fmt_dur(dur)}")

    # Chain the target's own on_success/on_failure hooks — same session,
    # explicit chain (env var doesn't see in-process calls).
    new_chain = [*chain, str(bp_path)]
    t_status = "success" if success else "failure"
    t_event = "on_success" if success else "on_failure"
    t_entries = t_bp.hooks.on_success if success else t_bp.hooks.on_failure
    if t_entries:
        run_hooks(
            t_entries, t_event,
            run_id=t_run_id, status=t_status,
            blueprint_id=t_bp.id, blueprint_path=str(target),
            allow_command_hooks=allow_command_hooks,
            session=session, _chain=new_chain,
        )
    return True


def static_hook_check(blueprint_path: Path) -> list[str]:
    """Doctor helper — walk `blueprint:` hook references without running.

    Follows the hook graph depth-first (raw YAML reads, no full parse) and
    returns human-readable problems: missing target files and cycles. Empty
    list = healthy.
    """
    import yaml

    problems: list[str] = []
    visiting: list[str] = []

    def _refs(path: Path) -> list[str]:
        try:
            # Raw structural pre-scan only (hooks.blueprint refs) — full parse()/path-anchoring
            # deliberately skipped here, resolution happens at run time.
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:  # noqa: BLE001 — unreadable YAML is reported, not raised
            problems.append(f"{path}: unreadable ({exc})")
            return []
        hooks = raw.get("hooks") or {}
        out: list[str] = []
        for event in ("on_success", "on_failure", "on_patch_pending", "on_healed"):
            for entry in hooks.get(event) or []:
                if isinstance(entry, dict) and entry.get("blueprint"):
                    out.append(str(entry["blueprint"]))
        return out

    def _walk(path: Path) -> None:
        rp = str(path.resolve())
        if rp in visiting:
            cycle = " → ".join([Path(p).name for p in visiting[visiting.index(rp):]] + [path.name])
            problems.append(f"hook cycle: {cycle}")
            return
        if len(visiting) >= _MAX_DEPTH:
            problems.append(f"hook chain deeper than {_MAX_DEPTH} at {path.name}")
            return
        visiting.append(rp)
        for ref in _refs(path):
            target = _resolve_target(ref, path.resolve())
            if not target.exists():
                problems.append(f"{path.name}: blueprint hook target not found: {ref}")
                continue
            _walk(target)
        visiting.pop()

    _walk(blueprint_path)
    return problems
