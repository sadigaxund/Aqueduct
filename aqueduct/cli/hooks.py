"""Blueprint lifecycle hooks — fire `hooks.on_success` / `hooks.on_failure`
after a run reaches its terminal state.

Entry types (parser guarantees exactly one per entry):
  blueprint: <path>   Chain another Blueprint as a fresh `aqueduct run`
                      subprocess — own session, run_id, and report (loose
                      coupling by design; tight coupling belongs in ONE
                      blueprint via Arcades / --parallel / `enabled:`).
  webhook: <url|map>  Fire-and-forget POST via the same endpoint model as
                      the engine-level `webhooks:` block (payload templating
                      included) — `aqueduct.surveyor.webhook.fire_webhook`.
  command: <string>   Arbitrary subprocess. Requires
                      `danger.allow_command_hooks: true` in aqueduct.yml —
                      the gate is operator-owned engine config, so a
                      Blueprint (or an LLM patch) cannot self-authorize.

Security model: fixed argv (shlex, never `shell=True`); interpolation is
limited to ${run.id} / ${run.status} / ${blueprint.id}; per-entry timeout;
exit codes logged. Hook outcomes NEVER change the pipeline's exit code.
A failing hook warns and skips the event's remaining hooks. The patch
grammar has no operation that can address `hooks:`, so the self-healer
cannot inject or alter hooks.

Cycle guard: AQUEDUCT_HOOK_CHAIN carries the resolved ancestor blueprint
paths (os.pathsep-joined) through `blueprint:` subprocesses. A hook whose
target is already in the chain is refused ([hook_cycle]); chain depth is
capped at 8 ([hook_depth]). `aqueduct doctor` runs the same walk statically
via `static_hook_check`.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
import time
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


def run_hooks(
    entries: Sequence[HookEntry],
    event: str,
    *,
    run_id: str,
    status: str,
    blueprint_id: str,
    blueprint_path: str,
    allow_command_hooks: bool,
) -> bool:
    """Execute one event's hook entries sequentially.

    Returns True when a hooks section was rendered (entries present) so the
    caller can close with a `run complete` footer. Never raises; never
    changes the pipeline's exit code.
    """
    if not entries:
        return False
    from aqueduct.cli.style import info as _info
    from aqueduct.cli.style import warn as _warn

    _info(f"· hooks  ·  {event} ({len(entries)})")
    run_vars = {"run.id": run_id, "run.status": status, "blueprint.id": blueprint_id}
    bp_path = Path(blueprint_path).resolve()
    chain = _chain_paths()

    def _ok(label: str, dur: float) -> None:
        icon = click.style("✓", fg="green")
        click.echo(f"  {icon} {label}    " + click.style(_fmt_dur(dur), dim=True))

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
        for event in ("on_success", "on_failure"):
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
