"""`blueprint` command group — per-blueprint diagnostics.

New in this phase: `aqueduct blueprint history` merges the store-side
remediation timeline (`stores/queries.py::blueprint_history`) with the git
commit history of the blueprint file (`git_blueprint_commits`) into one
chronological, read-only view. Query logic lives in `stores/queries.py`
(the one read layer); this module only resolves the blueprint id/path and
renders through `style.py` / `emit()`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
    cli,
)
from aqueduct.cli.output import emit
from aqueduct.cli.style import error as _error

_EVENT_ICON = {
    "heal_run_started": "·",  # ·
    "patch_apply": "✓",  # ✓
    "outcome": "✓",  # ✓ (color carries success/fail below)
    "patch_reject": "✗",  # ✗
    "manual_edit": "⚠",  # ⚠
}


def _resolve_blueprint(blueprint_arg: str) -> tuple[str, Path | None]:
    """(blueprint_id, blueprint_path-or-None) from an id or a YAML file path."""
    p = Path(blueprint_arg)
    if p.suffix in (".yml", ".yaml") and p.exists():
        try:
            from aqueduct.parser.parser import parse

            return parse(str(p)).id, p
        except Exception:
            return blueprint_arg, p
    return blueprint_arg, None


# ── blueprint command group ─────────────────────────────────────────────────


@cli.group("blueprint")
def blueprint_group() -> None:
    """Per-blueprint diagnostics."""


@blueprint_group.command("history")
@click.argument("blueprint_arg", metavar="BLUEPRINT_ID_OR_PATH")
@click.option("--store-dir", default=None, help="Observability store directory")
@click.option("--config", "config_path", default=None, help="Path to aqueduct.yml")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
@_env_options
def blueprint_history_cmd(
    blueprint_arg: str,
    store_dir: str | None,
    config_path: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Chronological remediation timeline for one blueprint.

    Merges healing activity from the observability store (heal run starts,
    PATCH_APPLY with confidence, run outcome, PATCH_REJECT — read from
    `patch_index` + `healing_outcomes`) with the blueprint file's git commit
    history (when it is git-tracked): a commit carrying no `---aqueduct---`
    trailer is shown as MANUAL_EDIT. Read-only; never writes.
    """
    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(env_file, Path(config_path) if config_path else None, cli_env=cli_env)
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    blueprint_id, blueprint_path = _resolve_blueprint(blueprint_arg)

    events = blueprint_history_events(cfg, blueprint_id, blueprint_path, store_dir)

    if fmt == "json":
        emit(
            {
                "blueprint_id": blueprint_id,
                "events": [
                    {
                        "timestamp": e["timestamp"],
                        "event_type": e["event_type"],
                        "description": e["description"],
                        "patch_id": e.get("patch_id"),
                        "confidence": e.get("confidence"),
                        "run_id": e.get("run_id"),
                        "git_sha": e.get("git_sha"),
                    }
                    for e in events
                ],
            },
            fmt="json",
        )
        return

    if not events:
        click.echo(f"No remediation history for blueprint {blueprint_id!r}.")
        return

    click.echo(f"Blueprint history — {blueprint_id}")
    click.echo("")
    for e in events:
        icon = _EVENT_ICON.get(e["event_type"], "·")
        sha = f"  [{e['git_sha']}]" if e.get("git_sha") else ""
        conf = f"  confidence={e['confidence']:.2f}" if e.get("confidence") is not None else ""
        ts = e["timestamp"] or "(unknown time)"
        click.echo(f"  {icon} {ts:<26} {e['event_type']:<18} {e['description']}{conf}{sha}")


def blueprint_history_events(
    cfg,
    blueprint_id: str,
    blueprint_path: Path | None,
    store_dir: str | None = None,
) -> list[dict]:
    """Merge store events + git commits into one chronological event list.

    Shared by the CLI command and the `blueprint_history` registry tool
    (dogfooding item 1) — the merge/sort logic lives once, here.
    """
    from aqueduct.stores.queries import blueprint_history as _store_history
    from aqueduct.stores.queries import git_blueprint_commits

    store_events = [
        {
            "timestamp": e.timestamp,
            "event_type": e.event_type,
            "description": e.description,
            "patch_id": e.patch_id,
            "confidence": e.confidence,
            "run_id": e.run_id,
            "git_sha": None,
        }
        for e in _store_history(cfg, blueprint_id, store_dir=store_dir)
    ]

    git_events: list[dict] = []
    if blueprint_path is not None:
        for c in git_blueprint_commits(blueprint_path):
            if c["manual_edit"]:
                git_events.append(
                    {
                        "timestamp": c["timestamp"],
                        "event_type": "manual_edit",
                        "description": f"manual edit: {c['subject']}",
                        "patch_id": None,
                        "confidence": None,
                        "run_id": None,
                        "git_sha": c["git_sha"],
                    }
                )
            else:
                # Attach the git sha to the matching patch_apply event(s)
                # instead of emitting a duplicate row.
                for pid in c["patch_ids"]:
                    for e in store_events:
                        if e["event_type"] == "patch_apply" and e["patch_id"] == pid:
                            e["git_sha"] = c["git_sha"]

    merged = store_events + git_events
    merged.sort(key=lambda e: e["timestamp"] or "")
    return merged
