"""Inspect the self-healing showcase: the latest run's outcome, the applied
PatchSpec the agent generated, and a before/after diff of the blueprint.

Just for fun / demo narration — not part of the engine's own CLI. Reads:
  - .aqueduct/<blueprint_id>/observability.db  (run_records, healing_outcomes)
  - patches/applied/*.json                     (the PatchSpec the agent wrote)
  - patches/backups/*.yml                       (blueprint snapshot pre-patch)
  - blueprints/showcase_orders.yml              (blueprint post-patch, "after")

`aqueduct patch preview` shows a diff too, but only for a still-PENDING patch
file (before apply) — there's no built-in command to re-show the diff for an
already-applied patch after the fact, so this reconstructs it from the
patches/backups/ snapshot apply_patch_file() writes before overwriting the
blueprint.
"""
from __future__ import annotations

import difflib
import json
import os
from pathlib import Path

import duckdb
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

console = Console()

BLUEPRINT_ID = "showcase.self_healing.orders"
BLUEPRINT_PATH = Path("blueprints/showcase_orders.yml")
OBS_DB = Path(".aqueduct") / BLUEPRINT_ID / "observability.db"


def _latest(paths: list[Path]) -> Path | None:
    return max(paths, key=lambda p: p.stat().st_mtime) if paths else None


def show_run_summary() -> str | None:
    if not OBS_DB.exists():
        console.print(f"[bold red]✗[/bold red] No observability store at {OBS_DB} — run the blueprint first.")
        return None

    con = duckdb.connect(str(OBS_DB), read_only=True)
    # A heal-and-retry cycle writes TWO run_records rows sharing the same
    # started_at: the original failure (status=error) and the successful
    # retry (status=patched, ExecutionStatus.PATCHED) — the retry always
    # FINISHES later, so finished_at DESC (not started_at DESC) is what
    # correctly surfaces the true terminal outcome.
    run = con.execute(
        "SELECT run_id, status, started_at, finished_at FROM run_records "
        "WHERE blueprint_id = ? ORDER BY finished_at DESC LIMIT 1",
        [BLUEPRINT_ID],
    ).fetchone()
    if run is None:
        console.print("[bold red]✗[/bold red] No runs recorded yet.")
        con.close()
        return None
    run_id, status, started_at, finished_at = run

    t = Table(title="Latest Run", header_style="bold cyan", show_header=False)
    t.add_column(style="dim")
    t.add_column()
    t.add_row("run_id", str(run_id))
    color = "green" if status in ("success", "patched") else "yellow"
    t.add_row("status", f"[{color}]{status}[/{color}]")
    t.add_row("started", str(started_at))
    t.add_row("finished", str(finished_at))
    console.print(t)

    # healing_outcomes has no blueprint_id column, but this observability
    # store is itself per-blueprint (routed at <path>/<blueprint_id>/...),
    # so every row in this file already belongs to this one blueprint —
    # no run_id join needed, just the most recent attempts.
    outcomes = con.execute(
        "SELECT failed_module, model, patch_id, confidence, patch_applied, "
        "run_success_after_patch, resolution, model_cascade_position "
        "FROM healing_outcomes ORDER BY applied_at DESC LIMIT 5",
    ).fetchall()
    con.close()

    if outcomes:
        t2 = Table(title="Healing Attempts", header_style="bold magenta")
        for col in ["failed_module", "model", "patch_id", "confidence", "applied", "run_ok", "resolution", "tier"]:
            t2.add_column(col)
        for o in outcomes:
            failed_module, model, patch_id, confidence, applied, run_ok, resolution, tier = o
            t2.add_row(
                failed_module or "", model or "", patch_id or "",
                f"{confidence:.2f}" if confidence is not None else "",
                "✓" if applied else "✗",
                "✓" if run_ok else "✗",
                resolution or "", str(tier) if tier is not None else "",
            )
        console.print(t2)
    return run_id


def show_patch_spec() -> str | None:
    applied = _latest(list(Path("patches/applied").glob("*.json")))
    if applied is None:
        console.print("[dim]No applied patch found under patches/applied/ — "
                       "either healing hasn't run yet, or no agent was reachable.[/dim]")
        return None

    spec = json.loads(applied.read_text())
    meta = spec.get("_aq_meta", {})

    body = (
        f"[bold]{spec.get('patch_id')}[/bold]\n\n"
        f"[dim]root cause:[/dim] {spec.get('root_cause')}\n"
        f"[dim]rationale:[/dim]  {spec.get('rationale')}\n"
        f"[dim]category:[/dim]   {spec.get('category')}   "
        f"[dim]confidence:[/dim] {spec.get('confidence')}\n"
        f"[dim]applied at:[/dim] {meta.get('applied_at')}   "
        f"[dim]approval:[/dim] {meta.get('approval_mode')}"
    )
    console.print(Panel(body, title="Applied PatchSpec", border_style="green"))

    ops_t = Table(title="Operations", header_style="bold green")
    ops_t.add_column("op")
    ops_t.add_column("module_id")
    ops_t.add_column("key")
    ops_t.add_column("value", overflow="fold")
    for op in spec.get("operations", []):
        ops_t.add_row(
            op.get("op", ""), op.get("module_id", ""), op.get("key", ""),
            str(op.get("value", ""))[:100],
        )
    console.print(ops_t)
    return spec.get("patch_id")


def show_diff(patch_id: str | None) -> None:
    if patch_id is None:
        return
    backups = [p for p in Path("patches/backups").glob("*.yml") if patch_id in p.name]
    backup = _latest(backups)
    if backup is None or not BLUEPRINT_PATH.exists():
        console.print("[dim]No before/after snapshot available to diff.[/dim]")
        return

    before = backup.read_text().splitlines(keepends=True)
    after = BLUEPRINT_PATH.read_text().splitlines(keepends=True)
    diff_text = "".join(difflib.unified_diff(
        before, after,
        fromfile=f"blueprint.yml (before — {backup.name})",
        tofile="blueprint.yml (after)",
        n=2,
    ))
    if not diff_text.strip():
        console.print("[dim]No textual difference between backup and current blueprint.[/dim]")
        return
    console.print(Panel(
        Syntax(diff_text, "diff", theme="ansi_dark", word_wrap=True),
        title="Before → After",
        border_style="yellow",
    ))


def show_outputs() -> None:
    paths = {
        "express": "data/output/express_orders.parquet",
        "standard": "data/output/standard_orders.parquet",
        "bulk": "data/output/bulk_orders.parquet",
        "quarantine": "data/output/quarantine.parquet",
    }
    import pandas as pd
    t = Table(title="Output Row Counts", header_style="bold blue")
    t.add_column("tier")
    t.add_column("rows")
    any_found = False
    for tier, path in paths.items():
        if os.path.exists(path):
            any_found = True
            t.add_row(tier, str(len(pd.read_parquet(path))))
        else:
            t.add_row(tier, "[dim](not written)[/dim]")
    console.print(t)
    if not any_found:
        console.print("[dim]No output yet — the run either hasn't happened or hasn't healed successfully.[/dim]")


def main() -> None:
#    show_run_summary()
    console.print()
    patch_id = show_patch_spec()
    console.print()
    show_diff(patch_id)
    console.print()
#    show_outputs()


if __name__ == "__main__":
    main()
