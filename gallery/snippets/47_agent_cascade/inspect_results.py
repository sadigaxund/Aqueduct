import glob
import os

import duckdb
from rich.console import Console
from rich.table import Table

console = Console()

BLUEPRINT_ID = "cascade_demo"


def _find_store():
    candidates = [f".aqueduct/{BLUEPRINT_ID}/observability.db"]
    candidates += sorted(glob.glob(".aqueduct/*/observability.db"))
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def main():
    output = "data/output/orders.parquet"
    if os.path.exists(output):
        con = duckdb.connect()
        try:
            rows = con.execute(
                f"SELECT * FROM read_parquet('{output}') ORDER BY order_id"
            ).fetchall()
            columns = [d[0] for d in con.description]
        finally:
            con.close()
        t = Table(title="Orders (after cascade healing)", header_style="bold cyan")
        for c in columns:
            t.add_column(c)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
    else:
        console.print(
            "[bold yellow]⚠[/bold yellow] Output not found. The pipeline fails on "
            "purpose (column 'total' doesn't exist).\n"
            "  Run [bold]aqueduct run blueprint.yml[/bold]. 'approval: auto' "
            "triggers the cascade automatically."
        )

    db_path = _find_store()
    if db_path is None:
        return

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT model, model_cascade_position, patch_applied, run_success_after_patch
            FROM healing_outcomes
            ORDER BY applied_at
            """
        ).fetchall()
    finally:
        con.close()

    if not rows:
        console.print("\n[dim]No healing_outcomes recorded yet.[/dim]")
        return

    t2 = Table(title="Cascade Healing Outcomes", header_style="bold magenta")
    t2.add_column("Model")
    t2.add_column("Cascade tier (0-based)")
    t2.add_column("Patch applied")
    t2.add_column("Run succeeded after patch")
    for model, tier, applied, succeeded in rows:
        t2.add_row(str(model), str(tier), str(applied), str(succeeded))
    console.print(t2)
    console.print(
        "\n[dim]tier 0 = the cheap model tried first; a non-zero tier means "
        "tier 0 got stuck/exhausted/deferred and the cascade escalated.[/dim]"
    )


if __name__ == "__main__":
    main()
