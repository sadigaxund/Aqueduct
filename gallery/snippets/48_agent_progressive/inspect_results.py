import glob
import os

import duckdb
from rich.console import Console
from rich.table import Table

console = Console()

BLUEPRINT_ID = "progressive_demo"


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
        t = Table(title="Orders (after progressive healing)", header_style="bold cyan")
        for c in columns:
            t.add_column(c)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
    else:
        console.print(
            "[bold yellow]⚠[/bold yellow] Output not found. The pipeline fails on "
            "purpose at 'priced' (bug #1: 'unit_cost' doesn't exist).\n"
            "  Run [bold]aqueduct run blueprint.yml[/bold]. 'approval: auto' plus "
            "'agent.progressive: true' chains both bugs' fixes into one combined patch."
        )

    db_path = _find_store()
    if db_path is None:
        return

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT attempt_num, where_field, normalized_message
            FROM heal_attempts
            ORDER BY attempt_num
            """
        ).fetchall()
    finally:
        con.close()

    if not rows:
        console.print("\n[dim]No heal_attempts recorded yet.[/dim]")
        return

    t2 = Table(title="Healing Attempts", header_style="bold magenta")
    t2.add_column("Attempt #")
    t2.add_column("Failed module")
    t2.add_column("Error")
    for attempt_num, where_field, msg in rows:
        t2.add_row(str(attempt_num), str(where_field), str(msg))
    console.print(t2)
    console.print(
        "\n[dim]A 'priced' row followed by a 'discounted' row is the "
        "progressive chain advancing: link 1 diagnosed 'priced' (bug #1), "
        "the re-run then failed at 'discounted' (bug #2, a DIFFERENT "
        "module), so link 1's fix was folded into the accumulated patch "
        "instead of discarded. Link 2 diagnosed 'discounted' against that "
        "already-patched manifest. One combined 2-op patch is what "
        "actually gets written to the Blueprint.[/dim]"
    )


if __name__ == "__main__":
    main()
