import glob
import json
import os

import duckdb
from rich.console import Console
from rich.table import Table

console = Console()

BLUEPRINT_ID = "schema_drift_demo"


def _find_store():
    candidates = [f".aqueduct/{BLUEPRINT_ID}/observability.db"]
    candidates += sorted(glob.glob(".aqueduct/*/observability.db"))
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def main():
    db_path = _find_store()
    if db_path is None:
        console.print(
            "[bold yellow]⚠[/bold yellow] No observability store found. Run "
            "'python populate_data.py' then 'aqueduct drift blueprint.yml' first."
        )
        return

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT module_id, checked_at, status, breaking_changes, benign_changes
            FROM drift_checks
            ORDER BY checked_at
            """
        ).fetchall()
    except duckdb.CatalogException:
        # `drift_checks` is only created by `aqueduct drift`, never by a
        # plain `aqueduct run`. A CI/plain-run pass with no drift check yet
        # is expected, not an error — see README's "How to run" for the
        # `aqueduct drift` walkthrough that populates this table.
        console.print(
            "[bold yellow]⚠[/bold yellow] No drift_checks table yet — run "
            "'aqueduct drift blueprint.yml' to populate it (see README)."
        )
        return
    finally:
        con.close()

    if not rows:
        console.print("[bold yellow]⚠[/bold yellow] drift_checks table is empty.")
        return

    t = Table(title="Schema Drift Checks", header_style="bold cyan")
    t.add_column("Module")
    t.add_column("Checked At")
    t.add_column("Status")
    t.add_column("Breaking")
    t.add_column("Benign")

    for module_id, checked_at, status, breaking, benign in rows:
        breaking_txt = ", ".join(c["column"] for c in json.loads(breaking)) if breaking else "-"
        benign_txt = ", ".join(c["column"] for c in json.loads(benign)) if benign else "-"
        style = "bold red" if status == "drift_breaking" else None
        t.add_row(module_id, str(checked_at), status, breaking_txt, benign_txt, style=style)

    console.print(t)
    console.print(
        "\n[dim]orders_a: a dropped column is 'breaking', it fires a synthetic "
        "FailureContext and (with an LLM configured) stages a heal patch. "
        "orders_b: an added column is 'benign', recorded for audit, no heal.[/dim]"
    )


if __name__ == "__main__":
    main()
