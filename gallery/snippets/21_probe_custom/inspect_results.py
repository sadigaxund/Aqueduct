import json
import os

import duckdb
from rich.console import Console
from rich.table import Table

console = Console()


def main():
    db_path = ".aqueduct/observability/probe_custom_demo/observability.db"

    if not os.path.exists(db_path):
        console.print("[bold red]Error:[/bold red] Observability store not found. Did you run the pipeline?")
        return

    conn = duckdb.connect(db_path)
    try:
        row = conn.execute("""
            SELECT payload
            FROM probe_signals
            WHERE signal_type = 'custom'
            ORDER BY captured_at DESC
            LIMIT 1
        """).fetchone()

        if not row:
            console.print("[bold red]Error:[/bold red] No custom signal found in store.")
            return

        payload = json.loads(row[0])

        table = Table(title="Custom Probe Signal")
        table.add_column("Key")
        table.add_column("Value")
        for k, v in payload.items():
            table.add_row(str(k), str(v))
        console.print(table)

        if "passed" in payload:
            verdict = "[bold green]OPEN[/bold green]" if payload["passed"] else "[bold red]CLOSED[/bold red]"
            console.print(f"Regulator gate verdict (from passed_when): {verdict}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
