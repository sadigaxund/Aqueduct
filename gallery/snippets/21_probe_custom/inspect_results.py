import duckdb
import json
from rich.console import Console
from rich.table import Table
import os

console = Console()


def main():
    db_path = ".aqueduct/observability.db"
    if not os.path.exists(db_path):
        console.print(f"[bold red]✗[/bold red] Observability DB not found at {db_path}. Did you run 'aqueduct run blueprint.yml'?")
        return

    con = duckdb.connect(db_path)
    rows = con.execute(
        "SELECT signal_type, payload, captured_at FROM probe_signals ORDER BY captured_at"
    ).fetchall()

    if not rows:
        console.print("[bold yellow]⚠[/bold yellow] No probe signals found in the database.")
        return

    t = Table(title="Custom Probe Signals", header_style="bold cyan")
    t.add_column("Signal Type")
    t.add_column("Estimate")
    t.add_column("Metadata")
    t.add_column("Captured At")

    for signal_type, payload_raw, captured_at in rows:
        payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        estimate = payload.get("estimate", "?")
        full_payload = json.dumps(payload)
        t.add_row(str(signal_type), str(estimate), full_payload, str(captured_at))

    console.print(t)
    console.print("\n[dim]Custom probe signals use a Python callable to compute arbitrary metrics "
                  "at runtime; results are stored in probe_signals with signal_type=custom.[/dim]")


if __name__ == "__main__":
    main()
