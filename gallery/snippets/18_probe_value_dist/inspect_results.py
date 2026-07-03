import duckdb
import json
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    db_path = ".aqueduct/observability.db"
    if not os.path.exists(db_path):
        console.print(f"[bold red]✗[/bold red] Observability store not found at {db_path}. Did you run the pipeline?")
        return

    conn = duckdb.connect(db_path)
    try:
        row = conn.execute("""
            SELECT payload
            FROM probe_signals
            WHERE signal_type = 'value_distribution'
            ORDER BY captured_at DESC
            LIMIT 1
        """).fetchone()

        if not row:
            console.print("[bold yellow]⚠[/bold yellow] No distribution signal found in store.")
            return

        payload = json.loads(row[0])

        if payload.get("blocked"):
            console.print("[bold yellow]⚠[/bold yellow] Probe was blocked by engine guardrails "
                          "(`danger.allow_full_probe_actions` is false).")
            return

        stats = payload.get("stats", {}).get("price", {})
        console.print("[bold green]✓[/bold green] Captured Price Distribution:\n")

        t = Table(title="Price Statistics (Sampled)", header_style="bold cyan")
        t.add_column("Metric")
        t.add_column("Value")

        t.add_row("Min", f"{stats.get('min'):.2f}")
        t.add_row("Max", f"{stats.get('max'):.2f}")
        t.add_row("Mean", f"{stats.get('mean'):.2f}")
        t.add_row("StdDev", f"{stats.get('stddev'):.2f}")
        t.add_row("Count (Non-Null)", str(stats.get('count_non_null')))
        console.print(t)

        p_table = Table(title="Percentiles", header_style="bold cyan")
        p_table.add_column("Percentile")
        p_table.add_column("Value")
        percentiles = stats.get("percentiles", {})
        for p in sorted(percentiles.keys()):
            p_table.add_row(f"{float(p)*100:.0f}th", f"{percentiles[p]:.2f}")
        console.print(p_table)

        console.print("\n[dim]Probe signal type=value_distribution captures column statistics "
                      "and percentiles without stopping the pipeline.[/dim]")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
