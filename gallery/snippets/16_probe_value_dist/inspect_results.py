import duckdb
import json
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    # Look in the default .aqueduct directory
    db_path = ".aqueduct/obs/probe_dist_demo/obs.db"
    
    if not os.path.exists(db_path):
        console.print("[bold red]Error:[/bold red] Observability store not found. Did you run the pipeline?")
        return

    conn = duckdb.connect(db_path)
    try:
        # Get the latest value_distribution signal
        row = conn.execute("""
            SELECT payload 
            FROM probe_signals 
            WHERE signal_type = 'value_distribution' 
            ORDER BY captured_at DESC 
            LIMIT 1
        """).fetchone()
        
        if not row:
            console.print("[bold red]Error:[/bold red] No distribution signal found in store.")
            return

        payload = json.loads(row[0])
        
        if payload.get("blocked"):
            console.print("[bold yellow]Warning:[/bold yellow] Probe was blocked by engine guardrails (`danger.allow_full_probe_actions` is false).")
            console.print("Make sure you have an `aqueduct.yml` file in this directory that enables it.")
            return

        stats = payload.get("stats", {}).get("price", {})
        
        console.print(f"[bold green]✓[/bold green] Captured Price Distribution:")
        
        table = Table(title="Price Statistics (Sampled)")
        table.add_column("Metric")
        table.add_column("Value")
        
        table.add_row("Min", f"{stats.get('min'):.2f}")
        table.add_row("Max", f"{stats.get('max'):.2f}")
        table.add_row("Mean", f"{stats.get('mean'):.2f}")
        table.add_row("StdDev", f"{stats.get('stddev'):.2f}")
        table.add_row("Count (Non-Null)", str(stats.get('count_non_null')))
        
        console.print(table)
        
        # Percentiles
        p_table = Table(title="Percentiles")
        p_table.add_column("Percentile")
        p_table.add_column("Value")
        
        percentiles = stats.get("percentiles", {})
        for p in sorted(percentiles.keys()):
            p_table.add_row(f"{float(p)*100:.0f}th", f"{percentiles[p]:.2f}")
            
        console.print(p_table)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
