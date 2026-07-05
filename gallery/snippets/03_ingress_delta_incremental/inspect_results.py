import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    path = "data/output/output.parquet"
    if not os.path.exists(path):
        console.print(f"[bold red]✗[/bold red] {path} not found — did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Found results. Reading...\n")
    df = pd.read_parquet(path)
    t = Table(title="Delta Time-Travel (Version 2)", header_style="bold cyan")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)}[/dim]")
    console.print("\n[dim]versionAsOf: 2 reads an older snapshot of the Delta table, before new rows were appended.[/dim]")

if __name__ == "__main__":
    main()
