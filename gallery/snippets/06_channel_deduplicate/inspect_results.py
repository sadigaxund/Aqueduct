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

    console.print(f"[bold green]✓[/bold green] Found results in {path}. Reading...\n")
    df = pd.read_parquet(path)
    t = Table(title="Deduplicated User Records (Latest Only)", header_style="bold cyan")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.head(10).iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)} — duplicates removed, latest row per user kept[/dim]")
    console.print("\n[dim]Channel op=deduplicate keeps only the most recent row per key column(s).[/dim]")

if __name__ == "__main__":
    main()
