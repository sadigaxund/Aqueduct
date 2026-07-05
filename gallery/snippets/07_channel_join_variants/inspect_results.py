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
    t = Table(title="Enriched Orders (LEFT JOIN)", header_style="bold cyan")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.head(10).iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)} — order 3 has no user match (name=NaN)[/dim]")
    console.print("\n[dim]Channel op=join with how=left merges orders and users on user_id.[/dim]")

if __name__ == "__main__":
    main()
