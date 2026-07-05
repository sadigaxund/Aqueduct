import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    path = "data/output/output.parquet"
    if not os.path.exists(path):
        console.print("[bold red]✗[/bold red] Output not found — did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Funnel merge complete! Gaps filled.\n")
    df = pd.read_parquet(path)
    t = Table(title="Merged User Data (Coalesced)", header_style="bold cyan")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)}[/dim]")
    console.print("[dim]  Note: User 2's email was NULL in primary but was filled from the fallback stream.[/dim]")
    console.print("\n[dim]Funnel op=coalesce picks the first non-null value from multiple input streams.[/dim]")

if __name__ == "__main__":
    main()
