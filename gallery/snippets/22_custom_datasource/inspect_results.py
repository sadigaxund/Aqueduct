import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    path = "data/output/out.parquet"
    if not os.path.exists(path):
        console.print(f"[bold red]✗[/bold red] {path} not found — did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Custom DataSource read successfully!\n")
    df = pd.read_parquet(path)
    t = Table(title="Custom DataSource Results", header_style="bold cyan")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)} — read via format: custom / datasource.MyDataSource[/dim]")
    console.print("\n[dim]Custom DataSources let you define Spark data readers in Python "
                  "(Spark 4.0+), registered in the udfs block.[/dim]")

if __name__ == "__main__":
    main()
