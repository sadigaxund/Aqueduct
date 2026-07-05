import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    out_path = "data/output/result.parquet"

    if not os.path.exists(out_path):
        console.print("[bold red]✗[/bold red] Output not found — did you run 'aqueduct run blueprint.yml'?")
        return

    df = pd.read_parquet(out_path)
    t = Table(title="Orders (Egress passthrough)", header_style="bold green")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)}[/dim]\n")
    console.print(
        "[dim]The interesting output is above the table, in the run's own "
        "terminal summary — each Probe's `report: stdout` prints its signal "
        "result inline under its row (row count, schema, null rates, sample "
        "rows, distinct counts, value distribution). No data store needed "
        "to inspect a Probe result.[/dim]"
    )

if __name__ == "__main__":
    main()
