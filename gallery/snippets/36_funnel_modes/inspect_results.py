import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    paths = {
        "Union All (7 rows)": "data/output/spend_all.parquet",
        "Union Dedup (6 rows)": "data/output/spend_union.parquet",
        "Zip (3 rows)": "data/output/spend_zip.parquet",
    }

    for _, path in paths.items():
        if not os.path.exists(path):
            console.print(f"[bold red]✗[/bold red] {path} not found — did you run 'aqueduct run blueprint.yml'?")
            return

    for title, path in paths.items():
        df = pd.read_parquet(path)
        t = Table(title=title, header_style="bold cyan")
        for c in df.columns:
            t.add_column(c)
        for _, r in df.iterrows():
            t.add_row(*[str(v) for v in r])
        console.print(t)
        console.print(f"[dim]  Row count: {len(df)}[/dim]\n")

    console.print("[bold]What to notice:[/bold]")
    console.print("  • Union All: all 7 rows, duplicates included")
    console.print("  • Union: 6 rows, duplicate order_id 1003 removed")
    console.print("  • Zip: 3 rows, interleaved by position (not by key)")

if __name__ == "__main__":
    main()
