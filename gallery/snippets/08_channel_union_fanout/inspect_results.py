import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    outputs = [
        ("High (val>15)", "data/output/high.parquet", "bold green"),
        ("Low (val<=15)", "data/output/low.parquet", "bold yellow"),
    ]
    for _, path, _ in outputs:
        if not os.path.exists(path):
            console.print(f"[bold red]✗[/bold red] {path} not found — did you run 'aqueduct run blueprint.yml'?")
            return

    for title, path, style in outputs:
        df = pd.read_parquet(path)
        t = Table(title=title, header_style=style)
        for c in df.columns:
            t.add_column(c, no_wrap=True)
        for _, r in df.head(10).iterrows():
            t.add_row(*[str(v) for v in r])
        console.print(t)
        console.print(f"[dim]  Row count: {len(df)}[/dim]\n")

    console.print("[dim]Fan-out: a single Channel feeds two downstream consumers. "
                  "Each consumer gets the same data, processed independently.[/dim]")

if __name__ == "__main__":
    main()
