import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

OUTPUTS = [
    ("Conditional — Active (NEW/PENDING)", "data/output/active_tickets.parquet", "bold green"),
    ("Conditional — Other (Else branch)", "data/output/other_tickets.parquet", "bold yellow"),
    ("Broadcast — Output A", "data/output/broadcast_A.parquet", "bold cyan"),
    ("Broadcast — Output B", "data/output/broadcast_B.parquet", "bold cyan"),
    ("Partition — Output A", "data/output/partition_A.parquet", "bold magenta"),
    ("Partition — Output B", "data/output/partition_B.parquet", "bold magenta"),
]

def main():
    for _, path, _ in OUTPUTS:
        if not os.path.exists(path):
            console.print(f"[bold red]✗[/bold red] {path} not found — did you run 'aqueduct run blueprint.yml'?")
            return

    for title, path, style in OUTPUTS:
        df = pd.read_parquet(path)
        t = Table(title=title, header_style=style)
        for c in df.columns:
            t.add_column(c)
        for _, r in df.iterrows():
            t.add_row(*[str(v) for v in r])
        console.print(t)
        console.print(f"[dim]  Row count: {len(df)}[/dim]\n")

    console.print("[bold]What to notice:[/bold]")
    console.print("  • Conditional: rows split by status (active ≠ other)")
    console.print("  • Broadcast: A and B are identical (all rows go to both)")
    console.print("  • Partition: A has NEW rows, B has PENDING rows (value-based on status)")

if __name__ == "__main__":
    main()
