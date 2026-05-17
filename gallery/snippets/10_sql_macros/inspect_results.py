import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    output_path = "data/output/output.parquet"
    if not os.path.exists(output_path):
        console.print(f"[bold red]Error:[/bold red] {output_path} not found. Did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Found results in {output_path}. Reading...")
    df = pd.read_parquet(output_path)
    table = Table(title="Active Orders Since 2026 (macro-filtered)", header_style="bold magenta")
    for col in df.columns:
        table.add_column(col)
    for _, row in df.head(10).iterrows():
        table.add_row(*[str(val) for val in row])
    console.print(table)
    console.print(f"\n[dim]Rows: {len(df)} — expect order_id 1 and 5 only[/dim]")

if __name__ == "__main__":
    main()
