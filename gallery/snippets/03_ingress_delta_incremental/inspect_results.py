import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    path = "data/output/output.parquet"
    if not os.path.exists(path):
        console.print(f"[bold red]Error:[/bold red] {path} not found. Did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Found results. Reading...")
    df = pd.read_parquet(path)
    
    table = Table(title="Delta Time-Travel Results (Version 2)", header_style="bold magenta")
    for col in df.columns:
        table.add_column(col)
    
    for _, row in df.iterrows():
        table.add_row(*[str(val) for val in row])
        
    console.print(table)
    console.print(f"\n[dim]Note: This shows only the records from version 2 of the source table.[/dim]")

if __name__ == "__main__":
    main()
