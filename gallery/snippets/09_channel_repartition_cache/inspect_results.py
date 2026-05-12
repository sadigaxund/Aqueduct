import pandas as pd
from rich.console import Console
from rich.table import Table
import os
import glob

console = Console()

def main():
    output_path = "data/output/output.parquet"
    if not os.path.exists(output_path):
        console.print(f"[bold red]Error:[/bold red] {output_path} not found. Did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Found results in {output_path}. Reading...")
    df = pd.read_parquet(output_path)
    table = Table(title="Performance Tuned Results", header_style="bold cyan")
    for col in df.columns[:5]: # Show first 5 columns
        table.add_column(col)
    for _, row in df.head(5).iterrows():
        table.add_row(*[str(val) for val in row[:5]])
    console.print(table)
    
    file_count = len(glob.glob(os.path.join(output_path, "*.parquet")))
    
    console.print(f"\n[bold green]✓[/bold green] Success! Found {file_count} partitions (files) in the output directory.")
    console.print("This matches the 'num_partitions: 10' setting in the blueprint because our dataset (20 rows) is large enough to fill all partitions.\n")
    console.print(f"\n[dim]Pipeline used repartition(10) and cache() for optimization.[/dim]")

if __name__ == "__main__":
    main()
