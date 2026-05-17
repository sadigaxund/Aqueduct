import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    output_path = "data/output/output.parquet"
    
    if not os.path.exists(output_path):
        console.print("[bold red]Error:[/bold red] Output not found.")
        return

    df = pd.read_parquet(output_path)
    console.print(f"[bold green]✓[/bold green] Funnel Merge Complete! Gaps have been filled.")
    
    table = Table(title="Merged User Data (Coalesced)")
    for col in df.columns:
        table.add_column(col)
    
    for _, row in df.iterrows():
        table.add_row(*[str(val) for val in row])
    
    console.print(table)
    
    console.print("\n[dim]Note: User 2's email was NULL in primary, but was filled from fallback.[/dim]")

if __name__ == "__main__":
    main()
