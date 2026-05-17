import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    active_path = "data/output/active_tickets.parquet"
    other_path = "data/output/other_tickets.parquet"
    
    if not os.path.exists(active_path) or not os.path.exists(other_path):
        console.print("[bold red]Error:[/bold red] Outputs not found.")
        return

    # Active Branch
    df_active = pd.read_parquet(active_path)
    table_active = Table(title="Active Tickets (NEW/PENDING)", header_style="bold green")
    for col in df_active.columns:
        table_active.add_column(col)
    for _, row in df_active.iterrows():
        table_active.add_row(*[str(val) for val in row])
    
    # Other Branch
    df_other = pd.read_parquet(other_path)
    table_other = Table(title="Other Tickets (Else Branch)", header_style="bold yellow")
    for col in df_other.columns:
        table_other.add_column(col)
    for _, row in df_other.iterrows():
        table_other.add_row(*[str(val) for val in row])
    
    console.print(table_active)
    console.print(table_other)
    
    console.print(f"\n[dim]Summary: {len(df_active)} active, {len(df_other)} other.[/dim]")

if __name__ == "__main__":
    main()
