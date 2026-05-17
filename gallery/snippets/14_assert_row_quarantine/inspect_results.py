import pandas as pd
from rich.console import Console
from rich.table import Table
import os
import glob

console = Console()

def main():
    clean_path = "data/output/clean_orders.parquet"
    quarantine_path = "data/output/quarantined_orders.csv"
    
    if not os.path.exists(clean_path):
        console.print("[bold red]Error:[/bold red] Clean output not found.")
        return

    # Clean Records
    df_clean = pd.read_parquet(clean_path)
    table_clean = Table(title="Clean Orders (Main Stream)", header_style="bold green")
    for col in df_clean.columns:
        table_clean.add_column(col)
    for _, row in df_clean.iterrows():
        table_clean.add_row(*[str(val) for val in row])
    
    # Quarantined Records
    csv_files = glob.glob(os.path.join(quarantine_path, "part-*.csv"))
    if not csv_files:
        df_bad = pd.DataFrame()
    else:
        df_bad = pd.concat([pd.read_csv(f) for f in csv_files])
    
    table_bad = Table(title="Quarantined Orders (Spillway)", header_style="bold red")
    if not df_bad.empty:
        for col in df_bad.columns:
            table_bad.add_column(col)
        for _, row in df_bad.iterrows():
            table_bad.add_row(*[str(val) for val in row])
    
    console.print(table_clean)
    console.print(table_bad)
    
    console.print(f"\n[dim]Summary: {len(df_clean)} passed, {len(df_bad)} quarantined.[/dim]")

if __name__ == "__main__":
    main()
