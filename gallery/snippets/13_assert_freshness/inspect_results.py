import pandas as pd
from rich.console import Console
from rich.table import Table
import os
import glob
from datetime import datetime, timezone

console = Console()

def main():
    fresh_path = "data/output/fresh_events.parquet"
    stale_path = "data/output/stale_events.csv"
    
    if not os.path.exists(fresh_path):
        console.print("[bold red]Error:[/bold red] Fresh output not found. The global SLA might have failed!")
        return

    # Fresh Table
    df_fresh = pd.read_parquet(fresh_path)
    table_fresh = Table(title="Fresh Events (Main Stream)", header_style="bold green")
    for col in df_fresh.columns:
        table_fresh.add_column(col)
    for _, row in df_fresh.iterrows():
        table_fresh.add_row(*[str(val) for val in row])
    
    # Stale Table (Spillway)
    csv_files = glob.glob(os.path.join(stale_path, "part-*.csv"))
    if not csv_files:
        df_stale = pd.DataFrame()
    else:
        df_stale = pd.concat([pd.read_csv(f) for f in csv_files])
    
    table_stale = Table(title="Stale Events (Quarantined to Spillway)", header_style="bold yellow")
    if not df_stale.empty:
        for col in df_stale.columns:
            table_stale.add_column(col)
        for _, row in df_stale.iterrows():
            table_stale.add_row(*[str(val) for val in row])
    
    console.print(table_fresh)
    console.print(table_stale)
    
    if not df_fresh.empty:
        latest_ts = pd.to_datetime(df_fresh['processed_at']).max()
        age = datetime.now(tz=timezone.utc) - latest_ts.to_pydatetime().replace(tzinfo=timezone.utc)
        console.print(f"\n[dim]Latest fresh record is {age.total_seconds() / 3600:.2f} hours old.[/dim]")

if __name__ == "__main__":
    main()
