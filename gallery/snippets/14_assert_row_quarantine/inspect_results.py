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
        console.print("[bold red]✗[/bold red] Clean output not found — did you run 'aqueduct run blueprint.yml'?")
        return

    df_clean = pd.read_parquet(clean_path)
    t = Table(title="Clean Orders (Main Stream)", header_style="bold green")
    for c in df_clean.columns:
        t.add_column(c)
    for _, r in df_clean.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_clean)}[/dim]\n")

    csv_files = glob.glob(os.path.join(quarantine_path, "part-*.csv"))
    df_bad = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True) if csv_files else pd.DataFrame()
    t = Table(title="Quarantined Orders (Spillway)", header_style="bold red")
    if not df_bad.empty:
        for c in df_bad.columns:
            t.add_column(c)
        for _, r in df_bad.iterrows():
            t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_bad)}[/dim]\n")

    console.print(f"[dim]Summary: {len(df_clean)} clean, {len(df_bad)} quarantined. "
                  "on_fail=quarantine routes bad rows to the spillway instead of aborting.[/dim]")

if __name__ == "__main__":
    main()
