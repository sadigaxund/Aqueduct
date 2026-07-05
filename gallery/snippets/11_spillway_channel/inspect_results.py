import pandas as pd
from rich.console import Console
from rich.table import Table
import os
import glob

console = Console()

def main():
    valid_path = "data/output/valid_scores.parquet"
    rejected_path = "data/output/rejected_scores.csv"

    if not os.path.exists(valid_path):
        console.print(f"[bold red]✗[/bold red] {valid_path} not found — did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Found results. Reading both streams...\n")

    df_valid = pd.read_parquet(valid_path)
    t = Table(title="Valid Scores (Main Stream)", header_style="bold green")
    for c in df_valid.columns:
        t.add_column(c)
    for _, r in df_valid.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_valid)}[/dim]\n")

    csv_files = glob.glob(os.path.join(rejected_path, "part-*.csv"))
    df_bad = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True) if csv_files else pd.DataFrame()
    t = Table(title="Rejected Scores (Spillway Stream)", header_style="bold red")
    if not df_bad.empty:
        for c in df_bad.columns:
            t.add_column(c)
        for _, r in df_bad.iterrows():
            t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_bad)}[/dim]\n")

    console.print(f"[dim]Summary: {len(df_valid)} valid, {len(df_bad)} rejected. "
                  "The spillway splits rows that fail the condition into a separate stream.[/dim]")

if __name__ == "__main__":
    main()
