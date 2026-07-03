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
        console.print("[bold red]✗[/bold red] Fresh output not found — the global SLA may have failed!")
        return

    df_fresh = pd.read_parquet(fresh_path)
    t = Table(title="Fresh Events (Main Stream)", header_style="bold green")
    for c in df_fresh.columns:
        t.add_column(c)
    for _, r in df_fresh.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_fresh)}[/dim]\n")

    csv_files = glob.glob(os.path.join(stale_path, "part-*.csv"))
    df_stale = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True) if csv_files else pd.DataFrame()
    t = Table(title="Stale Events (Quarantined to Spillway)", header_style="bold yellow")
    if not df_stale.empty:
        for c in df_stale.columns:
            t.add_column(c)
        for _, r in df_stale.iterrows():
            t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_stale)}[/dim]\n")

    if not df_fresh.empty:
        latest_ts = pd.to_datetime(df_fresh['processed_at']).max()
        age = datetime.now(tz=timezone.utc) - latest_ts.to_pydatetime().replace(tzinfo=timezone.utc)
        console.print(f"[dim]Latest fresh record is {age.total_seconds() / 3600:.2f} hours old "
                      f"(max_age_hours=12).[/dim]")
    console.print("\n[dim]Assert type=freshness quarantines rows older than max_age_hours "
                  "to the spillway; fresh rows pass through.[/dim]")

if __name__ == "__main__":
    main()
