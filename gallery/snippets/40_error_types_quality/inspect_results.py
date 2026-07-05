import pandas as pd
from rich.console import Console
from rich.table import Table
import os
import glob

console = Console()

def read_csv_from_dir(path):
    csv_files = glob.glob(os.path.join(path, "part-*.csv"))
    if not csv_files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

def main():
    clean_path = "data/output/clean.parquet"
    missing_path = "data/output/missing_values.csv"
    quality_path = "data/output/quality_violations.csv"

    if not os.path.exists(clean_path):
        console.print("[bold red]✗[/bold red] Run 'aqueduct run blueprint.yml' first.")
        return

    df_clean = pd.read_parquet(clean_path)
    t = Table(title="Clean Records (main port)", header_style="bold green")
    for c in df_clean.columns:
        t.add_column(c)
    for _, r in df_clean.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_clean)}\n[/dim]")

    df_missing = read_csv_from_dir(missing_path)
    t = Table(title="Missing Values (error_type=MissingValue)", header_style="bold yellow")
    if not df_missing.empty:
        for c in df_missing.columns:
            t.add_column(c)
        for _, r in df_missing.iterrows():
            t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_missing)}\n[/dim]")

    df_quality = read_csv_from_dir(quality_path)
    t = Table(title="Quality Violations (error_type=DataQualityViolation)", header_style="bold red")
    if not df_quality.empty:
        for c in df_quality.columns:
            t.add_column(c)
        for _, r in df_quality.iterrows():
            t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df_quality)}\n[/dim]")

    console.print(f"[bold]Summary:[/bold] {len(df_clean)} clean, {len(df_missing)} missing values, "
                  f"{len(df_quality)} quality violations")
    console.print("[dim]Typed spillway edges route failed rows to different outputs based on error_type "
                  "(MissingValue vs DataQualityViolation).[/dim]")

if __name__ == "__main__":
    main()
