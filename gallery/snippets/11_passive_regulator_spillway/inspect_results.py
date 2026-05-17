import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    valid_path = "data/output/valid_scores.parquet"
    rejected_path = "data/output/rejected_scores.csv"
    
    if not os.path.exists(valid_path):
        console.print(f"[bold red]Error:[/bold red] {valid_path} not found. Did you run 'aqueduct run blueprint.yml'?")
        return

    console.print(f"[bold green]✓[/bold green] Found results. Reading both streams...")
    
    # Valid Records
    df_valid = pd.read_parquet(valid_path)
    table_valid = Table(title="Valid Scores (Main Stream)", header_style="bold green")
    for col in df_valid.columns:
        table_valid.add_column(col)
    for _, row in df_valid.iterrows():
        table_valid.add_row(*[str(val) for val in row])
    
    # Rejected Records (Spark writes CSV as a directory of part-files)
    import glob
    csv_files = glob.glob(os.path.join(rejected_path, "part-*.csv"))
    if not csv_files:
        df_bad = pd.DataFrame(columns=df_valid.columns)
    else:
        df_bad = pd.concat([pd.read_csv(f) for f in csv_files])

    table_bad = Table(title="Rejected Scores (Spillway Stream)", header_style="bold red")
    for col in df_bad.columns:
        table_bad.add_column(col)
    for _, row in df_bad.iterrows():
        table_bad.add_row(*[str(val) for val in row])
        
    console.print(table_valid)
    console.print(table_bad)
    
    console.print(f"\n[dim]Summary: {len(df_valid)} valid, {len(df_bad)} rejected.[/dim]")

if __name__ == "__main__":
    main()
