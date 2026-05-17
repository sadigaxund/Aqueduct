import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    output_path = "data/output/output.parquet"
    
    if not os.path.exists(output_path):
        console.print("[bold red]Error:[/bold red] Output not found. The pipeline might have failed its assertion!")
        return

    df = pd.read_parquet(output_path)
    console.print(f"[bold green]✓[/bold green] Assertion Passed! Data saved to output.")
    
    table = Table(title="Validated User Data (Sample)")
    for col in df.columns:
        table.add_column(col)
    
    # Show first 5 rows
    for _, row in df.head(5).iterrows():
        table.add_row(*[str(val) for val in row])
    
    console.print(table)
    
    null_count = df['email'].isna().sum()
    null_rate = null_count / len(df)
    console.print(f"\n[dim]Actual full-set Null Rate for 'email': {null_rate:.2%}[/dim]")

if __name__ == "__main__":
    main()
