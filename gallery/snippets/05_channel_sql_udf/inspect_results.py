import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def show_table(path, title, note):
    if not os.path.exists(path):
        console.print(f"[bold red]✗[/bold red] {path} not found — did you run 'aqueduct run blueprint.yml'?")
        return
    console.print(f"[bold green]✓[/bold green] Found results in {path}\n")
    df = pd.read_parquet(path)
    t = Table(title=title, header_style="bold cyan")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.head(10).iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)}[/dim]")
    console.print(f"\n[dim]{note}[/dim]")

def main():
    show_table(
        "data/output/output.parquet",
        "Masked User Data (Static UDF)",
        "The static UDF masks email addresses using a function registered as `entry: mask_email`."
    )
    console.print()
    show_table(
        "data/output/phone_masked.parquet",
        "Phone Masked Data (Parameterized UDF)",
        "The parameterized UDF uses a factory (make_phone_masker) called with params: visible_digits=4,"
        " prefix='+1-***-***-'. The factory returns a closure that keeps only the last 4 digits."
    )

if __name__ == "__main__":
    main()
