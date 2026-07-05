import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    path = "data/output/output.parquet"
    if not os.path.exists(path):
        console.print("[bold red]✗[/bold red] Output not found — the pipeline may have failed its assertion!")
        return

    console.print(f"[bold green]✓[/bold green] Assertion passed! Data saved to output.\n")
    df = pd.read_parquet(path)
    t = Table(title="Validated User Data (null_rate check)", header_style="bold cyan")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.head(5).iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)}[/dim]")

    null_count = df['email'].isna().sum()
    null_rate = null_count / len(df)
    console.print(f"[dim]  Actual null rate for 'email': {null_rate:.2%} (threshold was 50%)[/dim]")
    console.print("\n[dim]Assert type=null_rate checks that column nulls stay below a threshold. "
                  "on_fail=abort stops the run if violated.[/dim]")

if __name__ == "__main__":
    main()
