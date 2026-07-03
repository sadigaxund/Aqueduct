import duckdb
from rich.console import Console
from rich.table import Table
import os
from pathlib import Path

console = Console()


def main():
    output = Path("data/output/US/products.parquet")
    if not output.exists():
        console.print(f"[bold red]✗[/bold red] Output not found at {output}. Did you run the pipeline?")
        return

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{output}') ORDER BY product_id").fetchall()
        columns = [desc[0] for desc in con.description]

        t = Table(title="Products (dev profile)", header_style="bold cyan")
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
        console.print(f"\n[dim]Row count: {len(rows)} — context vars (${{ctx.*}}) resolved at parse time[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
