import duckdb
from rich.console import Console
from rich.table import Table
import os
from pathlib import Path

console = Console()


def main():
    output_dir = Path("spark-warehouse/demo_output")
    if not output_dir.is_dir():
        console.print(f"[bold red]✗[/bold red] Output table directory not found at {output_dir}. Did you run the pipeline?")
        return

    parquet_files = list(output_dir.glob("*.parquet")) + list(output_dir.glob("part-*"))
    if not parquet_files:
        console.print(f"[bold yellow]⚠[/bold yellow] No Parquet files found in {output_dir}.")
        return

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{output_dir}/*.parquet') ORDER BY id").fetchall()
        columns = [desc[0] for desc in con.description]

        if not rows:
            console.print("[bold yellow]⚠[/bold yellow] Output table is empty.")
            return

        t = Table(title="Table-First Output", header_style="bold cyan")
        for col in columns:
            t.add_column(col)

        for row in rows:
            t.add_row(*[str(v) for v in row])

        console.print(t)
        console.print(f"\n[dim]Row count: {len(rows)} — read via table: demo_table, written via table: demo_output[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
