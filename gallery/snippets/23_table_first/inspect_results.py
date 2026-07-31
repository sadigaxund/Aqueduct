import duckdb
from rich.console import Console
from rich.table import Table
import os
from pathlib import Path

console = Console()


def main():
    # Spark writes a table as a directory of part files; a single-file
    # writer (e.g. DuckDB) would write one Parquet file at the same path.
    # DuckDB's read_parquet() accepts a directory, a glob, or a single file
    # transparently, so only the "does anything exist yet" check needs both
    # shapes.
    output_dir = Path("spark-warehouse/demo_output")
    if output_dir.is_dir():
        parquet_files = list(output_dir.glob("*.parquet")) + list(output_dir.glob("part-*"))
        if not parquet_files:
            console.print(f"[bold yellow]⚠[/bold yellow] No Parquet files found in {output_dir}.")
            return
        read_target = output_dir
    elif output_dir.exists():
        read_target = output_dir
    else:
        console.print(f"[bold red]✗[/bold red] Output table not found at {output_dir}. Did you run the pipeline?")
        return

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{read_target}') ORDER BY id").fetchall()
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
