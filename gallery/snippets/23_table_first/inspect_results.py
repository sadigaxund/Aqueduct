import duckdb
from rich.console import Console
from rich.table import Table
import os
from pathlib import Path

console = Console()


def main():
    # Which engine wrote the output determines WHERE it landed: Spark's
    # `table:` egress uses saveAsTable, a directory of part files under
    # spark-warehouse/; DuckDB's `table:` egress creates a table directly
    # inside its own persistent catalog file (demo.duckdb, per
    # aqueduct.yml's engine.duckdb.database_path). populate_tables.py
    # purges both before every run, so at most one of these exists at a
    # time — check Spark's shape first, then fall back to DuckDB's.
    output_dir = Path("spark-warehouse/demo_output")
    duckdb_catalog = Path("demo.duckdb")

    if output_dir.exists():
        # Spark writes a table as a directory of part files; a single-file
        # writer would write one Parquet file at the same path. DuckDB's
        # read_parquet() accepts a directory, a glob, or a single file
        # transparently, so only the "does anything exist yet" check needs
        # both shapes.
        if output_dir.is_dir():
            parquet_files = list(output_dir.glob("*.parquet")) + list(output_dir.glob("part-*"))
            if not parquet_files:
                console.print(
                    f"[bold yellow]⚠[/bold yellow] No Parquet files found in {output_dir}."
                )
                return
        con = duckdb.connect()
        try:
            rows = con.execute(f"SELECT * FROM read_parquet('{output_dir}') ORDER BY id").fetchall()
            columns = [desc[0] for desc in con.description]
        finally:
            con.close()
    elif duckdb_catalog.exists():
        con = duckdb.connect(str(duckdb_catalog))
        try:
            rows = con.execute("SELECT * FROM demo_output ORDER BY id").fetchall()
            columns = [desc[0] for desc in con.description]
        finally:
            con.close()
    else:
        console.print(
            f"[bold red]✗[/bold red] Output table not found at {output_dir} or in {duckdb_catalog}. "
            "Did you run the pipeline?"
        )
        return

    if not rows:
        console.print("[bold yellow]⚠[/bold yellow] Output table is empty.")
        return

    t = Table(title="Table-First Output", header_style="bold cyan")
    for col in columns:
        t.add_column(col)

    for row in rows:
        t.add_row(*[str(v) for v in row])

    console.print(t)
    console.print(
        f"\n[dim]Row count: {len(rows)} — read via table: demo_table, written via table: demo_output[/dim]"
    )


if __name__ == "__main__":
    main()
