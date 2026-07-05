import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    output = Path("data/output/delta_orders")
    if not output.is_dir():
        console.print(f"[bold red]✗[/bold red] Delta output not found at {output}. Did you run the pipeline?")
        console.print("  [dim]Delta requires Delta Lake JARs — see aqueduct.yml for DELTA_SPARK_VER / DELTA_VER.[/dim]")
        return

    parquet_files = list(output.rglob("*.parquet"))
    if not parquet_files:
        console.print(f"[bold yellow]⚠[/bold yellow] No Parquet files found in {output}.")
        return

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM delta_scan('{output}') ORDER BY order_id").fetchall()
        columns = [desc[0] for desc in con.description]

        t = Table(title="Delta MERGE Result", header_style="bold cyan")
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
        console.print(f"\n[dim]Row count: {len(rows)} — Delta MERGE (order_id=1002 updated, 1004 inserted; 1001, 1003 unchanged)[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
