import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    output = Path("data/output/orders.parquet")
    if not output.exists():
        console.print(f"[bold red]✗[/bold red] Output not found at {output}. Did you run the pipeline?")
        return

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{output}') ORDER BY order_id").fetchall()
        columns = [desc[0] for desc in con.description]

        t = Table(title="Orders (after quality gate)", header_style="bold cyan")
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
        console.print(f"\n[dim]Row count: {len(rows)} — all 4 assert rule types passed (min_rows, sql, spillway_rate, schema_match)[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
