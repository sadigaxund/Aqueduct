import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    partitioned_dir = Path("data/output/partitioned_txns")
    us_output = Path("data/output/us_txns.parquet")

    con = duckdb.connect()
    try:
        if partitioned_dir.is_dir():
            partition_files = list(partitioned_dir.rglob("*.parquet"))
            if partition_files:
                rows = con.execute(f"SELECT month, region, COUNT(*) AS cnt FROM read_parquet('{partitioned_dir}/**/*.parquet') GROUP BY month, region ORDER BY month, region").fetchall()
                t = Table(title="Partitioned Transactions (by month)", header_style="bold cyan")
                t.add_column("month")
                t.add_column("region")
                t.add_column("count")
                for row in rows:
                    t.add_row(*[str(v) for v in row])
                console.print(t)
                console.print(f"  [dim]Data written with partition_by: ['month'] — {len(partition_files)} partition files[/dim]\n")

        if us_output.exists():
            rows = con.execute(f"SELECT * FROM read_parquet('{us_output}') ORDER BY tx_id").fetchall()
            columns = [desc[0] for desc in con.description]
            t = Table(title="US Transactions (partition_filter applied)", header_style="bold cyan")
            for col in columns:
                t.add_column(col)
            for row in rows:
                t.add_row(*[str(v) for v in row])
            console.print(t)
            console.print(f"  [dim]Row count: {len(rows)} — partition_filters: region = 'US' filtered results at read time[/dim]")
        else:
            console.print(f"[bold red]✗[/bold red] Output not found at {us_output}.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
