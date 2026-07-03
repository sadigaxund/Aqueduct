import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    product_output = Path("data/output/products.parquet")
    customer_output = Path("data/output/customers.parquet")

    if not product_output.exists() and not customer_output.exists():
        console.print(f"[bold red]✗[/bold red] No output files found. Did you run the pipeline?")
        console.print("  [dim]This snippet requires a Java UDF JAR at udf/jars/aqueduct-udfs.jar — replace with your compiled JAR.[/dim]")
        return

    con = duckdb.connect()
    try:
        if product_output.exists():
            rows = con.execute(f"SELECT * FROM read_parquet('{product_output}') ORDER BY id").fetchall()
            columns = [desc[0] for desc in con.description]
            t = Table(title="Products (Parallel Component A)", header_style="bold cyan")
            for col in columns:
                t.add_column(col)
            for row in rows:
                t.add_row(*[str(v) for v in row])
            console.print(t)
            console.print(f"  [dim]Row count: {len(rows)} — Java UDF enrichment runs in parallel with Component B[/dim]\n")

        if customer_output.exists():
            rows = con.execute(f"SELECT * FROM read_parquet('{customer_output}') ORDER BY customer_id").fetchall()
            columns = [desc[0] for desc in con.description]
            t = Table(title="Customers (Parallel Component B)", header_style="bold cyan")
            for col in columns:
                t.add_column(col)
            for row in rows:
                t.add_row(*[str(v) for v in row])
            console.print(t)
            console.print(f"  [dim]Row count: {len(rows)} — runs concurrently with Component A[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
