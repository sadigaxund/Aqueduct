import duckdb
import json
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    output = Path("data/output/products_in_stock.parquet")
    db_path = Path(".aqueduct/observability.db")

    if output.exists():
        con = duckdb.connect()
        try:
            rows = con.execute(f"SELECT * FROM read_parquet('{output}') ORDER BY id").fetchall()
            columns = [desc[0] for desc in con.description]
            t = Table(title="In-Stock Products (register_as_table)", header_style="bold cyan")
            for col in columns:
                t.add_column(col)
            for row in rows:
                t.add_row(*[str(v) for v in row])
            console.print(t)
            console.print(f"  [dim]Row count: {len(rows)} — saved to parquet AND registered as 'in_stock_products' in the session catalog[/dim]\n")
        finally:
            con.close()

    if db_path.exists():
        con = duckdb.connect(str(db_path))
        try:
            row = con.execute(
                "SELECT payload FROM probe_signals WHERE signal_type = 'row_count_estimate' LIMIT 1"
            ).fetchone()
            if row:
                payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                estimate = payload.get("estimate", "?")
                console.print(f"[bold green]✓[/bold green] Readback probe confirms {estimate} rows at 'in_stock_products' table")
        finally:
            con.close()
    else:
        console.print(f"[bold yellow]⚠[/bold yellow] Observability DB not found at {db_path}.")


if __name__ == "__main__":
    main()
