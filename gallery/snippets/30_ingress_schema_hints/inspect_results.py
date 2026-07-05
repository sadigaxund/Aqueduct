import duckdb
import json
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    db_path = Path(".aqueduct/observability.db")
    output = Path("data/output/products.parquet")

    if output.exists():
        con = duckdb.connect()
        try:
            rows = con.execute(f"SELECT * FROM read_parquet('{output}') ORDER BY id").fetchall()
            columns = [desc[0] for desc in con.description]
            t = Table(title="Products (schema_hint applied)", header_style="bold cyan")
            for col in columns:
                t.add_column(col)
            for row in rows:
                t.add_row(*[str(v) for v in row])
            console.print(t)
            console.print(f"  [dim]Row count: {len(rows)} — schema_hint overrides Spark's inferred types[/dim]\n")
        finally:
            con.close()

    if db_path.exists():
        con = duckdb.connect(str(db_path))
        try:
            row = con.execute(
                "SELECT payload FROM probe_signals WHERE signal_type = 'schema_snapshot' LIMIT 1"
            ).fetchone()
            if row:
                payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                fields = payload.get("fields", [])
                t = Table(title="Schema Snapshot (after hints)", header_style="bold green")
                t.add_column("Column")
                t.add_column("Type")
                for f in fields:
                    t.add_row(f.get("name", "?"), f.get("type", "?"))
                console.print(t)
                console.print("  [dim]Schema hints validate: id→string (SKU prefix preserved), price→double, quantity→integer, listed_at→date[/dim]")
        finally:
            con.close()
    else:
        console.print(f"[bold yellow]⚠[/bold yellow] Observability DB not found at {db_path}.")


if __name__ == "__main__":
    main()
