import duckdb
import os
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    output = Path("data/output/users.parquet")
    if not output.exists():
        console.print(f"[bold red]✗[/bold red] Output not found at {output}. Did you run the pipeline?")
        return

    salt = os.environ.get("PIPELINE_SALT", "demo_salt")

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{output}') ORDER BY user_id").fetchall()
        columns = [desc[0] for desc in con.description]

        t = Table(title="Users (email hashed with secret)", header_style="bold cyan")
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
        console.print(f"\n[dim]Row count: {len(rows)} — salt injected: '{salt}' (set PIPELINE_SALT env var to override)[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
