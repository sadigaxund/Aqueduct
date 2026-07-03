import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    output = Path("data/output/items.parquet")
    if not output.exists():
        console.print(f"[bold red]✗[/bold red] Output not found at {output}. Did you run the pipeline?")
        return

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{output}') ORDER BY id").fetchall()
        columns = [desc[0] for desc in con.description]

        t = Table(title="Items (enriched with @aq.* functions)", header_style="bold cyan")
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
        console.print(f"\n[dim]Row count: {len(rows)} — @aq.run.id(), @aq.run.timestamp(), @aq.blueprint.name(), @aq.blueprint.id(), @aq.version() resolved[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
