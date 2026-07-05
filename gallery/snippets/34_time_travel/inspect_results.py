import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    v1_output = Path("data/output/v1_snapshot.parquet")
    current_output = Path("data/output/current_snapshot.parquet")

    if not v1_output.exists() and not current_output.exists():
        console.print(f"[bold red]✗[/bold red] No output files found. Did you run the pipeline?")
        console.print("  [dim]This snippet requires a Delta table at data/delta_events — see populate.py.[/dim]")
        return

    con = duckdb.connect()
    try:
        if v1_output.exists():
            rows = con.execute(f"SELECT * FROM read_parquet('{v1_output}') ORDER BY id").fetchall()
            columns = [desc[0] for desc in con.description]
            t = Table(title="V1 Snapshot (time_travel version=1)", header_style="bold cyan")
            for col in columns:
                t.add_column(col)
            for row in rows:
                t.add_row(*[str(v) for v in row])
            console.print(t)
            console.print(f"  [dim]Row count: {len(rows)} — Delta version 1 snapshot[/dim]\n")

        if current_output.exists():
            rows = con.execute(f"SELECT * FROM read_parquet('{current_output}') ORDER BY id").fetchall()
            columns = [desc[0] for desc in con.description]
            t = Table(title="Current Snapshot (latest)", header_style="bold cyan")
            for col in columns:
                t.add_column(col)
            for row in rows:
                t.add_row(*[str(v) for v in row])
            console.print(t)
            console.print(f"  [dim]Row count: {len(rows)} — latest Delta version[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
