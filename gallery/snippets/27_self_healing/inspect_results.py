import duckdb
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def main():
    output = Path("data/output/orders.parquet")

    if not output.exists():
        console.print("[bold yellow]⚠[/bold yellow] Output not found. The pipeline failed on purpose (wrong column 'total').")
        console.print("  Run [bold]aqueduct heal blueprint.yml[/bold] to trigger self-healing.")
        console.print("\n  [dim]The agent analyses the failure and proposes a fix (total → total_amt).[/dim]")
        console.print("  Then apply with [bold]aqueduct patch commit[/bold].")
        return

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT * FROM read_parquet('{output}') ORDER BY order_id").fetchall()
        columns = [desc[0] for desc in con.description]

        t = Table(title="Orders (after self-healing)", header_style="bold cyan")
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(v) for v in row])
        console.print(t)
        console.print(f"\n[dim]Row count: {len(rows)} — self-healing agent fixed the column name (total → total_amt)[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
