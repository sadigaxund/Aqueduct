import time
import subprocess
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    console.print("[bold cyan]·[/bold cyan] Running pipeline (expecting ~10s wait due to Regulator timeout)...\n")

    start_time = time.time()
    result = subprocess.run(["aqueduct", "run", "blueprint.yml"], capture_output=True, text=True)
    duration = time.time() - start_time

    if result.returncode == 0:
        console.print(f"[bold green]✓[/bold green] Pipeline finished in {duration:.1f} seconds.")
        if duration >= 10:
            console.print("[bold cyan]✓[/bold cyan] The Regulator correctly waited for the configured timeout!")
        else:
            console.print("[bold yellow]⚠[/bold yellow] Pipeline finished faster than expected — check the timeout setting.")
    else:
        console.print("[bold red]✗[/bold red] Pipeline failed!")
        console.print(result.stderr)

    if os.path.exists("data/output/output.parquet"):
        df = pd.read_parquet("data/output/output.parquet")
        t = Table(title="Regulator Output", header_style="bold cyan")
        for c in df.columns:
            t.add_column(c)
        for _, r in df.iterrows():
            t.add_row(*[str(v) for v in r])
        console.print(t)
        console.print(f"[dim]  Row count: {len(df)}[/dim]")
        console.print("\n[dim]The Regulator delayed execution by the configured duration, then the pipeline completed normally.[/dim]")
    else:
        console.print("[bold red]✗[/bold red] Output data missing.")

if __name__ == "__main__":
    import pandas as pd
    main()
