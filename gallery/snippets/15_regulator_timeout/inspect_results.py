import time
import subprocess
from rich.console import Console
import os

console = Console()

def main():
    print("Running pipeline (expecting a ~10s wait due to Regulator timeout)...")
    
    start_time = time.time()
    result = subprocess.run(["aqueduct", "run", "blueprint.yml"], capture_output=True, text=True)
    end_time = time.time()
    
    duration = end_time - start_time
    
    if result.returncode == 0:
        console.print(f"[bold green]✓[/bold green] Pipeline finished in {duration:.1f} seconds.")
        if duration >= 10:
            console.print("[bold cyan]Success:[/bold cyan] The Regulator correctly waited for the timeout!")
        else:
            console.print("[bold yellow]Warning:[/bold yellow] The pipeline finished faster than expected.")
    else:
        console.print("[bold red]Error:[/bold red] Pipeline failed!")
        print(result.stderr)

    if os.path.exists("data/output/output.parquet"):
        console.print("✓ Output data was successfully written after the timeout.")
    else:
        console.print("✗ Output data missing.")

if __name__ == "__main__":
    main()
