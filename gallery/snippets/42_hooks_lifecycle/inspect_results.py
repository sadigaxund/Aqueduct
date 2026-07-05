import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def _show(path, title):
    if not os.path.exists(path):
        console.print(f"[bold red]✗[/bold red] {title} not found at {path}")
        return
    df = pd.read_parquet(path)
    t = Table(title=title, header_style="bold green")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)
    console.print(f"[dim]  Row count: {len(df)}[/dim]\n")

def main():
    _show("data/output/result.parquet", "hooks_demo Output")
    # chain_target.yml's own path is relative to ITS directory (FsPath
    # anchoring), not the parent blueprint's — so the chained output lands
    # under chained/data/output/, not data/output/.
    _show("chained/data/output/chain_result.parquet", "Chained hooks_chain_target Output")

    signal = "data/output/hook_signal_ok.txt"
    if os.path.exists(signal):
        console.print("[bold green]✓[/bold green] on_success command hooks ran (hook_signal_ok.txt present)")
    else:
        console.print("[bold red]✗[/bold red] hook_signal_ok.txt missing — did on_success hooks run?")

if __name__ == "__main__":
    main()
