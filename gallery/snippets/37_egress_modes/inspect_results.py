import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

MODES = [
    ("Append", "data/output/modes_append/"),
    ("Error", "data/output/modes_error/"),
    ("Ignore", "data/output/modes_ignore/"),
    ("Overwrite Partitions", "data/output/modes_partitioned/"),
    ("Replace Where", "data/output/modes_replace/"),
]

def read_parquet_dir(path: str) -> pd.DataFrame:
    if not os.path.isdir(path):
        return pd.DataFrame()
    parts = [os.path.join(path, f) for f in os.listdir(path)
             if f.endswith(".parquet") or os.path.isdir(os.path.join(path, f))]
    if not parts:
        return pd.DataFrame()
    dfs = []
    for p in parts:
        if os.path.isdir(p):
            dfs.append(read_parquet_dir(p))
        elif p.endswith(".parquet"):
            dfs.append(pd.read_parquet(p))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def main():
    for label, path in MODES:
        df = read_parquet_dir(path)
        if df.empty:
            console.print(f"[bold yellow]⚠[/bold yellow] {label}: no data found at {path}")
            continue
        t = Table(title=f"Mode: {label}", header_style="bold cyan")
        for c in df.columns:
            t.add_column(c)
        for _, r in df.iterrows():
            t.add_row(*[str(v) for v in r])
        console.print(t)
        console.print(f"[dim]  Row count: {len(df)}[/dim]\n")

    console.print("[bold]What to notice:[/bold]")
    console.print("  • Append: adds rows to existing data")
    console.print("  • Error: fails if the target exists (protected)")
    console.print("  • Ignore: silently skips if target exists")
    console.print("  • Overwrite Partition: replaces only matching partitions")
    console.print("  • Replace Where: replaces rows matching a condition")

if __name__ == "__main__":
    main()
