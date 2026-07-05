import pandas as pd
from rich.console import Console
from rich.table import Table
import os

console = Console()

def main():
    out_path = "data/output/enriched.parquet"

    if not os.path.exists(out_path):
        # Expected in the default run: premium_enrich is disabled
        # (AQ_PREMIUM unset), and that disable cascades transitively through
        # merged_output (Funnel) and output (Egress) — neither ever runs, so
        # no parquet is written. This is the feature working correctly, not
        # a failure. Re-run with AQ_PREMIUM=true to see the full cascade.
        console.print(
            "[bold yellow]⏭[/bold yellow] No output written — expected when "
            "premium_enrich is disabled (default): the skip cascades through "
            "merged_output and output too. Re-run with AQ_PREMIUM=true to "
            "see the full pipeline execute."
        )
        return

    df = pd.read_parquet(out_path)
    t = Table(title="Enriched Orders", header_style="bold green")
    for c in df.columns:
        t.add_column(c)
    for _, r in df.iterrows():
        t.add_row(*[str(v) for v in r])
    console.print(t)

    tiers = df["tier"].value_counts().to_dict()
    console.print(f"\n[dim]Row count: {len(df)}  ·  tiers: {tiers}[/dim]")
    console.print(
        "[dim]Set AQ_PREMIUM=true and re-run to see premium_enrich cascade "
        "back in — its rows use a 5% tax rate and a '-gold' segment suffix "
        "instead of the disabled default (8% tax, 'standard' segment).[/dim]"
    )

if __name__ == "__main__":
    main()
