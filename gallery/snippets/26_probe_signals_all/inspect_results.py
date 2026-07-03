import duckdb
import json
from rich.console import Console
from rich.table import Table
from pathlib import Path

console = Console()


def _show_schema_snapshot(payload: dict):
    fields = payload.get("fields", [])
    if not fields:
        console.print("[dim]Schema Snapshot: (no fields captured)[/dim]")
        return
    t = Table(title="Schema Snapshot", header_style="bold blue")
    t.add_column("Column")
    t.add_column("Type")
    t.add_column("Nullable")
    for f in fields:
        t.add_row(f.get("name", "?"), f.get("type", "?"), str(f.get("nullable", "?")))
    console.print(t)


def _show_row_count_estimate(payload: dict):
    console.print(f"[bold blue]Row Count Estimate:[/bold blue] {payload.get('estimate', '?')}"
                  f"  [dim](method={payload.get('method', '?')}, "
                  f"fraction={payload.get('fraction', '?')})[/dim]")


def _show_null_rates(payload: dict):
    rates = payload.get("null_rates", {})
    if not rates:
        console.print("[dim]Null Rates: (none captured)[/dim]")
        return
    t = Table(title="Null Rates", header_style="bold blue")
    t.add_column("Column")
    t.add_column("Null Rate")
    for col, rate in sorted(rates.items()):
        t.add_row(col, f"{rate:.2%}" if isinstance(rate, float) else str(rate))
    console.print(t)


def _show_sample_rows(payload: dict):
    rows = payload.get("rows", [])
    if not rows:
        console.print("[dim]Sample Rows: (none)[/dim]")
        return
    keys = list(rows[0].keys())
    t = Table(title=f"Sample Rows ({len(rows)} rows, requested {payload.get('n', '?')})",
              header_style="bold blue")
    for k in keys:
        t.add_column(str(k))
    for row in rows:
        t.add_row(*[str(row.get(k, "")) for k in keys])
    console.print(t)


def _show_value_distribution(payload: dict):
    stats = payload.get("stats", {})
    if not stats:
        console.print("[dim]Value Distribution: (not computed)[/dim]")
        return
    for col, s in stats.items():
        if s.get("count_non_null", 0) == 0:
            console.print(f"[dim]Value Distribution — {col}: not computed "
                          f"(fraction={payload.get('fraction')}, "
                          f"sample too small)[/dim]")
            continue
        st = Table(title=f"Value Distribution — {col}", header_style="bold blue")
        st.add_column("Metric")
        st.add_column("Value")
        st.add_row("Min", f"{s['min']:.2f}")
        st.add_row("Max", f"{s['max']:.2f}")
        st.add_row("Mean", f"{s['mean']:.2f}")
        st.add_row("StdDev", f"{s['stddev']:.2f}")
        st.add_row("Count (Non-Null)", str(s["count_non_null"]))
        console.print(st)

        pcts = s.get("percentiles", {})
        if pcts:
            pt = Table(title=f"Percentiles — {col}", header_style="bold blue")
            pt.add_column("Percentile")
            pt.add_column("Value")
            for p in sorted(pcts, key=float):
                pt.add_row(f"{float(p)*100:.0f}th", f"{pcts[p]:.2f}")
            console.print(pt)


def _show_distinct_count(payload: dict):
    counts = payload.get("distinct_counts", {})
    if not counts:
        console.print("[dim]Distinct Counts: (not computed)[/dim]")
        return
    all_zero = all(v == 0 for v in counts.values())
    t = Table(title="Distinct Counts", header_style="bold blue")
    t.add_column("Column")
    t.add_column("Distinct Values")
    for col, cnt in sorted(counts.items()):
        t.add_row(col, str(cnt))
    console.print(t)
    if all_zero:
        console.print("[dim]All counts are 0 — sampling fraction "
                      f"{payload.get('fraction')} was too small for this dataset.[/dim]")


def _show_data_freshness(payload: dict):
    t = Table(title="Data Freshness", header_style="bold blue")
    t.add_column("Metric")
    t.add_column("Value")
    t.add_row("Column", str(payload.get("column", "?")))
    t.add_row("Max Value", str(payload.get("max_value", "?")))
    t.add_row("Sampled", str(payload.get("sampled", "?")))
    console.print(t)


def _show_partition_stats(payload: dict):
    num_parts = payload.get("num_partitions", None)
    console.print(f"[bold blue]Partition Stats:[/bold blue] {num_parts} partition(s)"
                  if num_parts else "[dim]Partition Stats: (none)[/dim]")


_SIGNAL_DISPLAY = {
    "schema_snapshot": _show_schema_snapshot,
    "row_count_estimate": _show_row_count_estimate,
    "null_rates": _show_null_rates,
    "sample_rows": _show_sample_rows,
    "value_distribution": _show_value_distribution,
    "distinct_count": _show_distinct_count,
    "data_freshness": _show_data_freshness,
    "partition_stats": _show_partition_stats,
}


def main():
    db_path = Path(".aqueduct/observability.db")
    if not db_path.exists():
        console.print(f"[bold red]✗[/bold red] Observability DB not found at {db_path}. Did you run the pipeline?")
        return

    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute(
            "SELECT signal_type, payload, captured_at FROM probe_signals ORDER BY captured_at"
        ).fetchall()

        if not rows:
            console.print("[bold yellow]⚠[/bold yellow] No probe signals found.")
            return

        summary = Table(title="Probe Signals Collected", header_style="bold cyan")
        summary.add_column("Signal Type")
        summary.add_column("Captured At")
        for signal_type, _, captured_at in rows:
            summary.add_row(str(signal_type), str(captured_at))
        console.print(summary)
        console.print(f"[dim]Total signals: {len(rows)}[/dim]\n")

        seen = set()
        for signal_type, payload_raw, _ in rows:
            st = str(signal_type)
            if st in seen:
                continue
            seen.add(st)
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else (payload_raw or {})
            console.print(f"[bold cyan]━━━ {st} ━━━[/bold cyan]")
            display_fn = _SIGNAL_DISPLAY.get(st)
            if display_fn:
                display_fn(payload)
            else:
                console.print(json.dumps(payload, indent=2, default=str))
            console.print()

        console.print("[dim]Each signal is collected without stopping the pipeline. "
                      "The payload column stores the full result as a JSON blob.\n"
                      "Signals requiring full data scans (value_distribution, distinct_count) "
                      "may be empty when the sampling fraction is too small.[/dim]")
    finally:
        con.close()


if __name__ == "__main__":
    main()
