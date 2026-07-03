"""`observability` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import UTC
from pathlib import Path
from typing import Any

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
    cli,
)
from aqueduct.cli.output import emit
from aqueduct.cli.style import error as _error
from aqueduct.executor.models import ExecutionStatus
from aqueduct.stores.read import open_obs_read  # Phase 69 — backend-aware reads

# ── aqueduct report ───────────────────────────────────────────────────────────

@cli.command()
@click.argument("run_id", required=False, default=None)
@click.option(
    "--store-dir",
    default=None,
    help="Store directory (default: aqueduct.yml or .aqueduct)",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@click.option(
    "--trend",
    "trend_column",
    default=None,
    metavar="COLUMN",
    help="Cross-run quality trend for one column (null-rate + type history) "
         "from probe signals. Blueprint-scoped — pass --blueprint.",
)
@click.option(
    "--blueprint",
    "blueprint_arg",
    default=None,
    metavar="PATH_OR_ID",
    help="Blueprint id or file path. Scopes --trend.",
)
@click.option(
    "--since",
    "since",
    default=None,
    metavar="ISO_DATE",
    help="With --trend: only include signals captured on/after this date "
         "(default: last 30 days).",
)
@click.option(
    "--profile",
    "profile",
    is_flag=True,
    default=False,
    help="Per-module resource profile (duration + I/O) over module_metrics. "
         "With RUN_ID: one run, heaviest module first. With --blueprint (no "
         "RUN_ID): cross-run trend over the last --last runs, flagging slowdowns.",
)
@click.option(
    "--last",
    "last_n",
    default=10,
    show_default=True,
    metavar="N",
    help="With --profile --blueprint: number of recent runs to trend over.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json", "csv", "html"]),
    default="table",
    show_default=True,
    help="Output format. 'html' emits a self-contained run report to stdout "
         "(redirect to a file, e.g. > run.html).",
)
@_env_options
def report(
    run_id: str | None, store_dir: str | None, config_path: str | None,
    trend_column: str | None, blueprint_arg: str | None, since: str | None,
    profile: bool, last_n: int, fmt: str,
    env_file: str | None, cli_env: tuple[str, ...],
) -> None:
    """Print the Flow Report for a completed run, or a cross-run column trend."""
    if profile:
        _report_profile(run_id, blueprint_arg, last_n, store_dir, config_path,
                        fmt, env_file, cli_env)
        return
    if trend_column:
        _report_trend(trend_column, blueprint_arg, since, store_dir, config_path,
                      fmt, env_file, cli_env)
        return
    from aqueduct.cli.style import error as _error
    if not run_id:
        _error("RUN_ID is required (or pass --trend COLUMN --blueprint ID)")
        sys.exit(exit_codes.USAGE_ERROR)
    import csv as _csv
    import io

    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    store = open_obs_read(cfg, store_dir, run_id=run_id)
    if store is None:
        click.echo(
            f"✗ observability store not found for run_id={run_id!r} "
            f"(searched: --store-dir, cfg.stores.observability.path, "
            f"and .aqueduct/observability/*/observability.db)",
            err=True,
        )
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    metrics_rows: list = []
    with store.connect() as cur:
        cur.execute(
            """
            SELECT run_id, blueprint_id, status,
                   CAST(started_at AS VARCHAR),
                   CAST(finished_at AS VARCHAR),
                   module_results
            FROM run_records WHERE run_id = ?
            """,
            [run_id],
        )
        row = cur.fetchone()
        if fmt == "html":
            # Resource profile rows for the HTML report (reuses Phase 62 data).
            try:
                cur.execute(
                    """
                    SELECT module_id, records_written, bytes_written, duration_ms
                    FROM module_metrics WHERE run_id = ?
                    ORDER BY duration_ms DESC NULLS LAST
                    """,
                    [run_id],
                )
                metrics_rows = cur.fetchall()
            except Exception:
                metrics_rows = []  # module_metrics table may not exist yet

    if row is None:
        _error(f"run {run_id!r} not found in the observability store")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    run_id_val, blueprint_id, status, started_at, finished_at, module_results_raw = row
    module_results = json.loads(module_results_raw) if isinstance(module_results_raw, str) else (module_results_raw or [])

    if fmt == "html":
        click.echo(_render_run_html(
            run_id_val, blueprint_id, status, started_at, finished_at,
            module_results, metrics_rows,
        ))
        return

    if fmt == "json":
        emit(
            {
                "run_id": run_id_val,
                "blueprint_id": blueprint_id,
                "status": status,
                "started_at": started_at,
                "finished_at": finished_at,
                "module_results": module_results,
            },
            fmt="json",
        )
        return

    if fmt == "csv":
        buf = io.StringIO()
        writer = _csv.writer(buf)
        writer.writerow(["run_id", "blueprint_id", "status", "started_at", "finished_at"])
        writer.writerow([run_id_val, blueprint_id, status, started_at, finished_at])
        click.echo(buf.getvalue(), nl=False)
        buf2 = io.StringIO()
        writer2 = _csv.writer(buf2)
        writer2.writerow(["module_id", "status", "error"])
        for mr in module_results:
            writer2.writerow([mr.get("module_id", ""), mr.get("status", ""), mr.get("error", "")])
        click.echo(buf2.getvalue(), nl=False)
        return

    # table format
    status_icon = "✓" if status == ExecutionStatus.SUCCESS else "✗"
    click.echo(f"{status_icon} run_id={run_id_val}  blueprint={blueprint_id}  status={status}")
    click.echo(f"  started:  {started_at}")
    click.echo(f"  finished: {finished_at or '(running)'}")
    click.echo("")
    click.echo(f"  {'Module':<30} {'Status':<10} Error")
    click.echo(f"  {'-'*30} {'-'*10} {'-'*40}")
    for mr in module_results:
        icon = "✓" if mr.get("status") == ExecutionStatus.SUCCESS else ("⏭" if mr.get("status") == ExecutionStatus.SKIPPED else "✗")
        err = mr.get("error") or ""
        if len(err) > 60:
            err = err[:57] + "..."
        click.echo(f"  {icon} {mr.get('module_id', ''):<28} {mr.get('status', ''):<10} {err}")



def _first_module_id(module_results: Any) -> str:
    """First module id from a run's module_results JSON (backend-portable —
    computed in Python instead of duckdb-specific json_extract_string)."""
    try:
        data = json.loads(module_results) if isinstance(module_results, str) else (module_results or [])
        return data[0].get("module_id", "") if data else ""
    except Exception:
        return ""


def _resolve_blueprint_id(blueprint_arg: str | None) -> str | None:
    """Resolve a blueprint id from an id or a YAML file path."""
    if not blueprint_arg:
        return None
    p = Path(blueprint_arg)
    if p.suffix in (".yml", ".yaml") and p.exists():
        try:
            from aqueduct.parser.parser import parse
            return parse(str(p)).id
        except Exception:
            return blueprint_arg
    return blueprint_arg


def _report_trend(
    column: str,
    blueprint_arg: str | None,
    since: str | None,
    store_dir: str | None,
    config_path: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Cross-run quality trend for one column.

    Read-side aggregate over ``probe_signals`` — no extra table, no duplicated
    storage. Unrolls ``null_rates`` and ``schema_snapshot`` payloads into long
    form, filters to ``column``, and reports null-rate + type history ordered by
    capture time. Type changes between consecutive snapshots are flagged.
    """
    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    blueprint_id = _resolve_blueprint_id(blueprint_arg)
    store = open_obs_read(cfg, store_dir, blueprint_id=blueprint_id)
    if store is None:
        _error("observability store not found")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    if since is None:
        from datetime import datetime, timedelta
        since = (datetime.now(tz=UTC) - timedelta(days=30)).isoformat()

    # Fetch the raw probe payloads (portable SQL — no `json_each`/`json_extract`,
    # which are DuckDB-only) and explode the JSON in Python so this works
    # identically on DuckDB and Postgres. `payload` is a str on DuckDB and an
    # already-parsed dict via psycopg2 on Postgres — handle both.
    payload_q = """
        SELECT run_id, CAST(captured_at AS VARCHAR) AS ts, signal_type, payload
        FROM probe_signals
        WHERE signal_type IN ('null_rates', 'schema_snapshot')
          AND captured_at >= ?
        ORDER BY captured_at
    """
    try:
        with store.connect() as cur:
            cur.execute(payload_q, [since])
            _raw = cur.fetchall()
    except Exception as exc:
        _error(f"trend query failed: {exc}")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    null_rows: list[tuple] = []
    type_rows: list[tuple] = []
    for run_id, ts, sig_type, payload in _raw:
        data = json.loads(payload) if isinstance(payload, str) else (payload or {})
        if sig_type == "null_rates":
            nr = (data.get("null_rates") or {}).get(column)
            if nr is not None:
                null_rows.append((run_id, ts, float(nr)))
        elif sig_type == "schema_snapshot":
            for f in data.get("fields", []):
                if f.get("name") == column:
                    type_rows.append((run_id, ts, f.get("type")))
                    break

    if not null_rows and not type_rows:
        click.echo(
            f"No probe signals for column {column!r} since {since}. "
            f"(Attach a null_rates or schema_snapshot Probe to track it.)"
        )
        return

    if fmt == "json":
        emit(
            {
                "column": column,
                "blueprint_id": blueprint_id,
                "since": since,
                "null_rate": [{"run_id": r[0], "captured_at": r[1], "null_rate": r[2]} for r in null_rows],
                "type": [{"run_id": r[0], "captured_at": r[1], "type": r[2]} for r in type_rows],
            },
            fmt="json",
        )
        return

    click.echo(f"Quality trend — column: {column}"
               + (f"  blueprint: {blueprint_id}" if blueprint_id else ""))
    if null_rows:
        click.echo("\n  null-rate:")
        click.echo(f"    {'captured_at':<28} {'run_id':<20} null_rate")
        for rid, ts, nr in null_rows:
            click.echo(f"    {ts:<28} {rid[:18]:<20} {nr:.4f}" if nr is not None else
                       f"    {ts:<28} {rid[:18]:<20} (n/a)")
    if type_rows:
        click.echo("\n  type history:")
        click.echo(f"    {'captured_at':<28} {'run_id':<20} type")
        prev = None
        for rid, ts, ct in type_rows:
            marker = "  ⚠ type drift" if (prev is not None and ct != prev) else ""
            click.echo(f"    {ts:<28} {rid[:18]:<20} {ct}{marker}")
            prev = ct


def _fmt_bytes(n: int | None) -> str:
    """Human-readable byte size for table output (raw ints kept in json/csv)."""
    if n is None:
        return "-"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


_HTML_CSS = """
body{font:14px/1.5 system-ui,sans-serif;margin:2rem;color:#1a1a1a;background:#fff}
h1{font-size:1.4rem;margin:0 0 1rem}h2{font-size:1.05rem;margin:1.75rem 0 .5rem;border-bottom:1px solid #eee;padding-bottom:.25rem}
.badge{display:inline-block;padding:.1rem .5rem;border-radius:.25rem;font-weight:600;font-size:.85rem}
.ok{background:#e6f4ea;color:#137333}.err{background:#fce8e6;color:#c5221f}.skip{background:#f1f3f4;color:#5f6368}
table{border-collapse:collapse;width:100%;margin:.25rem 0}
th,td{text-align:left;padding:.35rem .6rem;border-bottom:1px solid #eee;font-variant-numeric:tabular-nums}
th{color:#555;font-weight:600;font-size:.85rem}
th.num,td.num{text-align:right}.muted{color:#999}
.kv{width:auto;border:1px solid #eee;border-radius:.4rem;overflow:hidden}
.kv td,.kv th{border:none;border-bottom:1px solid #f3f3f3;padding:.3rem 1rem}
.kv tr:last-child td,.kv tr:last-child th{border-bottom:none}
.kv th{text-align:right;color:#555;white-space:nowrap;width:1%;background:#fafafa}
.kv td{font-weight:500}
.foot{margin-top:2rem;color:#999;font-size:.8rem}
"""


def _render_run_html(
    run_id: str,
    blueprint_id: str,
    status: str,
    started_at: str,
    finished_at: str | None,
    module_results: list,
    metrics_rows: list,
) -> str:
    """Render a single-file, self-contained HTML report for one run.

    No server, no external assets — renders offline. Reuses the same data the
    table/json formats already fetch, plus the Phase-62 resource profile rows.
    """
    from html import escape

    def e(v: object) -> str:
        return escape("" if v is None else str(v))

    status_cls = "ok" if status == ExecutionStatus.SUCCESS else "err"

    # Module results table
    res_rows = []
    for mr in module_results:
        st = mr.get("status", "")
        cls = "ok" if st == ExecutionStatus.SUCCESS else ("skip" if st == ExecutionStatus.SKIPPED else "err")
        res_rows.append(
            f"<tr><td>{e(mr.get('module_id'))}</td>"
            f"<td><span class='badge {cls}'>{e(st)}</span></td>"
            f"<td>{e(mr.get('error') or '')}</td></tr>"
        )
    results_html = "\n".join(res_rows) or "<tr><td colspan=3 class='muted'>no module results</td></tr>"

    # Resource profile table (heaviest first; metrics_rows already ordered)
    total_dur = sum((r[3] or 0) for r in metrics_rows)
    prof_rows = []
    for module_id, rec_w, by_w, dur in metrics_rows:
        pct = (100.0 * (dur or 0) / total_dur) if total_dur else 0.0
        prof_rows.append(
            f"<tr><td>{e(module_id)}</td>"
            f"<td class='num'>{e(dur) if dur is not None else '-'}</td>"
            f"<td class='num'>{pct:.1f}%</td>"
            f"<td class='num'>{('-' if rec_w is None else format(rec_w, ','))}</td>"
            f"<td class='num'>{_fmt_bytes(by_w)}</td></tr>"
        )
    profile_html = (
        "\n".join(prof_rows)
        or "<tr><td colspan=5 class='muted'>no module_metrics for this run</td></tr>"
    )

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Aqueduct run {e(run_id)}</title><style>{_HTML_CSS}</style></head>
<body>
<h1>Aqueduct run report</h1>
<table class="kv">
  <tr><th>Run</th><td>{e(run_id)}</td></tr>
  <tr><th>Blueprint</th><td>{e(blueprint_id)}</td></tr>
  <tr><th>Status</th><td><span class="badge {status_cls}">{e(status)}</span></td></tr>
  <tr><th>Started</th><td>{e(started_at)}</td></tr>
  <tr><th>Finished</th><td>{e(finished_at) or '<span class="muted">(running)</span>'}</td></tr>
</table>
<h2>Modules</h2>
<table><thead><tr><th>Module</th><th>Status</th><th>Error</th></tr></thead>
<tbody>{results_html}</tbody></table>
<h2>Resource profile</h2>
<table><thead><tr><th>Module</th><th class="num">Duration (ms)</th><th class="num">% of run</th><th class="num">Rows out</th><th class="num">Bytes out</th></tr></thead>
<tbody>{profile_html}</tbody></table>
<div class="foot">Generated by <code>aqueduct report --format html</code> · self-contained, no external assets.</div>
</body></html>"""


def _report_profile(
    run_id: str | None,
    blueprint_arg: str | None,
    last_n: int,
    store_dir: str | None,
    config_path: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Per-module resource profile (Phase 62) over ``module_metrics``.

    Pure read-side — no new table, no Spark action. Two modes:

      * **run-scoped** (``RUN_ID``) — one run's modules, heaviest (by duration)
        first, with each module's share of the run's total time and bytes.
      * **trend** (``--blueprint``, no RUN_ID) — per-module stats across the last
        ``--last`` runs (count, avg/max duration, most-recent duration), flagging
        a module whose latest run is >1.5× its window average as a slowdown.
    """
    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    if run_id:
        _profile_run(run_id, cfg, store_dir, fmt)
    elif blueprint_arg:
        _profile_trend(blueprint_arg, last_n, cfg, store_dir, fmt)
    else:
        _error("--profile needs RUN_ID (one run) or --blueprint ID (trend)")
        sys.exit(exit_codes.USAGE_ERROR)


def _profile_run(run_id, cfg, store_dir, fmt) -> None:
    store = open_obs_read(cfg, store_dir, run_id=run_id)
    if store is None:
        _error(f"observability store not found for run_id={run_id!r}")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    with store.connect() as cur:
        cur.execute(
            """
            SELECT module_id, records_read, bytes_read, records_written,
                   bytes_written, duration_ms
            FROM module_metrics WHERE run_id = ?
            ORDER BY duration_ms DESC NULLS LAST
            """,
            [run_id],
        )
        rows = cur.fetchall()

    if not rows:
        click.echo(f"No module_metrics for run {run_id!r}.")
        return

    cols = ["module_id", "records_read", "bytes_read", "records_written", "bytes_written", "duration_ms"]
    records = [dict(zip(cols, r)) for r in rows]
    total_dur = sum((r["duration_ms"] or 0) for r in records)
    total_bw = sum((r["bytes_written"] or 0) for r in records)

    if fmt == "json":
        emit(
            {
                "run_id": run_id,
                "total_duration_ms": total_dur,
                "total_bytes_written": total_bw,
                "modules": records,
            },
            fmt="json",
        )
        return
    if fmt == "csv":
        import csv as _csv
        import io
        buf = io.StringIO()
        w = _csv.writer(buf)
        w.writerow(cols + ["pct_duration"])
        for r in records:
            pct = (100.0 * (r["duration_ms"] or 0) / total_dur) if total_dur else 0.0
            w.writerow([r[c] for c in cols] + [f"{pct:.1f}"])
        click.echo(buf.getvalue(), nl=False)
        return

    click.echo(f"Resource profile — run_id={run_id}  modules={len(records)}")
    click.echo(f"  {'Module':<26}{'Duration':>10}{'%Dur':>7}{'RowsOut':>12}{'BytesOut':>11}")
    click.echo(f"  {'-'*26}{'-'*10:>10}{'-'*6:>7}{'-'*11:>12}{'-'*10:>11}")
    for r in records:
        pct = (100.0 * (r["duration_ms"] or 0) / total_dur) if total_dur else 0.0
        dur = f"{r['duration_ms']}ms" if r["duration_ms"] is not None else "-"
        rows_out = "-" if r["records_written"] is None else f"{r['records_written']:,}"
        click.echo(f"  {r['module_id']:<26}{dur:>10}{pct:>6.1f}%{rows_out:>12}{_fmt_bytes(r['bytes_written']):>11}")
    click.echo(f"  {'-'*66}")
    click.echo(f"  {'TOTAL':<26}{str(total_dur)+'ms':>10}{'100.0%':>7}{'':>12}{_fmt_bytes(total_bw):>11}")


def _profile_trend(blueprint_arg, last_n, cfg, store_dir, fmt) -> None:
    blueprint_id = _resolve_blueprint_id(blueprint_arg)
    if not blueprint_id:
        _error(f"could not resolve blueprint from {blueprint_arg!r}")
        sys.exit(exit_codes.USAGE_ERROR)

    store = open_obs_read(cfg, store_dir, blueprint_id=blueprint_id)
    if store is None:
        _error("observability store not found")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    with store.connect() as cur:
        cur.execute(
            "SELECT run_id FROM run_records WHERE blueprint_id = ? ORDER BY started_at DESC LIMIT ?",
            [blueprint_id, last_n],
        )
        run_ids = [r[0] for r in cur.fetchall()]
        if not run_ids:
            click.echo(f"No runs for blueprint {blueprint_id!r}.")
            return
        latest = run_ids[0]
        placeholders = ",".join("?" * len(run_ids))
        cur.execute(
            f"""
            SELECT module_id, COUNT(*) AS runs, AVG(duration_ms) AS avg_dur,
                   MAX(duration_ms) AS max_dur
            FROM module_metrics WHERE run_id IN ({placeholders})
            GROUP BY module_id
            """,
            run_ids,
        )
        agg = cur.fetchall()
        cur.execute(
            "SELECT module_id, duration_ms FROM module_metrics WHERE run_id = ?",
            [latest],
        )
        last_dur = dict(cur.fetchall())

    records = []
    for module_id, runs, avg_dur, max_dur in agg:
        ld = last_dur.get(module_id)
        regressed = bool(ld is not None and avg_dur and ld > 1.5 * avg_dur)
        records.append({
            "module_id": module_id, "runs": runs,
            "avg_duration_ms": round(avg_dur, 1) if avg_dur is not None else None,
            "max_duration_ms": max_dur, "last_duration_ms": ld,
            "regressed": regressed,
        })
    records.sort(key=lambda r: (r["avg_duration_ms"] or 0), reverse=True)

    if fmt == "json":
        emit(
            {
                "blueprint_id": blueprint_id,
                "runs_analyzed": len(run_ids),
                "modules": records,
            },
            fmt="json",
        )
        return
    if fmt == "csv":
        import csv as _csv
        import io
        buf = io.StringIO()
        w = _csv.writer(buf)
        cols = ["module_id", "runs", "avg_duration_ms", "max_duration_ms", "last_duration_ms", "regressed"]
        w.writerow(cols)
        for r in records:
            w.writerow([r[c] for c in cols])
        click.echo(buf.getvalue(), nl=False)
        return

    click.echo(f"Resource trend — blueprint={blueprint_id}  runs_analyzed={len(run_ids)}")
    click.echo(f"  {'Module':<26}{'Runs':>6}{'AvgDur':>10}{'MaxDur':>10}{'LastDur':>10}")
    click.echo(f"  {'-'*26}{'-'*6:>6}{'-'*9:>10}{'-'*9:>10}{'-'*9:>10}")
    for r in records:
        avg = f"{r['avg_duration_ms']}ms" if r["avg_duration_ms"] is not None else "-"
        mx = f"{r['max_duration_ms']}ms" if r["max_duration_ms"] is not None else "-"
        ld = f"{r['last_duration_ms']}ms" if r["last_duration_ms"] is not None else "-"
        flag = "  ⚠ slowdown" if r["regressed"] else ""
        click.echo(f"  {r['module_id']:<26}{r['runs']:>6}{avg:>10}{mx:>10}{ld:>10}{flag}")


# ── aqueduct runs ─────────────────────────────────────────────────────────────

@cli.command("runs")
@click.option("--blueprint", default=None, metavar="PATH_OR_ID", help="Filter by blueprint file path or blueprint ID")
@click.option("--failed", is_flag=True, default=False, help="Show only failed runs")
@click.option("--last", "limit", default=20, show_default=True, help="Max rows to show")
@click.option("--store-dir", default=None, help="Store directory (default: .aqueduct)")
@click.option("--config", "config_path", default=None, metavar="PATH", help="Path to aqueduct.yml")
@click.option(
    "--format", "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text", show_default=True,
    help="Output format. `json` for machine-readable consumption (Phase 30b).",
)
@click.option(
    "--heal-coverage", is_flag=True, default=False,
    help="Phase 45 — summarize healing_outcomes by resolution (llm / cached / "
         "replayed) and report zero-token heal coverage instead of listing runs.",
)
@_env_options
def runs(
    blueprint: str | None,
    failed: bool,
    limit: int,
    store_dir: str | None,
    config_path: str | None,
    out_format: str,
    heal_coverage: bool,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """List recent blueprint runs."""
    from aqueduct.config import load_config

    _resolve_and_load_env(
        env_file, Path(config_path) if config_path else None, cli_env=cli_env
    )
    cfg = load_config(Path(config_path) if config_path else None)
    _apply_warnings_from_cfg(cfg)

    blueprint_id: str | None = None
    if blueprint:
        arg_path = Path(blueprint)
        if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
            from aqueduct.parser.parser import ParseError, parse
            try:
                bp = parse(str(arg_path))
                blueprint_id = bp.id
            except ParseError:
                blueprint_id = blueprint
        else:
            blueprint_id = blueprint

    # Build the observability stores to scan. Postgres keeps every run in one
    # schema → a single store. DuckDB may have per-pipeline files → scan all
    # candidates (honouring --store-dir / a non-default path / --blueprint).
    stores: list = []
    if cfg.stores.observability.backend == "postgres":
        from aqueduct.stores.base import get_stores
        stores = [get_stores(cfg).observability]
    else:
        # 2.0: duckdb path = routing base directory; per-blueprint files at
        # <base>/<blueprint_id>/observability.db. Explicit-file mode and the
        # pre-routing flat `.aqueduct/observability.db` fallback are gone.
        candidates: list[Path] = []
        if store_dir:
            c = Path(store_dir) / "observability.db"
            if c.exists():
                candidates.append(c)
        else:
            from aqueduct.stores.read import _OBS_ROUTING_ROOT
            _base = Path(cfg.stores.observability.path or _OBS_ROUTING_ROOT)
            if blueprint_id:
                c = _base / blueprint_id / "observability.db"
                if c.exists():
                    candidates.append(c)
            if not candidates:
                candidates = sorted(_base.glob("*/observability.db"))
        if not candidates:
            click.echo("No runs found (no observability.db files discovered)")
            return
        from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
        stores = [DuckDBObservabilityStore(c) for c in candidates]

    if heal_coverage:
        # Phase 45 — playbook coverage: how many heals were resolved with
        # zero LLM tokens (resolution 'cached' or 'replayed'). One distinct
        # heal = one (parent_run_id-or-run_id, patch_id) outcome row group;
        # raw row counts are good enough at this granularity.
        _by_resolution: dict[str, int] = {}
        for _s in stores:
            try:
                # healing_outcomes has no blueprint_id column — scoping happens
                # via the per-pipeline store selection above.
                with _s.connect() as cur:
                    cur.execute(
                        """
                        SELECT COALESCE(resolution, 'llm') AS res, COUNT(*)
                        FROM healing_outcomes GROUP BY res
                        """
                    )
                    _rows = cur.fetchall()
            except Exception:
                continue
            for _res, _n in _rows:
                _by_resolution[_res] = _by_resolution.get(_res, 0) + int(_n)
        _total = sum(_by_resolution.values())
        _zero_token = _by_resolution.get("cached", 0) + _by_resolution.get("replayed", 0)
        _coverage = (_zero_token / _total) if _total else 0.0
        if out_format.lower() == "json":
            emit(
                {
                    "total_heals": _total,
                    "by_resolution": _by_resolution,
                    "zero_token_heals": _zero_token,
                    "zero_token_coverage": round(_coverage, 4),
                },
                fmt="json",
            )
        elif _total == 0:
            click.echo("No healing outcomes recorded yet.")
        else:
            click.echo(f"  heals recorded: {_total}")
            for _res in sorted(_by_resolution):
                click.echo(f"    {_res:<10} {_by_resolution[_res]}")
            click.echo(f"  zero-token coverage: {_coverage:.1%}  ({_zero_token}/{_total} heals needed no LLM call)")
        return

    where_parts = []
    params_base: list = []
    if blueprint_id:
        where_parts.append("blueprint_id = ?")
        params_base.append(blueprint_id)
    if failed:
        where_parts.append("status = 'error'")
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    rows: list = []
    for _s in stores:
        try:
            with _s.connect() as cur:
                cur.execute(
                    f"""
                    SELECT run_id, blueprint_id, status, started_at, finished_at,
                           module_results
                    FROM run_records
                    {where}
                    """,
                    params_base,
                )
                for run_id_v, bp_v, st_v, sa_v, fa_v, mr in cur.fetchall():
                    rows.append((run_id_v, bp_v, st_v, sa_v, fa_v, _first_module_id(mr)))
        except Exception:
            continue
    # Sort merged result by started_at DESC (col index 3), then limit
    rows.sort(key=lambda r: (r[3] is None, r[3]), reverse=True)
    rows = rows[:limit]

    if out_format.lower() == "json":
        emit(
            [
                {
                    "run_id": rv,
                    "blueprint_id": bp,
                    "status": st,
                    "started_at": str(sa) if sa else None,
                    "finished_at": str(fa) if fa else None,
                    "first_failed_module": ff,
                }
                for rv, bp, st, sa, fa, ff in rows
            ],
            fmt="json",
        )
        return

    if not rows:
        click.echo("No runs found.")
        return

    click.echo(f"  {'run_id':<38} {'blueprint':<30} {'status':<10} {'started':<22} {'failed_module'}")
    click.echo(f"  {'-'*38} {'-'*30} {'-'*10} {'-'*22} {'-'*20}")
    for run_id_val, bp_id, status, started_at, finished_at, first_failed in rows:
        icon = "✓" if status == ExecutionStatus.SUCCESS else ("↻" if status == "running" else "✗")
        failed_col = (first_failed or "") if status == ExecutionStatus.ERROR else ""
        click.echo(f"  {icon} {run_id_val:<37} {bp_id:<30} {status:<10} {str(started_at)[:19]:<22} {failed_col}")



# ── aqueduct lineage ──────────────────────────────────────────────────────────

@cli.command()
@click.argument("blueprint_id_or_blueprint")
@click.option(
    "--store-dir",
    default=None,
    help="Observability store directory",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@click.option(
    "--from",
    "from_table",
    default=None,
    help="Filter: only show lineage originating from this source table",
)
@click.option(
    "--column",
    "column_filter",
    default=None,
    help="Filter: only show lineage for this output column name",
)
@click.option(
    "--chain",
    "chain_column",
    default=None,
    metavar="COLUMN",
    help="Trace one column source→output as a vertical type-annotated chain "
         "(computed on demand from the blueprint; requires a blueprint file path).",
)
@click.option(
    "--types",
    "show_types",
    is_flag=True,
    default=False,
    help="With --chain: annotate each hop with the sqlglot-inferred SQL type "
         "and mark type changes.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
@_env_options
def lineage(
    blueprint_id_or_blueprint: str,
    store_dir: str | None,
    config_path: str | None,
    from_table: str | None,
    column_filter: str | None,
    chain_column: str | None,
    show_types: bool,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Print column-level lineage graph for a blueprint.

    PIPELINE_ID_OR_BLUEPRINT: blueprint id (e.g. nyc_taxi_demo) or path to
    the blueprint YAML file (e.g. blueprint.yml — id is extracted automatically).
    """
    from aqueduct.config import ConfigError, load_config

    # ── --chain: on-demand type-tracked trace, computed from the blueprint ──────
    if chain_column:
        _lineage_chain(blueprint_id_or_blueprint, chain_column, show_types, fmt,
                       config_path, env_file, cli_env)
        return

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    # Accept blueprint file path — extract blueprint id from it
    arg_path = Path(blueprint_id_or_blueprint)
    if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
        try:
            from aqueduct.parser.parser import parse
            bp = parse(str(arg_path))
            blueprint_id = bp.id
        except Exception as exc:
            _error(f"could not read blueprint id from {blueprint_id_or_blueprint!r}: {exc}")
            sys.exit(exit_codes.CONFIG_ERROR)
    else:
        blueprint_id = blueprint_id_or_blueprint

    # Phase 38: lineage merged into observability — column_lineage lives in the
    # observability store alongside all other observability tables.
    store = open_obs_read(cfg, store_dir, blueprint_id=blueprint_id)
    if store is None:
        _error("observability store not found")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    params: list[Any] = [blueprint_id]
    where_parts = ["blueprint_id = ?"]
    if from_table:
        where_parts.append("source_table = ?")
        params.append(from_table)
    if column_filter:
        where_parts.append("output_column = ?")
        params.append(column_filter)

    where = " AND ".join(where_parts)
    query = f"""
        SELECT DISTINCT channel_id, output_column, source_table, source_column
        FROM column_lineage
        WHERE {where}
        ORDER BY channel_id, output_column
    """

    with store.connect() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        click.echo(f"No lineage records found for blueprint {blueprint_id!r}.")
        return

    if fmt == "json":
        emit(
            [
                {
                    "channel_id": r[0],
                    "output_column": r[1],
                    "source_table": r[2],
                    "source_column": r[3],
                }
                for r in rows
            ],
            fmt="json",
        )
        return

    click.echo(f"Column lineage — blueprint: {blueprint_id}")
    click.echo(f"  {'Channel':<25} {'Output Column':<25} {'Source Table':<25} Source Column")
    click.echo(f"  {'-'*25} {'-'*25} {'-'*25} {'-'*25}")
    for channel_id, output_column, source_table, source_column in rows:
        click.echo(f"  {channel_id:<25} {output_column:<25} {source_table:<25} {source_column or ''}")



def _lineage_chain(
    blueprint_arg: str,
    column: str,
    show_types: bool,
    fmt: str,
    config_path: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Render a vertical, type-annotated source→output trace for one column.

    Computed on demand from the compiled manifest (no store read, no Spark
    action). Requires a blueprint *file* — an id alone cannot be recompiled.
    """
    arg_path = Path(blueprint_arg)
    if arg_path.suffix not in (".yml", ".yaml") or not arg_path.exists():
        _error("--chain requires a blueprint file path (not a blueprint id)")
        sys.exit(exit_codes.USAGE_ERROR)

    from aqueduct.compiler.chain import compute_type_chain
    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.parser.parser import ParseError, parse

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
    except Exception:
        pass  # env is best-effort for a pure compile

    try:
        bp = parse(str(arg_path))
        manifest = compiler_compile(bp, blueprint_path=arg_path)
    except (ParseError, CompileError) as exc:
        _error(f"could not compile {blueprint_arg!r}: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    hops = compute_type_chain(manifest.modules, manifest.edges, column)
    if not hops:
        click.echo(f"No SQL Channel produces column {column!r} in {bp.id!r}.")
        return

    if fmt == "json":
        emit(
            [
                {
                    "channel_id": h.channel_id,
                    "output_column": h.output_column,
                    "source_table": h.source_table,
                    "source_column": h.source_column,
                    "output_type": h.output_type,
                    "transform_op": h.transform_op,
                }
                for h in hops
            ],
            fmt="json",
        )
        return

    click.echo(f"Column chain — blueprint: {bp.id}  column: {column}")
    click.echo("")
    prev_type: str | None = None
    for i, h in enumerate(hops):
        connector = "  │" if i else "  "
        if i:
            click.echo("  │")
        type_note = ""
        if show_types:
            marker = ""
            if prev_type is not None and h.output_type != prev_type and h.output_type != "UNKNOWN":
                marker = "  ⚠ type change"
            type_note = f"  :: {h.output_type}{marker}"
            prev_type = h.output_type if h.output_type != "UNKNOWN" else prev_type
        click.echo(f"  ▸ {h.channel_id}.{h.output_column}{type_note}")
        click.echo(f"{connector}    ← {h.source_table or '(source)'}.{h.source_column}  [{h.transform_op}]")


# ── aqueduct signal ───────────────────────────────────────────────────────────

@cli.command()
@click.argument("signal_id")
@click.option(
    "--value",
    "value_str",
    type=click.Choice(["true", "false"]),
    default=None,
    help="Set override: 'false' closes gate (blocks all future runs), 'true' clears override",
)
@click.option(
    "--error",
    "error_msg",
    default=None,
    help="Attach a reason message (implies --value false)",
)
@click.option(
    "--store-dir",
    default=None,
    help="Observability store directory",
)
@click.option(
    "--blueprint",
    "blueprint_id",
    default=None,
    help=(
        "Blueprint id whose observability store holds the override (DuckDB "
        "routes per blueprint: <base>/<blueprint_id>/observability.db). "
        "Required for duckdb unless --store-dir is given; ignored for postgres."
    ),
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@_env_options
def signal(
    signal_id: str,
    value_str: str | None,
    error_msg: str | None,
    store_dir: str | None,
    blueprint_id: str | None,
    config_path: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Set or clear a persistent gate override for a Probe signal.

    \b
    Close gate (block all future runs):
      aqueduct signal my_probe --value false
      aqueduct signal my_probe --error "Source data is stale"

    Clear override (resume normal evaluation):
      aqueduct signal my_probe --value true
    """
    from datetime import datetime

    from aqueduct.config import ConfigError, load_config
    from aqueduct.stores import get_stores
    from aqueduct.surveyor.surveyor import _SIGNAL_OVERRIDES_DDL

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    # DuckDB routes per blueprint — without an id the override would land in
    # the `default/` store no run ever reads. (Postgres: one schema, no routing.)
    if (
        cfg.stores.observability.backend == "duckdb"
        and not blueprint_id
        and not store_dir
    ):
        _error(
            "signal: --blueprint <id> is required with the duckdb backend "
            "(overrides live in the blueprint's routed store)"
        )
        sys.exit(exit_codes.USAGE_ERROR)

    bundle = get_stores(
        cfg,
        store_dir_override=Path(store_dir) if store_dir else None,
        blueprint_id=blueprint_id,
    )

    if value_str is None and error_msg is None:
        # Show current override status
        with bundle.observability.connect() as cur:
            cur.execute(_SIGNAL_OVERRIDES_DDL)
            row = cur.execute(
                "SELECT passed, error_message, CAST(set_at AS VARCHAR) FROM signal_overrides WHERE signal_id = ?",
                [signal_id],
            ).fetchone()
        if row is None:
            click.echo(f"  {signal_id}: no persistent override (evaluates from Probe data)")
        else:
            state = "open" if row[0] else "closed (blocked)"
            click.echo(f"  {signal_id}: gate={state}  set_at={row[2]}")
            if row[1]:
                click.echo(f"  reason: {row[1]}")
        return

    if error_msg is not None and value_str == "true":
        _error("--error implies gate closed; cannot combine with --value true")
        sys.exit(exit_codes.USAGE_ERROR)

    # Resolve passed value
    passed = value_str == "true"

    now = datetime.now(tz=UTC).isoformat()
    with bundle.observability.connect() as cur:
        cur.execute(_SIGNAL_OVERRIDES_DDL)
        if passed:
            # Clear override — delete row entirely
            cur.execute("DELETE FROM signal_overrides WHERE signal_id = ?", [signal_id])
            click.echo(f"✓ signal {signal_id!r}: override cleared — gate resumes normal Probe evaluation")
        else:
            cur.execute(
                """
                INSERT INTO signal_overrides (signal_id, passed, error_message, set_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (signal_id) DO UPDATE SET
                    passed = excluded.passed,
                    error_message = excluded.error_message,
                    set_at = excluded.set_at
                """,
                [signal_id, False, error_msg, now],
            )
            msg_note = f"  reason: {error_msg}" if error_msg else ""
            click.echo(f"✓ signal {signal_id!r}: gate CLOSED — all future runs blocked at this Regulator")
            if msg_note:
                click.echo(msg_note)



# ── aqueduct studio (Phase 67) ────────────────────────────────────────────────

@cli.command()
@click.option("--config", "config_path", default=None, help="Path to aqueduct.yml")
@click.option(
    "--store-dir",
    default=None,
    help="Observability store dir (default: scan .aqueduct/observability/*)",
)
@_env_options
def studio(
    config_path: str | None,
    store_dir: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Launch the interactive Aqueduct studio (read-only TUI).

    Requires the optional 'tui' extra: pip install aqueduct-core[tui]
    """
    import importlib.util

    if importlib.util.find_spec("textual") is None:
        click.echo(
            "✗ aqueduct studio needs the 'tui' extra: pip install aqueduct-core[tui]",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
    except Exception:
        pass  # env loading is best-effort for a read-only viewer

    from aqueduct.tui.app import run_studio

    code = run_studio(config_path=config_path, store_dir=store_dir)
    sys.exit(exit_codes.SUCCESS if code == 0 else exit_codes.DATA_OR_RUNTIME)


# ── aqueduct dashboard (Phase 68) ─────────────────────────────────────────────

@cli.command()
@click.option("--config", "config_path", default=None, help="Path to aqueduct.yml")
@click.option(
    "--store-dir",
    default=None,
    help="Observability store dir (default: scan .aqueduct/observability/*)",
)
@click.option("--port", default=8501, show_default=True, help="Local port for the Streamlit server.")
@click.option("--no-browser", is_flag=True, default=False, help="Do not auto-open a browser.")
def dashboard(
    config_path: str | None,
    store_dir: str | None,
    port: int,
    no_browser: bool,
) -> None:
    """Launch the local, read-only observability dashboard (Streamlit).

    Requires the optional 'dashboard' extra: pip install aqueduct-core[dashboard]

    This is a LOCAL dev viewer (like the Spark UI) — on-demand, read-only, never a
    production server and never required by a pipeline. Shells out to
    `streamlit run` on the dashboard app; Ctrl-C to stop.
    """
    import importlib.util
    import subprocess

    if importlib.util.find_spec("streamlit") is None:
        click.echo(
            "✗ aqueduct dashboard needs the 'dashboard' extra: "
            "pip install aqueduct-core[dashboard]",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    app_path = str(Path(__file__).resolve().parent.parent / "dashboard" / "app.py")
    env = dict(os.environ)
    if config_path:
        env["AQ_DASH_CONFIG"] = config_path
    if store_dir:
        env["AQ_DASH_STORE_DIR"] = store_dir

    args = [
        sys.executable, "-m", "streamlit", "run", app_path,
        "--server.port", str(port),
        "--server.headless", "true" if no_browser else "false",
    ]
    try:
        sys.exit(subprocess.call(args, env=env))
    except KeyboardInterrupt:
        sys.exit(130)
