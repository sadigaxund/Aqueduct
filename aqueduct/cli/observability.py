"""`observability` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    cli,
    _DEFAULT_OBS_PATH,
    _apply_warnings_from_cfg,
    _resolve_and_load_env,
    _env_options,)
import aqueduct.cli as _aqcli  # noqa: E402  (monkeypatch-able helpers)


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
    "--format",
    "fmt",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    show_default=True,
)
@_env_options
def report(
    run_id: str | None, store_dir: str | None, config_path: str | None,
    trend_column: str | None, blueprint_arg: str | None, since: str | None, fmt: str,
    env_file: str | None, cli_env: tuple[str, ...],
) -> None:
    """Print the Flow Report for a completed run, or a cross-run column trend."""
    if trend_column:
        _report_trend(trend_column, blueprint_arg, since, store_dir, config_path,
                      fmt, env_file, cli_env)
        return
    if not run_id:
        click.echo("✗ RUN_ID is required (or pass --trend COLUMN --blueprint ID)", err=True)
        sys.exit(exit_codes.USAGE_ERROR)
    import csv as _csv
    import io

    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    observability_db = _aqcli._resolve_obs_db(cfg, store_dir, run_id=run_id)
    if observability_db is None or not observability_db.exists():
        click.echo(
            f"✗ observability.db not found for run_id={run_id!r} "
            f"(searched: --store-dir, cfg.stores.observability.path, "
            f"and .aqueduct/observability/*/observability.db)",
            err=True,
        )
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    conn = _duckdb.connect(str(observability_db), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT run_id, blueprint_id, status,
                   CAST(started_at AS VARCHAR),
                   CAST(finished_at AS VARCHAR),
                   module_results
            FROM run_records WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        click.echo(f"✗ run {run_id!r} not found in {observability_db}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    run_id_val, blueprint_id, status, started_at, finished_at, module_results_raw = row
    module_results = json.loads(module_results_raw) if isinstance(module_results_raw, str) else (module_results_raw or [])

    if fmt == "json":
        out = {
            "run_id": run_id_val,
            "blueprint_id": blueprint_id,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "module_results": module_results,
        }
        click.echo(json.dumps(out, indent=2))
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
    status_icon = "✓" if status == "success" else "✗"
    click.echo(f"{status_icon} run_id={run_id_val}  blueprint={blueprint_id}  status={status}")
    click.echo(f"  started:  {started_at}")
    click.echo(f"  finished: {finished_at or '(running)'}")
    click.echo("")
    click.echo(f"  {'Module':<30} {'Status':<10} Error")
    click.echo(f"  {'-'*30} {'-'*10} {'-'*40}")
    for mr in module_results:
        icon = "✓" if mr.get("status") == "success" else ("⏭" if mr.get("status") == "skipped" else "✗")
        err = mr.get("error") or ""
        if len(err) > 60:
            err = err[:57] + "..."
        click.echo(f"  {icon} {mr.get('module_id', ''):<28} {mr.get('status', ''):<10} {err}")



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
    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    blueprint_id = _resolve_blueprint_id(blueprint_arg)

    if store_dir:
        obs_db = Path(store_dir) / "observability.db"
    elif cfg.stores.observability.path != _DEFAULT_OBS_PATH:
        obs_db = Path(cfg.stores.observability.path)
        if obs_db.is_dir():
            obs_db = obs_db / "observability.db"
    elif blueprint_id:
        obs_db = Path(".aqueduct/observability") / blueprint_id / "observability.db"
    else:
        obs_db = Path(_DEFAULT_OBS_PATH)
    if not obs_db.exists():
        click.echo(f"✗ observability.db not found at {obs_db}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    if since is None:
        from datetime import datetime, timedelta, timezone
        since = (datetime.now(tz=timezone.utc) - timedelta(days=30)).isoformat()

    # Long-form unroll: null_rates map and schema_snapshot fields → one row per
    # (run_id, column, captured_at). DuckDB json_each explodes the payload; no
    # view is materialised, so nothing is duplicated on disk.
    null_q = """
        SELECT ps.run_id, CAST(ps.captured_at AS VARCHAR) AS ts,
               CAST(je.value AS DOUBLE) AS null_rate
        FROM probe_signals ps,
             json_each(json_extract(ps.payload, '$.null_rates')) je
        WHERE ps.signal_type = 'null_rates'
          AND je.key = ?
          AND ps.captured_at >= ?
        ORDER BY ps.captured_at
    """
    type_q = """
        SELECT ps.run_id, CAST(ps.captured_at AS VARCHAR) AS ts,
               json_extract_string(f.value, '$.type') AS col_type
        FROM probe_signals ps,
             json_each(json_extract(ps.payload, '$.fields')) f
        WHERE ps.signal_type = 'schema_snapshot'
          AND json_extract_string(f.value, '$.name') = ?
          AND ps.captured_at >= ?
        ORDER BY ps.captured_at
    """
    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        null_rows = conn.execute(null_q, [column, since]).fetchall()
        type_rows = conn.execute(type_q, [column, since]).fetchall()
    except Exception as exc:
        click.echo(f"✗ trend query failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    finally:
        conn.close()

    if not null_rows and not type_rows:
        click.echo(
            f"No probe signals for column {column!r} since {since}. "
            f"(Attach a null_rates or schema_snapshot Probe to track it.)"
        )
        return

    if fmt == "json":
        out = {
            "column": column,
            "blueprint_id": blueprint_id,
            "since": since,
            "null_rate": [{"run_id": r[0], "captured_at": r[1], "null_rate": r[2]} for r in null_rows],
            "type": [{"run_id": r[0], "captured_at": r[1], "type": r[2]} for r in type_rows],
        }
        click.echo(json.dumps(out, indent=2))
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
    import duckdb as _duckdb
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

    # Collect candidate DBs across per-pipeline dirs + legacy shared path.
    # When the user set an explicit obs path, honour it verbatim. When
    # `--blueprint` is given, prefer that per-pipeline dir.
    candidates: list[Path] = []
    if store_dir:
        c = Path(store_dir) / "observability.db"
        if c.exists():
            candidates.append(c)
    elif cfg.stores.observability.path != _DEFAULT_OBS_PATH:
        c = Path(cfg.stores.observability.path)
        if c.is_dir():
            c = c / "observability.db"
        if c.exists():
            candidates.append(c)
    else:
        if blueprint_id:
            c = Path(".aqueduct/observability") / blueprint_id / "observability.db"
            if c.exists():
                candidates.append(c)
        if not candidates:
            candidates = sorted(Path(".aqueduct/observability").glob("*/observability.db"))
        legacy = Path(_DEFAULT_OBS_PATH)
        if legacy.exists():
            candidates.append(legacy)

    if not candidates:
        click.echo("No runs found (no observability.db files discovered)")
        return

    if heal_coverage:
        # Phase 45 — playbook coverage: how many heals were resolved with
        # zero LLM tokens (resolution 'cached' or 'replayed'). One distinct
        # heal = one (parent_run_id-or-run_id, patch_id) outcome row group;
        # raw row counts are good enough at this granularity.
        _by_resolution: dict[str, int] = {}
        for db in candidates:
            try:
                conn = _duckdb.connect(str(db), read_only=True)
                try:
                    # healing_outcomes has no blueprint_id column — scoping
                    # happens via the per-pipeline DB selection above.
                    _rows = conn.execute(
                        """
                        SELECT COALESCE(resolution, 'llm') AS res, COUNT(*)
                        FROM healing_outcomes GROUP BY res
                        """
                    ).fetchall()
                finally:
                    conn.close()
            except Exception:
                continue
            for _res, _n in _rows:
                _by_resolution[_res] = _by_resolution.get(_res, 0) + int(_n)
        _total = sum(_by_resolution.values())
        _zero_token = _by_resolution.get("cached", 0) + _by_resolution.get("replayed", 0)
        _coverage = (_zero_token / _total) if _total else 0.0
        if out_format.lower() == "json":
            import json as _json
            click.echo(_json.dumps({
                "total_heals": _total,
                "by_resolution": _by_resolution,
                "zero_token_heals": _zero_token,
                "zero_token_coverage": round(_coverage, 4),
            }, indent=2))
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
    for db in candidates:
        try:
            conn = _duckdb.connect(str(db), read_only=True)
            try:
                rows.extend(conn.execute(
                    f"""
                    SELECT run_id, blueprint_id, status, started_at, finished_at,
                           json_extract_string(module_results, '$[0].module_id') AS first_failed
                    FROM run_records
                    {where}
                    """,
                    params_base,
                ).fetchall())
            finally:
                conn.close()
        except Exception:
            continue
    # Sort merged result by started_at DESC (col index 3), then limit
    rows.sort(key=lambda r: (r[3] is None, r[3]), reverse=True)
    rows = rows[:limit]

    if out_format.lower() == "json":
        import json as _json
        payload = [
            {
                "run_id": rv,
                "blueprint_id": bp,
                "status": st,
                "started_at": str(sa) if sa else None,
                "finished_at": str(fa) if fa else None,
                "first_failed_module": ff,
            }
            for rv, bp, st, sa, fa, ff in rows
        ]
        click.echo(_json.dumps(payload, indent=2))
        return

    if not rows:
        click.echo("No runs found.")
        return

    click.echo(f"  {'run_id':<38} {'blueprint':<30} {'status':<10} {'started':<22} {'failed_module'}")
    click.echo(f"  {'-'*38} {'-'*30} {'-'*10} {'-'*22} {'-'*20}")
    for run_id_val, bp_id, status, started_at, finished_at, first_failed in rows:
        icon = "✓" if status == "success" else ("↻" if status == "running" else "✗")
        failed_col = (first_failed or "") if status == "error" else ""
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
    import duckdb as _duckdb

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
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    # Accept blueprint file path — extract blueprint id from it
    arg_path = Path(blueprint_id_or_blueprint)
    if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
        try:
            from aqueduct.parser.parser import parse
            bp = parse(str(arg_path))
            blueprint_id = bp.id
        except Exception as exc:
            click.echo(f"✗ could not read blueprint id from {blueprint_id_or_blueprint!r}: {exc}", err=True)
            sys.exit(exit_codes.CONFIG_ERROR)
    else:
        blueprint_id = blueprint_id_or_blueprint

    # Phase 38: lineage merged into observability — column_lineage lives in
    # observability.db alongside all other observability tables.
    if store_dir:
        obs_db = Path(store_dir) / "observability.db"
    elif cfg.stores.observability.path != _DEFAULT_OBS_PATH:
        obs_db = Path(cfg.stores.observability.path)
    else:
        obs_db = Path(".aqueduct/observability") / blueprint_id / "observability.db"
        if not obs_db.exists():
            obs_db = Path(".aqueduct/observability.db")
    if not obs_db.exists():
        click.echo(f"✗ observability.db not found at {obs_db}", err=True)
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

    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    if not rows:
        click.echo(f"No lineage records found for blueprint {blueprint_id!r}.")
        return

    if fmt == "json":
        out = [
            {
                "channel_id": r[0],
                "output_column": r[1],
                "source_table": r[2],
                "source_column": r[3],
            }
            for r in rows
        ]
        click.echo(json.dumps(out, indent=2))
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
        click.echo("✗ --chain requires a blueprint file path (not a blueprint id)", err=True)
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
        click.echo(f"✗ could not compile {blueprint_arg!r}: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    hops = compute_type_chain(manifest.modules, manifest.edges, column)
    if not hops:
        click.echo(f"No SQL Channel produces column {column!r} in {bp.id!r}.")
        return

    if fmt == "json":
        out = [
            {
                "channel_id": h.channel_id,
                "output_column": h.output_column,
                "source_table": h.source_table,
                "source_column": h.source_column,
                "output_type": h.output_type,
                "transform_op": h.transform_op,
            }
            for h in hops
        ]
        click.echo(json.dumps(out, indent=2))
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
    from datetime import datetime, timezone

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
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    bundle = get_stores(cfg, store_dir_override=Path(store_dir) if store_dir else None)

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
        click.echo("✗ --error implies gate closed; cannot combine with --value true", err=True)
        sys.exit(exit_codes.USAGE_ERROR)

    # Resolve passed value
    passed = value_str == "true"

    now = datetime.now(tz=timezone.utc).isoformat()
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

