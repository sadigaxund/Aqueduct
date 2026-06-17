"""`aqueduct stores` command group — backend info + depot migration.

Extracted verbatim from aqueduct/cli/__init__.py — no behaviour change.
"""
from __future__ import annotations

import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    cli,
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
)

# ── aqueduct stores ──────────────────────────────────────────────────────────


@cli.group("stores")
def stores_group() -> None:
    """Inspect and migrate the configured store backends (Phase 28)."""


@stores_group.command("info")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@_env_options
def stores_info(
    config_path: str | None, env_file: str | None, cli_env: tuple[str, ...]
) -> None:
    """Print each store's resolved backend + location label."""
    from aqueduct.config import ConfigError, load_config
    from aqueduct.stores import get_stores

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    bundle = get_stores(cfg)
    rows = [
        ("observability", bundle.observability.backend, bundle.observability.location_label),
        ("lineage",       bundle.lineage.backend,       bundle.lineage.location_label),
        ("depot",         bundle.depot.backend,         bundle.depot.location_label),
        ("blob",          cfg.stores.blob.backend,      cfg.stores.blob.path or "(default)"),
        ("benchmark",     cfg.stores.benchmark.backend,  cfg.stores.benchmark.path or "(default)"),
    ]
    w0 = max(len(r[0]) for r in rows)
    w1 = max(len(r[1]) for r in rows)
    click.echo(f"  {'store'.ljust(w0)}  {'backend'.ljust(w1)}  location")
    click.echo(f"  {'-' * w0}  {'-' * w1}  --------")
    for store, backend, loc in rows:
        click.echo(f"  {store.ljust(w0)}  {backend.ljust(w1)}  {loc}")


@stores_group.command("migrate")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml (the TARGET config — backend must already be set to postgres/redis)",
)
@click.option(
    "--from-duckdb",
    "from_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the source DuckDB file (typically `.aqueduct/depot.db`)",
)
@click.option(
    "--store",
    type=click.Choice(["depot"], case_sensitive=False),
    default="depot",
    show_default=True,
    help=(
        "Which store to migrate. v1 ships depot migration only; observability/lineage "
        "migration requires schema-aware row copying and is tracked in TODOs.md "
        "for a follow-up phase. Document the manual route: COPY each DuckDB "
        "table to Parquet, then `\\copy observability.<table> FROM 'file.parquet'` on PG."
    ),
)
@_env_options
def stores_migrate(
    config_path: str | None, from_path: str, store: str,
    env_file: str | None, cli_env: tuple[str, ...],
) -> None:
    """Copy KV rows from a DuckDB file into the configured target backend.

    Useful when promoting an existing DuckDB-backed project to Postgres or Redis
    without losing depot watermarks and counters. Idempotent: re-running upserts
    the same keys with the same values.
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.stores import get_stores

    if store.lower() != "depot":
        click.echo(f"✗ unsupported --store: {store}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    bundle = get_stores(cfg)
    target_label = f"{bundle.depot.backend}:{bundle.depot.location_label}"
    if bundle.depot.backend == "duckdb" and Path(bundle.depot.location_label) == Path(from_path).resolve():
        click.echo("✗ source and target depot are the same DuckDB file; nothing to migrate", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    try:
        import duckdb as _duckdb
    except ImportError as exc:
        click.echo(f"✗ duckdb not installed: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    conn = _duckdb.connect(str(Path(from_path).resolve()), read_only=True)
    try:
        rows = conn.execute("SELECT key, value FROM depot_kv").fetchall()
    except Exception as exc:
        click.echo(f"✗ could not read depot_kv from {from_path}: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)
    finally:
        conn.close()

    if not rows:
        click.echo(f"  source depot has 0 rows — nothing to copy ({from_path})")
        return

    for key, value in rows:
        bundle.depot.kv_put(str(key), str(value))

    click.echo(f"✓ migrated {len(rows)} depot key(s)  source={from_path}  target={target_label}")
