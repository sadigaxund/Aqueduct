"""`aqueduct stores` command group — backend info + depot migration.

Extracted verbatim from aqueduct/cli/__init__.py — no behaviour change.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
    cli,
)
from aqueduct.cli.output import emit

# ── aqueduct stores ──────────────────────────────────────────────────────────


@cli.group("stores")
def stores_group() -> None:
    """Inspect the configured store backends (Phase 28)."""


@stores_group.command("info")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@_env_options
def stores_info(config_path: str | None, env_file: str | None, cli_env: tuple[str, ...]) -> None:
    """Print each store's resolved backend + location label."""
    from aqueduct.cli.style import error as _error
    from aqueduct.config import ConfigError, load_config
    from aqueduct.stores import get_stores

    try:
        _resolve_and_load_env(env_file, Path(config_path) if config_path else None, cli_env=cli_env)
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    bundle = get_stores(cfg)
    rows = [
        ("observability", bundle.observability.backend, bundle.observability.location_label),
        ("depot", bundle.depot.backend, bundle.depot.location_label),
        ("blob", cfg.stores.blob.backend, cfg.stores.blob.path or "(default)"),
        ("benchmark", cfg.stores.benchmark.backend, cfg.stores.benchmark.path or "(default)"),
    ]
    w0 = max(len(r[0]) for r in rows)
    w1 = max(len(r[1]) for r in rows)
    emit(f"  {'store'.ljust(w0)}  {'backend'.ljust(w1)}  location", fmt="text", redact=True)
    emit(f"  {'-' * w0}  {'-' * w1}  --------", fmt="text", redact=True)
    for store, backend, loc in rows:
        emit(f"  {store.ljust(w0)}  {backend.ljust(w1)}  {loc}", fmt="text", redact=True)
