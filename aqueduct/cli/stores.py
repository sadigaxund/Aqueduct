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


# ── aqueduct depot ────────────────────────────────────────────────────────────


@cli.group("depot")
def depot_group() -> None:
    """Depot (cross-run KV state) maintenance commands."""


@depot_group.command("clear-intent")
@click.argument("key")
@click.option(
    "--blueprint",
    "blueprint_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML whose depot holds the row (the depot is per blueprint).",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@click.option(
    "--store-dir",
    "store_dir",
    default=None,
    help="Store directory (overrides aqueduct.yml; must match the run's --store-dir)",
)
@_env_options
def depot_clear_intent(
    key: str,
    blueprint_path: str,
    config_path: str | None,
    store_dir: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Clear a leftover watermark crash-consistency intent row for KEY.

    KEY is the watermark's depot key (the same value the append Egress's
    `watermark_key:` and the depot Egress's `key:` both name), NOT the
    already-prefixed `__intent__:<key>` row. See docs/specs.md's watermark
    crash-consistency section.

    `--blueprint` is required because a depot is per blueprint either way:
    a pathless mount routes to its own `<root>/<blueprint_id>/depot.db`, and
    a mount with an explicit `path` prefixes its keys `<blueprint_id>:`.
    Without the id this command would open the wrong file, or delete a key
    that is not the one the run wrote.
    """
    from aqueduct.cli.style import error as _error
    from aqueduct.cli.style import success as _success
    from aqueduct.config import ConfigError, load_config
    from aqueduct.depot.depot import DepotStore as _DS
    from aqueduct.stores import get_stores

    try:
        _resolve_and_load_env(env_file, Path(config_path) if config_path else None, cli_env=cli_env)
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    # Only the `id` is needed, so read it straight off the YAML instead of a
    # full parse (same shape as `patch revert`'s `bp_raw.get("id")`): a full
    # parse would fail on unrelated Blueprint problems, and this command has
    # to keep working precisely when a run is refusing to start.
    import yaml as _yaml

    _raw = _yaml.safe_load(Path(blueprint_path).read_text(encoding="utf-8")) or {}
    blueprint_id = str(_raw.get("id") or "")
    if not blueprint_id:
        _error(f"{blueprint_path}: no top-level `id:` — cannot tell which depot to open")
        sys.exit(exit_codes.CONFIG_ERROR)

    bundle = get_stores(cfg, store_dir_override=store_dir, blueprint_id=blueprint_id)
    depot = _DS(backend=bundle.depot)
    existed = bool(depot.read_intent(key))
    depot.clear_intent(key)
    if existed:
        _success(f"cleared intent row for watermark key {key!r} on blueprint {blueprint_id!r}")
    else:
        emit(
            f"no pending intent row for watermark key {key!r} on blueprint "
            f"{blueprint_id!r}: nothing to clear",
            fmt="text",
        )
