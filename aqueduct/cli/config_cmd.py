"""`aqueduct config` command group: read-only inspection of resolved config.

`aqueduct config explain` answers one question that nothing else answers:
for every setting the engine will actually use, what is the value and WHERE
did it come from. Four sources, in precedence order:

    override  -s/--set on this invocation
    blueprint the Blueprint's own `agent:` block (that Blueprint's runs only)
    env       an `aqueduct.yml` value written as `${VAR}`
    file      a literal value in `aqueduct.yml`
    default   nothing declared it; the schema default applies

This command resolves nothing itself. It calls the same
`_resolve_and_load_env` + `load_config` + `route_overrides` + `apply_to_model`
path every other command calls, then labels each leaf by comparing that
result against the raw YAML and the override set. Adding a resolution rule
here would be a second source of truth, which is exactly the bug this
command exists to make visible.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click
from pydantic import BaseModel

from aqueduct import exit_codes
from aqueduct.cli import (
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
    _resolve_project_root,
    cli,
)
from aqueduct.cli.output import emit

SOURCE_OVERRIDE = "override"
SOURCE_BLUEPRINT = "blueprint"
SOURCE_ENV = "env"
SOURCE_FILE = "file"
SOURCE_DEFAULT = "default"

# Highest first. The one place precedence is written down in this module, and
# it mirrors docs/cli_reference.md "Config overrides".
_SOURCE_ORDER = (SOURCE_OVERRIDE, SOURCE_BLUEPRINT, SOURCE_ENV, SOURCE_FILE, SOURCE_DEFAULT)


def _flatten_model(model: BaseModel, prefix: str = "") -> dict[str, Any]:
    """Dotted-path leaf map for a pydantic model.

    A nested model is descended into. Everything else (scalars, lists, dicts
    of free-form keys such as `engine.spark.conf`) is a leaf, because those
    dicts have no schema to walk and their keys are themselves dotted.
    """
    out: dict[str, Any] = {}
    for name in type(model).model_fields:
        value = getattr(model, name, None)
        path = f"{prefix}{name}"
        if isinstance(value, BaseModel):
            out.update(_flatten_model(value, prefix=f"{path}."))
        else:
            out[path] = value
    return out


def _flatten_mapping(data: Any, prefix: str = "") -> dict[str, Any]:
    """Dotted-path leaf map for a plain parsed-YAML mapping.

    Stops at the first non-mapping, so a free-form dict value is reported at
    the path that owns it rather than being split into pseudo-fields.
    """
    out: dict[str, Any] = {}
    if not isinstance(data, dict):
        return out
    for key, value in data.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            nested = _flatten_mapping(value, prefix=f"{path}.")
            out[path] = value
            out.update(nested)
        else:
            out[path] = value
    return out


def _env_referencing_paths(raw_data: Any) -> dict[str, str]:
    """Paths whose RAW (pre-expansion) value mentions `${VAR}`, and the text.

    Read off the unexpanded file text, since by the time `load_config` returns
    the value is already substituted and indistinguishable from a literal.
    """
    return {
        path: value
        for path, value in _flatten_mapping(raw_data).items()
        if isinstance(value, str) and "${" in value
    }


def _render(value: Any) -> str:
    if value is None:
        return "(none)"
    if isinstance(value, (dict, list)):
        import json

        return json.dumps(value, default=str, sort_keys=True)
    return str(value)


def _blueprint_agent_paths(blueprint_path: Path) -> dict[str, Any]:
    """The `agent:` block a Blueprint declares, as `agent.*` dotted paths.

    Read straight off the YAML rather than through the parser: this command
    must describe what the file says even when the Blueprint does not compile.
    """
    import yaml

    try:
        data = yaml.safe_load(blueprint_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    agent = (data or {}).get("agent") if isinstance(data, dict) else None
    if not isinstance(agent, dict):
        return {}
    return _flatten_mapping(agent, prefix="agent.")


@cli.group("config")
def config_group() -> None:
    """Inspect resolved engine configuration."""


@config_group.command("explain")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@click.option(
    "--blueprint",
    "blueprint_path",
    default=None,
    type=click.Path(dir_okay=False, exists=True),
    help="Also report this Blueprint's own agent: overrides, which win for its runs.",
)
@click.option(
    "-s",
    "--set",
    "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Include an override in the resolution, exactly as `aqueduct run -s` would. "
    "Repeatable, in-memory, never written back.",
)
@click.option(
    "--source",
    "source_filter",
    default=None,
    type=click.Choice(list(_SOURCE_ORDER)),
    help="Show only values that came from this source.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
    help="Output data shape.",
)
@_env_options
def config_explain(
    config_path: str | None,
    blueprint_path: str | None,
    set_items: tuple[str, ...],
    source_filter: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Print every resolved config value with the source it came from."""
    import yaml

    from aqueduct.cli.render.style import error as _error
    from aqueduct.config import ConfigError, load_config
    from aqueduct.overrides import OverrideError, apply_to_model, route_overrides

    cfg_file = Path(config_path) if config_path else None
    anchor = cfg_file if cfg_file is not None else _resolve_project_root() / "aqueduct.yml"

    try:
        _resolve_and_load_env(env_file, anchor, cli_env=cli_env)
        cfg = load_config(cfg_file)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    override_nested: dict[str, Any] = {}
    if set_items:
        try:
            override_nested, _bp_nested = route_overrides(
                set_items, allow_blueprint=blueprint_path is not None
            )
            cfg = apply_to_model(cfg, override_nested)
        except OverrideError as exc:
            _error(str(exc))
            sys.exit(exit_codes.CONFIG_ERROR)

    resolved_file = anchor if cfg_file is None else cfg_file
    raw_data: Any = None
    if resolved_file.exists():
        try:
            raw_data = yaml.safe_load(resolved_file.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            raw_data = None

    file_paths = set(_flatten_mapping(raw_data))
    env_paths = _env_referencing_paths(raw_data)
    override_paths = set(_flatten_mapping(override_nested))
    blueprint_paths = _blueprint_agent_paths(Path(blueprint_path)) if blueprint_path else {}

    rows: list[dict[str, Any]] = []
    for path, value in sorted(_flatten_model(cfg).items()):
        if path in override_paths:
            source, detail = SOURCE_OVERRIDE, "-s/--set"
        elif path in env_paths:
            source, detail = SOURCE_ENV, env_paths[path]
        elif path in file_paths:
            source, detail = SOURCE_FILE, str(resolved_file)
        else:
            source, detail = SOURCE_DEFAULT, "schema default"
        rows.append({"path": path, "value": value, "source": source, "detail": detail})

    # Blueprint `agent:` values are reported as their own rows, not merged
    # into the engine config: they apply to that one Blueprint's runs, and
    # merging them here would duplicate the run path's resolution.
    for path, value in sorted(blueprint_paths.items()):
        rows.append(
            {
                "path": path,
                "value": value,
                "source": SOURCE_BLUEPRINT,
                "detail": str(blueprint_path),
            }
        )

    if source_filter:
        rows = [r for r in rows if r["source"] == source_filter]

    if fmt == "json":
        emit(
            [
                {
                    "path": r["path"],
                    "value": r["value"],
                    "source": r["source"],
                    "detail": r["detail"],
                }
                for r in rows
            ],
            fmt="json",
        )
        return

    if not rows:
        emit("  no values matched", fmt="text", redact=True)
        return

    w0 = max(len(r["path"]) for r in rows)
    w1 = max(len(_render(r["value"])) for r in rows)
    w1 = min(w1, 60)
    w2 = max(len(r["source"]) for r in rows)
    emit(
        f"  {'setting'.ljust(w0)}  {'value'.ljust(w1)}  {'source'.ljust(w2)}  from",
        fmt="text",
        redact=True,
    )
    emit(f"  {'-' * w0}  {'-' * w1}  {'-' * w2}  ----", fmt="text", redact=True)
    for r in rows:
        rendered = _render(r["value"])
        if len(rendered) > 60:
            rendered = rendered[:57] + "..."
        emit(
            f"  {r['path'].ljust(w0)}  {rendered.ljust(w1)}  "
            f"{r['source'].ljust(w2)}  {r['detail']}",
            fmt="text",
            redact=True,
        )
