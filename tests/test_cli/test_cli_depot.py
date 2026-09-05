"""`aqueduct depot clear-intent` CLI tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.depot.depot import depot_intent_key

pytestmark = pytest.mark.integration

_CFG = """\
aqueduct_config: "1.0"

stores:
  observability:
    backend: duckdb
    path: "{obs}"
  depots:
    default:
      backend: duckdb
      path: "{dep}"
"""

_ROUTED_CFG = """\
aqueduct_config: "1.0"
"""

_BP = """\
aqueduct: "1.0"
id: depot_intent_bp
name: Depot intent
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: /nonexistent/in.parquet
  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: /nonexistent/out.parquet
edges:
  - from: src
    to: sink
"""


def _write_project(tmp_path: Path, cfg_template: str = _CFG) -> tuple[Path, Path]:
    obs_dir = tmp_path / "obs"
    obs_dir.mkdir()
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        cfg_template.format(obs=str(obs_dir), dep=str(tmp_path / "depot.db")),
        encoding="utf-8",
    )
    bp = tmp_path / "blueprint.yml"
    bp.write_text(_BP, encoding="utf-8")
    return cfg, bp


def test_clear_intent_clears_existing_row(tmp_path):
    cfg, bp = _write_project(tmp_path)

    from aqueduct.stores.duckdb_ import DuckDBDepotStore

    backend = DuckDBDepotStore(tmp_path / "depot.db")
    # An explicit `path` mount namespaces its keys by blueprint id, so this is
    # the key an actual run of `depot_intent_bp` would have written.
    backend.kv_put(f"depot_intent_bp:{depot_intent_key('wk1')}", '{"run_id": "r1"}')

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["depot", "clear-intent", "wk1", "--blueprint", str(bp), "--config", str(cfg)],
    )

    assert result.exit_code == 0, result.output
    assert "cleared" in result.output.lower()
    assert backend.kv_get(f"depot_intent_bp:{depot_intent_key('wk1')}", "") == ""


def test_clear_intent_reports_when_nothing_to_clear(tmp_path):
    cfg, bp = _write_project(tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["depot", "clear-intent", "wk_missing", "--blueprint", str(bp), "--config", str(cfg)],
    )

    assert result.exit_code == 0, result.output
    assert "no pending intent row" in result.output.lower()


def test_clear_intent_finds_the_per_blueprint_routed_depot(tmp_path, monkeypatch):
    """With no `path` on the mount, the depot is its own per-blueprint FILE.

    The command has to resolve `<routing root>/<blueprint_id>/depot.db`, which
    it can only do from `--blueprint`. Reading the default mount without an id
    would open `<routing root>/default/depot.db` and report nothing to clear.
    """
    monkeypatch.chdir(tmp_path)
    cfg, bp = _write_project(tmp_path, cfg_template=_ROUTED_CFG)

    from aqueduct.config import DEFAULT_OBS_ROUTING_ROOT
    from aqueduct.stores.duckdb_ import DuckDBDepotStore

    routed = Path(DEFAULT_OBS_ROUTING_ROOT) / "depot_intent_bp" / "depot.db"
    routed.parent.mkdir(parents=True, exist_ok=True)
    backend = DuckDBDepotStore(routed)
    # A pathless mount is isolated by the FILE, so its keys are raw.
    backend.kv_put(depot_intent_key("wk1"), '{"run_id": "r1"}')

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["depot", "clear-intent", "wk1", "--blueprint", str(bp), "--config", str(cfg)],
    )

    assert result.exit_code == 0, result.output
    assert "cleared" in result.output.lower()
    assert backend.kv_get(depot_intent_key("wk1"), "") == ""
