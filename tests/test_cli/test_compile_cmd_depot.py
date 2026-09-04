"""`aqueduct compile` — preview depot wiring (Item 1 of the depot-preview fix).

Before this fix, `aqueduct compile` never threaded a real depot into
`compiler.compile()`, so `@aq.depot.get()` / `@aq.run.prev_id()` in a
Blueprint always hit the loud `_depot_get_or_raise` `CompileError` even with
`stores.depots` fully configured in `aqueduct.yml`. These tests prove:

  (a) with a configured depot, `aqueduct compile` resolves the REAL stored
      value, namespaced the SAME way a real `aqueduct run` would namespace it
      (`aqueduct.stores.get_stores`'s `<blueprint_id>:` prefix rule);
  (b) with no `aqueduct.yml` on disk, preview still inherits the IMPLICIT
      DuckDB `default` mount `StoresConfig.effective_depots()` synthesizes —
      exactly as the run path always has — so an absent key resolves to the
      Blueprint's own default instead of raising;
  (c) the loud `CompileError` backstop still fires when the compile is
      genuinely depot-LESS (depot construction failed → `depot=None`).
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.unit

_BP = """aqueduct: "1.0"
id: compile.depot.demo
name: D
modules:
  - id: load
    type: Ingress
    label: L
    config: {{ format: parquet, path: "data/@aq.depot.get('{fn}', '{default}')/in" }}
  - id: c
    type: Channel
    label: C
    config: {{ op: sql, query: "SELECT * FROM load" }}
edges:
  - {{ from: load, to: c }}
"""


def _write_bp(tmp_path, fn="watermark", default="2020-01-01"):
    (tmp_path / "bp.yml").write_text(_BP.format(fn=fn, default=default))


def test_compile_resolves_real_depot_value_with_run_namespacing(tmp_path, monkeypatch):
    from aqueduct.config import load_config
    from aqueduct.depot.depot import DepotStore
    from aqueduct.stores import get_stores

    (tmp_path / "aqueduct.yml").write_text(
        f"stores:\n  depots:\n    default:\n      backend: duckdb\n      path: {tmp_path}/depot.db\n"
    )
    _write_bp(tmp_path)

    cfg = load_config(tmp_path / "aqueduct.yml")
    bundle = get_stores(cfg, blueprint_id="compile.depot.demo")
    DepotStore(backend=bundle.depot).put("watermark", "2099-12-31")

    # `monkeypatch.chdir` (not a bare `os.chdir`) so the cwd is RESTORED after
    # this test. `aqueduct compile` auto-discovers aqueduct.yml by walking up
    # from the cwd — a leaked chdir makes a later "no aqueduct.yml" test find
    # THIS test's config and silently invert its assertion.
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(tmp_path / "bp.yml")])

    assert result.exit_code == 0, result.output
    json_start = result.output.find("{")
    manifest = json.loads(result.output[json_start:])
    load_mod = next(m for m in manifest["modules"] if m["id"] == "load")
    assert load_mod["config"]["path"] == str(tmp_path / "data" / "2099-12-31" / "in")

    # Prove it's namespaced the SAME way `aqueduct run` namespaces it — a
    # different blueprint_id must not see this value.
    import duckdb

    conn = duckdb.connect(str(tmp_path / "depot.db"), read_only=True)
    keys = [r[0] for r in conn.execute("SELECT key FROM depot_kv").fetchall()]
    conn.close()
    assert keys == ["compile.depot.demo:watermark"]


def test_compile_no_aqueduct_yml_uses_implicit_default_mount_like_run_does(tmp_path, monkeypatch):
    """No `aqueduct.yml` on disk still yields a depot — and that is CORRECT.

    `StoresConfig.effective_depots()` (aqueduct/config.py) always synthesizes
    an implicit DuckDB `default` mount when `depots:` declares none — routed
    per blueprint to `.aqueduct/observability/<blueprint_id>/depot.db`. The RUN path has always inherited that implicit mount via
    `get_stores`, so `@aq.depot.get` has never raised on a real run. Item 1's
    whole point is that preview matches run — so preview inherits it too, and
    an absent key resolves to the Blueprint's own default rather than raising.

    The loud `_depot_get_or_raise` CompileError therefore backstops a
    genuinely depot-LESS compile (`depot=None`), not a merely
    unconfigured-in-YAML one — see
    `test_compile_depot_build_failure_falls_back_to_loud_compile_error` for
    the path that still hard-fails, and tests/test_compiler/test_runtime.py
    for the programmatic `compile(depot=None)` case.
    """
    _write_bp(tmp_path)
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(tmp_path / "bp.yml")])

    assert result.exit_code == 0, result.output
    json_start = result.output.find("{")
    manifest = json.loads(result.output[json_start:])
    load_mod = next(m for m in manifest["modules"] if m["id"] == "load")
    # Key absent from the implicit mount → the Blueprint's own default wins.
    assert load_mod["config"]["path"] == str(tmp_path / "data" / "2020-01-01" / "in")

    # Item 3 end-to-end: the read-only preview connect must NOT create the
    # depot file (or its parent dir) just by compiling. A preview never writes.
    assert not (tmp_path / ".aqueduct" / "depot.db").exists()
    assert not (
        tmp_path / ".aqueduct" / "observability" / "compile.depot.demo" / "depot.db"
    ).exists()


def test_compile_depot_build_failure_falls_back_to_loud_compile_error(tmp_path, monkeypatch):
    """A depot-construction failure (e.g. unreachable backend) must never
    crash `aqueduct compile` with an unrelated traceback — it falls back to
    depot=None and the same loud CompileError backstop fires."""
    (tmp_path / "aqueduct.yml").write_text(
        f"stores:\n  depots:\n    default:\n      backend: duckdb\n      path: {tmp_path}/depot.db\n"
    )
    _write_bp(tmp_path)

    import aqueduct.depot.depot as depot_mod

    def _boom(cfg, blueprint_id):
        raise RuntimeError("simulated depot construction failure")

    monkeypatch.setattr(depot_mod, "preview_depots", _boom)
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(tmp_path / "bp.yml")])

    assert result.exit_code == exit_codes.CONFIG_ERROR, result.output
    assert "no depot backend is configured" in result.output


def test_compile_no_depot_reference_still_works_without_config(tmp_path, monkeypatch):
    """A Blueprint that never touches @aq.depot.*/@aq.run.prev_id compiles
    fine with no aqueduct.yml at all — unaffected by this change."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "bp.yml").write_text(
        "aqueduct: '1.0'\nid: p\nname: P\n"
        "modules:\n"
        "  - id: load\n    type: Ingress\n    label: L\n"
        "    config: { format: parquet, path: data/in }\n"
        "edges: []\n"
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(tmp_path / "bp.yml")])
    assert result.exit_code == 0, result.output
