"""Phase 56 — `aqueduct lineage <bp.yml> --chain <col> --types` rendering."""
from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit

_BP = """aqueduct: "1.0"
id: chain.demo
name: Chain
modules:
  - id: load
    type: Ingress
    label: L
    config: { format: parquet, path: data/in }
  - id: clean
    type: Channel
    label: C
    config:
      op: sql
      query: "SELECT CAST(amount AS DOUBLE) AS amount, id FROM load"
  - id: final
    type: Channel
    label: F
    config:
      op: sql
      query: "SELECT amount, id FROM clean"
edges:
  - { from: load, to: clean }
  - { from: clean, to: final }
"""


def _write_bp(tmp_path):
    p = tmp_path / "bp.yml"
    p.write_text(_BP)
    return str(p)


def test_chain_types_renders_cast_and_passthrough(tmp_path):
    res = CliRunner().invoke(cli, ["lineage", _write_bp(tmp_path), "--chain", "amount", "--types"])
    assert res.exit_code == 0, res.output
    assert "clean.amount" in res.output and "DOUBLE" in res.output
    assert "[CAST]" in res.output
    assert "final.amount" in res.output and "[passthrough]" in res.output


def test_chain_json_shape(tmp_path):
    res = CliRunner().invoke(cli, ["lineage", _write_bp(tmp_path), "--chain", "amount", "--format", "json"])
    assert res.exit_code == 0, res.output
    hops = json.loads(res.output)
    assert [h["channel_id"] for h in hops] == ["clean", "final"]
    assert hops[0]["transform_op"] == "CAST" and hops[0]["output_type"] == "DOUBLE"


def test_chain_requires_file_not_id(tmp_path):
    res = CliRunner().invoke(cli, ["lineage", "some.blueprint.id", "--chain", "amount"])
    assert res.exit_code != 0
    assert "requires a blueprint file path" in res.output


def test_chain_unknown_column_reports_empty(tmp_path):
    res = CliRunner().invoke(cli, ["lineage", _write_bp(tmp_path), "--chain", "ghost"])
    assert res.exit_code == 0, res.output
    assert "No SQL Channel produces column 'ghost'" in res.output
