"""`aqueduct patch revert` — the CLI half of the undo path.

Exercises the real command through `CliRunner`: a config heal is applied
with the real `patch apply`, then reverted, and the Blueprint is read back.
"""

from __future__ import annotations

import json

import pytest
import yaml
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.integration


_AQUEDUCT_YML = """
engine:
  spark:
    conf:
      spark.sql.shuffle.partitions: 200
"""

_BLUEPRINT = """
aqueduct: "1.0"
id: demo_bp
name: Demo Blueprint
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: csv
      path: data.csv
edges: []
"""


def _project(tmp_path, operations, patch_id="fix-shuffle"):
    (tmp_path / "aqueduct.yml").write_text(_AQUEDUCT_YML, encoding="utf-8")
    (tmp_path / "blueprint.yml").write_text(_BLUEPRINT, encoding="utf-8")
    pending = tmp_path / "patches" / "pending"
    pending.mkdir(parents=True)
    (pending / "00001_patch.json").write_text(
        json.dumps(
            {
                "patch_id": patch_id,
                "run_id": "r1",
                "rationale": "test",
                "operations": operations,
                "_aq_meta": {
                    "engine": "spark",
                    "run_id": "r1",
                    "blueprint_id": "demo_bp",
                },
            }
        ),
        encoding="utf-8",
    )
    return pending / "00001_patch.json"


def _apply(runner, tmp_path, patch_file):
    return runner.invoke(
        cli,
        [
            "patch",
            "apply",
            str(patch_file),
            "--blueprint",
            str(tmp_path / "blueprint.yml"),
            "--config",
            str(tmp_path / "aqueduct.yml"),
        ],
    )


def _revert(runner, tmp_path, patch_id, *extra):
    return runner.invoke(
        cli,
        [
            "patch",
            "revert",
            patch_id,
            "--blueprint",
            str(tmp_path / "blueprint.yml"),
            "--config",
            str(tmp_path / "aqueduct.yml"),
            *extra,
        ],
    )


_SET_CONF = {
    "op": "set_engine_config",
    "engine": "spark",
    "key": "spark.sql.shuffle.partitions",
    "value": 800,
}


def test_revert_restores_the_config_and_stamps_the_record(tmp_path):
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    assert _apply(runner, tmp_path, patch_file).exit_code == exit_codes.SUCCESS

    applied = yaml.safe_load((tmp_path / "blueprint.yml").read_text(encoding="utf-8"))
    assert applied["engine"]["spark"]["conf"]["spark.sql.shuffle.partitions"] == 800

    result = _revert(runner, tmp_path, "fix-shuffle")
    assert result.exit_code == exit_codes.SUCCESS, result.output

    after = yaml.safe_load((tmp_path / "blueprint.yml").read_text(encoding="utf-8"))
    assert after["engine"]["spark"]["conf"] == {}
    record = after["healed_by"][0]
    assert record["patch_id"] == "fix-shuffle"
    assert record["reverted_at"]
    # A backup of the pre-revert Blueprint is kept, same as `patch apply`.
    assert list((tmp_path / "patches" / "backups").glob("revert_fix-shuffle_*"))


def test_dry_run_writes_nothing(tmp_path):
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    _apply(runner, tmp_path, patch_file)
    before = (tmp_path / "blueprint.yml").read_text(encoding="utf-8")

    result = _revert(runner, tmp_path, "fix-shuffle", "--dry-run")
    assert result.exit_code == exit_codes.SUCCESS
    assert "spark.sql.shuffle.partitions" in result.output
    assert (tmp_path / "blueprint.yml").read_text(encoding="utf-8") == before


def test_json_format_emits_the_plan(tmp_path):
    runner = CliRunner()
    patch_file = _project(tmp_path, [_SET_CONF])
    _apply(runner, tmp_path, patch_file)

    result = _revert(runner, tmp_path, "fix-shuffle", "--format", "json")
    assert result.exit_code == exit_codes.SUCCESS
    payload = json.loads(result.output)
    assert payload["patch_id"] == "fix-shuffle"
    assert payload["engines"] == ["spark"]
    assert payload["restores"][0]["key"] == "spark.sql.shuffle.partitions"
    assert payload["restores"][0]["to"] == 200
    assert payload["reverted_at"]


def test_a_mixed_patch_is_refused_and_the_blueprint_is_untouched(tmp_path):
    runner = CliRunner()
    patch_file = _project(
        tmp_path,
        [
            _SET_CONF,
            {
                "op": "set_module_config_key",
                "module_id": "src",
                "key": "path",
                "value": "other.csv",
            },
        ],
        patch_id="mixed-fix",
    )
    assert _apply(runner, tmp_path, patch_file).exit_code == exit_codes.SUCCESS
    before = (tmp_path / "blueprint.yml").read_text(encoding="utf-8")

    result = _revert(runner, tmp_path, "mixed-fix")
    assert result.exit_code == exit_codes.DATA_OR_RUNTIME
    assert "set_module_config_key" in result.output
    assert "patch rollback" in result.output
    assert (tmp_path / "blueprint.yml").read_text(encoding="utf-8") == before


def test_unknown_patch_id_is_refused(tmp_path):
    runner = CliRunner()
    _project(tmp_path, [_SET_CONF])
    result = _revert(runner, tmp_path, "never-applied")
    assert result.exit_code == exit_codes.DATA_OR_RUNTIME
    assert "no healed_by record" in result.output
