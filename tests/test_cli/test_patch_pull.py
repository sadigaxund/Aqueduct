"""Phase 53 — `aqueduct patch pull`: resolve the index row, read the body from
the object store (`stores.blob`), write it into the local checkout.

Local backend only (no s3) — deterministic, no Spark, no network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

pytestmark = pytest.mark.unit

from aqueduct import exit_codes
from aqueduct.cli import cli


def _blueprint(path: Path) -> None:
    path.write_text(yaml.dump({
        "aqueduct": "1.0",
        "id": "test.bp",
        "name": "Test Blueprint",
        "modules": [
            {"id": "in", "type": "Ingress", "label": "In", "config": {"format": "parquet", "path": "p1"}}
        ],
        "edges": [],
    }), encoding="utf-8")


def _seed_index(row_status: str, object_key: str, patch_id: str = "p1") -> None:
    """Create the per-blueprint observability DB with one patch_index row."""
    from aqueduct.patch import index as ix
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    db = Path(".aqueduct/observability/test.bp/observability.db")
    db.parent.mkdir(parents=True, exist_ok=True)
    store = DuckDBObservabilityStore(db)
    with store.connect() as cur:
        ix.ensure_schema(cur)
        ix.upsert(cur, ix.PatchIndexRow(
            patch_id=patch_id, status=row_status, object_key=object_key,
            blueprint_id="test.bp",
        ))


def test_patch_pull_writes_local_body():
    runner = CliRunner()
    with runner.isolated_filesystem():
        _blueprint(Path("blueprint.yml"))
        # Body lives in the local patch store at patches/applied/p1.json
        body = {"patch_id": "p1", "rationale": "relabel", "operations": []}
        applied = Path("patches/applied")
        applied.mkdir(parents=True)
        (applied / "p1.json").write_text(json.dumps(body), encoding="utf-8")
        _seed_index("applied", "applied/p1.json")

        result = runner.invoke(cli, ["patch", "pull", "p1", "--blueprint", "blueprint.yml"])
        assert result.exit_code == 0, result.output

        out = Path("patches/pending/p1.json")
        assert out.exists()
        assert json.loads(out.read_text())["patch_id"] == "p1"
        assert "status=applied" in result.output


def test_patch_pull_unknown_id_exits_data_or_runtime():
    runner = CliRunner()
    with runner.isolated_filesystem():
        _blueprint(Path("blueprint.yml"))
        _seed_index("applied", "applied/other.json", patch_id="other")  # index exists, p1 absent

        result = runner.invoke(cli, ["patch", "pull", "p1", "--blueprint", "blueprint.yml"])
        assert result.exit_code == exit_codes.DATA_OR_RUNTIME
        assert "not found" in result.output.lower()


def test_patch_pull_unreadable_body_exits_data_or_runtime():
    runner = CliRunner()
    with runner.isolated_filesystem():
        _blueprint(Path("blueprint.yml"))
        # Index points at a body that does not exist on disk → unreadable.
        _seed_index("applied", "applied/missing.json")

        result = runner.invoke(cli, ["patch", "pull", "p1", "--blueprint", "blueprint.yml"])
        assert result.exit_code == exit_codes.DATA_OR_RUNTIME
