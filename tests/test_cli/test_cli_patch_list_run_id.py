"""`aqueduct patch list --format json` exposes `run_id` etc. (1.0.1 fix).

Covers ⏳ items in TEST_MANIFEST.md § CLI / Trigger — patch_list JSON
`run_id` propagation:

  - JSON entries include run_id / blueprint_id / failed_module from _aq_meta
  - Older patches (no _aq_meta) → those fields are None
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit


def _write_patch(patches_root: Path, status: str, name: str, body: dict) -> Path:
    sub = patches_root / status
    sub.mkdir(parents=True, exist_ok=True)
    out = sub / name
    out.write_text(json.dumps(body, indent=2), encoding="utf-8")
    return out


def test_patch_list_json_includes_run_id(tmp_path):
    patches = tmp_path / "patches"
    _write_patch(
        patches,
        "pending",
        "20260523T032437_abc.json",
        {
            "patch_id": "abc",
            "rationale": "fix column ref",
            "confidence": 0.9,
            "category": "schema_drift",
            "operations": [],
            "_aq_meta": {
                "run_id": "manual__2026-05-23T03:21:00",
                "blueprint_id": "etl_drift",
                "failed_module": "completed_orders",
            },
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "patch", "list",
            "--patches-dir", str(patches),
            "--status", "pending",
            "--format", "json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert len(payload) == 1
    entry = payload[0]
    assert entry["run_id"] == "manual__2026-05-23T03:21:00"
    assert entry["blueprint_id"] == "etl_drift"
    assert entry["failed_module"] == "completed_orders"
    assert entry["patch_id"] == "abc"


def test_patch_list_json_handles_missing_aq_meta(tmp_path):
    """Patches without _aq_meta (pre-1.0.1) → new fields are null."""
    patches = tmp_path / "patches"
    _write_patch(
        patches,
        "pending",
        "20260101T000000_old.json",
        {
            "patch_id": "old",
            "rationale": "legacy patch",
            "confidence": 0.5,
            "category": "other",
            "operations": [],
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "patch", "list",
            "--patches-dir", str(patches),
            "--status", "pending",
            "--format", "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["run_id"] is None
    assert payload[0]["blueprint_id"] is None
    assert payload[0]["failed_module"] is None
