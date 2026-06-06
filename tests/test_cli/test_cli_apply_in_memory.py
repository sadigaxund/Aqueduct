"""Unit tests for CLI patch application utilities: _apply_patch_in_memory, _stage_failed_patch."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

pytestmark = pytest.mark.unit


# ── _apply_patch_in_memory ────────────────────────────────────────────────────


def test_apply_patch_in_memory_uses_blueprint_parent_dir(tmp_path):
    """_apply_patch_in_memory uses blueprint_path.parent as base_dir, not /tmp/."""
    bp = tmp_path / "blueprints" / "test.yml"
    bp.parent.mkdir(parents=True)
    bp.write_text("""\
aqueduct: '1.0'
id: test.bp
name: Test
modules:
  - id: src
    type: Ingress
    config: {format: parquet, path: data/input.parquet}
edges: []
""")
    patch_spec = MagicMock()
    patch_spec.operations = []

    from aqueduct.cli import _apply_patch_in_memory

    with patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={}), \
         patch("aqueduct.parser.parser.parse_dict") as mock_parse, \
         patch("aqueduct.compiler.compiler.compile") as mock_compile:
        mock_compile.return_value = MagicMock()
        _apply_patch_in_memory(patch_spec, bp, depot=None, profile=None, cli_overrides={})

    call_kwargs = mock_parse.call_args[1]
    assert call_kwargs["base_dir"] == bp.parent


def test_apply_patch_in_memory_parse_error_returns_none(tmp_path):
    """Parse/compilation errors in apply_patch_in_memory return None."""
    bp = tmp_path / "blueprint.yml"
    bp.write_text("""\
aqueduct: '1.0'
id: test.bp
name: Test
modules: []
edges: []
""")
    patch_spec = MagicMock()
    patch_spec.operations = []

    from aqueduct.cli import _apply_patch_in_memory

    with patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={}), \
         patch("aqueduct.parser.parser.parse_dict", side_effect=Exception("parse failed")):
        result = _apply_patch_in_memory(patch_spec, bp, depot=None, profile=None, cli_overrides={})
    assert result is None


# ── _stage_failed_patch ───────────────────────────────────────────────────────


def test_stage_failed_patch_shows_actual_filename(tmp_path):
    """_stage_failed_patch prints the actual {ts}_{patch_id}.json filename."""
    from aqueduct.patch.grammar import PatchSpec
    from aqueduct.surveyor.models import FailureContext

    patches_dir = tmp_path / "patches"
    pending = patches_dir / "pending"
    pending.mkdir(parents=True)
    patch = PatchSpec(
        patch_id="p-test", rationale="fix",
        operations=[{"op": "set_module_config_key",
                     "module_id": "m1", "key": "k", "value": "v"}],
    )
    failure_ctx = FailureContext(
        run_id="run1", blueprint_id="bp1", failed_module=None,
        error_message="msg", stack_trace="", manifest_json="{}",
        started_at="2020-01-01T00:00:00Z", finished_at="2020-01-01T00:00:00Z",
    )

    class FakeClick:
        echo = MagicMock()

    fake_click = FakeClick()
    cfg = MagicMock()
    cfg.webhooks.on_patch_pending = None

    from aqueduct.cli import _stage_failed_patch

    # Let the real stage_patch_for_human create the file, then assert
    _stage_failed_patch("stage", patch, patches_dir, failure_ctx, cfg, fake_click)

    msg = fake_click.echo.call_args[0][0]
    assert any(x in msg for x in ("p-test.json", "patches/pending"))
