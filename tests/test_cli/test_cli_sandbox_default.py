"""Sandbox-mode defaults and emit-once behaviour (1.1.0).

Covers TEST_MANIFEST.md ⏳ items not already covered by
`test_cli_sandbox_mode.py`:

  * `agent.sandbox_mode: sample` (default) → 1000-row replay, drops Egress.
  * Startup-time `⚠ sandbox mode: preflight` / `⚠ DANGER: sandbox mode = off`
    banners emit exactly once per run.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aqueduct.cli import _run_patch_gates_inline, cli

pytestmark = pytest.mark.integration


def test_sandbox_mode_sample_is_default(tmp_path):
    """Blueprint without explicit `agent.sandbox_mode` defaults to "sample"."""
    from aqueduct.parser.parser import parse

    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test\n"
        "modules:\n"
        "  - id: m1\n"
        "    type: Ingress\n"
        "    label: M1\n"
        "    config: {format: csv, path: data.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    blueprint = parse(bp)
    assert blueprint.agent.sandbox_mode == "sample"


@patch("aqueduct.patch.preview.run_sandbox_gate")
def test_sandbox_mode_sample_forwards_default_1000_rows(mock_run_sandbox, tmp_path):
    """`sandbox_mode=sample` with the inline-gate default `sample_rows=1000`
    forwards 1000 to `run_sandbox_gate` — the documented replay budget.
    """
    blueprint_path = tmp_path / "blueprint.yml"
    blueprint_path.write_text(
        "aqueduct: '1.0'\nid: t\nname: t\nmodules: []\nedges: []\n",
        encoding="utf-8",
    )

    mock_patch = MagicMock()
    mock_patch.patch_id = "p-sample"
    mock_surveyor = MagicMock()
    mock_surveyor.latest_explain_snapshots.return_value = {}
    mock_run_sandbox.return_value = MagicMock(
        status="pass", sample_rows=1000, duration_ms=0, detail="OK"
    )
    mock_bundle = MagicMock()

    with patch(
        "aqueduct.patch.apply.apply_patch_to_dict", return_value={"modules": []}
    ), patch(
        "aqueduct.patch.preview.run_lineage_gate",
        return_value=MagicMock(status="pass", touched_modules=[]),
    ), patch(
        "aqueduct.patch.explain_gate.run_explain_gate",
        return_value=MagicMock(status="pass"),
    ):
        _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=mock_bundle,
            surveyor=mock_surveyor,
            failed_module="m1",
            iteration_run_id="r1",
            blueprint_id="b1",
            # both defaults — sandbox_mode=sample, sample_rows=1000
        )

    # First positional arg is the patched blueprint dict; sample_rows kw is 1000.
    assert mock_run_sandbox.call_args.kwargs["sample_rows"] == 1000


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_preflight_banner_emits_exactly_once(mock_get_exec, _mock_surveyor, tmp_path):
    """`⚠ sandbox mode: preflight` banner emits exactly once per run."""
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test\n"
        "agent:\n"
        "  approval_mode: human\n"
        "  sandbox_mode: preflight\n"
        "modules:\n"
        "  - id: m1\n"
        "    type: Ingress\n"
        "    label: M1\n"
        "    config: {format: csv, path: /nonexistent.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "aqueduct_config: '1.0'\n"
        "danger:\n"
        "  allow_full_preflight: true\n",
        encoding="utf-8",
    )

    mock_exec = MagicMock()
    mock_exec.return_value = MagicMock(status="success", module_results=(), trigger_agent=False)
    mock_get_exec.return_value = mock_exec

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    # Count banner emissions. Click test runner merges stdout+stderr into .output.
    needle = "⚠ sandbox mode: preflight"
    assert result.output.count(needle) == 1


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_off_banner_emits_exactly_once(mock_get_exec, _mock_surveyor, tmp_path):
    """`⚠ DANGER: sandbox mode = off` banner emits exactly once per run."""
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test\n"
        "agent:\n"
        "  approval_mode: human\n"
        "  sandbox_mode: 'off'\n"
        "modules:\n"
        "  - id: m1\n"
        "    type: Ingress\n"
        "    label: M1\n"
        "    config: {format: csv, path: /nonexistent.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "aqueduct_config: '1.0'\n"
        "danger:\n"
        "  allow_skip_sandbox: true\n",
        encoding="utf-8",
    )

    mock_exec = MagicMock()
    mock_exec.return_value = MagicMock(status="success", module_results=(), trigger_agent=False)
    mock_get_exec.return_value = mock_exec

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    needle = "⚠ DANGER: sandbox mode = off"
    assert result.output.count(needle) == 1
