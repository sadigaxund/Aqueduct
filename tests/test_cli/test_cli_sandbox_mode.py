import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli, _run_patch_gates_inline

pytestmark = [pytest.mark.integration]

_BP_TEMPLATE = """\
aqueduct: '1.0'
id: test_bp
name: Test BP
agent:
  approval: {approval}
  sandbox_mode: "{sandbox}"
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: /nonexistent/data.csv}}
edges: []
"""

_CFG_TEMPLATE = """\
aqueduct_config: "1.0"
danger:
  allow_full_preflight: {allow_preflight}
  allow_skip_sandbox: {allow_skip}
  allow_multi_patch: {allow_aggressive}
"""

def _write_project(tmp_path, approval, sandbox, allow_preflight=False, allow_skip=False, allow_aggressive=False):
    bp = tmp_path / "bp.yml"
    bp.write_text(_BP_TEMPLATE.format(approval=approval, sandbox=sandbox), encoding="utf-8")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_CFG_TEMPLATE.format(
        allow_preflight=str(allow_preflight).lower(),
        allow_skip=str(allow_skip).lower(),
        allow_aggressive=str(allow_aggressive).lower()
    ), encoding="utf-8")
    return bp, cfg

@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_sandbox_mode_preflight_blocks_without_danger_gate(mock_get_exec, mock_surveyor_cls, tmp_path):
    """agent.sandbox_mode: preflight without danger.allow_full_preflight -> exits 1 with helpful message"""
    bp, cfg = _write_project(tmp_path, "human", "preflight", allow_preflight=False)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == 1
    assert "agent.sandbox_mode: preflight requires danger.allow_full_preflight: true" in result.output

@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_sandbox_mode_preflight_allowed_with_danger_gate(mock_get_exec, mock_surveyor_cls, tmp_path):
    """agent.sandbox_mode: preflight WITH danger.allow_full_preflight -> succeeds/proceeds and prints startup warning"""
    bp, cfg = _write_project(tmp_path, "human", "preflight", allow_preflight=True)
    
    # Mock executor to return success to terminate early
    mock_executor = MagicMock()
    mock_executor.return_value = MagicMock(status="success")
    mock_get_exec.return_value = mock_executor
    
    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    # Since it is human mode and execution succeeded, it exits 0
    assert result.exit_code == 0
    assert "⚠ sandbox mode: preflight (full-dataset replay, no Egress) — slow but conclusive" in result.output

@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_sandbox_mode_off_blocks_without_danger_gate(mock_get_exec, mock_surveyor_cls, tmp_path):
    """agent.sandbox_mode: off without danger.allow_skip_sandbox -> exits 1"""
    bp, cfg = _write_project(tmp_path, "human", "off", allow_skip=False)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == 1
    assert "agent.sandbox_mode: off requires danger.allow_skip_sandbox: true" in result.output

@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_sandbox_mode_off_allowed_with_danger_gate(mock_get_exec, mock_surveyor_cls, tmp_path):
    """agent.sandbox_mode: off WITH danger.allow_skip_sandbox -> succeeds/proceeds and prints warning"""
    bp, cfg = _write_project(tmp_path, "human", "off", allow_skip=True)
    
    mock_executor = MagicMock()
    mock_executor.return_value = MagicMock(status="success")
    mock_get_exec.return_value = mock_executor
    
    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == 0
    assert "⚠ DANGER: sandbox mode = off (skipping pre-apply replay; patches apply to real data)" in result.output

@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_danger_combo_warning(mock_get_exec, mock_surveyor_cls, tmp_path):
    """sandbox_mode=off + max_patches > 1 -> prints DANGER COMBO warning.

    1.1.0: combo gate is now keyed on max_patches > 1, not the legacy
    `approval_mode=aggressive` literal. The blueprint sets
    `max_patches: 2` (deprecated alias) to opt into the loop
    so this test also covers alias-resolution for the gate.
    """
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test BP\n"
        "agent:\n"
        "  approval: aggressive\n"
        "  sandbox_mode: 'off'\n"
        "  max_patches: 2\n"
        "modules:\n"
        "  - id: src\n"
        "    type: Ingress\n"
        "    label: Src\n"
        "    config: {format: csv, path: /nonexistent/data.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_CFG_TEMPLATE.format(
        allow_preflight="false",
        allow_skip="true",
        allow_aggressive="true",
    ), encoding="utf-8")

    mock_executor = MagicMock()
    mock_executor.return_value = MagicMock(status="success")
    mock_get_exec.return_value = mock_executor

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == 0
    assert "⚠ DANGER COMBO: sandbox_mode=off + max_patches > 1" in result.output

def test_run_patch_gates_inline_off(tmp_path):
    """_run_patch_gates_inline skips the sandbox gate when sandbox_mode is off"""
    blueprint_path = tmp_path / "blueprint.yml"
    blueprint_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []")
    
    mock_patch = MagicMock()
    mock_surveyor = MagicMock()
    mock_surveyor.latest_explain_snapshots.return_value = {}
    
    with patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={}), \
         patch("aqueduct.patch.preview.run_lineage_gate", return_value=MagicMock(status="pass", touched_modules=[])), \
         patch("aqueduct.patch.explain_gate.run_explain_gate", return_value=MagicMock(status="pass")):
        
        g2, g3, g4, passed = _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=MagicMock(),
            surveyor=mock_surveyor,
            failed_module="m1",
            iteration_run_id="r1",
            blueprint_id="b1",
            sandbox_mode="off"
        )
    
    assert g3.status == "skip"
    assert "sandbox_mode=off" in g3.detail
    assert passed is True

@patch("aqueduct.patch.preview.run_sandbox_gate")
def test_run_patch_gates_inline_preflight_and_sample(mock_run_sandbox, tmp_path):
    """_run_patch_gates_inline forwards 0 to run_sandbox_gate on preflight, and positive number on sample"""
    blueprint_path = tmp_path / "blueprint.yml"
    blueprint_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []")
    
    mock_patch = MagicMock()
    mock_patch.patch_id = "p-test"
    mock_surveyor = MagicMock()
    mock_surveyor.latest_explain_snapshots.return_value = {}
    
    mock_run_sandbox.return_value = MagicMock(status="pass", sample_rows=0, duration_ms=0, detail="OK")
    
    mock_bundle = MagicMock()
    
    with patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={"modules": []}), \
         patch("aqueduct.patch.preview.run_lineage_gate", return_value=MagicMock(status="pass", touched_modules=[])), \
         patch("aqueduct.patch.explain_gate.run_explain_gate", return_value=MagicMock(status="pass")):
        
        # Test preflight -> forwards 0
        _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=mock_bundle,
            surveyor=mock_surveyor,
            failed_module="m1",
            iteration_run_id="r1",
            blueprint_id="b1",
            sandbox_mode="preflight",
            sample_rows=100
        )
        mock_run_sandbox.assert_called_with(
            {"modules": []},
            blueprint_path=blueprint_path,
            patch_id="p-test",
            failed_module="m1",
            sample_rows=0,
            observability_store=mock_bundle.observability,
            explain_capture={},
            sandbox_master_url=None
        )

        # Test sample -> forwards 100
        mock_run_sandbox.reset_mock()
        _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=mock_bundle,
            surveyor=mock_surveyor,
            failed_module="m1",
            iteration_run_id="r1",
            blueprint_id="b1",
            sandbox_mode="sample",
            sample_rows=100
        )
        mock_run_sandbox.assert_called_with(
            {"modules": []},
            blueprint_path=blueprint_path,
            patch_id="p-test",
            failed_module="m1",
            sample_rows=100,
            observability_store=mock_bundle.observability,
            explain_capture={},
            sandbox_master_url=None
        )


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_multi_patch_blocks_without_danger_gate(mock_get_exec, mock_surveyor_cls, tmp_path):
    """max_patches > 1 without danger.allow_multi_patch -> exits 1 with helpful message."""
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test BP\n"
        "agent:\n"
        "  approval: auto\n"
        "  max_patches: 2\n"
        "modules:\n"
        "  - id: src\n"
        "    type: Ingress\n"
        "    label: Src\n"
        "    config: {format: csv, path: /nonexistent/data.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "aqueduct_config: \"1.0\"\n"
        "danger:\n"
        "  allow_multi_patch: false\n"
        "  allow_full_preflight: false\n"
        "  allow_skip_sandbox: false\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == 1
    assert "requires danger.allow_multi_patch: true" in result.output


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_multi_patch_allowed_with_danger_gate(mock_get_exec, mock_surveyor_cls, tmp_path):
    """max_patches > 1 WITH danger.allow_multi_patch -> proceeds (executor mock handles it)."""
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test BP\n"
        "agent:\n"
        "  approval: auto\n"
        "  max_patches: 2\n"
        "modules:\n"
        "  - id: src\n"
        "    type: Ingress\n"
        "    label: Src\n"
        "    config: {format: csv, path: /nonexistent/data.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "aqueduct_config: \"1.0\"\n"
        "danger:\n"
        "  allow_multi_patch: true\n"
        "  allow_full_preflight: false\n"
        "  allow_skip_sandbox: false\n",
        encoding="utf-8",
    )

    mock_exec = MagicMock()
    mock_exec.return_value = MagicMock(status="success")
    mock_get_exec.return_value = mock_exec

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    # Should proceed (not exit 1)
    assert result.exit_code == 0


@patch("aqueduct.surveyor.surveyor.Surveyor")
@patch("aqueduct.executor.get_executor")
def test_multi_patch_cli_flag_overrides_danger_gate(mock_get_exec, mock_surveyor_cls, tmp_path):
    """--allow-multi-patch flag overrides danger.allow_multi_patch=false."""
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test BP\n"
        "agent:\n"
        "  approval: auto\n"
        "  max_patches: 2\n"
        "modules:\n"
        "  - id: src\n"
        "    type: Ingress\n"
        "    label: Src\n"
        "    config: {format: csv, path: /nonexistent/data.csv}\n"
        "edges: []\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "aqueduct_config: \"1.0\"\n"
        "danger:\n"
        "  allow_multi_patch: false\n",
        encoding="utf-8",
    )

    mock_exec = MagicMock()
    mock_exec.return_value = MagicMock(status="success")
    mock_get_exec.return_value = mock_exec

    runner = CliRunner()
    result = runner.invoke(
        cli, ["run", str(bp), "--config", str(cfg), "--allow-multi-patch"]
    )
    assert result.exit_code == 0
