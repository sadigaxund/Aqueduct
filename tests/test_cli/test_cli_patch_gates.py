import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from aqueduct.cli import _run_patch_gates_inline

pytestmark = [pytest.mark.spark, pytest.mark.integration]

def test_run_patch_gates_inline_returns_4_tuple_and_records(spark, tmp_path):
    """_run_patch_gates_inline returns the expected tuple and records explain gate simulation."""
    blueprint_path = tmp_path / "blueprint.yml"
    blueprint_path.write_text("""
aqueduct: '1.0'
id: test_gates
name: Test Gates
modules:
  - id: m1
    label: Ingress
    type: Ingress
    config: {format: parquet, path: /tmp/in.parquet}
edges: []
""")
    
    mock_patch = MagicMock()
    mock_patch.patch_id = "p_test"
    
    mock_bundle = MagicMock()
    mock_surveyor = MagicMock()
    # mock latest_explain_snapshots to return empty dict
    mock_surveyor.latest_explain_snapshots.return_value = {}
    
    # We need to mock apply_patch_to_dict to return a valid dict
    with patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={"modules": []}), \
         patch("aqueduct.patch.preview.run_lineage_gate", return_value=MagicMock(status="pass", touched_modules=["m1"])), \
         patch("aqueduct.patch.preview.run_sandbox_gate", return_value=MagicMock(status="pass", detail="OK", sample_rows=1000, duration_ms=10)), \
         patch("aqueduct.patch.explain_gate.run_explain_gate", return_value=MagicMock(status="warn", detail="Regression!", duration_ms=5)):
        
        g2, g3, g4, passed = _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=mock_bundle,
            surveyor=mock_surveyor,
            failed_module="m1",
            current_run_id="r1",
            blueprint_id="b1"
        )
    
    assert g2 is not None
    assert g3 is not None
    assert g4 is not None
    assert passed is True  # sandbox passed
    
    # Check recordings
    # 1. lineage
    # 2. sandbox
    # 3. explain
    assert mock_surveyor.record_patch_simulation.call_count == 3
    
    # Check the explain gate call specifically
    calls = mock_surveyor.record_patch_simulation.call_args_list
    explain_call = next(c for c in calls if c.kwargs["gate"] == "explain")
    assert explain_call.kwargs["status"] == "warn"
    assert explain_call.kwargs["detail"] == "Regression!"

def test_run_patch_gates_inline_handles_explain_failure(spark, tmp_path):
    """If explain gate raises, it returns None for g4 but still returns other gates."""
    blueprint_path = tmp_path / "blueprint.yml"
    blueprint_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []")
    
    mock_patch = MagicMock()
    mock_surveyor = MagicMock()
    # explain gate fails
    mock_surveyor.latest_explain_snapshots.side_effect = Exception("DB Boom!")
    
    with patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={}), \
         patch("aqueduct.patch.preview.run_lineage_gate", return_value=MagicMock(status="pass", touched_modules=[])), \
         patch("aqueduct.patch.preview.run_sandbox_gate", return_value=MagicMock(status="pass")):
        
        g2, g3, g4, passed = _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=MagicMock(),
            surveyor=mock_surveyor,
            failed_module="m1",
            current_run_id="r1",
            blueprint_id="b1"
        )
    
    assert g4 is None
    assert g2 is not None
    assert g3 is not None
    assert passed is True
