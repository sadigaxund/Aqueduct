from unittest.mock import MagicMock, patch

import pytest

from aqueduct.cli import _run_patch_gates_inline
from aqueduct.config import AqueductConfig

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
    # Phase 88 — Gate 5 (resolvability) iterates patch.operations; a bare
    # MagicMock() auto-creates a non-iterable attribute here, so it must be
    # set explicitly (no declare_dependency op → gate 5 is not_applicable).
    mock_patch.operations = []

    mock_bundle = MagicMock()
    mock_surveyor = MagicMock()
    # mock latest_explain_snapshots to return empty dict
    mock_surveyor.latest_explain_snapshots.return_value = {}

    # We need to mock apply_patch_to_dict to return a valid dict
    with (
        patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={"modules": []}),
        patch(
            "aqueduct.patch.preview.run_lineage_gate",
            return_value=MagicMock(status="pass", touched_modules=["m1"]),
        ),
        patch(
            "aqueduct.patch.preview.run_sandbox_gate",
            return_value=MagicMock(status="pass", detail="OK", sample_rows=1000, duration_ms=10),
        ),
        patch(
            "aqueduct.patch.explain_gate.run_explain_gate",
            return_value=MagicMock(status="warn", detail="Regression!", duration_ms=5),
        ),
    ):

        g2, g3, g4, g5, passed = _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=mock_bundle,
            surveyor=mock_surveyor,
            failed_module="m1",
            iteration_run_id="r1",
            blueprint_id="b1",
            engine="spark",
            cfg=AqueductConfig(),
        )

    assert g2 is not None
    assert g3 is not None
    assert g4 is not None
    assert g5 is not None
    assert g5.status == "not_applicable"  # no declare_dependency op
    assert passed is True  # sandbox passed, resolvability not_applicable

    # Check recordings
    # 1. engine_config
    # 2. lineage
    # 3. sandbox
    # 4. explain
    # 5. resolvability
    assert mock_surveyor.record_patch_simulation.call_count == 5
    assert [c.kwargs["gate"] for c in mock_surveyor.record_patch_simulation.call_args_list] == [
        "engine_config",
        "lineage",
        "sandbox",
        "explain",
        "resolvability",
    ]

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
    mock_patch.operations = []  # Phase 88 — see comment in the test above
    mock_surveyor = MagicMock()
    # explain gate fails
    mock_surveyor.latest_explain_snapshots.side_effect = Exception("DB Boom!")

    with (
        patch("aqueduct.patch.apply.apply_patch_to_dict", return_value={}),
        patch(
            "aqueduct.patch.preview.run_lineage_gate",
            return_value=MagicMock(status="pass", touched_modules=[]),
        ),
        patch("aqueduct.patch.preview.run_sandbox_gate", return_value=MagicMock(status="pass")),
    ):

        g2, g3, g4, g5, passed = _run_patch_gates_inline(
            patch=mock_patch,
            blueprint_path=blueprint_path,
            bundle=MagicMock(),
            surveyor=mock_surveyor,
            failed_module="m1",
            iteration_run_id="r1",
            blueprint_id="b1",
            engine="spark",
            cfg=AqueductConfig(),
        )

    assert g4 is None
    assert g2 is not None
    assert g3 is not None
    assert g5 is not None
    assert passed is True
