from unittest.mock import MagicMock, patch

import pytest

from aqueduct.cli import _run_patch_gates_inline
from aqueduct.config import AqueductConfig

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_run_patch_gates_inline_returns_4_tuple_and_records(spark, tmp_path):
    """_run_patch_gates_inline returns the expected tuple and records each gate's simulation."""
    blueprint_path = tmp_path / "blueprint.yml"
    blueprint_path.write_text(
        """
aqueduct: '1.0'
id: test_gates
name: Test Gates
modules:
  - id: m1
    label: Ingress
    type: Ingress
    config: {format: parquet, path: /tmp/in.parquet}
edges: []
"""
    )

    mock_patch = MagicMock()
    mock_patch.patch_id = "p_test"
    # Phase 88 — Gate 4 (resolvability) iterates patch.operations; a bare
    # MagicMock() auto-creates a non-iterable attribute here, so it must be
    # set explicitly (no declare_dependency op → gate 4 is not_applicable).
    mock_patch.operations = []

    mock_bundle = MagicMock()
    mock_surveyor = MagicMock()

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
    ):

        g2, g3, g4, passed = _run_patch_gates_inline(
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
    assert g4.status == "not_applicable"  # no declare_dependency op
    assert passed is True  # sandbox passed, resolvability not_applicable

    # Check recordings
    # 1. engine_config
    # 2. lineage
    # 3. sandbox
    # 4. resolvability
    assert mock_surveyor.record_patch_simulation.call_count == 4
    assert [c.kwargs["gate"] for c in mock_surveyor.record_patch_simulation.call_args_list] == [
        "engine_config",
        "lineage",
        "sandbox",
        "resolvability",
    ]
