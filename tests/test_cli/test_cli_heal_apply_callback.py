import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from click.testing import CliRunner

from aqueduct.agent.budget import StopReason
from aqueduct.cli import cli, _run_patch_gates_inline
from aqueduct.patch.grammar import PatchSpec, SetModuleConfigKeyOp
from aqueduct.surveyor.models import FailureContext

pytestmark = pytest.mark.integration

def test_run_patch_gates_inline_accepts_iteration_run_id(tmp_path):
    """Verify that _run_patch_gates_inline accepts iteration_run_id keyword argument and runs without TypeError."""
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text("""
aqueduct: "1.0"
id: test_bp
name: Test BP
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: csv
      path: data.csv
edges: []
""", encoding="utf-8")

    # Use correct operation instances
    op = SetModuleConfigKeyOp(
        op="set_module_config_key",
        module_id="m1",
        key="path",
        value="new_data.csv"
    )

    patch_spec = PatchSpec(
        patch_id="p1",
        rationale="test",
        operations=[op]
    )

    mock_surveyor = MagicMock()
    mock_bundle = MagicMock()
    mock_bundle.observability = None
    
    # Call with iteration_run_id
    res = _run_patch_gates_inline(
        patch=patch_spec,
        blueprint_path=bp_file,
        bundle=mock_bundle,
        surveyor=mock_surveyor,
        failed_module="m1",
        iteration_run_id="iter-run-123",
        blueprint_id="test_bp",
        engine="spark",
        sandbox_mode="off",
    )

    # Should not raise TypeError and return a tuple
    assert isinstance(res, tuple)
    assert len(res) == 4
    
    # Print calls if it fails
    print("CALLS:", mock_surveyor.record_patch_simulation.call_args_list)
    assert mock_surveyor.record_patch_simulation.call_count == 3
    
    # Verify the first call is gate="lineage" and run_id="iter-run-123"
    first_call = mock_surveyor.record_patch_simulation.call_args_list[0]
    assert first_call[1]["gate"] == "lineage"
    assert first_call[1]["run_id"] == "iter-run-123"
    assert first_call[1]["blueprint_id"] == "test_bp"

    # Verify the second call is gate="sandbox" and run_id="iter-run-123"
    second_call = mock_surveyor.record_patch_simulation.call_args_list[1]
    assert second_call[1]["gate"] == "sandbox"
    assert second_call[1]["run_id"] == "iter-run-123"
    assert second_call[1]["blueprint_id"] == "test_bp"

    # Verify the third call is gate="explain" and run_id="iter-run-123"
    third_call = mock_surveyor.record_patch_simulation.call_args_list[2]
    assert third_call[1]["gate"] == "explain"
    assert third_call[1]["run_id"] == "iter-run-123"
    assert third_call[1]["blueprint_id"] == "test_bp"

@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.cli._resolve_obs_db")
@patch("duckdb.connect")
def test_cli_run_self_heal_wires_apply_callback(
    mock_connect, mock_resolve_obs_db, mock_get_executor, mock_generate_patch, tmp_path
):
    """Verify that `aqueduct run` self-healing wires the apply_callback correctly and calls it within the loop."""
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text("""
aqueduct: "1.0"
id: test_bp
name: Test BP
agent:
  approval: auto
  sandbox_mode: "off"
  max_patches: 2
  guardrails:
    forbidden_ops: ["remove_module"]
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: csv
      path: data.csv
edges: []
""", encoding="utf-8")

    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text("""
aqueduct_config: "1.0"
agent:
  provider: openai_compat
  base_url: "http://localhost:8000"
danger:
  allow_multi_patch: true
  allow_skip_sandbox: true
""", encoding="utf-8")

    # Mock the executor to fail on first run (triggering self-healing)
    mock_exec = MagicMock()
    mock_exec.return_value = MagicMock(
        status="error",
        module_results=[
            MagicMock(module_id="m1", status="error", error="Boom", exception=ValueError("Boom"))
        ]
    )
    mock_get_executor.return_value = mock_exec

    # Mock generate_agent_patch to return a dummy result
    from aqueduct.agent import AgentPatchResult
    patch_spec = PatchSpec(
        patch_id="p1",
        rationale="test",
        operations=[
            SetModuleConfigKeyOp(
                op="set_module_config_key",
                module_id="m1",
                key="path",
                value="new_data.csv"
            )
        ]
    )
    mock_generate_patch.return_value = AgentPatchResult(
        patch=patch_spec,
        attempts=1,
        stop_reason=StopReason.SOLVED,
        tokens_in_total=10,
        tokens_out_total=20,
        attempt_records=[],
    )

    runner = CliRunner()
    with patch("aqueduct.agent.memory.find_pending", return_value=None), \
         patch("aqueduct.agent.memory.find_replay_candidate", return_value=None):
        result = runner.invoke(cli, ["run", str(bp_file), "--config", str(cfg_file), "--store-dir", str(tmp_path)])

    print("OUTPUT:", result.output)
    print("EXCEPTION:", result.exception)

    # Verify generate_agent_patch was called with apply_callback
    assert mock_generate_patch.called
    agent_cfg = mock_generate_patch.call_args[1]["agent_cfg"]
    assert agent_cfg.apply_callback is not None
    apply_cb = agent_cfg.apply_callback

    # Test the apply_callback with a valid patch
    success, err_class, msg, _ = apply_cb(patch_spec)
    assert success is True

    # Test the apply_callback with an invalid patch violating guardrails
    from aqueduct.patch.grammar import RemoveModuleOp
    invalid_patch = PatchSpec(
        patch_id="p2",
        rationale="test delete",
        operations=[
            RemoveModuleOp(
                op="remove_module",
                module_id="m1"
            )
        ]
    )
    success, err_class, msg, _ = apply_cb(invalid_patch)
    assert success is False
    assert err_class == "guardrail_violation"

@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.stores.read.open_obs_read")
def test_cli_heal_wires_apply_callback(
    mock_open, mock_generate_patch, tmp_path
):
    """Verify that `aqueduct heal` wires the apply_callback correctly and validates guardrails from store metadata."""
    # 1. Setup db failure context row (backend-aware store read)
    mock_cur = MagicMock()
    mock_store = MagicMock()
    mock_store.connect.return_value.__enter__.return_value = mock_cur
    mock_open.return_value = mock_store

    # row_records mock query results
    # run_id, blueprint_id, failed_module, error_message, stack_trace, manifest_json, provenance_json, started_at, finished_at, engine
    fc_row = (
        "run-123",
        "test_bp",
        "m1",
        "Test error",
        "Traceback test",
        # manifest_json carries the guardrails config under agent
        '{"id": "test_bp", "modules": [{"id": "m1", "type": "Ingress"}], "agent": {"guardrails": {"forbidden_ops": ["remove_module"]}}}',
        None,  # provenance_json
        "2023-01-01T00:00:00Z",
        "2023-01-01T00:01:00Z",
        "spark",  # engine
    )
    mock_cur.fetchone.return_value = fc_row

    # Mock generate_agent_patch
    from aqueduct.agent import AgentPatchResult
    patch_spec = PatchSpec(
        patch_id="p1",
        rationale="test",
        operations=[
            SetModuleConfigKeyOp(
                op="set_module_config_key",
                module_id="m1",
                key="path",
                value="new_data.csv"
            )
        ]
    )
    mock_generate_patch.return_value = AgentPatchResult(
        patch=patch_spec,
        attempts=1,
        stop_reason=StopReason.SOLVED,
        tokens_in_total=10,
        tokens_out_total=20,
        attempt_records=[],
    )

    runner = CliRunner()
    with patch("aqueduct.agent.memory.find_pending", return_value=None), \
         patch("aqueduct.agent.memory.find_replay_candidate", return_value=None):
        result = runner.invoke(cli, ["heal", "run-123", "--store-dir", str(tmp_path)])

    assert mock_generate_patch.called
    agent_cfg = mock_generate_patch.call_args[1]["agent_cfg"]
    assert agent_cfg.apply_callback is not None
    apply_cb = agent_cfg.apply_callback

    # Test the apply_callback with a valid patch
    success, err_class, msg, _ = apply_cb(patch_spec)
    assert success is True

    # Test the apply_callback with a forbidden op (remove_module)
    from aqueduct.patch.grammar import RemoveModuleOp
    invalid_patch = PatchSpec(
        patch_id="p2",
        rationale="test delete",
        operations=[
            RemoveModuleOp(
                op="remove_module",
                module_id="m1"
            )
        ]
    )
    success, err_class, msg, _ = apply_cb(invalid_patch)
    assert success is False
    assert err_class == "guardrail_violation"


@pytest.mark.unit
def test_apply_patch_to_dict_channel_missing_op_detected(tmp_path):
    """replace_module_config on a Channel that omits 'op' → patched dict has Channel without op (schema_drift)."""
    from aqueduct.patch.apply import apply_patch_to_dict
    from aqueduct.patch.grammar import ReplaceModuleConfigOp, PatchSpec

    bp_raw = {
        "aqueduct": "1.0",
        "id": "test_bp",
        "modules": [
            {"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT 1"}},
        ],
        "edges": [],
    }
    patch_spec = PatchSpec(
        patch_id="p1", rationale="replace without op",
        operations=[ReplaceModuleConfigOp(op="replace_module_config", module_id="ch1", config={"query": "SELECT 1"})],
    )
    bp_after = apply_patch_to_dict(bp_raw, patch_spec)
    for _m in (bp_after.get("modules") or []):
        if _m.get("type") == "Channel":
            assert "op" not in (_m.get("config") or {}), "Channel should have no 'op' after replace_module_config"
            return
    pytest.fail("No Channel module found")


@pytest.mark.unit
def test_apply_patch_to_dict_ingress_missing_format_detected(tmp_path):
    """replace_module_config on an Ingress that omits 'format' → patched dict has Ingress without format (schema_drift)."""
    from aqueduct.patch.apply import apply_patch_to_dict
    from aqueduct.patch.grammar import ReplaceModuleConfigOp, PatchSpec

    bp_raw = {
        "aqueduct": "1.0",
        "id": "test_bp",
        "modules": [
            {"id": "m1", "type": "Ingress", "config": {"format": "csv", "path": "data.csv"}},
        ],
        "edges": [],
    }
    patch_spec = PatchSpec(
        patch_id="p1", rationale="replace without format",
        operations=[ReplaceModuleConfigOp(op="replace_module_config", module_id="m1", config={"path": "data.csv"})],
    )
    bp_after = apply_patch_to_dict(bp_raw, patch_spec)
    for _m in (bp_after.get("modules") or []):
        if _m.get("type") == "Ingress":
            assert "format" not in (_m.get("config") or {}), "Ingress should have no 'format' after replace_module_config"
            return
    pytest.fail("No Ingress module found")


@pytest.mark.unit
def test_apply_patch_to_dict_skips_guardrail_after_schema_drift(tmp_path):
    """ReplaceModuleConfigOp on a Channel without 'op' is a schema issue, not a guardrails issue."""
    from aqueduct.patch.apply import apply_patch_to_dict, _check_guardrails, PatchError
    from aqueduct.patch.grammar import ReplaceModuleConfigOp, PatchSpec

    bp_raw = {
        "aqueduct": "1.0",
        "id": "test_bp",
        "modules": [
            {"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT 1"}},
        ],
        "edges": [],
        "agent": {"guardrails": {"forbidden_ops": ["replace_module_config"]}},
    }
    patch_spec = PatchSpec(
        patch_id="p1", rationale="replace without op",
        operations=[ReplaceModuleConfigOp(op="replace_module_config", module_id="ch1", config={"query": "SELECT 1"})],
    )
    bp_after = apply_patch_to_dict(bp_raw, patch_spec)

    # Confirm schema_drift condition in patched blueprint
    for _m in (bp_after.get("modules") or []):
        if _m.get("type") == "Channel":
            assert "op" not in (_m.get("config") or {})
    # Guardrail check would raise PatchError independently — confirm it catches
    # the forbidden op when schema is intact
    with pytest.raises(PatchError):
        # Test guardrails on the ORIGINAL (unpatched) bp — should catch the
        # forbidden op. This confirms guardrails work when schema is valid.
        _check_guardrails(patch_spec, bp_raw, provenance_map=None)
