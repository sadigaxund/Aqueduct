"""Airflow integration unit and scenario tests.

Covers:
- AqueductOperator._build_command
- AqueductOperator._resolved_patches_dir
- AqueductOperator.execute (exit codes 0, 2, 3, other)
- AqueductOperator.resume_from_patch
- AqueductPatchTrigger.serialize
- AqueductPatchTrigger._matches_run
- AqueductPatchTrigger._check_once
- AqueductPatchTrigger.run
- AqueductPatchSensor.execute
- AqueductPatchSensor.resume_from_patch
- aqueduct.integrations.airflow __getattr__ lazy-loading
- pyproject optional-dependencies
- specs.md §10.7 exit-code mapping validation
- Example DAG import (DagBag)
- Integration DAG runs (happy path and self-healing defer/resume flow)
"""

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch
import json
from datetime import timedelta
from pathlib import Path
import pytest

pytestmark = pytest.mark.unit

# Check if real airflow is available
try:
    import airflow
    from airflow.models import BaseOperator
    from airflow.exceptions import AirflowException
    from airflow.sensors.base import BaseSensorOperator
    from airflow.triggers.base import BaseTrigger, TriggerEvent
    AIRFLOW_INSTALLED = True
except ImportError:
    AIRFLOW_INSTALLED = False

if not AIRFLOW_INSTALLED:
    # Define and inject mock classes/modules
    class BaseOperatorMock:
        template_fields = ()
        def __init__(self, task_id, **kwargs):
            self.task_id = task_id
            self.log = MagicMock()
            for k, v in kwargs.items():
                setattr(self, k, v)
        def defer(self, trigger, method_name, timeout=None):
            pass

    class BaseSensorOperatorMock(BaseOperatorMock):
        pass

    class BaseTriggerMock:
        def __init__(self, *args, **kwargs):
            pass

    class TriggerEventMock:
        def __init__(self, data):
            self.data = data

    class AirflowExceptionMock(Exception):
        pass

    def make_fake_module(name, attrs):
        mod = ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod
        return mod

    make_fake_module("airflow", {})
    make_fake_module("airflow.models", {"BaseOperator": BaseOperatorMock})
    make_fake_module("airflow.exceptions", {"AirflowException": AirflowExceptionMock})
    make_fake_module("airflow.sensors", {})
    make_fake_module("airflow.sensors.base", {"BaseSensorOperator": BaseSensorOperatorMock})
    make_fake_module("airflow.triggers", {})
    make_fake_module("airflow.triggers.base", {
        "BaseTrigger": BaseTriggerMock,
        "TriggerEvent": TriggerEventMock,
    })

    # Alias mock exceptions to the ones we expect tests to raise
    AirflowException = AirflowExceptionMock


# ── Lazy Loading Tests ────────────────────────────────────────────────────────

def test_lazy_loading():
    import aqueduct.integrations.airflow as integration

    # Clean from sys.modules to force dynamic loading
    if "aqueduct.integrations.airflow.operator" in sys.modules:
        del sys.modules["aqueduct.integrations.airflow.operator"]
    if "aqueduct.integrations.airflow.sensor" in sys.modules:
        del sys.modules["aqueduct.integrations.airflow.sensor"]
    if "aqueduct.integrations.airflow.trigger" in sys.modules:
        del sys.modules["aqueduct.integrations.airflow.trigger"]

    # Verify attributes are resolved and correctly load modules
    assert integration.AqueductOperator is not None
    assert integration.AqueductPatchSensor is not None
    assert integration.AqueductPatchTrigger is not None

    with pytest.raises(AttributeError, match="has no attribute 'NonExistent'"):
        integration.NonExistent


# ── AqueductOperator Unit Tests ────────────────────────────────────────────────

def test_operator_build_command():
    from aqueduct.integrations.airflow.operator import AqueductOperator

    # Case 1: Minimal options
    op = AqueductOperator(task_id="t1", blueprint="my_bp.yml", run_id="my_run")
    assert op._build_command() == ["aqueduct", "run", "my_bp.yml", "--run-id", "my_run"]

    # Case 2: Custom config and extra_args
    op2 = AqueductOperator(
        task_id="t2",
        blueprint="my_bp.yml",
        run_id="my_run",
        config="my_config.yml",
        extra_args=["--parallel", "--from", "m1"]
    )
    assert op2._build_command() == [
        "aqueduct", "run", "my_bp.yml", "--run-id", "my_run",
        "--config", "my_config.yml",
        "--parallel", "--from", "m1"
    ]


def test_operator_resolved_patches_dir():
    from aqueduct.integrations.airflow.operator import AqueductOperator

    # Case 1: explicit patches_dir
    op = AqueductOperator(task_id="t1", blueprint="dir/my_bp.yml", patches_dir="/tmp/custom_patches")
    assert op._resolved_patches_dir() == "/tmp/custom_patches"

    # Case 2: default resolution
    op2 = AqueductOperator(task_id="t2", blueprint="dir/my_bp.yml")
    expected = str(Path("dir/my_bp.yml").resolve().parent / "patches")
    assert op2._resolved_patches_dir() == expected


def test_operator_template_fields_and_env():
    from aqueduct.integrations.airflow.operator import AqueductOperator

    assert AqueductOperator.template_fields == ("blueprint", "run_id", "extra_args", "env")

    op = AqueductOperator(task_id="t1", blueprint="bp.yml", env={"MY_VAR": "VAL1"})
    
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        op.execute(context={})
        
        # Check that environment was passed and merged
        called_args, called_kwargs = mock_run.call_args
        called_env = called_kwargs.get("env")
        assert called_env is not None
        assert called_env["MY_VAR"] == "VAL1"
        # Check that it didn't replace os.environ but merged
        assert "PATH" in called_env


def test_operator_execute_exit_codes():
    from aqueduct.integrations.airflow.operator import AqueductOperator
    from aqueduct.integrations.airflow.trigger import AqueductPatchTrigger
    
    # Case 0: SUCCESS
    op = AqueductOperator(task_id="t1", blueprint="bp.yml", run_id="run-0")
    with patch("subprocess.run") as mock_run, patch.object(op, "defer") as mock_defer:
        mock_run.return_value = MagicMock(returncode=0)
        res = op.execute(context={})
        assert res == {"run_id": "run-0", "exit_code": 0}
        mock_defer.assert_not_called()

    # Case 2: DATA_OR_RUNTIME
    op = AqueductOperator(task_id="t1", blueprint="bp.yml", run_id="run-2")
    with patch("subprocess.run") as mock_run, patch.object(op, "defer") as mock_defer:
        mock_run.return_value = MagicMock(returncode=2)
        with pytest.raises(AirflowException, match=r"run-2"):
            op.execute(context={})
        mock_defer.assert_not_called()

    # Case 3: HEAL_PENDING
    op = AqueductOperator(
        task_id="t1",
        blueprint="bp.yml",
        run_id="run-3",
        poll_interval=15.0,
        patch_timeout=120.0
    )
    with patch("subprocess.run") as mock_run, patch.object(op, "defer") as mock_defer:
        mock_run.return_value = MagicMock(returncode=3)
        res = op.execute(context={})
        assert res == {}
        mock_defer.assert_called_once()
        call_kwargs = mock_defer.call_args[1]
        assert call_kwargs["method_name"] == "resume_from_patch"
        assert call_kwargs["timeout"] == timedelta(seconds=120.0)
        trigger = call_kwargs["trigger"]
        assert isinstance(trigger, AqueductPatchTrigger)
        assert trigger.run_id == "run-3"
        assert trigger.blueprint == "bp.yml"
        assert trigger.poll_interval == 15.0

    # Case other: 1, 4, 5
    for rc in [1, 4, 5]:
        op = AqueductOperator(task_id="t1", blueprint="bp.yml", run_id=f"run-{rc}")
        with patch("subprocess.run") as mock_run, patch.object(op, "defer") as mock_defer:
            mock_run.return_value = MagicMock(returncode=rc)
            with pytest.raises(AirflowException, match=r"unexpected exit code"):
                op.execute(context={})
            mock_defer.assert_not_called()


def test_operator_resume_from_patch():
    from aqueduct.integrations.airflow.operator import AqueductOperator

    op = AqueductOperator(task_id="t1", blueprint="bp.yml", run_id="run-resume")

    # approved
    with patch.object(op, "execute") as mock_execute:
        mock_execute.return_value = {"status": "success"}
        res = op.resume_from_patch(context={"ctx_key": "val"}, event={"status": "approved", "patch_id": "p-123"})
        mock_execute.assert_called_once_with({"ctx_key": "val"})
        assert res == {"status": "success"}

    # rejected
    with pytest.raises(AirflowException, match=r"rejected.*reason-xyz"):
        op.resume_from_patch(context={}, event={"status": "rejected", "reason": "reason-xyz"})

    # unknown
    with pytest.raises(AirflowException, match=r"unexpected status"):
        op.resume_from_patch(context={}, event={"status": "unknown_status"})


# ── AqueductPatchTrigger Unit Tests ───────────────────────────────────────────

def test_trigger_serialize():
    from aqueduct.integrations.airflow.trigger import AqueductPatchTrigger

    trigger = AqueductPatchTrigger(
        run_id="run-1",
        blueprint="bp.yml",
        patches_dir="patches",
        aqueduct_cmd=["/usr/bin/aqueduct"],
        poll_interval=10.0
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "aqueduct.integrations.airflow.trigger.AqueductPatchTrigger"
    assert kwargs == {
        "run_id": "run-1",
        "blueprint": "bp.yml",
        "patches_dir": "patches",
        "aqueduct_cmd": ["/usr/bin/aqueduct"],
        "poll_interval": 10.0
    }


def test_trigger_matches_run():
    from aqueduct.integrations.airflow.trigger import AqueductPatchTrigger

    # Empty run_id matches anything
    t1 = AqueductPatchTrigger(run_id="", blueprint="bp.yml", patches_dir="patches")
    assert t1._matches_run({"file": "patch_1.json", "rationale": "fixes stuff"}) is True

    t2 = AqueductPatchTrigger(run_id="run-123", blueprint="bp.yml", patches_dir="patches")
    # match in file
    assert t2._matches_run({"file": "/path/to/patches/pending/run-123_patch.json"}) is True
    # match in rationale
    assert t2._matches_run({"file": "other.json", "rationale": "healing run-123 failure"}) is True
    # no match
    assert t2._matches_run({"file": "run-456_patch.json", "rationale": "fixes run-456"}) is False


def test_trigger_check_once():
    from aqueduct.integrations.airflow.trigger import AqueductPatchTrigger

    t = AqueductPatchTrigger(run_id="run-123", blueprint="bp.yml", patches_dir="patches")

    # Case 1: CLI non-zero exit
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        assert t._check_once() == ("pending", None, None)

    # Case 2: Malformed JSON
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="invalid json")
        assert t._check_once() == ("pending", None, None)

    # Case 3: Applied JSON entry matching run_id
    with patch("subprocess.run") as mock_run:
        payload = [
            {"status": "applied", "file": "run-123_patch.json", "patch_id": "p-123"}
        ]
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps(payload))
        assert t._check_once() == ("approved", "p-123", None)

    # Case 4: Rejected entry matching run_id
    with patch("subprocess.run") as mock_run:
        payload = [
            {"status": "rejected", "file": "run-123_patch.json", "patch_id": "p-123", "rationale": "no good"}
        ]
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps(payload))
        assert t._check_once() == ("rejected", "p-123", "no good")

    # Case 5: Only pending entries
    with patch("subprocess.run") as mock_run:
        payload = [
            {"status": "pending", "file": "run-123_patch.json", "patch_id": "p-123"}
        ]
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps(payload))
        assert t._check_once() == ("pending", None, None)


def test_trigger_run_approved_sync():
    import asyncio
    from aqueduct.integrations.airflow.trigger import AqueductPatchTrigger

    t = AqueductPatchTrigger(run_id="run-123", blueprint="bp.yml", patches_dir="patches", poll_interval=5.0)

    checks = [
        ("pending", None, None),
        ("approved", "p-123", None)
    ]
    check_index = 0

    def mock_check_once():
        nonlocal check_index
        res = checks[check_index]
        check_index += 1
        return res

    async def run_test():
        events = []
        async for event in t.run():
            events.append(event)
        return events

    with patch.object(t, "_check_once", side_effect=mock_check_once), patch("asyncio.sleep") as mock_sleep:
        events = asyncio.run(run_test())
        assert len(events) == 1
        assert events[0].data == {"status": "approved", "patch_id": "p-123", "run_id": "run-123"}
        mock_sleep.assert_called_once_with(5.0)


# ── AqueductPatchSensor Unit Tests ────────────────────────────────────────────

def test_sensor_execute():
    from aqueduct.integrations.airflow.sensor import AqueductPatchSensor
    from aqueduct.integrations.airflow.trigger import AqueductPatchTrigger

    # Case 1: patch_timeout=None
    s1 = AqueductPatchSensor(task_id="s1", run_id="run-1", blueprint="bp.yml", patches_dir="patches")
    with patch.object(s1, "defer") as mock_defer:
        s1.execute(context={})
        mock_defer.assert_called_once()
        call_kwargs = mock_defer.call_args[1]
        assert call_kwargs["method_name"] == "resume_from_patch"
        assert call_kwargs["timeout"] is None
        trigger = call_kwargs["trigger"]
        assert isinstance(trigger, AqueductPatchTrigger)
        assert trigger.run_id == "run-1"
        assert trigger.blueprint == "bp.yml"
        assert trigger.patches_dir == "patches"

    # Case 2: numeric patch_timeout
    s2 = AqueductPatchSensor(task_id="s2", run_id="run-2", blueprint="bp.yml", patch_timeout=60.0)
    with patch.object(s2, "defer") as mock_defer:
        s2.execute(context={})
        call_kwargs = mock_defer.call_args[1]
        assert call_kwargs["timeout"] == timedelta(seconds=60.0)


def test_sensor_resume_from_patch():
    from aqueduct.integrations.airflow.sensor import AqueductPatchSensor

    s = AqueductPatchSensor(task_id="s1", run_id="run-1", blueprint="bp.yml")

    # approved
    event = {"status": "approved", "patch_id": "p-1"}
    assert s.resume_from_patch(context={}, event=event) == event

    # rejected
    with pytest.raises(AirflowException, match=r"rejected.*reason-abc"):
        s.resume_from_patch(context={}, event={"status": "rejected", "reason": "reason-abc"})

    # unknown
    with pytest.raises(AirflowException, match=r"unexpected status"):
        s.resume_from_patch(context={}, event={"status": "unknown"})


# ── pyproject.toml & specs.md Verification Tests ────────────────────────────────

def test_pyproject_airflow_extras():
    import tomllib
    from pathlib import Path

    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)

    extras = data.get("project", {}).get("optional-dependencies", {})
    assert "airflow" in extras
    assert "apache-airflow>=2.7" in extras["airflow"]

    assert "schedulers" in extras
    assert "aqueduct-core[airflow]" in extras["schedulers"]

    assert "all" in extras
    assert "aqueduct-core[schedulers]" in extras["all"]


def test_specs_exit_codes():
    from pathlib import Path
    import re
    from aqueduct import exit_codes

    specs_path = Path(__file__).resolve().parents[1] / "docs" / "specs.md"
    with open(specs_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Search for the exit-codes table or lines of the form: | `0` | `SUCCESS` | ...
    pattern = re.compile(r"\|\s*`(\d+)`\s*\|\s*`([A-Z_]+)`\s*\|")
    matches = pattern.findall(content)

    assert len(matches) > 0, "No exit-code table found in specs.md"

    expected_codes = {
        "SUCCESS": exit_codes.SUCCESS,
        "CONFIG_ERROR": exit_codes.CONFIG_ERROR,
        "DATA_OR_RUNTIME": exit_codes.DATA_OR_RUNTIME,
        "HEAL_PENDING": exit_codes.HEAL_PENDING,
        "VALIDATION_GATE": exit_codes.VALIDATION_GATE,
        "USAGE_ERROR": exit_codes.USAGE_ERROR,
    }

    found_codes = {}
    for code_str, const_name in matches:
        found_codes[const_name] = int(code_str)

    for k, v in expected_codes.items():
        assert found_codes.get(k) == v, f"Exit code mismatch in specs.md for {k}: expected {v}, found {found_codes.get(k)}"


# ── Airflow Integration / Scenario Tests ──────────────────────────────────────

@pytest.mark.airflow
def test_dagbag_import():
    if not AIRFLOW_INSTALLED:
        pytest.skip("Airflow is not installed, skipping DagBag import test")

    from airflow.models import DagBag
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        dag_file = Path(tmpdir) / "test_dag.py"
        dag_file.write_text("""
from datetime import datetime
from airflow import DAG
from aqueduct.integrations.airflow import AqueductOperator

with DAG(
    dag_id="test_aqueduct_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    op = AqueductOperator(
        task_id="run_etl",
        blueprint="etl.blueprint.yml",
        run_id="run_123",
    )
""")
        db = DagBag(dag_folder=tmpdir, include_examples=False)
        assert len(db.import_errors) == 0, f"Import errors: {db.import_errors}"
        assert "test_aqueduct_dag" in db.dags


@pytest.mark.airflow
def test_airflow_integration_happy_path(tmp_path):
    if not AIRFLOW_INSTALLED:
        pytest.skip("Airflow is not installed, skipping integration tests")

    from airflow import DAG
    from aqueduct.integrations.airflow import AqueductOperator

    # We need input data
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("a,b\n1,2\n3,4")
    output_csv = tmp_path / "output.csv"

    # Create a tiny blueprint
    blueprint_path = tmp_path / "bp.yml"
    blueprint_path.write_text(f"""
aqueduct: '1.0'
id: integration_happy
name: Happy Integration
context:
  input_path: {str(input_csv)}
  output_path: {str(output_csv)}
modules:
  - id: src
    type: Ingress
    config:
      format: csv
      path: ${{ctx.input_path}}
  - id: sink
    type: Egress
    config:
      format: csv
      path: ${{ctx.output_path}}
      mode: overwrite
edges:
  - from: src
    to: sink
""")

    # Instantiate operator
    import sys
    op = AqueductOperator(
        task_id="test_happy_task",
        blueprint=str(blueprint_path),
        run_id="run-happy-123",
        aqueduct_cmd=[sys.executable, "-m", "aqueduct.cli"],
    )

    # Let's run execute
    res = op.execute(context={})
    assert res == {"run_id": "run-happy-123", "exit_code": 0}
    assert output_csv.exists()


@pytest.mark.airflow
def test_airflow_integration_defect_healing(tmp_path):
    if not AIRFLOW_INSTALLED:
        # Simulate/Mock the entire flow to test the execution contract of operator and trigger deferral loop
        from aqueduct.integrations.airflow.operator import AqueductOperator
        from aqueduct.integrations.airflow.trigger import AqueductPatchTrigger

        op = AqueductOperator(
            task_id="test_defect_task",
            blueprint="bp.yml",
            run_id="run-defect-123",
            poll_interval=1.0,
            patch_timeout=10.0,
        )

        # 1. Run execute first time. Mock subprocess.run to return exit code 3 (HEAL_PENDING).
        # We expect it to call defer, raising a simulated deferral event or calling mock defer.
        with patch("subprocess.run") as mock_run, patch.object(op, "defer") as mock_defer:
            mock_run.return_value = MagicMock(returncode=3)
            res = op.execute(context={})
            assert res == {}
            mock_defer.assert_called_once()
            
            # Extract arguments passed to defer
            trigger = mock_defer.call_args[1]["trigger"]
            method_name = mock_defer.call_args[1]["method_name"]
            
            assert isinstance(trigger, AqueductPatchTrigger)
            assert method_name == "resume_from_patch"

        # 2. Simulate the Trigger checking.
        # Check matching run logic
        assert trigger._matches_run({"file": "run-defect-123_patch.json"}) is True

        # Mock the trigger's subprocess checks.
        # First check returns pending, second returns approved.
        trigger_mock_runs = [
            # Check 1: returns pending
            MagicMock(returncode=0, stdout=json.dumps([{"status": "pending", "file": "run-defect-123_patch.json"}])),
            # Check 2: returns approved
            MagicMock(returncode=0, stdout=json.dumps([{"status": "applied", "file": "run-defect-123_patch.json", "patch_id": "p-001"}])),
        ]

        with patch("subprocess.run", side_effect=trigger_mock_runs), patch("asyncio.sleep") as mock_sleep:
            import asyncio
            events = []
            async def run_trigger():
                async for event in trigger.run():
                    events.append(event)
            asyncio.run(run_trigger())
            assert len(events) == 1
            assert events[0].data == {"status": "approved", "patch_id": "p-001", "run_id": "run-defect-123"}
            mock_sleep.assert_called_once_with(1.0)

        # 3. Simulate resuming from the patch event.
        # It calls resume_from_patch on the operator.
        # This re-invokes execute, which now succeeds (mock exit code 0).
        with patch.object(op, "execute", return_value={"run_id": "run-defect-123", "exit_code": 0}) as mock_execute:
            res = op.resume_from_patch(context={}, event=events[0].data)
            mock_execute.assert_called_once()
            assert res == {"run_id": "run-defect-123", "exit_code": 0}
            
    else:
        # Run under real Airflow if installed
        # Since running real Airflow triggers requires full triggerer process,
        # we still mock the defer/resume interaction internally to run deterministically.
        from aqueduct.integrations.airflow.operator import AqueductOperator
        
        op = AqueductOperator(
            task_id="test_defect_task",
            blueprint="bp.yml",
            run_id="run-defect-123",
        )
        
        with patch("subprocess.run") as mock_run, patch.object(op, "defer") as mock_defer:
            mock_run.return_value = MagicMock(returncode=3)
            op.execute(context={})
            mock_defer.assert_called_once()
            
            trigger = mock_defer.call_args[1]["trigger"]
            
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps([{"status": "applied", "file": "run-defect-123_patch.json", "patch_id": "p-001"}])
            )
            # Run check once to verify it sees the approved patch
            status, patch_id, rationale = trigger._check_once()
            assert status == "approved"
            assert patch_id == "p-001"
            
        with patch.object(op, "execute", return_value={"run_id": "run-defect-123", "exit_code": 0}):
            res = op.resume_from_patch(context={}, event={"status": "approved", "patch_id": "p-001", "run_id": "run-defect-123"})
            assert res == {"run_id": "run-defect-123", "exit_code": 0}
