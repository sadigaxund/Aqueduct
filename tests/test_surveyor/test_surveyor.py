"""Tests for the Surveyor observability recorder."""

from __future__ import annotations

import tempfile
from pathlib import Path
import pytest
pytestmark = pytest.mark.unit
from unittest.mock import MagicMock, patch

from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.surveyor.surveyor import Surveyor
from aqueduct.config import WebhookEndpointConfig


def test_surveyor_record_fires_webhook():
    manifest = MagicMock()
    manifest.blueprint_id = "test-bp"
    manifest.name = "Test Blueprint"
    manifest.to_dict.return_value = {"id": "test-bp"}
    manifest.provenance_map = None
    
    config = WebhookEndpointConfig(url="http://test.com")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        surveyor = Surveyor(manifest, store_dir=tmp_path, webhook_config=config)
        surveyor.start("run-123")
        
        result = ExecutionResult(
            blueprint_id="test-bp",
            run_id="run-123",
            status="error",
            module_results=(
                ModuleResult(module_id="mod1", status="error", error="Boom!"),
            )
        )
        
        with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_fire:
            surveyor.record(result)
            mock_fire.assert_called_once()
            args = mock_fire.call_args[0]
            # fire_webhook(config, full_payload, template_vars)
            assert args[0].url == "http://test.com"
            assert args[1]["failed_module"] == "mod1"
            assert args[1]["error_message"] == "Boom!"
            assert args[2]["blueprint_name"] == "Test Blueprint"
            assert args[2]["run_id"] == "run-123"


def test_surveyor_record_no_webhook_on_success():
    manifest = MagicMock()
    manifest.blueprint_id = "test-bp"
    manifest.name = "Test Blueprint"
    manifest.to_dict.return_value = {"id": "test-bp"}
    manifest.provenance_map = None
    
    config = WebhookEndpointConfig(url="http://test.com")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        surveyor = Surveyor(manifest, store_dir=tmp_path, webhook_config=config)
        surveyor.start("run-123")
        
        result = ExecutionResult(
            blueprint_id="test-bp",
            run_id="run-123",
            status="success",
            module_results=()
        )
        
        with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_fire:
            surveyor.record(result)
            mock_fire.assert_not_called()


class TestEvaluateRegulatorSignalOverride:
    """Tests that evaluate_regulator() checks signal_overrides before probe_signals."""

    def _make_manifest(self, probe_id="probe1", regulator_id="reg1"):
        from unittest.mock import MagicMock
        from aqueduct.compiler.models import Manifest
        edge = MagicMock()
        edge.from_id = probe_id
        edge.to_id = regulator_id
        edge.port = "signal"
        manifest = MagicMock(spec=Manifest)
        manifest.blueprint_id = "test-bp"
        manifest.edges = [edge]
        return manifest

    def test_override_false_blocks_even_if_probe_says_true(self, tmp_path):
        import duckdb
        from datetime import datetime, timezone
        from aqueduct.surveyor.surveyor import Surveyor, _SIGNAL_OVERRIDES_DDL
        manifest = self._make_manifest()
        store = tmp_path / "signals"
        store.mkdir()
        signals_db = store / "obs.db"
        conn = duckdb.connect(str(signals_db))
        conn.execute(_SIGNAL_OVERRIDES_DDL)
        conn.execute(
            "INSERT INTO signal_overrides VALUES (?, ?, ?, ?)",
            ["probe1", False, None, datetime.now(tz=timezone.utc).isoformat()],
        )
        conn.close()

        s = Surveyor(manifest, store_dir=store)
        s.start("run-x")
        assert s.evaluate_regulator("reg1") is False

    def test_no_override_returns_true_when_no_probe_signals(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        manifest = self._make_manifest()
        store = tmp_path / "signals"
        store.mkdir()
        s = Surveyor(manifest, store_dir=store)
        s.start("run-x")
        assert s.evaluate_regulator("reg1") is True


class TestSurveyorBlueprintSourceYaml:
    def test_surveyor_populates_blueprint_source_yaml_when_file_exists(self, tmp_path):
        """Surveyor reads blueprint YAML and sets blueprint_source_yaml on FailureContext."""
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.models import ExecutionResult, ModuleResult

        bp_path = tmp_path / "blueprint.yml"
        bp_path.write_text("id: my_blueprint\nname: Test", encoding="utf-8")

        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        surveyor = Surveyor(manifest, store_dir=tmp_path, blueprint_path=bp_path)
        surveyor.start("r1")

        result = ExecutionResult(
            blueprint_id="b1",
            run_id="r1",
            status="error",
            module_results=[ModuleResult(module_id="m1", status="error", error="err")],
        )
        ctx = surveyor.record(result)
        assert ctx is not None
        assert ctx.blueprint_source_yaml is not None
        assert "my_blueprint" in ctx.blueprint_source_yaml

    def test_surveyor_sets_blueprint_source_yaml_none_when_file_missing(self, tmp_path):
        """Surveyor sets blueprint_source_yaml=None when blueprint_path is None."""
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.models import ExecutionResult, ModuleResult

        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        surveyor = Surveyor(manifest, store_dir=tmp_path, blueprint_path=None)
        surveyor.start("r1")

        result = ExecutionResult(
            blueprint_id="b1",
            run_id="r1",
            status="error",
            module_results=[ModuleResult(module_id="m1", status="error", error="err")],
        )
        ctx = surveyor.record(result)
        assert ctx is not None
        assert ctx.blueprint_source_yaml is None

class TestSurveyorStores:
    def test_surveyor_uses_injected_stores(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.stores import StoreBundle
        from aqueduct.stores.duckdb_ import DuckDBObsStore, DuckDBLineageStore, DuckDBDepotStore
        
        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        
        obs = DuckDBObsStore(tmp_path / "obs.db")
        lineage = DuckDBLineageStore(tmp_path / "lineage.db")
        depot = DuckDBDepotStore(tmp_path / "depot.db")
        bundle = StoreBundle(obs=obs, lineage=lineage, depot=depot)
        
        surveyor = Surveyor(manifest, store_dir=tmp_path, stores=bundle)
        assert surveyor._obs_store is obs

    def test_surveyor_default_store(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.stores.duckdb_ import DuckDBObsStore
        
        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        
        surveyor = Surveyor(manifest, store_dir=tmp_path)
        assert isinstance(surveyor._obs_store, DuckDBObsStore)
