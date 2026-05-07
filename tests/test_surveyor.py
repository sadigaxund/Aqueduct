"""Tests for the Surveyor observability recorder."""

from __future__ import annotations

import tempfile
from pathlib import Path
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
