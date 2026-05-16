"""Tests for aqueduct/doctor.py check_secrets() and guardrail typo detection."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from aqueduct.doctor import CheckResult, check_secrets, _check_heal_guardrail_typos


# ── check_secrets() ───────────────────────────────────────────────────────────

class TestCheckSecrets:
    def test_env_provider_always_ok(self):
        result = check_secrets("env")
        assert result.status == "ok"
        assert "env" in result.detail

    def test_aws_boto3_missing_raises_error(self):
        with patch.dict(sys.modules, {"boto3": None}):
            result = check_secrets("aws")
        assert result.status == "fail"
        assert "pip install aqueduct-core[aws]" in result.detail

    def test_aws_boto3_present_ok(self):
        mock_boto3 = MagicMock()
        with patch.dict(sys.modules, {"boto3": mock_boto3}):
            result = check_secrets("aws")
        assert result.status == "ok"

    def test_gcp_sdk_missing_error(self):
        with patch.dict(sys.modules, {"google.cloud.secretmanager": None}):
            result = check_secrets("gcp")
        assert result.status == "fail"
        assert "[gcp]" in result.detail

    def test_azure_sdk_missing_error(self):
        with patch.dict(sys.modules, {"azure.keyvault.secrets": None}):
            result = check_secrets("azure")
        assert result.status == "fail"
        assert "[azure]" in result.detail

    def test_custom_no_resolver_error(self):
        result = check_secrets("custom", resolver=None)
        assert result.status == "fail"
        assert "resolver" in result.detail.lower()

    def test_custom_valid_resolver_ok(self):
        import types
        mod = types.ModuleType("_check_secrets_test_mod")
        mod.fetch = lambda key: "val"
        sys.modules["_check_secrets_test_mod"] = mod
        try:
            result = check_secrets("custom", resolver="_check_secrets_test_mod.fetch")
            assert result.status == "ok"
        finally:
            del sys.modules["_check_secrets_test_mod"]


# ── _check_heal_guardrail_typos() ─────────────────────────────────────────────

def _make_mock_manifest(heal_on=(), never_heal=(), assert_modules=()):
    """Build a minimal manifest-like object for typo detection tests."""
    guardrails = MagicMock()
    guardrails.heal_on_errors = list(heal_on)
    guardrails.never_heal_errors = list(never_heal)

    agent = MagicMock()
    agent.guardrails = guardrails

    modules = []
    for module_id, error_types in assert_modules:
        m = MagicMock()
        m.type = "Assert"
        m.config = {"rules": [{"type": "min_rows", "error_type": et} for et in error_types]}
        modules.append(m)

    manifest = MagicMock()
    manifest.agent = agent
    manifest.modules = modules
    return manifest


class TestHealGuardrailTypos:
    def test_no_entries_no_warnings(self):
        manifest = _make_mock_manifest(heal_on=[], never_heal=[])
        results = _check_heal_guardrail_typos(manifest)
        assert results == []

    def test_matching_error_type_no_warning(self):
        manifest = _make_mock_manifest(
            heal_on=["EmptyDataset"],
            assert_modules=[("assert1", ["EmptyDataset"])]
        )
        results = _check_heal_guardrail_typos(manifest)
        assert not any(r.name == "guardrail_typo" for r in results)

    def test_non_matching_heal_on_warns(self):
        manifest = _make_mock_manifest(
            heal_on=["TypoedErrorType"],
            assert_modules=[("assert1", ["EmptyDataset"])]
        )
        results = _check_heal_guardrail_typos(manifest)
        assert len(results) == 1
        assert results[0].status == "warn"
        assert "TypoedErrorType" in results[0].detail

    def test_never_heal_typo_warns(self):
        manifest = _make_mock_manifest(
            never_heal=["BadTypo"],
            assert_modules=[("assert1", ["RealError"])]
        )
        results = _check_heal_guardrail_typos(manifest)
        assert any("BadTypo" in r.detail for r in results)

    def test_no_assert_modules_any_entry_warns(self):
        manifest = _make_mock_manifest(
            heal_on=["SomeError"],
            assert_modules=[]
        )
        results = _check_heal_guardrail_typos(manifest)
        assert len(results) == 1
        assert results[0].status == "warn"

    def test_no_agent_returns_empty(self):
        manifest = MagicMock()
        manifest.agent = None
        results = _check_heal_guardrail_typos(manifest)
        assert results == []
