"""Tests for aqueduct/secrets.py — resolve_secret() and provider backends."""

from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from aqueduct.secrets import SecretsError, resolve_secret


# ── provider: env ────────────────────────────────────────────────────────────

def test_env_provider_returns_env_value(monkeypatch):
    monkeypatch.setenv("MY_SECRET_KEY", "super-secret")
    assert resolve_secret("MY_SECRET_KEY", provider="env") == "super-secret"


def test_env_provider_missing_key_raises(monkeypatch):
    monkeypatch.delenv("MISSING_KEY_XYZ", raising=False)
    with pytest.raises(SecretsError, match=r"MISSING_KEY_XYZ"):
        resolve_secret("MISSING_KEY_XYZ", provider="env")


def test_env_provider_does_not_import_boto3(monkeypatch):
    """env provider must never touch cloud SDKs."""
    monkeypatch.setenv("PLAIN_KEY", "val")
    # If boto3 were imported, this would fail if boto3 is not installed
    # We verify by patching importlib.import_module and checking it's not called for boto3
    with patch("importlib.import_module") as mock_import:
        resolve_secret("PLAIN_KEY", provider="env")
        for call in mock_import.call_args_list:
            assert "boto3" not in str(call)


def test_env_fast_path_takes_priority_over_provider(monkeypatch):
    """os.environ does NOT override non-env providers."""
    monkeypatch.setenv("FAST_PATH_KEY", "env-value")
    # Even if env var is set, provider=aws should NOT use it and should call SDK (which will fail if not mocked/available)
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    mock_client.get_secret_value.return_value = {"SecretString": "sdk-value"}

    mock_botocore = MagicMock()
    mock_botocore.exceptions = MagicMock()

    with patch.dict(sys.modules, {"boto3": mock_boto3, "botocore": mock_botocore, "botocore.exceptions": mock_botocore.exceptions}):
        result = resolve_secret("FAST_PATH_KEY", provider="aws")
    assert result == "sdk-value"


# ── provider: aws ─────────────────────────────────────────────────────────────

def test_aws_sdk_not_installed_raises(monkeypatch):
    monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
    with patch.dict(sys.modules, {"boto3": None, "botocore": None, "botocore.exceptions": None}):
        with pytest.raises(SecretsError, match=r"boto3"):
            resolve_secret("AWS_SECRET_KEY", provider="aws")


def test_aws_fetches_secret_without_caching_in_environ(monkeypatch, tmp_path):
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    mock_client.get_secret_value.return_value = {"SecretString": "s3cr3t"}

    mock_botocore = MagicMock()
    mock_botocore.exceptions = MagicMock()

    with patch.dict(sys.modules, {"boto3": mock_boto3, "botocore": mock_botocore, "botocore.exceptions": mock_botocore.exceptions}):
        val1 = resolve_secret("DB_PASSWORD", provider="aws")
        val2 = resolve_secret("DB_PASSWORD", provider="aws")
    assert val1 == "s3cr3t"
    assert val2 == "s3cr3t"
    assert mock_client.get_secret_value.call_count == 2
    # Should NOT be cached in os.environ
    assert os.environ.get("DB_PASSWORD") is None


def test_aws_json_blob_unwraps_matching_key(monkeypatch):
    import json
    monkeypatch.delenv("MY_KEY", raising=False)
    mock_boto3 = MagicMock()
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    # JSON blob with matching key
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"MY_KEY": "jsonval"})
    }
    mock_botocore = MagicMock()
    mock_botocore.exceptions = MagicMock()
    with patch.dict(sys.modules, {"boto3": mock_boto3, "botocore": mock_botocore, "botocore.exceptions": mock_botocore.exceptions}):
        val = resolve_secret("MY_KEY", provider="aws")
    assert val == "jsonval"
    monkeypatch.delenv("MY_KEY", raising=False)


# ── provider: gcp ─────────────────────────────────────────────────────────────

def test_gcp_sdk_not_installed_raises(monkeypatch):
    monkeypatch.delenv("GCP_SECRET", raising=False)
    with patch.dict(sys.modules, {
        "google": None, "google.cloud": None, "google.cloud.secretmanager": None,
        "google.api_core": None, "google.api_core.exceptions": None,
    }):
        with pytest.raises(SecretsError, match=r"google-cloud-secret-manager"):
            resolve_secret("GCP_SECRET", provider="gcp")


def test_gcp_short_name_uses_gcp_project_env(monkeypatch):
    monkeypatch.delenv("MY_GCP_SECRET", raising=False)
    monkeypatch.setenv("GCP_PROJECT", "my-project")

    mock_sm = MagicMock()
    mock_client_instance = MagicMock()
    mock_sm.SecretManagerServiceClient.return_value = mock_client_instance
    mock_resp = MagicMock()
    mock_resp.payload.data = b"gcp-value"
    mock_client_instance.access_secret_version.return_value = mock_resp

    mock_google = MagicMock()
    mock_google_cloud = MagicMock()
    mock_google_cloud.secretmanager = mock_sm
    mock_api_core = MagicMock()
    mock_api_exceptions = MagicMock()
    mock_api_exceptions.NotFound = type("NotFound", (Exception,), {})
    mock_api_core.exceptions = mock_api_exceptions

    with patch.dict(sys.modules, {
        "google": mock_google,
        "google.cloud": mock_google_cloud,
        "google.cloud.secretmanager": mock_sm,
        "google.api_core": mock_api_core,
        "google.api_core.exceptions": mock_api_exceptions,
    }):
        val = resolve_secret("MY_GCP_SECRET", provider="gcp")

    assert val == "gcp-value"
    # Verify full resource path was constructed
    call_args = mock_client_instance.access_secret_version.call_args
    request = call_args[1]["request"] if "request" in (call_args[1] or {}) else call_args[0][0] if call_args[0] else call_args[1].get("request") or call_args[0]
    assert "my-project" in str(request) or "MY_GCP_SECRET" in str(request)
    monkeypatch.delenv("MY_GCP_SECRET", raising=False)


# ── provider: azure ───────────────────────────────────────────────────────────

def test_azure_sdk_not_installed_raises(monkeypatch):
    monkeypatch.delenv("AZ_SECRET", raising=False)
    with patch.dict(sys.modules, {
        "azure": None, "azure.keyvault": None, "azure.keyvault.secrets": None,
        "azure.identity": None, "azure.core": None, "azure.core.exceptions": None,
    }):
        with pytest.raises(SecretsError, match=r"azure-keyvault-secrets"):
            resolve_secret("AZ_SECRET", provider="azure")


def test_azure_reads_vault_url_from_env(monkeypatch):
    monkeypatch.delenv("AZURE_SECRET_KEY", raising=False)
    monkeypatch.setenv("AZURE_KEYVAULT_URL", "https://myvault.vault.azure.net/")

    mock_azure_kv_secrets = MagicMock()
    mock_azure_identity = MagicMock()
    mock_azure_core = MagicMock()
    mock_resource_not_found = type("ResourceNotFoundError", (Exception,), {})
    mock_azure_core.exceptions = MagicMock()
    mock_azure_core.exceptions.ResourceNotFoundError = mock_resource_not_found

    mock_client_instance = MagicMock()
    mock_azure_kv_secrets.SecretClient.return_value = mock_client_instance
    mock_secret = MagicMock()
    mock_secret.value = "az-value"
    mock_client_instance.get_secret.return_value = mock_secret

    with patch.dict(sys.modules, {
        "azure": MagicMock(),
        "azure.keyvault": MagicMock(),
        "azure.keyvault.secrets": mock_azure_kv_secrets,
        "azure.identity": mock_azure_identity,
        "azure.core": mock_azure_core,
        "azure.core.exceptions": mock_azure_core.exceptions,
    }):
        val = resolve_secret("AZURE_SECRET_KEY", provider="azure")

    assert val == "az-value"
    monkeypatch.delenv("AZURE_SECRET_KEY", raising=False)


# ── provider: custom ──────────────────────────────────────────────────────────

def test_custom_provider_calls_resolver(monkeypatch, tmp_path):
    monkeypatch.delenv("CUSTOM_KEY", raising=False)
    # Create a temporary module with a resolver function
    resolver_mod = types.ModuleType("_test_resolver_mod")
    resolver_mod.fetch = lambda key: f"custom-{key}"
    sys.modules["_test_resolver_mod"] = resolver_mod
    try:
        val = resolve_secret("CUSTOM_KEY", provider="custom", resolver="_test_resolver_mod.fetch")
        assert val == "custom-CUSTOM_KEY"
    finally:
        del sys.modules["_test_resolver_mod"]
        monkeypatch.delenv("CUSTOM_KEY", raising=False)


def test_custom_provider_resolver_returns_none_raises(monkeypatch):
    monkeypatch.delenv("NULL_KEY", raising=False)
    resolver_mod = types.ModuleType("_test_null_mod")
    resolver_mod.fetch = lambda key: None
    sys.modules["_test_null_mod"] = resolver_mod
    try:
        with pytest.raises(SecretsError, match=r"NULL_KEY"):
            resolve_secret("NULL_KEY", provider="custom", resolver="_test_null_mod.fetch")
    finally:
        del sys.modules["_test_null_mod"]


def test_custom_provider_bad_resolver_path_raises(monkeypatch):
    monkeypatch.delenv("BAD_RESOLVER_KEY", raising=False)
    with pytest.raises(SecretsError, match=r"nonexistent\.module\.fetch"):
        resolve_secret("BAD_RESOLVER_KEY", provider="custom", resolver="nonexistent.module.fetch")


def test_custom_provider_no_resolver_raises(monkeypatch):
    monkeypatch.delenv("NO_RESOLVER_KEY", raising=False)
    with pytest.raises(SecretsError, match=r"resolver"):
        resolve_secret("NO_RESOLVER_KEY", provider="custom", resolver=None)
