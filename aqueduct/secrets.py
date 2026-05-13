"""Secrets resolution — pluggable provider backends for @aq.secret().

Supported providers:
  env    — reads os.environ (default, zero deps)
  aws    — AWS Secrets Manager via boto3 (pip install aqueduct-core[aws])
  gcp    — GCP Secret Manager via google-cloud-secret-manager (pip install aqueduct-core[gcp])
  azure  — Azure Key Vault via azure-keyvault-secrets (pip install aqueduct-core[azure])
  custom — user-supplied callable at secrets.resolver (dotted import path)

All providers cache resolved values into os.environ after first fetch so
subsequent @aq.secret() calls for the same key are free (no network).
"""

from __future__ import annotations

import importlib
import os
from typing import Any


class SecretsError(Exception):
    """Raised when a secret cannot be resolved."""


def resolve_secret(
    key: str,
    provider: str = "env",
    region: str | None = None,
    resolver: str | None = None,
) -> str:
    """Resolve a secret key to its string value.

    Always checks os.environ first — local overrides always win.
    On miss, delegates to the configured provider backend.
    Caches fetched value into os.environ so repeat calls are free.

    Args:
        key:      Secret name / environment variable name.
        provider: Backend: env | aws | gcp | azure | custom.
        region:   Cloud region (aws/gcp/azure only).
        resolver: Dotted import path (custom only).

    Returns:
        Secret value as string.

    Raises:
        SecretsError: Secret not found or provider misconfigured.
    """
    val = os.environ.get(key)
    if val is not None:
        return val

    if provider == "env":
        raise SecretsError(
            f"@aq.secret: {key!r} not found in environment. "
            "Set the variable before running aqueduct, or configure a secrets provider."
        )

    if provider == "aws":
        fetched = _fetch_aws(key, region)
    elif provider == "gcp":
        fetched = _fetch_gcp(key, region)
    elif provider == "azure":
        fetched = _fetch_azure(key, region)
    elif provider == "custom":
        fetched = _fetch_custom(key, resolver)
    else:
        raise SecretsError(
            f"Unknown secrets provider {provider!r}. "
            "Supported: env | aws | gcp | azure | custom"
        )

    if fetched is None:
        raise SecretsError(
            f"@aq.secret: {key!r} not found via provider={provider!r}."
        )

    os.environ[key] = fetched
    return fetched


def _fetch_aws(key: str, region: str | None) -> str | None:
    try:
        import boto3
        import botocore.exceptions
    except ImportError:
        raise SecretsError(
            "secrets.provider=aws requires boto3. "
            "Install: pip install aqueduct-core[aws]"
        )
    kwargs: dict[str, Any] = {}
    if region:
        kwargs["region_name"] = region
    client = boto3.client("secretsmanager", **kwargs)
    try:
        resp = client.get_secret_value(SecretId=key)
    except botocore.exceptions.ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("ResourceNotFoundException", "SecretNotFoundException"):
            return None
        raise SecretsError(f"AWS Secrets Manager error for {key!r}: {exc}") from exc
    # SecretString is a plain string or JSON blob
    secret = resp.get("SecretString")
    if secret is None:
        raise SecretsError(f"AWS secret {key!r} is binary — only string secrets supported.")
    # If JSON blob, try to extract key from it
    try:
        import json
        blob = json.loads(secret)
        if isinstance(blob, dict) and key in blob:
            return str(blob[key])
        if isinstance(blob, dict) and len(blob) == 1:
            return str(next(iter(blob.values())))
    except (json.JSONDecodeError, TypeError):
        pass
    return secret


def _fetch_gcp(key: str, region: str | None) -> str | None:
    try:
        from google.cloud import secretmanager
        from google.api_core.exceptions import NotFound
    except ImportError:
        raise SecretsError(
            "secrets.provider=gcp requires google-cloud-secret-manager. "
            "Install: pip install aqueduct-core[gcp]"
        )
    client = secretmanager.SecretManagerServiceClient()
    # key format: "projects/PROJECT/secrets/NAME/versions/latest"
    # or short form: "NAME" (requires GCP_PROJECT env var)
    if "/" not in key:
        project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not project:
            raise SecretsError(
                f"GCP secret short name {key!r} requires GCP_PROJECT or "
                "GOOGLE_CLOUD_PROJECT env var, or use full resource name."
            )
        name = f"projects/{project}/secrets/{key}/versions/latest"
    else:
        name = key
    try:
        resp = client.access_secret_version(request={"name": name})
        return resp.payload.data.decode("utf-8")
    except NotFound:
        return None
    except Exception as exc:
        raise SecretsError(f"GCP Secret Manager error for {key!r}: {exc}") from exc


def _fetch_azure(key: str, region: str | None) -> str | None:
    try:
        from azure.keyvault.secrets import SecretClient
        from azure.identity import DefaultAzureCredential
        from azure.core.exceptions import ResourceNotFoundError
    except ImportError:
        raise SecretsError(
            "secrets.provider=azure requires azure-keyvault-secrets and azure-identity. "
            "Install: pip install aqueduct-core[azure]"
        )
    vault_url = os.environ.get("AZURE_KEYVAULT_URL")
    if not vault_url:
        raise SecretsError(
            "secrets.provider=azure requires AZURE_KEYVAULT_URL env var "
            "(e.g. https://myvault.vault.azure.net/)"
        )
    client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
    try:
        secret = client.get_secret(key)
        return secret.value
    except ResourceNotFoundError:
        return None
    except Exception as exc:
        raise SecretsError(f"Azure Key Vault error for {key!r}: {exc}") from exc


def _fetch_custom(key: str, resolver: str | None) -> str | None:
    if not resolver:
        raise SecretsError(
            "secrets.provider=custom requires secrets.resolver to be set "
            "(dotted import path, e.g. 'mypackage.secrets.fetch')"
        )
    try:
        module_path, fn_name = resolver.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        fn = getattr(mod, fn_name)
    except (ImportError, AttributeError, ValueError) as exc:
        raise SecretsError(
            f"secrets.resolver {resolver!r} could not be loaded: {exc}"
        ) from exc
    try:
        result = fn(key)
    except Exception as exc:
        raise SecretsError(
            f"secrets.resolver {resolver!r} raised for key {key!r}: {exc}"
        ) from exc
    return None if result is None else str(result)
