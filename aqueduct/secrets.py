"""Secrets resolution — pluggable provider backends for @aq.secret().

Supported providers:
  env    — reads os.environ (default, zero deps)
  aws    — AWS Secrets Manager via boto3 (pip install aqueduct-core[aws])
  gcp    — GCP Secret Manager via google-cloud-secret-manager (pip install aqueduct-core[gcp])
  azure  — Azure Key Vault via azure-keyvault-secrets (pip install aqueduct-core[azure])
  custom — user-supplied callable at secrets.resolver (dotted import path)

Each call hits the provider directly — no in-process or os.environ cache.
This preserves provider-side rotation (a rotated secret takes effect on the
next call without restarting the process) and per-call audit logs. Caching
is intentionally deferred (see TODOs.md, Phase 32 deferred items).
"""

from __future__ import annotations

import os
from typing import Any, Callable

from aqueduct.errors import AqueductError


class SecretsError(AqueductError):
    """Raised when a secret cannot be resolved."""


def load_resolver_fn(resolver: str, base_dir: str | None = None) -> Callable[[str], Any]:
    """Load the callable named by a ``secrets.resolver`` dotted path.

    Thin delegate to :func:`aqueduct.infra.module_loading.load_callable` —
    the shared collision-proof loader (file-path load from ``base_dir`` when
    the file exists, ``importlib.import_module`` fallback otherwise; see that
    module's docstring for the full semantics and caveats). The module is
    re-loaded on every call (no caching), matching this file's "no cache,
    provider rotation takes effect immediately" design — a nice side effect
    is that editing the resolver file takes effect on the next call with no
    restart.
    """
    from aqueduct.infra.module_loading import load_callable
    return load_callable(resolver, base_dir)


def resolve_secret(
    key: str,
    provider: str = "env",
    region: str | None = None,
    resolver: str | None = None,
    base_dir: str | None = None,
) -> str:
    """Resolve a secret key to its string value.

    For provider=env, reads os.environ directly. For all other providers,
    delegates to the configured backend on every call — no caching, so
    rotation at the provider takes effect immediately. os.environ is NOT
    used as a fallback for non-env providers (would defeat the audit trail
    and let stale env values mask rotated secrets).

    Args:
        key:      Secret name / environment variable name.
        provider: Backend: env | aws | gcp | azure | custom.
        region:   Cloud region (aws/gcp/azure only).
        resolver: Dotted import path (custom only). Must name a single flat
                  file relative to base_dir (or an installed module when
                  base_dir is None) — a resolver that itself does a relative
                  or sibling-package import is not supported, see
                  load_resolver_fn.
        base_dir: Directory the custom resolver module is loaded from
                  (custom only) — lets a resolver file that lives next to
                  the config/blueprint file be found without requiring
                  install or PYTHONPATH. Loaded directly from its file path
                  (see load_resolver_fn), not via sys.path, so it can't
                  collide with an unrelated module of the same name (e.g.
                  a resolver file named "secrets.py").

    Returns:
        Secret value as string.

    Raises:
        SecretsError: Secret not found or provider misconfigured.
    """
    if provider == "env":
        val = os.environ.get(key)
        if val is not None:
            return val
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
        fetched = _fetch_custom(key, resolver, base_dir)
    else:
        raise SecretsError(
            f"Unknown secrets provider {provider!r}. "
            "Supported: env | aws | gcp | azure | custom"
        )

    if fetched is None:
        raise SecretsError(
            f"@aq.secret: {key!r} not found via provider={provider!r}."
        )

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
    try:
        client = boto3.client("secretsmanager", **kwargs)
    except botocore.exceptions.BotoCoreError as exc:
        raise SecretsError(
            f"AWS Secrets Manager client init failed for key {key!r}: {exc}"
        ) from exc
    try:
        resp = client.get_secret_value(SecretId=key)
    except botocore.exceptions.NoCredentialsError as exc:
        raise SecretsError(
            f"AWS Secrets Manager: no credentials available for key {key!r}. "
            "Provide credentials via the boto3 default chain (env vars "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, ~/.aws/credentials, "
            "IAM role on EC2/ECS/EKS/Lambda, or SSO)."
        ) from exc
    except botocore.exceptions.EndpointConnectionError as exc:
        raise SecretsError(
            f"AWS Secrets Manager endpoint unreachable for key {key!r}: {exc}. "
            "Check network connectivity and AWS_ENDPOINT_URL if you intended "
            "to use a local emulator (e.g. LocalStack)."
        ) from exc
    except botocore.exceptions.ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("ResourceNotFoundException", "SecretNotFoundException"):
            return None
        raise SecretsError(f"AWS Secrets Manager error for {key!r}: {exc}") from exc
    except botocore.exceptions.BotoCoreError as exc:
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
        from google.api_core.exceptions import NotFound
        from google.cloud import secretmanager
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
        from azure.core.exceptions import ResourceNotFoundError
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
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


def _fetch_custom(key: str, resolver: str | None, base_dir: str | None = None) -> str | None:
    if not resolver:
        raise SecretsError(
            "secrets.provider=custom requires secrets.resolver to be set "
            "(dotted import path, e.g. 'mypackage.secrets.fetch')"
        )
    try:
        fn = load_resolver_fn(resolver, base_dir)
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
