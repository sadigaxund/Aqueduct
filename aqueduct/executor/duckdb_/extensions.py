"""DuckDB extension + S3 credential wiring — the ``httpfs`` seam.

``httpfs`` is a DuckDB EXTENSION, not a Python package: nothing about it
enters ``pyproject.toml``, and no new dependency or extra is added by any of
this. On duckdb>=1.0, ``autoinstall_known_extensions``/
``autoload_known_extensions`` both default to ``True``, so touching an
``s3://``/``gs://`` URI from SQL (``read_parquet``, ``COPY ... TO``, ...)
already makes DuckDB install and load ``httpfs`` on its own with zero code
here. What this module adds:

  1. ``configure_s3_secret`` — feeds credentials resolved through the
     EXISTING ``secrets:`` block resolver (``aqueduct.secrets.resolve_secret``
     — the same function ``@aq.secret()`` calls) into DuckDB's own
     ``CREATE SECRET (TYPE S3, ...)`` via parameter binding, never string
     interpolation. Measured (2026-07-31, duckdb 1.5.4): ``CREATE SECRET``
     succeeds with ``httpfs`` UNLOADED — the secret manager is core DuckDB —
     so this never forces an extension load by itself.
  2. ``ensure_httpfs`` — an explicit ``INSTALL``/``LOAD`` for the cases where
     Aqueduct *knows* remote storage is in play (S3 credentials configured,
     or a custom extension repository given) and wants the airgapped-install
     failure to surface LOUDLY and EARLY, at session creation, as a proper
     ``AqueductError`` — never a bare ``duckdb.IOException`` (or an HTTP
     failure) escaping from deep inside a query mid-run. Measured: pointing
     ``custom_extension_repository`` at an unreachable host produces
     ``duckdb.IOException: IO Error: Failed to download extension ... (ERROR
     Could not establish connection)`` — exactly the failure mode an
     airgapped cluster or hermetic CI hits on first touch.

Deliberately NOT unified with Spark's jar-availability machinery
(``executor/spark/warnings/jar_availability.py``) — see AGENTS.md: jars ship
to an executor FLEET at session creation, extensions ``INSTALL``/``LOAD``
per-connection, in-process. Different lifecycles, same DIAGNOSTIC SHAPE only
(a per-engine "pluggable binary dependency may be missing" warning +
``aqueduct doctor`` row) — see ``duckdb_/warnings/httpfs_availability.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aqueduct.errors import AqueductError

if TYPE_CHECKING:
    import duckdb

# DuckDB's own S3 secret scope name Aqueduct always uses. Session-scoped
# (never `CREATE PERSISTENT SECRET`) so a credential is never written into a
# persistent `database_path` file — it lives only in the live connection's
# in-memory secret manager and disappears when the session closes.
_S3_SECRET_NAME = "aqueduct_s3"


class DuckDBExtensionError(AqueductError):
    """Raised when a DuckDB extension (``httpfs``, ...) cannot be installed
    or loaded.

    Wraps whatever ``duckdb.Error`` the failed ``INSTALL``/``LOAD`` raised —
    the common cause is ``autoinstall_known_extensions`` needing network
    access on first use and not getting it (an airgapped cluster, hermetic
    CI with no egress). Never let the raw ``duckdb.IOException``/
    ``duckdb.HTTPException`` escape unwrapped: a user hitting this needs to
    know it is a network/install problem with two concrete escapes, not a
    bare download-failed message with no next step.
    """


def ensure_extension(
    conn: "duckdb.DuckDBPyConnection", name: str, *, extension_repository: str | None = None,
) -> None:
    """``INSTALL``/``LOAD`` a DuckDB extension, wrapping any failure.

    ``extension_repository``, if given, is applied via
    ``SET custom_extension_repository`` BEFORE the install attempt — the
    airgapped escape hatch (point at an internal mirror instead of the
    public repository). The other escape hatch, a pre-populated
    ``~/.duckdb/extensions``, needs no code here: DuckDB checks its local
    cache before touching the network at all.
    """
    import duckdb as _duckdb

    if extension_repository:
        conn.execute("SET custom_extension_repository=?", [extension_repository])
    try:
        conn.execute(f"INSTALL {name}")
        conn.execute(f"LOAD {name}")
    except _duckdb.Error as exc:
        raise DuckDBExtensionError(
            f"Could not install/load the DuckDB {name!r} extension: {exc}. "
            "This most commonly means DuckDB's extension installer needed "
            "network access (INSTALL fetches from DuckDB's extension "
            "repository on first use) and could not reach it — the failure "
            "mode an airgapped cluster or hermetic CI hits on the first "
            "s3://-style path a Blueprint touches. Two escapes: (1) "
            f"pre-populate ~/.duckdb/extensions with the {name!r} extension "
            "(fetched once on a machine with network access, then shipped "
            "alongside the deployment — no config needed, DuckDB checks its "
            "local cache first); or (2) set "
            "engine.duckdb.extension_repository to an internal mirror URL."
        ) from exc


def configure_s3_secret(
    conn: "duckdb.DuckDBPyConnection",
    *,
    key_id: str,
    secret: str,
    region: str | None = None,
) -> None:
    """``CREATE OR REPLACE SECRET`` for S3/GCS access, via parameter binding.

    ``key_id``/``secret`` are already-RESOLVED credential values (the caller
    resolves the configured secret KEY NAMES through
    ``aqueduct.secrets.resolve_secret`` first — this function never touches
    the ``secrets:`` resolver itself, it only feeds DuckDB). Values are bound
    as query parameters, never string-interpolated into the SQL text, so a
    credential can never leak into a rendered/logged statement. Measured:
    ``CREATE SECRET`` succeeds with ``httpfs`` unloaded (the secret manager
    is core DuckDB) — this function never installs or loads an extension.

    Session-scoped (``CREATE OR REPLACE SECRET``, not ``CREATE PERSISTENT
    SECRET``): the credential lives only in this connection's in-memory
    secret manager and is never written into a persistent ``database_path``
    file.
    """
    if region:
        conn.execute(
            f"CREATE OR REPLACE SECRET {_S3_SECRET_NAME} "
            "(TYPE S3, KEY_ID ?, SECRET ?, REGION ?)",
            [key_id, secret, region],
        )
    else:
        conn.execute(
            f"CREATE OR REPLACE SECRET {_S3_SECRET_NAME} (TYPE S3, KEY_ID ?, SECRET ?)",
            [key_id, secret],
        )


def resolve_s3_secret_from_config(
    engine_config: dict[str, Any], secrets_options: dict[str, Any],
) -> tuple[str, str, str | None] | None:
    """Resolve ``engine.duckdb.s3_*`` secret KEY NAMES to VALUES, or ``None``.

    ``engine_config`` is ``SessionSpec.engine_config`` (this engine's own
    config bag — ``DuckDBEngineConfig.model_dump()`` in practice).
    ``secrets_options`` is ``SessionSpec.engine_options["secrets"]`` — the
    resolved ``secrets:`` block (``provider``/``region``/``resolver``/
    ``base_dir``), populated by the caller building the ``SessionSpec`` (see
    ``aqueduct/cli/run.py``/``aqueduct/executor/orchestrator.py``). Returns
    ``None`` when no S3 credentials are configured (the common case) —
    ``_make_session`` skips ``configure_s3_secret``/``ensure_httpfs``
    entirely rather than touching either.

    Raises whatever ``aqueduct.secrets.resolve_secret`` raises
    (``SecretsError``, an ``AqueductError`` subclass) if the configured
    secret key name cannot actually be resolved — a misconfigured
    ``s3_key_id_secret`` must fail loudly here, at session creation, not
    silently produce an unauthenticated S3 request later.
    """
    key_id_secret = engine_config.get("s3_key_id_secret")
    secret_access_key_secret = engine_config.get("s3_secret_access_key_secret")
    if not key_id_secret or not secret_access_key_secret:
        return None

    from aqueduct.secrets import resolve_secret

    key_id = resolve_secret(
        key_id_secret,
        provider=secrets_options.get("provider", "env"),
        region=secrets_options.get("region"),
        resolver=secrets_options.get("resolver"),
        base_dir=secrets_options.get("base_dir"),
    )
    secret_value = resolve_secret(
        secret_access_key_secret,
        provider=secrets_options.get("provider", "env"),
        region=secrets_options.get("region"),
        resolver=secrets_options.get("resolver"),
        base_dir=secrets_options.get("base_dir"),
    )
    return key_id, secret_value, engine_config.get("s3_region")


__all__ = [
    "DuckDBExtensionError",
    "ensure_extension",
    "configure_s3_secret",
    "resolve_s3_secret_from_config",
]
