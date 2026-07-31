"""Warn when a Blueprint touches remote storage but ``httpfs`` isn't loaded.

Mirrors ``executor/spark/warnings/jar_availability.py``'s SHAPE (scan the
Manifest for a pluggable-binary-dependency need, probe the live session,
warn if the dependency looks unavailable) applied to DuckDB's own
lifecycle: a jar ships to Spark's executor fleet at session creation; a
DuckDB extension ``INSTALL``/``LOAD``s per-connection, in-process. Different
mechanisms, same diagnostic shape — see AGENTS.md's "jars <-> extensions
symmetry" note and ``duckdb_/extensions.py``'s module docstring.

Scans Ingress/Egress ``path`` values for a remote URI scheme (s3://, gs://,
...). If any are found and ``httpfs`` is not yet loaded in this session,
warns that DuckDB's ``autoinstall_known_extensions``/
``autoload_known_extensions`` (both default ``True``) will attempt to fetch
it over the network on first touch — the exact failure mode an airgapped
cluster or hermetic CI hits — and names the two escape hatches
(``engine.duckdb.extension_repository``, a pre-populated
``~/.duckdb/extensions``). This is advisory only: ``_make_session``
(``duckdb_/engine.py``) already proactively loads ``httpfs`` (surfacing a
``DuckDBExtensionError`` immediately) whenever S3 credentials or a custom
extension repository are configured; this rule catches the REMAINING case —
a Blueprint reading/writing a remote path with no explicit
``engine.duckdb.s3_*``/``extension_repository`` config at all (e.g. relying
on DuckDB's own ambient AWS credential chain, or an anonymous/public
bucket).
"""

from __future__ import annotations

import logging
from typing import Any

from aqueduct.models import ModuleType

logger = logging.getLogger(__name__)

RULE_ID = "duckdb_httpfs_availability"

_REMOTE_SCHEMES: tuple[str, ...] = ("s3://", "s3a://", "s3n://", "gs://", "gcs://", "azure://", "abfss://", "r2://")


def _remote_paths_in_blueprint(manifest: Any) -> dict[str, list[str]]:
    """Return ``{module_id: [remote paths]}`` for declared Ingress/Egress."""
    out: dict[str, list[str]] = {}
    for m in manifest.modules:
        if m.type not in (ModuleType.Ingress, ModuleType.Egress):
            continue
        path = (m.config or {}).get("path") or ""
        if isinstance(path, str) and path.lower().startswith(_REMOTE_SCHEMES):
            out.setdefault(m.id, []).append(path)
    return out


def _httpfs_loaded(con: Any) -> bool | None:
    """Return whether ``httpfs`` is loaded in this session, or ``None`` if
    it could not be determined (session introspection failed)."""
    try:
        row = con.sql(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'httpfs'"
        ).fetchone()
        return bool(row[0]) if row is not None else False
    except Exception as exc:  # noqa: BLE001 — best-effort diagnostic, never fatal
        logger.debug("httpfs availability introspection unavailable: %s", exc)
        return None


def check(manifest: Any, con: Any) -> list[str]:
    remote = _remote_paths_in_blueprint(manifest)
    if not remote:
        return []

    loaded = _httpfs_loaded(con)
    if loaded is None:
        ids = ", ".join(repr(i) for i in remote)
        return [
            f"Modules {ids} read/write a remote (s3://, gs://, ...) path, but "
            "this session's httpfs extension status could not be determined."
        ]
    if loaded:
        return []

    ids = ", ".join(repr(i) for i in remote)
    return [
        f"Modules {ids} read/write a remote (s3://, gs://, ...) path. DuckDB's "
        "httpfs extension is not yet loaded in this session and will be "
        "installed/loaded automatically on first use, which needs network "
        "access. If this environment has no route to DuckDB's extension "
        "repository (an airgapped cluster, hermetic CI), the first read/write "
        "will fail — pre-populate ~/.duckdb/extensions on a machine with "
        "network access, or set engine.duckdb.extension_repository to an "
        "internal mirror, before this run."
    ]
