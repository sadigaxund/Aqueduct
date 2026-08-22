"""DuckDB engine registration — the ``aqueduct.engines`` entry point target.

Mirrors ``aqueduct/executor/spark/engine.py``. This module is what
``[project.entry-points."aqueduct.engines"]``'s ``duckdb = ...`` line
resolves to (see ``pyproject.toml``). ``aqueduct.executor.capabilities.load_engines()``
imports it once per process; importing it registers, as import side effects:

  - DuckDB's capability declaration (``aqueduct.executor.duckdb_.capabilities.DUCKDB``)
    into ``CAPABILITY_REGISTRY``.
  - DuckDB's ``ExecutorProtocol`` into ``aqueduct.executor.protocol.PROTOCOL_REGISTRY``.

``duckdb`` is a base dependency of aqueduct-core (the observability/depot
stores need it — see ``aqueduct/stores/duckdb_``), so unlike Spark's module
this one does not strictly need lazy imports to stay installable. The lazy
imports below are kept anyway, for the same reason Spark's are: constructing
the ``ExecutorProtocol`` object at ``load_engines()`` time must never require
opening an actual DuckDB connection, and consistency with the sibling engine
module makes the seam easier to read.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Side effect: registers "duckdb" into aqueduct.executor.capabilities.CAPABILITY_REGISTRY.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
from aqueduct.executor.duckdb_.prompt_rules import DUCKDB_PROMPT_RULES
from aqueduct.executor.duckdb_.type_render import render_duckdb_type
from aqueduct.executor.protocol import ExecutorProtocol, SessionSpec, register_protocol

if TYPE_CHECKING:
    from aqueduct.executor.models import ExecutionResult

# Execution kwargs the DuckDB engine's `execute()` genuinely accepts. The
# shared run path (`aqueduct/cli/run.py`) calls through
# `aqueduct.executor.protocol.call_execute()` with a superset that also
# carries Spark-only run options — `parallel`, `use_observe`,
# `observability_store`, `explain_capture`. Those are the "clearly optional
# capabilities an engine may ignore" half of the ExecutorProtocol contract
# (`OPTIONAL_EXECUTE_KWARGS` in `protocol.py`): DuckDB does not implement them
# (see capabilities.yml — `feature.parallel_mode`, `config.metrics.use_observe`,
# and the Spark explain-gate are all UNSUPPORTED/unimplemented this stage).
# `sampling` (Pass F) moved OUT of this "ignored" set once Probe grew a real
# DuckDB implementation that genuinely consumes `config.probes.*` — see
# `duckdb_/probe.py`. This set is handed to `ExecutorProtocol.execute_kwargs` below,
# so `call_execute()` drops anything outside it BEFORE calling `_execute` —
# emitting one suppressible `engine_kwarg_ignored` warning per dropped kwarg
# (Phase 79) instead of the prior silent drop. `_execute`'s own filter
# (immediately below) is kept as a defensive backstop for any caller that
# still reaches the protocol's `execute` directly instead of through
# `call_execute()`; it stays silent there deliberately — `call_execute()` is
# the seam responsible for user-facing signal, not this fallback.
_DUCKDB_EXECUTE_KWARGS: frozenset[str] = frozenset(
    {
        "run_id",
        "store_dir",
        "checkpoint_root",
        "surveyor",
        "depot",
        "resume_run_id",
        "from_module",
        "to_module",
        "block_full_actions",
        "warnings_suppress",
        "warnings_silence_all",
        # Phase 81 step 3 — `observability_store` backs every module's
        # `module_metrics` write (Phase 85 D1 gave this engine parity with
        # Spark's per-module write; previously it was Handoff-module-only —
        # see `duckdb_/executor.py`'s dispatch branches). `handoff_spill_uris`
        # is the {handoff_module_id: spill_uri} map the orchestrator
        # resolves, still Handoff-only and a no-op for a Manifest with no
        # Handoff module.
        "observability_store",
        "handoff_spill_uris",
        # Pass F — Probe sampling governance, genuinely consumed now.
        "sampling",
    }
)


def _execute(*args: Any, **kwargs: Any) -> ExecutionResult:
    """``ExecutorProtocol.execute`` for DuckDB — filters Spark-only run options.

    Positional args (``manifest``, the DuckDB connection) pass through; keyword
    run options are filtered to ``_DUCKDB_EXECUTE_KWARGS`` so a Spark-only
    option the shared CLI passes (``parallel``/``use_observe``/…) is dropped
    rather than raising ``TypeError`` from the real execute. Lazy import keeps
    this module importable without ``duckdb`` at module load.
    """
    from aqueduct.executor.duckdb_.executor import execute

    filtered = {k: v for k, v in kwargs.items() if k in _DUCKDB_EXECUTE_KWARGS}
    return execute(*args, **filtered)


def _extract_error(exc: BaseException | None) -> dict[str, Any] | None:
    """``ExecutorProtocol.extract_error`` for DuckDB."""
    from aqueduct.executor.duckdb_.error_extraction import extract_duckdb_error

    return extract_duckdb_error(exc)


def _make_session(spec: SessionSpec) -> Any:
    """``ExecutorProtocol.make_session`` for DuckDB.

    Reads ``spec.engine_config`` (``DuckDBEngineConfig.model_dump()`` — see
    ``aqueduct.config.DuckDBEngineConfig`` and the caller that builds this
    ``SessionSpec``) so every ``engine.duckdb.*`` field is a real knob, never
    a silent no-op:

      - ``database_path`` (unset -> ``:memory:``, the historical Stage A
        behaviour): a persistent file both raises a receiving handoff
        island's RAM ceiling and lets large intermediates spill to disk.
      - ``memory_limit``/``threads``: ``SET memory_limit=...``/
        ``SET threads=...``, applied right after connecting.
      - ``extension_repository``: ``SET custom_extension_repository=...``
        BEFORE any extension install attempt — the airgapped-mirror escape
        hatch (see ``duckdb_/extensions.py``).
      - ``s3_key_id_secret``/``s3_secret_access_key_secret``/``s3_region``/
        ``s3_endpoint``/``s3_url_style``/``s3_use_ssl``: secret KEY NAMES
        (for the credential pair) resolved through the ``secrets:`` block
        resolver (via ``spec.engine_options["secrets"]`` — the
        caller-populated provider/region/resolver/base_dir bag) plus literal
        endpoint/style/SSL overrides, all fed into DuckDB's own ``CREATE
        SECRET (TYPE S3, ...)``. The endpoint/style/SSL fields are the
        non-AWS-S3-compatible escape hatch (MinIO, on-prem object storage —
        AWS's virtual-hosted addressing and TLS assumptions don't hold for
        those). When credentials are configured, this also proactively
        ``INSTALL``/``LOAD``s ``httpfs`` (``ensure_httpfs``) so an
        airgapped-install failure surfaces LOUDLY at session creation as a
        ``DuckDBExtensionError``, not as a bare HTTP error buried inside
        some later query. When NOT configured, ``httpfs`` is left entirely
        alone — DuckDB's own ``autoinstall_known_extensions``/
        ``autoload_known_extensions`` (both default ``True``) still load it
        lazily on the first ``s3://``/``gs://`` touch, same as always; a
        user who never touches remote storage sees zero new behaviour.

    ``spec.timezone`` (Phase 81/82 — ``aqueduct.yml``'s top-level
    ``timezone:``) IS applied, via ``SET TimeZone`` — DuckDB has no
    ``engine.duckdb.*`` timezone override to defer to, so there is no
    divergence to check for (unlike Spark's
    ``engine.spark.conf.spark.sql.session.timeZone``).

    ``master_url``/``quiet``/``quiet_startup`` are accepted for cross-engine
    parity and ignored — meaningless for a single-process embedded engine.
    ``duckdb`` is imported inside the body so constructing the protocol
    object never requires it.
    """
    import duckdb

    from aqueduct.executor.duckdb_.extensions import (
        configure_s3_secret,
        ensure_extension,
        resolve_s3_secret_from_config,
    )

    engine_config = spec.engine_config or {}
    database_path = engine_config.get("database_path") or ":memory:"
    conn = duckdb.connect(database_path)

    if spec.timezone:
        conn.execute("SET TimeZone=?", [spec.timezone])

    memory_limit = engine_config.get("memory_limit")
    if memory_limit:
        conn.execute("SET memory_limit=?", [memory_limit])

    threads = engine_config.get("threads")
    if threads:
        conn.execute("SET threads=?", [threads])

    extension_repository = engine_config.get("extension_repository")

    secrets_options = (spec.engine_options or {}).get("secrets", {})
    s3_creds = resolve_s3_secret_from_config(engine_config, secrets_options)
    if s3_creds is not None:
        configure_s3_secret(conn, **s3_creds)
        # S3 credentials configured — remote storage is clearly intended, so
        # surface an airgapped-install failure LOUDLY here rather than
        # letting it hit as a bare HTTP error inside a later query.
        ensure_extension(conn, "httpfs", extension_repository=extension_repository)
    elif extension_repository:
        # No S3 creds, but an explicit mirror was configured — same signal
        # of deliberate remote-storage intent.
        ensure_extension(conn, "httpfs", extension_repository=extension_repository)

    return conn


def _close_session(session: Any) -> None:
    """``ExecutorProtocol.close_session`` for DuckDB — close the connection."""
    session.close()


def _read_source_schema(module: Any, session: Any) -> dict[str, str]:
    """``ExecutorProtocol.read_source_schema`` for DuckDB.

    Delegates to ``aqueduct.executor.duckdb_.schema_reader.read_source_schema``
    — this is the seam that lets the healing agent's ``get_source_schema``
    tool (``aqueduct/agent/toolbox.py``) read a DuckDB run's source schema
    without going through Spark. Lazy import kept for consistency with the
    sibling wrappers in this module.
    """
    from aqueduct.executor.duckdb_.schema_reader import read_source_schema

    return read_source_schema(module, session)


def _sample_source_rows(
    module: Any,
    session: Any,
    n: int = 10,
    base_dir: str | None = None,
) -> list[dict[str, Any]]:
    """``ExecutorProtocol.sample_source_rows`` for DuckDB.

    Delegates to ``aqueduct.executor.duckdb_.schema_reader.sample_source_rows``
    — bounded via a pushed-down ``LIMIT``, never a full scan.
    """
    from aqueduct.executor.duckdb_.schema_reader import sample_source_rows

    return sample_source_rows(module, session, n=n, base_dir=base_dir)


DUCKDB = ExecutorProtocol(
    engine="duckdb",
    execute=_execute,
    extract_error=_extract_error,
    prompt_rules=DUCKDB_PROMPT_RULES,
    make_session=_make_session,
    close_session=_close_session,
    read_source_schema=_read_source_schema,
    sample_source_rows=_sample_source_rows,
    render_type=render_duckdb_type,
    execute_kwargs=_DUCKDB_EXECUTE_KWARGS,
)
register_protocol(DUCKDB)


__all__ = ["DUCKDB"]
