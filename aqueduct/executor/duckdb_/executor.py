"""Executor orchestrator — runs a Manifest against a live DuckDB connection.

Stage A supported module types: Ingress, Channel, Egress, Junction, Funnel,
Regulator, Assert (Pass D), Probe (Pass F). ``_SUPPORTED_TYPES`` below is
defense in depth, mirroring Spark's — every dispatched type also carries a
real ``capabilities.yml`` verdict.

Execution model — same shape as Spark's ``execute()``
(``aqueduct/executor/spark/executor.py``), the contract
``aqueduct/executor/protocol.py`` was derived from, minus what Stage A
deliberately does not implement (see the module-level notes below):

  1. Topological sort of non-Probe/non-Assert modules by data-flow edges.
  2. Walk sorted order, dispatching each module type to its handler, writing
     results into ``frame_store`` keyed the same way Spark's executor keys
     it (``main`` port -> ``from_id``; branch/spillway ports ->
     ``f"{from_id}.{port}"``).
  3. Return a frozen ``ExecutionResult`` — identical shape to Spark's.

Incremental Channel watermark (``materialize: incremental`` /
``watermark_column`` — declared Module fields, see
``aqueduct.parser.schema.ModuleSchema``) mirrors Spark's
``read -> substitute -> run -> persist-new-max`` cycle exactly, including the
quoted substitution and the ``'1900-01-01 00:00:00'`` first-run sentinel: the
Depot's persisted ``MAX(watermark_column)`` is substituted for the literal
``${ctx._watermark}`` token in the Channel branch below before
``execute_channel`` runs, and after a successful downstream Egress write the
new ``MAX(watermark_column)`` is computed with a DuckDB SQL aggregate over
the WRITTEN output file (not the upstream DAG) and persisted back to the
Depot. The Depot is engine-independent (``aqueduct/depot/``), so this is the
same cycle Spark's executor runs, in DuckDB's own idiom (SQL instead of a
DataFrame action).

What this stage does NOT implement (honest gaps, not silent ones):
  - Parallel component execution (``feature.parallel_mode`` — UNSUPPORTED).
    Execution is always serial. DuckDB itself parallelizes a single query
    across threads internally; the Aqueduct-level "independent DAG subtrees
    on separate Python threads" optimization Spark's executor has is simply
    not built yet.
  - Sandbox/lineage/explain-snapshot capture, per-module runtime metrics
    persistence to the observability store, and hook firing are all left to
    the caller (the ``aqueduct run`` CLI path composes those around
    ``ExecutorProtocol.execute`` for any engine already — see
    ``aqueduct/cli/blueprint.py``) exactly as they are for Spark today for
    everything past `ExecutionResult`.
"""

from __future__ import annotations

import dataclasses
import logging
import random
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    import duckdb

from aqueduct.errors import ExecuteError
from aqueduct.executor.duckdb_.assert_ import (
    _AQ_ERROR_MODULE,
    _AQ_ERROR_MSG,
    _AQ_ERROR_TS,
    _AQ_ERROR_TYPE,
    AssertError,
    _sql_literal,
    execute_assert,
)
from aqueduct.executor.duckdb_.channel import ChannelError, execute_channel
from aqueduct.executor.duckdb_.egress import EgressError, _escape, write_egress
from aqueduct.executor.duckdb_.funnel import FunnelError, execute_funnel
from aqueduct.executor.duckdb_.ingress import IngressError, read_ingress
from aqueduct.executor.duckdb_.junction import JunctionError, execute_junction
from aqueduct.executor.duckdb_.probe import ProbeSampling, execute_probe
from aqueduct.executor.duckdb_.udf import UDFError, register_udfs
from aqueduct.executor.models import (
    ExecutionResult,
    ExecutionStatus,
    ModuleResult,
    _collect_module_warnings,
    concise_error,
    manifest_hash as _manifest_hash,
    write_module_metrics as _write_handoff_metrics,
)
from aqueduct.executor.spill import dir_size_bytes, is_remote_uri
from aqueduct.models import Edge, Manifest, Module, ModuleType, RetryPolicy

logger = logging.getLogger(__name__)


def _mr(**kwargs) -> ModuleResult:
    return ModuleResult(**kwargs, warnings=_collect_module_warnings())


_T = TypeVar("_T")


# ExecuteError is engine-agnostic — defined in ``aqueduct.errors`` and
# imported above (re-exported here, not redefined) so this executor raises
# the SAME type Spark's does (and the CLI/orchestrator catch); see
# ``aqueduct.errors.ExecuteError`` for why a private per-engine subclass was
# a bug — the CLI's ``except ExecuteError`` never caught it.


# Module types this executor actually dispatches — defense in depth, same as
# Spark's `_SUPPORTED_TYPES`; every entry also carries a `supported` verdict
# in capabilities.yml.
_SUPPORTED_TYPES: frozenset[str] = frozenset(
    {ModuleType.Ingress, ModuleType.Channel, ModuleType.Egress,
     ModuleType.Junction, ModuleType.Funnel, ModuleType.Regulator,
     ModuleType.Handoff, ModuleType.Assert, ModuleType.Probe}
)

_SIGNAL_PORTS: frozenset[str] = frozenset({"signal"})
_GATE_CLOSED: object = object()


# ── Retry helper — duplicated (not imported) from spark/executor.py ────────
# Pure Python, no engine dependency either way, but spark/executor.py imports
# pyspark at module scope; importing from it would leak a pyspark dependency
# into this duckdb-only package. Small enough to keep two copies in sync by
# hand rather than introduce a new shared module for Stage A.

def _is_retriable(exc: Exception, policy: RetryPolicy) -> bool:
    msg = str(exc)
    for pattern in policy.non_transient_errors:
        if pattern in msg:
            return False
    if policy.transient_errors:
        return any(p in msg for p in policy.transient_errors)
    return True


def _backoff_seconds(attempt: int, policy: RetryPolicy) -> float:
    strategy = policy.backoff_strategy
    base = float(policy.backoff_base_seconds)
    cap = float(policy.backoff_max_seconds)
    if strategy == "fixed":
        delay = base
    elif strategy == "linear":
        delay = base * (attempt + 1)
    else:
        delay = base * (2**attempt)
    delay = min(delay, cap)
    if policy.jitter:
        delay *= 0.5 + random.random() * 0.5
    return delay


def _with_retry(fn: Callable[[], _T], policy: RetryPolicy, module_id: str) -> _T:
    first_failure_at: float | None = None
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(max(1, policy.max_attempts)):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            now = time.monotonic()
            if first_failure_at is None:
                first_failure_at = now
            if policy.deadline_seconds is not None:
                elapsed = now - first_failure_at
                if elapsed >= policy.deadline_seconds:
                    logger.warning(
                        "[runtime_retry_deadline] Module %r: deadline exceeded "
                        "(%.0fs elapsed, limit=%ds); not retrying.",
                        module_id, elapsed, policy.deadline_seconds,
                    )
                    break
            is_last = attempt == policy.max_attempts - 1
            if is_last or not _is_retriable(exc, policy):
                if policy.max_attempts > 1:
                    logger.warning(
                        "[runtime_retry_exhausted] Module %r: attempt %d/%d failed (%s); giving up",
                        module_id, attempt + 1, policy.max_attempts, concise_error(str(exc)),
                    )
                break
            sleep = _backoff_seconds(attempt, policy)
            logger.warning(
                "[runtime_retry_waiting] Module %r: attempt %d/%d failed (%s); retrying in %.1fs",
                module_id, attempt + 1, policy.max_attempts, concise_error(str(exc)), sleep,
            )
            time.sleep(sleep)
    raise last_exc


def _module_retry_policy(module: Module, manifest_policy: RetryPolicy) -> RetryPolicy:
    if module.on_failure:
        try:
            return RetryPolicy(**module.on_failure)
        except TypeError as exc:
            raise ExecuteError(f"Module {module.id!r} on_failure has invalid keys: {exc}") from exc
    if module.retry is not None:
        return module.retry
    return manifest_policy


# ── DAG helpers — identical logic to Spark's, no engine dependency ─────────

def _is_data_edge(edge: Edge) -> bool:
    return edge.port not in _SIGNAL_PORTS


def _frame_key(from_id: str, port: str) -> str:
    return from_id if port == "main" else f"{from_id}.{port}"


def _effective_frame_key(edge: Edge, modules_by_id: dict[str, Module]) -> str:
    """Same as `executor/spark/executor.py::_effective_frame_key` — see its
    docstring. A Channel's SQL text or a Funnel's `inputs:` names its
    upstream by the ORIGINAL Blueprint id, authored before compilation ever
    knows a boundary will land there; when `edge.from_id` is a synthetic
    Handoff module, resolve through to its `config["from_module"]`/
    `config["port"]` instead of the handoff's own generated id."""
    src = modules_by_id.get(edge.from_id)
    if src is not None and src.type == ModuleType.Handoff:
        from_module = src.config.get("from_module", edge.from_id)
        from_port = src.config.get("port", "main")
        return _frame_key(from_module, from_port)
    return _frame_key(edge.from_id, edge.port)


def _is_gate_closed(value: Any) -> bool:
    return value is _GATE_CLOSED


def _incoming_data(module_id: str, edges: tuple[Edge, ...]) -> list[Edge]:
    return [e for e in edges if e.to_id == module_id and _is_data_edge(e)]


def _incoming_main(module_id: str, edges: tuple[Edge, ...]) -> list[Edge]:
    return [e for e in edges if e.to_id == module_id and e.port == "main"]


def _topo_sort(modules: tuple[Module, ...], edges: tuple[Edge, ...]) -> list[Module]:
    module_map: dict[str, Module] = {m.id: m for m in modules}
    in_degree: dict[str, int] = {m.id: 0 for m in modules}
    successors: dict[str, list[str]] = {m.id: [] for m in modules}
    for edge in edges:
        if _is_data_edge(edge) and edge.to_id in module_map and edge.from_id in module_map:
            in_degree[edge.to_id] += 1
            successors[edge.from_id].append(edge.to_id)
    queue: deque[str] = deque(mid for mid, deg in in_degree.items() if deg == 0)
    order: list[Module] = []
    while queue:
        mid = queue.popleft()
        order.append(module_map[mid])
        for succ in successors[mid]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)
    if len(order) != len(modules):
        processed = {m.id for m in order}
        remaining = [mid for mid in module_map if mid not in processed]
        raise ExecuteError(f"Cycle detected in Manifest execution graph. Modules in cycle: {remaining}")
    return order


def _build_execution_order(manifest: Manifest) -> list[Module]:
    """Topo-sort non-Probe modules, then insert each Probe immediately after
    its ``attach_to`` target — same shape as Spark's
    ``executor/spark/executor.py::_build_execution_order`` (Pass F). A Probe
    has no data edges (it binds via ``attach_to`, not the graph), so it is
    excluded from the topo-sort input and reinserted positionally afterward.
    This guarantees a Probe's signals are written to the store before a
    Regulator downstream of it evaluates them. Assert (Pass D) IS part of the
    topo sort — it participates like any other module.
    """
    probe_modules = [m for m in manifest.modules if m.type == ModuleType.Probe]
    non_probe_modules = tuple(m for m in manifest.modules if m.type != ModuleType.Probe)

    order = _topo_sort(non_probe_modules, manifest.edges)

    probes_by_attach: dict[str | None, list[Module]] = defaultdict(list)
    for probe in probe_modules:
        probes_by_attach[probe.attach_to].append(probe)

    final_order: list[Module] = []
    for module in order:
        final_order.append(module)
        for probe in probes_by_attach.pop(module.id, []):
            final_order.append(probe)

    # Probes with unresolved attach_to (shouldn't happen after compiler
    # validation — wirer.py's validate_probes rejects this at compile time).
    for remaining_probes in probes_by_attach.values():
        final_order.extend(remaining_probes)

    return final_order


def _write_checkpoint(
    con: duckdb.DuckDBPyConnection,
    module: Module,
    checkpoint_dir: Path | None,
    manifest: Manifest,
    data: dict[str, Any] | None = None,
) -> None:
    if checkpoint_dir is None or not (manifest.checkpoint or module.checkpoint):
        return
    module_ckpt = checkpoint_dir / module.id
    module_ckpt.mkdir(parents=True, exist_ok=True)
    if data:
        for name, rel in data.items():
            try:
                target = module_ckpt / name
                target.mkdir(parents=True, exist_ok=True)
                con.register("__ckpt__", rel)
                try:
                    con.sql(f"COPY __ckpt__ TO '{(target / 'part-0.parquet')!s}' (FORMAT PARQUET)")
                finally:
                    con.unregister("__ckpt__")
            except Exception as exc:
                logger.warning(
                    "[runtime_checkpoint_write_failed] Checkpoint write failed for %r/%s: %s",
                    module.id, name, exc,
                )
                return
    (module_ckpt / "_aq_done").write_text("", encoding="utf-8")
    logger.debug("Checkpoint written: %s", module_ckpt)


def _fail(blueprint_id: str, run_id: str, module_results: list[ModuleResult], *, trigger_agent: bool = False) -> ExecutionResult:
    return ExecutionResult(
        blueprint_id=blueprint_id, run_id=run_id, status=ExecutionStatus.ERROR,
        module_results=tuple(module_results), trigger_agent=trigger_agent,
    )


def _on_retry_exhausted(
    exc: Exception, policy: RetryPolicy, module: Module, blueprint_id: str, run_id: str,
    module_results: list[ModuleResult],
) -> tuple[bool, ExecutionResult | None]:
    """Handle retry exhaustion per on_exhaustion policy. Same contract as Spark's."""
    module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=str(exc), exception=exc))

    on_exhaustion = policy.on_exhaustion
    fire_webhook_for_module = on_exhaustion != "alert_only"
    if fire_webhook_for_module and module.on_failure_webhook is not None:
        import os as _os
        import re as _re

        from aqueduct.infra.http import _deliver_webhook_payload
        raw = module.on_failure_webhook
        full_payload = {
            "run_id": run_id, "blueprint_id": blueprint_id, "module_id": module.id,
            "error_message": str(exc), "error_type": type(exc).__name__,
        }
        if isinstance(raw, str):
            url, method, headers, timeout, attempts, backoff, secret = raw, "POST", {}, 10, 2, 2.0, None
        else:
            url = raw["url"]
            method = raw.get("method", "POST")
            headers = dict(raw.get("headers") or {})
            timeout = raw.get("timeout", 10)
            attempts = raw.get("max_retries", 1) + 1
            backoff = raw.get("backoff_seconds", 2.0)
            secret = raw.get("secret")
            token_re = _re.compile(r"\$\{([^}]+)\}")

            def _render_val(val):
                if not isinstance(val, str):
                    return val
                return token_re.sub(
                    lambda m: str(full_payload.get(m.group(1), _os.environ.get(m.group(1), m.group(0)))), val,
                )
            headers = {k: _render_val(v) for k, v in headers.items()}
            if secret:
                secret = _render_val(secret)
        _deliver_webhook_payload(
            url, full_payload, method=method, headers=headers, timeout=timeout,
            attempts=attempts, backoff_seconds=backoff, secret=secret,
        )

    if on_exhaustion == "alert_only":
        logger.warning("[runtime_retry_exhausted_alert] [%s] Retry exhausted (alert_only): %s — blueprint continues.", module.id, exc)
        return True, None
    if on_exhaustion == "trigger_agent":
        return False, _fail(blueprint_id, run_id, module_results, trigger_agent=True)
    return False, _fail(blueprint_id, run_id, module_results)


# ── Watermark persistence (Depot-only) — mirrors
# executor/spark/executor.py's "Watermark persistence" section exactly, in
# DuckDB's own idiom (SQL aggregate instead of a DataFrame action). ─────────


def _find_downstream_egress_ids(channel_id: str, manifest: Manifest) -> list[str]:
    """Return IDs of all Egress modules reachable (any depth) downstream of channel_id."""
    reachable = _reachable_forward(channel_id, manifest.edges)
    return [
        m.id for m in manifest.modules
        if m.id in reachable and m.id != channel_id and m.type == ModuleType.Egress
    ]


def _compute_watermark_from_output(
    con: duckdb.DuckDBPyConnection,
    path: str,
    fmt: str,
    watermark_col: str,
) -> str | None:
    """Compute MAX(watermark_col) from already-written Egress output.

    Mirrors Spark's ``_compute_watermark_from_output`` — reads the WRITTEN
    Egress output (never the upstream DAG). DuckDB's Stage A Egress
    (``egress.py``) supports ``format: parquet`` / ``format: csv`` only and
    ``COPY ... TO`` always writes a single file at ``path`` (no Delta
    transaction-log shortcut to take — DuckDB has no Delta writer, see
    AGENTS.md's standing decision), so this is a plain file read + SQL
    aggregate. Returns None on any failure (missing file, bad column, ...) —
    the caller logs and skips advancing the watermark this run, same
    fail-open contract as Spark's.
    """
    try:
        reader = "read_parquet" if fmt == "parquet" else "read_csv"
        result = con.sql(
            f'SELECT MAX("{watermark_col}") FROM {reader}(\'{_escape(path)}\')'
        ).fetchone()
        return str(result[0]) if result is not None and result[0] is not None else None
    except Exception:
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def execute(
    manifest: Manifest,
    con: duckdb.DuckDBPyConnection,
    run_id: str | None = None,
    store_dir: Path | None = None,
    checkpoint_root: Path | None = None,
    surveyor: Any | None = None,
    depot: Any | None = None,
    resume_run_id: str | None = None,
    from_module: str | None = None,
    to_module: str | None = None,
    block_full_actions: bool = False,
    warnings_suppress: set[str] | None = None,
    warnings_silence_all: bool = False,
    observability_store: Any = None,
    handoff_spill_uris: dict[str, str] | None = None,
    sampling: ProbeSampling = ProbeSampling(),
) -> ExecutionResult:
    """Execute a compiled Manifest against a live DuckDB connection.

    Args mirror Spark's ``execute()`` where the concept exists on this engine;
    Spark-only kwargs (``spark``/``parallel``/``use_observe``/
    ``explain_capture``) have no DuckDB counterpart this stage and are simply
    not part of this signature — see ``ExecutorProtocol.execute``'s docstring:
    "the uniform part of the contract is Manifest in, ExecutionResult out;
    kwargs are a per-engine extension point."

    ``block_full_actions`` (config.danger.allow_full_probe_actions, inverted)
    and ``sampling`` (config.probes.default_sample_fraction/max_sample_rows)
    are now genuine Probe knobs (Pass F — see ``duckdb_/probe.py``), not the
    inert parity placeholders they were before Probe existed on this engine.

    ``observability_store``/``handoff_spill_uris`` (Phase 81 step 3) back the
    Handoff module ONLY — see the Handoff dispatch branch below. A Manifest
    with no Handoff module never touches either.

    Raises:
        ExecuteError: Unsupported module type, cycle detected.
    """
    from aqueduct.warnings import emit as _aq_emit

    run_id = run_id or str(uuid.uuid4())

    checkpoints_base: Path | None = checkpoint_root if checkpoint_root else (
        store_dir / "checkpoints" if store_dir else None
    )
    checkpoint_dir: Path | None = None
    any_checkpoint = manifest.checkpoint or any(m.checkpoint for m in manifest.modules)
    if checkpoints_base and any_checkpoint:
        checkpoint_dir = checkpoints_base / run_id
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        (checkpoint_dir / "_manifest_hash").write_text(_manifest_hash(manifest), encoding="utf-8")

    resume_dir: Path | None = None
    if checkpoints_base and resume_run_id:
        resume_dir = checkpoints_base / resume_run_id
        if not resume_dir.exists():
            raise ExecuteError(f"Resume run_id={resume_run_id!r} has no checkpoints at {resume_dir}")
        # Mirrors executor/spark/executor.py's resume-hash comparison: a
        # checkpoint dir's stored `_manifest_hash` (written above, on the
        # ORIGINAL run) is compared against this run's Manifest hash. This
        # engine writes that file on every checkpointed run but, until now,
        # never read it back on resume — checkpoints from a different
        # Manifest were silently reused with no signal at all. Same
        # permissive policy as Spark: warn, then proceed (module checkpoint
        # reuse is deliberately permissive; only the island handoff spill
        # path is strict).
        stored_hash_path = resume_dir / "_manifest_hash"
        if stored_hash_path.exists():
            stored_hash = stored_hash_path.read_text(encoding="utf-8").strip()
            current_hash = _manifest_hash(manifest)
            if stored_hash != current_hash:
                _aq_emit(
                    "runtime_resume_hash_changed",
                    f"Resuming run {resume_run_id!r}: Manifest has changed since "
                    f"original run (hash {stored_hash} → {current_hash}). "
                    "Checkpoint data may be stale.",
                    suppress=warnings_suppress,
                )

    # ── Session-startup warnings (httpfs availability, ...) ──────────────
    # Mirrors executor/spark/executor.py's tier-2 session-startup pass.
    # `warnings_suppress`/`warnings_silence_all` were accepted by this
    # signature but never read anywhere in this function — a silent no-op —
    # until this call gave them a real consumer.
    if not warnings_silence_all:
        try:
            from aqueduct.executor.duckdb_.warnings import run_all as _run_session_warnings
            for _rid, _msg in _run_session_warnings(manifest, con, suppress=warnings_suppress):
                _aq_emit(_rid, _msg, suppress=warnings_suppress)
        except Exception:
            pass  # session-startup warnings must never crash the executor

    # Register UDFs before any module executes so Channel SQL can reference
    # them — same placement as Spark's executor.py.
    try:
        register_udfs(manifest.udf_registry, con, base_dir=manifest.base_dir)
    except UDFError as exc:
        raise ExecuteError(str(exc)) from exc

    order = _build_execution_order(manifest)

    included_ids: set[str] | None = None
    if from_module or to_module:
        included_ids = _selector_included(manifest.modules, manifest.edges, from_module, to_module)

    frame_store: dict[str, Any] = {}
    module_results: list[ModuleResult] = []
    # Maps egress_id -> (channel_id, watermark_col, depot_key) for the
    # sidecar write after Egress success — see "Watermark persistence" above.
    _pending_watermarks: dict[str, tuple[str, str, str]] = {}
    # For `_effective_frame_key` — resolving a BY-NAME upstream reference
    # (Channel SQL, Funnel `inputs:`) transparently across a Handoff.
    modules_by_id: dict[str, Module] = {m.id: m for m in manifest.modules}

    for module in order:
        _collect_module_warnings()

        if not getattr(module, "enabled", True):
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
            continue

        if module.type not in _SUPPORTED_TYPES:
            raise ExecuteError(
                f"Module type {module.type!r} (id={module.id!r}) is not supported by the "
                f"DuckDB engine in Stage A. Supported: {sorted(m.value for m in _SUPPORTED_TYPES)}."
            )

        if included_ids is not None and module.id not in included_ids:
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
            continue

        if resume_dir:
            module_ckpt = resume_dir / module.id
            done_marker = module_ckpt / "_aq_done"
            if done_marker.exists():
                if module.type in (ModuleType.Ingress, ModuleType.Channel, ModuleType.Funnel):
                    data_ckpt = module_ckpt / "data" / "part-0.parquet"
                    if data_ckpt.exists():
                        frame_store[module.id] = con.read_parquet(str(data_ckpt))
                elif module.type == ModuleType.Junction:
                    for branch in module.config.get("branches", []):
                        bid = branch.get("id", "")
                        branch_ckpt = module_ckpt / bid / "part-0.parquet"
                        if branch_ckpt.exists():
                            frame_store[f"{module.id}.{bid}"] = con.read_parquet(str(branch_ckpt))
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))
                logger.info("Module %r: resumed from checkpoint, skipping execution", module.id)
                continue

        # ── Ingress ──────────────────────────────────────────────────────
        if module.type == ModuleType.Ingress:
            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                rel = _with_retry(lambda: read_ingress(module, con, base_dir=manifest.base_dir), mod_policy, module.id)
            except IngressError as exc:
                gate_closed, fail_result = _on_retry_exhausted(exc, mod_policy, module, manifest.blueprint_id, run_id, module_results)
                if gate_closed:
                    frame_store[module.id] = _GATE_CLOSED
                    continue
                return fail_result
            frame_store[module.id] = rel
            _write_checkpoint(con, module, checkpoint_dir, manifest, data={"data": rel})
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

        # ── Channel ──────────────────────────────────────────────────────
        elif module.type == ModuleType.Channel:
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] Channel has no main-port incoming edges"))
                return _fail(manifest.blueprint_id, run_id, module_results)

            upstream: dict[str, Any] = {}
            gate_closed_upstream = False
            for edge in main_edges:
                val = frame_store.get(edge.from_id)
                if _is_gate_closed(val):
                    frame_store[module.id] = _GATE_CLOSED
                    module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                    gate_closed_upstream = True
                    break
                if val is None:
                    module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] upstream {edge.from_id!r} produced no relation."))
                    return _fail(manifest.blueprint_id, run_id, module_results)
                upstream[_effective_frame_key(edge, modules_by_id)] = val
            if gate_closed_upstream:
                continue

            # ── Incremental watermark injection ────────────────────────
            # `materialize`/`watermark_column` are declared module fields
            # (2.40), not `config:` keys — see
            # `aqueduct.parser.schema.ModuleSchema.materialize`. Mirrors
            # `executor/spark/executor.py`'s Channel branch exactly.
            _incremental = module.materialize == "incremental"
            _channel_module = module
            _watermark_col = ""
            _depot_key = ""
            if _incremental:
                _watermark_col = module.watermark_column or ""
                _depot_key = f"{manifest.blueprint_id}:{module.id}:_watermark"
                # Depot is the sole watermark store (same as Spark).
                _watermark_val = (
                    (depot.get(_depot_key, "") if depot else "")
                    or "1900-01-01 00:00:00"
                )
                _query = module.config.get("query", "")
                _patched_query = _query.replace("${ctx._watermark}", f"'{_watermark_val}'")
                if _patched_query != _query:
                    _channel_module = dataclasses.replace(
                        module, config={**module.config, "query": _patched_query},
                    )
                # Warn if downstream Egress uses mode=overwrite (compiler
                # also warns statically; this is the runtime echo, same as
                # Spark's).
                _downstream_ids = {e.to_id for e in manifest.edges if e.from_id == module.id}
                for _ds_m in manifest.modules:
                    if (
                        _ds_m.id in _downstream_ids
                        and _ds_m.type == ModuleType.Egress
                        and _ds_m.config.get("mode") == "overwrite"
                    ):
                        logger.warning(
                            "[runtime_incremental_overwrite] [%s] "
                            "materialize=incremental → downstream Egress "
                            "%r uses mode=overwrite; incremental rows will "
                            "replace prior data.",
                            module.id, _ds_m.id,
                        )

            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                rel = _with_retry(lambda: execute_channel(_channel_module, upstream, con), mod_policy, module.id)
            except ChannelError as exc:
                gate_closed, fail_result = _on_retry_exhausted(exc, mod_policy, module, manifest.blueprint_id, run_id, module_results)
                if gate_closed:
                    frame_store[module.id] = _GATE_CLOSED
                    continue
                return fail_result

            spillway_condition: str | None = module.config.get("spillway_condition")
            has_spillway_edge = any(e.from_id == module.id and e.port == "spillway" for e in manifest.edges)
            if spillway_condition and has_spillway_edge:
                frame_store[module.id] = rel.filter(f"NOT ({spillway_condition})")
                # Stamp the same error columns Spark's Channel spillway
                # branch does (executor/spark/executor.py's
                # SpillwayCondition path) so a typed spillway edge
                # (edge.error_types) filtering on _aq_error_type finds the
                # column on either engine instead of hitting a DuckDB
                # Binder error against a relation that never had it.
                frame_store[f"{module.id}.spillway"] = rel.filter(spillway_condition).project(
                    f"*, {_sql_literal(module.id)} AS {_AQ_ERROR_MODULE}, "
                    f"{_sql_literal('spillway_condition matched')} AS {_AQ_ERROR_MSG}, "
                    f"{_sql_literal('SpillwayCondition')} AS {_AQ_ERROR_TYPE}, "
                    f"current_timestamp AS {_AQ_ERROR_TS}"
                )
            elif spillway_condition and not has_spillway_edge:
                frame_store[module.id] = rel
            elif has_spillway_edge and not spillway_condition:
                frame_store[module.id] = rel
                frame_store[f"{module.id}.spillway"] = rel.filter("1=0")
            else:
                frame_store[module.id] = rel

            # ── Incremental watermark — defer update to post-Egress ────
            # The actual MAX computation happens after the Egress write,
            # reading from the materialized output instead of re-executing
            # the DAG (same as Spark).
            if _incremental and _watermark_col:
                for _eg_id in _find_downstream_egress_ids(module.id, manifest):
                    _pending_watermarks[_eg_id] = (module.id, _watermark_col, _depot_key)

            _write_checkpoint(con, module, checkpoint_dir, manifest, data={"data": frame_store[module.id]})
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

        # ── Junction ─────────────────────────────────────────────────────
        elif module.type == ModuleType.Junction:
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] Junction has no main-port incoming edges"))
                return _fail(manifest.blueprint_id, run_id, module_results)
            upstream_id = main_edges[0].from_id
            val = frame_store.get(upstream_id)
            if _is_gate_closed(val):
                for branch in module.config.get("branches", []):
                    frame_store[f"{module.id}.{branch.get('id', '')}"] = _GATE_CLOSED
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue
            if val is None:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] upstream {upstream_id!r} produced no relation."))
                return _fail(manifest.blueprint_id, run_id, module_results)

            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                branch_rels = _with_retry(lambda: execute_junction(module, val), mod_policy, module.id)
            except JunctionError as exc:
                gate_closed, fail_result = _on_retry_exhausted(exc, mod_policy, module, manifest.blueprint_id, run_id, module_results)
                if gate_closed:
                    for branch in module.config.get("branches", []):
                        frame_store[f"{module.id}.{branch.get('id', '')}"] = _GATE_CLOSED
                    continue
                return fail_result
            for branch_id, branch_rel in branch_rels.items():
                frame_store[f"{module.id}.{branch_id}"] = branch_rel
            _write_checkpoint(con, module, checkpoint_dir, manifest, data=branch_rels)
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

        # ── Funnel ───────────────────────────────────────────────────────
        elif module.type == ModuleType.Funnel:
            data_edges = _incoming_data(module.id, manifest.edges)
            if not data_edges:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] Funnel has no incoming data edges"))
                return _fail(manifest.blueprint_id, run_id, module_results)

            funnel_upstream: dict[str, Any] = {}
            skipped = False
            for edge in data_edges:
                store_key = _frame_key(edge.from_id, edge.port)
                val = frame_store.get(store_key)
                if _is_gate_closed(val):
                    skipped = True
                    break
                if val is None:
                    module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] upstream {store_key!r} produced no relation."))
                    return _fail(manifest.blueprint_id, run_id, module_results)
                if edge.port == "spillway" and edge.error_types:
                    val = val.filter(
                        f"{_AQ_ERROR_TYPE} IN (" + ",".join(f"'{t}'" for t in edge.error_types) + ")"
                    )
                funnel_upstream[_effective_frame_key(edge, modules_by_id)] = val
            if skipped:
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue

            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                rel = _with_retry(lambda: execute_funnel(module, funnel_upstream, con), mod_policy, module.id)
            except FunnelError as exc:
                gate_closed, fail_result = _on_retry_exhausted(exc, mod_policy, module, manifest.blueprint_id, run_id, module_results)
                if gate_closed:
                    frame_store[module.id] = _GATE_CLOSED
                    continue
                return fail_result
            frame_store[module.id] = rel
            _write_checkpoint(con, module, checkpoint_dir, manifest, data={"data": rel})
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

        # ── Assert (Pass D — quality gates, quarantine via spillway port) ──
        elif module.type == ModuleType.Assert:
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] Assert has no main-port incoming edges"))
                return _fail(manifest.blueprint_id, run_id, module_results)
            upstream_id = main_edges[0].from_id
            val = frame_store.get(upstream_id)
            if _is_gate_closed(val):
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue
            if val is None:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] upstream {upstream_id!r} produced no relation."))
                return _fail(manifest.blueprint_id, run_id, module_results)

            has_spillway_edge = any(e.from_id == module.id and e.port == "spillway" for e in manifest.edges)

            try:
                passing_rel, quarantine_rel = execute_assert(
                    module, val, con, run_id, manifest.blueprint_id, base_dir=manifest.base_dir,
                )
            except AssertError as exc:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=str(exc), error_type=exc.error_type, exception=exc))
                return _fail(manifest.blueprint_id, run_id, module_results, trigger_agent=exc.trigger_agent)
            except Exception as exc:  # noqa: BLE001 — assert dispatch must fail cleanly, not leak a raw traceback
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=str(exc), exception=exc))
                return _fail(manifest.blueprint_id, run_id, module_results)

            frame_store[module.id] = passing_rel
            if quarantine_rel is not None and has_spillway_edge:
                frame_store[f"{module.id}.spillway"] = quarantine_rel
            elif quarantine_rel is not None:
                logger.warning(
                    "[runtime_assert_quarantine_no_spillway] [%s] Assert "
                    "quarantine rows produced but no spillway edge; discarded.",
                    module.id,
                )
            elif has_spillway_edge:
                # A quarantine-eligible rule (e.g. `custom`) reported no rows
                # to quarantine this run — still supply an empty relation so
                # a wired spillway Egress doesn't see "upstream produced no
                # relation" (same rule as the Channel spillway_condition
                # mismatch case above).
                frame_store[f"{module.id}.spillway"] = val.filter("1=0")
            # No _write_checkpoint call here — mirrors Spark's Assert branch,
            # which does not checkpoint either (and the resume-from-checkpoint
            # branch above never special-cases ModuleType.Assert).
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

        # ── Regulator (pass-through gate; identical logic to Spark's) ──────
        elif module.type == ModuleType.Regulator:
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] Regulator has no main-port incoming edges"))
                return _fail(manifest.blueprint_id, run_id, module_results)
            upstream_id = main_edges[0].from_id
            val = frame_store.get(upstream_id)
            if _is_gate_closed(val):
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue
            if val is None:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] upstream {upstream_id!r} produced no relation."))
                return _fail(manifest.blueprint_id, run_id, module_results)

            timeout_sec = float(module.config.get("timeout_seconds", 0))
            poll_interval = max(0.5, float(module.config.get("poll_seconds", 30.0)))
            elapsed = 0.0
            gate_open = surveyor.evaluate_regulator(module.id) if surveyor else True
            if not gate_open and timeout_sec > 0 and surveyor:
                while not gate_open and elapsed < timeout_sec:
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                    gate_open = surveyor.evaluate_regulator(module.id)

            if gate_open:
                frame_store[module.id] = val
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))
            else:
                on_block = module.config.get("on_block", "skip")
                if on_block == "abort":
                    module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] Regulator gate closed; on_block=abort"))
                    return _fail(manifest.blueprint_id, run_id, module_results)
                if on_block == "trigger_agent":
                    module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] Regulator gate closed; on_block=trigger_agent"))
                    return _fail(manifest.blueprint_id, run_id, module_results, trigger_agent=True)
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))

        # ── Egress ───────────────────────────────────────────────────────
        elif module.type == ModuleType.Egress:
            data_edges = _incoming_data(module.id, manifest.edges)
            if not data_edges:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] no main-port edge arriving at this Egress module"))
                return _fail(manifest.blueprint_id, run_id, module_results)
            edge = data_edges[0]
            key = _frame_key(edge.from_id, edge.port)
            val = frame_store.get(key)
            if _is_gate_closed(val):
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue
            if val is None:
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.ERROR, error=f"[{module.id}] upstream {edge.from_id!r} produced no relation."))
                return _fail(manifest.blueprint_id, run_id, module_results)
            if edge.port == "spillway" and edge.error_types:
                val = val.filter(f"{_AQ_ERROR_TYPE} IN (" + ",".join(f"'{t}'" for t in edge.error_types) + ")")

            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                _with_retry(lambda: write_egress(val, module, con, depot=depot, base_dir=manifest.base_dir), mod_policy, module.id)
            except EgressError as exc:
                gate_closed, fail_result = _on_retry_exhausted(exc, mod_policy, module, manifest.blueprint_id, run_id, module_results)
                if gate_closed:
                    continue
                return fail_result
            _write_checkpoint(con, module, checkpoint_dir, manifest)

            # ── Watermark update (Depot-only) — mirrors Spark's ────────────
            if module.id in _pending_watermarks:
                _ch_id, _wm_col, _wm_depot_key = _pending_watermarks.pop(module.id)
                _eg_path = module.config.get("path", "")
                _eg_fmt = module.config.get("format", "parquet")
                if _eg_path and _eg_fmt not in ("depot",):
                    _new_wm = _compute_watermark_from_output(con, _eg_path, _eg_fmt, _wm_col)
                    if _new_wm is None:
                        logger.warning(
                            "[runtime_watermark_compute_failed] [%s] Could "
                            "not compute watermark from output path %r; "
                            "watermark not advanced this run.",
                            module.id, _eg_path,
                        )
                    elif depot is None:
                        logger.warning(
                            "[runtime_watermark_no_depot] [%s] Incremental "
                            "Channel %r advanced watermark to %s=%s but no "
                            "depot is configured — it cannot be persisted, "
                            "so the next run re-scans all source data. "
                            "Configure stores.depots.",
                            module.id, _ch_id, _wm_col, _new_wm,
                        )
                    else:
                        depot.put(_wm_depot_key, _new_wm)
                        logger.debug(
                            "[%s] Watermark persisted to depot: %s=%s",
                            module.id, _wm_col, _new_wm,
                        )

            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

        # ── Handoff (Phase 81 step 3 — cross-engine boundary) ───────────────
        # Same shape as Spark's Handoff branch (executor/spark/executor.py):
        # a main-port incoming edge present in THIS sub-manifest means this
        # island produced the data (WRITE side); its absence means this
        # island only consumes it via the outgoing edge to `to_module`
        # (READ side). Engine-native parquet on both sides — DuckDB's own
        # `COPY ... TO ... (FORMAT PARQUET)` / `read_parquet`, no fsspec.
        # Spill directories are treated as a single-file-per-boundary
        # directory (`<spill_uri>/part-0.parquet`), the same convention
        # DuckDB's own checkpoint writer already uses — this and Spark's
        # multi-part `df.write.parquet(dir)` are cross-readable because both
        # sides glob/scan every `*.parquet` file under the directory.
        elif module.type == ModuleType.Handoff:
            # `_incoming_data` (any non-signal port), NOT `_incoming_main` —
            # see the matching fix + comment in `executor/spark/executor.py`'s
            # Handoff branch: a Junction branch edge crossing the boundary
            # directly preserves its branch-id port onto the handoff's
            # incoming edge, not `"main"`, and `_incoming_main` silently
            # misdispatched that WRITE side as a READ side.
            main_edges = _incoming_data(module.id, manifest.edges)
            spill_uri = (handoff_spill_uris or {}).get(module.id)
            if not spill_uri:
                module_results.append(_mr(
                    module_id=module.id, status=ExecutionStatus.ERROR,
                    error=f"[{module.id}] no spill URI resolved for this handoff module",
                ))
                return _fail(manifest.blueprint_id, run_id, module_results)

            _t0 = time.monotonic()
            if main_edges:
                # WRITE side.
                edge = main_edges[0]
                key = _frame_key(edge.from_id, edge.port)
                val = frame_store.get(key)
                if _is_gate_closed(val):
                    module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                    continue
                if val is None:
                    module_results.append(_mr(
                        module_id=module.id, status=ExecutionStatus.ERROR,
                        error=f"[{module.id}] upstream {edge.from_id!r} produced no relation.",
                    ))
                    return _fail(manifest.blueprint_id, run_id, module_results)

                if not is_remote_uri(spill_uri):
                    Path(spill_uri).mkdir(parents=True, exist_ok=True)
                part_path = f"{spill_uri.rstrip('/')}/part-0.parquet"
                input_name = f"__handoff_write_{module.id.replace('-', '_')}__"
                con.register(input_name, val)
                try:
                    con.sql(f"COPY {input_name} TO '{_escape(part_path)}' (FORMAT PARQUET)")
                except Exception as exc:
                    module_results.append(_mr(
                        module_id=module.id, status=ExecutionStatus.ERROR,
                        error=f"[{module.id}] handoff spill write to {spill_uri!r} failed: {exc}",
                        exception=exc,
                    ))
                    return _fail(manifest.blueprint_id, run_id, module_results)
                finally:
                    con.unregister(input_name)
                _write_handoff_metrics(
                    store_dir, observability_store, run_id, module.id,
                    {"bytes_written": dir_size_bytes(spill_uri),
                     "duration_ms": int((time.monotonic() - _t0) * 1000)},
                )
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))
            else:
                # READ side.
                glob_path = f"{spill_uri.rstrip('/')}/*.parquet"
                try:
                    frame_store[module.id] = con.read_parquet(glob_path)
                except Exception as exc:
                    module_results.append(_mr(
                        module_id=module.id, status=ExecutionStatus.ERROR,
                        error=f"[{module.id}] handoff spill read from {spill_uri!r} failed: {exc}",
                        exception=exc,
                    ))
                    return _fail(manifest.blueprint_id, run_id, module_results)
                _write_handoff_metrics(
                    store_dir, observability_store, run_id, module.id,
                    {"bytes_read": dir_size_bytes(spill_uri),
                     "duration_ms": int((time.monotonic() - _t0) * 1000)},
                )
                module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

        # ── Probe (Pass F — signal capture for Regulator gates) ─────────────
        # Same shape as Spark's Probe branch: a Probe has no main-port edge
        # (it binds via attach_to, positioned right after its target by
        # _build_execution_order above), so it reads frame_store directly
        # instead of going through _incoming_main.
        elif module.type == ModuleType.Probe:
            source_id = module.attach_to
            source_val = frame_store.get(source_id) if source_id else None
            _probe_notes: tuple[str, ...] = ()

            if source_val is None or _is_gate_closed(source_val):
                logger.debug(
                    "Probe %r: attach_to=%r not available; skipping.",
                    module.id, source_id,
                )
            elif store_dir is not None:
                try:
                    _probe_notes = execute_probe(
                        module, source_val, con, run_id, store_dir,
                        block_full_actions=block_full_actions,
                        observability_store=observability_store,
                        sampling=sampling,
                        base_dir=manifest.base_dir,
                        target_module=modules_by_id.get(source_id) if source_id else None,
                    ) or ()
                except Exception as exc:
                    logger.warning("[runtime_probe_error] Probe %r failed: %s", module.id, exc)
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS, notes=_probe_notes))

    return ExecutionResult(
        blueprint_id=manifest.blueprint_id, run_id=run_id, status=ExecutionStatus.SUCCESS,
        module_results=tuple(module_results),
    )


def _reachable_forward(start_id: str, edges: tuple[Edge, ...]) -> set[str]:
    graph: dict[str, list[str]] = defaultdict(list)
    for e in edges:
        if _is_data_edge(e):
            graph[e.from_id].append(e.to_id)
    visited: set[str] = set()
    queue: deque[str] = deque([start_id])
    while queue:
        mid = queue.popleft()
        if mid in visited:
            continue
        visited.add(mid)
        queue.extend(graph[mid])
    return visited


def _reachable_backward(start_id: str, edges: tuple[Edge, ...]) -> set[str]:
    rev: dict[str, list[str]] = defaultdict(list)
    for e in edges:
        if _is_data_edge(e):
            rev[e.to_id].append(e.from_id)
    visited: set[str] = set()
    queue: deque[str] = deque([start_id])
    while queue:
        mid = queue.popleft()
        if mid in visited:
            continue
        visited.add(mid)
        queue.extend(rev[mid])
    return visited


def _selector_included(
    modules: tuple[Module, ...], edges: tuple[Edge, ...], from_module: str | None, to_module: str | None,
) -> set[str] | None:
    if not from_module and not to_module:
        return None
    all_ids = {m.id for m in modules}
    if from_module:
        if from_module not in all_ids:
            raise ExecuteError(f"--from module {from_module!r} not found in Manifest")
        forward = _reachable_forward(from_module, edges)
    else:
        forward = all_ids
    if to_module:
        if to_module not in all_ids:
            raise ExecuteError(f"--to module {to_module!r} not found in Manifest")
        backward = _reachable_backward(to_module, edges)
    else:
        backward = all_ids
    included = forward & backward

    # Probes (Pass F): include when their tap target is included — same rule
    # as Spark's _selector_included (a Probe has no data edge of its own).
    for m in modules:
        if m.type == ModuleType.Probe and m.attach_to in included:
            included.add(m.id)

    return included


__all__ = ["execute", "ExecuteError"]
