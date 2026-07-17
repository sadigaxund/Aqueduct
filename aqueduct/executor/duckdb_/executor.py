"""Executor orchestrator — runs a Manifest against a live DuckDB connection.

Stage A supported module types: Ingress, Channel, Egress, Junction, Funnel,
Regulator. Assert and Probe are UNSUPPORTED this stage (see
``capabilities.yml``'s ``module.type.Assert`` / ``module.type.Probe`` rows) —
a Blueprint using either gets a clean ``CompileError`` before it ever reaches
this module, so ``_SUPPORTED_TYPES`` below never actually sees them; the
check exists as defense in depth, mirroring Spark's.

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

What this stage does NOT implement (honest gaps, not silent ones):
  - Parallel component execution (``feature.parallel_mode`` — UNSUPPORTED).
    Execution is always serial. DuckDB itself parallelizes a single query
    across threads internally; the Aqueduct-level "independent DAG subtrees
    on separate Python threads" optimization Spark's executor has is simply
    not built yet.
  - Incremental Channel materialize / watermark persistence. Not gated by a
    capability leaf (``materialize`` is a freeform Channel config key), so a
    Blueprint using it compiles cleanly but the ``${ctx._watermark}`` token
    is never substituted — it reaches DuckDB verbatim and fails at the SQL
    layer with a plain column-not-found error. Documented as a known Stage A
    gap in the Phase 78 report, not hidden.
  - Sandbox/lineage/explain-snapshot capture, per-module runtime metrics
    persistence to the observability store, and hook firing are all left to
    the caller (the ``aqueduct run`` CLI path composes those around
    ``ExecutorProtocol.execute`` for any engine already — see
    ``aqueduct/cli/blueprint.py``) exactly as they are for Spark today for
    everything past `ExecutionResult`.
"""

from __future__ import annotations

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

from aqueduct.errors import AqueductError
from aqueduct.executor.duckdb_.channel import ChannelError, execute_channel
from aqueduct.executor.duckdb_.egress import EgressError, write_egress
from aqueduct.executor.duckdb_.funnel import FunnelError, execute_funnel
from aqueduct.executor.duckdb_.ingress import IngressError, read_ingress
from aqueduct.executor.duckdb_.junction import JunctionError, execute_junction
from aqueduct.executor.models import (
    ExecutionResult,
    ExecutionStatus,
    ModuleResult,
    _collect_module_warnings,
    concise_error,
)
from aqueduct.models import Edge, Manifest, Module, ModuleType, RetryPolicy

logger = logging.getLogger(__name__)


def _mr(**kwargs) -> ModuleResult:
    return ModuleResult(**kwargs, warnings=_collect_module_warnings())


_T = TypeVar("_T")


class ExecuteError(AqueductError):
    """Raised for unrecoverable execution failures (config, unsupported type, etc.)."""


# Module types this executor actually dispatches. Assert/Probe are declared
# UNSUPPORTED in capabilities.yml and are rejected at compile time before a
# Manifest naming them ever reaches here — this set is defense in depth, same
# as Spark's `_SUPPORTED_TYPES`.
_SUPPORTED_TYPES: frozenset[str] = frozenset(
    {ModuleType.Ingress, ModuleType.Channel, ModuleType.Egress,
     ModuleType.Junction, ModuleType.Funnel, ModuleType.Regulator}
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
    """Topo-sort dispatchable modules. Probe/Assert are excluded — Stage A
    never dispatches them (see module docstring); they never reach this
    function on a real compile because the capability gate refuses them
    first, but the filter is kept as defense in depth.
    """
    dispatchable = tuple(
        m for m in manifest.modules if m.type not in (ModuleType.Probe, ModuleType.Assert)
    )
    return _topo_sort(dispatchable, manifest.edges)


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


def _manifest_hash(manifest: Manifest) -> str:
    import hashlib
    import json as _json
    return hashlib.sha256(_json.dumps(manifest.to_dict(), sort_keys=True).encode()).hexdigest()[:12]


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
) -> ExecutionResult:
    """Execute a compiled Manifest against a live DuckDB connection.

    Args mirror Spark's ``execute()`` where the concept exists on this engine;
    Spark-only kwargs (``spark``/``parallel``/``use_observe``/``sampling``/
    ``explain_capture``) have no DuckDB counterpart this stage and are simply
    not part of this signature — see ``ExecutorProtocol.execute``'s docstring:
    "the uniform part of the contract is Manifest in, ExecutionResult out;
    kwargs are a per-engine extension point."

    ``block_full_actions`` is accepted for Blueprint/config parity (Probe's
    ``config.danger.allow_full_probe_actions`` leaf is SUPPORTED even though
    Probe itself is not — see capabilities.yml) but Stage A never executes a
    Probe module, so it is currently inert.

    Raises:
        ExecuteError: Unsupported module type, cycle detected.
    """
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

    order = _build_execution_order(manifest)

    included_ids: set[str] | None = None
    if from_module or to_module:
        included_ids = _selector_included(manifest.modules, manifest.edges, from_module, to_module)

    frame_store: dict[str, Any] = {}
    module_results: list[ModuleResult] = []
    _pending: dict[str, Any] = {}

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
                upstream[edge.from_id] = val
            if gate_closed_upstream:
                continue

            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                rel = _with_retry(lambda: execute_channel(module, upstream, con), mod_policy, module.id)
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
                frame_store[f"{module.id}.spillway"] = rel.filter(spillway_condition)
            elif spillway_condition and not has_spillway_edge:
                frame_store[module.id] = rel
            elif has_spillway_edge and not spillway_condition:
                frame_store[module.id] = rel
                frame_store[f"{module.id}.spillway"] = rel.filter("1=0")
            else:
                frame_store[module.id] = rel

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
                        "_aq_error_type IN (" + ",".join(f"'{t}'" for t in edge.error_types) + ")"
                    )
                funnel_upstream[store_key] = val
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
                val = val.filter("_aq_error_type IN (" + ",".join(f"'{t}'" for t in edge.error_types) + ")")

            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                _with_retry(lambda: write_egress(val, module, con, depot=depot, base_dir=manifest.base_dir), mod_policy, module.id)
            except EgressError as exc:
                gate_closed, fail_result = _on_retry_exhausted(exc, mod_policy, module, manifest.blueprint_id, run_id, module_results)
                if gate_closed:
                    continue
                return fail_result
            _write_checkpoint(con, module, checkpoint_dir, manifest)
            module_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

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
    return forward & backward


__all__ = ["execute", "ExecuteError"]
