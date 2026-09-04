"""Phase 29a — Patch preview + validation pyramid (Gates 2 and 3).

This module sits next to `aqueduct/patch/apply.py` and `aqueduct/patch/grammar.py`
and powers two surfaces:

- `aqueduct patch preview <patch>` — interactive review of a pending patch.
  Renders a unified diff of the Blueprint YAML, then runs Gate 2 (lineage
  impact) and optionally Gate 3 (sandbox replay) against the patched
  Blueprint.

- The internal `_apply_patch_in_memory` path used by `auto` mode
  — invokes `run_lineage_gate` and `run_sandbox_gate` ahead of the
  expensive full-pipeline re-run controlled by `agent.patch_validation`.

Both gates operate on **live** lineage computed from the patched Blueprint
via `sqlglot` — no dependency on a pre-existing successful run is required,
so the gates work on brand-new pipelines.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from aqueduct.compiler.lineage import _extract_sql_lineage
from aqueduct.parser.models import ModuleType
from aqueduct.patch.gate_status import GateStatus

logger = logging.getLogger(__name__)


# ── Result dataclasses ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LineageWarning:
    """A single lineage-gate finding."""

    severity: str  # "warn" | "fail"
    channel_id: str  # module that was patched
    consumer_module: str  # downstream module that breaks
    missing_column: str  # column the consumer reads but the patch removes
    detail: str


@dataclass
class LineageGateResult:
    # See `aqueduct/patch/gate_status.py` for the vocabulary the gates
    # share: "pass" | "warn" | "fail" | "not_applicable". This gate has no
    # `unavailable` path — its analysis is pure sqlglot over the two
    # Blueprint dicts it is handed, with no engine, session or store that
    # could be missing.
    status: str = GateStatus.PASS
    warnings: list[LineageWarning] = field(default_factory=list)
    touched_modules: list[str] = field(default_factory=list)
    duration_ms: int = 0
    # Short human-readable reason. Populated on the `not_applicable` path (why
    # the gate has no signal for this patch); the sibling gate
    # `SandboxGateResult` already carries an analogous
    # `detail` field for its non-structured status explanation, so this
    # follows the same vocabulary rather than overloading `warnings` — a
    # `list[LineageWarning]` is typed and rendered specifically as *findings*
    # (missing-column regressions), which an informational "nothing to check"
    # state is not.
    detail: str = ""


@dataclass
class SandboxGateResult:
    # "pass" | "fail" | "not_applicable" | "unavailable" — see
    # `aqueduct/patch/gate_status.py`. `not_applicable` means no replay was
    # owed; `unavailable` means one was owed and the environment prevented
    # it, and is the one status here that BLOCKS auto-apply
    # (`sandbox_gate_permits_auto_apply`). This gate has no `warn`: a replay
    # either ran, or did not.
    status: str
    detail: str
    sample_rows: int | None = None
    duration_ms: int = 0
    egress_targets: list[dict[str, Any]] = field(default_factory=list)


# ── Touched-module detection from PatchSpec operations ────────────────────────


def touched_module_ids(patch_spec: Any) -> list[str]:
    """Return module IDs the patch ops mutate (config, label, structure)."""
    ids: list[str] = []
    for op in getattr(patch_spec, "operations", []) or []:
        # Pydantic ops carry the relevant ID/module dict on different fields
        if hasattr(op, "module_id") and getattr(op, "module_id", None):
            ids.append(op.module_id)
        elif hasattr(op, "module") and isinstance(op.module, dict):
            mid = op.module.get("id")
            if mid:
                ids.append(mid)
        elif hasattr(op, "from_id") and hasattr(op, "to_id"):
            # replace_edge touches both endpoints — only the consumer (to_id)
            # is interesting for downstream impact analysis
            if op.to_id:
                ids.append(op.to_id)
    # Preserve insertion order, de-dup
    seen: set[str] = set()
    return [m for m in ids if not (m in seen or seen.add(m))]  # type: ignore[func-returns-value]


# ── Live lineage from a raw Blueprint dict (no Manifest required) ─────────────


def _channel_modules(bp: dict) -> list[dict]:
    """Return raw module dicts whose type is a SQL-flavoured Channel."""
    out: list[dict] = []
    for m in bp.get("modules") or []:
        if not isinstance(m, dict):
            continue
        if m.get("type") != ModuleType.Channel.value:
            continue
        cfg = m.get("config") or {}
        if cfg.get("op") != "sql":
            continue
        out.append(m)
    return out


def _upstream_map(bp: dict) -> dict[str, list[str]]:
    """For every module, list of main-port upstream module IDs (parsed from edges)."""
    edges = bp.get("edges") or []
    by_to: dict[str, list[str]] = {}
    for e in edges:
        if not isinstance(e, dict):
            continue
        port = e.get("port", "main") or "main"
        if port != "main":
            continue
        from_id = e.get("from")
        to_id = e.get("to")
        if from_id and to_id:
            by_to.setdefault(to_id, []).append(from_id)
    return by_to


def _live_lineage_rows(bp: dict) -> list[dict[str, str]]:
    """Walk every Channel SQL in `bp` and return sqlglot-derived lineage rows."""
    upstream = _upstream_map(bp)
    rows: list[dict[str, str]] = []
    for m in _channel_modules(bp):
        mid = m.get("id") or ""
        sql = (m.get("config") or {}).get("query", "")
        if not mid or not sql:
            continue
        rows.extend(_extract_sql_lineage(mid, sql, upstream.get(mid, [])))
    return rows


def _output_columns_by_module(rows: Iterable[dict[str, str]]) -> dict[str, set[str]]:
    """Group lineage rows by `channel_id` → set of `output_column`."""
    out: dict[str, set[str]] = {}
    for r in rows:
        cid = r.get("channel_id") or ""
        col = r.get("output_column") or ""
        if cid and col:
            out.setdefault(cid, set()).add(col)
    return out


def _consumers_of(rows: Iterable[dict[str, str]], source_table: str) -> list[tuple[str, str]]:
    """Return `(consumer_module_id, source_column)` pairs that consume *source_table*."""
    out: list[tuple[str, str]] = []
    for r in rows:
        if r.get("source_table") == source_table:
            out.append((r.get("channel_id") or "", r.get("source_column") or ""))
    return out


# ── lineage gate ────────────────────────────────────────────────────────────────────


def run_lineage_gate(
    blueprint_before: dict,
    blueprint_after: dict,
    patch_spec: Any,
) -> LineageGateResult:
    """Compare live lineage before and after the patch.

    For each module the patch touches:
      - Compute output columns under the OLD Blueprint via sqlglot.
      - Compute output columns under the NEW Blueprint.
      - Look up consumers of the touched module (from OLD lineage).
      - For each consumer, check that every column it reads still exists in
        the NEW outputs. Missing columns become LineageWarning entries.

    Wildcards (`*`) in lineage rows are treated as "all columns present" — they
    suppress false positives when sqlglot couldn't resolve column names from
    `SELECT *` queries.

    Status:
      `pass`            no findings
      `warn`            at least one missing-column finding (default)
      `fail`            reserved for future hard breakage classes (kept for caller policy)
      `not_applicable`  the patch's operations touch zero modules — e.g.
                        `set_engine_config`, whose only fields are
                        `engine`/`key`/`value`, carries no `module_id`/
                        `module`/`from_id`/`to_id` for `touched_module_ids()`
                        to extract. This means the gate has NO signal for
                        this patch, which is
                        distinct from having checked it and found nothing —
                        collapsing the two into `pass` would silently claim a
                        column-impact check that never ran. Informational
                        only: it does not block the patch, exactly like the
                        pre-existing default did not (see `detail` for why).
    """
    t0 = time.monotonic()
    result = LineageGateResult()
    result.touched_modules = touched_module_ids(patch_spec)
    if not result.touched_modules:
        # Explicit, not a fallen-through default (AGENTS.md "No silent
        # no-ops" / "Never make the break go away with a default"): a patch
        # whose ops carry no module reference has no lineage surface for
        # this gate to check at all, so it must not be indistinguishable
        # from "checked and clean".
        result.status = GateStatus.NOT_APPLICABLE
        result.detail = "no module-lineage surface for this patch's ops"
        result.duration_ms = int((time.monotonic() - t0) * 1000)
        return result

    old_rows = _live_lineage_rows(blueprint_before)
    new_rows = _live_lineage_rows(blueprint_after)
    new_outputs = _output_columns_by_module(new_rows)

    for mid in result.touched_modules:
        consumers = _consumers_of(old_rows, mid)
        if not consumers:
            continue
        new_cols = new_outputs.get(mid, set())
        # Wildcard means "we cannot resolve the column set" — skip narrow checks
        if "*" in new_cols:
            continue
        for consumer_id, src_col in consumers:
            if src_col == "*" or not src_col:
                continue
            if src_col not in new_cols:
                result.warnings.append(
                    LineageWarning(
                        severity="warn",
                        channel_id=mid,
                        consumer_module=consumer_id,
                        missing_column=src_col,
                        detail=(
                            f"downstream module {consumer_id!r} consumes "
                            f"{src_col!r} from {mid!r} but the patch no longer "
                            "emits that column."
                        ),
                    )
                )

    if result.warnings:
        result.status = GateStatus.WARN
    result.duration_ms = int((time.monotonic() - t0) * 1000)
    return result


# ── Sandbox manifest transform (shared: gate + `run --sandbox`) ───────────────


def build_sandbox_manifest(manifest: Any, sample_rows: int) -> tuple[Any, list[dict[str, Any]]]:
    """Transform a compiled Manifest into a sandbox-safe one.

    Drops every Egress module (snapshotting its declared sink for the report)
    and, when ``sample_rows > 0``, marks every Ingress with a ``sandbox_limit``
    so the Spark ingress executor caps the read at ``sample_rows`` rows. Edges
    whose endpoints were dropped are removed.

    Returns ``(sandboxed_manifest, egress_targets)``. Shared by
    ``run_sandbox_gate`` (patch validation, Gate 3) and ``aqueduct run
    --sandbox`` (dev dry-run) so both strip/limit identically.
    """
    import dataclasses as _dc

    egress_targets: list[dict[str, Any]] = []
    sandboxed_modules = []
    for m in manifest.modules:
        if m.type == ModuleType.Egress:
            egress_targets.append(
                {
                    "id": m.id,
                    "format": (m.config or {}).get("format"),
                    "path": (m.config or {}).get("path"),
                    "mode": (m.config or {}).get("mode"),
                }
            )
            continue
        sandboxed_modules.append(m)

    if sample_rows and sample_rows > 0:
        sandboxed_modules = [
            (
                _dc.replace(m, config={**m.config, "sandbox_limit": sample_rows})
                if m.type == ModuleType.Ingress
                else m
            )
            for m in sandboxed_modules
        ]

    keep_ids = {m.id for m in sandboxed_modules}
    sandboxed_edges = [e for e in manifest.edges if e.from_id in keep_ids and e.to_id in keep_ids]
    sandboxed_manifest = _dc.replace(
        manifest,
        modules=tuple(sandboxed_modules),
        edges=tuple(sandboxed_edges),
    )
    return sandboxed_manifest, egress_targets


# ── sandbox gate sandbox replay ─────────────────────────────────────────────────────


def run_sandbox_gate(
    blueprint_after: dict,
    *,
    blueprint_path: Any,
    patch_id: str,
    failed_module: str | None,
    engine: str,
    cfg: Any,
    sample_rows: int = 1000,
    cli_overrides: dict[str, str] | None = None,
    profile: str | None = None,
    spark_session: Any = None,
    observability_store: Any = None,
    sandbox_master_url: str | None = None,
    warnings_suppress: Iterable[str] | None = None,
    timezone: str | None = None,
    patch_spec: Any = None,
    depot_reads_at_failure: dict[str, str] | None = None,
) -> SandboxGateResult:
    """Compile and replay the patched Blueprint with a row limit + Egress skipped.

    The sandbox:
      1. Parses + compiles the patched Blueprint via the standard Parser/Compiler.
      2. Drops every Egress module from the compiled Manifest and snapshots
         their declared sinks for the report.
      3. If `sample_rows > 0`, wraps every Ingress module with a synthetic
         `op: limit` Channel downstream so the rest of the DAG sees at most
         `sample_rows` rows. `sample_rows == 0` means "no limit".
      4. Runs the modified Manifest through the TARGET ENGINE's own
         ``ExecutorProtocol`` (``aqueduct.executor.protocol.get_protocol``) —
         the engine's own session factory builds the sandbox session and its
         own ``execute()`` runs the replay; no Egress writes happen.

    ``engine`` is REQUIRED (no default) — mirrors the ``Surveyor`` decision
    (``aqueduct/surveyor/surveyor.py``): every construction site must resolve
    and pass the real target engine rather than silently falling back to
    Spark, which is exactly the bug this signature closes (Phase 79). Callers
    resolve it the same way ``Surveyor``/``FailureContext`` do —
    ``cfg.deployment.engine`` (or, for a polyglot Manifest, the touched
    island's own ``.engine``).

    ``cfg`` (an ``AqueductConfig``) is REQUIRED (no default) — every
    production caller (``aqueduct/cli/patch.py``'s ``patch preview
    --sandbox``, ``aqueduct/cli/__init__.py``'s ``_run_patch_gates_inline``)
    already loads one before reaching this gate. It is used, when this gate
    builds its OWN sandbox session (``spark_session`` omitted), to resolve
    that session's ``SessionSpec.engine_config``/``engine_options`` through
    the SAME ``aqueduct.executor.session_config.resolve_session_engine_config``
    / ``session_secrets_options`` helpers ``aqueduct run`` uses — closing the
    gap where a DuckDB sandbox replay used to see NONE of ``engine.duckdb.*``
    (memory_limit/threads/database_path/extension_repository/s3_*) or any
    httpfs/secrets wiring a real ``engine=="duckdb"`` run would have applied
    (the resolver used to read ``manifest.spark_config`` on a Spark-only
    branch and fall through to nothing for every other engine — see
    ``resolve_session_engine_config``'s own docstring). Ignored when
    ``spark_session`` is given — that session was already built (with
    whatever config its own builder resolved) by the caller.

    ``spark_session``, if given, is used as-is regardless of engine (the
    caller already built it — e.g. the live session a heal loop is running
    against); named ``spark_session`` for backward compatibility with
    existing callers/tests predating multi-engine support, same precedent as
    ``aqueduct.agent.toolbox.ToolBox.spark_session``. When omitted, this gate
    builds and owns its own sandbox session via the target engine's
    ``ExecutorProtocol.session_factory()`` and tears it down afterwards
    through ``session_closer()``.

    ``timezone``: ``aqueduct.yml``'s top-level ``timezone:`` (Phase 81/82),
    forwarded to the owned sandbox session's ``SessionSpec`` when this gate
    builds its own session (ignored when ``spark_session`` is given — that
    session's timezone was already resolved by whoever built it).

    ``patch_spec``, if given, is used ONLY to word a ``pass`` result's
    ``detail`` honestly — never to change scope or status. When the patch's
    operations touch zero modules (``touched_module_ids(patch_spec)`` is
    empty — the SAME predicate ``run_lineage_gate`` already uses for its
    ``not_applicable`` status; today this means a `set_engine_config`-only
    patch), a sample replay proves the session built and rows flowed under
    the PATCHED engine config, but a 1000-row local sample can never
    reproduce the cluster-scale resource failure (OOM, shuffle spill) the
    patch is usually trying to fix — that is only proven by the full
    re-run. `pass`'s `detail` says so explicitly instead of reading as a
    validated fix. Omitted (the default): `detail` keeps its ordinary
    "sandbox replay succeeded" wording — unchanged for a module-touching
    patch, and unchanged for any caller that hasn't been updated to pass
    this new, optional parameter.

    Status (the shared vocabulary — ``aqueduct/patch/gate_status.py``):
      ``pass``            manifest compiled and executed without raising
      ``fail``            parse/compile error, or the executor surfaced a
                          module failure — the patch itself is refused
      ``unavailable``     a replay was OWED and the environment prevented
                          it: the sandbox's own imports failed, the target
                          engine is not registered, its session would not
                          start (the detail names the ACTUAL target engine,
                          never Spark), or the Blueprint is polyglot and a
                          single-engine replay would cover only one of its
                          engines. **Nothing about this patch was
                          verified**, so it BLOCKS auto-apply
                          (``sandbox_gate_permits_auto_apply``) and the
                          patch waits for a human.
      ``not_applicable``  no replay was owed. This gate never returns it
                          itself — a config-only patch still gets a real
                          replay here (see ``patch_spec`` above) — but
                          ``cli/__init__.py`` synthesises one for
                          ``agent.sandbox_mode: off``, where the operator
                          declared the check unowed.
      ``not_requested``  this gate was never invoked at all. This function
                          never returns it either — a CALLER that chose not
                          to invoke this gate (``aqueduct patch preview``
                          with no ``--sandbox``) synthesises it instead of
                          leaving the result ``None``, so
                          ``sandbox_gate_permits_auto_apply`` has an
                          explicit status to fail closed on rather than a
                          bare ``None``.

    ``unavailable`` and ``not_applicable`` were both spelled ``skip`` until
    the split: "I could not check this" and "there was nothing to check"
    are opposite facts, and reporting them with one word let a patch that
    was never replayed auto-apply as though it had been. ``not_requested``
    is a third, later fact again: "nobody asked" is neither of those two.

    ``depot_reads_at_failure``, if given, is the ``{key: value}`` map of
    depot reads resolved while compiling the Manifest that FAILED (see
    ``AqFunctions.depot_reads`` / ``compile(depot_reads_out=...)``). A
    failing run may itself have written to the depot (an Egress with
    ``format: depot``) before it failed, so the patch here is being
    validated against different inputs than the failure saw. This gate
    recompiles the patched Blueprint with its OWN depot-read sink and, for
    every key present in BOTH maps whose value differs, prints one
    ``depot key 'X' changed since failure: 'old' -> 'new'`` line to stderr
    and folds the same lines into a ``pass`` result's ``detail``. Purely
    informational: it NEVER changes ``status`` — a key present on only one
    side is not a staleness signal (the patch may have added/removed a
    depot reference) and is skipped.
    """
    t0 = time.monotonic()
    egress_targets: list[dict[str, Any]] = []

    try:
        from pathlib import Path as _Path

        from aqueduct.compiler.compiler import CompileError
        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.errors import AqueductError
        from aqueduct.executor.protocol import SessionSpec, call_execute, get_protocol
        from aqueduct.executor.session_config import (
            resolve_session_engine_config,
            session_secrets_options,
        )
        from aqueduct.parser.parser import ParseError, parse_dict
    except Exception as exc:  # pragma: no cover
        return SandboxGateResult(
            status=GateStatus.UNAVAILABLE,
            detail=(
                f"sandbox replay did not run — its own dependencies failed to "
                f"import ({exc}); this patch was NOT replayed"
            ),
            duration_ms=int((time.monotonic() - t0) * 1000),
        )

    try:
        protocol = get_protocol(engine)
    except Exception as exc:
        return SandboxGateResult(
            status=GateStatus.UNAVAILABLE,
            detail=(
                f"sandbox replay did not run — engine {engine!r} could not be "
                f"resolved ({exc}); this patch was NOT replayed"
            ),
            duration_ms=int((time.monotonic() - t0) * 1000),
        )

    # Parse the patched dict in-memory, anchored to the
    # original Blueprint's parent. Replaces the tempfile dance whose only
    # purpose was to feed the file-only parse(path) API; that detour
    # broke 1.1.0 path anchoring whenever the tempfile landed in ``/tmp``
    # and relative module paths resolved against ``/tmp``.
    _bp_orig = _Path(blueprint_path) if blueprint_path else None
    base_dir = _bp_orig.parent if _bp_orig and _bp_orig.exists() else _Path.cwd()
    try:
        try:
            bp = parse_dict(
                blueprint_after,
                base_dir=base_dir,
                profile=profile,
                cli_overrides=cli_overrides or None,
            )
        except ParseError as exc:
            return SandboxGateResult(
                status=GateStatus.FAIL,
                detail=f"patched Blueprint failed to parse: {exc}",
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        depot = None
        depots = None
        try:
            from aqueduct.depot.depot import preview_depots

            depot, depots = preview_depots(cfg, bp.id)
        except Exception as exc:  # pragma: no cover — depot build must never crash the gate
            logger.warning(
                "sandbox gate: could not build preview depot (%s) — "
                "@aq.depot.*/@aq.run.prev_id will hard-fail if this patch's Blueprint uses them",
                exc,
            )
            depot, depots = None, None

        _depot_reads_now: dict[str, str] = {}
        try:
            manifest = compiler_compile(
                bp,
                blueprint_path=_bp_orig,
                engine=engine,
                depot=depot,
                depots=depots,
                depot_reads_out=_depot_reads_now,
            )
        except CompileError as exc:
            return SandboxGateResult(
                status=GateStatus.FAIL,
                detail=f"patched Blueprint failed to compile: {exc}",
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        # ── Depot staleness notice (informational only — never touches status) ──
        # See this function's docstring, `depot_reads_at_failure` paragraph.
        _depot_staleness_notices: list[str] = []
        if depot_reads_at_failure is not None:
            for _key, _new_val in _depot_reads_now.items():
                _old_val = depot_reads_at_failure.get(_key)
                if _old_val is not None and _old_val != _new_val:
                    _depot_staleness_notices.append(
                        f"depot key {_key!r} changed since failure: {_old_val!r} → {_new_val!r}"
                    )
            if _depot_staleness_notices:
                import sys as _sys

                for _line in _depot_staleness_notices:
                    print(_line, file=_sys.stderr)

        # ── Polyglot Blueprints are not sandbox-replayed in v1 ──────────────────
        # A single-`engine` replay session (this function's whole design —
        # one `protocol`, one `session`) would validate a multi-engine
        # Blueprint against only ONE of its engines, which is a worse outcome
        # than no replay at all: it would look like a real pre-apply
        # validation while actually checking nothing about the islands on
        # any other engine. `unavailable`, not `fail`: nothing is known to
        # be wrong with the patch — the gate simply cannot cover this
        # Blueprint's shape. It is also NOT `not_applicable`: a replay was
        # owed here and did not happen, so the patch stops for a human
        # instead of being applied on the strength of a check that never
        # ran (until the split this was `skip`, which callers read as
        # acceptance and applied straight through).
        if len(manifest.islands) > 1:
            _island_engines = ", ".join(sorted({isl.engine for isl in manifest.islands}))
            return SandboxGateResult(
                status=GateStatus.UNAVAILABLE,
                detail=(
                    f"sandbox replay did not run — polyglot Blueprint "
                    f"({len(manifest.islands)} islands: {_island_engines}); this gate "
                    "replays through ONE engine's session and would leave every other "
                    "island unchecked. This patch was NOT replayed"
                ),
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        # ── Strip Egress + cap Ingress rows (shared with `run --sandbox`) ───────
        # The Spark ingress executor honours the `sandbox_limit` marker (if
        # present) by calling `.limit(N)` after `.load()` — see `ingress.py`.
        sandboxed_manifest, egress_targets = build_sandbox_manifest(manifest, sample_rows)

        # ── Engine session ────────────────────────────────────────────────────
        # Built THROUGH THE PROTOCOL REGISTRY, not a hardcoded `make_spark_session`
        # — the same seam `aqueduct/cli/run.py`'s real run path uses (Phase 78).
        # A DuckDB target gets a real DuckDB sandbox session here instead of
        # either crashing against a Spark session or a hardcoded "Spark
        # unavailable" skip that blames the wrong engine.
        session = spark_session
        _owns_session = False
        if session is None:
            _owns_session = True
            master = sandbox_master_url or "local[*]"
            if sandbox_master_url:
                logger.debug(
                    "Sandbox gate: connecting engine %r to master %r", engine, sandbox_master_url
                )
            try:
                session = protocol.session_factory()(
                    SessionSpec(
                        blueprint_id=f"aqueduct.sandbox.{patch_id}",
                        # Resolved through the SAME per-engine helper
                        # `aqueduct run` uses (Blueprint-level
                        # `manifest.engine_config[engine]` layered over
                        # `cfg.engine.<engine>`, Blueprint wins, for every
                        # registered engine) — not a Spark-only shortcut
                        # that used to make a DuckDB sandbox replay see
                        # NONE of `engine.duckdb.*` regardless of what a
                        # real run would apply.
                        engine_config=resolve_session_engine_config(cfg, engine, manifest),
                        master_url=master,
                        quiet=True,
                        quiet_startup=True,
                        timezone=timezone,
                        engine_options=session_secrets_options(cfg, manifest),
                    )
                )
            except Exception as exc:
                return SandboxGateResult(
                    status=GateStatus.UNAVAILABLE,
                    detail=(
                        f"sandbox replay did not run — engine {engine!r} would not "
                        f"start ({exc}); this patch was NOT replayed"
                    ),
                    egress_targets=egress_targets,
                    duration_ms=int((time.monotonic() - t0) * 1000),
                )

        # ── Run the executor ───────────────────────────────────────────────────
        try:
            try:
                # 1.1.0 fix — run the WHOLE patched DAG, not from `failed_module`
                # onwards. Skipping upstream Ingress/Channels left frame_store
                # empty for the failed module's inputs ("produced no DataFrame"
                # false-fail). sample_rows wrapping makes the full replay cheap
                # enough that the prior optimisation isn't worth the false
                # negatives.
                #
                # observability_store is a Spark-flavoured
                # optional capability (`OPTIONAL_EXECUTE_KWARGS`,
                # `aqueduct/executor/protocol.py`) — routed through
                # `call_execute()` so an engine that can't honour it (e.g.
                # DuckDB Stage A) gets a suppressible `engine_kwarg_ignored`
                # warning instead of a TypeError or a silent drop.
                result = call_execute(
                    engine,
                    sandboxed_manifest,
                    session,
                    run_id=f"sandbox-{patch_id}",
                    store_dir=None,
                    surveyor=None,
                    observability_store=observability_store,
                    suppress=warnings_suppress,
                )
            except AqueductError as exc:
                return SandboxGateResult(
                    status=GateStatus.FAIL,
                    detail=f"sandbox execution raised: {exc}",
                    sample_rows=sample_rows if sample_rows > 0 else None,
                    egress_targets=egress_targets,
                    duration_ms=int((time.monotonic() - t0) * 1000),
                )

            if result.status != "success":
                failing = next(
                    (r for r in result.module_results if r.status == "error"),
                    None,
                )
                return SandboxGateResult(
                    status=GateStatus.FAIL,
                    detail=(
                        f"sandbox run ended with status={result.status!r}"
                        + (
                            f"; first error in {failing.module_id!r}: {failing.error}"
                            if failing
                            else ""
                        )
                    ),
                    sample_rows=sample_rows if sample_rows > 0 else None,
                    egress_targets=egress_targets,
                    duration_ms=int((time.monotonic() - t0) * 1000),
                )

            is_config_only_patch = patch_spec is not None and not touched_module_ids(patch_spec)
            if is_config_only_patch:
                pass_detail = (
                    "session built and sample replay succeeded under the patched "
                    f"engine config against {sample_rows or '∞'} row(s) per Ingress; "
                    "sample replay cannot reproduce the originating resource "
                    "failure (e.g. cluster-scale OOM/shuffle spill); efficacy is "
                    "validated by the full re-run, not this gate"
                )
            else:
                pass_detail = (
                    f"sandbox replay succeeded against {sample_rows or '∞'} "
                    f"row(s) per Ingress; {len(egress_targets)} Egress module(s) skipped"
                )
            if _depot_staleness_notices:
                pass_detail = pass_detail + " | " + "; ".join(_depot_staleness_notices)
            return SandboxGateResult(
                status=GateStatus.PASS,
                detail=pass_detail,
                sample_rows=sample_rows if sample_rows > 0 else None,
                egress_targets=egress_targets,
                duration_ms=int((time.monotonic() - t0) * 1000),
            )
        finally:
            if _owns_session and session is not None:
                try:
                    protocol.session_closer()(session)
                except Exception:
                    pass  # best-effort teardown of a sandbox-owned session
    finally:
        # No tempfile to unlink; parse_dict() consumes the
        # patched Blueprint directly. Block retained for parity with previous
        # control flow in case future side-effects need teardown.
        pass


# ── Unified diff helper ───────────────────────────────────────────────────────


def render_unified_diff(blueprint_before: dict, blueprint_after: dict) -> str:
    """Return a unified-diff string of the Blueprint YAML representations."""
    import difflib
    from io import StringIO

    from aqueduct.patch.apply import _yaml_dumps

    before = _yaml_dumps(blueprint_before)
    after = _yaml_dumps(blueprint_after)
    diff = difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile="blueprint.yml (before)",
        tofile="blueprint.yml (after)",
        n=3,
    )
    buf = StringIO()
    buf.writelines(diff)
    return buf.getvalue()
