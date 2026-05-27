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
from dataclasses import dataclass, field
from typing import Any, Iterable

from aqueduct.compiler.lineage import _extract_sql_lineage

logger = logging.getLogger(__name__)


# ── Result dataclasses ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class LineageWarning:
    """A single lineage-gate finding."""

    severity: str               # "warn" | "fail"
    channel_id: str             # module that was patched
    consumer_module: str        # downstream module that breaks
    missing_column: str         # column the consumer reads but the patch removes
    detail: str


@dataclass
class LineageGateResult:
    status: str = "pass"        # "pass" | "warn" | "fail"
    warnings: list[LineageWarning] = field(default_factory=list)
    touched_modules: list[str] = field(default_factory=list)
    duration_ms: int = 0


@dataclass
class SandboxGateResult:
    status: str                 # "pass" | "fail" | "skip"
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
        if m.get("type") != "Channel":
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
      `pass`  no findings
      `warn`  at least one missing-column finding (default)
      `fail`  reserved for future hard breakage classes (kept for caller policy)
    """
    t0 = time.monotonic()
    result = LineageGateResult()
    result.touched_modules = touched_module_ids(patch_spec)
    if not result.touched_modules:
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
                result.warnings.append(LineageWarning(
                    severity="warn",
                    channel_id=mid,
                    consumer_module=consumer_id,
                    missing_column=src_col,
                    detail=(
                        f"downstream module {consumer_id!r} consumes "
                        f"{src_col!r} from {mid!r} but the patch no longer "
                        "emits that column."
                    ),
                ))

    if result.warnings:
        result.status = "warn"
    result.duration_ms = int((time.monotonic() - t0) * 1000)
    return result


# ── sandbox gate sandbox replay ─────────────────────────────────────────────────────

def run_sandbox_gate(
    blueprint_after: dict,
    *,
    blueprint_path: Any,
    patch_id: str,
    failed_module: str | None,
    sample_rows: int = 1000,
    cli_overrides: dict[str, str] | None = None,
    profile: str | None = None,
    spark_session: Any = None,
    observability_store: Any = None,
    lineage_store: Any = None,
    explain_capture: dict[str, dict] | None = None,
) -> SandboxGateResult:
    """Compile and replay the patched Blueprint with a row limit + Egress skipped.

    The sandbox:
      1. Parses + compiles the patched Blueprint via the standard Parser/Compiler.
      2. Drops every Egress module from the compiled Manifest and snapshots
         their declared sinks for the report.
      3. If `sample_rows > 0`, wraps every Ingress module with a synthetic
         `op: limit` Channel downstream so the rest of the DAG sees at most
         `sample_rows` rows. `sample_rows == 0` means "no limit".
      4. Runs the modified Manifest through the existing executor — the
         executor sees a real SparkSession but no Egress writes happen.

    Status:
      `pass`  manifest compiled and executed without raising
      `fail`  parse/compile error or executor surfaced a module failure
      `skip`  Spark unavailable (no session and no `make_spark_session` import)
    """
    t0 = time.monotonic()
    egress_targets: list[dict[str, Any]] = []

    try:
        from pathlib import Path as _Path
        import tempfile

        from aqueduct.parser.parser import ParseError, parse
        from aqueduct.compiler.compiler import CompileError, compile as compiler_compile
        from aqueduct.patch.apply import _yaml_dump
        from aqueduct.executor import ExecuteError, get_executor
    except Exception as exc:  # pragma: no cover
        return SandboxGateResult(
            status="skip",
            detail=f"sandbox dependencies missing: {exc}",
            duration_ms=int((time.monotonic() - t0) * 1000),
        )

    # ── Persist patched Blueprint to a tempfile so the standard Parser
    # ── (which reads from disk) can ingest it.
    # 1.1.0 — write the tempfile NEXT TO the original blueprint, not in
    # /tmp/, so the parser's path-anchoring rule (relative module paths
    # resolve to the blueprint's parent dir) still finds the real data
    # files. Using /tmp/ would cause every relative `path:` field to
    # resolve under /tmp/, breaking sandbox replay.
    _bp_orig = _Path(blueprint_path) if blueprint_path else None
    _anchor_dir = _bp_orig.parent if _bp_orig and _bp_orig.exists() else None
    with tempfile.NamedTemporaryFile(
        suffix=".aq-sandbox.yml",
        delete=False,
        mode="w",
        dir=str(_anchor_dir) if _anchor_dir else None,
    ) as tmp:
        tmp_path = _Path(tmp.name)
    try:
        _yaml_dump(blueprint_after, tmp_path)

        try:
            bp = parse(str(tmp_path), profile=profile, cli_overrides=cli_overrides or None)
        except ParseError as exc:
            return SandboxGateResult(
                status="fail",
                detail=f"patched Blueprint failed to parse: {exc}",
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        try:
            manifest = compiler_compile(bp, blueprint_path=tmp_path)
        except CompileError as exc:
            return SandboxGateResult(
                status="fail",
                detail=f"patched Blueprint failed to compile: {exc}",
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        # ── Capture + strip Egress modules ──────────────────────────────────────
        sandboxed_modules = []
        for m in manifest.modules:
            if m.type == "Egress":
                egress_targets.append({
                    "id": m.id,
                    "format": (m.config or {}).get("format"),
                    "path":   (m.config or {}).get("path"),
                    "mode":   (m.config or {}).get("mode"),
                })
                continue
            sandboxed_modules.append(m)

        # ── Apply LIMIT to every Ingress when sample_rows > 0 ───────────────────
        # We inject a Channel `op: sql` immediately downstream of each Ingress
        # via a temp module ID, but a simpler trick — and the one used here — is
        # to rewrite the Ingress config to carry a `sandbox_limit` marker. The
        # Spark ingress executor honours `sandbox_limit` (if present) by calling
        # `.limit(N)` after `.load()`. This is implemented in `ingress.py`.
        import dataclasses as _dc
        if sample_rows and sample_rows > 0:
            sandboxed_modules = [
                _dc.replace(m, config={**m.config, "sandbox_limit": sample_rows})
                if m.type == "Ingress"
                else m
                for m in sandboxed_modules
            ]

        # Drop edges whose endpoints disappear.
        keep_ids = {m.id for m in sandboxed_modules}
        sandboxed_edges = [
            e for e in manifest.edges
            if e.from_id in keep_ids and e.to_id in keep_ids
        ]

        sandboxed_manifest = _dc.replace(
            manifest,
            modules=tuple(sandboxed_modules),
            edges=tuple(sandboxed_edges),
        )

        # ── Spark session ──────────────────────────────────────────────────────
        if spark_session is None:
            try:
                from aqueduct.executor.spark.session import make_spark_session
                spark_session = make_spark_session(
                    f"aqueduct.sandbox.{patch_id}",
                    sandboxed_manifest.spark_config,
                    quiet=True,
                )
            except Exception as exc:
                return SandboxGateResult(
                    status="skip",
                    detail=f"sandbox could not start Spark: {exc}",
                    egress_targets=egress_targets,
                    duration_ms=int((time.monotonic() - t0) * 1000),
                )

        # ── Run the executor ───────────────────────────────────────────────────
        try:
            execute = get_executor(sandboxed_manifest.spark_config.get("deployment_engine", "spark"))
        except Exception:
            from aqueduct.executor import execute as execute  # noqa: F401
        try:
            # 1.1.0 fix — run the WHOLE patched DAG, not from `failed_module`
            # onwards. Skipping upstream Ingress/Channels left frame_store
            # empty for the failed module's inputs ("produced no DataFrame"
            # false-fail). sample_rows wrapping makes the full replay cheap
            # enough that the prior optimisation isn't worth the false
            # negatives.
            result = execute(  # type: ignore[operator]
                sandboxed_manifest,
                spark_session,
                run_id=f"sandbox-{patch_id}",
                store_dir=None,
                surveyor=None,
                observability_store=observability_store,
                lineage_store=lineage_store,
                explain_capture=explain_capture,
            )
        except ExecuteError as exc:
            return SandboxGateResult(
                status="fail",
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
                status="fail",
                detail=(
                    f"sandbox run ended with status={result.status!r}"
                    + (f"; first error in {failing.module_id!r}: {failing.error}" if failing else "")
                ),
                sample_rows=sample_rows if sample_rows > 0 else None,
                egress_targets=egress_targets,
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

        return SandboxGateResult(
            status="pass",
            detail=(
                f"sandbox replay succeeded against {sample_rows or '∞'} "
                f"row(s) per Ingress; {len(egress_targets)} Egress module(s) skipped"
            ),
            sample_rows=sample_rows if sample_rows > 0 else None,
            egress_targets=egress_targets,
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
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
