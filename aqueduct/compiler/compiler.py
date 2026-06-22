"""Main Compiler orchestrator.

Pipeline:
  Blueprint (AST)
    → [1] Tier 1 resolution (@aq.*)
    → [2] Arcade expansion (flat module list)
    → [3] Probe / Spillway validation
    → [4] Passive Regulator compile-away
    → Manifest (JSON-ready)

The Compiler does NOT:
  - Initialize a SparkSession.
  - Write to disk (the CLI/Executor does that).
  - Make LLM calls.
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

from aqueduct.compiler.expander import ExpandError, expand_arcades
from aqueduct.compiler.macros import MacroError, resolve_macros_in_config
from aqueduct.compiler.models import Manifest
from aqueduct.compiler.provenance import (
    ModuleProvenance,
    ProvenanceMap,
    ValueProvenance,
    build_config_provenance,
)
from aqueduct.compiler.runtime import AqFunctions, resolve_tier1
from aqueduct.compiler.wirer import (
    WireError,
    compile_away_regulators,
    validate_probes,
    validate_spillway_edges,
)
from aqueduct.parser.models import Blueprint, Edge, Module, ModuleType
from aqueduct.parser.resolver import _CTX_RE, _sub_ctx  # Tier 0 re-pass after Tier 1
from aqueduct.executor.path_keys import CLOUD_SCHEMES, PATHLESS_INGRESS_FORMATS
from aqueduct.errors import AqueductError


class CompileError(AqueductError):
    """Raised for any compilation failure."""


def _resolve_module_tier1(m: Module, registry: AqFunctions) -> Module:
    """Return a copy of Module with all @aq.* tokens resolved in its config."""
    resolved_config: Any = resolve_tier1(m.config, registry)
    if resolved_config is m.config:
        return m
    return dataclasses.replace(m, config=resolved_config)


def _resolve_udf_params_tier1(
    udf_registry: tuple[dict[str, Any], ...], registry: AqFunctions
) -> tuple[dict[str, Any], ...]:
    """Resolve @aq.* tokens (incl. @aq.secret()) inside parameterized-UDF params.

    Only the ``params`` sub-dict is run through Tier 1 — the rest of the entry
    (id, module, entry, return_type) is structural and never carries tokens.
    """
    out: list[dict[str, Any]] = []
    for entry in udf_registry:
        params = entry.get("params")
        if params:
            entry = {**entry, "params": resolve_tier1(params, registry)}
        out.append(entry)
    return tuple(out)


def compile(  # noqa: A001
    blueprint: Blueprint,
    blueprint_path: Path | None = None,
    run_id: str | None = None,
    depot: Any = None,
    depots: "dict[str, Any] | None" = None,
    execution_date: Any = None,
    secrets_provider: str = "env",
    secrets_region: str | None = None,
    secrets_resolver: str | None = None,
    deployment_env: str | None = None,
    deployment_target: str | None = None,
    warnings_suppress: set[str] | None = None,
    warnings_silence_all: bool = False,
) -> Manifest:
    """Compile a parsed Blueprint into a fully-resolved Manifest.

    Args:
        blueprint:      Parsed, validated Blueprint AST from the Parser.
        blueprint_path: Path to the Blueprint file on disk. Required for Arcade
                        expansion (sub-Blueprint paths are relative to this file).
        run_id:         Optional run UUID. Auto-generated if not provided.
        depot:          Optional Depot connection for @aq.depot.get() resolution.

    Returns:
        A frozen Manifest ready for the Executor.

    Raises:
        CompileError: On any Tier 1 resolution, expansion, or wiring failure.
    """
    registry = AqFunctions(
        run_id=run_id,
        depot=depot,
        depots=depots,
        execution_date=execution_date,
        secrets_provider=secrets_provider,
        secrets_region=secrets_region,
        secrets_resolver=secrets_resolver,
        blueprint_id=blueprint.id,
        blueprint_name=blueprint.name,
        blueprint_path=blueprint_path,
        deployment_env=deployment_env,
        deployment_target=deployment_target,
    )

    # ── 1. Resolve Tier 1 in context values ───────────────────────────────────
    try:
        resolved_ctx: dict[str, str] = {
            k: resolve_tier1(v, registry)
            for k, v in blueprint.context.values.items()
        }
    except (ValueError, RuntimeError) as exc:
        raise CompileError(f"Tier 1 context resolution failed: {exc}") from exc

    # ── 2. Re-run ${ctx.*} substitution with the now-fully-resolved context ───
    # Needed for context values that referenced other Tier 1 context entries.
    # e.g. context.path = "data/${ctx.today}/out" where ctx.today was @aq.date.today()
    try:
        for key in list(resolved_ctx):
            if _CTX_RE.search(resolved_ctx[key]):
                resolved_ctx[key] = _sub_ctx(resolved_ctx[key], resolved_ctx)
    except ValueError as exc:
        raise CompileError(f"Post-Tier-1 context re-resolution failed: {exc}") from exc

    # ── 3. Resolve Tier 1 in module configs ───────────────────────────────────
    try:
        modules: list[Module] = [
            _resolve_module_tier1(m, registry) for m in blueprint.modules
        ]
    except (ValueError, RuntimeError) as exc:
        raise CompileError(f"Tier 1 module config resolution failed: {exc}") from exc

    # ── 3.5. Resolve SQL macros in module configs ─────────────────────────────
    if blueprint.macros:
        try:
            modules = [
                dataclasses.replace(m, config=resolve_macros_in_config(m.config, blueprint.macros))
                for m in modules
            ]
        except MacroError as exc:
            raise CompileError(f"SQL macro resolution failed: {exc}") from exc

    # ── 3.7. Build provenance for top-level non-Arcade modules ───────────────
    # We re-load the raw YAML to get the original expressions (${ctx.*}, etc.)
    # for provenance. The blueprint object only has resolved values.
    raw_module_configs: dict[str, dict] = {}
    raw_context: dict[str, Any] = {}
    if blueprint_path and blueprint_path.exists():
        try:
            raw_yaml = yaml.safe_load(blueprint_path.read_text(encoding="utf-8"))
            if isinstance(raw_yaml, dict):
                raw_context = raw_yaml.get("context") or {}
                for m_raw in raw_yaml.get("modules") or []:
                    if isinstance(m_raw, dict) and "id" in m_raw:
                        raw_module_configs[m_raw["id"]] = m_raw.get("config") or {}
        except Exception:
            logger.warning("Failed to load raw YAML for provenance from %s", blueprint_path)

    context_provenance: dict[str, ValueProvenance] = build_config_provenance(raw_context, resolved_ctx)
    
    module_provenance: dict[str, ModuleProvenance] = {}
    for m in modules:
        if m.type == ModuleType.Arcade:
            continue  # arcade provenance built during expansion
        raw_cfg = raw_module_configs.get(m.id, {})
        config_prov = build_config_provenance(raw_cfg, m.config or {})
        module_provenance[m.id] = ModuleProvenance(
            module_id=m.id,
            module_type=m.type,
            config=config_prov,
        )

    # ── 3.8. Linear-edge sugar ────────────────────────────────────────────────
    # When the Blueprint omits `edges:` entirely, chain the modules in
    # declaration order. Only applies to pipelines built solely from
    # single-input/single-output module types — fan-out (Junction), fan-in
    # (Funnel), sub-pipeline (Arcade), tap (Probe), and gate (Regulator) types
    # need explicit wiring (their ports are ambiguous in a flat chain), so a
    # Blueprint that omits edges while using them is a hard error rather than a
    # silent miswire. Injected edges carry `injected=True` for provenance.
    _LINEAR_CHAIN_TYPES: frozenset[str] = frozenset({
        ModuleType.Ingress, ModuleType.Channel, ModuleType.Egress, ModuleType.Assert,
    })
    edges = list(blueprint.edges)
    if not edges and len(modules) > 1:
        _nonlinear = [m for m in modules if m.type not in _LINEAR_CHAIN_TYPES]
        if _nonlinear:
            _bad = ", ".join(f"{m.id!r} ({m.type})" for m in _nonlinear)
            raise CompileError(
                "Blueprint omits `edges:` but contains module type(s) that cannot "
                f"be auto-chained in declaration order: {_bad}. Linear-edge sugar "
                "only applies when every module is single-input/single-output "
                "(Ingress, Channel, Egress, Assert). Declare `edges:` explicitly to "
                "wire Junction / Funnel / Arcade / Probe / Regulator modules."
            )
        edges = [
            Edge(from_id=a.id, to_id=b.id, port="main", injected=True)
            for a, b in zip(modules, modules[1:])
        ]
        logger.info(
            "linear-edge sugar: no edges declared — injected %d edge(s) chaining "
            "%s in declaration order",
            len(edges),
            " → ".join(m.id for m in modules),
        )

    # ── 4. Expand Arcades ─────────────────────────────────────────────────────
    arcade_prov: dict = {}
    if any(m.type == ModuleType.Arcade for m in modules):
        if blueprint_path is None:
            raise CompileError(
                "Blueprint contains Arcade modules but blueprint_path was not provided. "
                "Pass the Blueprint file path to compile() so Arcade refs can be resolved."
            )
        try:
            modules, edges, arcade_prov = expand_arcades(modules, edges, blueprint_path.parent)
        except ExpandError as exc:
            raise CompileError(f"Arcade expansion failed: {exc}") from exc
    module_provenance.update(arcade_prov)

    # ── 5. Validate Probes and Spillways ──────────────────────────────────────
    try:
        validate_probes(modules)
        validate_spillway_edges(modules, edges)
    except WireError as exc:
        raise CompileError(f"Wiring validation failed: {exc}") from exc

    # ── 5.5. Validate Assert rule / on_fail combinations ─────────────────────
    _AGG_NO_QUARANTINE = {"min_rows", "max_rows", "sql", "null_rate"}
    _assert_spillway_ids = {
        e.from_id for e in edges if e.port == "spillway"
    }
    for m in modules:
        if m.type != ModuleType.Assert:
            continue
        for rule in m.config.get("rules", []):
            rtype = rule.get("type", "")
            on_fail = rule.get("on_fail", "abort")
            action = on_fail if isinstance(on_fail, str) else on_fail.get("action", "abort")
            if action == "quarantine":
                if rtype in _AGG_NO_QUARANTINE:
                    raise CompileError(
                        f"Assert '{m.id}' rule type={rtype!r} uses on_fail=quarantine, "
                        "but quarantine requires a per-row predicate. "
                        f"{rtype!r} is an aggregate rule with no derivable row filter. "
                        "Use on_fail=abort or on_fail=warn instead. "
                        "Row-level quarantine is supported by: sql_row, custom, freshness."
                    )
                if rtype in ("freshness", "sql_row", "custom") and m.id not in _assert_spillway_ids:
                    raise CompileError(
                        f"Assert '{m.id}' rule type={rtype!r} uses on_fail=quarantine "
                        "but no spillway edge is connected to this Assert module. "
                        "Quarantine rows would be silently discarded. "
                        "Add an edge with port: spillway from this Assert, or change on_fail to abort/warn."
                    )

    # ── 6. Compile away passive Regulators ────────────────────────────────────
    modules, edges = compile_away_regulators(modules, edges)

    # ── 6.5. Build inputs fingerprint ─────────────────────────────────────────
    inputs_fingerprint: dict[str, dict[str, Any]] = {}
    for m in modules:
        if m.type != ModuleType.Ingress:
            continue
        fmt = m.config.get("format", "")
        path = m.config.get("path", "")
        table = m.config.get("table", "")
        if table:
            # Table-addressed Ingress — record the catalog identifier; no local
            # file metadata to stat.
            inputs_fingerprint[m.id] = {"table": table, "size_bytes": None, "last_modified": None}
            continue
        if not path or fmt in PATHLESS_INGRESS_FORMATS or any(path.startswith(s) for s in CLOUD_SCHEMES):
            inputs_fingerprint[m.id] = {"path": path, "size_bytes": None, "last_modified": None}
            continue
        try:
            st = Path(path).stat()
            inputs_fingerprint[m.id] = {
                "path": path,
                "size_bytes": st.st_size,
                "last_modified": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            }
        except (OSError, ValueError):
            # OSError: file missing / permission denied.
            # ValueError: malformed path (embedded null byte, oversized component,
            # URI-shaped string not caught by _REMOTE_SCHEMES). In either case the
            # fingerprint is "not collected" rather than a hard failure.
            inputs_fingerprint[m.id] = {"path": path, "size_bytes": None, "last_modified": None}

    # ── 7. Delivery semantics warning ─────────────────────────────────────────
    from aqueduct.warnings import emit as _aq_emit
    _supp = warnings_suppress
    if warnings_silence_all:
        _supp = {"*"}  # universal suppress sentinel — emit() short-circuits on "*"

    def _w(rule_id: str, msg: str) -> None:
        if warnings_silence_all:
            return
        _aq_emit(rule_id, msg, suppress=_supp)

    if blueprint.retry_policy.max_attempts > 1:
        for m in modules:
            if m.type == ModuleType.Egress and m.config.get("mode") == "append":
                _w(
                    "delivery_append_retry_dupes",
                    f"Egress '{m.id}' uses mode=append with "
                    f"max_attempts={blueprint.retry_policy.max_attempts} — "
                    "retries may produce duplicate rows. "
                    "Use mode=overwrite for idempotent writes, or set max_attempts=1.",
                )

    # ── 8. Performance diagnostics ────────────────────────────────────────────

    # 8a. Probe sample() signals — full dataset scan despite the name
    _SAMPLE_SCAN_SIGNALS = {"null_rates", "row_count_estimate", "value_distribution", "distinct_count"}
    for m in modules:
        if m.type != ModuleType.Probe:
            continue
        for sig in m.config.get("signals", []):
            sig_type = sig.get("type", "")
            if sig_type not in _SAMPLE_SCAN_SIGNALS:
                continue
            # row_count_estimate with spark_listener method is zero-action
            if sig_type == "row_count_estimate" and sig.get("method") == "spark_listener":
                continue
            _w(
                "perf_probe_sample_full_scan",
                f"Probe '{m.id}' signal '{sig_type}' uses df.sample() — "
                "this is a FULL DATASET SCAN. sample() is a row-level filter, not a "
                "partition prune: all data is read before rows are discarded. "
                "Use method: spark_listener for zero-cost row counts. "
                "See docs/spark_guide.md#probe-sample-cost.",
            )

    # 8b. Incremental channel without cache — MAX() watermark triggers extra scan
    for m in modules:
        if m.type != ModuleType.Channel or m.config.get("materialize") != "incremental":
            continue
        # Check if any upstream module in the edge list is a cache/checkpoint
        upstream_ids = {e.from_id for e in edges if e.to_id == m.id}
        upstream_checkpointed = any(
            um.checkpoint for um in modules if um.id in upstream_ids
        )
        if not upstream_checkpointed:
            _w(
                "perf_incremental_watermark_scan",
                f"Channel '{m.id}' uses materialize=incremental. "
                "After each run, Aqueduct computes MAX(watermark_column) on the output — "
                "a second full scan if the DataFrame is not cached. "
                "Add a Checkpoint upstream or accept the extra Spark action. "
                "See docs/spark_guide.md#incremental-watermark-scan.",
            )

    # 8c. Python UDF registered — row-at-a-time execution warning
    for udf_entry in blueprint.udf_registry:
        if udf_entry.get("lang", "python") == "python":
            _w(
                "perf_python_udf_row_at_a_time",
                f"UDF '{udf_entry.get('id', '?')}' uses lang=python — "
                "Python UDFs execute row-at-a-time and bypass Arrow/vectorized execution. "
                "For high-volume channels, prefer native Spark SQL expressions or "
                "pandas_udf (Arrow-optimized). Spillway routing itself is SQL-native "
                "and unaffected, but the UDF body will not be vectorized. "
                "See docs/spark_guide.md#python-udf-performance.",
            )

    # 8d. Delta append without partition hint — small-file accumulation risk
    for m in modules:
        if m.type != ModuleType.Egress:
            continue
        if m.config.get("format") in ("delta", "parquet") and m.config.get("mode") == "append":
            has_partition = bool(m.config.get("partition_by") or m.config.get("repartition"))
            if not has_partition:
                _w(
                    "perf_delta_append_no_partition",
                    f"Egress '{m.id}' uses format={m.config.get('format')!r} with mode=append "
                    "but has no partition_by or repartition hint. "
                    "Incremental appends without partitioning accumulate small files over time, "
                    "degrading read performance. Add partition_by or schedule external OPTIMIZE. "
                    "See docs/spark_guide.md#planned-future-checks.",
                )

    # 8e. Multi-consumer Channel without cache — DAG re-evaluated per consumer
    consumer_counts: dict[str, int] = {}
    for e in edges:
        consumer_counts[e.from_id] = consumer_counts.get(e.from_id, 0) + 1

    checkpointed_ids = {m.id for m in modules if m.checkpoint}
    for m in modules:
        if m.type != ModuleType.Channel:
            continue
        if consumer_counts.get(m.id, 0) > 1 and m.id not in checkpointed_ids:
            _w(
                "perf_multi_consumer_no_cache",
                f"Channel '{m.id}' has {consumer_counts[m.id]} downstream consumers "
                "but no Checkpoint upstream. Spark will re-evaluate the full DAG for each "
                "consumer branch — consider adding a Checkpoint or cache() boundary to "
                "avoid redundant computation. "
                "See docs/spark_guide.md#caching-strategy.",
            )

    # 8f. Hadoop filesystem keys in Ingress options — must be in spark_config instead
    _HADOOP_FS_PREFIXES = ("fs.s3a.", "fs.gs.", "fs.azure.", "fs.hdfs.", "fs.abfs.")
    for m in modules:
        if m.type != ModuleType.Ingress:
            continue
        bad_keys = [
            k for k in m.config.get("options", {})
            if any(str(k).startswith(p) for p in _HADOOP_FS_PREFIXES)
        ]
        if bad_keys:
            _w(
                "perf_hadoop_fs_in_options",
                f"Ingress '{m.id}' has Hadoop filesystem keys in 'options': "
                f"{bad_keys}. DataFrameReader.option() does NOT propagate these to "
                "Spark's HadoopConfiguration — the S3A/GCS/Azure FileSystem will not "
                "see them and authentication will fail. Move these to spark_config with "
                "the 'spark.hadoop.' prefix instead: e.g. "
                "'spark.hadoop.fs.s3a.access.key'. "
                "See docs/spark_guide.md#jdbc-ingress-parallelism.",
            )

    # 8g. maintenance.optimize on non-delta Egress — OPTIMIZE is Delta-only
    for m in modules:
        if m.type != ModuleType.Egress:
            continue
        maint = m.config.get("maintenance", {})
        if maint and maint.get("optimize") and m.config.get("format", "").lower() != "delta":
            _w(
                "maintenance_optimize_non_delta",
                f"Egress '{m.id}' has maintenance.optimize=true but format="
                f"{m.config.get('format')!r}. OPTIMIZE is a Delta Lake operation and "
                "will fail at runtime on non-Delta tables. Set format: delta or remove "
                "the maintenance block.",
            )

    prov_map = ProvenanceMap(
        blueprint_id=blueprint.id,
        blueprint_path=str(blueprint_path.resolve()) if blueprint_path else "",
        modules=module_provenance,
        context=context_provenance,
    )

    manifest = Manifest(
        blueprint_id=blueprint.id,
        name=blueprint.name,
        description=blueprint.description,
        aqueduct_version=blueprint.aqueduct_version,
        context=resolved_ctx,
        modules=tuple(modules),
        edges=tuple(edges),
        spark_config=dict(blueprint.spark_config),
        retry_policy=blueprint.retry_policy,
        agent=blueprint.agent,
        udf_registry=_resolve_udf_params_tier1(blueprint.udf_registry, registry),
        macros=dict(blueprint.macros),
        checkpoint=blueprint.checkpoint,
        provenance_map=prov_map,
        inputs_fingerprint=inputs_fingerprint,
    )

    # ── Phase 30a tier 1 — extended Spark warnings (modular registry) ─────────
    if not warnings_silence_all:
        try:
            from aqueduct.compiler.warnings import run_all as _run_compile_warnings
            from aqueduct.warnings import emit as _emit
            for _rid, _msg in _run_compile_warnings(manifest, suppress=warnings_suppress):
                _emit(_rid, _msg, suppress=warnings_suppress)
        except Exception:
            pass  # warnings must never block compilation

    return manifest
