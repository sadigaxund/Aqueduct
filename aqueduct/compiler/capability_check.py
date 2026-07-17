"""Compile-time capability gate (Phase 78).

Pure function: given a compiled ``Manifest`` and an engine name, walk the
modules actually USED by the blueprint, map each to the capability leaves it
touches, and look up the target engine's verdict for each. An ``UNSUPPORTED``
leaf is a hard ``CompileError`` (naming the module, the leaf, the engine, and
the capability's ``hint``). An ``IGNORED_WITH_WARNING`` leaf is a suppressible
warning under rule_id ``engine_key_ignored``.

Version-constrained ``SUPPORTED`` capabilities (``Capability.requires``) do
NOT fail compile — compile-time has no way to know which dependency versions
are actually installed at run time. That check belongs to
``aqueduct/doctor/`` (Phase 78 item 6), which inspects the real environment.

Since Spark declares default-ALLOW for (almost) the entire grammar today
(``aqueduct/executor/spark/capabilities.py``), this gate is a no-op for every
existing blueprint — see ``tests/test_capabilities/test_gate_noop.py``.

An engine with no registered capability declaration fails closed:
``get_capabilities()`` raises ``UnknownEngineError`` (a ``CompileError``
subclass) rather than degrading to an empty problem list — see
``aqueduct/executor/capabilities.py``. A plugin whose ``aqueduct.engines``
entry point fails to import raises ``EnginePluginError`` from the same place, and
an engine whose ``capabilities.yml`` is incomplete/invalid raises
``CapabilityDeclarationError`` (a different state with a different fix — see
``aqueduct/errors.py``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aqueduct.executor.capabilities import Capability, EngineCapabilities, Support, get_capabilities

RULE_ID_IGNORED = "engine_key_ignored"


@dataclass(frozen=True)
class CapabilityProblem:
    """One capability-gate finding for one module."""

    module_id: str
    leaf_id: str
    support: Support
    capability: Capability


def leaves_for_module(module: Any) -> list[str]:
    """Map one compiled Module to the capability leaves it actually touches.

    Two kinds of leaf are checked:

      - ``module.type.<Type>`` — WHICH module kind this is. An engine may not
        implement a whole module type (DuckDB Stage A does not run ``Assert``
        or ``Probe``), and that verdict must fire at compile time. Emitted for
        every module. (The ``module.field.*`` / ``<block>.field.*`` leaves are
        NOT checked — they describe grammar SHAPE, already enforced by pydantic
        at parse time; every parseable Blueprint necessarily uses only fields
        that exist.)
      - config-derived dispatch leaves — op names, write modes, fan modes, and
        the curated formats: the parts of the grammar an engine can plausibly
        refuse per-configuration.

    ``feature.*`` leaves are NOT per-module (a Python UDF is declared once in
    the manifest's ``udf_registry`` and referenced from SQL, not owned by one
    module), so they are derived at the manifest level by
    ``feature_leaves_for_manifest`` and checked in ``check_capabilities``.
    """
    # Lazy import: capability_leaves.py pulls in aqueduct.executor.spark.egress
    # (for its op/mode/format constants), which imports aqueduct.models, which
    # re-exports aqueduct.compiler.models — and aqueduct/compiler/__init__.py
    # imports this module's own package (compiler.compiler -> capability_check).
    # A module-level import here would be a circular import at package-load
    # time; deferring it to call time (well after all modules have finished
    # loading) breaks the cycle without restructuring the layer boundary.
    from aqueduct.executor.capability_leaves import EGRESS_FORMATS, INGRESS_FORMATS

    leaves: list[str] = []
    mtype = str(getattr(module, "type", ""))
    cfg = module.config if isinstance(module.config, dict) else {}

    # module.type.<Type> — every module declares its kind. `module.type.*` is a
    # real leaf for every ModuleType (parse-time pydantic guarantees mtype is a
    # valid one), so `caps.verdict()` always finds an explicit verdict; on Spark
    # (all supported) this is a no-op, on an engine that does not run a whole
    # module type it becomes a clean CompileError instead of a runtime crash.
    if mtype:
        leaves.append(f"module.type.{mtype}")

    if mtype == "Channel":
        op = cfg.get("op")
        if op:
            leaves.append(f"channel.op.{op}")
    elif mtype == "Egress":
        mode = cfg.get("mode")
        if mode:
            leaves.append(f"egress.mode.{mode}")
        on_new = cfg.get("on_new_columns")
        if on_new:
            leaves.append(f"egress.on_new_columns.{on_new}")
        fmt = cfg.get("format")
        # Only the curated formats with a dedicated engine code path carry a
        # capability leaf (see capability_leaves.py module docstring) —
        # Ingress/Egress otherwise accept ANY Spark-supported format string
        # verbatim, so a plain pass-through format (parquet, json, orc, …)
        # has no verdict to check and must not be gated.
        if fmt in EGRESS_FORMATS:
            leaves.append(f"egress.format.{fmt}")
    elif mtype == "Ingress":
        fmt = cfg.get("format")
        if fmt in INGRESS_FORMATS:
            leaves.append(f"ingress.format.{fmt}")
    elif mtype == "Junction":
        mode = cfg.get("mode")
        if mode:
            leaves.append(f"junction.mode.{mode}")
    elif mtype == "Funnel":
        mode = cfg.get("mode")
        if mode:
            leaves.append(f"funnel.mode.{mode}")

    return leaves


# UDF `lang` value -> the engine feature leaf that language exercises. Python
# UDFs and JVM (java/scala) UDFs are separate engine capabilities. Driven off
# the real ``udf_registry`` `lang` field, never a hardcoded blueprint list.
_UDF_LANG_FEATURE: dict[str, str] = {
    "python": "feature.python_udf",
    "java": "feature.java_udf",
    "scala": "feature.java_udf",  # JVM UDF — same engine capability as java
}


def feature_leaves_for_manifest(manifest: Any) -> list[tuple[str, str]]:
    """Derive the ``feature.*`` leaves a compiled Manifest actually EXERCISES.

    Returns ``(leaf_id, source_label)`` pairs — ``source_label`` names what in
    the Blueprint pulls the feature in (a UDF id, ...) so the compile error can
    point at it. Unlike ``leaves_for_module`` these are manifest-scoped, not
    owned by one module: a ``lang: python`` UDF is declared once in
    ``udf_registry`` and referenced from SQL across any number of Channels, so
    the capability it needs (``feature.python_udf``) is a property of the
    manifest, not of a single module.

    Driven entirely off real manifest fields (today: ``udf_registry`` `lang`),
    never a hardcoded list — a Blueprint that declares no UDF exercises no UDF
    feature and produces nothing here. Only the ``feature.*`` leaves with an
    unambiguous manifest signal are derived; a leaf with no manifest evidence
    (e.g. ``feature.parallel_mode`` is a runtime ``--parallel`` flag, not a
    Blueprint fact) is deliberately NOT emitted, so the gate never fabricates
    usage a Blueprint did not actually declare.
    """
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for udf in getattr(manifest, "udf_registry", ()) or ():
        if not isinstance(udf, dict):
            continue
        lang = str(udf.get("lang", "")).lower()
        leaf = _UDF_LANG_FEATURE.get(lang)
        if leaf is None:
            continue
        source = str(udf.get("id") or udf.get("label") or f"udf<{lang}>")
        key = (leaf, source)
        if key not in seen:
            seen.add(key)
            pairs.append(key)
    return pairs


def check_capabilities(manifest: Any, engine: str = "spark") -> list[CapabilityProblem]:
    """Return every capability problem (UNSUPPORTED or IGNORED_WITH_WARNING).

    Raises:
        UnknownEngineError: ``engine`` has no registered capability declaration
            (see ``aqueduct.executor.capabilities.get_capabilities``). A
            ``CompileError`` subclass, so ``compile()``'s existing error
            contract is unchanged. An unknown/misspelled engine is a hard
            compile-time failure, not a silently-empty result — the whole point
            of the capability gate is to fail closed, and a future
            default-UNSUPPORTED engine (e.g. DuckDB) must not be waved through
            just because its declaration failed to load.
        EnginePluginError: an ``aqueduct.engines`` entry point failed to import
            (the plugin is broken or half-installed).
        CapabilityDeclarationError: a registered engine's capability declaration
            is incomplete or invalid (a dev-time build failure — run
            ``aqueduct dev capabilities sync`` and declare a verdict).
    """
    caps: EngineCapabilities = get_capabilities(engine)

    problems: list[CapabilityProblem] = []
    for module in getattr(manifest, "modules", ()):
        if not getattr(module, "enabled", True):
            continue  # disabled modules never run — nothing to gate
        for leaf_id in leaves_for_module(module):
            cap = caps.verdict(leaf_id)
            if cap.support in (Support.UNSUPPORTED, Support.IGNORED_WITH_WARNING):
                problems.append(
                    CapabilityProblem(
                        module_id=module.id,
                        leaf_id=leaf_id,
                        support=cap.support,
                        capability=cap,
                    )
                )

    # Manifest-scoped feature leaves (UDF languages, ...). ``source_label``
    # (a UDF id) stands in for ``module_id`` in the problem — the feature is
    # not owned by one module. Same verdict handling as the per-module leaves.
    for leaf_id, source_label in feature_leaves_for_manifest(manifest):
        cap = caps.verdict(leaf_id)
        if cap.support in (Support.UNSUPPORTED, Support.IGNORED_WITH_WARNING):
            problems.append(
                CapabilityProblem(
                    module_id=source_label,
                    leaf_id=leaf_id,
                    support=cap.support,
                    capability=cap,
                )
            )
    return problems


def format_unsupported_error(problem: CapabilityProblem, engine: str) -> str:
    hint = f" {problem.capability.hint}" if problem.capability.hint else ""
    return (
        f"Module {problem.module_id!r} uses {problem.leaf_id!r}, which engine "
        f"{engine!r} does not support.{hint}"
    )


def format_ignored_warning(problem: CapabilityProblem, engine: str) -> str:
    hint = f" {problem.capability.hint}" if problem.capability.hint else ""
    return (
        f"Module {problem.module_id!r} uses {problem.leaf_id!r}, which engine "
        f"{engine!r} ignores (accepted but has no effect).{hint}"
    )
