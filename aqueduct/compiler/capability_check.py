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

from aqueduct.compiler.type_surfaces import module_type_spellings, udf_return_type_spellings
from aqueduct.executor.capabilities import Capability, EngineCapabilities, Support, get_capabilities

RULE_ID_IGNORED = "engine_key_ignored"
RULE_ID_CROSS_ENGINE_HEAL = "cross_engine_heal"


@dataclass(frozen=True)
class CapabilityProblem:
    """One capability-gate finding for one module.

    ``spelling`` is populated only for ``type.*`` leaves (Phase 80 work
    package 2) — the exact raw type string (``"duckdb:HUGEINT"``,
    ``"array<map<string,int>>"``) that triggered the leaf, so the
    CompileError can name the offending spelling, not just the constructor
    leaf id. Empty for every other leaf category (module.type.*,
    channel.op.*, ... already name themselves precisely via the leaf id).
    """

    module_id: str
    leaf_id: str
    support: Support
    capability: Capability
    spelling: str = ""


def _type_leaves_for_hub_type(t: Any) -> list[str]:
    """Recursively map one parsed hub type (or ``NativeType``) to the
    ``type.*`` capability leaves it uses — one leaf per CONSTRUCTOR at every
    nesting level, so ``array<map<string,int>>`` yields
    ``["type.array", "type.map", "type.string", "type.int"]``.
    """
    from aqueduct.typehub import Array, Decimal, Map, NativeType, Struct, render

    if isinstance(t, NativeType):
        return [f"type.native.{t.engine}"]
    if isinstance(t, Array):
        return ["type.array", *_type_leaves_for_hub_type(t.element)]
    if isinstance(t, Map):
        return [
            "type.map",
            *_type_leaves_for_hub_type(t.key),
            *_type_leaves_for_hub_type(t.value),
        ]
    if isinstance(t, Struct):
        leaves = ["type.struct"]
        for f in t.fields:
            leaves.extend(_type_leaves_for_hub_type(f.type))
        return leaves
    if isinstance(t, Decimal):
        return ["type.decimal"]
    return [f"type.{render(t)}"]


def _type_leaves_for_spelling(spelling: str) -> list[str]:
    """Parse one raw type spelling and return the ``type.*`` leaves it uses.

    Re-parses rather than reusing a value threaded from compiler step 8h —
    see ``aqueduct/compiler/type_surfaces.py``'s docstring for why the
    surface EXTRACTION (where the spellings live) is shared while the parse
    call is not: 8h has already validated every spelling reaching this point
    parses cleanly (a bad spelling is a hard CompileError raised in 8h,
    before section 9's gate ever runs), so this call cannot legitimately
    fail — the try/except is defense in depth, not an expected path. Doing
    the parse here keeps the gate's only input `module.config` /
    `udf_registry` (the SAME data the Manifest already carries verbatim, see
    typehub.py's module docstring), with no new Manifest field, no
    serialization footprint growth, and no risk of a cached type-usage value
    silently going stale relative to the config it was derived from.
    """
    from aqueduct.typehub import AMBIGUOUS_TYPE_SPELLING_RULE_ID, TypeSpellingError, parse_type

    try:
        # `ambiguous_type_spelling` (bare `timestamp`) is a hard
        # TypeSpellingError now, not a warning — step 8h already rejects it
        # before this gate ever runs, so the suppress set below is inert
        # today. Kept (rather than dropped) because a spelling reaching this
        # point is guaranteed valid, and passing the rule id costs nothing.
        hub_type = parse_type(spelling, suppress={AMBIGUOUS_TYPE_SPELLING_RULE_ID})
    except TypeSpellingError:
        return []
    return _type_leaves_for_hub_type(hub_type)


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

    Most ``feature.*`` leaves are NOT per-module (a Python UDF is declared
    once in the manifest's ``udf_registry`` and referenced from SQL, not owned
    by one module), so they are derived at the manifest level by
    ``feature_leaves_for_manifest`` and checked in ``check_capabilities``.
    ``feature.table_addressing`` is the exception: catalog ``table:``
    addressing IS owned by one Ingress/Egress module (the ``table`` config
    key lives on that module, same as ``format``/``path``), so it is emitted
    here rather than at manifest scope — that gives the CompileError the
    actual module id for free, matching how ``ingress.format.*`` /
    ``egress.format.*`` are detected from the same module's ``cfg``.
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
        if cfg.get("table"):
            leaves.append("feature.table_addressing")
    elif mtype == "Ingress":
        fmt = cfg.get("format")
        if fmt in INGRESS_FORMATS:
            leaves.append(f"ingress.format.{fmt}")
        if cfg.get("table"):
            leaves.append("feature.table_addressing")
    elif mtype == "Junction":
        mode = cfg.get("mode")
        if mode:
            leaves.append(f"junction.mode.{mode}")
    elif mtype == "Funnel":
        mode = cfg.get("mode")
        if mode:
            leaves.append(f"funnel.mode.{mode}")

    # type.* — Phase 80 work package 2: every hub type constructor / native
    # escape hatch this module's cast columns / schema_hint fields actually
    # use. Surface extraction is shared with compiler step 8h (which already
    # validated every spelling here parses) via type_surfaces.py; see
    # _type_leaves_for_spelling's docstring for why the parse itself is not
    # also shared.
    for _where, spelling in module_type_spellings(module):
        leaves.extend(_type_leaves_for_spelling(spelling))

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


def type_leaves_for_manifest(manifest: Any) -> list[tuple[str, str, str]]:
    """Derive the ``type.*`` leaves a compiled Manifest's UDF ``return_type``
    entries use — manifest-scoped like ``feature_leaves_for_manifest`` (a UDF
    is declared once in ``udf_registry``, not owned by one module).

    Returns ``(leaf_id, source_label, spelling)`` triples: ``source_label``
    names the UDF (for the CompileError's module/source attribution) and
    ``spelling`` is the exact raw return_type string that produced the leaf
    (for the CompileError's "offending spelling" attribution — see
    ``CapabilityProblem.spelling``).
    """
    out: list[tuple[str, str, str]] = []
    for where, spelling in udf_return_type_spellings(getattr(manifest, "udf_registry", ())):
        for leaf in _type_leaves_for_spelling(spelling):
            out.append((leaf, where, spelling))
    return out


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
        # leaf -> the exact raw spelling that produced it (type.* leaves
        # only), so a problem on this module can name the offending
        # spelling, not just the constructor leaf id. First producer wins —
        # good enough for attribution when the same leaf is used twice.
        spelling_by_leaf: dict[str, str] = {}
        for _where, spelling in module_type_spellings(module):
            for leaf in _type_leaves_for_spelling(spelling):
                spelling_by_leaf.setdefault(leaf, spelling)
        for leaf_id in leaves_for_module(module):
            cap = caps.verdict(leaf_id)
            if cap.support in (Support.UNSUPPORTED, Support.IGNORED_WITH_WARNING):
                problems.append(
                    CapabilityProblem(
                        module_id=module.id,
                        leaf_id=leaf_id,
                        support=cap.support,
                        capability=cap,
                        spelling=spelling_by_leaf.get(leaf_id, ""),
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

    # Manifest-scoped type.* leaves (UDF return_type — not owned by one
    # module, same reasoning as feature_leaves_for_manifest above).
    for leaf_id, source_label, spelling in type_leaves_for_manifest(manifest):
        cap = caps.verdict(leaf_id)
        if cap.support in (Support.UNSUPPORTED, Support.IGNORED_WITH_WARNING):
            problems.append(
                CapabilityProblem(
                    module_id=source_label,
                    leaf_id=leaf_id,
                    support=cap.support,
                    capability=cap,
                    spelling=spelling,
                )
            )
    return problems


def format_unsupported_error(problem: CapabilityProblem, engine: str) -> str:
    hint = f" {problem.capability.hint}" if problem.capability.hint else ""
    spelling = f" (spelling: {problem.spelling!r})" if problem.spelling else ""
    return (
        f"Module {problem.module_id!r} uses {problem.leaf_id!r}{spelling}, which engine "
        f"{engine!r} does not support.{hint}"
    )


def format_ignored_warning(problem: CapabilityProblem, engine: str) -> str:
    hint = f" {problem.capability.hint}" if problem.capability.hint else ""
    spelling = f" (spelling: {problem.spelling!r})" if problem.spelling else ""
    return (
        f"Module {problem.module_id!r} uses {problem.leaf_id!r}{spelling}, which engine "
        f"{engine!r} ignores (accepted but has no effect).{hint}"
    )


# ── Cross-engine heal-patch provenance gate (Phase 79) ──────────────────────
#
# Separate from the leaf-verdict gate above: this checks the Blueprint's
# `healed_by:` provenance block (see `parser/schema.py::HealedByRecordSchema`,
# machine-written by `aqueduct patch apply` — `aqueduct/patch/apply.py`)
# against the engine THIS compile targets, not against per-leaf capability
# verdicts. A patch healed on engine X, classified `engine_shaped` (its
# operations could carry X's SQL dialect / cast syntax / format options —
# see `aqueduct/patch/provenance.py`), compiling for a different engine Y
# that hasn't green-run-validated it since, is a real trustworthiness gap:
# the healing feature would otherwise silently manufacture a production
# defect. `dialect_neutral`-only records (retry/timeout/structural patches)
# never trigger this — they carry no dialect content to be wrong about.

@dataclass(frozen=True)
class CrossEngineHealProblem:
    """One `healed_by` record whose engine-shaped provenance doesn't match
    (or hasn't been validated against) the compile's target engine."""

    patch_id: str
    origin_engine: str
    target_engine: str


def check_cross_engine_heal(blueprint: Any, engine: str) -> list[CrossEngineHealProblem]:
    """Return every `healed_by` record that is engine-shaped, from a
    different engine than *engine*, and not yet validated on *engine*.

    Pure and side-effect-free — does not raise, does not emit warnings; the
    caller (``compiler.compile()``) decides warn vs strict-escalate.
    """
    problems: list[CrossEngineHealProblem] = []
    for rec in getattr(blueprint, "healed_by", ()) or ():
        if getattr(rec, "classification", None) != "engine_shaped":
            continue
        origin = getattr(rec, "engine", None)
        if not origin or origin == engine:
            continue
        if engine in (getattr(rec, "validated_on", ()) or ()):
            continue
        problems.append(
            CrossEngineHealProblem(
                patch_id=getattr(rec, "patch_id", "") or "",
                origin_engine=origin,
                target_engine=engine,
            )
        )
    return problems


def format_cross_engine_heal_warning(problem: CrossEngineHealProblem) -> str:
    return (
        f"Patch {problem.patch_id!r} was healed on engine "
        f"{problem.origin_engine!r} with engine-shaped changes (SQL/dialect/"
        f"format content that may not be valid on another engine) but this "
        f"Blueprint is compiling for {problem.target_engine!r}, which has not "
        f"validated it with a green run since. Review the patch before "
        f"deploying to {problem.target_engine!r}, or run it there to clear "
        f"this warning."
    )
