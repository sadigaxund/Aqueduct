"""PatchSpec op classification — dialect_neutral vs engine_shaped (Phase 79).

The self-heal loop's LLM prompts are engine-flavored BY DESIGN (each engine
registers its own `PromptRules` pack — see `aqueduct/executor/protocol.py`
and AGENTS.md's "healing system prompt is COMPOSED" rule). That means a patch
generated while healing a DuckDB run can carry DuckDB-dialect SQL, cast
syntax, or format options; if that same Blueprint is later compiled for
Spark, the patch's content may simply be wrong there.

This module classifies each `aqueduct.patch.grammar` op ONCE, in core, so
every consumer (the compile-time cross-engine gate in
`aqueduct/compiler/capability_check.py`, the `healed_by` provenance block
written by `aqueduct/patch/apply.py`) uses the same answer:

  dialect_neutral — the op cannot introduce engine-specific SQL/dialect
                     content (retry/timeout knobs, structural rewiring,
                     resource numerics, schema hints, ...). Safe to carry
                     across engines unmodified.
  engine_shaped   — the op CAN introduce SQL text, cast syntax, format/
                     session config, or other dialect-bearing content. A
                     patch containing at least one engine_shaped op is only
                     trustworthy on the engine it was healed against (or one
                     it has since been green-run validated on).

Classification is per OP TYPE, with one field-sensitive refinement
(`set_module_config_key` — classified by which config key it touches, since
that op can write anything from a retry count to a raw SQL string).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aqueduct.patch.grammar import VALID_PATCH_OPS

DIALECT_NEUTRAL = "dialect_neutral"
ENGINE_SHAPED = "engine_shaped"

# engine name -> installed distribution name, for detect_engine_version().
# Read via importlib.metadata (package METADATA only) rather than importing
# the engine package itself — this module must stay pyspark-free per
# AGENTS.md's top-level-purity rule (pyspark only imports inside
# aqueduct/executor/spark/ and the documented lazy-import exceptions).
_ENGINE_DIST_NAME: dict[str, str] = {
    "duckdb": "duckdb",
    "spark": "pyspark",
}


def detect_engine_version(engine: str) -> str | None:
    """Best-effort, nullable — the installed engine package's version string.

    Rides along in patch `_aq_meta` and the `healed_by:` provenance block
    purely as a diagnostic; nothing gates on it. Never raises — an unknown
    engine, or one whose distribution isn't installed in THIS process (e.g. a
    remote-submitted Spark job whose driver ran the heal), yields None rather
    than building real version plumbing for a nice-to-have field.
    """
    dist = _ENGINE_DIST_NAME.get(engine)
    if not dist:
        return None
    try:
        from importlib.metadata import PackageNotFoundError, version

        return version(dist)
    except PackageNotFoundError:
        return None
    except Exception:
        return None


@dataclass(frozen=True)
class HealProvenance:
    """One applied patch's provenance, split by where it is written.

    ``record`` is the bounded ``healed_by:`` entry that goes into the
    Blueprint YAML: only what the compile-time cross-engine gate and
    ``aqueduct patch revert`` must be able to read out of a travelling
    artifact. ``index_facts`` is everything else — the apply-time facts that
    grow with every green run and belong in the ``patch_index`` table
    (``aqueduct/patch/index.py::record_heal_provenance``), keyed by the same
    ``patch_id``.

    Both halves are built here, once, so the two apply paths (``aqueduct
    patch apply`` in ``patch/apply.py`` and the ``agent.approval: auto``
    direct-write in ``cli/__init__.py``) cannot drift on the record shape.
    """

    record: dict[str, Any]
    index_facts: dict[str, Any]


def build_heal_provenance(
    *,
    patch_id: str,
    operations: Any,
    meta: dict[str, Any] | None,
    applied_at: str,
    fallback_run_id: str | None = None,
    engine_config_delta: dict[str, dict[str, Any]] | None = None,
    perf_baseline: dict[str, Any] | None = None,
) -> HealProvenance | None:
    """Build one applied patch's provenance, or None when there is none.

    ``meta`` is a patch's ``_aq_meta`` dict (or an equivalent built directly
    from a live ``FailureContext`` — see ``aqueduct/cli/__init__.py::
    _write_patch_to_blueprint``). A patch with no ``engine`` in its meta (a
    hand-authored patch applied outside the heal loop) is NOT stamped —
    provenance documents heals, not every blueprint edit.

    ``engine_config_delta`` is the EFFECTIVE session-config diff Gate 1
    computed for this patch (``aqueduct/patch/config_delta.py``): what the
    target engine will actually see change, after
    ``resolve_session_engine_config`` merges ``aqueduct.yml``'s
    ``engine.<name>`` block with the Blueprint's own. It is an APPLY-time
    fact, not a generation-time one — the same patch applied against a
    different ``aqueduct.yml`` has a different effective delta, and the model
    that wrote the patch never saw either — which is why it is recorded here
    rather than in ``_aq_meta``, and in the patch index rather than in the
    Blueprint: a full before/after dict per engine is store data, not
    artifact data.

    ``perf_baseline`` is the last GREEN run of this blueprint BEFORE the
    patch (``aqueduct/patch/perf_attribution.py::capture_baseline_perf``),
    captured because ``validated_on`` is binary while config-op success is
    not: a patch that leaves the pipeline three times slower still records as
    validated. Absent when no green run preceded the patch, which is what
    later makes the observation ``not_applicable`` rather than a comparison
    against nothing.
    """
    meta = meta or {}
    engine = meta.get("engine")
    if not engine:
        return None
    record = {
        "patch_id": patch_id,
        "engine": engine,
        "classification": classify_ops(operations),
        "applied_at": applied_at,
        "validated_on": [],
    }
    index_facts = {
        "engine": engine,
        "engine_version": meta.get("engine_version"),
        "run_id": meta.get("run_id") or fallback_run_id,
        "engine_config_delta": engine_config_delta or {},
        "perf_baseline": perf_baseline or {},
    }
    return HealProvenance(record=record, index_facts=index_facts)


# ── Field-sensitive refinement for set_module_config_key ───────────────────
# Config keys (dot-notation, matched by exact name or a leading prefix) that
# carry SQL text, format/dialect options, or cast syntax — anything else set
# via this op (paths, resource numerics, retry knobs, boolean flags) is
# dialect_neutral. Prefix entries end with "." (e.g. "options." covers
# Ingress/Egress `options.*` format-driver knobs, some of which are
# dialect-specific — e.g. Delta merge SQL predicates).
_DIALECT_BEARING_KEYS: frozenset[str] = frozenset(
    {
        "query",
        "sql",
        "format",
        "mode",
        "cast_type",
        "on_new_columns",
    }
)
_DIALECT_BEARING_PREFIXES: tuple[str, ...] = ("options.",)


def _set_module_config_key_classification(key: str | None) -> str:
    if not key:
        # No key visible (shouldn't happen — schema requires it) — treat
        # conservatively as engine_shaped rather than silently trusting it.
        return ENGINE_SHAPED
    last_segment = key.rsplit(".", 1)[-1]
    if key in _DIALECT_BEARING_KEYS or last_segment in _DIALECT_BEARING_KEYS:
        return ENGINE_SHAPED
    if any(key.startswith(p) for p in _DIALECT_BEARING_PREFIXES):
        return ENGINE_SHAPED
    return DIALECT_NEUTRAL


# ── Per-op classification table ─────────────────────────────────────────────
# One row per aqueduct.patch.grammar.VALID_PATCH_OPS entry — the closure test
# (tests/test_patch/test_provenance.py) fails the build if a 15th op is added
# to the grammar without a row here.
#
# op                        classification     rationale
# ------------------------  -----------------  --------------------------------
# replace_module_config     engine_shaped       full config replacement — the
#                                                new block routinely carries SQL
#                                                text (Channel op=sql), format/
#                                                options, or cast syntax; treated
#                                                conservatively as shaped.
# set_module_config_key     field-sensitive     see _set_module_config_key_classification
# replace_module_label      dialect_neutral     cosmetic label only, no config.
# insert_module              engine_shaped       a brand-new module's config dict
#                                                can embed dialect-specific SQL/
#                                                format the same way
#                                                replace_module_config can.
# remove_module              dialect_neutral     structural removal + rewiring;
#                                                introduces no new SQL/dialect
#                                                content.
# replace_context_value      dialect_neutral     context values are data
#                                                (paths/params substituted into
#                                                templates), not dialect syntax
#                                                by themselves.
# add_probe                  dialect_neutral     Probe signal types
#                                                (schema_snapshot, row_count_
#                                                estimate, ...) are declarative
#                                                and engine-agnostic; a custom
#                                                probe's `module:` points at
#                                                plain Python, not SQL.
# replace_edge                dialect_neutral     graph rewiring only.
# set_module_on_failure       dialect_neutral     retry/quarantine policy —
#                                                numerics and enum values, no
#                                                dialect content.
# replace_retry_policy        dialect_neutral     retry/timeout numerics.
# add_arcade_ref               dialect_neutral     a path reference + params to a
#                                                sub-Blueprint; the referenced
#                                                file's own content is out of
#                                                scope for THIS patch's
#                                                classification.
# defer_to_human               dialect_neutral     makes zero Blueprint changes.
# set_engine_config             engine_shaped       explicitly an engine/session
#                                                config key (a conf-bag entry
#                                                like spark.sql.shuffle.
#                                                partitions, or a typed field
#                                                like duckdb.memory_limit) —
#                                                by construction not portable
#                                                to a different engine.
# replace_macro                engine_shaped       replaces a raw SQL macro body
#                                                verbatim — always dialect-
#                                                bearing.
# declare_dependency           dialect_neutral     a PEP 508-lite requirement
#                                                string (package name +
#                                                version specifier) — no SQL,
#                                                cast syntax, or session
#                                                config; portable across
#                                                engines unmodified.
_STATIC_OP_CLASSIFICATION: dict[str, str] = {
    "replace_module_config": ENGINE_SHAPED,
    "replace_module_label": DIALECT_NEUTRAL,
    "insert_module": ENGINE_SHAPED,
    "remove_module": DIALECT_NEUTRAL,
    "replace_context_value": DIALECT_NEUTRAL,
    "add_probe": DIALECT_NEUTRAL,
    "replace_edge": DIALECT_NEUTRAL,
    "set_module_on_failure": DIALECT_NEUTRAL,
    "replace_retry_policy": DIALECT_NEUTRAL,
    "add_arcade_ref": DIALECT_NEUTRAL,
    "defer_to_human": DIALECT_NEUTRAL,
    "set_engine_config": ENGINE_SHAPED,
    "replace_macro": ENGINE_SHAPED,
    "declare_dependency": DIALECT_NEUTRAL,
}

# set_module_config_key is field-sensitive (classified by which config key it
# touches — see _set_module_config_key_classification) rather than a static
# verdict; it is handled as a special case in classify_op() and is therefore
# EXCLUDED from _STATIC_OP_CLASSIFICATION. Both sets together must equal the
# grammar's full op set — this is what the closure check below (and the test
# suite, against the live grammar) enforces.
_FIELD_SENSITIVE_OPS: frozenset[str] = frozenset({"set_module_config_key"})

# Sanity: every VALID_PATCH_OPS entry must be covered by either the static
# table or the field-sensitive set (closure guard — also re-checked by the
# test suite against the live grammar so a 15th op fails the build, not just
# this module's own internal consistency).
_covered = set(_STATIC_OP_CLASSIFICATION) | _FIELD_SENSITIVE_OPS
_missing = set(VALID_PATCH_OPS) - _covered
if _missing:
    raise RuntimeError(
        f"aqueduct.patch.provenance: unclassified PatchSpec op(s) {sorted(_missing)} — "
        "add a row to _STATIC_OP_CLASSIFICATION."
    )


def classify_op(op: Any) -> str:
    """Classify one PatchOperation instance (or dict with an `op` key + fields).

    Returns ``ENGINE_SHAPED`` or ``DIALECT_NEUTRAL``.
    """
    op_name = getattr(op, "op", None) if not isinstance(op, dict) else op.get("op")
    if op_name in _FIELD_SENSITIVE_OPS:
        key = getattr(op, "key", None) if not isinstance(op, dict) else op.get("key")
        return _set_module_config_key_classification(key)
    verdict = _STATIC_OP_CLASSIFICATION.get(op_name)
    if verdict is None:
        # Unknown op name (shouldn't happen — PatchSpec validation rejects
        # anything outside the grammar) — classify conservatively.
        return ENGINE_SHAPED
    return verdict


def classify_ops(ops: Any) -> str:
    """Classify a whole patch — the MAX over its operations.

    A single engine_shaped op makes the whole patch engine_shaped; an empty
    or all-dialect_neutral op list classifies as dialect_neutral.
    """
    for op in ops:
        if classify_op(op) == ENGINE_SHAPED:
            return ENGINE_SHAPED
    return DIALECT_NEUTRAL


__all__ = [
    "DIALECT_NEUTRAL",
    "ENGINE_SHAPED",
    "HealProvenance",
    "build_heal_provenance",
    "classify_op",
    "classify_ops",
    "detect_engine_version",
]
