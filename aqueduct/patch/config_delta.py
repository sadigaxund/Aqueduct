"""Effective-engine-config delta for a patch — Gate 1's efficacy check.

**The hole this closes.** ``set_engine_config`` writes a key into the
Blueprint's ``engine.<name>:`` block, but the config an engine actually runs
with is a MERGE (``aqueduct.executor.session_config.
resolve_session_engine_config``): that engine's ``aqueduct.yml``-level
config, with the Blueprint's own ``engine.<name>`` entry layered on top.
So a write can apply perfectly cleanly — schema-valid, allowlist-clean,
lineage/sandbox gates green — and change **nothing** the engine will see,
because the value it writes is already what resolves. Before this module
that was invisible: the patch was recorded as applied, every gate reported
success, and the heal attempt was spent proving nothing. That is the silent
no-op AGENTS.md forbids.

**Two answers, both derived from data, neither from an op-name list.**
AGENTS.md's "classify by what you EXCLUDE, not by what you INCLUDE" rule
bans gating this on a hand-written list of ops that "are config ops" — such
a list silently stops covering the 15th op the day someone adds one. Two
mechanically-derived facts replace it:

  *write targets* — re-apply the patch's operations to a copy of the
      Blueprint whose ``engine:`` block has been REMOVED, then read the
      ``engine:`` block back out (``engine_config_write_targets``). Whatever
      appears there is exactly what this patch writes into engine config,
      obtained through the REAL op dispatch
      (``aqueduct.patch.operations.apply_operation``).
      A pipeline-only patch writes nothing there and the gate reports
      ``not_applicable``; a future op that writes engine config is covered
      the day it is added, with no edit here.

  *effective delta* — the before/after diff of
      ``resolve_effective_engine_configs``, i.e. what the ENGINE would see,
      not what the YAML says.

Stripping the block first is what makes the write-target set independent of
whether the value happened to change: a patch re-writing the value the
Blueprint already carries produces an identical ``bp_after``, so a plain
before/after diff of the ``engine:`` block would classify the most obvious
no-op of all as "this patch has no config surface". No other op reads the
``engine:`` block (``aqueduct.patch.operations._DISPATCH`` — only
``apply_set_engine_config`` touches it, and it auto-creates what it needs),
so removing it cannot change how any other op behaves.

**Non-empty write targets + empty effective delta = refuse.** Not warn:
the whole value of a config op is the config change, so an op that provably
changes no config has nothing left to be right about, and the fix is a
different patch — which is what makes it a ``PatchError`` (the same
reasoning that makes an allowlist violation a ``PatchError`` while a
malformed shipped allowlist is an ``EngineConfigAllowlistError``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from aqueduct.executor.session_config import resolve_effective_engine_configs
from aqueduct.parser.schema import EngineBlockSchema

__all__ = [
    "ABSENT",
    "EngineConfigDeltaResult",
    "blueprint_engine_layers",
    "canonical_config_value",
    "engine_config_write_targets",
    "run_engine_config_delta_gate",
]


class _Absent:
    """Sentinel for "this key is not in the resolved config at all".

    Distinct from ``None``, which is a real resolved value for an unset
    typed engine field (``engine.duckdb.threads`` dumps as ``None``) — the
    falsy-trap rule in AGENTS.md applies directly here.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debug aid only
        return "<absent>"


ABSENT = _Absent()


@dataclass(frozen=True)
class EngineConfigDeltaResult:
    """Outcome of the effective-engine-config gate for one patch.

    ``status`` vocabulary follows ``aqueduct.patch.preview.LineageGateResult``
    deliberately:

      ``pass``            the patch writes engine config AND the effective
                          config changes — the delta is in ``delta``.
      ``not_applicable``  the patch writes no engine config at all. NOT
                          ``pass``: reporting a clean verdict for a check
                          that had nothing to look at is the same lie
                          ``LineageGateResult.not_applicable`` was added to
                          stop. Free for pipeline-only patches.

    There is no ``fail`` member. The only failing state — writes engine
    config, changes nothing — is raised as ``PatchError`` by
    ``run_engine_config_delta_gate`` rather than returned, because every
    apply path must refuse it, not render it.
    """

    status: str
    detail: str
    delta: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    write_targets: dict[str, tuple[str, ...]] = field(default_factory=dict)


def blueprint_engine_layers(bp: Any) -> dict[str, dict[str, Any]]:
    """Raw Blueprint dict -> ``{engine: that engine's override dict}``.

    The same shape ``Manifest.engine_config`` carries, and derived by the
    same structural question ``aqueduct.parser.parser._resolve_engine_block_raw``
    asks: does this engine's own block schema declare a ``conf`` sub-field?

      - Yes (today: Spark) -> the override IS the ``conf`` bag.
      - No (today: DuckDB) -> the override is the keys the author actually
        wrote on the block. Reading a raw YAML dict gives that for free —
        it is the dict equivalent of the parser's ``exclude_unset=True``,
        which exists so a field the Blueprint never mentioned cannot
        clobber a real ``aqueduct.yml`` value with ``None``.

    Engines come from ``EngineBlockSchema``'s own fields, so a third engine
    is read correctly the moment its block exists — never a hardcoded
    ``if engine == "spark"``.
    """
    engine_block = bp.get("engine") if hasattr(bp, "get") else None
    if not isinstance(engine_block, dict):
        engine_block = {}
    layers: dict[str, dict[str, Any]] = {}
    for engine_name, field_info in EngineBlockSchema.model_fields.items():
        raw = engine_block.get(engine_name)
        raw = raw if isinstance(raw, dict) else {}
        block_cls = field_info.annotation
        if "conf" in getattr(block_cls, "model_fields", {}):
            conf = raw.get("conf")
            layers[engine_name] = dict(conf) if isinstance(conf, dict) else {}
        else:
            layers[engine_name] = {k: v for k, v in raw.items()}
    return layers


def _copy_without_engine_block(bp: Any) -> Any | None:
    """Ruamel-safe copy of *bp* with its ``engine:`` block removed."""
    # Local import: `apply.py` imports THIS module (Gate 1 calls the gate
    # below), so a module-level import back into it would be circular.
    from aqueduct.patch.apply import _ruamel_copy

    copy = _ruamel_copy(bp)
    try:
        copy.pop("engine", None)
    except AttributeError:  # pragma: no cover - a non-mapping Blueprint
        return None
    return copy


def _apply_ops_tolerant(bp: Any, patch_spec: Any) -> Any:
    """Apply *patch_spec*'s operations to *bp*, skipping ops that cannot apply.

    Deliberately per-op and deliberately silent, for one reason that holds
    for EVERY exception reachable here: this module reads ENGINE CONFIG out
    of the result, it does not validate the patch. An op that cannot apply
    writes nothing — including nothing into ``engine:`` — so skipping it can
    never hide an engine-config write, and the real apply is all-or-nothing
    and refuses the same op with its own better-worded error moments later.

    Raising instead would make Gate 1 report apply failures, which it is
    documented NOT to do: an arcade-expanded module ID is deliberately left
    for the apply step to explain (``_check_guardrails``' own docstring), and
    ``aqueduct heal`` runs this gate against a Blueprint stub carrying only
    ``agent.guardrails``, where no module op can apply at all — turning that
    into a rejection would break every module-touching heal.
    """
    from aqueduct.patch.operations import apply_operation

    working = bp
    for op in getattr(patch_spec, "operations", None) or []:
        try:
            working = apply_operation(working, op)
        except Exception:
            continue
    return working


def engine_config_write_targets(
    blueprint_before: Any, patch_spec: Any
) -> dict[str, tuple[str, ...]]:
    """``{engine: (key, ...)}`` — the engine-config keys this patch WRITES.

    Independent of whether the written value differs from what is already
    there: the patch is re-applied to a copy of the Blueprint with its
    ``engine:`` block removed, so every write shows up as a fresh key. See
    this module's docstring for why that is the exclusion-safe substitute
    for a list of "config ops".

    Empty tuples are dropped, so ``not this_function(...)`` reads as
    "this patch touches no engine config".
    """
    probe_src = _copy_without_engine_block(blueprint_before)
    if probe_src is None:  # pragma: no cover - a non-mapping Blueprint
        return {}
    written = blueprint_engine_layers(_apply_ops_tolerant(probe_src, patch_spec))
    return {engine: tuple(keys) for engine, keys in written.items() if keys}


def canonical_config_value(value: Any) -> Any:
    """Compare engine-config values the way an engine session sees them.

    Every engine-config value ends up as a string on the session (Spark's
    ``.config(k, v)``, DuckDB's ``SET k = v``), so ``400`` and ``"400"`` are
    the SAME effective setting and a patch that only changes the YAML
    spelling changes nothing. Comparing raw Python values would call that a
    real delta and let the no-op through, which is the failure this gate
    exists to catch. ``ABSENT`` stays itself so "key not present" never
    collapses into the string form of any value.

    Public because ``aqueduct/patch/revert.py`` has to answer the SAME
    question in the opposite direction ("is the key still at the value this
    patch wrote?"), and two copies of this rule would drift.
    """
    if value is ABSENT:
        return ABSENT
    return str(value)


def _diff_effective(
    before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]]
) -> dict[str, dict[str, dict[str, Any]]]:
    """``{engine: {key: {"before": ..., "after": ...}}}`` for changed keys."""
    delta: dict[str, dict[str, dict[str, Any]]] = {}
    for engine in sorted(set(before) | set(after)):
        b = before.get(engine) or {}
        a = after.get(engine) or {}
        per_key: dict[str, dict[str, Any]] = {}
        for key in sorted(set(b) | set(a)):
            bv = b.get(key, ABSENT)
            av = a.get(key, ABSENT)
            if canonical_config_value(bv) == canonical_config_value(av):
                continue
            per_key[key] = {
                "before": None if bv is ABSENT else bv,
                "after": None if av is ABSENT else av,
            }
        if per_key:
            delta[engine] = per_key
    return delta


def run_engine_config_delta_gate(
    *,
    cfg: Any,
    blueprint_before: Any,
    patch_spec: Any,
    blueprint_after: Any = None,
) -> EngineConfigDeltaResult:
    """Resolve the effective session config before and after *patch_spec*.

    ``blueprint_after`` is optional purely as a caching hint — when the
    caller has already applied the patch in memory, passing it avoids a
    second apply. Omitted, it is computed here; the gate never depends on a
    caller remembering to supply it.

    Raises:
        PatchError: the patch writes engine config but the effective
            config is identical before and after — the write cannot change
            what any engine does, so applying it would burn a heal attempt
            while reporting success. A patch problem: the fix is a
            different patch (a different key, a different value, or an op
            that is not a config write at all).
    """
    from aqueduct.patch.apply import PatchError, _ruamel_copy

    write_targets = engine_config_write_targets(blueprint_before, patch_spec)
    if not write_targets:
        return EngineConfigDeltaResult(
            status="not_applicable",
            detail="patch writes no engine/session config",
        )

    if blueprint_after is None:
        # Same tolerance, same reason as the write-set probe: a module op
        # that cannot apply here cannot change any engine's config either,
        # and refusing the patch for it is the apply step's job, not this
        # gate's.
        blueprint_after = _apply_ops_tolerant(_ruamel_copy(blueprint_before), patch_spec)

    before_effective = resolve_effective_engine_configs(
        cfg, blueprint_engine_layers(blueprint_before)
    )
    after_effective = resolve_effective_engine_configs(
        cfg, blueprint_engine_layers(blueprint_after)
    )
    delta = _diff_effective(before_effective, after_effective)

    if not delta:
        written = "; ".join(
            f"{engine}: {', '.join(keys)}" for engine, keys in sorted(write_targets.items())
        )
        already = []
        for engine, keys in sorted(write_targets.items()):
            for key in keys:
                current = (before_effective.get(engine) or {}).get(key, ABSENT)
                already.append(
                    f"engine {engine!r} key {key!r} already resolves to {current!r}"
                )
        raise PatchError(
            "set_engine_config write has no effect: the patch writes "
            f"{written}, but the effective session config is identical before "
            "and after it is applied — the engine's behaviour would not change "
            f"({'; '.join(already)}). The effective config is a merge of "
            "aqueduct.yml's engine.<name> block and the Blueprint's own "
            "engine.<name> block (Blueprint wins), so writing the value that "
            "already resolves changes nothing. Refusing rather than recording "
            "an applied patch that does nothing — write a different value or a "
            "different key."
        )

    changed = sum(len(keys) for keys in delta.values())
    return EngineConfigDeltaResult(
        status="pass",
        detail=f"{changed} effective engine-config key(s) change",
        delta=delta,
        write_targets=write_targets,
    )
