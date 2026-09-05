"""Undo one applied heal patch's engine-config writes, or refuse loudly.

**The hole this closes.** A healed patch persists into the Blueprint via the
``healed_by:`` provenance block, and every later run inherits it. Perf
attribution (``aqueduct/patch/perf_attribution.py``) can now OBSERVE that a
healed config patch made runs slower; until this module there was no
supported way to act on that observation. Hand-editing the YAML works but
silently desynchronises the provenance record from the file it describes.

**What this can and cannot undo — and why the line is where it is.** The
only prior state Aqueduct actually RECORDS is the effective-engine-config
diff Gate 1 writes into the patch index as ``engine_config_delta``
(``{engine: {key: {before, after}}}``, keyed by ``patch_id`` — see
``aqueduct/patch/index.py``; the Blueprint's ``healed_by:`` record carries
only the bounded facts the compile-time gate needs). Nothing anywhere stores
the previous value of a module config key, an edge, or a SQL body:
``set_module_config_key``
carries only the new value, so a module patch has no inverse to compute. This
module therefore reverts engine-config writes and REFUSES everything else by
name, rather than reverting the half it can and leaving a Blueprint that
matches no state that ever existed. For the rest, ``aqueduct patch rollback
--to <patch_id>`` restores the whole file from git.

**Refusal is the main feature.** Every condition below raises ``RevertError``
naming what it cannot do:

  - the patch id is absent from ``healed_by:``, or matches more than one
    record (patch ids are model-authored and not unique by construction);
  - the record is already reverted;
  - the patch index carries no ``engine_config_delta`` for the patch —
    nothing prior was recorded, so there is nothing to restore;
  - the applied patch body cannot be read, or contains any operation other
    than ``set_engine_config`` — a mixed patch's non-config half cannot be
    undone, so undoing its config half would leave a hybrid;
  - a LATER, not-itself-reverted ``healed_by`` record wrote one of the same
    keys — reverting patch 2 of 3 under patch 3 produces a state no run ever
    saw. Reverting in reverse order works, because a reverted later record is
    skipped;
  - the key's CURRENT effective value is no longer what the patch wrote (a
    hand edit since), so the revert would silently overwrite a human's change;
  - the computed revert does not reproduce the recorded pre-patch effective
    config exactly, or moves any key the patch never wrote. This last one is
    a mechanical proof rather than an argument: the plan is applied to a copy
    and re-resolved through ``resolve_effective_engine_configs`` — the SAME
    function Gate 1 measured the delta with — before anything is written.

**A revert is not a patch.** The ``PatchSpec`` grammar is permanently closed
and gains no op here. Two independent reasons: the inverse of a write whose
recorded ``before`` is "absent" is a key DELETION, which no op expresses
(``set_engine_config`` can only set); and a PatchSpec carries heal semantics
a human-initiated undo must not inherit (it would stamp a fresh ``healed_by``
record, take a ``patch_index`` lifecycle slot, and need a model-authored
``patch_id``/``rationale``/``confidence`` that no model produced). What the
restore DOES reuse is the real op dispatch wherever it fits: a restore to a
concrete value goes through ``apply_set_engine_config`` via a synthesized
``SetEngineConfigOp``, so key addressing (Spark's ``conf`` bag vs DuckDB's
typed fields) is answered in exactly one place.

**The provenance record is annotated, never deleted.** Dropping it would
erase the fact that a heal ever happened — the thing ``healed_by:`` exists to
carry — and leaving it untouched would make it describe a Blueprint that no
longer contains its change. It gains ``reverted_at`` instead, and every
consumer of the block reads that: the cross-engine compile gate stops
flagging it, and the green-run stamps stop appending ``validated_on``
entries to it or perf notes to its index row.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from aqueduct.errors import AqueductError

__all__ = [
    "KeyRestore",
    "RevertError",
    "RevertPlan",
    "apply_revert",
    "find_healed_by_records",
    "plan_revert",
    "recorded_deltas",
]


SET_ENGINE_CONFIG = "set_engine_config"


class RevertError(AqueductError):
    """A revert was refused, naming what could not be undone.

    A direct ``AqueductError`` subclass, deliberately NOT a ``PatchError``:
    the two are different states with different fixes. ``PatchError`` means
    "this patch cannot be applied"; this means "this ALREADY-applied patch
    cannot be safely undone", whose remedy is usually a different command
    (``aqueduct patch rollback``) or a human edit. Callers branch by type.
    """


@dataclass(frozen=True)
class KeyRestore:
    """One engine-config key this revert puts back, and how.

    ``action`` is ``"set"`` (write ``value`` into the Blueprint's
    ``engine.<engine>`` block) or ``"remove"`` (delete the key from that
    block so the ``aqueduct.yml`` layer resolves again). Which one applies is
    DERIVED, not assumed: the key is dropped from the Blueprint layer and the
    effective config re-resolved — if that already reproduces the recorded
    ``before``, the value came from ``aqueduct.yml`` and removal is the
    correct undo; otherwise the Blueprint has to carry it explicitly.
    """

    engine: str
    key: str
    action: str
    current: Any
    target: Any
    value: Any = None


@dataclass(frozen=True)
class RevertPlan:
    """A verified revert, ready to write. Produced only by ``plan_revert``."""

    patch_id: str
    record_index: int
    restores: tuple[KeyRestore, ...]

    @property
    def engines(self) -> tuple[str, ...]:
        return tuple(sorted({r.engine for r in self.restores}))

    def to_dict(self) -> dict[str, Any]:
        return {
            "patch_id": self.patch_id,
            "engines": list(self.engines),
            "restores": [
                {
                    "engine": r.engine,
                    "key": r.key,
                    "action": r.action,
                    "from": r.current,
                    "to": r.target,
                }
                for r in self.restores
            ],
        }


def recorded_deltas(obs_store: Any | None, blueprint: Any) -> dict[str, dict[str, Any]]:
    """``{patch_id: engine_config_delta}`` for every patch this Blueprint names.

    The deltas moved out of the Blueprint into ``patch_index``, so a revert
    now needs the observability store the heal was applied against. A missing
    or unreadable store is a REFUSAL, not an empty mapping: an empty mapping
    would be indistinguishable from "this patch recorded nothing", and would
    turn a store outage into a confident, wrong "there is nothing to undo".
    """
    if obs_store is None:
        raise RevertError(
            "cannot revert: no observability store is reachable, and a patch's "
            "engine-config delta (the only prior state Aqueduct records) lives "
            "in the `patch_index` table there, not in the Blueprint. Point "
            "`stores.observability` at the store this patch was applied "
            "against, or restore the whole file with `aqueduct patch rollback`."
        )
    healed_by = blueprint.get("healed_by") if hasattr(blueprint, "get") else None
    ids = [
        str(rec.get("patch_id") or "")
        for rec in (healed_by or [])
        if hasattr(rec, "get") and rec.get("patch_id")
    ]
    try:
        from aqueduct.patch import index as _ix

        with obs_store.connect() as cur:
            _ix.ensure_schema(cur)
            return {pid: _ix.heal_provenance(cur, pid)["engine_config_delta"] for pid in ids}
    except Exception as exc:
        raise RevertError(
            f"cannot revert: the patch index could not be read ({exc}). It holds "
            "the engine-config delta this revert restores from; without it there "
            "is no recorded prior state to restore."
        ) from exc


def find_healed_by_records(blueprint: Any, patch_id: str) -> list[tuple[int, Any]]:
    """``[(index, record), ...]`` for every ``healed_by`` record with *patch_id*.

    Returns a list rather than one record because a patch id is authored by
    the model and is not unique by construction — the ambiguity is a state
    the caller must refuse, not one this function may resolve by picking.
    """
    healed_by = blueprint.get("healed_by") if hasattr(blueprint, "get") else None
    out: list[tuple[int, Any]] = []
    for idx, rec in enumerate(healed_by or []):
        if not hasattr(rec, "get"):
            continue
        if str(rec.get("patch_id") or "") == patch_id:
            out.append((idx, rec))
    return out


def _op_name(op: Any) -> str:
    return str((op.get("op") if isinstance(op, dict) else getattr(op, "op", "")) or "")


def _require_config_only(patch_id: str, operations: Any) -> None:
    """Refuse unless every operation in the applied patch is a config write.

    A mixed patch (a config key AND a module edit) is the dangerous case: its
    module half has no recorded prior state, so undoing only the config half
    leaves a Blueprint that is neither the pre-patch nor the post-patch one.
    ``engine_config_delta`` alone cannot distinguish that from a config-only
    patch, so the applied patch BODY is required as proof.
    """
    ops = list(operations or [])
    if not ops:
        raise RevertError(
            f"cannot revert patch {patch_id!r}: its applied patch body could not "
            "be read, so there is no proof the patch wrote nothing but engine "
            "config. Aqueduct records a prior value for engine-config keys only; "
            "a patch that also changed a module has no recorded prior state to "
            "restore. Restore the whole file instead: "
            f"aqueduct patch rollback <blueprint> --to {patch_id}"
        )
    other = sorted({_op_name(op) for op in ops if _op_name(op) != SET_ENGINE_CONFIG})
    if other:
        raise RevertError(
            f"cannot revert patch {patch_id!r}: it also carries the operation(s) "
            f"{other}, whose prior state Aqueduct never recorded (a "
            "set_module_config_key op stores the new value only, so it has no "
            "inverse). Reverting only its engine-config half would leave a "
            "Blueprint matching no state that ever ran. Restore the whole file "
            f"instead: aqueduct patch rollback <blueprint> --to {patch_id}"
        )


def _require_no_later_writer(
    patch_id: str,
    healed_by: Any,
    index: int,
    delta: dict[str, Any],
    deltas: dict[str, dict[str, Any]],
) -> None:
    """Refuse when a later, non-reverted record wrote one of the same keys.

    ``deltas`` maps ``patch_id`` to that patch's recorded engine-config delta,
    read from the patch index by the caller (``plan_revert``) — the Blueprint
    record itself no longer carries one.
    """
    conflicts: list[str] = []
    for later_index in range(index + 1, len(healed_by or [])):
        later = healed_by[later_index]
        if not hasattr(later, "get"):
            continue
        if later.get("reverted_at"):
            # Already undone, so its write is not standing on top of ours —
            # this is what makes reverse-order reverting work.
            continue
        later_delta = deltas.get(str(later.get("patch_id") or "")) or {}
        for engine, keys in delta.items():
            later_keys = later_delta.get(engine) or {}
            for key in keys:
                if key in later_keys:
                    conflicts.append(
                        f"engine {engine!r} key {key!r} was written again by "
                        f"patch {str(later.get('patch_id') or '?')!r}"
                    )
    if conflicts:
        raise RevertError(
            f"cannot revert patch {patch_id!r}: a later patch overwrote the same "
            f"engine-config key(s) — {'; '.join(conflicts)}. Undoing this one "
            "underneath them would produce a configuration no run has ever used. "
            "Revert the later patch(es) first (a reverted record is skipped "
            "here), or restore the whole file with aqueduct patch rollback."
        )


def plan_revert(
    *,
    cfg: Any,
    blueprint: Any,
    patch_id: str,
    operations: Any,
    deltas: dict[str, dict[str, Any]],
) -> RevertPlan:
    """Build a VERIFIED plan to undo *patch_id*'s engine-config writes.

    ``blueprint`` is the raw (ruamel) Blueprint dict; ``operations`` are the
    applied patch body's operations, needed as proof the patch was
    config-only (see ``_require_config_only``); ``deltas`` maps ``patch_id``
    to the engine-config delta the patch index recorded for it, for THIS
    patch and for every later record the no-later-writer check has to
    consider. The caller reads it from the index because a Blueprint's
    ``healed_by`` record no longer carries the delta (see
    ``aqueduct/patch/index.py::heal_provenance``).

    Raises:
        RevertError: for every condition under which the revert would not
            reproduce a state the Blueprint actually had. This function
            writes nothing — a returned plan is the only path to a write.
    """
    from aqueduct.executor.session_config import resolve_effective_engine_configs
    from aqueduct.patch.config_delta import (
        ABSENT,
        blueprint_engine_layers,
        canonical_config_value,
    )

    healed_by = blueprint.get("healed_by") if hasattr(blueprint, "get") else None
    matches = find_healed_by_records(blueprint, patch_id)
    if not matches:
        known = [str(r.get("patch_id") or "?") for r in (healed_by or []) if hasattr(r, "get")]
        raise RevertError(
            f"no healed_by record for patch {patch_id!r} in this Blueprint. "
            + (
                f"Recorded patches: {known}."
                if known
                else "This Blueprint carries no healed_by: block at all — nothing "
                "was ever applied to it by a heal."
            )
        )
    if len(matches) > 1:
        raise RevertError(
            f"cannot revert patch {patch_id!r}: {len(matches)} healed_by records "
            f"share that id (indices {[i for i, _ in matches]}). A patch id is "
            "written by the model and is not unique; Aqueduct will not guess "
            "which application to undo."
        )

    index, record = matches[0]
    if record.get("reverted_at"):
        raise RevertError(
            f"patch {patch_id!r} was already reverted at {record.get('reverted_at')!r} "
            "— its engine-config writes are no longer in this Blueprint."
        )

    delta = {engine: dict(keys) for engine, keys in (deltas.get(patch_id) or {}).items()}
    if not delta:
        raise RevertError(
            f"cannot revert patch {patch_id!r}: the patch index carries no "
            "engine_config_delta for it, so no prior state was ever recorded. "
            "Only engine-config writes record a before-value; module, edge and "
            "SQL changes do not. Restore the whole file instead: "
            f"aqueduct patch rollback <blueprint> --to {patch_id}"
        )

    _require_config_only(patch_id, operations)
    _require_no_later_writer(patch_id, healed_by, index, delta, deltas)

    layers_now = blueprint_engine_layers(blueprint)
    effective_now = resolve_effective_engine_configs(cfg, layers_now)

    restores: list[KeyRestore] = []
    for engine in sorted(delta):
        for key in sorted(delta[engine]):
            recorded = delta[engine][key] or {}
            recorded_after = recorded.get("after")
            recorded_before = recorded.get("before")
            current = (effective_now.get(engine) or {}).get(key, ABSENT)
            if canonical_config_value(current) != canonical_config_value(recorded_after):
                raise RevertError(
                    f"cannot revert patch {patch_id!r}: engine {engine!r} key "
                    f"{key!r} now resolves to {current!r}, but the patch recorded "
                    f"it as {recorded_after!r} when it was applied. Something "
                    "changed it since — reverting would overwrite that change "
                    "with a value nobody chose. Edit the Blueprint by hand, or "
                    "restore the whole file with aqueduct patch rollback."
                )

            # Derive removal-vs-set: does dropping the key from the BLUEPRINT
            # layer already reproduce the recorded prior value (i.e. it came
            # from aqueduct.yml)?
            probe_layers = {e: dict(v) for e, v in layers_now.items()}
            probe_layers.setdefault(engine, {}).pop(key, None)
            probe_value = (
                resolve_effective_engine_configs(cfg, probe_layers).get(engine) or {}
            ).get(key, ABSENT)
            if canonical_config_value(probe_value) == canonical_config_value(recorded_before):
                restores.append(
                    KeyRestore(
                        engine=engine,
                        key=key,
                        action="remove",
                        current=current if current is not ABSENT else None,
                        target=recorded_before,
                    )
                )
            elif recorded_before is None:
                raise RevertError(
                    f"cannot revert patch {patch_id!r}: engine {engine!r} key "
                    f"{key!r} recorded no prior value, but removing it from the "
                    f"Blueprint leaves {probe_value!r} rather than nothing — the "
                    "aqueduct.yml layer has gained this key since the patch was "
                    "applied, so the pre-patch configuration is no longer "
                    "reachable from this Blueprint."
                )
            else:
                restores.append(
                    KeyRestore(
                        engine=engine,
                        key=key,
                        action="set",
                        current=current if current is not ABSENT else None,
                        target=recorded_before,
                        value=recorded_before,
                    )
                )

    plan = RevertPlan(patch_id=patch_id, record_index=index, restores=tuple(restores))
    _verify(cfg=cfg, blueprint=blueprint, plan=plan, effective_now=effective_now)
    return plan


def _verify(*, cfg: Any, blueprint: Any, plan: RevertPlan, effective_now: dict) -> None:
    """Prove the plan reproduces the recorded prior config, or refuse.

    Applies the plan to a throwaway copy and re-resolves every engine's
    effective config through the SAME function Gate 1 measured the delta
    with. Two properties are checked: each reverted key lands exactly on its
    recorded ``before``, and NO other key moves. The second is what stops a
    restore whose side effects nobody predicted (a removal that unmasks a
    different ``aqueduct.yml`` value elsewhere) from being written.
    """
    from aqueduct.executor.session_config import resolve_effective_engine_configs
    from aqueduct.patch.config_delta import (
        ABSENT,
        blueprint_engine_layers,
        canonical_config_value,
    )

    try:
        probe = _rewrite_engine_config(blueprint, plan)
    except Exception as exc:
        raise RevertError(
            f"cannot revert patch {plan.patch_id!r}: the restore could not be "
            f"applied to the Blueprint ({exc})."
        ) from exc

    effective_after = resolve_effective_engine_configs(cfg, blueprint_engine_layers(probe))
    expected = {(r.engine, r.key): r.target for r in plan.restores}

    for engine in sorted(set(effective_now) | set(effective_after)):
        before = effective_now.get(engine) or {}
        after = effective_after.get(engine) or {}
        for key in sorted(set(before) | set(after)):
            got = after.get(key, ABSENT)
            if (engine, key) in expected:
                want = expected[(engine, key)]
                if canonical_config_value(got) != canonical_config_value(want):
                    raise RevertError(
                        f"refusing to revert patch {plan.patch_id!r}: undoing "
                        f"engine {engine!r} key {key!r} would leave it at "
                        f"{got!r}, not the recorded pre-patch value {want!r}. "
                        "The resulting Blueprint would match no state this "
                        "pipeline ever ran with."
                    )
                continue
            was = before.get(key, ABSENT)
            if canonical_config_value(got) != canonical_config_value(was):
                raise RevertError(
                    f"refusing to revert patch {plan.patch_id!r}: it would also "
                    f"change engine {engine!r} key {key!r} ({was!r} -> {got!r}), "
                    "which the patch never wrote. A revert that moves a key "
                    "outside the patch it is undoing is not a revert."
                )


def _remove_engine_config_key(blueprint: Any, engine: str, key: str) -> None:
    """Delete one key from the Blueprint's ``engine.<engine>`` block.

    Addressed by the SAME structural question
    ``operations.apply_set_engine_config`` asks (does this engine's block
    schema declare a ``conf`` bag?), never a hardcoded engine name. This is
    the half of a revert the closed patch grammar cannot express — no op
    removes a key — which is why a revert is a Blueprint rewrite and not a
    PatchSpec.

    An emptied ``conf:``/``engine.<name>:`` mapping is deliberately left in
    place rather than pruned: whether the author wrote the block before the
    patch is not recorded anywhere, so removing it would be a guess.
    """
    from aqueduct.parser.schema import EngineBlockSchema

    engine_fields = EngineBlockSchema.model_fields
    block = (blueprint.get("engine") or {}).get(engine)
    if not isinstance(block, dict):
        return
    block_cls = engine_fields[engine].annotation if engine in engine_fields else None
    has_conf_bag = "conf" in getattr(block_cls, "model_fields", {})
    target = block.get("conf") if has_conf_bag else block
    if isinstance(target, dict):
        target.pop(key, None)


def _rewrite_engine_config(blueprint: Any, plan: RevertPlan) -> Any:
    """Apply *plan*'s key restores to a copy of *blueprint*. No stamping."""
    from aqueduct.patch.apply import _ruamel_copy
    from aqueduct.patch.grammar import SetEngineConfigOp
    from aqueduct.patch.operations import apply_operation

    working = _ruamel_copy(blueprint)
    for restore in plan.restores:
        if restore.action == "set":
            # Through the REAL op dispatch: key addressing (conf bag vs typed
            # field) and value quoting are then answered in one place only.
            working = apply_operation(
                working,
                SetEngineConfigOp(
                    op=SET_ENGINE_CONFIG,
                    engine=restore.engine,
                    key=restore.key,
                    value=restore.value,
                ),
            )
        else:
            _remove_engine_config_key(working, restore.engine, restore.key)
    return working


def apply_revert(blueprint: Any, plan: RevertPlan, *, reverted_at: str) -> Any:
    """Return a copy of *blueprint* with *plan* applied and the record stamped.

    The ``healed_by`` record is kept and annotated with ``reverted_at``, never
    deleted — see this module's docstring for why both alternatives are
    wrong. Callers write the returned document; this function does no I/O.
    """
    working = _rewrite_engine_config(blueprint, plan)
    healed_by = working.get("healed_by") or []
    if plan.record_index < len(healed_by):
        healed_by[plan.record_index]["reverted_at"] = reverted_at
    return working
