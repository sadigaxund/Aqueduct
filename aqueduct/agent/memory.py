"""Heal memory — signature-keyed lookups over the ``patch_index`` table.

Phase 45 introduced signature memory; Phase 53 moved its backing store from an
``os.scandir`` over the local ``patches/`` directory to the ``patch_index``
relational table (see ``aqueduct/patch/index.py``). The three zero-token paths
that run BEFORE the LLM are unchanged in behaviour; only the source is now
backend-blind SQL, so they work identically whether patch bodies live on local
disk, s3, gcs, or adls:

* ``find_pending(obs_store, sig_hash)`` — a patch for the same failure already
  awaits review → surface it, skip the LLM (``stop_reason: cached``).
* ``find_replay_candidate(obs_store, patch_store, sig_hash, successful_ids)`` —
  an applied patch with the same signature already fixed this failure once
  (``healing_outcomes.run_success_after_patch = true``) → fetch its body from
  the object store and replay it through the gate pyramid
  (``stop_reason: replayed``). Gate fail falls through to the LLM.
* ``find_coaching_examples(...)`` — nearest-signature retrieval of past
  (failure → validated fix) pairs for the few-shot prompt section, served
  entirely from index metadata (no body reads).

Signatures + metadata are stamped into ``patch_index`` by
``stage_patch_for_human`` / ``archive_patch`` when a patch body is written.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aqueduct.patch import index as _ix

if TYPE_CHECKING:
    from aqueduct.stores.base import ObservabilityStore
    from aqueduct.stores.object_store import PatchStore

logger = logging.getLogger(__name__)

__all__ = [
    "CoachingExample",
    "PendingHit",
    "ReplayCandidate",
    "find_pending",
    "find_replay_candidate",
    "find_coaching_examples",
]

_COACHING_MAX = 3


@dataclass(frozen=True)
class PendingHit:
    """A pending patch whose failure signature matches the current failure."""

    object_key: str
    patch_id: str
    staged_at: str | None
    source: str  # "llm" | "replay"
    # Phase 78 — the engine the pending patch was staged for. The signature
    # already scopes the match by engine (folded into the hash), so this is
    # for auditability, not filtering.
    engine: str = ""


@dataclass(frozen=True)
class ReplayCandidate:
    """An applied, previously successful patch eligible for zero-token replay."""

    object_key: str
    patch_id: str
    payload: dict  # full patch JSON incl. operations — feed to PatchSpec.model_validate after stripping _aq_meta
    engine: str = ""  # Phase 78 — auditability, see PendingHit.engine


@dataclass(frozen=True)
class CoachingExample:
    """One (failure → validated fix) pair for the few-shot coaching section."""

    patch_id: str
    error_class: str
    where: str
    normalized_message: str
    rationale: str
    ops: list[str] = field(default_factory=list)
    tier: int = 4  # 1 exact sig, 2 coarse sig, 3 same error class, 4 chronological fill
    engine: str = ""  # Phase 78 — auditability, see PendingHit.engine


# ── Lookups ───────────────────────────────────────────────────────────────


def find_pending(obs_store: ObservabilityStore | None, sig_hash: str) -> PendingHit | None:
    """Newest pending patch whose exact failure signature matches, if any."""
    if obs_store is None or not sig_hash:
        return None
    try:
        with obs_store.connect() as cur:
            row = _ix.find_pending(cur, sig_hash)
    except Exception:
        logger.debug("find_pending query failed", exc_info=True)
        return None
    if row is None:
        return None
    return PendingHit(
        object_key=str(row.get("object_key") or ""),
        patch_id=str(row.get("patch_id") or ""),
        staged_at=row.get("created_at"),
        source=str(row.get("source") or "llm"),
        engine=str(row.get("engine") or ""),
    )


def find_replay_candidate(
    obs_store: ObservabilityStore | None,
    patch_store: PatchStore | None,
    sig_hash: str,
    successful_patch_ids: set[str],
) -> ReplayCandidate | None:
    """Newest applied patch matching the signature AND confirmed successful.

    The index gives the matching row; the body (with ``operations``) is fetched
    from the object store via ``object_key``. ``successful_patch_ids`` comes from
    ``healing_outcomes.run_success_after_patch = true`` — an applied patch with
    no success record is never replayed."""
    if obs_store is None or patch_store is None or not sig_hash or not successful_patch_ids:
        return None
    try:
        with obs_store.connect() as cur:
            row = _ix.find_replay(cur, sig_hash, successful_patch_ids)
    except Exception:
        logger.debug("find_replay query failed", exc_info=True)
        return None
    if row is None:
        return None
    object_key = str(row.get("object_key") or "")
    try:
        payload = patch_store.get_json(object_key)
    except Exception:
        logger.debug("Replay body unreadable at %s", object_key, exc_info=True)
        return None
    if not isinstance(payload, dict):
        return None
    return ReplayCandidate(
        object_key=object_key,
        patch_id=str(row.get("patch_id") or ""),
        payload=payload,
        engine=str(row.get("engine") or ""),
    )


def find_coaching_examples(
    obs_store: ObservabilityStore | None,
    exact_hash: str,
    coarse_hash: str,
    error_class: str,
    blueprint_id: str = "",
    limit: int = _COACHING_MAX,
) -> list[CoachingExample]:
    """Nearest-signature (failure → validated fix) pairs from applied patches.

    Tiered match (1 exact sig, 2 coarse sig, 3 same error_class, 4 chronological
    fill), deduped by patch_id, newest first within tier, capped at *limit* —
    served from index metadata, no body reads."""
    if obs_store is None:
        return []
    try:
        with obs_store.connect() as cur:
            rows = _ix.find_coaching(cur, exact_hash, coarse_hash, error_class, blueprint_id, limit)
    except Exception:
        logger.debug("find_coaching query failed", exc_info=True)
        return []
    out: list[CoachingExample] = []
    for d in rows:
        ops = d.get("ops")
        out.append(CoachingExample(
            patch_id=str(d.get("patch_id") or "?"),
            error_class=str(d.get("error_class") or "?"),
            where=str(d.get("where_field") or "?"),
            normalized_message=str(d.get("normalized_message") or ""),
            rationale=str(d.get("rationale") or ""),
            ops=list(ops) if isinstance(ops, list) else [],
            tier=int(d.get("_tier") or 4),
            engine=str(d.get("engine") or ""),
        ))
    return out
