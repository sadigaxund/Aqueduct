"""Heal memory — signature-keyed lookups over the patch lifecycle dirs.

Phase 45 signature memory. Three zero-token paths consulted BEFORE the LLM:

* ``find_pending(sig_hash, patches_dir)`` — a patch for the same failure is
  already awaiting review in ``patches/pending/`` → surface it, skip the LLM
  (``stop_reason: cached``). Fixes the human/ci-mode "re-heal every run while
  review pending" token burn.
* ``find_replay_candidate(sig_hash, patches_dir, successful_patch_ids)`` —
  an archived patch with the same signature already fixed this failure once
  (``healing_outcomes.run_success_after_patch = true``) → replay it through
  the normal gate pyramid (``stop_reason: replayed``). Gate fail falls
  through to the LLM.
* ``find_coaching_examples(...)`` — nearest-signature retrieval of past
  (failure → validated fix) pairs used as few-shot prompt examples.

Signatures are stamped into each patch file's ``_aq_meta`` by
``stage_patch_for_human`` / ``archive_patch`` (``failure_signature`` full
dict + ``failure_signature_coarse`` hash). Files staged before Phase 45
carry no signature and are simply never matched.

This module is intentionally pure: file reads only — no LLM calls, no DB
connections (callers pass in the success set from the observability store).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

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

    path: Path
    patch_id: str
    staged_at: str | None
    source: str  # "llm" | "replay"


@dataclass(frozen=True)
class ReplayCandidate:
    """An archived, previously successful patch eligible for zero-token replay."""

    path: Path
    patch_id: str
    payload: dict  # full patch JSON incl. operations — feed to PatchSpec.model_validate after stripping _aq_meta


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


# ── File scanning ─────────────────────────────────────────────────────────


def _iter_patch_payloads(directory: Path) -> list[tuple[Path, float, dict]]:
    """Yield ``(path, mtime, payload)`` for every readable patch JSON, newest first.

    Mirrors ``prompts._load_previous_patches``: ``os.scandir`` for bounded
    stat overhead, malformed files skipped at DEBUG.
    """
    if not directory.exists():
        return []
    entries = [
        e for e in os.scandir(directory)
        if e.name.endswith(".json") and e.is_file()
    ]
    out: list[tuple[Path, float, dict]] = []
    for e in sorted(entries, key=lambda e: e.stat().st_mtime, reverse=True):
        try:
            payload = json.loads(Path(e.path).read_text(encoding="utf-8"))
        except Exception:
            logger.debug("Skipping unreadable patch file %s", e.path, exc_info=True)
            continue
        if isinstance(payload, dict):
            out.append((Path(e.path), e.stat().st_mtime, payload))
    return out


def _sig_meta(payload: dict) -> tuple[str | None, str | None, dict]:
    """Extract ``(exact_hash, coarse_hash, exact_sig_dict)`` from ``_aq_meta``.

    ``failure_signature`` is a full dict on Phase-45 files; tolerate a bare
    hash string for forward robustness.
    """
    meta = payload.get("_aq_meta") or {}
    raw = meta.get("failure_signature")
    if isinstance(raw, dict):
        return raw.get("hash"), meta.get("failure_signature_coarse"), raw
    if isinstance(raw, str):
        return raw, meta.get("failure_signature_coarse"), {}
    return None, meta.get("failure_signature_coarse"), {}


# ── Lookups ───────────────────────────────────────────────────────────────


def find_pending(sig_hash: str, patches_dir: Path) -> PendingHit | None:
    """Newest pending patch whose exact failure signature matches, if any."""
    for path, _mtime, payload in _iter_patch_payloads(patches_dir / "pending"):
        exact, _coarse, _sig = _sig_meta(payload)
        if exact == sig_hash:
            meta = payload.get("_aq_meta") or {}
            return PendingHit(
                path=path,
                patch_id=str(payload.get("patch_id") or path.stem),
                staged_at=meta.get("staged_at"),
                source=str(meta.get("source") or "llm"),
            )
    return None


def find_replay_candidate(
    sig_hash: str,
    patches_dir: Path,
    successful_patch_ids: set[str],
) -> ReplayCandidate | None:
    """Newest archived patch matching the signature AND confirmed successful.

    ``successful_patch_ids`` comes from
    ``healing_outcomes.run_success_after_patch = true`` — an archived patch
    without a success record is never replayed (it may not have worked).
    """
    if not successful_patch_ids:
        return None
    for path, _mtime, payload in _iter_patch_payloads(patches_dir / "applied"):
        exact, _coarse, _sig = _sig_meta(payload)
        patch_id = payload.get("patch_id")
        if exact == sig_hash and patch_id in successful_patch_ids:
            return ReplayCandidate(path=path, patch_id=str(patch_id), payload=payload)
    return None


def find_coaching_examples(
    exact_hash: str,
    coarse_hash: str,
    error_class: str,
    patches_dir: Path,
    limit: int = _COACHING_MAX,
) -> list[CoachingExample]:
    """Nearest-signature (failure → validated fix) pairs from ``patches/applied/``.

    Tiered match, newest first within each tier:

    1. exact signature hash (same error, same module),
    2. coarse signature hash (same error shape, any module),
    3. same ``error_class``,
    4. chronological fill (pre-Phase-45 files without signatures land here),

    deduplicated by patch_id across tiers, capped at ``limit``.
    """
    tiers: dict[int, list[CoachingExample]] = {1: [], 2: [], 3: [], 4: []}
    for _path, _mtime, payload in _iter_patch_payloads(patches_dir / "applied"):
        exact, coarse, sig = _sig_meta(payload)
        if exact == exact_hash:
            tier = 1
        elif coarse is not None and coarse == coarse_hash:
            tier = 2
        elif sig.get("error_class") and sig.get("error_class") == error_class:
            tier = 3
        else:
            tier = 4
        tiers[tier].append(CoachingExample(
            patch_id=str(payload.get("patch_id") or "?"),
            error_class=str(sig.get("error_class") or "?"),
            where=str(sig.get("where") or "?"),
            normalized_message=str(sig.get("normalized_message") or ""),
            rationale=str(payload.get("rationale") or payload.get("description") or ""),
            ops=[op.get("op", "?") for op in payload.get("operations", []) if isinstance(op, dict)],
            tier=tier,
        ))
    picked: list[CoachingExample] = []
    seen: set[str] = set()
    for tier in (1, 2, 3, 4):
        for ex in tiers[tier]:
            if ex.patch_id in seen:
                continue
            seen.add(ex.patch_id)
            picked.append(ex)
            if len(picked) >= limit:
                return picked
    return picked
