"""Patch apply orchestrator — atomic Blueprint update with backup and archive.

Lifecycle for apply_patch_file():
  1. Load and validate PatchSpec JSON.
  2. Load Blueprint YAML.
  3. Deep-copy Blueprint dict.
  4. Apply each operation left-to-right on the copy.
  5. Re-parse with the Parser to verify the result is a valid Blueprint.
  6. Backup the original Blueprint to patches/backups/.
  7. Write the patched Blueprint atomically (write temp → os.replace).
  8. Archive the PatchSpec to patches/applied/ (with applied_at timestamp).
  9. Return ApplyResult.

On any failure between steps 4 and 8, the original Blueprint is unchanged —
the temp file is cleaned up and the backup is left in place.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from aqueduct.patch.grammar import PatchSpec
from aqueduct.patch.operations import PatchOperationError, apply_operation
from aqueduct.parser.parser import ParseError, parse


class PatchError(Exception):
    """Raised when a patch cannot be applied."""


# ── Result model ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ApplyResult:
    patch_id: str
    blueprint_path: Path
    backup_path: Path
    archive_path: Path
    operations_applied: int
    applied_at: str


# ── Public API ────────────────────────────────────────────────────────────────

def load_patch_spec(patch_path: Path) -> PatchSpec:
    """Load and validate a PatchSpec from a JSON file.

    Raises:
        PatchError: File not found, invalid JSON, or schema validation failure.
    """
    if not patch_path.exists():
        raise PatchError(f"Patch file not found: {patch_path}")

    try:
        raw = patch_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PatchError(f"Cannot read patch file {patch_path}: {exc}") from exc

    try:
        return PatchSpec.model_validate_json(raw)
    except ValidationError as exc:
        raise PatchError(
            f"PatchSpec validation failed for {patch_path}:\n{exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise PatchError(f"Invalid JSON in {patch_path}: {exc}") from exc


def apply_patch_to_dict(bp: dict, patch_spec: PatchSpec) -> dict:
    """Apply all operations in patch_spec to a deep copy of bp.

    Returns the modified Blueprint dict.  bp is never mutated.

    Raises:
        PatchError: If any operation fails (all-or-nothing).
    """
    working = copy.deepcopy(bp)
    for i, op in enumerate(patch_spec.operations):
        try:
            working = apply_operation(working, op)
        except PatchOperationError as exc:
            raise PatchError(
                f"Operation {i + 1}/{len(patch_spec.operations)} "
                f"({op.op!r}) failed: {exc}"
            ) from exc
    return working


def apply_patch_file(
    blueprint_path: Path,
    patch_path: Path,
    patches_dir: Path = Path("patches"),
) -> ApplyResult:
    """Full apply lifecycle: validate → apply → verify → backup → write → archive.

    Args:
        blueprint_path: Path to the Blueprint YAML file to patch.
        patch_path:     Path to the PatchSpec JSON file.
        patches_dir:    Root directory for patch lifecycle dirs (backups/, applied/).

    Returns:
        ApplyResult with paths and metadata.

    Raises:
        PatchError: On validation failure, operation failure, or post-patch
                    Blueprint parse failure.
    """
    if not blueprint_path.exists():
        raise PatchError(f"Blueprint not found: {blueprint_path}")

    # ── 1. Load and validate PatchSpec ────────────────────────────────────────
    patch_spec = load_patch_spec(patch_path)

    # ── 2. Load Blueprint YAML ────────────────────────────────────────────────
    try:
        bp_raw = yaml.safe_load(blueprint_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PatchError(f"Cannot load Blueprint YAML from {blueprint_path}: {exc}") from exc

    # ── 3 & 4. Apply operations on deep copy ──────────────────────────────────
    patched = apply_patch_to_dict(bp_raw, patch_spec)

    # ── 5. Re-parse to verify Blueprint validity ──────────────────────────────
    tmp_verify = blueprint_path.with_suffix(".patch_verify.tmp.yml")
    try:
        tmp_verify.write_text(yaml.dump(patched, allow_unicode=True), encoding="utf-8")
        parse(str(tmp_verify))
    except ParseError as exc:
        raise PatchError(
            f"Patched Blueprint is invalid (PatchSpec operations produced a "
            f"Blueprint that does not pass the Parser):\n{exc}"
        ) from exc
    except Exception as exc:
        raise PatchError(f"Unexpected error during post-patch verification: {exc}") from exc
    finally:
        if tmp_verify.exists():
            tmp_verify.unlink()

    # ── 6. Backup original Blueprint ──────────────────────────────────────────
    backup_dir = patches_dir / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = backup_dir / f"{patch_spec.patch_id}_{ts}_{blueprint_path.name}"
    shutil.copy2(blueprint_path, backup_path)

    # ── 7. Write patched Blueprint atomically ─────────────────────────────────
    tmp_out = blueprint_path.with_suffix(".patch_out.tmp.yml")
    try:
        tmp_out.write_text(yaml.dump(patched, allow_unicode=True), encoding="utf-8")
        os.replace(tmp_out, blueprint_path)
    except Exception as exc:
        if tmp_out.exists():
            tmp_out.unlink()
        raise PatchError(f"Failed to write patched Blueprint: {exc}") from exc

    # ── 8. Archive PatchSpec to patches/applied/ ──────────────────────────────
    applied_dir = patches_dir / "applied"
    applied_dir.mkdir(parents=True, exist_ok=True)
    archive_path = applied_dir / patch_path.name

    applied_at = datetime.now(tz=timezone.utc).isoformat()
    try:
        raw_spec = json.loads(patch_path.read_text(encoding="utf-8"))
        raw_spec["applied_at"] = applied_at
        raw_spec["blueprint_path"] = str(blueprint_path)
        archive_path.write_text(json.dumps(raw_spec, indent=2), encoding="utf-8")
    except Exception as exc:
        # Archive failure is non-fatal — patch already applied to Blueprint
        import sys
        print(f"[patch] warning: could not archive patch to {archive_path}: {exc}", file=sys.stderr)

    return ApplyResult(
        patch_id=patch_spec.patch_id,
        blueprint_path=blueprint_path,
        backup_path=backup_path,
        archive_path=archive_path,
        operations_applied=len(patch_spec.operations),
        applied_at=applied_at,
    )


def reject_patch(
    patch_id: str,
    reason: str,
    patches_dir: Path = Path("patches"),
) -> Path:
    """Move a pending patch to patches/rejected/ with a reason annotation.

    Args:
        patch_id:    The patch_id (filename without .json extension).
        reason:      Human-readable rejection reason.
        patches_dir: Root directory for patch lifecycle dirs.

    Returns:
        Path to the rejected patch file.

    Raises:
        PatchError: Patch not found in patches/pending/.
    """
    pending_path = patches_dir / "pending" / f"{patch_id}.json"
    if not pending_path.exists():
        raise PatchError(
            f"Patch {patch_id!r} not found in {patches_dir / 'pending'}. "
            f"Available: {[p.stem for p in (patches_dir / 'pending').glob('*.json')]}"
        )

    rejected_dir = patches_dir / "rejected"
    rejected_dir.mkdir(parents=True, exist_ok=True)
    rejected_path = rejected_dir / f"{patch_id}.json"

    try:
        raw = json.loads(pending_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PatchError(f"Cannot read pending patch {patch_id}: {exc}") from exc

    raw["rejected_at"] = datetime.now(tz=timezone.utc).isoformat()
    raw["rejection_reason"] = reason
    rejected_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    pending_path.unlink()

    return rejected_path
