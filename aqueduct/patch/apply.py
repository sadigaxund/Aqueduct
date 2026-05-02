"""Patch apply orchestrator — atomic Blueprint update and archive.

Lifecycle for apply_patch_file():
  1. Load and validate PatchSpec JSON.
  2. Load Blueprint YAML.
  3. Deep-copy Blueprint dict.
  4. Apply each operation left-to-right on the copy.
  5. Re-parse with the Parser to verify the result is a valid Blueprint.
  6. Write the patched Blueprint atomically (write temp → os.replace).
  7. Archive the PatchSpec to patches/applied/ (with applied_at timestamp).
  8. Return ApplyResult.

Rollback strategy: use git (aqueduct rollback). No file backup is kept.
On failure between steps 4 and 6, original Blueprint is unchanged.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from io import StringIO

from pydantic import ValidationError
from ruamel.yaml import YAML

from aqueduct.patch.grammar import PatchSpec
from aqueduct.patch.operations import PatchOperationError, apply_operation
from aqueduct.parser.parser import ParseError, parse

_ryaml = YAML()
_ryaml.preserve_quotes = True
_ryaml.default_flow_style = False
_ryaml.width = 4096  # prevent unwanted line wrapping
_ryaml.indent(mapping=2, sequence=4, offset=2)  # ensures `  - item` style (dash at col+2, content at col+4)


def _yaml_load(path: Path) -> Any:
    with path.open(encoding="utf-8") as f:
        return _ryaml.load(f)


def _yaml_dump(data: Any, path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        _ryaml.dump(data, f)


def _yaml_dumps(data: Any) -> str:
    buf = StringIO()
    _ryaml.dump(data, buf)
    return buf.getvalue()


class PatchError(Exception):
    """Raised when a patch cannot be applied."""


# ── Result model ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ApplyResult:
    patch_id: str
    blueprint_path: Path
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
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PatchError(f"Invalid JSON in {patch_path}: {exc}") from exc

    # Strip metadata injected by _stage_for_human / _auto_apply before schema validation.
    data.pop("_aq_meta", None)

    try:
        return PatchSpec.model_validate(data)
    except ValidationError as exc:
        raise PatchError(
            f"PatchSpec validation failed for {patch_path}:\n{exc}"
        ) from exc


def _ruamel_copy(data: Any) -> Any:
    """Deep-copy a ruamel CommentedMap/CommentedSeq via round-trip serialization.

    copy.deepcopy() on ruamel objects corrupts internal lc/ca metadata, causing
    invalid YAML output (e.g. `modules: -` instead of block sequence). Round-trip
    through the YAML serializer preserves all comment and indent state.
    """
    buf = StringIO()
    _ryaml.dump(data, buf)
    return _ryaml.load(buf.getvalue())


def _to_ruamel(data: Any) -> Any:
    """Convert a plain Python dict/list to a ruamel CommentedMap/CommentedSeq.

    Needed when PatchSpec operations inject plain dicts (parsed from JSON) into
    a ruamel tree — without this, the dumper loses block-style formatting.
    """
    buf = StringIO()
    _ryaml.dump(data, buf)
    return _ryaml.load(buf.getvalue())


def apply_patch_to_dict(bp: dict, patch_spec: PatchSpec) -> dict:
    """Apply all operations in patch_spec to a ruamel-safe copy of bp.

    Returns the modified Blueprint dict.  bp is never mutated.

    Raises:
        PatchError: If any operation fails (all-or-nothing).
    """
    working = _ruamel_copy(bp)
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

    # ── 2. Load Blueprint YAML (ruamel preserves comments + key order) ───────
    try:
        bp_raw = _yaml_load(blueprint_path)
    except Exception as exc:
        raise PatchError(f"Cannot load Blueprint YAML from {blueprint_path}: {exc}") from exc

    # ── 3 & 4. Apply operations on deep copy ──────────────────────────────────
    patched = apply_patch_to_dict(bp_raw, patch_spec)

    # ── 5. Re-parse to verify Blueprint validity ──────────────────────────────
    tmp_verify = blueprint_path.with_suffix(".patch_verify.tmp.yml")
    try:
        _yaml_dump(patched, tmp_verify)
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

    # ── 6. Write patched Blueprint atomically ─────────────────────────────────
    tmp_out = blueprint_path.with_suffix(".patch_out.tmp.yml")
    try:
        _yaml_dump(patched, tmp_out)
        os.replace(tmp_out, blueprint_path)
    except Exception as exc:
        if tmp_out.exists():
            tmp_out.unlink()
        raise PatchError(f"Failed to write patched Blueprint: {exc}") from exc

    # ── 7. Archive PatchSpec to patches/applied/ ──────────────────────────────
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
        import sys
        print(f"[patch] warning: could not archive patch to {archive_path}: {exc}", file=sys.stderr)

    return ApplyResult(
        patch_id=patch_spec.patch_id,
        blueprint_path=blueprint_path,
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
    pending_dir = patches_dir / "pending"
    # Try exact filename first, then glob for new-style {seq}_{ts}_{slug}.json naming
    pending_path = pending_dir / f"{patch_id}.json"
    if not pending_path.exists():
        matches = list(pending_dir.glob(f"*_{patch_id}.json")) if pending_dir.exists() else []
        if not matches:
            raise PatchError(
                f"Patch {patch_id!r} not found in {pending_dir}. "
                f"Available: {[p.name for p in pending_dir.glob('*.json')] if pending_dir.exists() else []}"
            )
        pending_path = sorted(matches)[-1]

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
