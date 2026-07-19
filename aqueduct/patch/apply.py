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

import fnmatch
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from ruamel.yaml import YAML

from aqueduct.compiler.expander import is_arcade_expanded_id
from aqueduct.errors import AqueductError
from aqueduct.parser.parser import ParseError, parse
from aqueduct.patch.grammar import PATCH_META_KEY, PatchSpec
from aqueduct.patch.operations import PatchOperationError, apply_operation
from aqueduct.patch.provenance import build_healed_by_record
from aqueduct.redaction import redact as _redact
from aqueduct.stores.object_store import PatchStore

_ryaml = YAML()
_ryaml.preserve_quotes = True
_ryaml.default_flow_style = False
_ryaml.width = 4096  # prevent unwanted line wrapping
_ryaml.indent(mapping=2, sequence=4, offset=2)  # ensures `  - item` style (dash at col+2, content at col+4)

logger = logging.getLogger(__name__)


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


class PatchError(AqueductError):
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
    data.pop(PATCH_META_KEY, None)

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


def _append_healed_by(patched: Any, record: dict[str, Any] | None) -> Any:
    """Append one ``healed_by:`` record to a ruamel-loaded Blueprint dict.

    No-op (returns ``patched`` unchanged) when ``record`` is None — a patch
    with no heal-engine provenance (hand-authored, no ``_aq_meta.engine``)
    does not get a `healed_by` entry. Ruamel round-trip safe: the record is
    converted through ``_to_ruamel`` before insertion so block-style
    formatting/comments on the rest of the document survive.
    """
    if record is None:
        return patched
    if patched.get("healed_by") is None:
        patched["healed_by"] = _to_ruamel([])
    patched["healed_by"].append(_to_ruamel(record))
    return patched


def stamp_validated_engine(blueprint_path: Path, engine: str) -> bool:
    """Self-clearing provenance stamp (Phase 79): append *engine* to every
    ``healed_by`` record's ``validated_on`` list on a GREEN run, when it
    isn't already there.

    Best-effort and MUST NEVER raise — called from the CLI's run-success
    path; a stamp failure must never fail an otherwise-successful run. A
    Blueprint with no `healed_by:` block is left untouched entirely (no write
    at all), never given an empty block.

    Returns True if the file was rewritten (something changed), False
    otherwise (no provenance records, nothing to add, or an error — check the
    log for the latter).
    """
    try:
        if not blueprint_path.exists():
            return False
        raw = _yaml_load(blueprint_path)
        healed_by = raw.get("healed_by") if hasattr(raw, "get") else None
        if not healed_by:
            return False
        changed = False
        for rec in healed_by:
            validated = rec.get("validated_on")
            if validated is None:
                rec["validated_on"] = _to_ruamel([engine])
                changed = True
            elif engine not in list(validated):
                validated.append(engine)
                changed = True
        if changed:
            _yaml_dump(raw, blueprint_path)
        return changed
    except Exception:
        logger.warning(
            "stamp_validated_engine failed for %s (engine=%s) — provenance "
            "validated_on not updated; run success is unaffected",
            blueprint_path, engine, exc_info=True,
        )
        return False


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


_CTX_REF_RE = __import__("re").compile(r"^\$\{ctx\.([a-zA-Z0-9_.]+)\}$")


def _resolve_path_for_guardrail(value: Any, provenance_map: Any | None) -> str:
    """Resolve a ${ctx.*} template path to its runtime value via ProvenanceMap.

    Non-template values pass through unchanged. Used so allowed_paths fnmatch
    patterns are evaluated against actual runtime paths, not template strings.
    """
    text = str(value or "")
    ctx_match = _CTX_REF_RE.match(text)
    if ctx_match and provenance_map is not None:
        ctx_key = ctx_match.group(1)
        ctx_prov = provenance_map.context.get(ctx_key)
        if ctx_prov is not None and ctx_prov.resolved_value is not None:
            return str(ctx_prov.resolved_value)
    return text


def _check_path_against_allowlist(
    path_value: Any,
    allowed_paths: list[str],
    op_name: str,
    module_id: str,
    provenance_map: Any | None,
    key_hint: str = "path",
) -> None:
    """Raise PatchError if a resolved path doesn't match any allowed_paths pattern."""
    resolved = _resolve_path_for_guardrail(path_value, provenance_map)
    if not resolved:
        return
    if not any(fnmatch.fnmatch(resolved, pat) for pat in allowed_paths):
        raise PatchError(
            f"Path value {resolved!r} (key={key_hint!r}) in op {op_name!r} "
            f"(module {module_id!r}) does not match any "
            f"agent.guardrails.allowed_paths pattern: {allowed_paths}"
        )


def _check_config_dict_paths(
    config: Any,
    allowed_paths: list[str],
    op_name: str,
    module_id: str,
    provenance_map: Any | None,
) -> None:
    """Check path/output_path keys inside a module config dict (full or partial)."""
    if not isinstance(config, dict):
        return
    for key in ("path", "output_path"):
        if key in config:
            _check_path_against_allowlist(
                config[key], allowed_paths, op_name, module_id, provenance_map, key_hint=key,
            )


def _check_guardrails(
    patch_spec: PatchSpec,
    bp_raw: dict,
    provenance_map: Any | None = None,
) -> None:
    """Deterministically enforce agent.guardrails declared in the Blueprint.

    Raises PatchError if any operation violates:
      - forbidden_ops: op type is in the blocklist
      - allowed_paths: any op that writes a `path` or `output_path` value to a
                       module config — whether via set_module_config_key,
                       replace_module_config, insert_module, add_probe, or
                       add_arcade_ref — must resolve to a value matching at least
                       one fnmatch pattern. ${ctx.*} values are resolved via
                       provenance_map before matching.

    Arcade-expanded modules (id contains `__`) are skipped for path checks —
    those IDs do not exist in the Blueprint YAML and the apply step will raise
    a clearer "Module not found" error.
    """
    guardrails = (bp_raw.get("agent") or {}).get("guardrails") or {}
    forbidden_ops: list[str] = guardrails.get("forbidden_ops") or []
    allowed_paths: list[str] = guardrails.get("allowed_paths") or []

    for op in patch_spec.operations:
        op_name = op.op

        if op_name in forbidden_ops:
            raise PatchError(
                f"Operation {op_name!r} is forbidden by agent.guardrails.forbidden_ops. "
                f"Blocked ops: {forbidden_ops}"
            )

        if not allowed_paths:
            continue

        # set_module_config_key — single dotted key inside an existing module config
        if op_name == "set_module_config_key":
            module_id = getattr(op, "module_id", "") or ""
            if is_arcade_expanded_id(module_id):
                continue
            key = getattr(op, "key", None)
            if key in ("path", "output_path"):
                _check_path_against_allowlist(
                    getattr(op, "value", None), allowed_paths,
                    op_name, module_id, provenance_map, key_hint=str(key),
                )

        # replace_module_config — full config dict replacement on an existing module
        elif op_name == "replace_module_config":
            module_id = getattr(op, "module_id", "") or ""
            if is_arcade_expanded_id(module_id):
                continue
            _check_config_dict_paths(
                getattr(op, "config", None), allowed_paths,
                op_name, module_id, provenance_map,
            )

        # insert_module / add_probe / add_arcade_ref — new module definitions carry full config
        elif op_name in ("insert_module", "add_probe", "add_arcade_ref"):
            module_dict = getattr(op, "module", None) or {}
            module_id = (module_dict.get("id") or "") if isinstance(module_dict, dict) else ""
            if is_arcade_expanded_id(module_id):
                continue
            cfg = module_dict.get("config") if isinstance(module_dict, dict) else None
            _check_config_dict_paths(
                cfg, allowed_paths,
                op_name, module_id, provenance_map,
            )


def _set_index_status(
    obs_store: Any | None, patch_id: str, status: str, object_key: str | None = None,
) -> None:
    """Best-effort ``patch_index`` status update (Phase 53). Never raises.

    ``object_key`` MUST be passed when the body moves to a new lifecycle prefix
    (pending → applied/rejected) so the heal cache's ``find_replay`` fetches the
    body from its new key, not the stale pending one."""
    if obs_store is None:
        return
    try:
        from aqueduct.patch import index as _ix
        with obs_store.connect() as cur:
            _ix.ensure_schema(cur)
            _ix.set_status(cur, patch_id, status, object_key=object_key)
    except Exception:
        logger.warning("_set_index_status failed for %s", patch_id, exc_info=True)


def apply_patch_file(
    blueprint_path: Path,
    patch_path: Path,
    patches_dir: Path = Path("patches"),
    provenance_map: Any | None = None,
    obs_store: Any | None = None,
    patch_store: Any | None = None,
    pending_key: str | None = None,
) -> ApplyResult:
    """Full apply lifecycle: validate → apply → verify → backup → write → archive.

    Args:
        blueprint_path: Path to the Blueprint YAML file to patch.
        patch_path:     Path to the PatchSpec JSON file (operations source).
        patches_dir:    Root directory for patch lifecycle dirs (backups/, applied/).
        patch_store:    When given, the patch BODY lifecycle (archive to
                        ``applied/`` and remove the ``pending/`` source) runs in
                        this ``PatchStore`` — local OR object backend — instead of
                        raw local-FS writes, so a remote-staged patch is moved in
                        the store (no drift vs ``patch list``). ``object_key`` in
                        the index is updated to the new applied key.
        pending_key:    Store-relative key of the pending body to delete after
                        archiving (None for an external import with no pending entry).

    Returns:
        ApplyResult with paths and metadata.

    Raises:
        PatchError: On validation failure, operation failure, or post-patch
                    Blueprint parse failure.
    """
    if not blueprint_path.exists():
        raise PatchError(f"Blueprint not found: {blueprint_path}")

    # Single timestamp for both the healed_by record (below) and the applied
    # patch archive (step 7) — one apply, one applied_at.
    applied_at = datetime.now(tz=UTC).isoformat()

    # ── 1. Load and validate PatchSpec ────────────────────────────────────────
    patch_spec = load_patch_spec(patch_path)

    # Raw JSON (pre-validation) carries `_aq_meta` — PATCH_META_KEY is popped
    # before PatchSpec validation in load_patch_spec, so re-read it here for
    # the healed_by provenance record (engine/engine_version/run_id the heal
    # loop stamped — see aqueduct/agent/loop.py). Best-effort: an unreadable
    # or meta-less patch simply gets no healed_by record (see
    # build_healed_by_record's docstring — that's correct for a
    # hand-authored patch, not just a fallback).
    try:
        _raw_patch_json = json.loads(patch_path.read_text(encoding="utf-8"))
    except Exception:
        _raw_patch_json = {}
    _patch_meta_raw = (
        _raw_patch_json.get(PATCH_META_KEY, {})
        if isinstance(_raw_patch_json, dict) else {}
    )
    _patch_meta: dict[str, Any] = _patch_meta_raw if isinstance(_patch_meta_raw, dict) else {}

    # ── 2. Load Blueprint YAML (ruamel preserves comments + key order) ───────
    try:
        bp_raw = _yaml_load(blueprint_path)
    except Exception as exc:
        raise PatchError(f"Cannot load Blueprint YAML from {blueprint_path}: {exc}") from exc

    # ── 2.5 Guardrail enforcement (deterministic — blueprint config, not prompt) ─
    _check_guardrails(patch_spec, bp_raw, provenance_map=provenance_map)

    # ── 3 & 4. Apply operations on deep copy ──────────────────────────────────
    patched = apply_patch_to_dict(bp_raw, patch_spec)

    # ── 4.5 Heal-patch provenance (Phase 79) — append/extend `healed_by:` ────
    # BEFORE the post-patch parse verification (next step), so a malformed
    # HealedByRecordSchema fails the apply atomically like any other
    # operation, and so `validate_unique_module_ids`/schema checks below see
    # the final document.
    _healed_by_record = build_healed_by_record(
        patch_id=patch_spec.patch_id,
        operations=patch_spec.operations,
        meta=_patch_meta,
        applied_at=applied_at,
        fallback_run_id=patch_spec.run_id,
    )
    patched = _append_healed_by(patched, _healed_by_record)

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

    # ── 7. Archive PatchSpec to applied/ (in the PatchStore when given) ───────
    filename = Path(pending_key).name if pending_key else patch_path.name
    archive_path = (patches_dir / "applied" / filename)
    applied_key: str | None = None
    try:
        raw_spec = _raw_patch_json if isinstance(_raw_patch_json, dict) else json.loads(patch_path.read_text(encoding="utf-8"))
        raw_spec["applied_at"] = applied_at
        raw_spec["blueprint_path"] = str(blueprint_path)
        if patch_store is not None:
            # Backend-aware move: write applied/<file> + delete pending/<file> in
            # the store (local rename or s3 object move). Unifies local + remote.
            applied_key = patch_store.write_applied(filename, _redact(raw_spec))
            if pending_key:
                try:
                    patch_store.delete(pending_key)
                except Exception:
                    logger.debug("could not delete pending body %s", pending_key, exc_info=True)
            archive_path = Path(getattr(patch_store, "location_label", str(patches_dir))) / "applied" / filename
            # Drop the local materialised copy (the store now owns the body).
            if patch_path.exists():
                try:
                    patch_path.unlink()
                except Exception:
                    pass
        else:
            applied_dir = patches_dir / "applied"
            applied_dir.mkdir(parents=True, exist_ok=True)
            archive_path = applied_dir / patch_path.name
            archive_path.write_text(json.dumps(_redact(raw_spec), indent=2), encoding="utf-8")
            if patch_path.exists():  # remove from pending — applied + archived
                patch_path.unlink()
    except Exception as exc:
        logger.warning("could not archive patch to %s: %s", archive_path, exc)

    # Phase 53 — mark applied in the index (+ new object_key) so the heal cache
    # replays it from applied/ and stops surfacing it as still-pending.
    _set_index_status(obs_store, patch_spec.patch_id, PatchStore.APPLIED, object_key=applied_key)

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
    obs_store: Any | None = None,
    patch_store: Any | None = None,
    pending_key: str | None = None,
) -> Path:
    """Move a pending patch to rejected/ with a reason annotation.

    Args:
        patch_id:    The patch_id (filename without .json extension).
        reason:      Human-readable rejection reason.
        patches_dir: Root directory for patch lifecycle dirs.
        patch_store: When given (with ``pending_key``), the body move runs in this
                     ``PatchStore`` (local OR object backend) — write ``rejected/``
                     + delete ``pending/`` — so a remote-staged patch is moved in
                     the store and the index ``object_key`` is updated.
        pending_key: Store-relative key of the pending body.

    Returns:
        Path (or store-key display path) to the rejected patch.

    Raises:
        PatchError: Patch not found in patches/pending/.
    """
    if patch_store is not None and pending_key is not None:
        try:
            raw = json.loads(patch_store.get_text(pending_key))
        except Exception as exc:
            raise PatchError(f"Cannot read pending patch {patch_id}: {exc}") from exc
        raw["rejected_at"] = datetime.now(tz=UTC).isoformat()
        raw["rejection_reason"] = reason
        filename = Path(pending_key).name
        rejected_key = patch_store.write_rejected(filename, _redact(raw))
        try:
            patch_store.delete(pending_key)
        except Exception:
            logger.debug("could not delete pending body %s", pending_key, exc_info=True)
        _set_index_status(obs_store, patch_id, PatchStore.REJECTED, object_key=rejected_key)
        return Path(getattr(patch_store, "location_label", str(patches_dir))) / "rejected" / filename

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

    raw["rejected_at"] = datetime.now(tz=UTC).isoformat()
    raw["rejection_reason"] = reason
    rejected_path.write_text(json.dumps(_redact(raw), indent=2), encoding="utf-8")
    pending_path.unlink()

    _set_index_status(obs_store, patch_id, PatchStore.REJECTED)

    return rejected_path
