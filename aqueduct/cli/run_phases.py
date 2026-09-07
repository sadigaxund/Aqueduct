"""`run` command phases — extracted verbatim from aqueduct/cli/run.py
(Phase 91 `run()` decomposition).

`run()`'s ~2500-line body is a sequence of phases (see
`run-decomposition-map.md` for the full phase table); this module collects
the ones that are pure moves — module-level functions with an explicit
keyword-only parameter list, no closure over `run()`'s other locals. Each
function is copied verbatim from its original line range in `run.py` (only
indentation and the `_handoff_root_abs` → `handoff_root_abs` local→param
rename are adjusted); no behaviour change.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING as _t

import click

from aqueduct import exit_codes

if _t:
    from aqueduct.compiler.models import Manifest


def check_resume_hash_guard(
    *,
    resume_run_id: str | None,
    force_resume: bool,
    manifest: Manifest,
    checkpoint_root_abs: Path | None,
    resolved_store_dir,
    handoff_root_abs: str,
) -> None:
    """`--resume` fails closed on a manifest-hash mismatch.

    `--resume <run_id>` reuses checkpoints from a PRIOR run. Two
    independent checkpoint mechanisms exist, both keyed off
    `aqueduct.executor.models.manifest_hash(manifest)` (a content hash
    of the WHOLE compiled Manifest — any Blueprint edit changes it):

      1. Module checkpoints (`checkpoint_root`/`store_dir/checkpoints`)
         — `<base>/<run_id>/_manifest_hash` stores the hash the ORIGINAL
         run compiled. Both engines' `execute()` already read this back
         and compare it (`spark/executor.py`, `duckdb_/executor.py`) —
         but only ever WARN (`runtime_resume_hash_changed`) and proceed;
         that permissive behaviour is a deliberate, separately-tested
         contract at the engine layer (see
         `test_resume_mismatched_manifest_warns_and_continues`) and is
         left untouched. The hard refusal below happens one layer up, at
         the CLI, BEFORE any engine session is even built.

      2. Handoff spill (`aqueduct/executor/spill.py`, polyglot Blueprints
         only) — laid out as `<handoff.root>/<manifest_hash>/<run_id>/`,
         keyed STRICTLY by the CURRENT hash. A mismatch here is not an
         observable "wrong hash" condition inside the orchestrator at
         all — it just finds nothing under the new hash and silently
         starts that island fresh. `find_run_under_other_hash` is the
         detector: it tells "genuinely first run of this run_id" apart
         from "run_id exists, but under a stale hash" by scanning every
         OTHER hash directory.

    Either mechanism finding a stale hash is refused identically here,
    unless `--force` (validated above) opts back into today's
    behaviour (module checkpoints keep warning-and-proceeding; handoff
    spill keeps silently re-executing that island).
    """
    if resume_run_id and not force_resume:
        from aqueduct.executor.models import manifest_hash as _manifest_hash_fn
        from aqueduct.executor.spill import find_run_under_other_hash as _find_other_hash

        _current_hash = _manifest_hash_fn(manifest)
        _stale_hash: str | None = None

        _checkpoints_base = (
            checkpoint_root_abs
            if checkpoint_root_abs
            else (resolved_store_dir / "checkpoints" if resolved_store_dir else None)
        )
        if _checkpoints_base is not None:
            _stored_hash_path = Path(_checkpoints_base) / resume_run_id / "_manifest_hash"
            if _stored_hash_path.exists():
                _stored_hash = _stored_hash_path.read_text(encoding="utf-8").strip()
                if _stored_hash != _current_hash:
                    _stale_hash = _stored_hash

        if _stale_hash is None and len(manifest.islands) > 1:
            _stale_hash = _find_other_hash(handoff_root_abs, resume_run_id, _current_hash)

        if _stale_hash is not None:
            click.echo(
                f"✗ --resume {resume_run_id!r} refused: checkpoint manifest hash "
                f"{_stale_hash!r} does not match this run's compiled Manifest hash "
                f"{_current_hash!r} — the Blueprint (or its context/profile) has "
                "changed since that run's checkpoints were written. Pass --force to "
                "reuse them anyway, or drop --resume to start fresh.",
                err=True,
            )
            sys.exit(exit_codes.CONFIG_ERROR)


def check_from_to_island_guard(
    *,
    manifest: Manifest,
    from_module: str | None,
    to_module: str | None,
) -> None:
    """`--from` / `--to` are not yet island-aware.

    Module-range selection assumes ONE execution graph; which island(s)
    a `--from`/`--to` pair spans, and how a sub-manifest gets built per
    island for a partial range, is real cross-island work this batch
    does not attempt. Refusing loudly (CONFIG_ERROR) beats silently
    running the whole polyglot graph while looking like it honoured the
    flag — the same "loud, not silent" choice `--sandbox` already makes
    for a polyglot Manifest below.
    """
    if len(manifest.islands) > 1 and (from_module or to_module):
        click.echo(
            "✗ --from/--to do not yet support a polyglot Blueprint "
            f"({len(manifest.islands)} islands) — module-range selection "
            "across engine islands is not implemented in this release",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)
