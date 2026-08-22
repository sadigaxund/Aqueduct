"""Shared verbosity resolution — the one place the CLI's `-v` flags agree.

Phase 85 ruling: there is exactly ONE verbosity *concept* (a 0/1/2 tier),
even though Click can't forward a root-group option placed AFTER a
subcommand (`aqueduct run -v bp.yml`). So `run` and `doctor` keep their own
`-v` COUNT option (not a second, independently-meaning flag — see their
help text) purely so that postfix usage still works, and every consumer
merges the two via `resolve_verbosity()` instead of reading either flag
directly.

Tiers (documented once here; every consumer/help string should agree):
    0        — clean narrative (default).
    1  (-v)  — full Aqueduct-side story: untruncated errors/warnings,
               uncollapsed doctor rows, uncapped probe notes, transcript
               detail.
    2 (-vv)  — raw layer: engine/Spark log4j output, prompt text, streamed
               model text (``quiet_startup`` flips off only here).

``--debug`` (root-only) is a SEPARATE knob — Python logging level — and is
intentionally NOT part of this resolution.
"""

from __future__ import annotations

import click


def resolve_verbosity(local: int = 0) -> int:
    """Effective verbosity = max(root ``-v`` count, this subcommand's own).

    Reads ``ctx.obj["verbosity"]`` off the ROOT context (``cli()``, the
    top-level group) via ``click.get_current_context().find_root()`` so any
    consumer — no matter how deeply nested in ``run()``'s call graph — sees
    the same number without a second hand-threaded flag. ``local`` is the
    subcommand's own ``-v`` count (0 when the subcommand declares none, or
    when the user only used the root-level flag).

    Falls back to 0 for the root count when no Click context is active
    (e.g. called from a unit test with no CLI invocation).
    """
    root_verbosity = 0
    try:
        ctx = click.get_current_context()
    except RuntimeError:
        ctx = None
    if ctx is not None:
        root = ctx.find_root()
        obj = root.obj or {}
        root_verbosity = int(obj.get("verbosity", 0) or 0)
    return max(root_verbosity, local)
