"""Q4 step 2 — the build-enforceable invariant that replaces a committed
core-leaf snapshot (dropped 2026-07-31, see TODOs.md's "LOCKED" block and
AGENTS.md's config-leaf-scoping enforcement table beside "Never make the
break go away with a default").

THE INVARIANT: any ``config.*`` leaf that ANY registered engine declares
non-``supported`` (``unsupported`` or ``ignored_with_warning``) must be
engine-scoped, permanently — i.e. it must NEVER appear in
``core_config_leaves()``.

WHY THIS IS THE LOAD-BEARING CHECK (not a nice-to-have): a leaf some engine
declares non-``supported`` has a live user-visible warning path
(``aqueduct/config.py::_warn_ignored_config_keys`` emits ``engine_key_ignored``
for any explicitly-set leaf whose verdict isn't ``SUPPORTED``). Reclassifying
such a leaf to core would SILENTLY DELETE that warning path — the leaf would
leave the checklist entirely, so no engine ever gets asked about it again and
the warning can never fire, with no error and no other guard noticing. The
enforcement table (AGENTS.md) walks every OTHER way someone could get config
scoping wrong and shows each is caught by something else already:

  | someone does this                                    | caught by |
  |-------------------------------------------------------|-----------|
  | untags a key some engine declared non-supported        | THIS TEST |
  | untags a key but leaves the rows behind                | existing orphaned-row check (`dev capabilities check`) |
  | untags a key every engine declared `supported`         | nothing needed — a `supported` verdict has no warning path to delete |
  | adds a config field, forgets the tag under engine.<name>.* | the walker raises CapabilityScopeError |
  | tags a new field                                       | new `undeclared` rows (existing closure test) |

Row 3 is why no snapshot file is needed: the ONLY keys whose reclassification
can destroy user-visible behavior are exactly the keys this test forbids
reclassifying.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from aqueduct.executor.config_leaves import core_config_leaves

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_DECLARATIONS = sorted((_REPO / "aqueduct" / "executor").glob("*/capabilities.yml"))


def _verdict_of(row: object) -> str:
    return row if isinstance(row, str) else (row or {}).get("support", "")


def _non_supported_leaves_across_all_engines() -> dict[str, list[str]]:
    """``{leaf_id: [engine names that declare it non-supported]}`` — read
    straight off disk (not through the registry), same independence
    guarantee as the closure test in ``test_closure.py``."""
    offenders: dict[str, list[str]] = {}
    for path in _DECLARATIONS:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        engine = str(raw.get("engine") or path.parent.name)
        rows = raw.get("leaves") or {}
        for leaf_id, row in rows.items():
            if not leaf_id.startswith("config."):
                continue
            verdict = _verdict_of(row)
            if verdict in ("unsupported", "ignored_with_warning"):
                offenders.setdefault(leaf_id, []).append(engine)
    return offenders


def test_declarations_discovered():
    """Guard the discovery glob itself — if it silently matched nothing, the
    invariant below would vacuously pass with zero cases."""
    assert _DECLARATIONS, "no engine capabilities.yml found — this test would be vacuous"


def test_core_leaves_never_declared_non_supported_by_any_engine():
    """THE INVARIANT. core_config_leaves() ∩ {leaves any engine declares
    non-supported} must be empty.

    A red run here NAMES the offending leaf and the engine(s) that declare
    it non-supported — see ``test_invariant_is_falsifiable`` below for proof
    this assertion can actually fail (AGENTS.md's unfalsifiable-meta-test
    rule): a green run against the healthy repo alone proves nothing.
    """
    core = core_config_leaves()
    offenders = _non_supported_leaves_across_all_engines()
    violations = {leaf: engines for leaf, engines in offenders.items() if leaf in core}
    assert not violations, (
        "core_config_leaves() contains leaves that some engine declares "
        f"non-supported — reclassifying these to core would silently delete a "
        f"live engine_key_ignored warning path: {violations}. Tag the field(s) "
        "engine_scoped: True in aqueduct/config.py."
    )


def test_invariant_is_falsifiable(monkeypatch):
    """AGENTS.md's 'a meta-test that guards CI can be unfalsifiable' rule,
    applied programmatically (the manual proof — untagging
    ``config.probes.max_sample_rows`` by hand, rerunning, confirming RED and
    that the message names the leaf, then restoring — is run once and its
    real output is quoted in the phase report; this is the automated,
    permanent regression guard for the same property).

    Simulates exactly what a human untagging an already-engine-scoped field
    does: the leaf disappears from ``core_config_leaves()``'s complement
    (i.e. it now incorrectly LOOKS core) while ITS ROW ON DISK survives
    untouched (DuckDB still says ``ignored_with_warning`` for it, as it does
    today) — the invariant must catch exactly this shape.
    """
    import aqueduct.executor.config_leaves as cfgl

    offenders = _non_supported_leaves_across_all_engines()
    assert offenders, "fixture precondition: at least one engine must declare " \
        "some config leaf non-supported today (duckdb does, e.g. probes.*)"
    victim = sorted(offenders)[0]

    real_core = cfgl.core_config_leaves()
    monkeypatch.setattr(cfgl, "core_config_leaves", lambda: real_core | {victim})

    core = cfgl.core_config_leaves()
    violations = {leaf: engines for leaf, engines in offenders.items() if leaf in core}
    assert violations, "the invariant failed to go red on a planted violation — decorative guard"
    assert victim in violations, "the failure did not name the untagged leaf"
