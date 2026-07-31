"""Spark engine capability declaration (Phase 78) — loader only.

The declaration itself is DATA: ``capabilities.yml`` next to this file, one
explicit row per capability leaf (206 today — 189 Blueprint-grammar leaves +
17 ENGINE-SCOPED config leaves; Q4 step 2 narrowed the config checklist from
105 to the leaves that actually dispatch through an engine — see
``aqueduct/executor/config_leaves.py`` and ``docs/specs.md`` §10.9. The other
~88 config leaves are CORE — no engine declares a verdict for them at all).
This module only loads and registers it. It makes
NO changes to any Spark executor module and must stay importable without
``pyspark`` installed (it is read by the compile-time capability gate, which
runs before any Spark session exists, and by the closure test).

**There is no default-verdict sweep, deliberately.** Spark used to declare
``all_leaves_default(all_leaves(), Support.SUPPORTED)`` — derived from the
SAME walker the closure test compared it against. Two sides, one source: they
could never disagree, every new grammar/config leaf was auto-swept into
SUPPORTED, and the advertised guarantee ("a new schema key without a
per-engine verdict fails the build") was a tautology that could not fail.
Now the walker (code) and ``capabilities.yml`` (data) are independent, and
``load_declaration()`` hard-fails registration on any leaf that has no row or
is still parked on the ``UNDECLARED`` sentinel. Spark saying "I support these
206 things" explicitly is the point; "Spark supports everything by
assumption" is what was broken.

Adding a leaf: the build breaks, ``aqueduct dev capabilities sync``
appends it to every engine's YAML as ``undeclared``, and a human replaces
that with a real verdict. Do not reintroduce a default sweep to make the
break go away — the break IS the feature.

Version-gated leaves (``requires:`` in the YAML — ``format: custom`` needing
pyspark>=4.0, the Delta-backed leaves needing delta-spark) are mined from
currently-documented claims (``docs/compatibility.md``, ``session.py``,
``udf.py``); the data file invents no new version claims. Compile cannot check
them (it does not know the runtime environment) — that is ``aqueduct doctor``'s
job.
"""

from __future__ import annotations

from pathlib import Path

from aqueduct.executor.capabilities import (
    CAPABILITY_REGISTRY,  # noqa: F401 — re-exported for convenience
    load_declaration,
    register,
)
from aqueduct.executor.capability_leaves import all_leaves
from aqueduct.executor.config_leaves import all_config_leaves

DECLARATION_PATH = Path(__file__).with_name("capabilities.yml")

# The full governed leaf set: Blueprint grammar ∪ Spark's own engine-scoped
# config leaves (aqueduct.yml). Passed to load_declaration() purely as the
# CHECKLIST the YAML is validated against — never as a source of default
# verdicts. `all_config_leaves(engine="spark")` (Q4 step 2) excludes core
# config leaves entirely (webhooks/secrets/stores/most agent/danger — no
# engine declares those) and excludes every OTHER engine's `engine.<name>.*`
# leaves (positionally not Spark's to declare).
_ALL_LEAVES = all_leaves() | all_config_leaves(engine="spark")

SPARK = load_declaration(DECLARATION_PATH, _ALL_LEAVES)

register(SPARK)

__all__ = ["DECLARATION_PATH", "SPARK"]
