"""Declarative path-key registry per module type (Phase 36 Part B).

Replaces the hand-maintained ``("path", "data_dir", "input_dir",
"output_dir", "jar")`` tuple in ``aqueduct/parser/parser.py``. When
adding a new module type with path-typed free-form config keys, declare
them here — not in the parser.

**Engine-agnostic by design.** Parser imports this module; this module
imports nothing from ``aqueduct.executor.spark.*`` (and never will), so
the 4-layer boundary (Parser → Compiler → Executor → Surveyor) stays
clean. Parser does not need to import any Spark handler module to learn
which keys to anchor.

**Decision (rejected alternative).** A register-on-import design
(``register_path_keys()`` called inside each executor handler module)
was considered and rejected: it requires parser to import every
executor handler at parse time just to trigger the side effect,
breaking the layer rule; test fixtures that mock executors don't
trigger registration; cold start order matters. A single declarative
table is more discoverable (`grep "Ingress"` finds the spec
immediately), guarantees registration happens, and is engine-agnostic.

Adding a path key for a new executor module type is a one-edit
operation: extend the relevant tuple here, or add a new key.
"""

from __future__ import annotations


# Strict per-type tuples. Each entry lists the free-form config keys
# whose string values are filesystem paths and must be anchored to
# ``base_dir`` during ``parse_dict`` resolution.
#
# Unknown module types fall through to ``_LEGACY_FALLBACK`` for backward
# compatibility — pre-Phase-36 parser anchored a single blanket tuple for
# every type. The fallback unblocks adoption; the long-term goal is a
# row per module type and an empty fallback. Audited types: Ingress,
# Egress, UDF. Pending audit: Arcade (when streaming lands), Probe (custom
# probe ``fn:`` paths), Channel (none today).
_PATH_KEYS: dict[str, tuple[str, ...]] = {
    "Ingress": ("path", "data_dir", "input_dir", "jar"),
    "Egress":  ("path", "output_dir", "jar"),
    "UDF":     ("jar",),
}

# Module types not in ``_PATH_KEYS`` fall back to this union — preserves
# the pre-Phase-36 blanket behaviour. Shrink to ``()`` once every executor
# module type has an explicit entry above.
_LEGACY_FALLBACK: tuple[str, ...] = (
    "path", "data_dir", "input_dir", "output_dir", "jar",
)


def get_path_keys(module_type: str) -> tuple[str, ...]:
    """Return the tuple of free-form config keys to anchor for a module type.

    Look up by type name (e.g. ``"Ingress"``, ``"Channel"``,
    ``"Arcade"``). Returns the legacy fallback tuple for unregistered
    types — never raises. Empty tuple means "no path keys for this type".
    """
    return _PATH_KEYS.get(module_type, _LEGACY_FALLBACK)


__all__ = ["get_path_keys"]
