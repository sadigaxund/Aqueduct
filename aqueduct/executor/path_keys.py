"""Declarative path-key registry per module type.

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
# EVERY module type has an explicit row (the long-term goal this module's
# docstring used to defer) — the Pass C per-module-type schema audit
# (2026-08) verified, for every one of the 9 authorable types, exactly
# which config keys any executor actually reads. Result: only Ingress and
# Egress `path` are real filesystem-path keys; `jar` is real only on
# `UdfSchema` (java/scala UDF loading), never on Ingress/Egress — grepping
# `executor/spark/ingress.py`/`egress.py` and their DuckDB counterparts for
# `"jar"` returns zero hits. `data_dir`/`input_dir`/`output_dir` were dead
# entries too (carried since an early parser revision; no executor for any
# module type ever read them — confirmed by the same audit). Channel,
# Junction, Funnel, Probe, Regulator, Assert have no path-typed config
# keys at all. Arcade's `ref` IS a filesystem path but is resolved by the
# compiler's expander (`aqueduct/compiler/expander.py`), not this
# mechanism — it is a module-top-level field, not a `config:` entry, so it
# is out of `_anchor_paths`' scope (which only ever walks `config:`).
#
# The "UDF" row is NOT a module type (there is no `ModuleType.UDF`) — it
# keys `udf_registry:` entries (top-level list, not a `config:` block),
# anchored by parser.py's own `udf_registry=` dump step, which looks up
# this row by literal string "UDF" rather than by iterating module types.
# Both `jar` and `path` are listed because `UdfSchema` accepts either as
# the JAR location ("java / scala: load `class` from `jar` (or `path`)");
# either one is a relative path a `_register_java_udf` caller resolves
# against the CWD unless anchored here first.
_PATH_KEYS: dict[str, tuple[str, ...]] = {
    "Ingress":  ("path",),
    "Channel":  (),
    "Egress":   ("path",),
    "Junction": (),
    "Funnel":   (),
    "Probe":    (),
    "Regulator": (),
    "Arcade":   (),
    "Assert":   (),
    "UDF":      ("jar", "path"),
}

# Module types not in ``_PATH_KEYS`` fall back to this union. Empty now
# that every module type has an explicit row above (Pass C, 2026-08) — kept
# only as a structural safety net for a type this table has not yet been
# extended to cover, never a source of real anchoring behaviour.
_LEGACY_FALLBACK: tuple[str, ...] = ()


def get_path_keys(module_type: str) -> tuple[str, ...]:
    """Return the tuple of free-form config keys to anchor for a module type.

    Look up by type name (e.g. ``"Ingress"``, ``"Channel"``,
    ``"Arcade"``). Returns the legacy fallback tuple for unregistered
    types — never raises. Empty tuple means "no path keys for this type".
    """
    return _PATH_KEYS.get(module_type, _LEGACY_FALLBACK)


# Ingress formats that locate data via options (url / dbtable / topic / …),
# not a file path.  Used by both the compiler (inputs_fingerprint, to avoid
# stat() on a non‑path value) and the executor (path‑required validation).
PATHLESS_INGRESS_FORMATS: frozenset[str] = frozenset({"jdbc", "kafka", "depot"})

# Cloud storage URI schemes used by compiler and executor to skip
# filesystem-based validation for remote paths. Engine-agnostic tuple
# (no pyspark dependency).
CLOUD_SCHEMES: tuple[str, ...] = (
    "s3://", "s3a://", "s3n://", "hdfs://", "gs://",
    "abfs://", "wasbs://", "wasb://", "dbfs://",
)

__all__ = ["get_path_keys", "PATHLESS_INGRESS_FORMATS", "CLOUD_SCHEMES"]
