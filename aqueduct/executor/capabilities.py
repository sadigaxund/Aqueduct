"""Engine capability model — pure data, zero pyspark imports.

Phase 78 — the engine capability framework. Aqueduct's grammar (module types,
Channel ops, Egress modes, feature flags, …) is engine-agnostic by design, but
not every engine implements every grammar leaf. This module is the vocabulary
an engine uses to declare, for each leaf, whether it is supported, unsupported,
or supported-with-a-warning — optionally gated by a dependency version.

Consumers:
  - ``aqueduct/compiler/capability_check.py`` — compile-time gate: UNSUPPORTED
    leaves become a ``CompileError``; ``IGNORED_WITH_WARNING`` leaves become a
    suppressible compiler warning (``engine_key_ignored``).
  - ``aqueduct/doctor/`` — runtime gate: version-constrained ``SUPPORTED``
    leaves are checked against the actually-installed dependency version.
  - ``tests/test_capabilities/test_closure.py`` — anti-drift gate: every leaf
    the grammar walker derives must have an explicit verdict in every
    registered engine's table, and vice versa.

This module must stay importable without ``pyspark`` (or any other engine's
runtime deps) installed — it is pure declaration.

**Engine registration seam.** Nothing in this module (or anywhere on the
compile/config/doctor path) imports a specific engine's package by name. An
engine registers itself by declaring an ``aqueduct.engines`` setuptools
entry point (see ``pyproject.toml``'s ``[project.entry-points."aqueduct.engines"]``
table and ``aqueduct/executor/spark/engine.py`` for the reference
implementation). ``load_engines()`` resolves and imports every entry point in
that group exactly once per process; each engine module registers its
``EngineCapabilities`` via ``register()`` as an import side effect (mirroring
how ``aqueduct/executor/spark/capabilities.py`` already does it).
``get_capabilities()`` calls ``load_engines()`` before its registry lookup, so
the registry is always populated on the real compile/doctor/config path with
no explicit per-engine import anywhere in core. A future engine (e.g.
DuckDB) needs zero edits to this file, ``aqueduct/compiler/``, or
``aqueduct/config.py`` — it ships its own entry point and is automatically in
scope.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

from aqueduct.errors import EnginePluginError, UnknownEngineError


class Support(StrEnum):
    """Verdict an engine assigns to one grammar leaf."""

    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    IGNORED_WITH_WARNING = "ignored_with_warning"


# Minimal PEP 440-lite specifier grammar: one or more comma-separated clauses,
# each `<op><dotted-version>` with op in {==, !=, >=, <=, >, <, ~=}. This is
# NOT a full PEP 440 parser (no pre-releases, no wildcards, no epochs) — it is
# deliberately just enough to express the version floors/ceilings this engine
# actually declares (e.g. ">=4.0", ">=4.0,<5.0"). `packaging` is not a
# declared dependency of aqueduct-core (pyproject.toml `dependencies`), so we
# do not import `packaging.specifiers` even though it happens to be present
# in this dev environment — a fresh `pip install aqueduct-core` would not
# have it.
_CLAUSE_RE = re.compile(r"^(==|!=|>=|<=|>|<|~=)\s*([0-9]+(?:\.[0-9]+)*)$")


def validate_specifier(spec: str) -> bool:
    """Return True if ``spec`` is a well-formed comma-separated clause list.

    Empty/whitespace-only clauses are rejected. Does not evaluate versions —
    see ``version_satisfies`` for that.
    """
    if not spec or not spec.strip():
        return False
    for clause in spec.split(","):
        if not _CLAUSE_RE.match(clause.strip()):
            return False
    return True


def _version_tuple(v: str) -> tuple[int, ...]:
    return tuple(int(p) for p in v.split("."))


def _cmp(a: tuple[int, ...], b: tuple[int, ...]) -> int:
    """Compare two version tuples, right-padding the shorter with zeros."""
    n = max(len(a), len(b))
    a = a + (0,) * (n - len(a))
    b = b + (0,) * (n - len(b))
    return (a > b) - (a < b)


def version_satisfies(installed: str, specifier: str) -> bool:
    """Check ``installed`` version string against a comma-separated specifier.

    Raises:
        ValueError: ``specifier`` is not well-formed (see ``validate_specifier``).
    """
    if not validate_specifier(specifier):
        raise ValueError(f"malformed version specifier: {specifier!r}")
    try:
        inst = _version_tuple(installed.split("+")[0].split("rc")[0].split("a")[0].split("b")[0])
    except ValueError:
        # Unparseable installed version string — cannot prove satisfaction.
        return False
    for clause in specifier.split(","):
        m = _CLAUSE_RE.match(clause.strip())
        assert m is not None  # validated above
        op, ver = m.group(1), _version_tuple(m.group(2))
        c = _cmp(inst, ver)
        ok = {
            "==": c == 0,
            "!=": c != 0,
            ">=": c >= 0,
            "<=": c <= 0,
            ">": c > 0,
            "<": c < 0,
            "~=": inst[: len(ver) - 1] == ver[: len(ver) - 1] and c >= 0,
        }[op]
        if not ok:
            return False
    return True


@dataclass(frozen=True)
class Capability:
    """One engine's verdict for one grammar leaf."""

    support: Support
    # Dependency name -> PEP440-lite specifier string, e.g. {"pyspark": ">=4.0"}.
    # Only meaningful when support == SUPPORTED (a leaf that IS supported but
    # only above a certain dependency version). Compile-time cannot check
    # this (it doesn't know the runtime environment) — that is doctor's job.
    requires: dict[str, str] | None = None
    # Shown in the compile error / warning — what to do instead.
    hint: str | None = None

    def __post_init__(self) -> None:
        if self.requires:
            for dep, spec in self.requires.items():
                if not validate_specifier(spec):
                    raise ValueError(
                        f"Capability.requires[{dep!r}] = {spec!r} is not a "
                        "well-formed version specifier"
                    )


@dataclass(frozen=True)
class EngineCapabilities:
    """One engine's full capability declaration — a leaf-id -> Capability table."""

    engine: str
    table: dict[str, Capability] = field(default_factory=dict)

    def verdict(self, leaf_id: str) -> Capability:
        """Return this engine's Capability for ``leaf_id``.

        Leaves with no explicit entry default to UNSUPPORTED — an engine must
        opt IN to supporting a leaf (or opt in wholesale via
        ``all_leaves_default(Support.SUPPORTED)``). This is the default-deny
        posture every future engine starts from; Spark overrides it with
        default-ALLOW because it implements (almost) the entire grammar today.
        """
        return self.table.get(leaf_id, Capability(support=Support.UNSUPPORTED))


def all_leaves_default(leaf_ids: frozenset[str], default: Support) -> dict[str, Capability]:
    """Build a leaf-id -> Capability table with every leaf set to ``default``.

    Lets an engine declare its baseline posture in one line:
      - Spark: ``all_leaves_default(ALL_LEAVES, Support.SUPPORTED)`` then
        layer targeted UNSUPPORTED/version-gated overrides on top.
      - A future engine: ``all_leaves_default(ALL_LEAVES, Support.UNSUPPORTED)``
        then layer targeted SUPPORTED overrides on top as it earns them.
    """
    return {leaf_id: Capability(support=default) for leaf_id in leaf_ids}


CAPABILITY_REGISTRY: dict[str, EngineCapabilities] = {}

# setuptools entry-point group engines register themselves under. Same
# precedent as AQ_PROBE_ENTRYPOINT_GROUP (executor/probe_plugins.py) and
# AQ_TOOLS_ENTRYPOINT_GROUP (tools/registry.py).
AQ_ENGINES_ENTRYPOINT_GROUP = "aqueduct.engines"

_engines_loaded = False


def register(caps: EngineCapabilities) -> None:
    """Register (or replace) an engine's capability table."""
    CAPABILITY_REGISTRY[caps.engine] = caps


# Actionable message for the empty-registry state. NOT the same diagnosis as a
# misspelled engine name: an empty registry means aqueduct's OWN entry points
# are invisible to importlib.metadata, which in practice means a stale editable
# install (the dist-info predates the entry-point declaration). Because engine
# validation is fail-closed, that state would otherwise hard-fail every
# aqueduct.yml load with an alarming "Registered engines: []".
NO_ENGINES_HINT = (
    "no execution engines are registered at all — aqueduct's "
    f"{AQ_ENGINES_ENTRYPOINT_GROUP!r} entry points are not visible to "
    "importlib.metadata. This is almost always a stale install whose metadata "
    "predates the entry-point declaration: reinstall the package "
    "(`pip install -e .` for a source checkout, `pip install --force-reinstall "
    "aqueduct-core` otherwise) and retry."
)


def load_engines() -> None:
    """Load every ``aqueduct.engines`` entry point, populating ``CAPABILITY_REGISTRY``.

    Idempotent and cached at module scope — the first call resolves and
    imports each registered engine module (each registers itself via
    ``register()`` as an import side effect, e.g.
    ``aqueduct/executor/spark/engine.py``); every subsequent call is a no-op.
    Safe to call from any layer (compiler, doctor, config) with no engine
    named explicitly and no ``pyspark`` (or other engine runtime dep)
    required — entry-point modules are themselves declaration-only, deferring
    their actual executor import until called (see ``spark/engine.py``'s
    ``load_executor()``).

    Raises:
        EnginePluginError: an entry point in the group failed to import. A
            broken or half-installed third-party engine plugin surfaces as a
            clean Aqueduct error naming the failing entry point and its cause,
            never as a bare ImportError escaping out of `aqueduct.yml` loading.
    """
    global _engines_loaded
    if _engines_loaded:
        return
    import importlib.metadata

    for ep in importlib.metadata.entry_points(group=AQ_ENGINES_ENTRYPOINT_GROUP):
        try:
            ep.load()
        except Exception as exc:
            # Deliberately broad: an engine plugin is third-party code and can
            # fail to import for any reason (missing transitive dep, syntax
            # error, side-effect crash). Every one of those is user-reachable
            # (it fires during `aqueduct.yml` load), so all of them must become
            # an AqueductError naming the culprit rather than an opaque
            # ImportError/AttributeError from deep inside a plugin.
            raise EnginePluginError(
                f"engine plugin {ep.name!r} (entry point {ep.value!r} in group "
                f"{AQ_ENGINES_ENTRYPOINT_GROUP!r}) failed to load: "
                f"{type(exc).__name__}: {exc}. Reinstall or uninstall the package "
                "providing it."
            ) from exc
    _engines_loaded = True


def get_capabilities(engine: str) -> EngineCapabilities:
    """Look up a registered engine's capability table.

    Calls ``load_engines()`` first, so this resolves correctly even when no
    engine module has been explicitly imported anywhere on the call path —
    the entry-point group is the only registration seam.

    Raises:
        UnknownEngineError: no engine registered under that name. A
            ``CompileError`` subclass, so existing ``except CompileError``
            callers keep working; a distinct type so callers that must tell an
            unregistered engine apart from a failed compile can do it by type.
            When NOTHING is registered (a stale install, see ``NO_ENGINES_HINT``)
            the message says so instead of reporting an empty engine list.
        EnginePluginError: an engine plugin's entry point failed to import.
    """
    load_engines()
    if engine not in CAPABILITY_REGISTRY:
        registered = sorted(CAPABILITY_REGISTRY)
        if not registered:
            raise UnknownEngineError(
                f"cannot validate engine {engine!r}: {NO_ENGINES_HINT}",
                engine=engine,
                engines=registered,
            )
        raise UnknownEngineError(
            f"unknown engine {engine!r} — no capability declaration registered. "
            f"Registered engines: {registered}",
            engine=engine,
            engines=registered,
        )
    return CAPABILITY_REGISTRY[engine]
