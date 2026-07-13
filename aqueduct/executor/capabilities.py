"""Engine capability model — pure data, zero pyspark imports.

Phase 78 — the engine capability framework. Aqueduct's grammar (module types,
Channel ops, Egress modes, feature flags, …) is engine-agnostic by design, but
not every engine implements every grammar leaf. This module is the vocabulary
an engine uses to declare, for each leaf, whether it is supported, unsupported,
or supported-with-a-warning — optionally gated by a dependency version.

**Declarations are DATA, not code.** Each engine ships a YAML capability
declaration next to its package (``aqueduct/executor/spark/capabilities.yml``
for the reference engine) with ONE EXPLICIT ROW PER LEAF — no default-verdict
sweep exists anywhere in this module, deliberately. ``load_declaration()``
hard-validates it at registration: an unknown leaf id, an illegal verdict, a
malformed version specifier, a missing row, or a row still parked on the
``UNDECLARED`` sentinel is a ``CapabilityDeclarationError`` (a build failure),
never a silent pass. A third-party engine author therefore ships reviewable
data, not Python, and cannot register a half-declared engine.

Two error types, two different states — kept apart on purpose (they were once
conflated, and the resulting message told a developer who had just added a
schema key to "reinstall the package", which fixes nothing):

  - ``EnginePluginError`` — the ``aqueduct.engines`` entry point failed to
    IMPORT. The plugin is broken or half-installed; an install-time problem,
    and the message says reinstall.
  - ``CapabilityDeclarationError`` — the declaration is incomplete or invalid.
    A dev-time build failure; the message names the offending leaves and tells
    you to run ``aqueduct dev capabilities sync`` and declare a verdict.

Callers that need to tell them apart branch by TYPE, never by message text.

That verbosity is the whole point. The previous design derived the engine's
table from ``all_leaves_default(walker_leaves, SUPPORTED)`` — the SAME walker
the closure test compared the registry against — so the two sides could never
disagree, every new grammar/config leaf was auto-swept into SUPPORTED, and the
advertised guarantee ("a new schema key without a per-engine verdict fails the
build") was vacuously true. Two independent sources — the walker (code) and
the YAML (data) — are what make the closure test able to fail at all. Keep
them independent; do not reintroduce a default sweep.

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
from pathlib import Path

from aqueduct.errors import CapabilityDeclarationError, EnginePluginError, UnknownEngineError

# The ACTUAL fix for an incomplete/invalid declaration. Note what it is NOT:
# reinstall advice. Reinstalling the package cannot fill in a verdict a human
# has not decided yet — that advice belongs to EnginePluginError (a broken or
# half-installed plugin), and gluing the two together is what made the old
# message useless for the far more common first-party case.
_SYNC_ADVICE = (
    "Run `aqueduct dev capabilities sync` to append every missing leaf to each "
    "engine's capabilities.yml as 'undeclared', then replace each with a real "
    "verdict for that engine. `aqueduct dev capabilities check` reports the "
    "remaining gaps without writing."
)
_SCAFFOLD_ADVICE = (
    "Run `aqueduct dev capabilities scaffold --engine <name>` to generate a "
    "complete declaration (every leaf present, every verdict 'undeclared'), then "
    "replace each row with a real verdict."
)


class Support(StrEnum):
    """Verdict an engine assigns to one grammar leaf.

    ``UNDECLARED`` is a real sentinel, NOT a synonym for ``UNSUPPORTED``.
    "Nobody has decided yet" and "we decided this engine cannot do it" are
    different states, and conflating them is exactly how the capability
    closure guarantee silently became a tautology (see the module docstring's
    anti-drift note). An ``UNDECLARED`` row is a BUILD FAILURE at engine
    registration — it can never reach the compile-time gate or the runtime.
    """

    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    IGNORED_WITH_WARNING = "ignored_with_warning"
    UNDECLARED = "undeclared"


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

        By construction every REAL leaf has an explicit row: ``load_declaration()``
        refuses to register an engine whose YAML omits one or leaves it
        ``UNDECLARED``. So this fallback only fires for an id that is not a real
        capability leaf at all, and it fails closed (UNSUPPORTED) rather than
        waving an unrecognised id through.
        """
        return self.table.get(leaf_id, Capability(support=Support.UNSUPPORTED))


def load_declaration(path: Path | str, leaf_ids: frozenset[str]) -> EngineCapabilities:
    """Load + HARD-VALIDATE one engine's YAML capability declaration.

    This is the forcing function. There is deliberately NO default-verdict
    sweep anywhere in this module: an engine must state a verdict for every
    single leaf, one row each. The previous design (``all_leaves_default(
    walker_leaves, SUPPORTED)``) derived the engine's table from the SAME
    walker the closure test compared it against, so the two sides could never
    disagree and every new grammar/config leaf was auto-swept into SUPPORTED.
    The "adding a schema key without a per-engine verdict is a build failure"
    guarantee was therefore vacuously true. Two independent sources — the
    walker (code) and the YAML (data) — are what make it real.

    Validated, all as ``CapabilityDeclarationError`` (an ``AqueductError``; an
    engine author is a user of this seam, so never a bare builtin):
      - the file parses and has ``engine:`` + ``leaves:``
      - every row's leaf id EXISTS in ``leaf_ids`` (no orphans/typos)
      - every row's verdict is a legal ``Support`` value
      - every ``requires`` specifier is well-formed
      - every leaf in ``leaf_ids`` HAS a row (no silent omission), and no row
        is left at the ``UNDECLARED`` sentinel

    Deliberately NOT ``EnginePluginError``: that means "the entry point failed
    to load — the plugin is broken or half-installed", whose fix is a reinstall.
    An incomplete/invalid declaration is a DEV-time build failure (a developer
    added a schema key; every engine now owes it a verdict) and reinstalling
    fixes nothing. Two different states, two different types — callers branch by
    TYPE, never by matching the message.

    Row shorthand: a bare string is the verdict (``channel.op.filter:
    supported``). A mapping carries the full ``Capability`` (``support:`` plus
    optional ``requires:`` / ``hint:``).
    """
    import yaml

    p = Path(path)
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise CapabilityDeclarationError(
            f"capability declaration {p} could not be read/parsed: "
            f"{type(exc).__name__}: {exc}",
            path=str(p),
        ) from exc

    if not isinstance(raw, dict) or "engine" not in raw or "leaves" not in raw:
        raise CapabilityDeclarationError(
            f"capability declaration {p} must be a mapping with 'engine:' and "
            "'leaves:' keys. " + _SCAFFOLD_ADVICE,
            path=str(p),
        )
    engine = raw["engine"]
    rows = raw["leaves"]
    if not isinstance(rows, dict):
        raise CapabilityDeclarationError(
            f"capability declaration {p}: 'leaves:' must be a mapping.",
            engine=str(engine),
            path=str(p),
        )

    table: dict[str, Capability] = {}
    undeclared: list[str] = []
    for leaf_id, row in rows.items():
        if leaf_id not in leaf_ids:
            raise CapabilityDeclarationError(
                f"capability declaration {p} (engine {engine!r}) declares a verdict for "
                f"{leaf_id!r}, which is not a real capability leaf. It was likely renamed "
                f"or removed from the grammar/config schema. " + _SYNC_ADVICE,
                engine=str(engine),
                path=str(p),
                leaves=[leaf_id],
            )
        if isinstance(row, str):
            row = {"support": row}
        if not isinstance(row, dict) or "support" not in row:
            raise CapabilityDeclarationError(
                f"capability declaration {p}: leaf {leaf_id!r} must be a verdict string or "
                "a mapping with a 'support:' key.",
                engine=str(engine),
                path=str(p),
                leaves=[leaf_id],
            )
        try:
            support = Support(row["support"])
        except ValueError:
            raise CapabilityDeclarationError(
                f"capability declaration {p}: leaf {leaf_id!r} has invalid verdict "
                f"{row['support']!r}. Legal verdicts: {[s.value for s in Support]}.",
                engine=str(engine),
                path=str(p),
                leaves=[leaf_id],
            ) from None
        if support is Support.UNDECLARED:
            undeclared.append(leaf_id)
            continue
        try:
            table[leaf_id] = Capability(
                support=support,
                requires=row.get("requires"),
                hint=row.get("hint"),
            )
        except ValueError as exc:  # malformed `requires` specifier
            raise CapabilityDeclarationError(
                f"capability declaration {p}: leaf {leaf_id!r}: {exc}",
                engine=str(engine),
                path=str(p),
                leaves=[leaf_id],
            ) from exc

    missing = sorted(leaf_ids - set(rows))
    if missing or undeclared:
        parts = []
        if missing:
            parts.append(f"{len(missing)} leaf/leaves have NO row: {missing[:10]}")
        if undeclared:
            parts.append(
                f"{len(undeclared)} leaf/leaves are still UNDECLARED: {sorted(undeclared)[:10]}"
            )
        raise CapabilityDeclarationError(
            f"capability declaration {p} (engine {engine!r}) is incomplete — "
            + "; ".join(parts)
            + ". Every capability leaf needs an explicit per-engine verdict "
            "(supported | unsupported | ignored_with_warning). " + _SYNC_ADVICE,
            engine=str(engine),
            path=str(p),
            leaves=sorted(set(missing) | set(undeclared)),
        )

    return EngineCapabilities(engine=engine, table=table)


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
        CapabilityDeclarationError: an engine module imported fine but its
            capability declaration is incomplete/invalid. Propagates UNCHANGED
            (see the re-raise below) — re-wrapping it as an EnginePluginError
            would bury the leaf names under "reinstall the package", which is
            exactly the conflation this split exists to undo.
    """
    global _engines_loaded
    if _engines_loaded:
        return
    import importlib.metadata

    for ep in importlib.metadata.entry_points(group=AQ_ENGINES_ENTRYPOINT_GROUP):
        try:
            ep.load()
        except CapabilityDeclarationError:
            # The module IMPORTED — it just declared its capabilities badly.
            # That is a dev-time build failure with its own type and its own
            # fix (`aqueduct dev capabilities sync`, then declare a verdict).
            # Re-raise it unchanged: wrapping it in the EnginePluginError below
            # would replace the offending leaf names with reinstall advice that
            # cannot possibly fix it.
            raise
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
