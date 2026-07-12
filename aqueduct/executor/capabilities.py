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
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum


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


def register(caps: EngineCapabilities) -> None:
    """Register (or replace) an engine's capability table."""
    CAPABILITY_REGISTRY[caps.engine] = caps


def get_capabilities(engine: str) -> EngineCapabilities:
    """Look up a registered engine's capability table.

    Raises:
        KeyError: no engine registered under that name.
    """
    if engine not in CAPABILITY_REGISTRY:
        raise KeyError(
            f"no capability declaration registered for engine {engine!r}. "
            f"Registered: {sorted(CAPABILITY_REGISTRY)}"
        )
    return CAPABILITY_REGISTRY[engine]
