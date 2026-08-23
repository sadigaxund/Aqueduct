"""PEP 508-lite requirement parsing and compile-time preflight (Phase 88).

Backs the Blueprint top-level ``dependencies:`` block — a flat list of PEP
508 requirement strings (e.g. ``holidays>=0.40``) the Blueprint author
declares the runtime environment must already satisfy. Aqueduct NEVER
installs packages; this module only *checks*. It is deliberately NOT
engine-scoped and carries no capability leaf — see
``aqueduct/parser/schema.py::BlueprintSchema.dependencies`` and
``aqueduct/compiler/compiler.py``'s preflight call for the two places that
actually wire this in.

**Layering note.** This is a top-level, core module (no pyspark import, no
new third-party dependency). It reuses the hand-rolled PEP 440-lite
comparator (``validate_specifier`` / ``version_satisfies``) already living
in ``aqueduct/executor/capabilities.py`` rather than importing the
``packaging`` library, which is not a declared dependency of aqueduct-core.
Importing an ``aqueduct.executor.*`` module from a core module has
precedent: ``aqueduct/parser/parser.py`` already imports
``aqueduct/executor/path_keys.py`` for the same reason (an engine-agnostic
helper that happens to live under ``executor/``).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from importlib import metadata as _importlib_metadata

from aqueduct.executor.capabilities import validate_specifier, version_satisfies

logger = logging.getLogger(__name__)

# name [ extras ] specifier
# e.g. "holidays", "holidays[extra1,extra2]", "holidays>=0.40",
#      "holidays[x]>=1.2,<2"
_NAME_RE = r"[A-Za-z0-9][A-Za-z0-9._-]*"
_REQ_RE = re.compile(
    rf"^(?P<name>{_NAME_RE})" rf"(?:\[(?P<extras>[^\]]*)\])?" rf"(?P<specifier>.*)$"
)

# PEP 503 name normalization: runs of -_. collapse to a single "-", lowercased.
_NORMALIZE_RE = re.compile(r"[-_.]+")

# Mirrors the pre-release/local-version stripping `version_satisfies` applies
# internally (aqueduct/executor/capabilities.py) — used here only to detect
# whether the INSTALLED version string is one that comparator can actually
# read, never to do the comparison itself.
_PARSEABLE_VERSION_RE = re.compile(r"[0-9]+(?:\.[0-9]+)*")


def _strip_version_suffix(v: str) -> str:
    return v.split("+")[0].split("rc")[0].split("a")[0].split("b")[0]


def normalize_name(name: str) -> str:
    """Normalize a distribution name per PEP 503 for lookup purposes."""
    return _NORMALIZE_RE.sub("-", name).lower()


@dataclass(frozen=True)
class Requirement:
    """One parsed PEP 508-lite requirement.

    ``raw`` is kept verbatim (for error messages / the pip command);
    ``name`` is PEP 503-normalized for ``importlib.metadata`` lookup.
    ``specifier`` is the raw comma-separated clause string (may be ``""``
    when the requirement carries no version constraint).
    """

    raw: str
    name: str
    extras: tuple[str, ...]
    specifier: str


def parse_requirement(s: str) -> Requirement:
    """Parse a PEP 508-lite requirement string.

    Accepts ``name``, ``name[extra1,extra2]``, ``name>=1.2``,
    ``name[x]>=1.2,<2``. Environment markers (``; python_version < "3.12"``)
    are explicitly rejected rather than silently ignored — Aqueduct does not
    do silent no-ops.

    Raises:
        ValueError: ``s`` is empty, carries an environment marker, or does
            not match the accepted grammar (including a malformed version
            specifier).
    """
    raw = s
    text = s.strip()
    if not text:
        raise ValueError(f"empty dependency requirement: {raw!r}")

    if ";" in text:
        raise ValueError(
            f"environment markers are not supported in dependency requirements: {raw!r}"
        )

    m = _REQ_RE.match(text)
    if not m:
        raise ValueError(f"malformed dependency requirement: {raw!r}")

    name = m.group("name")
    extras_raw = m.group("extras")
    extras = tuple(e.strip() for e in extras_raw.split(",")) if extras_raw else ()
    if extras_raw is not None and any(not e for e in extras):
        raise ValueError(f"malformed extras in dependency requirement: {raw!r}")

    specifier = (m.group("specifier") or "").strip()
    if specifier and not validate_specifier(specifier):
        raise ValueError(f"malformed version specifier in dependency requirement: {raw!r}")

    return Requirement(
        raw=raw,
        name=normalize_name(name),
        extras=extras,
        specifier=specifier,
    )


def requirement_status(req: Requirement) -> tuple[str, str | None]:
    """Check ``req`` against the installed environment.

    Returns a ``(status, installed_version)`` pair where ``status`` is one
    of ``"satisfied"``, ``"missing"``, ``"version_conflict"``, or
    ``"unknown_version"``.

    ``unknown_version`` covers the case where the package IS installed but
    its version string (or the requirement's specifier) cannot be evaluated
    by the PEP 440-lite comparator (e.g. ``1.2.3.post1``, ``2024.1a0``).
    This is DELIBERATELY treated as passing by ``check_requirements`` below
    — the whole point of the preflight is to catch requirements that are
    definitely unsatisfied, not to reject installs the comparator merely
    doesn't understand. A real false-negative here (package genuinely too
    old) still fails loudly at import time, same as before this feature
    existed.
    """
    try:
        installed = _importlib_metadata.version(req.name)
    except _importlib_metadata.PackageNotFoundError:
        return "missing", None

    if not req.specifier:
        return "satisfied", installed

    # `version_satisfies` swallows an unparseable INSTALLED version internally
    # (returns False, does not raise — see its own docstring/implementation
    # in aqueduct/executor/capabilities.py), so a genuinely-too-old package
    # and a package whose version string this PEP440-lite comparator simply
    # can't read would otherwise look identical from here. Pre-check the
    # installed version's shape ourselves so the two cases stay distinct.
    if not _PARSEABLE_VERSION_RE.fullmatch(_strip_version_suffix(installed)):
        logger.debug(
            "dependency preflight: installed version %r for %r is not "
            "parseable by the PEP440-lite comparator — treating %r as "
            "satisfied (unknown_version)",
            installed,
            req.name,
            req.raw,
        )
        return "unknown_version", installed

    try:
        ok = version_satisfies(installed, req.specifier)
    except ValueError:
        logger.debug(
            "dependency preflight: malformed specifier %r for %r — "
            "treating as satisfied (unknown_version)",
            req.specifier,
            req.raw,
        )
        return "unknown_version", installed

    return ("satisfied" if ok else "version_conflict"), installed


def check_requirements(reqs: Iterable[str]) -> list[str]:
    """Check each requirement string against the installed environment.

    Returns human-readable problem lines (one per ``missing`` or
    ``version_conflict`` requirement) — empty when every requirement is
    satisfied (or ``unknown_version``, which passes silently by design).

    Raises:
        ValueError: a requirement string is malformed (should not happen
            for a Blueprint that already passed schema validation, since
            ``BlueprintSchema`` parses every entry at parse time).
    """
    problems: list[str] = []
    for raw in reqs:
        req = parse_requirement(raw)
        status, installed = requirement_status(req)
        if status == "missing":
            problems.append(f"{req.raw}  (not installed)")
        elif status == "version_conflict":
            problems.append(f"{req.raw}  (installed: {installed})")
    return problems
