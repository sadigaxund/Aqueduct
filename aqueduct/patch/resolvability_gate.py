"""Phase 88 — Gate 4: resolvability of declared dependencies.

Sits next to the lineage and sandbox gates in the gate pyramid
(``aqueduct/cli/__init__.py::_run_patch_gates_inline``, ``aqueduct/cli/
patch.py::patch_preview``). A patch that carries one or more
``declare_dependency`` ops (``aqueduct/patch/grammar.py``) declares that the
runtime environment must satisfy a PEP 508-lite requirement — this gate asks
whether that requirement is at least *resolvable* (installed already, or
resolvable from PyPI) before letting the patch auto-apply.

Aqueduct never installs packages (see ``aqueduct/dependencies.py``'s module
docstring) — this gate only CHECKS, exactly like the compile-time preflight
it sits beside. The four checkable verdicts:

  ``NOT_APPLICABLE``  the patch carries no ``declare_dependency`` op — no
                       check was owed.
  ``PASS``             the requirement is already installed
                       (``aqueduct.dependencies.requirement_status`` →
                       ``satisfied`` or ``unknown_version``) — auto-apply
                       eligible.
  ``WARN``              not installed, but the package+version resolves on
                       PyPI — a deliberate DEFER to a human: install it,
                       then re-run ``aqueduct patch apply``. Never
                       auto-applied.
  ``FAIL``              no such package on PyPI, or no published version
                       satisfies the specifier — rejection, feeds the
                       reprompt loop.
  ``UNAVAILABLE``       the PyPI check itself could not run (network
                       unreachable, timeout, non-JSON response). Nothing
                       about the requirement was verified — never
                       auto-applied, same fail-closed posture as Gate 3's
                       ``unavailable``.

Multiple ``declare_dependency`` ops in one patch: every one is checked and
the WORST verdict wins, ranked ``FAIL > UNAVAILABLE > WARN > PASS`` (worse
than ``NOT_APPLICABLE`` in every case once at least one op exists — the
patch owes a check the moment it declares anything).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from aqueduct.dependencies import Requirement, parse_requirement, requirement_status
from aqueduct.executor.capabilities import version_satisfies
from aqueduct.patch.gate_status import GateStatus

logger = logging.getLogger(__name__)

# Worse-verdict-wins ranking used to reduce multiple per-requirement verdicts
# to one gate result. Higher = worse. NOT_APPLICABLE never appears here — it
# is only ever the whole-gate verdict when there is nothing to check at all.
_VERDICT_RANK: dict[str, int] = {
    GateStatus.PASS: 0,
    GateStatus.WARN: 1,
    GateStatus.UNAVAILABLE: 2,
    GateStatus.FAIL: 3,
}


@dataclass
class ResolvabilityGateResult:
    status: str = GateStatus.NOT_APPLICABLE
    detail: str = ""
    duration_ms: int = 0
    requirements: list[str] = field(default_factory=list)


def _declared_requirements(patch_spec: Any) -> list[str]:
    """Return the ``requirement`` string of every ``declare_dependency`` op,
    in patch order."""
    out: list[str] = []
    for op in getattr(patch_spec, "operations", []) or []:
        if getattr(op, "op", None) == "declare_dependency":
            out.append(op.requirement)
    return out


def _pypi_url(name: str) -> str:
    return f"https://pypi.org/pypi/{name}/json"


def _check_one(req: Requirement, *, timeout: float, patch_id: str) -> tuple[str, str]:
    """Check one parsed requirement. Returns ``(status, detail)``.

    ``status`` is one of PASS / WARN / FAIL / UNAVAILABLE (never
    NOT_APPLICABLE — that is a whole-gate verdict, decided by the caller
    when there are zero requirements to check at all).
    """
    status, installed = requirement_status(req)
    if status in ("satisfied", "unknown_version"):
        return GateStatus.PASS, (
            f"{req.raw!r} already satisfied (installed: {installed})"
            if installed
            else f"{req.raw!r} already satisfied"
        )

    warn_detail = (
        f"{req.raw!r} resolves on PyPI but is not installed. "
        f"Install it (pip install {req.raw!r}) then: "
        f"aqueduct patch apply {patch_id}"
    )

    # Not installed (or a version conflict — still worth asking PyPI whether
    # SOME published version satisfies the specifier, since the installed
    # one not satisfying it does not mean nothing does).
    import httpx

    try:
        resp = httpx.get(_pypi_url(req.name), timeout=timeout)
    except httpx.HTTPError as exc:
        return GateStatus.UNAVAILABLE, (f"resolvability check for {req.raw!r} could not run: {exc}")

    if resp.status_code == 404:
        return GateStatus.FAIL, f"{req.raw!r}: no such package {req.name!r} on PyPI"

    try:
        resp.raise_for_status()
        data = resp.json()
    except (ValueError, httpx.HTTPError) as exc:
        return GateStatus.UNAVAILABLE, (f"resolvability check for {req.raw!r} could not run: {exc}")

    versions = data.get("releases") or data.get("versions") or {}
    if not isinstance(versions, dict):
        versions = {v: None for v in versions} if isinstance(versions, list) else {}

    if not req.specifier:
        if versions:
            return GateStatus.WARN, warn_detail
        return GateStatus.FAIL, f"{req.raw!r}: no such package {req.name!r} on PyPI"

    for version in versions:
        try:
            if version_satisfies(version, req.specifier):
                return GateStatus.WARN, warn_detail
        except ValueError:
            # A version string this PEP440-lite comparator cannot read —
            # skip it, don't count it as failing (mirrors
            # aqueduct.dependencies.requirement_status's unknown_version
            # reasoning).
            continue

    return GateStatus.FAIL, (
        f"{req.raw!r}: no version satisfying {req.specifier!r} found on PyPI " f"for {req.name!r}"
    )


def run_resolvability_gate(patch_spec: Any, *, timeout: float = 5.0) -> ResolvabilityGateResult:
    """Check every ``declare_dependency`` op in ``patch_spec`` and take the
    worst verdict (FAIL > UNAVAILABLE > WARN > PASS)."""
    t0 = time.monotonic()
    result = ResolvabilityGateResult()

    raw_requirements = _declared_requirements(patch_spec)
    if not raw_requirements:
        result.status = GateStatus.NOT_APPLICABLE
        result.detail = "no declare_dependency op in this patch"
        result.duration_ms = int((time.monotonic() - t0) * 1000)
        return result

    patch_id = getattr(patch_spec, "patch_id", None) or "<patch_id>"
    result.requirements = list(raw_requirements)
    per_req_details: list[str] = []
    worst_status = GateStatus.PASS
    worst_rank = _VERDICT_RANK[GateStatus.PASS]

    for raw in raw_requirements:
        try:
            req = parse_requirement(raw)
        except ValueError as exc:
            # Should not happen — DeclareDependencyOp already validates via
            # the same parser at construction time — but never crash the
            # gate over it; report it as a resolvability failure.
            per_req_details.append(f"{raw!r}: {exc}")
            if _VERDICT_RANK[GateStatus.FAIL] > worst_rank:
                worst_status, worst_rank = GateStatus.FAIL, _VERDICT_RANK[GateStatus.FAIL]
            continue

        status, detail = _check_one(req, timeout=timeout, patch_id=patch_id)
        per_req_details.append(detail)
        if _VERDICT_RANK[status] > worst_rank:
            worst_status, worst_rank = status, _VERDICT_RANK[status]

    result.status = worst_status
    result.detail = "; ".join(per_req_details)
    result.duration_ms = int((time.monotonic() - t0) * 1000)
    return result
