"""Phase 85 F-17 — shared seam for gate-pyramid validation.

``aqueduct/cli/run.py`` used to build an inline ``_validate_cb`` closure (one
per heal, ~a dozen captured locals) every time ``deep_loop`` was enabled, to
give ``aqueduct.agent.loop.run_agent_loop``'s ``validate_callback`` parameter
(``Callable[[Any], tuple[bool, str]]`` — see ``agent/loop.py``) something to
call. That closure ran the lineage/sandbox/explain gate pyramid
(``aqueduct.cli._run_patch_gates_inline``) against a candidate patch and
formatted the result into the ``(ok, feedback)`` shape the deep-loop
in-conversation validation protocol expects.

This module is the real function/class that replaces it. It belongs here
rather than in ``aqueduct/cli/`` because it is pure agent-boundary glue: its
entire reason to exist is producing a ``validate_callback`` for the agent
loop, and its signature is dictated by that loop's protocol, not by the CLI.
The call site in ``run.py`` now does::

    from functools import partial
    from aqueduct.agent.gate_validation import validate_patch_via_gates

    _validate_cb = partial(validate_patch_via_gates, blueprint_path=..., ...)

binding the per-heal locals as explicit keyword arguments instead of closing
over them.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def format_gate_feedback(g2: Any, g3: Any, g4: Any) -> tuple[bool, str]:
    """Convert lineage/sandbox/explain gate results into ``(ok, feedback)``.

    Copied verbatim (Phase 85 F-17) from the body of the ``_validate_cb``
    closure that used to live inline in ``aqueduct/cli/run.py::run()``
    (~line 2830 pre-split).
    """
    failures: list[str] = []
    if g2 is not None and g2.status == "fail":
        failures.append(f"Lineage gate: {g2.detail or 'column impact detected'}")
    if g3 is not None and g3.status == "fail":
        failures.append(f"Sandbox gate: {g3.detail}")
    if g4 is not None and g4.status == "fail":
        failures.append(f"Explain gate: {g4.detail or 'plan regression detected'}")
    if failures:
        return False, " | ".join(failures)
    return True, ""


def validate_patch_via_gates(
    patch_spec: Any,
    *,
    blueprint_path,
    bundle,
    surveyor,
    failed_module,
    iteration_run_id: str,
    blueprint_id: str,
    engine: str,
    cfg,
    sandbox_mode: str = "sample",
    sandbox_master_url: str | None = None,
    warnings_suppress=None,
    timezone: str | None = None,
    announce_unavailable: Callable[[Any], None] | None = None,
) -> tuple[bool, str]:
    """Run the gate pyramid against ``patch_spec`` and format the verdict.

    This is the F-17 extraction of the ``_validate_cb`` closure body that
    used to live inline in ``aqueduct/cli/run.py::run()``. Bind the fixed
    (per-heal) keyword arguments with ``functools.partial`` to get a
    ``Callable[[Any], tuple[bool, str]]`` suitable for
    ``aqueduct.agent.loop``'s ``validate_callback`` parameter.

    ``announce_unavailable``, when given, is called with the sandbox gate
    result — the caller's hook for one-shot "polyglot sandbox unavailable"
    narration (``run.py``'s ``_announce_polyglot_sandbox_unavailable``,
    which needs ``nonlocal`` access to a per-run warned-flag and so cannot
    move out of ``run()`` itself).

    Imports ``aqueduct.cli`` lazily (inside the function body) to avoid a
    module-level ``aqueduct.agent`` → ``aqueduct.cli`` import edge, mirroring
    how ``run.py`` already imports ``aqueduct.agent`` lazily in the other
    direction.
    """
    from aqueduct.cli import _run_patch_gates_inline

    try:
        g2, g3, g4, _g3_passed = _run_patch_gates_inline(
            patch=patch_spec,
            blueprint_path=blueprint_path,
            bundle=bundle,
            surveyor=surveyor,
            failed_module=failed_module,
            iteration_run_id=iteration_run_id,
            blueprint_id=blueprint_id,
            engine=engine,
            cfg=cfg,
            sandbox_mode=sandbox_mode,
            sandbox_master_url=sandbox_master_url,
            warnings_suppress=warnings_suppress,
            timezone=timezone,
        )
        if announce_unavailable is not None:
            announce_unavailable(g3)
        return format_gate_feedback(g2, g3, g4)
    except Exception as exc:
        return False, f"Validation error: {exc}"
