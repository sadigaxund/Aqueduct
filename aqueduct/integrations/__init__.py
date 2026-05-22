"""Orchestrator integrations (Phase 31+).

Each submodule wraps Aqueduct's stable exit-code + patch-CLI JSON contract for
a specific scheduler. The engine itself stays orchestrator-agnostic — these
modules only run when the user installs the matching extra
(``pip install aqueduct-core[airflow]``, etc.).

Never import a scheduler SDK at module top level — keep imports inside the
class bodies / functions so that ``import aqueduct.integrations`` stays cheap
and importable without the extra installed.
"""
