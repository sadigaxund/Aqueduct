"""Phase 85 Wave 4 — the ``tooling.*`` / ``observability.*`` leaf family.

These leaves describe TOOLING/HOST capabilities (``aqueduct test``'s test
runner, ``aqueduct drift``'s schema reader, doctor's table-existence /
cloud-preflight / session-preflight checks, and per-module
``module_metrics`` writing) rather than Blueprint grammar — no Blueprint
field ever spells one of these ids. See the "Tooling/host leaves" note in
``aqueduct/executor/capability_leaves.py``'s module docstring for the full
reasoning.

This file adds three things the generic closure tests in ``test_closure.py``
already cover structurally (every governed leaf needs a row in every engine's
declaration, every declared row needs a real leaf) but does not name by leaf
id:

  1. Spot-checks that the six leaves this family introduces are actually
     present, correctly namespaced, and excluded from ``execution_leaves()``.
  2. A direct proof that a ``tooling.*``/``observability.*`` leaf CANNOT gate
     Blueprint compilation — the family's defining property, called out
     explicitly in the work brief. Constructs a manifest touching every
     module type and confirms ``check_capabilities()`` never reports a
     ``tooling.*``/``observability.*`` leaf, even against DuckDB's
     declaration (which marks four of the six ``unsupported``).
  3. Verdict spot-checks per engine, matching the audit findings this family
     formalizes (``tmp/phase85/engine_parity_audit.md`` category (c) items
     1-5 and the module_metrics finding in category (b)).
"""

from __future__ import annotations

import pytest

# Register both engines' capability tables.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.compiler.capability_check import check_capabilities
from aqueduct.executor.capabilities import CAPABILITY_REGISTRY, Support
from aqueduct.executor.capability_leaves import all_leaves, execution_leaves
from aqueduct.models import Edge, Manifest, Module

pytestmark = pytest.mark.unit

_TOOLING_LEAVES = frozenset(
    {
        "tooling.test_runner",
        "tooling.drift_schema_read",
        "tooling.doctor.table_exists",
        "tooling.doctor.cloud_preflight",
        "tooling.doctor.session_preflight",
        "observability.module_metrics.per_module",
    }
)


def _module(id_, type_, config=None):
    return Module(id=id_, type=type_, label=id_, config=config or {})


# ── 1. presence + aggregate membership ──────────────────────────────────────


def test_tooling_leaves_present_in_all_leaves():
    """Every leaf this family introduces is a real, governed leaf — every
    registered engine's declaration must carry an explicit verdict for it
    (enforced generically by test_closure.py's forward/reverse checks)."""
    leaves = all_leaves()
    missing = _TOOLING_LEAVES - leaves
    assert not missing, f"tooling.* leaves not derived by all_leaves(): {missing}"


def test_tooling_leaves_excluded_from_execution_leaves():
    """No Blueprint field ever spells a tooling.*/observability.* leaf id, so
    there is no runtime dispatch path for a compiled Blueprint to exercise —
    these must NOT be part of execution_leaves() (which feeds the
    verdict-test-link requirement in test_verdict_test_links.py)."""
    exec_leaves = execution_leaves()
    overlap = _TOOLING_LEAVES & exec_leaves
    assert not overlap, f"tooling.*/observability.* leaked into execution_leaves(): {overlap}"


def test_every_engine_declares_every_tooling_leaf():
    """Direct, named spot-check (the generic closure test proves this too,
    but not by leaf id) — every registered engine's table has an explicit
    Capability for every leaf in this family, no UNDECLARED sentinel
    reaching the registry (registration itself would already have failed if
    one had)."""
    assert CAPABILITY_REGISTRY, "no engine registered"
    for engine, caps in CAPABILITY_REGISTRY.items():
        for leaf in _TOOLING_LEAVES:
            assert leaf in caps.table, f"{engine} has no declared verdict for {leaf!r}"


# ── 2. a tooling.* leaf cannot gate Blueprint compilation ───────────────────


def _manifest_touching_every_module_type() -> Manifest:
    modules = (
        _module("i", "Ingress", {"format": "csv", "path": "x.csv"}),
        _module("c", "Channel", {"op": "filter", "condition": "a > 1"}),
        _module("j", "Junction", {"mode": "conditional", "branches": []}),
        _module("f", "Funnel", {"mode": "union_all"}),
        _module("p", "Probe", {"signals": [{"type": "threshold", "expr": "COUNT(*) > 0"}]}),
        _module("a", "Assert", {"rules": []}),
        _module("e", "Egress", {"format": "parquet", "path": "o.parquet", "mode": "overwrite"}),
    )
    return Manifest(
        blueprint_id="bp",
        context={},
        modules=modules,
        edges=(
            Edge(from_id="i", to_id="c"),
            Edge(from_id="c", to_id="j"),
            Edge(from_id="j", to_id="f"),
            Edge(from_id="f", to_id="e"),
        ),
        engine_config={},
    )


@pytest.mark.parametrize("engine", ["spark", "duckdb"])
def test_no_blueprint_can_fail_compile_on_a_tooling_leaf(engine):
    """The defining property of this leaf family. DuckDB's declaration marks
    FOUR of the six tooling.* leaves `unsupported`
    (tooling.test_runner/doctor.table_exists/doctor.cloud_preflight/
    doctor.session_preflight) — if the gate ever looked one up for a real
    module, this manifest (which touches every module type) would report a
    CompileError for it. It must not: no Blueprint field spells a
    tooling.*/observability.* leaf id, so leaves_for_module() /
    feature_leaves_for_manifest() / type_leaves_for_manifest() never emit
    one, and check_capabilities() never queries the engine's verdict for it.
    """
    m = _manifest_touching_every_module_type()
    problems = check_capabilities(m, engine=engine)
    tooling_problems = [p for p in problems if p.leaf_id in _TOOLING_LEAVES]
    assert not tooling_problems, (
        f"a tooling.*/observability.* leaf gated compilation on {engine!r}, which should be "
        f"structurally impossible: {[(p.module_id, p.leaf_id) for p in tooling_problems]}"
    )


def test_duckdb_declares_unsupported_tooling_leaves_that_never_gate():
    """Companion to the parametrized test above, stated the other way round:
    confirm DuckDB really DOES mark these unsupported (so the previous test
    is not vacuous), then confirm compiling the same manifest against
    DuckDB is still clean."""
    caps = CAPABILITY_REGISTRY["duckdb"]
    truly_unsupported = {
        leaf for leaf in _TOOLING_LEAVES if caps.verdict(leaf).support is Support.UNSUPPORTED
    }
    assert truly_unsupported == {
        "tooling.test_runner",
        "tooling.doctor.table_exists",
        "tooling.doctor.cloud_preflight",
        "tooling.doctor.session_preflight",
    }, truly_unsupported

    m = _manifest_touching_every_module_type()
    problems = check_capabilities(m, engine="duckdb")
    assert not any(p.leaf_id in truly_unsupported for p in problems)


# ── 3. per-engine verdict spot-checks (pins the audit findings) ────────────


def test_spark_declares_all_tooling_leaves_supported():
    caps = CAPABILITY_REGISTRY["spark"]
    for leaf in _TOOLING_LEAVES:
        assert (
            caps.verdict(leaf).support is Support.SUPPORTED
        ), f"spark should support {leaf!r} (audit: tmp/phase85/engine_parity_audit.md)"


def test_duckdb_test_runner_is_honestly_unsupported():
    """aqueduct/cli/project.py's test_cmd unconditionally imports
    aqueduct.executor.spark.test_runner regardless of deployment.engine, and
    aqueduct/executor/duckdb_ has no test_runner.py at all — there is no
    DuckDB test-running implementation to claim, so this must be
    `unsupported`, not `supported` (a leaf could not honestly be marked
    'undeclared' either, per the framework's own rules)."""
    caps = CAPABILITY_REGISTRY["duckdb"]
    cap = caps.verdict("tooling.test_runner")
    assert cap.support is Support.UNSUPPORTED
    assert cap.hint  # every unsupported row must tell the user what to do


def test_duckdb_drift_schema_read_and_module_metrics_are_supported():
    """DuckDB genuinely implements both: read_source_schema via the
    ExecutorProtocol seam (aqueduct/executor/duckdb_/schema_reader.py) and
    per-module module_metrics writing (Phase 85 D1,
    aqueduct/executor/duckdb_/executor.py) — real handlers, real tests."""
    caps = CAPABILITY_REGISTRY["duckdb"]
    assert caps.verdict("tooling.drift_schema_read").support is Support.SUPPORTED
    assert caps.verdict("observability.module_metrics.per_module").support is Support.SUPPORTED


def test_duckdb_doctor_leaves_are_unsupported_with_hints():
    """None of the three doctor tooling checks exist for DuckDB yet
    (tmp/phase85/engine_parity_audit.md findings #3/#4/#5) — every one must
    be `unsupported` and every `unsupported` row must carry a hint telling
    the user what is and is not covered."""
    caps = CAPABILITY_REGISTRY["duckdb"]
    for leaf in (
        "tooling.doctor.table_exists",
        "tooling.doctor.cloud_preflight",
        "tooling.doctor.session_preflight",
    ):
        cap = caps.verdict(leaf)
        assert cap.support is Support.UNSUPPORTED, f"{leaf} should be unsupported on duckdb"
        assert cap.hint, f"{leaf} unsupported verdict has no hint"
