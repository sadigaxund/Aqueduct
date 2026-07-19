"""Phase 79 — verdict -> test-id linking closure test.

"A `supported` capability row requires a test exercising it on that engine"
was policy, not mechanism, until now: nothing verified a `supported` row was
actually backed by a real test. This is the enforcement.

Scope is EXECUTION leaves only (``capability_leaves.execution_leaves()`` —
``module.type.*``, ``channel.op.*``, ``ingress.format.*``, ``egress.format.*``
/ ``egress.mode.*`` / ``egress.on_new_columns.*``, ``junction.mode.*``,
``funnel.mode.*``, ``feature.*``). ``config.*`` leaves (warn-only governance,
no runtime dispatch) and the ENGINE-INVARIANT grammar leaves (``module.field.*``,
every ``<block>.field.*`` — Blueprint-authoring fields core orchestration
handles identically regardless of engine) are deliberately out of scope, and
that scope is derived from ``execution_leaves()`` itself (a function composed
from the SAME per-category walkers ``all_leaves()`` unions) — never a
hand-listed set of leaf-id strings here.

Reads each engine's YAML straight from disk, same independent-sources style
as ``test_closure.py``, so a leaf's test-link gap is reported even if the
same declaration also has an unrelated hole that would make engine
REGISTRATION blow up.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from aqueduct.executor.capability_leaves import execution_leaves
from aqueduct.executor.capability_tooling import resolve_test_id

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_DECLARATIONS = sorted((_REPO / "aqueduct" / "executor").glob("*/capabilities.yml"))


def _declared_rows(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return raw.get("leaves") or {}


def _verdict_of(row: object) -> str:
    return row if isinstance(row, str) else (row or {}).get("support", "")


def _tests_of(row: object) -> list[str]:
    if isinstance(row, dict):
        val = row.get("tests")
        if isinstance(val, list):
            return [str(v) for v in val]
    return []


def test_declarations_discovered():
    """Guard the discovery glob — if it silently matched nothing, every
    parametrized test below would vacuously pass."""
    assert _DECLARATIONS, "no engine capabilities.yml found — these tests would be vacuous"


@pytest.mark.parametrize("decl_path", _DECLARATIONS, ids=[p.parent.name for p in _DECLARATIONS])
def test_every_supported_execution_leaf_has_a_test_link(decl_path):
    """Every `supported` EXECUTION-leaf row on this engine must name >=1 test.

    This is the presence half of the enforcement. It is allowed to fail on a
    genuinely unbacked verdict — that failure list is the honest deliverable,
    not a bug to paper over by inventing an id or downgrading the verdict.
    """
    rows = _declared_rows(decl_path)
    exec_leaves = execution_leaves()
    unbacked = sorted(
        leaf
        for leaf, row in rows.items()
        if leaf in exec_leaves and _verdict_of(row) == "supported" and not _tests_of(row)
    )
    assert not unbacked, (
        f"{decl_path.parent.name}: {len(unbacked)} `supported` EXECUTION leaf/leaves declare "
        f"no `tests:` id: {unbacked}.\n"
        "A `supported` execution verdict must name >=1 pytest id (or bare file path) that "
        "actually exercises the leaf on this engine. Add `tests:` to the row in "
        f"{decl_path}, or if no test exists, this failure IS the correct signal — do not "
        "downgrade the verdict to make it go away."
    )


@pytest.mark.parametrize("decl_path", _DECLARATIONS, ids=[p.parent.name for p in _DECLARATIONS])
def test_declared_test_ids_resolve(decl_path):
    """Every declared `tests:` id (on any row, not just EXECUTION-supported
    ones — a dangling id is a bug wherever it appears) must resolve against
    the real test tree: the file must exist, and a `::name` node id must name
    a real, pytest-collectible function/class (see `resolve_test_id`)."""
    rows = _declared_rows(decl_path)
    failures: list[str] = []
    for leaf, row in rows.items():
        for test_id in _tests_of(row):
            ok, reason = resolve_test_id(test_id, repo_root=_REPO)
            if not ok:
                failures.append(f"{leaf} -> {test_id!r}: {reason}")
    assert not failures, (
        f"{decl_path.parent.name}: {len(failures)} declared test id(s) do not resolve:\n"
        + "\n".join(failures)
    )


def test_execution_leaves_is_a_strict_subset_of_all_leaves():
    """Sanity guard on the scope function itself — execution_leaves() must
    never introduce a leaf id all_leaves() doesn't also derive, and must
    exclude the module.field.*/<block>.field.* categories (the ENGINE-
    INVARIANT leaves this Phase 79 scope deliberately does not require tests
    for)."""
    from aqueduct.executor.capability_leaves import all_leaves

    exec_leaves = execution_leaves()
    assert exec_leaves <= all_leaves()
    assert not any(leaf.startswith("module.field.") for leaf in exec_leaves)
    assert not any(leaf.startswith("config.") for leaf in exec_leaves)


def test_execution_leaves_deterministic():
    assert execution_leaves() == execution_leaves()


def test_execution_leaves_nonempty():
    assert len(execution_leaves()) > 20  # sanity floor


def test_resolve_test_id_file_level(tmp_path):
    f = tmp_path / "test_x.py"
    f.write_text("def test_a():\n    assert True\n", encoding="utf-8")
    ok, reason = resolve_test_id("test_x.py", repo_root=tmp_path)
    assert ok, reason


def test_resolve_test_id_function(tmp_path):
    f = tmp_path / "test_x.py"
    f.write_text("def test_a():\n    assert True\n", encoding="utf-8")
    ok, reason = resolve_test_id("test_x.py::test_a", repo_root=tmp_path)
    assert ok, reason


def test_resolve_test_id_class_method(tmp_path):
    f = tmp_path / "test_x.py"
    f.write_text(
        "class TestFoo:\n    def test_a(self):\n        assert True\n", encoding="utf-8"
    )
    ok, reason = resolve_test_id("test_x.py::TestFoo::test_a", repo_root=tmp_path)
    assert ok, reason


def test_resolve_test_id_missing_file(tmp_path):
    ok, reason = resolve_test_id("test_ghost.py::test_a", repo_root=tmp_path)
    assert not ok
    assert "not found" in reason


def test_resolve_test_id_missing_function(tmp_path):
    f = tmp_path / "test_x.py"
    f.write_text("def test_a():\n    assert True\n", encoding="utf-8")
    ok, reason = resolve_test_id("test_x.py::test_ghost", repo_root=tmp_path)
    assert not ok
    assert "not a collectible" in reason


def test_resolve_test_id_non_test_prefixed_function_is_not_collectible(tmp_path):
    """A function that pytest itself would never collect (no `test_` prefix)
    must not resolve — this is what makes the check stronger than a bare
    'does a def with this name exist anywhere in the file' grep."""
    f = tmp_path / "test_x.py"
    f.write_text("def helper():\n    assert True\n", encoding="utf-8")
    ok, reason = resolve_test_id("test_x.py::helper", repo_root=tmp_path)
    assert not ok
    assert "not a collectible" in reason


def test_sync_preserves_existing_tests_key(tmp_path, monkeypatch):
    """The likeliest silent data-loss bug this phase could introduce: `sync`
    rewriting a declaration and dropping an existing multi-line `tests:`
    block. `sync` only ever APPENDS new leaves as raw text — it must never
    touch a byte of an existing row."""
    import aqueduct.executor.capability_tooling as tooling

    decl = tmp_path / "capabilities.yml"
    decl.write_text(
        "engine: toy\n"
        "leaves:\n"
        "  feature.a:\n"
        "    support: supported\n"
        "    tests:\n"
        "      - tests/test_x.py::test_a\n"
        "      - tests/test_x.py::test_b\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(tooling, "discover_declarations", lambda extra=None: [decl])
    monkeypatch.setattr(
        tooling, "governed_leaves", lambda: frozenset({"feature.a", "feature.new"})
    )

    tooling.sync()

    rows = _declared_rows(decl)
    assert rows["feature.a"]["support"] == "supported"
    assert rows["feature.a"]["tests"] == [
        "tests/test_x.py::test_a",
        "tests/test_x.py::test_b",
    ]
    assert rows["feature.new"] == "undeclared"


def test_scaffold_emits_new_rows_without_a_tests_key(tmp_path):
    """A freshly scaffolded engine has nothing to link tests to yet — every
    row is `undeclared` with no `tests:` key at all (not an empty list)."""
    from aqueduct.executor.capability_tooling import scaffold

    out = tmp_path / "capabilities.yml"
    scaffold("toyengine", out=out)
    raw = yaml.safe_load(out.read_text(encoding="utf-8"))
    for leaf, row in raw["leaves"].items():
        assert row == "undeclared", f"{leaf}: scaffold must emit a bare 'undeclared' string"
