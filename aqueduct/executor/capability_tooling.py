"""Engine capability-declaration tooling — SHIPPED, not a repo script.

Phase 78. The capability framework holds two INDEPENDENT sources in agreement:

  - the leaf walker (CODE) — ``capability_leaves.py`` (Blueprint grammar) +
    ``config_leaves.py`` (``aqueduct.yml`` engine config)
  - each engine's declaration (DATA) — ``<engine package>/capabilities.yml``

Because they are independent they can disagree, which is what makes the closure
test able to fail at all. This module is how you reconcile them: ``check`` (read
only), ``sync`` (append newly-derived leaves as ``undeclared``), ``scaffold``
(a complete all-``undeclared`` table for a brand-new engine) and ``render_matrix``
(the published engine matrix, generated from the declarations).

**Why it lives in the package and not in ``scripts/``.** ``scripts/`` is not in
the wheel (``[tool.hatch.build.targets.wheel] packages = ["aqueduct"]``), so a
third-party engine author who ``pip install``s aqueduct had no way to generate
the table their engine cannot register without — and the table is 261 rows, so
the alternatives are hand-writing it or copying Spark's (which hands the new
engine 261 ``supported`` rows: a silent claim to implement the entire grammar,
the exact blindness this framework exists to prevent). The CLI surface is
``aqueduct dev capabilities …`` (``aqueduct/cli/dev.py``); ``scripts/capabilities.py``
is a thin wrapper over this module so there is exactly one implementation.

Pure data + filesystem work: no ``pyspark``, no ``click``. Rendering (colour,
icons) belongs to the CLI layer, so every function here returns values.
"""

from __future__ import annotations

import ast
import importlib.metadata
import importlib.util
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from aqueduct.executor.capabilities import AQ_ENGINES_ENTRYPOINT_GROUP, Support
from aqueduct.executor.capability_leaves import all_leaves, execution_leaves
from aqueduct.executor.config_leaves import all_config_leaves

UNDECLARED = Support.UNDECLARED.value
DECLARATION_FILENAME = "capabilities.yml"

MATRIX_START = "<!-- ENGINE_MATRIX_START -->"
MATRIX_END = "<!-- ENGINE_MATRIX_END -->"

# Repo root — three parents up from aqueduct/executor/capability_tooling.py.
# Used to resolve a `tests:` entry's repo-relative file path against disk.
_REPO_ROOT = Path(__file__).resolve().parents[2]


# ── verdict -> test-id linking ─────────────────────────────────────────────
#
# A `supported` EXECUTION leaf (capability_leaves.execution_leaves()) must
# name >=1 pytest id proving it is actually exercised on that engine
# (Phase 79). Resolution is a STATIC check — parse the target file with `ast`
# and look for a matching `def test_*` / `class Test*` — rather than shelling
# out to a full pytest collection, so `aqueduct dev capabilities check` stays
# fast and dependency-light. The stricter authoritative gate is the pytest
# closure test (test_verdict_test_links.py), which uses this SAME resolver so
# the two never drift apart.


def _pytest_names(file_path: Path) -> tuple[frozenset[str], dict[str, frozenset[str]]]:
    """Return (module-level test function/class names, {class: {method names}}),
    using pytest's default collection convention (`test_*` functions, `Test*`
    classes with `test_*` methods). Raises SyntaxError on an unparseable file.
    """
    tree = ast.parse(file_path.read_text(encoding="utf-8"))
    funcs: set[str] = set()
    classes: dict[str, set[str]] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith(
            "test_"
        ):
            funcs.add(node.name)
        elif isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            methods = {
                n.name
                for n in node.body
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                and n.name.startswith("test_")
            }
            classes[node.name] = methods
    return frozenset(funcs), {k: frozenset(v) for k, v in classes.items()}


def resolve_test_id(test_id: str, repo_root: Path | None = None) -> tuple[bool, str]:
    """Resolve one declared `tests:` entry against the real test tree.

    A bare file path (no ``::``) is a whole-file link — valid iff the file
    exists (the whole file is taken to exercise the leaf). A ``::name`` or
    ``::Class::method`` node id additionally requires the named function/
    class (or class + method) to actually be defined there, using pytest's
    default `test_*`/`Test*` naming convention — so an id pointing at
    something pytest would never collect is correctly reported unresolved.

    Returns ``(True, "")`` on success, ``(False, reason)`` otherwise.
    """
    root = repo_root or _REPO_ROOT
    parts = test_id.split("::")
    file_part = parts[0]
    file_path = root / file_part
    if not file_path.is_file():
        return False, f"file not found: {file_part}"
    if len(parts) == 1:
        return True, ""  # whole-file link
    try:
        funcs, classes = _pytest_names(file_path)
    except SyntaxError as exc:
        return False, f"{file_part} has a syntax error: {exc}"
    if len(parts) == 2:
        name = parts[1]
        if name in funcs or name in classes:
            return True, ""
        return False, f"{name!r} is not a collectible test function/class in {file_part}"
    if len(parts) == 3:
        cls, meth = parts[1], parts[2]
        if cls in classes and meth in classes[cls]:
            return True, ""
        return False, f"{cls}::{meth} is not a collectible test method in {file_part}"
    return False, f"unrecognized node id shape: {test_id!r}"


def governed_leaves() -> frozenset[str]:
    """Blueprint-grammar leaves ∪ engine-config leaves — the checklist."""
    return all_leaves() | all_config_leaves()


# ── declaration discovery ─────────────────────────────────────────────────────


def _engine_dir_of_entry_point(ep: importlib.metadata.EntryPoint) -> Path | None:
    """Locate an engine entry point's package directory WITHOUT importing it.

    Importing the module is exactly what we cannot do here: an engine whose
    declaration is incomplete raises ``CapabilityDeclarationError`` on import,
    and that is precisely the engine ``sync``/``check`` exist to help. So the
    module is located through ``find_spec`` (which imports only the parent
    package, never the engine module itself).
    """
    target = ep.value.split(":")[0]
    try:
        spec = importlib.util.find_spec(target)
    except (ImportError, AttributeError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    return Path(spec.origin).parent


def discover_declarations(extra: list[Path] | None = None) -> list[Path]:
    """Every ``capabilities.yml`` this install governs.

    Two sources, unioned:

      - every registered ``aqueduct.engines`` entry point's package dir (so a
        third-party engine installed alongside aqueduct is covered), and
      - every ``capabilities.yml`` under ``aqueduct/executor/*/`` (so an engine
        that has been scaffolded but not yet wired to an entry point is covered
        too — the state a new engine spends its first hours in).
    """
    found: set[Path] = set()

    for ep in importlib.metadata.entry_points(group=AQ_ENGINES_ENTRYPOINT_GROUP):
        d = _engine_dir_of_entry_point(ep)
        if d is not None and (d / DECLARATION_FILENAME).is_file():
            found.add((d / DECLARATION_FILENAME).resolve())

    executor_root = Path(__file__).resolve().parent
    for p in executor_root.glob(f"*/{DECLARATION_FILENAME}"):
        found.add(p.resolve())

    for p in extra or []:
        found.add(Path(p).resolve())

    return sorted(found)


def load_rows(path: Path | str) -> dict:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return raw.get("leaves") or {}


def verdict_of(row: object) -> str:
    if isinstance(row, str):
        return row
    if isinstance(row, dict):
        return str(row.get("support", ""))
    return ""


def tests_of(row: object) -> list[str]:
    if isinstance(row, dict):
        val = row.get("tests")
        if isinstance(val, list):
            return [str(v) for v in val]
    return []


# ── check ─────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DeclarationReport:
    """What one engine's declaration is missing, orphaning, or ducking.

    ``missing_test_links`` / ``dangling_test_links`` are REPORTED but do not
    affect ``ok`` — they are informational findings from `check`/`sync`
    (Phase 79). The build-breaking enforcement of "a `supported` EXECUTION
    leaf must carry a resolvable test id" lives in the pytest closure test
    (``tests/test_capabilities/test_verdict_test_links.py``), not here, so
    that a still-unbacked verdict fails LOUDLY in CI without also flipping
    this dev-tooling command's exit code (which existing callers key off of
    for the leaf-completeness/orphan checks this class already reported
    before Phase 79).
    """

    path: Path
    engine: str
    total: int
    missing: list[str] = field(default_factory=list)
    undeclared: list[str] = field(default_factory=list)
    orphaned: list[str] = field(default_factory=list)
    missing_test_links: list[str] = field(default_factory=list)
    dangling_test_links: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.missing or self.undeclared or self.orphaned)


def _engine_name(path: Path) -> str:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return path.parent.name
    return str(raw.get("engine") or path.parent.name)


def check(paths: list[Path] | None = None) -> list[DeclarationReport]:
    """Report drift for every discovered declaration. Writes nothing.

    Also reports (Phase 79) verdict-test-link gaps for EXECUTION leaves
    (``capability_leaves.execution_leaves()``): a ``supported`` row with no
    ``tests:`` entry is ``missing_test_links``; a declared ``tests:`` id that
    does not resolve against the real test tree (``resolve_test_id``) is
    ``dangling_test_links``. Both are informational — see ``DeclarationReport``
    docstring for why they do not flip ``.ok``.
    """
    leaves = governed_leaves()
    exec_leaves = execution_leaves()
    reports: list[DeclarationReport] = []
    for path in paths if paths is not None else discover_declarations():
        rows = load_rows(path)
        missing_test_links: list[str] = []
        dangling_test_links: list[tuple[str, str, str]] = []
        for leaf, row in rows.items():
            declared_tests = tests_of(row)
            if leaf in exec_leaves and verdict_of(row) == "supported" and not declared_tests:
                missing_test_links.append(leaf)
            for test_id in declared_tests:
                ok, reason = resolve_test_id(test_id)
                if not ok:
                    dangling_test_links.append((leaf, test_id, reason))
        reports.append(
            DeclarationReport(
                path=path,
                engine=_engine_name(path),
                total=len(rows),
                missing=sorted(leaves - set(rows)),
                undeclared=sorted(k for k, v in rows.items() if verdict_of(v) == UNDECLARED),
                orphaned=sorted(set(rows) - leaves),
                missing_test_links=sorted(missing_test_links),
                dangling_test_links=sorted(dangling_test_links),
            )
        )
    return reports


# ── sync ──────────────────────────────────────────────────────────────────────


def sync(paths: list[Path] | None = None) -> list[DeclarationReport]:
    """Append every missing leaf to each declaration as ``undeclared``.

    Deliberately NEVER writes a real verdict and never removes a row — a human
    decides what an engine does with a new leaf. Orphaned rows are reported, not
    deleted, so a rename is reviewed rather than silently dropped.

    Returns the PRE-sync reports (what was appended / what is orphaned).
    """
    reports = check(paths)
    for r in reports:
        if r.missing:
            text = r.path.read_text(encoding="utf-8").rstrip("\n")
            block = "\n".join(f"  {leaf}: {UNDECLARED}" for leaf in r.missing)
            r.path.write_text(f"{text}\n{block}\n", encoding="utf-8")
    return reports


# ── scaffold ──────────────────────────────────────────────────────────────────


SCAFFOLD_HEADER = """\
# {engine} engine capability declaration — SCAFFOLD. Every row below is `undeclared`.
#
# READ THIS BEFORE EDITING:
#
#   * Every row must be replaced with a REAL verdict:
#         supported | unsupported | ignored_with_warning
#     A row may also carry `requires:` (dependency version constraints, checked
#     by `aqueduct doctor`) and `hint:` (actionable text shown when the leaf is
#     refused). See aqueduct/executor/spark/capabilities.yml for the shape.
#
#   * The build FAILS while any `undeclared` row remains. Engine registration
#     raises CapabilityDeclarationError and the closure test stays red. That is
#     deliberate: `undeclared` means "nobody has decided yet", which is NOT the
#     same as `unsupported` ("we decided this engine cannot do it").
#
#   * Spark's capabilities.yml is a REFERENCE TO READ, NOT A FILE TO COPY.
#     Copying it hands you ~{n} `supported` rows, i.e. a silent claim that this
#     engine supports the entire grammar and every aqueduct.yml key. That
#     blind-inheritance is exactly what this framework exists to prevent —
#     decide each leaf on its own merits.
#
#   * This file was generated from the live grammar + config walkers, so it
#     cannot go stale relative to a static template. Re-run
#     `aqueduct dev capabilities sync` whenever a new leaf lands.
#
# Register the engine by declaring an `aqueduct.engines` entry point pointing at
# its engine module (see pyproject.toml's [project.entry-points."aqueduct.engines"]).

engine: {engine}

leaves:
"""


@dataclass(frozen=True)
class ScaffoldResult:
    path: Path
    engine: str
    leaves: int
    grammar_leaves: int
    config_leaves: int


def scaffold(engine: str, out: Path | str | None = None, force: bool = False) -> ScaffoldResult:
    """Write a COMPLETE ``capabilities.yml`` for a brand-new engine, all ``undeclared``.

    This is the answer to "how do I implement a new engine". Not a static
    template (it would go stale the moment the grammar changed) and emphatically
    not "copy Spark's table" (that inherits ~261 ``supported`` rows — a new
    engine silently claiming to support everything). Generated from the walkers,
    so it cannot drift and cannot smuggle in a default verdict.

    Raises:
        FileExistsError: ``out`` exists and ``force`` is False.
    """
    target = (
        Path(out)
        if out is not None
        else Path(__file__).resolve().parent / engine / DECLARATION_FILENAME
    )
    if target.exists() and not force:
        raise FileExistsError(target)

    leaves = sorted(governed_leaves())
    body = "".join(f"  {leaf}: {UNDECLARED}\n" for leaf in leaves)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(SCAFFOLD_HEADER.format(engine=engine, n=len(leaves)) + body, encoding="utf-8")
    return ScaffoldResult(
        path=target,
        engine=engine,
        leaves=len(leaves),
        grammar_leaves=len(all_leaves()),
        config_leaves=len(all_config_leaves()),
    )


# ── docs matrix ───────────────────────────────────────────────────────────────


def render_matrix(paths: list[Path] | None = None) -> str:
    """Render the engine capability matrix FROM THE DECLARATIONS.

    The YAML is the source of truth, so this is generated, not hand-maintained.
    A 261-row table per engine would be noise, so the matrix reports the summary
    (verdict counts per engine) plus every row that is NOT a plain unconditional
    ``supported`` — the version-gated, ignored, and unsupported leaves are
    exactly the ones a user needs to know about.
    """
    decls = paths if paths is not None else discover_declarations()
    rows_by_engine = {_engine_name(p): load_rows(p) for p in decls}
    engines = sorted(rows_by_engine)

    out: list[str] = [""]
    out.append("<!-- Generated by `aqueduct dev capabilities docs` — do not edit by hand. -->")
    out.append("")
    out.append("### Declared capability totals")
    out.append("")
    out.append(
        "| Engine | Leaves declared | Supported | Version-gated | "
        "Ignored with warning | Unsupported |"
    )
    out.append("|---|---|---|---|---|---|")
    for eng in engines:
        rows = rows_by_engine[eng]
        supported = [k for k, v in rows.items() if verdict_of(v) == "supported"]
        gated = [k for k, v in rows.items() if isinstance(v, dict) and v.get("requires")]
        ignored = [k for k, v in rows.items() if verdict_of(v) == "ignored_with_warning"]
        unsupported = [k for k, v in rows.items() if verdict_of(v) == "unsupported"]
        out.append(
            f"| `{eng}` | {len(rows)} | {len(supported)} | {len(gated)} | "
            f"{len(ignored)} | {len(unsupported)} |"
        )
    out.append("")
    out.append("### Conditional and refused capabilities")
    out.append("")
    out.append(
        "Every leaf that is not unconditionally supported. A version-gated leaf runs "
        "only above the stated dependency version (`aqueduct doctor` checks this against "
        "what is actually installed); an ignored leaf is accepted but has no effect; an "
        "unsupported leaf fails compilation."
    )
    out.append("")
    out.append("| Engine | Capability leaf | Verdict | Requires | Notes |")
    out.append("|---|---|---|---|---|")
    any_row = False
    for eng in engines:
        for leaf, row in sorted(rows_by_engine[eng].items()):
            verdict = verdict_of(row)
            requires = row.get("requires") if isinstance(row, dict) else None
            hint = row.get("hint", "") if isinstance(row, dict) else ""
            if verdict == "supported" and not requires:
                continue
            any_row = True
            req = ", ".join(f"`{d}{s}`" for d, s in (requires or {}).items()) or "—"
            out.append(f"| `{eng}` | `{leaf}` | {verdict} | {req} | {hint or '—'} |")
    if not any_row:
        out.append("| — | — | — | — | Every declared leaf is unconditionally supported. |")
    out.append("")
    return "\n".join(out)


def write_matrix(doc: Path | str, paths: list[Path] | None = None) -> bool:
    """Splice ``render_matrix()`` into ``doc`` between the matrix markers.

    Returns True if the file changed.

    Raises:
        ValueError: the document has no ``<!-- ENGINE_MATRIX_START/END -->`` markers.
    """
    p = Path(doc)
    text = p.read_text(encoding="utf-8")
    if MATRIX_START not in text or MATRIX_END not in text:
        raise ValueError(f"{p} is missing the {MATRIX_START} / {MATRIX_END} markers.")
    head, rest = text.split(MATRIX_START, 1)
    _, tail = rest.split(MATRIX_END, 1)
    new = f"{head}{MATRIX_START}{render_matrix(paths)}{MATRIX_END}{tail}"
    if new == text:
        return False
    p.write_text(new, encoding="utf-8")
    return True
