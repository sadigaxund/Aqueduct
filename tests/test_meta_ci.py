"""Structural guards on the CI environment lock.

Two properties, and both are load-bearing in OPPOSITE directions:

  1. The reproducible lanes install under the committed lock (`PIP_CONSTRAINT`),
     so they cannot go red because of what PyPI published that morning.
  2. The CANARY lane (`version-matrix.yml`'s `snippets` job) installs UNPINNED,
     on purpose. Its job is to resolve fresh and go red first when upstream
     drifts — that early warning is the whole feature. Adding a constraint to it
     would look like an improvement and would silently delete the only lane
     watching upstream, so a test says so out loud.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

from aqueduct.executor.capabilities import CAPABILITY_REGISTRY, load_engines

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[1]
_WORKFLOWS = _REPO / ".github" / "workflows"
_REQUIREMENTS = _REPO / "requirements"

_LOCKS = [
    "ci-py311.txt",
    "compat-py3.11.txt",
    "compat-py3.12.txt",
    "compat-py3.13.txt",
]

# `tests/test_*/` directories intentionally absent from every PRE-merge lane
# in test-suite.yml. Landing here requires a reason — an unexplained gap is
# exactly the bug class this file exists to catch (duckdb/typehub/deploy were
# all silently main-only before this guard existed). Keyed by the directory's
# repo-relative posix path (no trailing slash).
_MAIN_ONLY_TEST_DIR_ALLOWLIST: dict[str, str] = {
    "tests/test_dashboard": (
        "aqueduct/dashboard needs the `dashboard` extra (streamlit/plotly) — a "
        "local, read-only, on-demand observability viewer, same dev-tooling "
        "carve-out as `tui`/`mcp` (AGENTS.md Packaging & Extras Policy). Out of "
        "scope for the reproducible pre-merge lanes; stays main-only/manual."
    ),
}


def _workflow(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text(encoding="utf-8"))


def _registered_engines() -> list[str]:
    """The list of engines registered via the `aqueduct.engines` entry-point
    group — the single source of truth every "is engine X covered somewhere"
    guard in this file reuses. Never hardcode an engine name list here; that
    is exactly what let the DuckDB compat-matrix gap (and this file's mirror
    gap in test-suite.yml) go undetected."""
    load_engines()
    engines = sorted(CAPABILITY_REGISTRY)
    assert engines, (
        "no engines registered via the `aqueduct.engines` entry-point group — "
        "a stale editable install? (see NO_ENGINES_HINT in capabilities.py)"
    )
    return engines


def _is_pre_merge_job(job: dict) -> bool:
    """A `test-suite.yml` job counts as PRE-merge if its `if:` can be satisfied
    on a plain branch push, not only on `main` / `workflow_dispatch`. Every
    path-filtered lane in this workflow follows the shape
    ``needs.changes.outputs.<x> == 'true' || github.ref == 'refs/heads/main'``
    — the ``needs.changes.outputs.`` clause is exactly what lets it fire on a
    feature/phase branch push, so its presence is the structural signal used
    here. The `coverage` job's `if`
    (``github.ref == 'refs/heads/main' || github.event_name ==
    'workflow_dispatch'``) has no such clause — it is genuinely main-only, on
    purpose (see its own header comment) — and must NOT count as pre-merge
    coverage, or this guard would be exactly as blind as the bug it fixes."""
    return "needs.changes.outputs." in str(job.get("if", ""))


def _pre_merge_pytest_commands() -> list[str]:
    """The exact `pytest ...` command line from every PRE-merge job's `run:`
    step in `test-suite.yml` (one line per job — every lane in this workflow
    shells out to pytest exactly once). Restricted to lines starting with
    ``pytest `` — never a `pip install -e ".[...]"` line — so an
    engine/directory name showing up only as an extras-install artifact (e.g.
    `spark` inside `pip install -e ".[dev,spark,llm,redis]"`) can't fake
    coverage."""
    commands = []
    for job in _workflow("test-suite.yml")["jobs"].values():
        if not _is_pre_merge_job(job):
            continue
        for step in job.get("steps", []):
            run = step.get("run", "")
            for line in run.splitlines():
                line = line.strip()
                if line.startswith("pytest "):
                    commands.append(line)
    return commands


def _positive_marker_terms(marker_expr: str) -> set[str]:
    """Marker names in `marker_expr` that are POSITIVELY selected — i.e. not
    immediately negated by `not`. `"not spark"` -> `set()` (spark is
    EXCLUDED by that expression, so it must never count as coverage).
    `"spark"` -> `{"spark"}`. `"spark or duckdb"` -> `{"spark", "duckdb"}`.

    This is deliberately not a general boolean-expression parser (no
    `not (a or b)` grouping) — every `-m` expression in this repo's workflows
    is either a single term or `not <term>`, and this only needs to get those
    right without silently mis-scoring something more exotic later, so it
    only recognises the flat `not X` shape it was built to catch."""
    tokens = re.findall(r"\(|\)|\w+", marker_expr)
    positive: set[str] = set()
    negate_next = False
    for tok in tokens:
        if tok == "not":
            negate_next = True
            continue
        if tok in ("and", "or", "(", ")"):
            negate_next = False
            continue
        if not negate_next:
            positive.add(tok)
        negate_next = False
    return positive


def _positive_coverage(commands: list[str]) -> tuple[str, set[str]]:
    """Splits every pytest command line into (path text, positively-selected
    marker terms), and returns the aggregate across all `commands`.

    The `-m "..."` clause is cut OUT of the path text before it's added to
    the corpus — this is what makes `test_no_test_directory_is_absent_...`
    immune to the same negation blindness by construction: a directory path
    can never masquerade as marker text once the marker clause is removed.
    Engine coverage is then `path in path_corpus or engine in positive_terms`
    — the first form is legitimate because running a directory really does
    execute every test under it whose OWN marker isn't excluded by `-m`
    (e.g. `pytest tests/test_executor_duckdb/ -m "not spark"` really does run
    the duckdb-marked tests there — "not spark" only excludes spark); the
    second form is for a job that selects an engine's tests by marker alone
    (`-m spark` in `executor-tests`) rather than by a dedicated directory.
    """
    path_parts = []
    positive_terms: set[str] = set()
    marker_re = re.compile(r'-m\s+"([^"]+)"|-m\s+(\S+)')
    for cmd in commands:
        m = marker_re.search(cmd)
        if m:
            marker_expr = m.group(1) or m.group(2)
            positive_terms |= _positive_marker_terms(marker_expr)
            rest = cmd[: m.start()] + cmd[m.end() :]
        else:
            rest = cmd
        path_parts.append(rest)
    return "\n".join(path_parts), positive_terms


@pytest.mark.parametrize("lock", _LOCKS)
def test_lock_file_is_committed_and_pinned(lock):
    text = (_REQUIREMENTS / lock).read_text(encoding="utf-8")
    pins = [ln for ln in text.splitlines() if ln and not ln.startswith((" ", "#"))]
    assert pins, f"{lock} declares no pins"
    assert all("==" in ln for ln in pins), f"{lock} has a non-exact pin: {pins[:3]}"


def test_pyproject_dependencies_stay_ranges_not_pins():
    """The other half of the two-tier rule: the LIBRARY must not be pinned. Exact
    pins in pyproject would make aqueduct-core uninstallable next to a user's own
    stack — the lock belongs to the environment, never to the published package."""
    text = (_REPO / "pyproject.toml").read_text(encoding="utf-8")
    deps = text.split("dependencies = [", 1)[1].split("]", 1)[0]
    assert "==" not in deps, f"pyproject dependencies must stay ranges, found: {deps}"


def test_every_test_suite_lane_installs_under_the_lock():
    wf = _workflow("test-suite.yml")
    constraint = (wf.get("env") or {}).get("PIP_CONSTRAINT", "")
    assert "requirements/ci-py311.txt" in constraint, (
        "test-suite.yml lanes are reproducible lanes and must install under the "
        "committed lock (PIP_CONSTRAINT)."
    )


def test_compat_promise_lane_installs_under_a_per_interpreter_lock():
    jobs = _workflow("version-matrix.yml")["jobs"]
    compat = (jobs["compat"].get("env") or {}).get("PIP_CONSTRAINT", "")
    assert "requirements/compat-py" in compat


def test_canary_lane_stays_unpinned_on_purpose():
    """DO NOT "fix" this by pinning the canary. It resolves fresh so that a
    breaking upstream release shows up here FIRST, as a red annotation, instead
    of arriving unannounced in a promised lane later. Locking it would remove
    the early-warning signal entirely."""
    wf = _workflow("version-matrix.yml")
    assert "PIP_CONSTRAINT" not in (
        wf.get("env") or {}
    ), "a workflow-level PIP_CONSTRAINT would leak onto the canary lane"
    snippets = wf["jobs"]["snippets"]
    assert "PIP_CONSTRAINT" not in (snippets.get("env") or {})
    steps = yaml.dump(snippets)
    assert "PIP_CONSTRAINT" not in steps
    assert snippets.get("continue-on-error") is True  # early warning, never a blocker


def test_snippets_lts_lane_is_pinned_and_blocking():
    """The mirror image of the canary invariant above: `snippets-lts` is the
    PROMISE-lane counterpart to the `snippets` canary — it exists to hold
    gallery/** to a version combo Aqueduct actually supports, so it must be
    pinned (a promise lane cannot go red because of what PyPI published this
    morning) AND blocking (an honest red on a broken snippet is the point of
    this lane; continue-on-error would silently defeat it, same as it would
    on any `compat` combo)."""
    wf = _workflow("version-matrix.yml")
    lts = wf["jobs"]["snippets-lts"]
    constraint = (lts.get("env") or {}).get("PIP_CONSTRAINT", "")
    assert "requirements/compat-py" in constraint, (
        "snippets-lts must install under a per-interpreter environment lock, "
        "same as the compat job's promise lanes"
    )
    assert lts.get("continue-on-error") is not True, (
        "snippets-lts is a PROMISE lane — continue-on-error would make a "
        "broken snippet silently non-blocking, defeating its purpose"
    )


def test_snippets_lanes_cover_every_registered_engine():
    """The gallery-snippets twin of `test_compat_lane_covers_every_registered_engine`
    below. `run_snippets.sh -e ENGINE` forwards to `--set deployment.engine=ENGINE`
    (see scripts/run_snippets.sh), so a matrix job covers an engine only if that
    engine name actually appears in `strategy.matrix.engine` — a job that just
    runs the script with no `-e` flag exercises whatever each snippet's own
    aqueduct.yml defaults to (today: spark for every snippet), never the other
    registered engine, no matter how many times it runs.

    Both `snippets` (the unpinned canary) and `snippets-lts` (the pinned,
    blocking lane) must cover every engine — a DuckDB-only regression must be
    visible on both the early-warning lane and the promise lane, exactly like
    Spark already is. The engine list is resolved from the registry
    (`_registered_engines()`, shared with the `compat`-lane guard below), never
    hardcoded, so a third engine registered tomorrow is covered here with no
    edit to this test.
    """
    engines = _registered_engines()
    wf = _workflow("version-matrix.yml")
    for job_name in ("snippets", "snippets-lts"):
        matrix_engines = ((wf["jobs"][job_name].get("strategy") or {}).get("matrix") or {}).get(
            "engine", []
        )
        for engine in engines:
            assert engine in matrix_engines, (
                f"engine {engine!r} is registered (aqueduct.engines entry point) "
                f"but version-matrix.yml's {job_name!r} job's strategy.matrix.engine "
                f"({matrix_engines!r}) does not include it. Add it to the matrix "
                "(and forward it via `run_snippets.sh -e ${{ matrix.engine }}`) so "
                f"the gallery snippets are actually exercised against {engine!r}, "
                "not just whichever engine each snippet's own aqueduct.yml defaults to."
            )


def _compat_run_tests_script() -> str:
    """The `compat` job's ``Run tests`` step's shell script (its pytest invocation)."""
    steps = _workflow("version-matrix.yml")["jobs"]["compat"]["steps"]
    for step in steps:
        if step.get("name") == "Run tests":
            return step["run"]
    raise AssertionError(
        "version-matrix.yml's `compat` job has no step named 'Run tests' — "
        "did it get renamed? Update this test's step-name lookup."
    )


def test_compat_lane_covers_every_registered_engine():
    """A shipped engine with ZERO compat-matrix coverage is exactly the gap this
    test exists to close — DuckDB registered an `aqueduct.engines` entry point,
    shipped a full `capabilities.yml`, and an `ExecutorProtocol`, and STILL had
    no coverage in `version-matrix.yml`'s `compat` job, because that job's test
    selection was two hardcoded engine names (a path list + `-m "spark"`) that
    nothing kept in sync with the engine registry.

    The engine list is resolved from the REGISTRY — ``load_engines()`` /
    ``CAPABILITY_REGISTRY``, the exact seam ``aqueduct.executor.capabilities.
    get_capabilities()`` uses on the real compile path — never a hardcoded name
    list here. A third engine registered tomorrow is covered by this guard with
    no edit to this test.

    Mechanism: pytest marker name == engine name. This is not a convention this
    test invents — ``pyproject.toml``'s ``[tool.pytest.ini_options] markers``
    already documents ``spark`` as "Tests that require a SparkSession" and
    ``duckdb`` as "Tests that require the DuckDB execution engine", and
    ``tests/conftest.py::pytest_collection_modifyitems`` auto-applies exactly
    one of them per test from its fixtures. So "does the compat job's `-m`
    marker expression POSITIVELY select this engine" is a direct, mechanical
    check — no heuristic path-matching that a differently-organized test
    directory could quietly defeat.

    Coverage means POSITIVE selection, via ``_positive_marker_terms`` (shared
    with ``test_every_registered_engine_has_a_pre_merge_lane_in_test_suite_yml``
    below): a bare word-boundary search against the raw marker expression
    would call ``"not duckdb"`` covered, when that expression EXCLUDES duckdb
    — exactly the tautology found and fixed in this file's `test-suite.yml`
    twin. Today's expression (`"spark or duckdb"`) is purely positive, so this
    was not yet observably red, but the old regex-only check would have
    silently passed the moment anyone rewrote it with a `not <engine>` clause.
    """
    engines = _registered_engines()

    run_script = _compat_run_tests_script()
    m = re.search(r'-m\s+"([^"]+)"', run_script)
    assert m, (
        'compat job\'s pytest invocation has no `-m "..."` marker expression '
        "to check engine coverage against"
    )
    marker_expr = m.group(1)
    positive_terms = _positive_marker_terms(marker_expr)

    for engine in engines:
        assert engine in positive_terms, (
            f"engine {engine!r} is registered (aqueduct.engines entry point) but "
            f"the compat job's marker expression ({marker_expr!r}) does not "
            f"POSITIVELY select @pytest.mark.{engine} tests (a `not {engine}` "
            "clause does not count — that EXCLUDES it). Extend "
            "version-matrix.yml's `compat` job (its test path list and `-m` "
            f"marker expression) so the {engine!r} engine is actually "
            "exercised by the compatibility matrix — see AGENTS.md's 'Adding "
            "an engine' playbook, step 4."
        )


def test_every_registered_engine_has_a_pre_merge_lane_in_test_suite_yml():
    """The `test-suite.yml` twin of `test_compat_lane_covers_every_registered_engine`
    above — same bug class, different file. DuckDB registered an
    `aqueduct.engines` entry point, shipped a full `capabilities.yml`, an
    `ExecutorProtocol`, AND its own test directory
    (`tests/test_executor_duckdb/`), and STILL had zero occurrences of "duckdb"
    anywhere in `test-suite.yml`. The only catch-all was the `coverage` job,
    gated `github.ref == 'refs/heads/main' || workflow_dispatch` — main-only —
    so a break in the DuckDB engine was caught AFTER merge, never before.

    The engine list is resolved from the registry (`_registered_engines()`,
    shared with the compat-lane guard above), never a hardcoded name list — a
    third engine registered tomorrow is covered here with no edit to this
    test. "Covered" means: some PRE-merge job (`_is_pre_merge_job` — NOT a job
    gated solely on `main`/`workflow_dispatch`, like `coverage`) has a pytest
    invocation whose PATH TEXT names the engine (e.g. running
    `tests/test_executor_duckdb/`) or whose `-m` marker expression POSITIVELY
    selects it (`-m spark`, for the Spark engine's `executor-tests` job).

    Positively is load-bearing: `-m "not spark"` appears on most lanes in this
    file (they deliberately EXCLUDE Spark tests) — naive substring-matching
    "spark" anywhere in the corpus would call that coverage, when it is the
    opposite. `_positive_coverage` strips the `-m` clause out of the path text
    and separately tracks only non-negated marker terms so a lane that
    excludes an engine can never be mistaken for a lane that covers it.
    """
    engines = _registered_engines()
    path_corpus, positive_terms = _positive_coverage(_pre_merge_pytest_commands())

    for engine in engines:
        covered = engine in path_corpus or engine in positive_terms
        assert covered, (
            f"engine {engine!r} is registered (aqueduct.engines entry point) "
            "but no PRE-merge job in test-suite.yml invokes pytest against "
            "its tests — a job whose `if:` is gated solely on `github.ref == "
            "'refs/heads/main'` / workflow_dispatch (like `coverage`) does "
            "NOT count, because that only catches a break AFTER merge, and a "
            "lane that only NEGATES the engine's marker (`-m \"not "
            f'{engine}"`) does not count either. Add a lane shaped like the '
            "other `*-tests` jobs (copy `parser-tests`), gated on a `changes` "
            "path filter covering the engine's source + test directories — "
            "see AGENTS.md's 'Adding an engine' playbook."
        )


def test_no_test_directory_is_absent_from_every_pre_merge_lane():
    """Companion structural guard, independent of the engine registry: no
    `tests/test_*/` directory may go completely unreferenced by every
    PRE-merge lane in `test-suite.yml`. `tests/test_typehub/` and
    `tests/test_deploy/` were exactly this gap — neither is engine-registry
    tracked, so the guard above wouldn't have caught them either, yet both
    were only ever reachable through the main-only `coverage` job.

    A directory that is genuinely meant to stay main-only (or fully out of
    CI) must be named in `_MAIN_ONLY_TEST_DIR_ALLOWLIST` with a reason —
    silence is not a valid way to opt out, it's the bug.

    Uses the same `path_corpus` as the engine guard above (the `-m` clause
    stripped out before matching) even though a directory path could never
    literally appear inside a marker expression (markers don't contain `/`) —
    sharing the helper keeps both guards provably immune to the same class of
    blindness rather than relying on that argument staying true by accident.
    """
    path_corpus, _ = _positive_coverage(_pre_merge_pytest_commands())
    test_dirs = sorted(
        p.relative_to(_REPO).as_posix()
        for p in (_REPO / "tests").iterdir()
        if p.is_dir() and p.name.startswith("test_")
    )
    assert test_dirs, "no tests/test_*/ directories found — did the layout move?"

    for test_dir in test_dirs:
        if test_dir in _MAIN_ONLY_TEST_DIR_ALLOWLIST:
            continue
        assert f"{test_dir}/" in path_corpus, (
            f"{test_dir}/ is not referenced by any PRE-merge job in "
            "test-suite.yml, so it is only ever exercised after merge (or "
            "never). Either add a lane for it (copy `parser-tests`'s shape), "
            "or, if this is deliberate, add it to "
            "_MAIN_ONLY_TEST_DIR_ALLOWLIST above with a reason."
        )

    for allowlisted in _MAIN_ONLY_TEST_DIR_ALLOWLIST:
        assert (_REPO / allowlisted).is_dir(), (
            f"{allowlisted!r} in _MAIN_ONLY_TEST_DIR_ALLOWLIST no longer "
            "exists as a directory — remove the stale entry"
        )


# ── File-level lane coverage (Phase 081/082 cross-engine remediation) ────────
# The directory-level guard above strips the `-m` clause before matching on
# purpose (see its own docstring) — which means it calls a directory
# "referenced" even when EVERY file inside it is actually excluded by that
# directory's one lane's marker filter. That blind spot is exactly how this
# repo shipped ~191 tests across 11 files that ran in NO pre-merge lane:
# spark-marked tests living outside tests/test_executor/ (excluded by every
# `-m "not spark"` lane, and never reached by the one `-m spark` lane, which
# only points at tests/test_executor/ + test_blueprints.py), AND — the mirror
# image, found while re-verifying that list instead of trusting it — plain
# `unit`-marked tests trapped INSIDE tests/test_executor/ and tests/test_stores/,
# whose only pre-merge lane filtered `-m spark` / `-m integration` respectively.
#
# This guard closes both directions at FILE granularity: a file counts as
# covered only if some pre-merge command's path args reach it AND that
# command's `-m` expression would actually select at least one of the
# markers the file declares.

_LAYER_VOCAB = frozenset(
    {"unit", "integration", "e2e", "spark", "duckdb", "agent", "airflow", "todo"}
)
# Mirrors tests/conftest.py::_LAYER_OR_CAP verbatim. Duplicated, not imported
# — importing tests.conftest for this one constant would run its module-level
# `_spark_is_healthy()` call (a REAL `SparkSession.builder...getOrCreate()`)
# as a side effect whenever pyspark happens to be importable, which this
# fast `unit` guard must never trigger just to read a name list.


def _explicit_file_markers(text: str) -> set[str]:
    """Every `pytest.mark.<name>` token in `text` that's in the layer/cap
    vocabulary — module-level `pytestmark`, per-function decorators, and
    `pytest.param(..., marks=pytest.mark.X)` all match the same regex, since
    all this needs is "could ANY test in this file carry this marker",
    not which one. Markers outside the vocabulary (`slow`, `parametrize`,
    `skipif`, `usefixtures`, ...) are dropped by the set intersection.
    """
    return set(re.findall(r"pytest\.mark\.(\w+)", text)) & _LAYER_VOCAB


_DEF_TEST_RE = re.compile(r"^\s*def (test_\w+)")
_DECORATOR_RE = re.compile(r"^\s*@")


def _file_marker_groups(text: str) -> list[frozenset[str]]:
    """The DISTINCT per-test marker combinations that actually occur in this
    file: the module-level `pytestmark` line(s) (applied to EVERY test),
    unioned per test function with that function's own immediately-preceding
    `@pytest.mark.*` decorators — pytest ADDS decorator markers to the
    module-level ones, it never replaces them. A function with no decorator
    of its own just carries the module-level combination (or the implicit
    `{"unit"}` default — see `test_no_test_file_relies_on_fixture_only_
    auto_marking` for the one case this static scan can't see).

    Returns the deduplicated set of these per-test combinations as a list of
    frozensets, so `_marker_expr_selects` only has to find ONE combination a
    lane's `-m` expression accepts for the whole file to count as covered.

    Critically, this is NOT the same as flattening every marker in the file
    into one set. `pytestmark = [pytest.mark.spark, pytest.mark.integration]`
    means every test carries BOTH tags TOGETHER — `-m "not spark"` excludes
    the whole file, full stop; the `integration` tag doesn't buy it a way
    back in, because no test in the file is integration-WITHOUT-also-spark.
    A flat union (`{"spark", "integration"} - {"spark"} = {"integration"}`,
    non-empty) would have wrongly concluded a `-m "not spark"` lane covers
    such a file — which is exactly the ORIGINAL bug this guard exists to
    catch: `tests/test_surveyor/test_surveyor.py` and five sibling files were
    marked precisely `[spark, integration]` and reachable by NO lane at all,
    and a flat-union version of this check would have missed it.
    """
    lines = text.splitlines()
    base: set[str] = set()
    for line in lines:
        if line.strip().startswith("pytestmark"):
            base |= set(re.findall(r"pytest\.mark\.(\w+)", line)) & _LAYER_VOCAB

    groups: set[frozenset[str]] = set()
    for i, line in enumerate(lines):
        if not _DEF_TEST_RE.match(line):
            continue
        decor: set[str] = set()
        j = i - 1
        while j >= 0 and _DECORATOR_RE.match(lines[j]):
            decor |= set(re.findall(r"pytest\.mark\.(\w+)", lines[j])) & _LAYER_VOCAB
            j -= 1
        groups.add(frozenset(base | decor) or frozenset({"unit"}))

    if not groups:
        # No top-level-matched `def test_...` line at all (e.g. every test
        # lives as an indented method the regex still matches via `^\s*def`,
        # so in practice this only fires for a file with zero test functions)
        # — fall back to the file-wide base combination as the one group.
        groups.add(frozenset(base) or frozenset({"unit"}))
    return list(groups)


def _parse_pytest_command(cmd: str) -> tuple[list[str], "str | None"]:
    """One `pytest ...` line -> (its `tests/...` path args, its `-m` marker
    expression or None). Sibling of `_positive_coverage` above, but keeps
    each command's path list and marker expression paired instead of pooling
    them across every lane — file-level coverage needs to know which
    SPECIFIC command's `-m` clause a candidate path was matched under, not
    just whether the marker showed up somewhere in the whole workflow.
    """
    marker_re = re.compile(r'-m\s+"([^"]+)"|-m\s+(\S+)')
    m = marker_re.search(cmd)
    marker_expr = None
    rest = cmd
    if m:
        marker_expr = m.group(1) or m.group(2)
        rest = cmd[: m.start()] + cmd[m.end() :]
    paths = [tok for tok in rest.split() if tok.startswith("tests/")]
    return paths, marker_expr


def _marker_expr_selects_group(marker_expr: "str | None", group: frozenset[str]) -> bool:
    """True if a lane whose `-m` clause is `marker_expr` (None = no filter)
    would run a test carrying EXACTLY the marker combination `group`.
    Restricted to the flat `<term>` / `not <term>` grammar every `-m` clause
    in this workflow actually uses — same documented restriction as
    `_positive_marker_terms` above, not a general boolean-expression
    evaluator. Membership is checked against `group` directly (never a set
    difference against some flattened superset) — see `_file_marker_groups`
    for why that distinction is load-bearing: a `not spark` lane must reject
    a group that contains `spark` outright, regardless of what else is in
    that SAME group.
    """
    if not marker_expr:
        return True
    expr = marker_expr.strip()
    if expr.startswith("not "):
        return expr[4:].strip() not in group
    return expr in group


def _path_covers_file(path_arg: str, file_rel: str) -> bool:
    """True if pytest path argument `path_arg` (a file or a directory,
    exactly as written in a `run:` step — always `tests/...`) would collect
    `file_rel` (also `tests/...`)."""
    if path_arg == file_rel:
        return True
    prefix = path_arg if path_arg.endswith("/") else path_arg + "/"
    return file_rel.startswith(prefix)


def test_no_test_file_relies_on_fixture_only_auto_marking():
    """Guards the one blind spot `_file_marker_groups` documents: a file that
    uses the real `spark` or `duckdb_con` fixture but declares NO explicit
    `pytest.mark.*` anywhere would be auto-promoted away from `unit` by
    `tests/conftest.py::pytest_collection_modifyitems` at real collection
    time — invisibly, to this static scan. `test_every_test_file_is_selected_
    by_some_pre_merge_lane` below would then misjudge such a file as `unit`
    and could wrongly call it "covered" by a lane that never actually reaches
    a Spark/DuckDB-only test inside it. Keeping this list empty is what makes
    that guard's static approximation trustworthy instead of merely
    plausible.
    """
    offenders = []
    for path in sorted((_REPO / "tests").rglob("test_*.py")):
        text = path.read_text(encoding="utf-8")
        if _explicit_file_markers(text):
            continue
        uses_spark_fixture = re.search(r"def test_[^\n(]*\([^)]*\bspark\b", text)
        uses_duckdb_fixture = "duckdb_con" in text
        if uses_spark_fixture or uses_duckdb_fixture:
            offenders.append(path.relative_to(_REPO).as_posix())
    assert not offenders, (
        f"{offenders} use the `spark`/`duckdb_con` fixture but declare no "
        "explicit pytest.mark.* anywhere in the file — add one (matching "
        "whatever tests/conftest.py would auto-assign: `spark` or `duckdb`) "
        "so the file-level lane-coverage guard's static marker scan can see "
        "it too."
    )


def test_every_test_file_is_selected_by_some_pre_merge_lane():
    """The FILE-granularity, marker-aware version of
    `test_no_test_directory_is_absent_from_every_pre_merge_lane` above — see
    the block comment before `_LAYER_VOCAB` for the bug class this closes
    (spark-marked tests stranded outside the one `-m spark` lane, and
    unit-marked tests stranded inside it or inside the one `-m integration`
    lane). A file is covered if, for at least one pre-merge command, its
    path is reached by that command's path args AND that command's `-m`
    expression would select at least one of the file's actual per-test
    marker combinations (`_file_marker_groups` — never a flattened union of
    every marker anywhere in the file; see its docstring for why that
    distinction is exactly what the original bug needed to hide behind).

    Deliberately parses `test-suite.yml` + the files themselves rather than
    hardcoding which lane covers which file — a hardcoded list is exactly
    the drift this repo has been bitten by repeatedly (see the `changes:`
    path-filter drift this whole guard family exists to catch, and AGENTS.md
    on hand-maintained lists). The one static-analysis gap (fixture-only
    auto-marking) is independently asserted empty by the test above, so this
    guard's approximation stays honest without needing to actually run
    pytest collection (which would need every lane's extras installed in
    whatever environment runs THIS test, including ones — Spark, MinIO,
    Postgres — this fast `unit` lane deliberately doesn't have).
    """
    commands = _pre_merge_pytest_commands()
    assert commands, "no pre-merge pytest commands found in test-suite.yml"
    parsed = [_parse_pytest_command(cmd) for cmd in commands]

    test_files = sorted(
        p.relative_to(_REPO).as_posix() for p in (_REPO / "tests").rglob("test_*.py")
    )
    assert test_files, "no tests/**/test_*.py files found — did the layout move?"

    for file_rel in test_files:
        if any(
            file_rel == allowlisted or file_rel.startswith(allowlisted + "/")
            for allowlisted in _MAIN_ONLY_TEST_DIR_ALLOWLIST
        ):
            continue
        groups = _file_marker_groups((_REPO / file_rel).read_text(encoding="utf-8"))
        covered = any(
            any(_marker_expr_selects_group(marker_expr, g) for g in groups)
            and any(_path_covers_file(p, file_rel) for p in paths)
            for paths, marker_expr in parsed
        )
        assert covered, (
            f"{file_rel} (per-test marker combinations {sorted(map(sorted, groups))!r}) "
            "is not selected by ANY pre-merge job in test-suite.yml once "
            "both its path and its own pytest.mark.* markers are accounted "
            "for — it only ever "
            "runs post-merge (`coverage`) or at release time. Either add its "
            "path to a lane whose `-m` filter accepts one of those markers "
            "(see AGENTS.md's CI-lane table), fix a stale marker if the file "
            "is actually mislabelled, or — if this is genuinely deliberate — "
            "add it to _MAIN_ONLY_TEST_DIR_ALLOWLIST with a reason."
        )


# ── Extra-gated test paths must be named by a lane that installs the extra ──
# (Phase 84.) The bug class: a file/directory whose behavior is gated on an
# optional package at IMPORT time (module-level `pytest.importorskip(...)`,
# or a module-level `try: import X / except ImportError:` fallback) still
# gets picked up by every guard above as long as SOME pre-merge lane's pytest
# path args reach it — even when that lane never installs X. Two very
# different failure shapes hide behind that gap, and this guard is the only
# one that catches either:
#   - `tests/test_airflow.py` falls back to MOCK Operator/Sensor/Trigger
#     classes and runs green either way, so a lane that never installs real
#     airflow silently tests nothing real — a fake-green, not a skip.
#   - `tests/test_dashboard/` (see its allowlist entry) calls
#     `pytest.importorskip`, which honestly SKIPS instead of faking a pass —
#     lower severity, but still only meaningful in a lane that installs the
#     extra; it is exempted here, same as everywhere else in this file, via
#     `_MAIN_ONLY_TEST_DIR_ALLOWLIST` because it has NO pre-merge lane at all.
#
# Detection is restricted to MODULE-LEVEL (column-0) constructs on purpose —
# a `pytest.importorskip(...)` or `try/except ImportError` nested inside an
# individual test function is a normal per-test soft-skip (e.g.
# `tests/test_cli/test_cli.py`'s `pytest.importorskip("psycopg2")` inside one
# test), not a claim about the whole file/directory, and every such case in
# this repo already lands inside a directory some OTHER lane installs the
# extra for anyway. Column-0 is also exactly the shape the two real bugs
# above take: a whole-file/module decision made once at collection time.
#
# The extra a gated import maps to is resolved from `pyproject.toml`'s own
# `[project.optional-dependencies]` table — never a hardcoded package->extra
# dict (see the module-level guidance in this file and in AGENTS.md) — by
# normalized substring match against each extra's real (non-aggregate)
# dependency specs, and aggregate extras (`aqueduct-core[x]` cross-refs,
# e.g. `all` -> `schedulers` -> `airflow`) are expanded the same way, purely
# by parsing those cross-refs out of the table itself.

_AGGREGATE_EXTRA_RE = re.compile(r"aqueduct-core\[([\w-]+)\]")
_MODULE_LEVEL_IMPORTORSKIP_RE = re.compile(
    r'^(?:\w+\s*=\s*)?pytest\.importorskip\(\s*["\']([\w.]+)["\']'
)
_TRY_BLOCK_IMPORT_RE = re.compile(r"^\s*(?:import|from)\s+([\w.]+)")


def _optional_dependencies() -> dict[str, list[str]]:
    data = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
    return data["project"]["optional-dependencies"]


# `[project.optional-dependencies]` extras, keyed to the exact category
# vocabulary in AGENTS.md's "Packaging & Extras Policy". Nothing reads this
# section of pyproject.toml anywhere else in the test suite — adding a
# feature-named extra (`[drift]`, `[openlineage]`, ...) without updating both
# the policy prose AND this set would otherwise ship silently. Each set below
# is a closed, explicit list on purpose: the whole point of a falsifiable
# guard is that growing the vocabulary requires touching the test, not that
# the test infers the vocabulary from whatever pyproject.toml happens to say.
_PER_VENDOR_LEAVES = {"aws", "gcp", "azure", "postgres", "redis", "airflow", "object-store"}
_CAPABILITY_AGGREGATES = {"secrets", "stores", "schedulers", "all"}
# Engine leaves: register an `aqueduct.engines` entry point and ship their
# own capabilities.yml — see AGENTS.md's "Engine leaves" bullet. Not vendor
# SDKs, not dev tooling, and (deliberately) not rolled into any aggregate.
_ENGINE_LEAVES = {"spark", "duckdb"}
# Dev-tooling extras: the one documented exception to the two-axis rule.
# Never a runtime-capability dep, never in `all`.
_DEV_TOOLING_EXTRAS = {"dev", "dashboard", "mcp"}

# Runtime-capability leaves that must roll up into a capability aggregate —
# per-vendor leaves belonging to `secrets`/`stores`/`schedulers`. Engine
# leaves are deliberately excluded (see AGENTS.md: no "engines" aggregate).
_AGGREGATE_MEMBERSHIP = {
    "secrets": {"aws", "gcp", "azure"},
    "stores": {"postgres", "redis", "object-store"},
    "schedulers": {"airflow"},
}


def _load_pyproject_extras() -> dict[str, list[str]]:
    import tomllib

    data = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
    return data["project"]["optional-dependencies"]


def _module_level_gated_imports(text: str) -> set[str]:
    """Top-level module names gated by a COLUMN-0 `pytest.importorskip(...)`
    or `try: import X ... except ImportError:` in `text`. Only the first
    dotted segment is kept (`pyspark.sql` -> `pyspark`) since that's the
    distributable unit an extra actually installs."""
    names: set[str] = set()
    lines = text.splitlines()
    for line in lines:
        m = _MODULE_LEVEL_IMPORTORSKIP_RE.match(line)
        if m:
            names.add(m.group(1).split(".")[0])
    for i, line in enumerate(lines):
        if not line.startswith("except ImportError"):
            continue
        j = i - 1
        while j >= 0 and not lines[j].startswith("try:"):
            j -= 1
        if j < 0:
            continue
        for k in range(j + 1, i):
            m = _TRY_BLOCK_IMPORT_RE.match(lines[k])
            if m:
                names.add(m.group(1).split(".")[0])
    return names


def _leaf_extras_for_import(import_name: str, optional_deps: dict[str, list[str]]) -> set[str]:
    """Extra names that directly declare a REAL (non-aggregate) dependency
    matching `import_name`, via normalized ('-', '_', '.' stripped,
    lowercased) substring match against that dependency's bare distribution
    name — e.g. `airflow` matches `apache-airflow` (the `airflow` extra's
    real payload), `psycopg2` matches `psycopg2-binary` (the `postgres`
    extra's). An import with no match anywhere (almost always an internal
    `aqueduct.*` fallback import, e.g. `tests/test_redaction.py`'s
    `try: from aqueduct.surveyor.surveyor import Surveyor`) yields an empty
    set — such imports are not extra-gating at all and are silently dropped,
    exactly as intended."""
    norm_imp = re.sub(r"[-_.]", "", import_name.lower())
    if not norm_imp:
        return set()
    leaves: set[str] = set()
    for extra, deps in optional_deps.items():
        for dep in deps:
            if _AGGREGATE_EXTRA_RE.fullmatch(dep.strip()):
                continue
            pkg_m = re.match(r"[A-Za-z0-9_.\-]+", dep.strip())
            if not pkg_m:
                continue
            norm_pkg = re.sub(r"[-_.]", "", pkg_m.group().lower())
            if norm_pkg and (norm_imp in norm_pkg or norm_pkg in norm_imp):
                leaves.add(extra)
                break
    return leaves


def _leaf_extras_reachable(extra_names: set[str], optional_deps: dict[str, list[str]]) -> set[str]:
    """Every extra name in `extra_names` (or reachable from one of them by
    following `aqueduct-core[x]` aggregate cross-refs, e.g. `all` ->
    `schedulers` -> `airflow`) that carries at least one REAL dependency —
    i.e. an extra that, once resolved, actually installs a real package, not
    just a name that only re-exports other extras. Purely graph-derived from
    `optional_deps` itself; no part of this is a hardcoded extra hierarchy."""
    seen: set[str] = set()
    stack = list(extra_names)
    leaves: set[str] = set()
    while stack:
        cur = stack.pop()
        if cur in seen or cur not in optional_deps:
            continue
        seen.add(cur)
        has_real = False
        for dep in optional_deps[cur]:
            m = _AGGREGATE_EXTRA_RE.fullmatch(dep.strip())
            if m:
                stack.append(m.group(1))
            else:
                has_real = True
        if has_real:
            leaves.add(cur)
    return leaves


def _job_pip_install_extras(job: dict) -> set[str]:
    extras: set[str] = set()
    for step in job.get("steps", []):
        for line in step.get("run", "").splitlines():
            m = re.search(r'pip install -e "\.\[([^\]]+)\]"', line.strip())
            if m:
                extras |= {e.strip() for e in m.group(1).split(",")}
    return extras


def _job_pytest_path_args(job: dict) -> list[str]:
    paths: list[str] = []
    for step in job.get("steps", []):
        for line in step.get("run", "").splitlines():
            line = line.strip()
            if line.startswith("pytest "):
                p, _ = _parse_pytest_command(line)
                paths.extend(p)
    return paths


def _covers_entity(path_arg: str, entity_rel: str) -> bool:
    """True if pytest path argument `path_arg` reaches `entity_rel` — either
    because `path_arg` IS `entity_rel` (file or directory, with or without a
    trailing slash), or because `path_arg` names something INSIDE it (a
    specific file inside a directory entity, e.g. `executor-tests` naming
    `tests/test_surveyor/test_probe.py` while the entity is the whole
    `tests/test_surveyor` directory)."""
    if path_arg == entity_rel or path_arg == entity_rel + "/":
        return True
    return path_arg.startswith(entity_rel + "/")


def test_extra_gated_test_path_is_named_by_a_lane_that_installs_that_extra():
    """Every top-level `tests/test_*` directory / loose `tests/test_*.py`
    file that gates on an optional extra at module level must be named by a
    PRE-merge job in `test-suite.yml` whose `pip install -e ".[...]"` line
    actually installs that extra (directly, or via an aggregate like `all`).

    This is the guard that would have caught `tests/test_airflow.py`: it was
    named by `misc-tests` (`pip install -e ".[dev,llm,redis]"` — no airflow),
    so the file ran green against its own MOCK fallback classes on every
    pre-merge push, never against real Airflow.
    """
    optional_deps = _optional_dependencies()
    jobs = [job for job in _workflow("test-suite.yml")["jobs"].values() if _is_pre_merge_job(job)]
    assert jobs, "no pre-merge jobs found in test-suite.yml"

    entities: list[tuple[str, list[Path]]] = []
    for p in sorted((_REPO / "tests").iterdir()):
        if p.is_dir() and p.name.startswith("test_"):
            rel = p.relative_to(_REPO).as_posix()
            if rel in _MAIN_ONLY_TEST_DIR_ALLOWLIST:
                continue
            entities.append((rel, sorted(p.rglob("*.py"))))
        elif p.is_file() and p.name.startswith("test_") and p.suffix == ".py":
            entities.append((p.relative_to(_REPO).as_posix(), [p]))
    assert entities, "no tests/test_* directories or files found — did the layout move?"

    for rel, files in entities:
        gated_imports: set[str] = set()
        for f in files:
            gated_imports |= _module_level_gated_imports(f.read_text(encoding="utf-8"))
        gated_extras: set[str] = set()
        for imp in gated_imports:
            gated_extras |= _leaf_extras_for_import(imp, optional_deps)
        if not gated_extras:
            continue

        for extra in sorted(gated_extras):
            covered = any(
                any(_covers_entity(p, rel) for p in _job_pytest_path_args(job))
                and extra in _leaf_extras_reachable(_job_pip_install_extras(job), optional_deps)
                for job in jobs
            )
            assert covered, (
                f"{rel} is gated at module level (pytest.importorskip / "
                f"try-except ImportError) on something that maps to the "
                f"{extra!r} extra, but no pre-merge job in test-suite.yml "
                f"both runs pytest against {rel} AND installs the {extra!r} "
                "extra (directly, or via an aggregate like `all`). Either "
                f"add {extra!r} (or an aggregate that reaches it) to the "
                f'`pip install -e ".[...]"` line of whichever job already '
                f"names {rel}, or add a new lane for it (copy the "
                "`all-extras-tests` shape), or — if this is genuinely "
                "deliberate — add it to _MAIN_ONLY_TEST_DIR_ALLOWLIST with a "
                "reason."
            )


def test_pyproject_extras_match_the_packaging_policy_categories():
    """Every extra in `[project.optional-dependencies]` must fall into one of
    the four explicit categories AGENTS.md's "Packaging & Extras Policy"
    names: per-vendor leaf, capability aggregate, engine leaf, or the
    documented dev-tooling exception. An extra outside all four is exactly
    the feature-named-extra drift the policy forbids — this guard exists so
    that drift fails CI instead of just prose review.

    `databricks` currently exists in this worktree's pyproject.toml but is
    slated for removal alongside the rest of the Databricks deploy layer on
    the group branch (out of scope here); it is not added to any category
    set below on purpose, so it stays visible as a named failure rather than
    being quietly allow-listed.
    """
    extras = _load_pyproject_extras()
    known = _PER_VENDOR_LEAVES | _CAPABILITY_AGGREGATES | _ENGINE_LEAVES | _DEV_TOOLING_EXTRAS
    unknown = sorted(set(extras) - known - {"databricks"})
    assert not unknown, (
        f"extra(s) {unknown!r} in pyproject.toml's [project.optional-dependencies] "
        "are not named in any of AGENTS.md's Packaging & Extras Policy categories "
        "(per-vendor leaf / capability aggregate / engine leaf / dev-tooling "
        "exception). Map the new extra onto an existing axis, or update the "
        "policy AND this test's category sets together if it is genuinely new."
    )


def test_pyproject_aggregate_extras_include_every_member_leaf():
    """The other direction of the same guard: an aggregate that DOESN'T list
    one of its own member leaves is a silent under-install, not a syntax
    error — `aqueduct-core[secrets]` would quietly stop covering a vendor.
    Checked against the exact `aqueduct-core[<leaf>]` self-reference spelling
    the aggregates already use in pyproject.toml.
    """
    extras = _load_pyproject_extras()
    for aggregate, members in _AGGREGATE_MEMBERSHIP.items():
        declared = set(extras[aggregate])
        for member in members:
            expected = f"aqueduct-core[{member}]"
            assert expected in declared, (
                f"[{aggregate}] is missing '{expected}' — every per-vendor leaf "
                f"in {sorted(members)!r} must appear in its aggregate."
            )


def test_pyproject_dev_tooling_extras_stay_out_of_all():
    """Dev-tooling extras are the documented exception to the two-axis rule
    specifically BECAUSE they never run in the data path — `all` bundling
    them would bloat headless Spark-driver / CI installs. Guards against the
    dev-tooling carve-out quietly regressing into a runtime axis."""
    extras = _load_pyproject_extras()
    all_members = set(extras["all"])
    for name in _DEV_TOOLING_EXTRAS:
        leaked = f"aqueduct-core[{name}]"
        assert leaked not in all_members, (
            f"dev-tooling extra '{name}' appears inside [all] — dev-tooling "
            "extras are documented as staying OUT of all/the runtime axes."
        )
