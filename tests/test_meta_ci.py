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


def _workflow(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text(encoding="utf-8"))


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


def test_compat_promise_lanes_install_under_a_per_interpreter_lock():
    jobs = _workflow("version-matrix.yml")["jobs"]
    compat = (jobs["compat"].get("env") or {}).get("PIP_CONSTRAINT", "")
    assert "requirements/compat-py" in compat
    unit = (jobs["unit-tests"].get("env") or {}).get("PIP_CONSTRAINT", "")
    assert "requirements/ci-py311.txt" in unit


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
    marker expression name this engine" is a direct, mechanical check — no
    heuristic path-matching that a differently-organized test directory could
    quietly defeat.
    """
    load_engines()
    engines = sorted(CAPABILITY_REGISTRY)
    assert engines, (
        "no engines registered via the `aqueduct.engines` entry-point group — "
        "a stale editable install? (see NO_ENGINES_HINT in capabilities.py)"
    )

    run_script = _compat_run_tests_script()
    m = re.search(r'-m\s+"([^"]+)"', run_script)
    assert m, (
        "compat job's pytest invocation has no `-m \"...\"` marker expression "
        "to check engine coverage against"
    )
    marker_expr = m.group(1)

    for engine in engines:
        assert re.search(rf"\b{re.escape(engine)}\b", marker_expr), (
            f"engine {engine!r} is registered (aqueduct.engines entry point) but "
            f"the compat job's marker expression ({marker_expr!r}) does not "
            f"select @pytest.mark.{engine} tests. Extend version-matrix.yml's "
            "`compat` job (its test path list and `-m` marker expression) so "
            f"the {engine!r} engine is actually exercised by the compatibility "
            "matrix — see AGENTS.md's 'Adding an engine' playbook, step 4."
        )
