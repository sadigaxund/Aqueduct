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

from pathlib import Path

import pytest
import yaml

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
