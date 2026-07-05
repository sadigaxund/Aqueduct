"""Gallery guard — the integration layer that keeps gallery/snippets/ and
gallery/aqtests/ and gallery/aqscenarios/ from rotting.

Every shipped snippet/test/scenario is exercised so a refactor that breaks
the engine's public surface fails here instead of silently in a user's
copy-paste:

* every ``gallery/snippets/*/blueprint.yml`` parses + compiles (fast, static —
  no Spark execution; catches schema/config-shape regressions, not runtime
  wiring bugs. Full end-to-end runs with real data are scripts/run_snippets.sh's
  job, in the separate `snippets` CI workflow — this file and that script are
  NOT duplicates, they catch different failure classes.)
* every ``*.aqtest.yml`` runs green on a real ``local[1]`` Spark
* every ``*.aqscenario.yml`` loads and heals correctly with a mocked agent

``gallery/showcase/**`` is intentionally NOT covered here — showcases are
interactive human-run demos (self-healing needs a real or absent LLM to be
meaningful), not CI-gated artifacts.

Parametrization is by discovery: drop a new example in the gallery and it is
guarded automatically — no edit here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from aqueduct.compiler.compiler import compile as compile_bp
from aqueduct.parser.parser import parse

_REPO = Path(__file__).resolve().parents[1]
_GALLERY = _REPO / "gallery"

_SNIPPETS = sorted((_GALLERY / "snippets").glob("*/blueprint.yml"))
_AQTESTS = sorted((_GALLERY / "aqtests").glob("*/*.aqtest.yml"))
_AQSCENARIOS = sorted((_GALLERY / "aqscenarios").glob("*.aqscenario.yml"))

_ENV_TOKEN = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def _set_placeholder_env(path: Path, monkeypatch) -> None:
    """Give every ``${VAR}`` referenced by *path* a dummy value so the guard
    tests structure, not a developer's local environment (e.g. JDBC creds)."""
    for var in set(_ENV_TOKEN.findall(path.read_text(encoding="utf-8"))):
        monkeypatch.setenv(var, f"placeholder-{var.lower()}")


def _id(p: Path) -> str:
    return p.parent.name if p.name in ("blueprint.yml",) else p.stem


# ── integration: snippets parse + compile ─────────────────────────────────────


@pytest.mark.integration
@pytest.mark.parametrize("bp_path", _SNIPPETS, ids=[_id(p) for p in _SNIPPETS])
def test_snippet_parses_and_compiles(bp_path, monkeypatch):
    _set_placeholder_env(bp_path, monkeypatch)
    # deployment_env/target default to None in compile() itself (the real
    # value normally comes from the caller's loaded aqueduct.yml, which this
    # generic compile-only guard never loads) — match config.py's own
    # DeploymentConfig defaults so blueprints using @aq.deployment.* don't
    # spuriously fail here.
    manifest = compile_bp(
        parse(bp_path), blueprint_path=bp_path,
        deployment_env="local", deployment_target="local",
    )
    assert manifest.modules, f"{bp_path} compiled to an empty manifest"


# ── integration: aqscenarios load ─────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.parametrize("sc_path", _AQSCENARIOS, ids=[p.stem for p in _AQSCENARIOS])
def test_aqscenario_loads(sc_path):
    from aqueduct.surveyor.scenario import load_scenario

    scenario = load_scenario(sc_path)
    assert scenario is not None


# ── integration: aqtests run on real Spark ────────────────────────────────────


@pytest.mark.integration
@pytest.mark.spark
@pytest.mark.parametrize("test_path", _AQTESTS, ids=[_id(p) for p in _AQTESTS])
def test_aqtest_runs_green(test_path, spark):
    from aqueduct.executor.spark.test_runner import run_test_file

    result = run_test_file(test_path, spark)
    assert result.total > 0, f"{test_path} declared no test cases"
    failed = [r.test_id for r in result.results if not r.passed]
    assert result.success, f"{test_path}: failing cases {failed}"


# ── integration: every aqscenario heals with a mocked agent ─────────────────


@pytest.mark.integration
@pytest.mark.parametrize("sc_path", _AQSCENARIOS, ids=[p.stem for p in _AQSCENARIOS])
def test_aqscenario_heals_with_mocked_agent(sc_path, tmp_path):
    from unittest.mock import patch

    from aqueduct.surveyor.scenario import load_scenario, run_scenario
    from aqueduct.agent import AgentPatchResult, StopReason

    scenario = load_scenario(sc_path)

    with patch("aqueduct.agent.generate_agent_patch") as m:
        m.return_value = AgentPatchResult(
            patch=None, attempts=1,
            stop_reason=StopReason.SOLVED,
        )
        result = run_scenario(scenario, model="test-model", patches_dir=tmp_path)

    assert result is not None
    assert result.scenario_id == scenario.id
    assert result.model == "test-model"
    assert hasattr(result, "passed")
    assert hasattr(result, "failures")
