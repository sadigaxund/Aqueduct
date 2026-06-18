"""Gallery guard — the integration + e2e layer that keeps gallery/ from rotting.

Every shipped example is exercised so a refactor that breaks the engine's
public surface fails here instead of silently in a user's copy-paste:

* **integration** — every ``gallery/snippets/*/blueprint.yml`` parses + compiles;
  every ``*.aqtest.yml`` runs green on a real ``local[1]`` Spark; every
  ``*.aqscenario.yml`` loads.
* **e2e** — every ``gallery/showcase/*/blueprint.yml`` compiles.

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

# Showcases keep blueprints either at the top (01) or under blueprints/ (02).
# 03-self-healing is intentionally excluded — its blueprints are deliberately
# broken to demonstrate healing, so a "compiles cleanly" guard doesn't apply.
_SHOWCASES = sorted(
    p for p in [
        *(_GALLERY / "showcase").glob("*/blueprint.yml"),
        *(_GALLERY / "showcase").glob("*/blueprints/*.yml"),
    ]
    if "03-self-healing" not in p.parts
)

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
    manifest = compile_bp(parse(bp_path), blueprint_path=bp_path)
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


# ── e2e: showcase blueprints compile ──────────────────────────────────────────


@pytest.mark.e2e
@pytest.mark.parametrize("bp_path", _SHOWCASES, ids=[_id(p) for p in _SHOWCASES])
def test_showcase_blueprint_compiles(bp_path, monkeypatch):
    _set_placeholder_env(bp_path, monkeypatch)
    manifest = compile_bp(parse(bp_path), blueprint_path=bp_path)
    assert manifest.modules

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


# ── e2e: showcase 03 self-healing through mocked agent ─────────────────


@pytest.mark.e2e
def test_showcase_self_healing_heal_flow(tmp_path):
    """Exercise the heal pipeline against a showcase 03 blueprint with mocked agent.

    Creates an inline scenario that mimics the schema-drift blueprint's failure,
    runs it through ``run_scenario`` with a mocked ``generate_agent_patch``,
    and verifies the result shape.
    """
    from unittest.mock import patch

    from aqueduct.surveyor.scenario import load_scenario, run_scenario
    from aqueduct.agent import AgentPatchResult, StopReason

    sc_yaml = (
        'aqueduct_scenario: "1.0"\n'
        "id: showcase_schema_drift\n"
        "description: Phase 56 gallery guard\n"
        "blueprint: blueprints/01_schema_drift.yml\n"
        "inject_failure:\n"
        "  module: clean_events\n"
        "  error_message: |\n"
        "    AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]\n"
        "    Cannot resolve `event_ts`\n"
        "  structured:\n"
        "    error_class: UNRESOLVED_COLUMN.WITH_SUGGESTION\n"
        "    object_name: event_ts\n"
        "    suggested_columns: [event_time]\n"
        "    root_exception:\n"
        '      type: org.apache.spark.sql.AnalysisException\n'
        '      message: Cannot resolve\n'
        "grader:\n"
        "  effect: { column_ref: { module: clean_events, old: event_ts, new: event_time } }\n"
    )
    sc_path = tmp_path / "scenario.aqscenario.yml"
    sc_path.write_text(sc_yaml)
    (tmp_path / "blueprints").mkdir()
    bp_path = tmp_path / "blueprints" / "01_schema_drift.yml"
    bp_path.write_text(
        (_REPO / "gallery" / "showcase" / "03-self-healing" / "blueprints" / "01_schema_drift.yml").read_text()
    )

    scenario = load_scenario(sc_path)

    with patch("aqueduct.agent.generate_agent_patch") as m:
        m.return_value = AgentPatchResult(
            patch=None, attempts=1,
            stop_reason=StopReason.SOLVED,
        )
        result = run_scenario(scenario, model="test-model", patches_dir=tmp_path)

    assert result is not None
    assert result.scenario_id == "showcase_schema_drift"
    assert result.model == "test-model"
    assert hasattr(result, "passed")
