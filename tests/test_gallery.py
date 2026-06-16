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

# (Heavier e2e — full self-healing run, mocked-agent scenario heals — are staged
#  in tests/test_backlog.py until the Spark+mock harness lands.)
