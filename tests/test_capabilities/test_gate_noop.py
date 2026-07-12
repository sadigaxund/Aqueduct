"""Phase 78 — the compile-time capability gate is a no-op for every shipped
gallery snippet, because Spark declares default-ALLOW for the whole grammar.

Reuses the same discovery pattern as ``tests/test_gallery.py``
(``gallery/snippets/*/blueprint.yml``) rather than importing that module, to
keep this test independently runnable and to avoid coupling to
``test_gallery.py``'s internal helpers.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import aqueduct.executor.spark.capabilities  # noqa: F401 — registers "spark"
from aqueduct.compiler.capability_check import check_capabilities
from aqueduct.compiler.compiler import compile as compile_bp
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SNIPPETS = sorted((_REPO / "gallery" / "snippets").glob("*/blueprint.yml"))
_ENV_TOKEN = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def _set_placeholder_env(path: Path, monkeypatch) -> None:
    for var in set(_ENV_TOKEN.findall(path.read_text(encoding="utf-8"))):
        monkeypatch.setenv(var, f"placeholder-{var.lower()}")


@pytest.mark.parametrize("bp_path", _SNIPPETS, ids=[p.parent.name for p in _SNIPPETS])
def test_capability_gate_is_noop_for_snippet(bp_path, monkeypatch):
    _set_placeholder_env(bp_path, monkeypatch)
    manifest = compile_bp(
        parse(bp_path), blueprint_path=bp_path,
        deployment_env="local", deployment_target="local",
        engine="spark",
    )
    problems = check_capabilities(manifest, engine="spark")
    assert problems == [], f"{bp_path} hit capability problems on engine=spark: {problems}"
