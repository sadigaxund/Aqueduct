"""Tests for aqueduct/models.py — the cross-layer boundary re-export surface.

`aqueduct/models.py` is a PURE re-export: it imports `Manifest` (from
`compiler.models`), `Island` (from `compiler.islands`), and `Edge`/`Module`/
`ModuleType`/`RetryPolicy` (from `parser.models`) and does nothing else. The
hazard a re-export surface like this can silently develop is drop-in
equivalence drift: someone replaces `from aqueduct.compiler.models import
Manifest` with a locally-defined look-alike dataclass in `models.py`. Every
consumer that imports `aqueduct.models.Manifest` and merely constructs one
keeps passing, while the compiler still emits the REAL `compiler.models.
Manifest` — a field added upstream would then silently never reach an
executor/surveyor reader going through the `aqueduct.models` surface. These
tests pin identity (not just equality) so that drift fails loudly.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from aqueduct import models
from aqueduct.compiler.islands import Island as _CompilerIsland
from aqueduct.compiler.models import Manifest as _CompilerManifest
from aqueduct.parser.models import Edge as _ParserEdge
from aqueduct.parser.models import Module as _ParserModule
from aqueduct.parser.models import ModuleType as _ParserModuleType
from aqueduct.parser.models import RetryPolicy as _ParserRetryPolicy


def test_models_all_matches_re_exported_symbols():
    """`__all__` names exactly the 6 symbols the module imports — no orphans, no gaps."""
    assert set(models.__all__) == {
        "Edge",
        "Island",
        "Manifest",
        "Module",
        "ModuleType",
        "RetryPolicy",
    }


@pytest.mark.parametrize(
    "name, origin",
    [
        ("Manifest", _CompilerManifest),
        ("Island", _CompilerIsland),
        ("Edge", _ParserEdge),
        ("Module", _ParserModule),
        ("ModuleType", _ParserModuleType),
        ("RetryPolicy", _ParserRetryPolicy),
    ],
)
def test_models_re_export_is_identical_object(name, origin):
    """Each `aqueduct.models.<name>` IS (not just equals) its origin-module object.

    A locally-defined replacement class would be `==`-compatible for simple
    dataclasses in many tests but fails this identity check immediately,
    which is the point: this test exists to catch that substitution before
    any downstream consumer test would.
    """
    assert getattr(models, name) is origin
