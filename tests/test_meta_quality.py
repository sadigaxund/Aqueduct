"""Meta-test — the suite polices its own honesty.

Fails if any ``test_*`` function verifies *nothing*: no ``assert``, no
``pytest.raises``/``warns``/``fail``/``approx``, no mock ``assert_called*``, no
warnings-as-error, and no delegation to a same-file helper that asserts. A test
that just runs code and never checks an outcome passes even when the code is
broken — that's the cheating this guard exists to catch.

Legitimate exits: mark it ``@pytest.mark.todo("why")`` (an honest backlog stub)
or ``@pytest.mark.xfail(strict=True, reason=...)`` (a known bug). Fixtures named
``test_*`` are excluded.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.unit

_TESTS_ROOT = pathlib.Path(__file__).resolve().parent

# Recognised verifications beyond a bare `assert`.
_VERIFY_TOKENS = (
    "raises", "warns", "fail", "approx", "deprecated_call",
    "assert_called", "assert_not_called", "assert_any_call", "assert_has_calls",
    ".called", "simplefilter", "catch_warnings",
)


def _has_decorator(fn: ast.FunctionDef, *needles: str) -> bool:
    return any(any(n in ast.dump(d) for n in needles) for d in fn.decorator_list)


def _verifies(fn: ast.FunctionDef) -> bool:
    if any(isinstance(x, ast.Assert) for x in ast.walk(fn)):
        return True
    dump = ast.dump(fn)
    return any(tok in dump for tok in _VERIFY_TOKENS)


def _zero_assertion_tests(path: pathlib.Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    # Same-file helpers that themselves assert → a test calling one is verified.
    asserting_helpers = {
        n.name for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and not n.name.startswith("test_") and _verifies(n)
    }
    out: list[str] = []
    for n in ast.walk(tree):
        if not (isinstance(n, ast.FunctionDef) and n.name.startswith("test_")):
            continue
        if _has_decorator(n, "fixture"):           # a fixture, not a test
            continue
        if _has_decorator(n, "todo", "xfail", "skip"):  # honest backlog / known-bug
            continue
        if _verifies(n):
            continue
        called = {
            c.func.id for c in ast.walk(n)
            if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
        }
        if called & asserting_helpers:             # delegates to an asserting helper
            continue
        out.append(f"{path.relative_to(_TESTS_ROOT)}:{n.lineno}:{n.name}")
    return out


def test_no_zero_assertion_tests():
    offenders: list[str] = []
    for f in sorted(_TESTS_ROOT.rglob("test_*.py")):
        if f.name == pathlib.Path(__file__).name:
            continue
        try:
            offenders.extend(_zero_assertion_tests(f))
        except SyntaxError:
            continue
    assert not offenders, (
        "These tests verify nothing (no assert / raises / warns / mock-assert / "
        "asserting-helper). Add a real check, or mark @pytest.mark.todo / "
        "xfail(strict=True):\n  " + "\n  ".join(offenders)
    )
