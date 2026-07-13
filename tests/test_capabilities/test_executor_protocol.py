"""Phase 78 Step 2 — ``ExecutorProtocol``, the engine-agnostic execution contract.

Covers the structural healing guarantee TODOs.md demands: an engine cannot
register without an error extractor (exception -> ``FailureContext`` fields)
or a prompt-rules pack (engine-specific healing system-prompt content). Also
covers ``get_executor()``'s Phase 78 Step 2 rewiring (resolves through the
``aqueduct.engines`` registry + ``ExecutorProtocol`` instead of a hardcoded
Spark-only branch) and the pyspark-free import guarantee on
``aqueduct/executor/protocol.py``.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from aqueduct.errors import AqueductError, EnginePluginError
from aqueduct.executor.protocol import (
    DeferRules,
    ExecutorProtocol,
    PromptRules,
    get_protocol,
)

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=str(_REPO),
        capture_output=True,
        text=True,
        timeout=60,
    )


# ── Structural guarantee: no extractor / no prompt_rules -> cannot register ──

_FAKE_DEFER = DeferRules(infra_examples="fake lock contention", udf_languages="Python")

_FAKE_RULES = PromptRules(
    persona="You are a fake engine repair agent.",
    root_cause_note="fake error class",
    rules="- a fake engine rule.",
    defer=_FAKE_DEFER,
)


def test_engine_without_error_extractor_cannot_register():
    with pytest.raises(EnginePluginError, match="extract_error is required"):
        ExecutorProtocol(
            engine="fake",
            execute=lambda *a, **k: None,
            extract_error=None,  # type: ignore[arg-type]
            prompt_rules=_FAKE_RULES,
        )


def test_engine_without_prompt_rules_cannot_register():
    with pytest.raises(EnginePluginError, match="prompt_rules is required"):
        ExecutorProtocol(
            engine="fake",
            execute=lambda *a, **k: None,
            extract_error=lambda exc: None,
            prompt_rules=None,  # type: ignore[arg-type]
        )


def test_engine_with_bare_string_prompt_rules_cannot_register():
    """A bare string is not a PromptRules pack — the engine must supply the
    persona / root-cause note / rules triple, not one blob of prose."""
    with pytest.raises(EnginePluginError, match="must "):
        ExecutorProtocol(
            engine="fake",
            execute=lambda *a, **k: None,
            extract_error=lambda exc: None,
            prompt_rules="just a string",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("missing", ["persona", "root_cause_note", "rules"])
def test_prompt_rules_pack_rejects_empty_field(missing):
    """Every field of the pack is required and non-empty — a half-filled pack
    silently strips engine context out of the healing prompt."""
    kwargs = {
        "persona": "You are a fake engine repair agent.",
        "root_cause_note": "fake error class",
        "rules": "- a fake engine rule.",
        "defer": _FAKE_DEFER,
    }
    kwargs[missing] = "   \n "
    with pytest.raises(EnginePluginError, match=f"PromptRules.{missing} is required"):
        PromptRules(**kwargs)


def test_prompt_rules_pack_requires_a_defer_slice():
    """The defer-to-human section is assembled at runtime, outside the template
    constant — an engine that omits its slice would silently inherit whatever
    examples the scaffold happened to hardcode. It cannot omit it."""
    with pytest.raises(EnginePluginError, match="PromptRules.defer is required"):
        PromptRules(
            persona="You are a fake engine repair agent.",
            root_cause_note="fake error class",
            rules="- a fake engine rule.",
            defer=None,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("missing", ["infra_examples", "udf_languages"])
def test_defer_rules_rejects_empty_required_field(missing):
    kwargs = {"infra_examples": "fake lock contention", "udf_languages": "Python"}
    kwargs[missing] = "  "
    with pytest.raises(EnginePluginError, match=f"DeferRules.{missing} is required"):
        DeferRules(**kwargs)


def test_defer_rules_extra_bullets_is_optional():
    """An engine with no extra defer category is complete — unlike the other
    fields, "none" is a legitimate answer here, not a missing declaration."""
    d = DeferRules(infra_examples="fake lock contention", udf_languages="Python")
    assert d.extra_bullets == ""


def test_engine_without_execute_cannot_register():
    with pytest.raises(EnginePluginError, match="execute is required"):
        ExecutorProtocol(
            engine="fake",
            execute=None,  # type: ignore[arg-type]
            extract_error=lambda exc: None,
            prompt_rules=_FAKE_RULES,
        )


def test_engine_without_name_cannot_register():
    with pytest.raises(EnginePluginError, match="non-empty string"):
        ExecutorProtocol(
            engine="",
            execute=lambda *a, **k: None,
            extract_error=lambda exc: None,
            prompt_rules=_FAKE_RULES,
        )


def test_registration_failures_are_aqueduct_errors_not_bare_builtins():
    """Error-taxonomy rule: an engine-plugin author hitting a registration
    guard is a user of this seam — the error must be an AqueductError."""
    assert issubclass(EnginePluginError, AqueductError)
    with pytest.raises(AqueductError):
        ExecutorProtocol(
            engine="fake",
            execute=lambda *a, **k: None,
            extract_error=lambda exc: None,
            prompt_rules=None,  # type: ignore[arg-type]
        )


def test_well_formed_protocol_constructs():
    """The positive case: every required field present -> no error."""
    proto = ExecutorProtocol(
        engine="fake",
        execute=lambda *a, **k: None,
        extract_error=lambda exc: None,
        prompt_rules=_FAKE_RULES,
    )
    assert proto.engine == "fake"
    assert callable(proto.execute)
    assert callable(proto.extract_error)
    assert proto.prompt_rules is _FAKE_RULES


# ── Spark's ExecutorProtocol — resolved through the real registration seam ──


def test_spark_protocol_resolves_via_get_protocol():
    proto = get_protocol("spark")
    assert proto.engine == "spark"
    assert callable(proto.execute)
    assert callable(proto.extract_error)
    assert isinstance(proto.prompt_rules, PromptRules)


def test_spark_extract_error_delegates_to_existing_structured_extractor():
    """Refactor, not new behavior — Spark's extract_error is a thin
    pass-through to error_extraction._extract_structured_error (the wrapper
    exists only to defer the pyspark import to call time; the actual logic
    is not reimplemented), verified by identical output on the same input."""
    from aqueduct.surveyor.error_extraction import _extract_structured_error

    proto = get_protocol("spark")
    exc = ValueError("boom")
    assert proto.extract_error(exc) == _extract_structured_error(exc)


def test_spark_prompt_rules_is_the_spark_owned_pack():
    """Spark's prompt-rules pack lives in the EXECUTOR layer
    (aqueduct/executor/spark/prompt_rules.py), not in the agent layer."""
    from aqueduct.executor.spark.prompt_rules import SPARK_PROMPT_RULES

    proto = get_protocol("spark")
    assert proto.prompt_rules is SPARK_PROMPT_RULES
    assert "Spark" in proto.prompt_rules.persona


def test_extract_error_returns_none_for_none_exception():
    """extract_error(None) must not raise — the no-live-exception path
    (execute() caught the failure internally and reported via ModuleResult)."""
    proto = get_protocol("spark")
    assert proto.extract_error(None) is None


def test_unknown_engine_get_protocol_raises_unknown_engine_error():
    from aqueduct.errors import UnknownEngineError

    with pytest.raises(UnknownEngineError, match="unknown engine 'bogus-engine'"):
        get_protocol("bogus-engine")


# ── get_executor() — Phase 78 Step 2 rewiring ────────────────────────────────


def test_get_executor_returns_spark_execute():
    from aqueduct.executor import get_executor
    from aqueduct.executor.protocol import get_protocol as _get_protocol

    fn = get_executor("spark")
    assert fn is _get_protocol("spark").execute


def test_get_executor_unknown_engine_raises_unknown_engine_error():
    from aqueduct.errors import UnknownEngineError
    from aqueduct.executor import get_executor

    with pytest.raises(UnknownEngineError, match="unknown engine 'bogus-engine'"):
        get_executor("bogus-engine")


# ── pyspark-free import guarantee (fresh subprocess, real block) ────────────


def test_protocol_module_importable_without_pyspark():
    """``aqueduct.executor.protocol`` must import cleanly with pyspark made
    unimportable — a fresh subprocess proves it, not just "pyspark happens to
    already be imported by the time this test runs" in-process."""
    proc = _run("""
        import sys

        class _BlockPyspark:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError("pyspark blocked for this test")
                return None

        sys.meta_path.insert(0, _BlockPyspark())

        import aqueduct.executor.protocol as protocol
        assert "pyspark" not in sys.modules
        print("OK", protocol.ExecutorProtocol)
    """)
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "OK" in proc.stdout


def test_spark_engine_module_importable_without_pyspark():
    """Importing ``aqueduct.executor.spark.engine`` (the entry-point target)
    — and therefore constructing Spark's ``ExecutorProtocol`` — must not
    require pyspark. Only calling ``.execute(...)`` should need it."""
    proc = _run("""
        import sys

        class _BlockPyspark:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError("pyspark blocked for this test")
                return None

        sys.meta_path.insert(0, _BlockPyspark())

        import aqueduct.executor.spark.engine as engine_mod
        assert "pyspark" not in sys.modules
        assert engine_mod.SPARK.engine == "spark"
        assert callable(engine_mod.SPARK.execute)
        assert callable(engine_mod.SPARK.extract_error)
        assert engine_mod.SPARK.prompt_rules.persona
        print("OK")
    """)
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "OK" in proc.stdout
