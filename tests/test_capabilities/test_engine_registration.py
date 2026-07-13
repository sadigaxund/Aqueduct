"""Phase 78 Step 1 — the ``aqueduct.engines`` entry-point registration seam.

The bug being fixed: before Step 1, nothing on the CLI/compile path imported
``aqueduct.executor.spark.capabilities`` — only tests did (via a direct or
fixture-forced import). ``CAPABILITY_REGISTRY`` was therefore empty in a real
process, ``get_capabilities("spark")`` raised, and
``aqueduct/compiler/capability_check.py::check_capabilities`` swallowed that
into an empty list (a silent no-op capability gate). This file proves the
fix: a *fresh subprocess* that imports nothing but the public entry points
(``aqueduct.doctor.checks_io``, ``aqueduct.compiler.compiler``,
``aqueduct.config``) resolves the "spark" capability declaration with no
explicit capability import anywhere.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_PLAIN_SNIPPET = _REPO / "gallery" / "snippets" / "01_ingress_csv_options" / "blueprint.yml"


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=str(_REPO),
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_registry_populated_on_real_compile_path_no_explicit_import():
    """Regression test for the original bug: in a fresh process that only
    imports `aqueduct.doctor.checks_io` (never any `*.capabilities` module
    directly), the doctor capability check must NOT report `skip` for
    engine="spark". This is the exact reproduction from the bug report and
    fails against the pre-fix code (empty registry -> KeyError -> swallowed
    -> `skip`)."""
    proc = _run(f"""
        from pathlib import Path
        from aqueduct.doctor.checks_io import check_capabilities
        bp = Path({str(_PLAIN_SNIPPET)!r})
        results = check_capabilities(bp, engine="spark")
        statuses = {{r.status for r in results}}
        assert "skip" not in statuses or all(
            "did not parse/compile" not in r.detail and "no capability declaration" not in r.detail
            for r in results
        ), f"capability gate is a no-op: {{results}}"
        print("OK", [r.status for r in results])
    """)
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "OK" in proc.stdout


def test_registry_populated_via_compiler_only_no_explicit_import():
    """Same regression proof through the compiler entry point: compiling any
    blueprint against engine="spark" must resolve a real (non-empty)
    capability table with zero explicit capability-module imports."""
    proc = _run(f"""
        from pathlib import Path
        from aqueduct.compiler.compiler import compile as compile_bp
        from aqueduct.parser.parser import parse
        from aqueduct.executor.capabilities import CAPABILITY_REGISTRY

        bp_path = Path({str(_PLAIN_SNIPPET)!r})
        manifest = compile_bp(parse(str(bp_path)), blueprint_path=bp_path, engine="spark")
        assert "spark" in CAPABILITY_REGISTRY, CAPABILITY_REGISTRY
        print("OK")
    """)
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "OK" in proc.stdout


def test_unknown_engine_raises_unknown_engine_error_not_silent_empty():
    """get_capabilities()/check_capabilities() fail closed on an unregistered
    engine — UnknownEngineError, never a silently-empty problem list."""
    from aqueduct.compiler.capability_check import check_capabilities
    from aqueduct.compiler.compiler import compile as compile_bp
    from aqueduct.errors import UnknownEngineError
    from aqueduct.executor.capabilities import get_capabilities
    from aqueduct.parser.parser import parse

    with pytest.raises(UnknownEngineError, match="unknown engine 'flink'") as excinfo:
        get_capabilities("flink")
    assert excinfo.value.engine == "flink"
    assert excinfo.value.engines == ["spark"]
    assert excinfo.value.no_engines_registered is False

    manifest = compile_bp(parse(_PLAIN_SNIPPET), blueprint_path=_PLAIN_SNIPPET, engine="spark")
    with pytest.raises(UnknownEngineError, match="unknown engine 'flink'"):
        check_capabilities(manifest, engine="flink")


def test_unknown_engine_error_is_a_compile_error_subclass():
    """UnknownEngineError subclasses CompileError, so compile()'s existing
    `except CompileError` callers keep working unchanged — and it is an
    AqueductError, never a bare builtin."""
    from aqueduct.errors import AqueductError, CompileError, UnknownEngineError

    assert issubclass(UnknownEngineError, CompileError)
    assert issubclass(UnknownEngineError, AqueductError)


def test_unknown_engine_fails_compile_not_silently_passes():
    """compile() itself must fail (not silently ignore the gate) when asked
    to compile against an unregistered engine."""
    from aqueduct.compiler.compiler import compile as compile_bp
    from aqueduct.errors import CompileError
    from aqueduct.parser.parser import parse

    # Caught as the BASE class on purpose — proves the subclass keeps the
    # existing compile() error contract intact.
    with pytest.raises(CompileError, match="flink"):
        compile_bp(parse(_PLAIN_SNIPPET), blueprint_path=_PLAIN_SNIPPET, engine="flink")


# ── Broken engine plugin (entry point fails to import) ───────────────────────

class _BoomEntryPoint:
    """Stand-in for an importlib.metadata EntryPoint whose target is broken."""

    name = "boom"
    value = "not_a_real_module.engine"

    def load(self):
        raise ImportError("No module named 'not_a_real_module'")


@pytest.fixture
def _reset_engine_load_cache():
    """load_engines() is cached per-process; reset it around tests that need
    to force a re-resolution of the entry-point group."""
    import aqueduct.executor.capabilities as caps

    prev_loaded = caps._engines_loaded
    prev_registry = dict(caps.CAPABILITY_REGISTRY)
    caps._engines_loaded = False
    yield caps
    caps._engines_loaded = prev_loaded
    caps.CAPABILITY_REGISTRY.clear()
    caps.CAPABILITY_REGISTRY.update(prev_registry)


def test_broken_engine_plugin_raises_aqueduct_error_naming_it(
    monkeypatch, _reset_engine_load_cache
):
    """A broken/half-installed third-party engine plugin must surface as an
    AqueductError naming the failing entry point — never a bare ImportError
    escaping out of aqueduct.yml loading."""
    import importlib.metadata

    from aqueduct.errors import AqueductError, EnginePluginError

    caps = _reset_engine_load_cache
    monkeypatch.setattr(
        importlib.metadata, "entry_points", lambda **kw: [_BoomEntryPoint()]
    )

    with pytest.raises(EnginePluginError) as excinfo:
        caps.load_engines()

    msg = str(excinfo.value)
    assert "boom" in msg  # names the entry point
    assert "not_a_real_module.engine" in msg  # names its target
    assert "ImportError" in msg  # names the underlying cause
    assert isinstance(excinfo.value, AqueductError)


def test_broken_engine_plugin_surfaces_through_config_load(
    monkeypatch, tmp_path, _reset_engine_load_cache
):
    """The plugin error reaches the user through aqueduct.yml loading as an
    AqueductError, not a raw import crash."""
    import importlib.metadata

    from aqueduct.config import load_config
    from aqueduct.errors import AqueductError

    monkeypatch.setattr(
        importlib.metadata, "entry_points", lambda **kw: [_BoomEntryPoint()]
    )
    p = tmp_path / "aqueduct.yml"
    p.write_text("deployment:\n  engine: spark\n", encoding="utf-8")

    with pytest.raises(AqueductError, match="boom"):
        load_config(p)


# ── Empty registry (stale install) gets its own actionable message ───────────

def test_empty_registry_get_capabilities_says_reinstall(
    monkeypatch, _reset_engine_load_cache
):
    """Fail-closed validation must not report a stale install as
    'Registered engines: []' — that is alarming and misleading. The
    empty-registry state names the real problem (entry points invisible ->
    reinstall)."""
    import importlib.metadata

    from aqueduct.errors import UnknownEngineError

    caps = _reset_engine_load_cache
    caps.CAPABILITY_REGISTRY.clear()
    monkeypatch.setattr(importlib.metadata, "entry_points", lambda **kw: [])

    with pytest.raises(UnknownEngineError) as excinfo:
        caps.get_capabilities("spark")

    msg = str(excinfo.value)
    assert excinfo.value.no_engines_registered is True
    assert "no execution engines are registered at all" in msg
    assert "pip install -e ." in msg
    assert "Registered engines: []" not in msg


def test_empty_registry_config_load_says_reinstall(
    monkeypatch, tmp_path, _reset_engine_load_cache
):
    """Same for aqueduct.yml loading — the stale-install state must not read
    as 'your engine name is wrong'."""
    import importlib.metadata

    from aqueduct.config import ConfigError, load_config

    caps = _reset_engine_load_cache
    caps.CAPABILITY_REGISTRY.clear()
    monkeypatch.setattr(importlib.metadata, "entry_points", lambda **kw: [])

    p = tmp_path / "aqueduct.yml"
    p.write_text("deployment:\n  engine: spark\n", encoding="utf-8")

    with pytest.raises(ConfigError) as excinfo:
        load_config(p)

    msg = str(excinfo.value)
    assert "no execution engines are registered at all" in msg
    assert "pip install -e ." in msg
    assert "is not a registered engine" not in msg


def test_empty_registry_doctor_reports_reinstall_fail(
    monkeypatch, _reset_engine_load_cache
):
    """Doctor's capability check reports the stale-install diagnosis, told
    apart from a bad engine name by TYPE (UnknownEngineError.engines), never
    by matching message text."""
    import importlib.metadata

    from aqueduct.doctor.checks_io import check_capabilities as doctor_caps

    caps = _reset_engine_load_cache
    caps.CAPABILITY_REGISTRY.clear()
    monkeypatch.setattr(importlib.metadata, "entry_points", lambda **kw: [])

    results = doctor_caps(_PLAIN_SNIPPET, engine="spark")
    assert len(results) == 1
    assert results[0].status == "fail"
    assert "no execution engines are registered at all" in results[0].detail


def test_doctor_unknown_engine_message_is_type_driven_not_string_matched():
    """An unregistered engine name (registry NON-empty) still reports the
    'engine not registered' diagnosis, and names what IS registered."""
    from aqueduct.doctor.checks_io import check_capabilities as doctor_caps

    results = doctor_caps(_PLAIN_SNIPPET, engine="nonexistent-engine")
    assert len(results) == 1
    assert results[0].status == "fail"
    assert "is not registered" in results[0].detail
    assert "spark" in results[0].detail
    assert "no execution engines are registered at all" not in results[0].detail


def test_doctor_capability_check_no_longer_skips_for_spark():
    """aqueduct doctor's capability check must resolve a real verdict for
    engine="spark" — the original live symptom was every gallery snippet
    reporting `skip — no capability declaration registered for engine
    'spark'`."""
    from aqueduct.doctor.checks_io import check_capabilities

    results = check_capabilities(_PLAIN_SNIPPET, engine="spark")
    assert results
    for r in results:
        assert not (r.status == "skip" and "no capability declaration registered" in r.detail), r
