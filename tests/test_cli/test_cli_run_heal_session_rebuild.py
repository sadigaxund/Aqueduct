"""Regression tests for the Phase 82 remediation defect: a heal retry that
executes a patched Manifest was running on the SAME (pre-patch) execution
session, so any engine/session config the patch changed had no effect on the
retry — and the retry's failure got misattributed to the patch.

The fix (``aqueduct/cli/run.py``): before every heal retry that executes a
patched manifest, close the current single-engine session and build a fresh
one from the PATCHED manifest, re-resolved through
``resolve_session_engine_config``.

These tests are SEAM-level by design (mock
``aqueduct.executor.protocol.get_protocol`` rather than building a real
SparkSession) — ``tests/conftest.py``'s autouse fixture that no-ops
``SparkContext.stop`` for the shared test session means a real-session
assertion would pass even against the PRE-FIX code (``getOrCreate()`` just
hands back the live session either way, conf changes or not). Mocking the
protocol's ``session_factory``/``session_closer`` instead proves the
mechanism directly: how many times a session was built, with what
``engine_config``, and that the previous one was closed first — and each
assertion was verified BY HAND to fail against the pre-fix code (see the
per-test notes below).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import aqueduct.executor.orchestrator  # noqa: F401  (import side effect only — see note below)
from aqueduct.agent import AgentPatchResult
from aqueduct.agent.budget import StopReason
from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ExecutionStatus, ModuleResult
from aqueduct.executor.protocol import get_protocol as _real_get_protocol
from aqueduct.patch.grammar import PatchSpec, SetEngineConfigOp

# The bare `aqueduct.executor.orchestrator` import above forces that module
# to load NOW, at collection time, before any `@patch(
# "aqueduct.executor.protocol.get_protocol")` in this file has ever run.
# `orchestrator.py` binds its own `get_protocol` name via `from
# aqueduct.executor.protocol import ... get_protocol` at ITS module top level
# — a ONE-TIME binding to whatever `aqueduct.executor.protocol.get_protocol`
# IS at the moment `orchestrator.py` first imports. On this environment's
# Python 3.14, stacked `@patch` decorators enter through `unittest.mock`'s
# `decoration_helper`/`ExitStack`, which does not guarantee the "outermost
# decorator enters first" order older Python's nested-wrapper implementation
# gave — the `get_protocol` patch can activate before the `run_polyglot`
# patch's target-resolution imports `orchestrator.py` for the FIRST time in
# the whole test session. Whichever test happens to trigger that first
# import while `get_protocol` is mid-patch permanently binds
# `orchestrator.get_protocol` to the MOCK for the rest of the process —
# every later, unrelated, unmocked polyglot test then resolves a fake
# protocol and crashes with an AttributeError deep inside Spark/DuckDB code.
# Importing eagerly above, before any decorator in this module can possibly
# be active, makes that binding real and permanent instead. Verified: with
# the eager import removed, `pytest test_cli_run_heal_session_rebuild.py
# test_cli_polyglot_run.py` fails the LATER, unrelated file with exactly
# this symptom.

pytestmark = pytest.mark.integration

# `set_engine_config(engine="spark", ...)` writes into the Blueprint's
# `engine.spark.conf` block, which is the ONE thing
# `resolve_session_engine_config` folds a patch's effect into for Spark
# (`{**cfg.engine.spark.conf, **manifest.engine_config.get("spark", {})}` —
# see `aqueduct/executor/session_config.py`). It is the only patch op that
# can make a retry's `SessionSpec.engine_config` observably differ from the
# pre-patch run's, which is exactly what these tests need to prove the
# rebuild happened for real (not just "a session object changed identity").
_BP = """\
aqueduct: "1.0"
id: test_bp
name: Test BP
agent:
  approval: auto
  sandbox_mode: "off"
  max_patches: {max_patches}
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: csv
      path: data.csv
edges: []
"""

_CFG = """\
aqueduct_config: "1.0"
agent:
  provider: openai_compat
  base_url: "http://localhost:8000"
danger:
  allow_multi_patch: true
  allow_skip_sandbox: true
"""


class _FakeSession:
    """Distinct identity per build — session identity is what the ordering
    assertions (closed-before-next-build) key off."""

    _counter = 0

    def __init__(self) -> None:
        type(self)._counter += 1
        self.n = type(self)._counter

    def __repr__(self) -> str:
        return f"<FakeSession#{self.n}>"


class _TrackingProtocol:
    """Wraps the REAL registered ``spark`` ``ExecutorProtocol``, recording
    every ``session_factory()``/``session_closer()`` call — the seam this
    fix's contract lives at — while delegating everything else (notably
    ``extract_error``, which ``Surveyor.record()`` calls on every failing
    execution regardless of this fix) to the real implementation via
    ``__getattr__``. A protocol double that only stubbed
    ``session_factory``/``session_closer`` and left the rest as a bare
    ``MagicMock`` broke on ``extract_error`` (AttributeError) and, worse,
    on serializing a MagicMock into the observability store — this wrapper
    avoids re-litigating what a fake ``ExecutorProtocol`` needs to support.
    """

    def __init__(self, engine: str = "spark") -> None:
        self._real = _real_get_protocol(engine)
        self.built_specs: list = []  # SessionSpec objects, build order
        self.built_sessions: list = []  # returned session objects, build order
        self.closed_sessions: list = []  # session objects passed to the closer, in order

    def __getattr__(self, name):
        return getattr(self._real, name)

    def session_factory(self):
        def _make(spec):
            self.built_specs.append(spec)
            session = _FakeSession()
            self.built_sessions.append(session)
            return session

        return _make

    def session_closer(self):
        def _close(session):
            self.closed_sessions.append(session)

        return _close


def _failing_result() -> ExecutionResult:
    return ExecutionResult(
        blueprint_id="test_bp",
        run_id="fake-run",
        status=ExecutionStatus.ERROR,
        module_results=(
            ModuleResult(
                module_id="m1",
                status=ExecutionStatus.ERROR,
                error="Boom",
                exception=ValueError("Boom"),
            ),
        ),
        failed_engine=None,
    )


def _spark_config_patch(patch_id: str, value: str) -> AgentPatchResult:
    patch_spec = PatchSpec(
        patch_id=patch_id,
        rationale="bump shuffle partitions",
        operations=[
            SetEngineConfigOp(
                op="set_engine_config", engine="spark",
                key="spark.sql.shuffle.partitions", value=value,
            )
        ],
    )
    return AgentPatchResult(
        patch=patch_spec,
        attempts=1,
        stop_reason=StopReason.SOLVED,
        tokens_in_total=10,
        tokens_out_total=20,
        attempt_records=[],
    )


def _assert_no_unexpected_crash(result) -> None:
    """These tests deliberately make ``execute()`` fail forever, so the run
    ALWAYS ends in the CLI's normal ``sys.exit(exit_codes.DATA_OR_RUNTIME)``
    for an unhealed blueprint — a real ``SystemExit(2)``, not a bug. Fail
    only on anything else (an uncaught exception from a wiring mistake in
    the test double, e.g. the ``AttributeError``/``TypeError`` this test
    file hit while it still used a bare ``MagicMock`` protocol double)."""
    if result.exception is not None and not isinstance(result.exception, SystemExit):
        raise AssertionError(f"unexpected crash: {result.exception!r}\n{result.output}")


def _invoke(tmp_path, max_patches: int):
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text(_BP.format(max_patches=max_patches), encoding="utf-8")
    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(_CFG, encoding="utf-8")

    runner = CliRunner()
    with (
        patch("aqueduct.agent.memory.find_pending", return_value=None),
        patch("aqueduct.agent.memory.find_replay_candidate", return_value=None),
    ):
        result = runner.invoke(
            cli, ["run", str(bp_file), "--config", str(cfg_file), "--store-dir", str(tmp_path)]
        )
    return result


@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.executor.protocol.get_protocol")
def test_heal_retry_rebuilds_session_from_patched_manifest(
    mock_get_protocol, mock_get_executor, mock_generate_patch, tmp_path
):
    """A single heal retry (max_patches=1) must build a NEW session whose
    ``engine_config`` reflects the patched manifest, and must close the
    PRE-patch session before doing so.

    Verified to fail pre-fix: with the rebuild call removed (i.e. calling
    ``_execute_target`` directly after ``_apply_patch_in_memory`` with no
    ``_rebuild_session_for_patch`` in between — the exact pre-fix shape),
    ``get_protocol().session_factory()`` is invoked exactly ONCE for the
    whole run (only the initial ``_setup_surveyor`` build), so
    ``len(fake_protocol.built_sessions) == 1`` and this test's `== 2`
    assertion fails; the retry silently runs on the original session with
    the original (unpatched) ``engine_config``.
    """
    fake_protocol = _TrackingProtocol()
    mock_get_protocol.return_value = fake_protocol
    mock_get_executor.return_value = MagicMock(return_value=_failing_result())
    mock_generate_patch.return_value = _spark_config_patch("p1", "99")

    result = _invoke(tmp_path, max_patches=1)
    _assert_no_unexpected_crash(result)

    # Initial session (unpatched) + one retry rebuild.
    assert len(fake_protocol.built_sessions) == 2, (
        f"expected 2 session builds (initial + 1 retry), got "
        f"{len(fake_protocol.built_sessions)}: engine_configs="
        f"{[s.engine_config for s in fake_protocol.built_specs]}"
    )

    initial_spec, retry_spec = fake_protocol.built_specs
    assert "spark.sql.shuffle.partitions" not in initial_spec.engine_config
    assert retry_spec.engine_config.get("spark.sql.shuffle.partitions") == "99", (
        "the retry's SessionSpec.engine_config does not carry the patched "
        f"spark config key — got {retry_spec.engine_config}"
    )

    # The pre-patch session (built first) must be closed before the retry
    # session gets used — proves a genuine teardown, not a getOrCreate()
    # reuse that would silently keep the pre-patch conf alive.
    assert fake_protocol.built_sessions[0] in fake_protocol.closed_sessions


@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.executor.get_executor")
@patch("aqueduct.executor.protocol.get_protocol")
def test_heal_rebuild_happens_on_every_iteration_not_only_the_first(
    mock_get_protocol, mock_get_executor, mock_generate_patch, tmp_path
):
    """Two heal iterations (max_patches=2, each patch attempt still fails)
    must rebuild the session on BOTH retries, not just the first — the
    unconditional-rebuild contract this fix commits to (no "first patch
    only" shortcut).

    Verified to fail pre-fix the same way as the single-retry test: zero
    rebuilds happen at all pre-fix, so ``built_sessions`` would stay at 1
    instead of growing to 3, and neither retry's ``engine_config`` would
    show its patch's key.
    """
    fake_protocol = _TrackingProtocol()
    mock_get_protocol.return_value = fake_protocol
    mock_get_executor.return_value = MagicMock(return_value=_failing_result())
    mock_generate_patch.side_effect = [
        _spark_config_patch("p1", "11"),
        _spark_config_patch("p2", "22"),
    ]

    result = _invoke(tmp_path, max_patches=2)
    _assert_no_unexpected_crash(result)

    assert mock_generate_patch.call_count == 2
    # Initial session + one rebuild per patch attempt.
    assert len(fake_protocol.built_sessions) == 3, (
        f"expected 3 session builds (initial + 2 retries), got "
        f"{len(fake_protocol.built_sessions)}: engine_configs="
        f"{[s.engine_config for s in fake_protocol.built_specs]}"
    )

    _initial, retry1, retry2 = fake_protocol.built_specs
    assert retry1.engine_config.get("spark.sql.shuffle.partitions") == "11"
    assert retry2.engine_config.get("spark.sql.shuffle.partitions") == "22"

    # Each session is closed before the NEXT one is built — session[0]
    # closed before session[1] exists to be used, session[1] closed before
    # session[2].
    assert fake_protocol.built_sessions[0] in fake_protocol.closed_sessions
    assert fake_protocol.built_sessions[1] in fake_protocol.closed_sessions


@patch("aqueduct.agent.generate_agent_patch")
@patch("aqueduct.executor.orchestrator.run_polyglot")
@patch("aqueduct.executor.protocol.get_protocol")
def test_polyglot_run_never_touches_session_holder(
    mock_get_protocol, mock_run_polyglot, mock_generate_patch, tmp_path
):
    """A polyglot Manifest (>1 island) keeps ``session`` (and, since this
    fix, ``_session_holder.session``) at ``None`` for the entire run —
    ``run_polyglot()`` already opens/closes a session per island on every
    call, so this fix's rebuild must be a complete no-op there:
    ``session_factory()``/``session_closer()`` (the seam both the initial
    single-engine session build AND ``_rebuild_session_for_patch`` go
    through) must never be called at all for a polyglot run. NB
    ``get_protocol`` itself IS legitimately called here regardless of this
    fix (``Surveyor.record()`` resolves the failing island's
    ``extract_error`` through it on every failing execution) — the
    assertion has to target the session-lifecycle methods specifically, not
    ``get_protocol`` call count, or it would fail for a reason unrelated to
    this fix.

    Verified to fail if the polyglot guard were removed (i.e.
    ``_rebuild_session_for_patch`` unconditionally called
    ``get_protocol(engine).session_factory()`` instead of returning early
    when ``_session_holder.session is None``): ``built_sessions`` would go
    from empty to non-empty the moment the patched polyglot manifest gets
    retried.
    """
    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text(
        """
aqueduct: "1.0"
id: polyglot_bp
name: Polyglot BP
agent:
  approval: auto
  sandbox_mode: "off"
  max_patches: 1
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: csv
      path: data.csv
  - id: m2
    type: Egress
    label: M2
    engine: duckdb
    config:
      format: csv
      path: out.csv
      mode: overwrite
edges:
  - from: m1
    to: m2
""",
        encoding="utf-8",
    )
    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(_CFG, encoding="utf-8")

    fake_protocol = _TrackingProtocol()
    mock_get_protocol.return_value = fake_protocol
    mock_run_polyglot.return_value = ExecutionResult(
        blueprint_id="polyglot_bp",
        run_id="fake-run",
        status=ExecutionStatus.ERROR,
        module_results=(
            ModuleResult(
                module_id="m1",
                status=ExecutionStatus.ERROR,
                error="Boom",
                exception=ValueError("Boom"),
            ),
        ),
        failed_engine="spark",
    )
    mock_generate_patch.return_value = _spark_config_patch("p1", "99")

    runner = CliRunner()
    with (
        patch("aqueduct.agent.memory.find_pending", return_value=None),
        patch("aqueduct.agent.memory.find_replay_candidate", return_value=None),
    ):
        result = runner.invoke(
            cli, ["run", str(bp_file), "--config", str(cfg_file), "--store-dir", str(tmp_path)]
        )

    _assert_no_unexpected_crash(result)
    assert mock_run_polyglot.called, "expected the polyglot orchestrator to be invoked"
    assert fake_protocol.built_sessions == [], (
        "a polyglot run must never build a session through "
        "get_protocol().session_factory() — run_polyglot() owns its own "
        f"per-island session lifecycle. Built: {fake_protocol.built_sessions!r}"
    )
    assert fake_protocol.closed_sessions == [], (
        "a polyglot run must never close a session through "
        f"get_protocol().session_closer(). Closed: {fake_protocol.closed_sessions!r}"
    )
