"""Phase 79 — engine-agnostic sandbox gate (Defect A) + kwarg-ignored warning
(Defect B).

Defect A: ``run_sandbox_gate`` (Gate 3, ``aqueduct/patch/preview.py``) used to
hardcode ``make_spark_session`` and call the resolved engine's ``execute()``
with Spark-only kwargs regardless of the patch's actual target engine — a
DuckDB blueprint either crashed (pyspark installed, TypeError from duckdb's
``execute()``) or the gate silently disabled itself with a "could not start
Spark" skip that named the wrong engine. This file proves the fix: a DuckDB
target gets a REAL DuckDB sandbox run (no Spark touched at all), and a
missing-session skip names the actual target engine.

Defect B: ``observability_store``/``explain_capture`` are Spark-flavoured
optional execute() capabilities (``aqueduct.executor.protocol.
OPTIONAL_EXECUTE_KWARGS``). Passing them to an engine that cannot honour them
used to either raise (Spark) or drop silently (DuckDB's old ad-hoc
allowlist). ``call_execute()`` now warns through the SAME suppressible
``aqueduct.warnings.emit`` machinery ``engine_key_ignored`` uses, under its
own rule id ``engine_kwarg_ignored`` — proven here directly against
``call_execute`` (engine-agnostic, no pyspark needed) and end-to-end through
``run_sandbox_gate`` on a real DuckDB connection.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from aqueduct.config import AqueductConfig, DuckDBEngineConfig, EngineConfig
from aqueduct.executor.protocol import (
    OPTIONAL_EXECUTE_KWARGS,
    call_execute,
    get_protocol,
)
from aqueduct.patch.gate_status import sandbox_gate_permits_auto_apply
from aqueduct.patch.preview import run_sandbox_gate
from aqueduct.warnings import AqueductWarning

pytestmark = pytest.mark.duckdb


def _csv_blueprint(path: str) -> dict:
    return {
        "aqueduct": "1.0",
        "id": "test.gate3.duckdb",
        "name": "Test Gate 3 DuckDB",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "In",
                "config": {"format": "csv", "path": path},
            },
        ],
        "edges": [],
    }


@pytest.fixture
def _orders_csv(tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,amount\n1,10.5\n2,20.0\n3,5.25\n", encoding="utf-8")
    return str(path)


# ── Defect A: a DuckDB blueprint gets a real DuckDB sandbox run ─────────────


def test_duckdb_sandbox_gate_actually_executes_on_duckdb(_orders_csv, tmp_path):
    """No Spark anywhere on this path: engine="duckdb" builds its own DuckDB
    session (via ExecutorProtocol.session_factory) and runs the replay for
    real — status must be "pass", never "skip"."""
    bp = _csv_blueprint(_orders_csv)
    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p-duckdb",
        failed_module=None,
        engine="duckdb",
        cfg=AqueductConfig(),
        sample_rows=2,
    )
    assert result.status == "pass"
    assert result.sample_rows == 2
    # Positive control for the three `not ...permits_auto_apply` assertions
    # below: without this, they would also hold if the helper always returned
    # False, and would prove nothing about the split.
    assert sandbox_gate_permits_auto_apply(result)


def test_sandbox_gate_unavailable_on_a_polyglot_blueprint_naming_its_islands(tmp_path):
    """A Blueprint compiling to more than one island (a spark module handing
    off to a duckdb module) must not be sandbox-replayed against only ONE of
    its engines — that would look like real pre-apply validation while
    covering nothing about the rest. The gate returns `unavailable` (not
    `fail` — the patch is not wrong, it is unverified), naming the island
    count and engines, WITHOUT ever building a session for either engine
    (this is checked right after compile, before session_factory).

    `unavailable`, not `not_applicable`: a replay WAS owed here and the
    environment prevented it, so it must block auto-apply."""
    bp = {
        "aqueduct": "1.0",
        "id": "bp.polyglot",
        "name": "t",
        "modules": [
            {
                "id": "extract",
                "label": "extract",
                "type": "Channel",
                "engine": "spark",
                "config": {"op": "sql", "query": "SELECT 1 AS x"},
            },
            {
                "id": "agg",
                "label": "agg",
                "type": "Channel",
                "engine": "duckdb",
                "config": {"op": "sql", "query": "SELECT * FROM extract"},
            },
        ],
        "edges": [{"from": "extract", "to": "agg"}],
    }
    with patch("aqueduct.executor.protocol.get_protocol") as mock_get_protocol:
        result = run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="p-polyglot",
            failed_module=None,
            engine="spark",
            cfg=AqueductConfig(),
        )
    assert result.status == "unavailable"
    assert not sandbox_gate_permits_auto_apply(result)
    assert "polyglot" in result.detail
    assert "2 islands" in result.detail
    assert "spark" in result.detail and "duckdb" in result.detail
    # session_factory()/session_closer() are only reachable AFTER this
    # check — get_protocol() itself is called once (to resolve `engine` up
    # front) but its session factory must never be invoked here.
    mock_get_protocol.return_value.session_factory.assert_not_called()


def test_missing_engine_unavailable_names_the_actual_engine(_orders_csv, tmp_path):
    """When the target engine's session factory fails, the `unavailable`
    detail must name the REAL target engine (duckdb), not Spark."""
    bp = _csv_blueprint(_orders_csv)
    with patch("duckdb.connect") as mock_connect:
        mock_connect.side_effect = Exception("duckdb down")
        result = run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="p-duckdb-skip",
            failed_module=None,
            engine="duckdb",
            cfg=AqueductConfig(),
        )
    assert result.status == "unavailable"
    assert not sandbox_gate_permits_auto_apply(result)
    assert "duckdb" in result.detail
    assert "Spark" not in result.detail


def test_sandbox_gate_unknown_engine_unavailable_naming_it():
    """A misspelled/unregistered engine is a clean `unavailable`, not a crash
    — and the detail names the requested (bogus) engine, not a Spark
    default."""
    result = run_sandbox_gate(
        {"aqueduct": "1.0", "id": "x", "name": "x", "modules": [], "edges": []},
        blueprint_path=None,
        patch_id="p-bogus",
        failed_module=None,
        engine="bogus-engine",
        cfg=AqueductConfig(),
    )
    assert result.status == "unavailable"
    assert not sandbox_gate_permits_auto_apply(result)
    assert "bogus-engine" in result.detail


# ── Defect B: engine_kwarg_ignored — engine-agnostic mechanism ──────────────


def test_call_execute_warns_and_drops_unsupported_optional_kwarg(monkeypatch):
    """An engine that declares a narrow `execute_kwargs` allowlist gets the
    unsupported optional kwarg DROPPED and a suppressible `engine_kwarg_ignored`
    warning — never a TypeError, never silence."""
    captured: dict = {}

    def _fake_execute(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "ok"

    narrow = SimpleNamespace(execute=_fake_execute, execute_kwargs=frozenset({"run_id"}))
    monkeypatch.setattr(
        "aqueduct.executor.protocol.get_protocol",
        lambda engine: narrow,
    )

    with pytest.warns(AqueductWarning, match="engine_kwarg_ignored"):
        result = call_execute(
            "fake-narrow",
            "manifest",
            "session",
            run_id="r1",
            observability_store=object(),
            explain_capture={},
        )

    assert result == "ok"
    assert captured["kwargs"] == {"run_id": "r1"}  # both optional kwargs dropped
    assert "observability_store" not in captured["kwargs"]
    assert "explain_capture" not in captured["kwargs"]


def test_call_execute_no_warning_when_engine_declares_no_allowlist(monkeypatch):
    """`execute_kwargs=None` (Spark's declaration) means "consumes everything"
    — call_execute must apply zero filtering and emit zero warnings."""
    captured: dict = {}

    def _fake_execute(*args, **kwargs):
        captured["kwargs"] = kwargs
        return "ok"

    wide = SimpleNamespace(execute=_fake_execute, execute_kwargs=None)
    monkeypatch.setattr(
        "aqueduct.executor.protocol.get_protocol",
        lambda engine: wide,
    )

    with warnings_must_not_fire():
        result = call_execute(
            "fake-wide",
            "manifest",
            "session",
            observability_store=object(),
            explain_capture={},
        )

    assert result == "ok"
    assert set(captured["kwargs"]) == {"observability_store", "explain_capture"}


def test_call_execute_suppress_silences_the_warning(monkeypatch):
    def _fake_execute(*args, **kwargs):
        return "ok"

    narrow = SimpleNamespace(execute=_fake_execute, execute_kwargs=frozenset())
    monkeypatch.setattr(
        "aqueduct.executor.protocol.get_protocol",
        lambda engine: narrow,
    )

    with warnings_must_not_fire():
        result = call_execute(
            "fake-narrow",
            "manifest",
            "session",
            observability_store=object(),
            suppress={"engine_kwarg_ignored"},
        )
    assert result == "ok"


def test_spark_declares_no_execute_kwargs_allowlist():
    """Spark's real ExecutorProtocol: execute_kwargs=None — its real execute()
    has a parameter for every OPTIONAL_EXECUTE_KWARGS name, so it needs no
    filtering. Checked by inspection, not by starting a real SparkSession."""
    assert get_protocol("spark").execute_kwargs is None


def test_duckdb_execute_kwargs_excludes_every_spark_only_capability():
    """DuckDB's real declaration accepts only the optional capabilities it
    genuinely implements; every Spark-only one gets filtered+warned.

    Two deliberate exceptions, both of which must stay accepted:

    - `handoff_spill_uris` — a Handoff module runs on BOTH sides of a
      cross-engine boundary, so an engine that can execute one needs the
      spill URIs. Filtering it would strip the transport's own addresses on
      the DuckDB side of every handoff.
    - `observability_store` — DuckDB writes `module_metrics` rows for a
      Handoff module (bytes transferred, duration), so it needs the store.
    - `sampling` (Pass F) — Probe on DuckDB genuinely consumes
      `config.probes.*` (row-sampling governance for signal collection); see
      `aqueduct/executor/duckdb_/probe.py`. It moved out of the excluded set
      once that implementation landed, same as the two above.

    A new name added to OPTIONAL_EXECUTE_KWARGS belongs on the excluded side
    of this assertion unless DuckDB actually implements it.
    """
    duckdb_implements = {"handoff_spill_uris", "observability_store", "sampling"}
    accepted = get_protocol("duckdb").execute_kwargs
    assert accepted is not None
    spark_only = OPTIONAL_EXECUTE_KWARGS - duckdb_implements
    assert not (spark_only & accepted)
    assert duckdb_implements <= accepted


def test_duckdb_sandbox_gate_warns_engine_kwarg_ignored_for_observability_kwargs(
    _orders_csv,
    tmp_path,
):
    """End-to-end: a real DuckDB sandbox run forwarding Spark-flavoured
    observability_store/explain_capture must warn under `engine_kwarg_ignored`
    (never crash, never silence) and still complete the replay."""
    bp = _csv_blueprint(_orders_csv)
    with pytest.warns(AqueductWarning, match="engine_kwarg_ignored"):
        result = run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="p-duckdb-kwarg",
            failed_module=None,
            engine="duckdb",
            cfg=AqueductConfig(),
            observability_store=object(),
            explain_capture={},
        )
    assert result.status == "pass"


def test_duckdb_sandbox_gate_kwarg_warning_suppressible(_orders_csv, tmp_path):
    bp = _csv_blueprint(_orders_csv)
    with warnings_must_not_fire():
        result = run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="p-duckdb-kwarg-suppressed",
            failed_module=None,
            engine="duckdb",
            cfg=AqueductConfig(),
            observability_store=object(),
            explain_capture={},
            warnings_suppress={"engine_kwarg_ignored"},
        )
    assert result.status == "pass"


# ── Task 1 proof: the sandbox gate now sees real engine.duckdb.* config ─────


def test_duckdb_sandbox_gate_applies_real_engine_duckdb_config(_orders_csv, tmp_path):
    """A DuckDB sandbox replay's OWNED session must be built with a genuine
    non-default `engine.duckdb.*` value threaded all the way from `cfg`
    through `run_sandbox_gate` -> `SessionSpec.engine_config` -> the REAL
    `_make_session` -> the live DuckDB connection's own settings — not
    merely an argument that would have been forwarded.

    Before this fix, `run_sandbox_gate` built its owned session's
    `engine_config` from `sandboxed_manifest.spark_config` (a Spark-only
    field, always `{}` for a non-Spark target), so `engine.duckdb.
    memory_limit`/`threads` never reached the connection at all — a DuckDB
    sandbox replay ran under a completely different session shape than a
    real `aqueduct run` on the same `aqueduct.yml` would use.

    The proof intercepts the REGISTERED `DUCKDB.make_session` (bypassing
    `ExecutorProtocol`'s frozen-dataclass `__setattr__` via
    `object.__setattr__`, restored in `finally`) with a spy that calls
    through to the real `_make_session`, then queries the resulting
    connection's OWN reported settings — proving the session was actually
    built with the configured values, not just that an argument was passed.
    """
    from aqueduct.executor.duckdb_.engine import DUCKDB
    from aqueduct.executor.duckdb_.engine import _make_session as real_make_session

    observed: dict[str, Any] = {}

    def _spying_make_session(spec):
        # The value must already be present in the resolved engine_config
        # BEFORE _make_session ever runs.
        assert spec.engine_config.get("memory_limit") == "111MB"
        assert spec.engine_config.get("threads") == 3
        conn = real_make_session(spec)
        # And the LIVE DuckDB connection must reflect it — the strong half
        # of the proof: the session was actually BUILT with it.
        observed["memory_limit"] = conn.execute(
            "SELECT current_setting('memory_limit')"
        ).fetchone()[0]
        observed["threads"] = conn.execute("SELECT current_setting('threads')").fetchone()[0]
        return conn

    original_make_session = DUCKDB.make_session
    object.__setattr__(DUCKDB, "make_session", _spying_make_session)
    try:
        cfg = AqueductConfig(
            engine=EngineConfig(
                duckdb=DuckDBEngineConfig(memory_limit="111MB", threads=3),
            ),
        )
        bp = _csv_blueprint(_orders_csv)
        result = run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="p-duckdb-real-config",
            failed_module=None,
            engine="duckdb",
            cfg=cfg,
            sample_rows=2,
        )
    finally:
        object.__setattr__(DUCKDB, "make_session", original_make_session)

    assert result.status == "pass"
    # 111MB (SI) DuckDB reports back as ~105.8 MiB (binary) — assert the
    # UNIT/magnitude changed from the multi-GiB system default rather than
    # pin an exact string that would drift with DuckDB's own formatting.
    assert "MiB" in observed["memory_limit"]
    assert "GiB" not in observed["memory_limit"]
    assert observed["threads"] == 3


# ── helper ────────────────────────────────────────────────────────────────


class warnings_must_not_fire:
    """Context manager: fail the test if ANY warning is emitted inside it."""

    def __enter__(self):
        import warnings as _w

        self._cm = _w.catch_warnings()
        self._cm.__enter__()
        _w.simplefilter("error")
        return self

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)
