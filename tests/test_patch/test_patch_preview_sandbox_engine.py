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
from unittest.mock import patch

import pytest

from aqueduct.executor.protocol import (
    OPTIONAL_EXECUTE_KWARGS,
    call_execute,
    get_protocol,
)
from aqueduct.patch.preview import run_sandbox_gate
from aqueduct.warnings import AqueductWarning

pytestmark = pytest.mark.duckdb


def _csv_blueprint(path: str) -> dict:
    return {
        "aqueduct": "1.0",
        "id": "test.gate3.duckdb",
        "name": "Test Gate 3 DuckDB",
        "modules": [
            {"id": "in", "type": "Ingress", "label": "In", "config": {"format": "csv", "path": path}},
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
        sample_rows=2,
    )
    assert result.status == "pass"
    assert result.sample_rows == 2


def test_missing_engine_skip_names_the_actual_engine(_orders_csv, tmp_path):
    """When the target engine's session factory fails, the skip detail must
    name the REAL target engine (duckdb), not Spark."""
    bp = _csv_blueprint(_orders_csv)
    with patch("duckdb.connect") as mock_connect:
        mock_connect.side_effect = Exception("duckdb down")
        result = run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="p-duckdb-skip",
            failed_module=None,
            engine="duckdb",
        )
    assert result.status == "skip"
    assert "duckdb" in result.detail
    assert "Spark" not in result.detail


def test_sandbox_gate_unknown_engine_skips_naming_it():
    """A misspelled/unregistered engine is a clean `skip`, not a crash — and
    the detail names the requested (bogus) engine, not a Spark default."""
    result = run_sandbox_gate(
        {"aqueduct": "1.0", "id": "x", "name": "x", "modules": [], "edges": []},
        blueprint_path=None,
        patch_id="p-bogus",
        failed_module=None,
        engine="bogus-engine",
    )
    assert result.status == "skip"
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
        "aqueduct.executor.protocol.get_protocol", lambda engine: narrow,
    )

    with pytest.warns(AqueductWarning, match="engine_kwarg_ignored"):
        result = call_execute(
            "fake-narrow", "manifest", "session",
            run_id="r1", observability_store=object(), explain_capture={},
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
        "aqueduct.executor.protocol.get_protocol", lambda engine: wide,
    )

    with warnings_must_not_fire():
        result = call_execute(
            "fake-wide", "manifest", "session",
            observability_store=object(), explain_capture={},
        )

    assert result == "ok"
    assert set(captured["kwargs"]) == {"observability_store", "explain_capture"}


def test_call_execute_suppress_silences_the_warning(monkeypatch):
    def _fake_execute(*args, **kwargs):
        return "ok"

    narrow = SimpleNamespace(execute=_fake_execute, execute_kwargs=frozenset())
    monkeypatch.setattr(
        "aqueduct.executor.protocol.get_protocol", lambda engine: narrow,
    )

    with warnings_must_not_fire():
        result = call_execute(
            "fake-narrow", "manifest", "session",
            observability_store=object(), suppress={"engine_kwarg_ignored"},
        )
    assert result == "ok"


def test_spark_declares_no_execute_kwargs_allowlist():
    """Spark's real ExecutorProtocol: execute_kwargs=None — its real execute()
    has a parameter for every OPTIONAL_EXECUTE_KWARGS name, so it needs no
    filtering. Checked by inspection, not by starting a real SparkSession."""
    assert get_protocol("spark").execute_kwargs is None


def test_duckdb_execute_kwargs_excludes_every_optional_capability():
    """DuckDB's real declaration names none of OPTIONAL_EXECUTE_KWARGS —
    every one of them is a genuinely optional Spark-only capability DuckDB
    Stage A does not implement, so all of them get filtered+warned."""
    accepted = get_protocol("duckdb").execute_kwargs
    assert accepted is not None
    assert not (OPTIONAL_EXECUTE_KWARGS & accepted)


def test_duckdb_sandbox_gate_warns_engine_kwarg_ignored_for_observability_kwargs(
    _orders_csv, tmp_path,
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
            observability_store=object(),
            explain_capture={},
            warnings_suppress={"engine_kwarg_ignored"},
        )
    assert result.status == "pass"


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
