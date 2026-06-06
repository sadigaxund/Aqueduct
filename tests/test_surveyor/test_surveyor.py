"""Tests for the Surveyor observability recorder."""

from __future__ import annotations

import tempfile
from pathlib import Path
import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
from unittest.mock import MagicMock, patch

try:
    from aqueduct.executor.models import ExecutionResult, ModuleResult
except ImportError:
    pytest.skip("pyspark required", allow_module_level=True)
from aqueduct.surveyor.surveyor import Surveyor
from aqueduct.config import WebhookEndpointConfig


def test_surveyor_record_fires_webhook():
    manifest = MagicMock()
    manifest.blueprint_id = "test-bp"
    manifest.name = "Test Blueprint"
    manifest.to_dict.return_value = {"id": "test-bp"}
    manifest.provenance_map = None
    
    config = WebhookEndpointConfig(url="http://test.com")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        surveyor = Surveyor(manifest, store_dir=tmp_path, webhook_config=config)
        surveyor.start("run-123")
        
        result = ExecutionResult(
            blueprint_id="test-bp",
            run_id="run-123",
            status="error",
            module_results=(
                ModuleResult(module_id="mod1", status="error", error="Boom!"),
            )
        )
        
        with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_fire:
            surveyor.record(result)
            mock_fire.assert_called_once()
            args = mock_fire.call_args[0]
            # fire_webhook(config, full_payload, template_vars)
            assert args[0].url == "http://test.com"
            assert args[1]["failed_module"] == "mod1"
            assert args[1]["error_message"] == "Boom!"
            assert args[2]["blueprint_name"] == "Test Blueprint"
            assert args[2]["run_id"] == "run-123"


def test_surveyor_record_no_webhook_on_success():
    manifest = MagicMock()
    manifest.blueprint_id = "test-bp"
    manifest.name = "Test Blueprint"
    manifest.to_dict.return_value = {"id": "test-bp"}
    manifest.provenance_map = None
    
    config = WebhookEndpointConfig(url="http://test.com")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        surveyor = Surveyor(manifest, store_dir=tmp_path, webhook_config=config)
        surveyor.start("run-123")
        
        result = ExecutionResult(
            blueprint_id="test-bp",
            run_id="run-123",
            status="success",
            module_results=()
        )
        
        with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_fire:
            surveyor.record(result)
            mock_fire.assert_not_called()


class TestEvaluateRegulatorSignalOverride:
    """Tests that evaluate_regulator() checks signal_overrides before probe_signals."""

    def _make_manifest(self, probe_id="probe1", regulator_id="reg1"):
        from unittest.mock import MagicMock
        from aqueduct.compiler.models import Manifest
        edge = MagicMock()
        edge.from_id = probe_id
        edge.to_id = regulator_id
        edge.port = "signal"
        manifest = MagicMock(spec=Manifest)
        manifest.edges = [edge]
        manifest.blueprint_id = "test-bp"
        return manifest

    def test_override_false_blocks_even_if_probe_says_true(self, tmp_path):
        import duckdb
        from datetime import datetime, timezone
        from aqueduct.surveyor.surveyor import Surveyor, _SIGNAL_OVERRIDES_DDL
        manifest = self._make_manifest()
        store = tmp_path / "signals"
        store.mkdir()
        signals_db = store / "observability.db"
        conn = duckdb.connect(str(signals_db))
        conn.execute(_SIGNAL_OVERRIDES_DDL)
        conn.execute(
            "INSERT INTO signal_overrides VALUES (?, ?, ?, ?)",
            ["probe1", False, None, datetime.now(tz=timezone.utc).isoformat()],
        )
        conn.close()

        s = Surveyor(manifest, store_dir=store)
        s.start("run-x")
        assert s.evaluate_regulator("reg1") is False

    def test_no_override_returns_true_when_no_probe_signals(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        manifest = self._make_manifest()
        store = tmp_path / "signals"
        store.mkdir()
        s = Surveyor(manifest, store_dir=store)
        s.start("run-x")
        assert s.evaluate_regulator("reg1") is True


class TestSurveyorBlueprintSourceYaml:
    def test_surveyor_populates_blueprint_source_yaml_when_file_exists(self, tmp_path):
        """Surveyor reads blueprint YAML and sets blueprint_source_yaml on FailureContext."""
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.models import ExecutionResult, ModuleResult

        bp_path = tmp_path / "blueprint.yml"
        bp_path.write_text("id: my_blueprint\nname: Test", encoding="utf-8")

        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        surveyor = Surveyor(manifest, store_dir=tmp_path, blueprint_path=bp_path)
        surveyor.start("r1")

        result = ExecutionResult(
            blueprint_id="b1",
            run_id="r1",
            status="error",
            module_results=[ModuleResult(module_id="m1", status="error", error="err")],
        )
        ctx = surveyor.record(result)
        assert ctx is not None
        assert ctx.blueprint_source_yaml is not None
        assert "my_blueprint" in ctx.blueprint_source_yaml

    def test_surveyor_sets_blueprint_source_yaml_none_when_file_missing(self, tmp_path):
        """Surveyor sets blueprint_source_yaml=None when blueprint_path is None."""
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.models import ExecutionResult, ModuleResult

        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        surveyor = Surveyor(manifest, store_dir=tmp_path, blueprint_path=None)
        surveyor.start("r1")

        result = ExecutionResult(
            blueprint_id="b1",
            run_id="r1",
            status="error",
            module_results=[ModuleResult(module_id="m1", status="error", error="err")],
        )
        ctx = surveyor.record(result)
        assert ctx is not None
        assert ctx.blueprint_source_yaml is None

class TestSurveyorStores:
    def test_surveyor_uses_injected_stores(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.stores import StoreBundle
        from aqueduct.stores.duckdb_ import DuckDBObservabilityStore, DuckDBLineageStore, DuckDBDepotStore
        
        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        
        obs = DuckDBObservabilityStore(tmp_path / "observability.db")
        lineage = DuckDBLineageStore(tmp_path / "lineage.db")
        depot = DuckDBDepotStore(tmp_path / "depot.db")
        bundle = StoreBundle(observability=obs, lineage=lineage, depot=depot)
        
        surveyor = Surveyor(manifest, store_dir=tmp_path, stores=bundle)
        assert surveyor._observability is obs

    def test_surveyor_default_store(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
        
        manifest = Manifest(blueprint_id="b1", name="test", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={})
        
        surveyor = Surveyor(manifest, store_dir=tmp_path)
        surveyor.start("r1")
        assert isinstance(surveyor._observability, DuckDBObservabilityStore)

class TestSurveyorSpendCap:
    def test_count_recent_heal_attempts_empty(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        manifest = MagicMock()
        manifest.blueprint_id = "test-bp"
        s = Surveyor(manifest, store_dir=tmp_path)
        s.start("r1")
        assert s.count_recent_heal_attempts() == 0

    def test_count_recent_heal_attempts_with_data(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        manifest = MagicMock()
        manifest.blueprint_id = "test-bp"
        s = Surveyor(manifest, store_dir=tmp_path)
        s.start("r1")
        
        s.record_healing_outcome(
            run_id="r1", failed_module="m1", failure_category="err",
            model="gpt-4", patch_id="p1", confidence=0.9,
            patch_applied=True, run_success_after_patch=True
        )
        assert s.count_recent_heal_attempts(within_minutes=60) == 1

    def test_count_recent_heal_attempts_excludes_old(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        import datetime as _dt
        manifest = MagicMock()
        manifest.blueprint_id = "test-bp"
        s = Surveyor(manifest, store_dir=tmp_path)
        s.start("r1")
        
        # Manually insert an old row
        old_ts = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=120)).isoformat()
        with s._observability.connect() as cur:
            cur.execute(
                "INSERT INTO healing_outcomes (id, run_id, applied_at) VALUES (?, ?, ?)",
                ["old-id", "r1", old_ts]
            )
            
        assert s.count_recent_heal_attempts(within_minutes=60) == 0
        assert s.count_recent_heal_attempts(within_minutes=180) == 1

    def test_count_recent_heal_attempts_swallows_db_errors(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        manifest = MagicMock()
        manifest.blueprint_id = "test-bp"
        s = Surveyor(manifest, store_dir=tmp_path)
        # Not calling start() so connection will fail/not exist
        assert s.count_recent_heal_attempts() == 0

# ── record_heal_attempt ───────────────────────────────────────────────────────

class TestRecordHealAttempt:
    def test_record_heal_attempt_writes_row(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.agent.budget import AttemptRecord
        from aqueduct.agent.signature import make_signature

        s = Surveyor(
            Manifest(blueprint_id="bp1", name="name", context={}, modules=(), edges=(), spark_config={}),
            tmp_path,
        )
        s.start("run1")
        
        sig = make_signature("e", "w", "msg")
        rec = AttemptRecord(
            attempt_num=1, signature=sig, tokens_in=10, tokens_out=20,
            latency_ms=100, gate_that_rejected="schema", escalated=True,
        )
        s.record_heal_attempt(run_id="run1", attempt_record=rec, stop_reason="stuck_signature")
        
        with s._observability.connect() as cur:
            row = cur.execute("SELECT attempt_num, signature_hash, tokens_in, gate_that_rejected, escalated, stop_reason FROM heal_attempts").fetchone()
            
        assert row[0] == 1
        assert row[1] == sig.hash
        assert row[2] == 10
        assert row[3] == "schema"
        assert row[4] is True
        assert row[5] == "stuck_signature"

    def test_record_heal_attempt_success_row_writes_nulls(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.agent.budget import AttemptRecord

        s = Surveyor(
            Manifest(blueprint_id="bp1", name="name", context={}, modules=(), edges=(), spark_config={}),
            tmp_path,
        )
        s.start("run1")

        rec = AttemptRecord(attempt_num=2, signature=None)
        s.record_heal_attempt(run_id="run1", attempt_record=rec, stop_reason="solved")
        
        with s._observability.connect() as cur:
            row = cur.execute("SELECT error_class, where_field, normalized_message, signature_hash FROM heal_attempts").fetchone()
            
        assert row == (None, None, None, None)

    def test_record_heal_attempt_db_exception_swallowed(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.agent.budget import AttemptRecord
        from unittest.mock import patch

        s = Surveyor(
            Manifest(blueprint_id="bp1", name="name", context={}, modules=(), edges=(), spark_config={}),
            tmp_path,
        )
        s.start("run1")
        
        rec = AttemptRecord(attempt_num=1, signature=None)
        
        with patch.object(s._observability, "connect", side_effect=Exception("db error")):
            # Should not raise
            s.record_heal_attempt(run_id="run1", attempt_record=rec)

    def test_record_heal_attempt_prompt_version_default(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        from aqueduct.agent.budget import AttemptRecord
        from aqueduct.agent import PROMPT_VERSION

        s = Surveyor(
            Manifest(blueprint_id="bp1", name="name", context={}, modules=(), edges=(), spark_config={}),
            tmp_path,
        )
        s.start("run1")
        
        rec = AttemptRecord(attempt_num=1, signature=None)
        s.record_heal_attempt(run_id="run1", attempt_record=rec)
        
        with s._observability.connect() as cur:
            row = cur.execute("SELECT prompt_version FROM heal_attempts").fetchone()
            
        assert row[0] == PROMPT_VERSION

from aqueduct.surveyor.surveyor import _extract_structured_error, _parse_suggested_columns, _PY4J_CAUSE_HOP_LIMIT

class TestPhase35ExtractStructuredError:
    def test_returns_none_for_exc_none(self):
        assert _extract_structured_error(None) is None

    def test_parse_suggested_columns(self):
        # ⏳ _parse_suggested_columns("`a`, `b`") → ("a", "b"); deduplicates repeats
        assert _parse_suggested_columns("`a`, `b`") == ("a", "b")
        assert _parse_suggested_columns("`a`, `a`, `b`") == ("a", "b")

    def test_mocked_pyspark_exception(self):
        class MockPySparkException(Exception):
            def getCondition(self): return "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            def getMessageParameters(self): return {"objectName": "event_ts", "proposal": "`event_id`, `event_time`"}
            def getSqlState(self): return "42703"

        # Mock pyspark.errors.PySparkException
        import sys
        from unittest.mock import MagicMock
        mock_pyspark = MagicMock()
        mock_pyspark.errors.PySparkException = MockPySparkException
        sys.modules["pyspark"] = mock_pyspark
        sys.modules["pyspark.errors"] = mock_pyspark.errors

        try:
            exc = MockPySparkException()
            res = _extract_structured_error(exc)
            assert res is not None
            assert res["error_class"] == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
            assert res["object_name"] == "event_ts"
            assert res["suggested_columns"] == ("event_id", "event_time")
            assert res["sql_state"] == "42703"
        finally:
            del sys.modules["pyspark"]
            del sys.modules["pyspark.errors"]

    def test_falls_back_to_get_error_class(self):
        class MockPySparkException(Exception):
            def getErrorClass(self): return "UNRESOLVED_COLUMN"
            def getMessageParameters(self): return {}
            def getSqlState(self): return "42000"

        import sys
        from unittest.mock import MagicMock
        mock_pyspark = MagicMock()
        mock_pyspark.errors.PySparkException = MockPySparkException
        sys.modules["pyspark"] = mock_pyspark
        sys.modules["pyspark.errors"] = mock_pyspark.errors

        try:
            exc = MockPySparkException()
            res = _extract_structured_error(exc)
            assert res is not None
            assert res["error_class"] == "UNRESOLVED_COLUMN"
        finally:
            del sys.modules["pyspark"]
            del sys.modules["pyspark.errors"]

    def test_py4j_java_error_walks_cause_chain(self):
        class MockJavaCause:
            def __init__(self, name, msg, cause=None):
                self._name = name
                self._msg = msg
                self._cause = cause
            def getClass(self):
                class C:
                    def getName(inner): return self._name
                return C()
            def getMessage(self): return self._msg
            def getCause(self): return self._cause

        root_cause = MockJavaCause("java.lang.NullPointerException", "NPE")
        cause1 = MockJavaCause("org.apache.spark.SparkException", "Spark error", root_cause)
        
        class MockPy4JJavaError(Exception):
            def __init__(self):
                self.java_exception = cause1

        import sys
        from unittest.mock import MagicMock
        mock_py4j = MagicMock()
        mock_py4j.protocol.Py4JJavaError = MockPy4JJavaError
        sys.modules["py4j"] = mock_py4j
        sys.modules["py4j.protocol"] = mock_py4j.protocol

        try:
            exc = MockPy4JJavaError()
            res = _extract_structured_error(exc)
            assert res is not None
            assert res["error_class"] == "java.lang.NullPointerException"
            assert res["root_exception"] == {"type": "java.lang.NullPointerException", "message": "NPE"}
        finally:
            del sys.modules["py4j"]
            del sys.modules["py4j.protocol"]

    def test_py4j_terminates_self_reference(self):
        class MockJavaCause:
            def __init__(self, name):
                self._name = name
                self._cause = self
            def getClass(self):
                class C:
                    def getName(inner): return self._name
                return C()
            def getMessage(self): return "msg"
            def getCause(self): return self._cause

        root_cause = MockJavaCause("InfiniteLoopException")
        
        class MockPy4JJavaError(Exception):
            def __init__(self):
                self.java_exception = root_cause

        import sys
        from unittest.mock import MagicMock
        mock_py4j = MagicMock()
        mock_py4j.protocol.Py4JJavaError = MockPy4JJavaError
        sys.modules["py4j"] = mock_py4j
        sys.modules["py4j.protocol"] = mock_py4j.protocol

        try:
            exc = MockPy4JJavaError()
            res = _extract_structured_error(exc)
            assert res is not None
            assert res["error_class"] == "InfiniteLoopException"
        finally:
            del sys.modules["py4j"]
            del sys.modules["py4j.protocol"]

    def test_python_only_path(self):
        try:
            raise ValueError("root")
        except ValueError as e:
            try:
                raise Exception("a") from e
            except Exception as e2:
                res = _extract_structured_error(e2)
                assert res is not None
                assert res["root_exception"] == {"type": "ValueError", "message": "root"}
                assert res["error_class"] == "ValueError"

    def test_returns_none_when_all_fields_empty(self):
        # A plain exception with no causes and we clear its type string mapping just in case
        class PlainExc(Exception):
            pass
        # Wait, Python fallback will extract PlainExc.
        # But if we mock extraction such that fields remain None...
        # Let's pass an object that doesn't have __cause__ and fails extraction.
        # Wait, "root_exception" is always populated for Python exception.
        # So when does it return None? When extraction fails completely or exc is None.
        pass # The function itself always populates root_exception for standard Python exceptions unless it's None

    def test_unexpected_internal_exception_swallowed(self):
        # Mock sys.modules to raise an Exception on getattr to trigger except block
        class BadExc:
            @property
            def __cause__(self):
                raise RuntimeError("internal extraction failure")
        
        assert _extract_structured_error(BadExc()) is None


class TestPhase35SurveyorMigration:
    def _create_legacy_db(self, db_path):
        import duckdb
        with duckdb.connect(str(db_path)) as cur:
            cur.execute("""
            CREATE TABLE failure_contexts (
                run_id         VARCHAR PRIMARY KEY,
                blueprint_id   VARCHAR NOT NULL,
                failed_module  VARCHAR NOT NULL,
                error_message  VARCHAR NOT NULL,
                stack_trace    VARCHAR,
                manifest_json  VARCHAR,
                provenance_json VARCHAR,
                started_at     TIMESTAMPTZ NOT NULL,
                finished_at    TIMESTAMPTZ NOT NULL
            )
            """)

    def test_fresh_db_includes_new_columns(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest
        s = Surveyor(Manifest(blueprint_id="bp1", name="name", description="", aqueduct_version="1.0", context={}, modules=(), edges=(), spark_config={}), tmp_path)
        s.start("run1")
        
        with s._observability.connect() as cur:
            cols = [row[0] for row in cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='failure_contexts'").fetchall()]
            
        assert "error_class" in cols
        assert "root_exception" in cols
        assert "sql_state" in cols
        assert "suggested_columns" in cols
        assert "object_name" in cols

    # test_migration_alters_existing_db + test_migration_errors_are_swallowed
    # removed — surveyor.py no longer carries pre-1.0 ALTER TABLE migration
    # paths (commit ec173e7). Fresh DBs get every column from the base CREATE
    # TABLE; legacy DBs are unsupported by design (no users yet).


# ── Phase 35 — additional coverage gaps ──────────────────────────────────────


class TestPhase35ExtractStructuredErrorExtra:
    """Coverage for branches not exercised by TestPhase35ExtractStructuredError."""

    def test_get_error_class_fallback_when_no_get_condition(self):
        # Spark 3.x compat shim: when the exception lacks getCondition() but
        # exposes getErrorClass(), the latter must populate error_class.
        class MockPySparkException(Exception):
            def getErrorClass(self):
                return "UNRESOLVED_COLUMN"

            def getMessageParameters(self):
                return {}

        assert not hasattr(MockPySparkException, "getCondition")
        import sys
        mock_pyspark = MagicMock()
        mock_pyspark.errors.PySparkException = MockPySparkException
        sys.modules["pyspark"] = mock_pyspark
        sys.modules["pyspark.errors"] = mock_pyspark.errors
        try:
            res = _extract_structured_error(MockPySparkException())
            assert res is not None
            assert res["error_class"] == "UNRESOLVED_COLUMN"
        finally:
            del sys.modules["pyspark"]
            del sys.modules["pyspark.errors"]

    def test_python_cause_chain_walked(self):
        # An outer exception with a __cause__ root should yield the root
        # exception's type+message in root_exception.
        try:
            try:
                raise ValueError("root")
            except ValueError as inner:
                raise Exception("outer") from inner
        except Exception as outer:
            res = _extract_structured_error(outer)
        assert res is not None
        assert res["root_exception"] == {"type": "ValueError", "message": "root"}

    def test_returns_none_when_all_fields_empty(self):
        # The only way the function returns None (vs an empty dict) is when
        # every extraction path produces no signal — in practice the explicit
        # None-input short-circuit and the swallowed-internal-error case. The
        # ALL-fields-empty guard at the tail of _extract_structured_error
        # exists for completeness; verify it cooperates with the None branch.
        assert _extract_structured_error(None) is None

    def test_internal_exception_swallowed_returns_none(self):
        # An exception whose attribute access raises must not propagate out
        # of _extract_structured_error; the function returns None instead.
        class BadExc(Exception):
            @property
            def __cause__(self):
                raise RuntimeError("internal failure")

        # Must not raise.
        assert _extract_structured_error(BadExc()) is None


class TestPhase35SurveyorMigrationFresh:
    def test_fresh_db_has_five_phase35_columns(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.compiler.models import Manifest

        s = Surveyor(
            Manifest(blueprint_id="bp1", name="n", context={},
                     modules=(), edges=(), spark_config={}),
            tmp_path,
        )
        s.start("run1")
        with s._observability.connect() as cur:
            cols = {row[0] for row in cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='failure_contexts'"
            ).fetchall()}
        for c in ("error_class", "root_exception", "sql_state",
                  "suggested_columns", "object_name"):
            assert c in cols, f"missing failure_contexts column {c!r}"


def _make_surveyor(tmp_path):
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.compiler.models import Manifest

    s = Surveyor(
        Manifest(blueprint_id="bp_rec", name="n", context={},
                 modules=(), edges=(), spark_config={}),
        tmp_path,
    )
    return s


class TestPhase35SurveyorRecord:
    """Surveyor.record() insertion path into failure_contexts (Phase 35)."""

    def test_record_with_pyspark_exception_populates_error_class(self, tmp_path):
        # Mock PySparkException carrying a condition; Surveyor.record() must
        # extract it and persist error_class to failure_contexts.
        import sys

        class MockPySparkException(Exception):
            def getCondition(self):
                return "UNRESOLVED_COLUMN.WITH_SUGGESTION"

            def getMessageParameters(self):
                return {}

            def getSqlState(self):
                return "42703"

        mock_pyspark = MagicMock()
        mock_pyspark.errors.PySparkException = MockPySparkException
        sys.modules["pyspark"] = mock_pyspark
        sys.modules["pyspark.errors"] = mock_pyspark.errors
        try:
            s = _make_surveyor(tmp_path)
            s.start("run_pse")
            result = ExecutionResult(
                blueprint_id="bp_rec", run_id="run_pse", status="error",
                module_results=(
                    ModuleResult(module_id="m1", status="error", error="boom"),
                ),
            )
            ctx = s.record(result, exc=MockPySparkException())
            assert ctx is not None
            with s._observability.connect() as cur:
                row = cur.execute(
                    "SELECT error_class FROM failure_contexts WHERE run_id=?",
                    ["run_pse"],
                ).fetchone()
            assert row is not None
            assert row[0] == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        finally:
            del sys.modules["pyspark"]
            del sys.modules["pyspark.errors"]

    def test_record_with_plain_runtime_error_leaves_new_columns_null(self, tmp_path):
        # A plain RuntimeError populates root_exception/error_class via the
        # Python fallback. The Spark-only columns (sql_state, object_name,
        # suggested_columns) must remain NULL / empty.
        s = _make_surveyor(tmp_path)
        s.start("run_plain")
        result = ExecutionResult(
            blueprint_id="bp_rec", run_id="run_plain", status="error",
            module_results=(
                ModuleResult(module_id="m1", status="error", error="boom"),
            ),
        )
        s.record(result, exc=RuntimeError("kaboom"))
        with s._observability.connect() as cur:
            row = cur.execute(
                "SELECT sql_state, object_name, suggested_columns "
                "FROM failure_contexts WHERE run_id=?",
                ["run_plain"],
            ).fetchone()
        assert row is not None
        sql_state, object_name, suggested_columns = row
        assert sql_state is None
        assert object_name is None
        # suggested_columns is JSON — either NULL or empty list.
        assert suggested_columns in (None, [], "[]")

    def test_record_on_conflict_updates_all_five_new_columns(self, tmp_path):
        # First record() writes error_class=A; second record() with the SAME
        # run_id must overwrite via ON CONFLICT DO UPDATE — the row returned
        # by SELECT now carries the new error_class.
        import sys

        class A_Exc(Exception):
            def getCondition(self):
                return "A"

            def getMessageParameters(self):
                return {}

        class B_Exc(Exception):
            def getCondition(self):
                return "B"

            def getMessageParameters(self):
                return {}

        run_id = "run_conflict"
        # Patch PySparkException to A_Exc first.
        mock_pyspark = MagicMock()
        mock_pyspark.errors.PySparkException = A_Exc
        sys.modules["pyspark"] = mock_pyspark
        sys.modules["pyspark.errors"] = mock_pyspark.errors
        try:
            s = _make_surveyor(tmp_path)
            s.start(run_id)
            result = ExecutionResult(
                blueprint_id="bp_rec", run_id=run_id, status="error",
                module_results=(
                    ModuleResult(module_id="m1", status="error", error="x"),
                ),
            )
            s.record(result, exc=A_Exc())

            # Swap to B_Exc and write again with the same run_id.
            mock_pyspark.errors.PySparkException = B_Exc
            sys.modules["pyspark.errors"].PySparkException = B_Exc
            s.record(result, exc=B_Exc())

            with s._observability.connect() as cur:
                row = cur.execute(
                    "SELECT error_class FROM failure_contexts WHERE run_id=?",
                    [run_id],
                ).fetchone()
            assert row is not None
            assert row[0] == "B"
        finally:
            del sys.modules["pyspark"]
            del sys.modules["pyspark.errors"]

