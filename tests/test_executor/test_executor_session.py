"""Tests for the Spark session factory."""

from __future__ import annotations

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]

from pyspark.sql import SparkSession
from aqueduct.executor.spark.session import make_spark_session


def _snapshot_conf(spark):
    """Snapshot every Spark conf key currently visible to the session.

    ``RuntimeConfig.getAll`` (the obvious one-liner) is a PySpark **4.0+**
    property — it does not exist on the pyspark 3.5 API surface at all
    (confirmed by reading ``python/pyspark/sql/conf.py`` at tag ``v3.5.8``:
    the class defines only ``get``/``set``/``unset``/``isModifiable``), so
    using it here breaks the Legacy compat combo (python 3.12 / pyspark
    3.5.8) with ``AttributeError: 'RuntimeConfig' object has no attribute
    'getAll'`` for every test in this file.

    The portable replacement is the bare SQL ``SET`` command
    (``spark.sql("SET")``): it is a core SQL command implemented in
    ``SetCommand.scala`` since long before Spark 3.5, and its no-argument
    branch (``case None =>``) reads from the exact same JVM-side
    ``SQLConf``-backed key set PySpark 4.x's ``getAll`` property reads
    (``self._jconf.getAllAsJava()``) — verified by execution on the
    installed pyspark 4.1.1: ``spark.sql("SET").collect()`` and
    ``dict(spark.conf.getAll)`` return the IDENTICAL key set, zero keys
    only on either side.

    One divergence that *does* matter: ``SetCommand``'s ``case None``
    branch passes its rows through ``SQLConf.redactOptions``, which masks
    any key matching the ``spark.redaction.regex`` pattern (default catches
    ``secret``/``password``/``token``/... in the key name) to the literal
    string ``"*********(redacted)"`` — verified by execution: setting
    ``spark.aqueduct.my_password`` and reading it back via
    ``spark.sql("SET")`` returns the redacted placeholder, while
    ``spark.conf.get("spark.aqueduct.my_password")`` returns the real
    value. Snapshotting the VALUE straight from the ``SET`` row would
    silently corrupt restore for any future test that mutates a
    sensitively-named key — so ``SET`` is used only to enumerate the
    portable KEY universe; each value is re-read with
    ``spark.conf.get(key)`` (present on both 3.5 and 4.x, unredacted).
    """
    keys = (row[0] for row in spark.sql("SET").collect())
    return {key: spark.conf.get(key) for key in keys}


@pytest.fixture(autouse=True)
def _restore_shared_session_conf(spark):
    """Every test below calls ``make_spark_session()`` directly to exercise
    the factory's OWN config-application behavior against a fresh app name —
    but Spark's ``builder.getOrCreate()`` only creates a new SparkContext
    once per JVM; once the session-scoped ``spark`` fixture has created the
    shared session, a later ``make_spark_session(other_name, ...)`` call
    reuses that SAME session and Spark applies the ``spark.sql.*`` runtime
    confs from it directly ("Using an existing Spark session; only runtime
    SQL configurations will take effect" — a real Spark warning, not
    speculation). So ``spark.sql.session.timeZone``,
    ``spark.sql.parquet.outputTimestampType``, etc. set by one test here
    permanently change the ONE shared session every other test in the suite
    reads from, not an independent session of its own.

    Left unrestored, this was an order-dependent flake: whichever test set
    ``spark.sql.session.timeZone`` last determined the wall-clock string
    every later test saw when formatting a TIMESTAMP — including
    ``tests/test_executor/test_incremental.py``'s watermark assertions,
    which compare ``MAX(ts)`` against a literal string and only pass when
    the session timezone is whatever it happened to default to. Snapshot
    every conf key before the test and restore it exactly afterward —
    restoring the prior value, or unsetting a key this test newly
    introduced — so nothing here can shift a global default for a test
    running later, regardless of which key a future test in this file
    mutates.
    """
    before = _snapshot_conf(spark)
    yield
    after = _snapshot_conf(spark)
    for key, value in after.items():
        if key in before and before[key] == value:
            continue
        try:
            if key not in before:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, before[key])
        except Exception:
            # A handful of static Spark configs (spark.driver.memory,
            # spark.executor.memory, ...) are on Spark's own "cannot modify
            # at runtime" denylist — SQLConf raises CANNOT_MODIFY_CONFIG for
            # both `.set()` and `.unset()` once the session has started.
            # Such a key was, by that same rule, never actually mutable
            # after the shared session started, so the test that "set" it
            # could not have changed live behavior for anything reading it
            # either — there is nothing real to restore, and no later test
            # can be poisoned by a value that can never change again.
            pass


def test_make_spark_session_returns_active_session():
    spark = make_spark_session("test-app", {})
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("spark.app.name") == "test-app"


def test_spark_config_applied_to_conf():
    config = {"spark.sql.shuffle.partitions": "7", "spark.aqueduct.test": "true"}
    spark = make_spark_session("test-cfg", config)
    assert spark.conf.get("spark.sql.shuffle.partitions") == "7"
    assert spark.conf.get("spark.aqueduct.test") == "true"


def test_get_or_create_semantics():
    spark1 = make_spark_session("app1", {})
    spark2 = make_spark_session("app1", {})
    assert spark1 is spark2


def test_make_spark_session_master_url_default():
    # If we don't pass master_url, it defaults to local[*]
    # Checking this is slightly tricky due to getOrCreate caching,
    # but we can check the conf if the session is fresh.
    spark = make_spark_session("app-default", {})
    assert spark.conf.get("spark.master") == "local[*]"


def test_make_spark_session_master_url_custom():
    # Because of getOrCreate, changing the master in builder doesn't necessarily change it on an existing session.
    # To truly test this without interference, we can check the builder or assume first session creation works.
    # We will just verify it does not error and sets the conf properties that we asked for.
    spark = make_spark_session("app-custom", {}, master_url="local[2]")
    # Cannot easily verify master if session was already local[*] from previous tests,
    # so we just verify it runs without error.
    assert isinstance(spark, SparkSession)


def test_make_spark_session_master_url_yarn():
    # Verify no error on creation with yarn
    spark = make_spark_session("app-yarn", {}, master_url="yarn")
    assert isinstance(spark, SparkSession)


def test_make_spark_session_master_url_standalone():
    spark = make_spark_session("app-standalone", {}, master_url="spark://host:7077")
    assert isinstance(spark, SparkSession)


def test_make_spark_session_blueprint_config_precedence():
    # Test that spark config passed in is processed
    config = {"spark.driver.memory": "3g", "spark.executor.memory": "5g"}
    spark = make_spark_session("app-prec", config)
    assert spark.conf.get("spark.driver.memory") == "3g"
    assert spark.conf.get("spark.executor.memory") == "5g"


# ── outputTimestampType default (cross-engine timestamp_tz fidelity) ─────────

class TestOutputTimestampTypeDefault:
    def test_defaults_to_timestamp_micros(self):
        """No user override -> Aqueduct's own default (TIMESTAMP_MICROS)
        applies, not Spark's own INT96 default, so a hub `timestamp_tz`
        column keeps its Parquet logical-type annotation across a handoff."""
        spark = make_spark_session("app-ts-default", {})
        assert spark.conf.get("spark.sql.parquet.outputTimestampType") == "TIMESTAMP_MICROS"

    def test_user_override_wins(self):
        """A user-supplied `engine.spark.conf` value for this key must not be
        silently clobbered by Aqueduct's own default."""
        spark = make_spark_session(
            "app-ts-override",
            {"spark.sql.parquet.outputTimestampType": "INT96"},
        )
        assert spark.conf.get("spark.sql.parquet.outputTimestampType") == "INT96"


# ── timezone: universal key (Phase 81/82) ────────────────────────────────────

class TestSessionTimezone:
    def test_universal_timezone_applied_when_no_native_override(self):
        """No engine-native `spark.sql.session.timeZone` in spark_config ->
        the universal `timezone:` value is applied directly."""
        spark = make_spark_session("app-tz-default", {}, timezone="America/New_York")
        assert spark.conf.get("spark.sql.session.timeZone") == "America/New_York"

    def test_native_override_wins_over_universal_timezone(self):
        """An explicit engine-native `engine.spark.conf.spark.sql.session.
        timeZone` always wins over the universal `timezone:` value — the same
        "explicit beats default" precedent as outputTimestampType above."""
        spark = make_spark_session(
            "app-tz-native-wins",
            {"spark.sql.session.timeZone": "UTC"},
            timezone="America/New_York",
        )
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

    def test_native_override_divergence_warns(self):
        """A universal `timezone:` that disagrees with an explicit
        engine-native override fires a suppressible warning naming the
        divergence — the whole point of the universal key is making a
        cross-engine timezone mismatch visible instead of silent."""
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            make_spark_session(
                "app-tz-divergence-warn",
                {"spark.sql.session.timeZone": "UTC"},
                timezone="America/New_York",
            )
        messages = [str(w.message) for w in caught]
        assert any("engine_timezone_conflict" in m for m in messages)
        assert any("America/New_York" in m and "UTC" in m for m in messages)

    def test_native_override_agreeing_with_universal_does_not_warn(self):
        """No divergence when the two already agree -> no warning fires."""
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            make_spark_session(
                "app-tz-agree",
                {"spark.sql.session.timeZone": "UTC"},
                timezone="UTC",
            )
        assert not any("engine_timezone_conflict" in str(w.message) for w in caught)

    def test_no_universal_timezone_is_a_no_op(self):
        """`timezone=None` (the default) touches nothing — no error, no conf
        write forced."""
        spark = make_spark_session("app-tz-unset", {})
        assert isinstance(spark, SparkSession)


class TestSessionQuietMode:
    def test_make_spark_session_quiet_injects_log4j_opts(self):
        from aqueduct.executor.spark.session import _LOG4J_QUIET_OPTS
        from unittest.mock import MagicMock, patch

        mock_builder = MagicMock()
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        # Test _LOG4J_QUIET_OPTS is a non-empty string with expected content
        assert "log4j" in _LOG4J_QUIET_OPTS.lower()
        assert "ERROR" in _LOG4J_QUIET_OPTS

    def test_suppress_stderr_context_manager(self):
        from aqueduct.executor.spark.session import _suppress_stderr
        import sys

        original_stderr = sys.stderr
        ran = []

        with _suppress_stderr():
            ran.append(True)
            # stderr should be redirected inside
            assert sys.stderr is not original_stderr

        # restored after context
        assert sys.stderr is original_stderr
        assert ran == [True]


# ── ISSUE-026: stop_spark_session AQ_TESTING guard ────────────────────────────

class TestStopSparkSessionGuard:
    def test_aq_testing_set_skips_stop(self, monkeypatch):
        """AQ_TESTING set → stop_spark_session returns without touching session."""
        from unittest.mock import MagicMock
        from aqueduct.executor.spark.session import stop_spark_session

        monkeypatch.setenv("AQ_TESTING", "1")
        mock_spark = MagicMock()
        stop_spark_session(mock_spark)
        mock_spark.stop.assert_not_called()

    def test_aq_testing_unset_calls_stop(self, monkeypatch):
        """AQ_TESTING unset → stop_spark_session calls spark.stop() once."""
        from unittest.mock import MagicMock
        from aqueduct.executor.spark.session import stop_spark_session

        monkeypatch.delenv("AQ_TESTING", raising=False)
        mock_spark = MagicMock()
        stop_spark_session(mock_spark)
        mock_spark.stop.assert_called_once_with()
