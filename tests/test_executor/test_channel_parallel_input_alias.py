"""Two Channels running in parallel must not clobber each other's `__input__`.

`__input__` is a single name in the SparkSession's temp-view catalog, reused by
every single-input SQL Channel in a Manifest. Under `--parallel` the executor
runs independent components on a ThreadPoolExecutor against the SAME
SparkSession (`aqueduct/executor/spark/executor.py`), so two Channels can be
inside the register/analyze/drop window at once: thread A creates `__input__`
over its frame, thread B drops and recreates it over a different frame, and
thread A's `spark.sql()` then analyzes against B's data. Wrong results, no
error.

The DuckDB engine is not affected: it declares `feature.parallel_mode:
unsupported` and runs the topological order serially.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_parallel_run_sql_calls_do_not_clobber_the_input_alias(spark):
    """A slow analyze in one thread must still resolve `__input__` to its own
    frame while a second thread is trying to register a different one."""
    from aqueduct.executor.spark import channel as ch

    df_a = spark.createDataFrame([(1,)], "v int")
    df_b = spark.createDataFrame([(2,)], "v int")

    real_sql = type(spark).sql
    seen: dict[str, list[int]] = {}

    def slow_sql(self, sqlQuery, *args, **kwargs):
        # Open a wide window between "the view is registered" and "the query
        # is analyzed". Without the lock the other thread walks straight into
        # it; with the lock it cannot start registering at all.
        time.sleep(0.4)
        name = threading.current_thread().name
        seen[name] = [
            r.v for r in real_sql(self, f"SELECT v FROM {ch._SINGLE_INPUT_ALIAS}").collect()
        ]
        return real_sql(self, sqlQuery, *args, **kwargs)

    errors: dict[str, BaseException] = {}

    def run(name: str, df, upstream_id: str) -> None:
        try:
            ch._run_sql(name, f"SELECT v FROM {ch._SINGLE_INPUT_ALIAS}", {upstream_id: df}, spark)
        except BaseException as exc:  # recorded, re-asserted on the main thread
            errors[name] = exc

    with patch.object(type(spark), "sql", slow_sql):
        t_a = threading.Thread(target=run, args=("A", df_a, "src_a"), name="A")
        t_b = threading.Thread(target=run, args=("B", df_b, "src_b"), name="B")
        t_a.start()
        time.sleep(0.05)  # A is inside the window before B starts
        t_b.start()
        t_a.join(timeout=60)
        t_b.join(timeout=60)

    assert not errors, errors
    assert seen["A"] == [1], (
        "thread A resolved __input__ to another thread's frame: the alias is "
        "shared across the whole SparkSession and the registration window is "
        "not serialized"
    )
    assert seen["B"] == [2]


def test_concurrent_single_input_channels_do_not_collide_on_the_alias(spark):
    """The second symptom of the same race, and the louder one.

    Without serialization, a thread that reaches `createTempView("__input__")`
    while another thread still holds the alias gets
    `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS` instead of silently wrong data, since
    the drop-then-create pair is not atomic either. Neither outcome is
    acceptable, and this one is the one a user actually sees.
    """
    from aqueduct.executor.spark import channel as ch

    real_sql = type(spark).sql

    def slow_sql(self, sqlQuery, *args, **kwargs):
        time.sleep(0.3)
        return real_sql(self, sqlQuery, *args, **kwargs)

    errors: dict[str, BaseException] = {}
    df = spark.createDataFrame([(1,)], "v int")

    def run(name: str, upstream_id: str) -> None:
        try:
            ch._run_sql(name, f"SELECT v FROM {ch._SINGLE_INPUT_ALIAS}", {upstream_id: df}, spark)
        except BaseException as exc:  # recorded, re-asserted on the main thread
            errors[name] = exc

    with patch.object(type(spark), "sql", slow_sql):
        threads = [
            threading.Thread(target=run, args=(f"m{i}", f"u{i}"), name=f"m{i}") for i in range(3)
        ]
        for t in threads:
            t.start()
            time.sleep(0.05)
        for t in threads:
            t.join(timeout=60)

    assert not errors, f"concurrent Channels collided on {ch._SINGLE_INPUT_ALIAS}: {errors}"
