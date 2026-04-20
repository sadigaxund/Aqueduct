"""SparkListener — captures stage metrics per module (non-fatal, async).

Registered once on SparkContext at session creation.  Executor calls
``set_active_module(module_id)`` before each action and ``collect_metrics()``
after to retrieve rows/bytes written for that module.

Metrics arrive via ``onStageCompleted`` which fires on the listener thread
after each stage finishes.  The executor's action (e.g. ``.save()``) blocks
the driver thread until all stages complete, so by the time
``collect_metrics()`` is called the listener has already received all events
for that action.

Collected per module (summed across all stages triggered by that action):
  records_written  — output rows (from OutputMetrics)
  bytes_written    — output bytes
  records_read     — input rows (from InputMetrics)
  bytes_read       — input bytes
  duration_ms      — wall-clock stage duration sum
"""

from __future__ import annotations

import threading
from typing import Any


class AqueductMetricsListener:
    """Pure-Python SparkListener implementation via py4j callbacks.

    Usage::

        listener = AqueductMetricsListener()
        listener.register(spark)

        listener.set_active_module("my_egress")
        df.write.parquet(path)                  # action blocks until done
        metrics = listener.collect_metrics()    # returns dict, resets state
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_module: str | None = None
        self._accumulated: dict[str, Any] = _zero_metrics()

    # ── Public API ─────────────────────────────────────────────────────────────

    def register(self, spark: Any) -> None:
        """Attach listener to the SparkContext via py4j gateway."""
        try:
            jvm = spark.sparkContext._jvm
            jsc = spark.sparkContext._jsc
            # Wrap this Python object as a Java SparkListener via py4j
            jlistener = jvm.org.apache.spark.scheduler.SparkListener
            # PySpark exposes addSparkListener via _jsc
            jsc.sc().addSparkListener(
                jvm.PythonUtils.toSparkListener(self)  # type: ignore[attr-defined]
            )
        except Exception:
            # py4j bridge unavailable (e.g. unit tests with mock Spark) — no-op
            pass

    def set_active_module(self, module_id: str) -> None:
        with self._lock:
            self._active_module = module_id
            self._accumulated = _zero_metrics()

    def collect_metrics(self) -> dict[str, Any]:
        """Return accumulated metrics for the active module and reset state."""
        with self._lock:
            result = dict(self._accumulated)
            self._accumulated = _zero_metrics()
            self._active_module = None
        return result

    # ── SparkListener callbacks (called by Spark on listener thread) ───────────

    def onStageCompleted(self, stage_completed: Any) -> None:  # noqa: N802
        try:
            info = stage_completed.stageInfo()
            tm = info.taskMetrics()
            input_m = tm.inputMetrics()
            output_m = tm.outputMetrics()
            stage_duration = (
                info.completionTime().get() - info.submissionTime().get()
                if info.completionTime().isDefined() and info.submissionTime().isDefined()
                else 0
            )
            with self._lock:
                if self._active_module is None:
                    return
                acc = self._accumulated
                acc["records_read"] += input_m.recordsRead()
                acc["bytes_read"] += input_m.bytesRead()
                acc["records_written"] += output_m.recordsWritten()
                acc["bytes_written"] += output_m.bytesWritten()
                acc["duration_ms"] += max(0, stage_duration)
        except Exception:
            pass  # never let listener crash the pipeline

    # Required no-ops for the SparkListener interface
    def onJobStart(self, job_start: Any) -> None: pass  # noqa: N802, E704
    def onJobEnd(self, job_end: Any) -> None: pass  # noqa: N802, E704
    def onTaskStart(self, task_start: Any) -> None: pass  # noqa: N802, E704
    def onTaskEnd(self, task_end: Any) -> None: pass  # noqa: N802, E704
    def onStageSubmitted(self, stage_submitted: Any) -> None: pass  # noqa: N802, E704


def _zero_metrics() -> dict[str, Any]:
    return {
        "records_read": 0,
        "bytes_read": 0,
        "records_written": 0,
        "bytes_written": 0,
        "duration_ms": 0,
    }
