"""Warning-rule correctness — pins the 2026-07-03 audit fixes.

Each test asserts BOTH the firing condition and the load-bearing part of the
advice text, so a rule whose claim drifts from the implementation (the failure
class that prompted the audit) breaks a test instead of silently rotting.
"""

import warnings as W
from pathlib import Path

import pytest

from aqueduct.compiler.compiler import CompileError
from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.parser.parser import parse_dict

pytestmark = pytest.mark.unit

BASE = Path(".")


def _compile_caught(raw):
    with W.catch_warnings(record=True) as caught:
        W.simplefilter("always")
        compiler_compile(parse_dict(raw, BASE))
    return [str(w.message) for w in caught]


def _bp(modules, edges, **top):
    d = {"aqueduct": "1.0", "id": "bp", "name": "BP",
         "modules": modules, "edges": edges}
    d.update(top)
    return d


def _ingress(mid="raw"):
    return {"id": mid, "label": "R", "type": "Ingress",
            "config": {"format": "csv", "path": "d.csv"}}


class TestAppendRetryDupes:
    def _raw(self):
        return _bp(
            [_ingress(),
             {"id": "out", "label": "O", "type": "Egress",
              "config": {"format": "parquet", "path": "o", "mode": "append", "coalesce": 1}}],
            [{"from": "raw", "to": "out"}],
            retry_policy={"max_attempts": 3},
        )

    def test_fires_and_advice_is_safe(self):
        msgs = [m for m in _compile_caught(self._raw()) if "delivery_append_retry_dupes" in m]
        assert msgs, "rule did not fire"
        # The old advice ("Use mode=overwrite for idempotent writes") destroys
        # history on incremental sinks — must never come back unqualified.
        assert "txnAppId" in msgs[0]
        assert "destroys history" in msgs[0]


class TestIncrementalWatermarkScan:
    def _raw(self, egress_format):
        return _bp(
            [_ingress(),
             {"id": "inc", "label": "C", "type": "Channel",
              "config": {"op": "sql", "query": "SELECT * FROM raw",
                         "materialize": "incremental", "watermark_column": "ts"}},
             {"id": "out", "label": "O", "type": "Egress",
              "config": {"format": egress_format, "path": "o", "coalesce": 1,
                         "partition_by": ["d"], "mode": "append"}}],
            [{"from": "raw", "to": "inc"}, {"from": "inc", "to": "out"}],
        )

    def test_fires_for_non_delta_output_with_written_output_claim(self):
        msgs = [m for m in _compile_caught(self._raw("parquet"))
                if "perf_incremental_watermark_scan" in m]
        assert msgs
        # Claim must match the implementation (_compute_watermark_from_output
        # reads WRITTEN files) — not the old "second DAG scan" story.
        assert "WRITTEN" in msgs[0]
        assert "Checkpoint upstream" not in msgs[0]

    def test_all_delta_outputs_exempt(self):
        msgs = [m for m in _compile_caught(self._raw("delta"))
                if "perf_incremental_watermark_scan" in m]
        assert not msgs, "delta MAX() is txn-log stats — must not warn"


class TestPythonUdfArrowGate:
    def _raw(self, spark_config=None):
        return _bp(
            [_ingress(),
             {"id": "out", "label": "O", "type": "Egress",
              "config": {"format": "parquet", "path": "o", "coalesce": 1}}],
            [{"from": "raw", "to": "out"}],
            udf_registry=[{"id": "my_udf", "lang": "python", "module": "m", "entry": "f"}],
            spark_config=spark_config or {},
        )

    def test_fires_without_arrow_flag(self):
        msgs = [m for m in _compile_caught(self._raw())
                if "perf_python_udf_row_at_a_time" in m]
        assert msgs
        assert "pythonUDF.arrow.enabled" in msgs[0]  # actionable escape hatch named

    def test_skipped_when_arrow_enabled(self):
        cfg = {"spark.sql.execution.pythonUDF.arrow.enabled": "true"}
        msgs = [m for m in _compile_caught(self._raw(cfg))
                if "perf_python_udf_row_at_a_time" in m]
        assert not msgs, "Arrow-enabled blueprint must not get the row-at-a-time claim"


class TestDeltaAppendNoPartition:
    def _raw(self, maintenance=None):
        cfg = {"format": "delta", "path": "o", "mode": "append"}
        if maintenance:
            cfg["maintenance"] = maintenance
        return _bp(
            [_ingress(),
             {"id": "out", "label": "O", "type": "Egress", "config": cfg}],
            [{"from": "raw", "to": "out"}],
        )

    def test_fires_without_mitigation(self):
        assert any("perf_delta_append_no_partition" in m for m in _compile_caught(self._raw()))

    def test_skipped_when_maintenance_optimize_set(self):
        msgs = [m for m in _compile_caught(self._raw({"optimize": True}))
                if "perf_delta_append_no_partition" in m]
        assert not msgs, "maintenance.optimize already handles small files"


class TestMultiConsumerNoCache:
    def test_message_matches_code_and_carries_tradeoff(self):
        raw = _bp(
            [_ingress(),
             {"id": "ch", "label": "C", "type": "Channel",
              "config": {"op": "sql", "query": "SELECT * FROM raw"}},
             {"id": "o1", "label": "O1", "type": "Egress",
              "config": {"format": "parquet", "path": "a", "coalesce": 1}},
             {"id": "o2", "label": "O2", "type": "Egress",
              "config": {"format": "parquet", "path": "b", "coalesce": 1}}],
            [{"from": "raw", "to": "ch"}, {"from": "ch", "to": "o1"}, {"from": "ch", "to": "o2"}],
        )
        msgs = [m for m in _compile_caught(raw) if "perf_multi_consumer_no_cache" in m]
        assert msgs
        assert "is not checkpointed" in msgs[0]      # code checks the channel itself
        assert "no Checkpoint upstream" not in msgs[0]
        assert "measure" in msgs[0]                   # caching-tradeoff caveat


class TestNondeterministicFanoutAdvice:
    def test_advice_points_at_this_channel(self):
        raw = _bp(
            [_ingress(),
             {"id": "ch", "label": "C", "type": "Channel",
              "config": {"op": "sql", "query": "SELECT uuid() AS u FROM raw"}},
             {"id": "o1", "label": "O1", "type": "Egress",
              "config": {"format": "parquet", "path": "a", "coalesce": 1}},
             {"id": "o2", "label": "O2", "type": "Egress",
              "config": {"format": "parquet", "path": "b", "coalesce": 1}}],
            [{"from": "raw", "to": "ch"}, {"from": "ch", "to": "o1"}, {"from": "ch", "to": "o2"}],
        )
        msgs = [m for m in _compile_caught(raw) if "nondeterministic_fanout" in m]
        assert msgs
        # Exemption checks THIS channel's checkpoint flag — advice must agree.
        assert "checkpoint: true` on this Channel" in msgs[0]
        assert "upstream" not in msgs[0]


class TestMaintenanceOptimizeNonDeltaIsError:
    def test_escalated_to_compile_error(self):
        raw = _bp(
            [_ingress(),
             {"id": "out", "label": "O", "type": "Egress",
              "config": {"format": "parquet", "path": "o", "coalesce": 1,
                         "maintenance": {"optimize": True}}}],
            [{"from": "raw", "to": "out"}],
        )
        with pytest.raises(CompileError, match="OPTIMIZE is a Delta Lake operation"):
            compiler_compile(parse_dict(raw, BASE))
