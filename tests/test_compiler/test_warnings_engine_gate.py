"""Engine-gating for Spark-physical compiler warnings (Phase 78 duckdb-engine).

`aqueduct/compiler/warnings/*` rules give advice that is sometimes physical to
one engine (Spark task partitions, JDBC read parallelism, Kafka micro-batches,
driver-side custom-probe cost). Those rules must fire only when
``engine == "spark"`` and stay silent on any other engine — the advice itself
is correct for Spark and must not be reworded, only gated on emission.

Three of the four gated rules (JDBC ingress, Kafka ingress, Probe module) key
off a manifest shape that the DuckDB capability declaration already rejects at
compile time (``ingress.format.jdbc`` / ``ingress.format.kafka`` /
``module.type.Probe`` are all ``unsupported`` in
``aqueduct/executor/duckdb_/capabilities.yml``), so a full end-to-end
``compile(..., engine="duckdb")`` for those shapes raises ``CompileError``
before ever reaching the warnings pass — there is no warning to observe
because the pipeline never gets that far. Those three rules are therefore
tested by calling ``check(manifest, engine)`` directly against a Manifest
built via a normal Spark compile (Spark accepts every one of these shapes),
proving the gate itself works independent of today's capability table (which
could legitimately change to `ignored_with_warning` later).

`file_format_no_repartition` (Egress format=parquet/json/csv without
repartition/coalesce/partition_by) is NOT capability-gated on DuckDB
(parquet/json/csv are "ungated" formats per capability_leaves.py — always
available via read_parquet/read_json), so it is tested fully end-to-end on
both engines.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.compiler.warnings import (
    custom_probe_driver_code,
    file_format_no_repartition,
    jdbc_missing_partition,
    kafka_checkpoint_stale,
)
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit


def _compile_yaml(yaml_str: str, tmp_path: Path, engine: str = "spark"):
    bp_path = tmp_path / "test.yml"
    bp_path.write_text(yaml_str)
    bp = parse(str(bp_path))
    return compiler_compile(bp, blueprint_path=bp_path, engine=engine)


class TestFileFormatNoRepartitionEngineGate:
    """Spark-physical: 'one file per task partition' is Spark's shuffle
    behaviour, meaningless on DuckDB. Not capability-gated on DuckDB (parquet
    is an ungated format), so testable end to end on both engines."""

    _YAML = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: data
"""

    def test_warns_on_spark(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="spark")
        assert file_format_no_repartition.check(manifest, "spark")

    def test_does_not_warn_on_duckdb(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="duckdb")
        assert file_format_no_repartition.check(manifest, "duckdb") == []


class TestJdbcMissingPartitionEngineGate:
    """Spark-physical: partitionColumn/lowerBound/upperBound are Spark's own
    JDBC-source read options. `ingress.format.jdbc` is `unsupported` on
    DuckDB today, so the manifest is built via a Spark compile and the rule
    is exercised directly against both engine names."""

    _YAML = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: in
    type: Ingress
    label: IN
    config:
      format: jdbc
      options:
        url: "jdbc:postgresql://db/warehouse"
        dbtable: "orders"
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: data
      coalesce: 1
edges:
  - from: in
    to: out
"""

    def test_warns_on_spark(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="spark")
        assert jdbc_missing_partition.check(manifest, "spark")

    def test_does_not_warn_on_duckdb(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="spark")
        assert jdbc_missing_partition.check(manifest, "duckdb") == []


class TestKafkaCheckpointStaleEngineGate:
    """Spark-physical: 'micro-batch' is Spark Structured Streaming.
    `ingress.format.kafka` is `unsupported` on DuckDB today, so the manifest
    is built via a Spark compile and the rule exercised directly."""

    _YAML = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: in
    type: Ingress
    label: IN
    config:
      format: kafka
      options:
        subscribe: topic
        kafka.bootstrap.servers: "localhost:9092"
  - id: ch
    type: Channel
    label: CH
    checkpoint: true
    config:
      op: sql
      query: SELECT * FROM in
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: data
      coalesce: 1
edges:
  - from: in
    to: ch
  - from: ch
    to: out
"""

    def test_warns_on_spark(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="spark")
        assert kafka_checkpoint_stale.check(manifest, "spark")

    def test_does_not_warn_on_duckdb(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="spark")
        assert kafka_checkpoint_stale.check(manifest, "duckdb") == []


class TestCustomProbeDriverCodeEngineGate:
    """HALF-true rule (see module docstring): the underlying risk is not
    Spark-exclusive, but the wording ('driver', '.collect()/.count()') is —
    gated whole-hog on Spark per the brief rather than inventing unverified
    DuckDB advice. `module.type.Probe` is `unsupported` on DuckDB today, so
    the manifest is built via a Spark compile and the rule exercised
    directly."""

    _YAML = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: in
    type: Ingress
    label: IN
    config:
      format: parquet
      path: /dummy/in
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: /dummy/out
  - id: p1
    type: Probe
    label: P1
    attach_to: in
    config:
      signals:
        - type: custom
          module: myorg.probes
          entry: my_signal
edges:
  - from: in
    to: out
"""

    def test_warns_on_spark(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="spark")
        assert custom_probe_driver_code.check(manifest, "spark")

    def test_does_not_warn_on_duckdb(self, tmp_path):
        manifest = _compile_yaml(self._YAML, tmp_path, engine="spark")
        assert custom_probe_driver_code.check(manifest, "duckdb") == []


class TestEngineAgnosticWarningsFireOnBothEngines:
    """Guard: engine-agnostic rules (SQL-semantics / pure wiring checks) must
    keep firing on every engine — only Spark-physical rules get gated."""

    _YAML = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: in
    type: Ingress
    label: IN
    config: {format: parquet, path: data}
  - id: ch
    type: Channel
    label: CH
    config:
      op: sql
      query: "SELECT COUNT(id) AS n FROM in"
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: out
      coalesce: 1
edges:
  - from: in
    to: ch
  - from: ch
    to: out
"""

    @pytest.mark.parametrize("engine", ["spark", "duckdb"])
    def test_count_col_likely_count_star_fires_on_every_engine(self, tmp_path, engine):
        import warnings as w

        from aqueduct import AqueductWarning

        with w.catch_warnings(record=True) as caught:
            w.simplefilter("always", AqueductWarning)
            # separate tmp dirs per param to avoid path collisions
            engine_tmp = tmp_path / engine
            engine_tmp.mkdir()
            _compile_yaml(self._YAML, engine_tmp, engine=engine)
            assert any("count_col_likely_count_star" in str(x.message) for x in caught)
