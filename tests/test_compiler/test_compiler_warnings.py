import pytest
import warnings
from pathlib import Path
from aqueduct import AqueductWarning
from aqueduct.parser.parser import parse
from aqueduct.compiler.compiler import compile as compiler_compile

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _ensure_warnings_caught():
    warnings.simplefilter("always", AqueductWarning)
    yield


def _compile_yaml(yaml_str: str, tmp_path: Path):
    bp_path = tmp_path / "test.yml"
    bp_path.write_text(yaml_str)
    bp = parse(str(bp_path))
    return compiler_compile(bp, blueprint_path=bp_path)


class TestCompilerWarnings:
    def test_probe_sample_signals_warn(self, tmp_path):
        for sig in ["null_rates", "row_count_estimate", "value_distribution", "distinct_count"]:
            yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Channel
    label: M1
    config: {{}}
  - id: p1
    type: Probe
    label: P1
    attach_to: m1
    config:
      signals:
        - type: {sig}
        - type: schema_snapshot
"""
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always", AqueductWarning)
                _compile_yaml(yaml_str, tmp_path)
                assert any(
                    "FULL DATASET SCAN" in str(x.message) and "probe-sample-cost" in str(x.message)
                    for x in w
                ), f"No warning for signal {sig}"

    def test_probe_safe_signals_no_warn(self, tmp_path):
        for sig_yaml in [
            "type: row_count_estimate\n          method: spark_listener",
            "type: schema_snapshot",
            "type: partition_stats"
        ]:
            yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Channel
    label: M1
    config: {{}}
  - id: p1
    type: Probe
    label: P1
    attach_to: m1
    config:
      signals:
        - {sig_yaml}
            """
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                _compile_yaml(yaml_str, tmp_path)

    def test_incremental_channel_no_checkpoint_warns(self, tmp_path):
        yaml_str = """
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
      materialize: incremental
edges:
  - from: in
    to: ch
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert any("incremental-watermark-scan" in str(x.message) for x in w)

    def test_incremental_channel_with_checkpoint_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: cp
    type: Channel
    label: CP
    checkpoint: true
    config: {}
  - id: ch
    type: Channel
    label: CH
    config:
      materialize: incremental
edges:
  - from: cp
    to: ch
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_python_udf_warns(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
udf_registry:
  - id: my_udf
    lang: python
modules: []
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert any("row-at-a-time" in str(x.message) and "python-udf-performance" in str(x.message) for x in w), f"No UDF warning in {[str(x.message) for x in w]}"
            
    def test_default_udf_warns(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
udf_registry:
  - id: my_udf
modules: []
        """
        # Default lang is python, so it should warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert any("row-at-a-time" in str(x.message) and "python-udf-performance" in str(x.message) for x in w)

    def test_java_udf_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
udf_registry:
  - id: my_udf
    lang: java
modules: []
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_egress_append_small_files_warn(self, tmp_path):
        for fmt in ["delta", "parquet"]:
            yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: out
    type: Egress
    label: OUT
    config:
      format: {fmt}
      mode: append
      path: data
            """
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always", AqueductWarning)
                _compile_yaml(yaml_str, tmp_path)
                assert any("small files" in str(x.message) and "planned-future-checks" in str(x.message) for x in w), f"No egress warning for format {fmt}: {[str(x.message) for x in w]}"

    def test_egress_append_with_partition_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: out
    type: Egress
    label: OUT
    config:
      format: delta
      mode: append
      path: data
      partition_by: [col]
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_egress_overwrite_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: out
    type: Egress
    label: OUT
    config:
      format: delta
      mode: overwrite
      path: data
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_channel_multi_consumer_no_checkpoint_warns(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: ch
    type: Channel
    label: CH
    config: {}
  - id: out1
    type: Egress
    label: O1
    config: {format: noop}
  - id: out2
    type: Egress
    label: O2
    config: {format: noop}
edges:
  - from: ch
    to: out1
  - from: ch
    to: out2
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert any("downstream consumers" in str(x.message) and "caching-strategy" in str(x.message) for x in w)

    def test_channel_multi_consumer_with_checkpoint_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: ch
    type: Channel
    label: CP
    checkpoint: true
    config: {}
  - id: out1
    type: Egress
    label: O1
    config: {format: noop}
  - id: out2
    type: Egress
    label: O2
    config: {format: noop}
edges:
  - from: ch
    to: out1
  - from: ch
    to: out2
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_channel_single_consumer_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: ch
    type: Channel
    label: CH
    config: {}
  - id: out1
    type: Egress
    label: O1
    config: {format: noop}
edges:
  - from: ch
    to: out1
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_ingress_hadoop_options_warn(self, tmp_path):
        for opt_key in ["fs.s3a.access.key", "fs.gs.project.id", "fs.azure.account.key"]:
            yaml_str = f"""
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: in
    type: Ingress
    label: IN
    config:
      format: parquet
      path: data
      options:
        {opt_key}: "secret"
            """
            with pytest.warns(AqueductWarning, match="Hadoop filesystem keys in 'options'"):
                _compile_yaml(yaml_str, tmp_path)

    def test_ingress_non_hadoop_options_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: in
    type: Ingress
    label: IN
    config:
      format: csv
      path: data
      options:
        header: "true"
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_non_ingress_hadoop_options_no_warn(self, tmp_path):
        yaml_str = """
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
      options:
        fs.s3a.access.key: "secret"
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)


class TestMaintenanceOptimizeWarning:
    """Warning 8g — maintenance.optimize on non-Delta Egress."""

    def test_optimize_true_parquet_warns(self, tmp_path):
        yaml_str = """
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
      maintenance:
        optimize: true
        """
        with pytest.warns(AqueductWarning, match="OPTIMIZE is a Delta Lake operation"):
            _compile_yaml(yaml_str, tmp_path)

    def test_optimize_true_delta_no_warn(self, tmp_path):
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: out
    type: Egress
    label: OUT
    config:
      format: delta
      path: data
      maintenance:
        optimize: true
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)

    def test_vacuum_only_parquet_no_warn(self, tmp_path):
        yaml_str = """
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
      maintenance:
        vacuum: 168
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _compile_yaml(yaml_str, tmp_path)
