import warnings
from pathlib import Path

import pytest

from aqueduct import AqueductWarning
from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.parser.parser import parse

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
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
  - id: p1
    type: Probe
    label: P1
    attach_to: m1
    config:
      signals:
        - type: {sig}
        - type: schema_snapshot
edges:
  - from: m1
    to: e1
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
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
  - id: p1
    type: Probe
    label: P1
    attach_to: m1
    config:
      signals:
        - {sig_yaml}
edges:
  - from: m1
    to: e1
            """
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                _compile_yaml(yaml_str, tmp_path)

    def test_spillway_condition_no_edge_warns(self, tmp_path):
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
      op: sql
      query: SELECT * FROM in
      spillway_condition: "id < 0"
  - id: out
    type: Egress
    label: OUT
    config: {format: parquet, path: out, mode: overwrite}
edges:
  - from: in
    to: ch
  - from: ch
    to: out
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert any(
                "spillway_port_mismatch" in str(x.message)
                and "no spillway edge" in str(x.message)
                for x in w
            )

    def test_spillway_both_present_no_warn(self, tmp_path):
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
      op: sql
      query: SELECT * FROM in
      spillway_condition: "id < 0"
  - id: main
    type: Egress
    label: MAIN
    config: {format: parquet, path: main, mode: overwrite}
  - id: spill
    type: Egress
    label: SPILL
    config: {format: parquet, path: spill, mode: overwrite}
edges:
  - from: in
    to: ch
  - from: ch
    to: main
  - from: ch
    to: spill
    port: spillway
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert not any("spillway_port_mismatch" in str(x.message) for x in w)

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
                assert any("small files" in str(x.message) and "append-no-partition" in str(x.message) for x in w), f"No egress warning for format {fmt}: {[str(x.message) for x in w]}"

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


class TestCustomProbe:
    """Phase 60 — custom probe signal compile-time behaviour."""

    def _bp(self, signal_block: str) -> str:
        return f"""
aqueduct: "1.0"
id: test
name: Test
modules:
  - id: m1
    type: Ingress
    label: M1
    config:
      format: parquet
      path: /dummy/in
  - id: e1
    type: Egress
    label: E1
    config:
      format: parquet
      path: /dummy/out
  - id: p1
    type: Probe
    label: P1
    attach_to: m1
    config:
      signals:
        - {signal_block}
edges:
  - from: m1
    to: e1
"""

    def test_pointer_custom_signal_warns_driver_code(self, tmp_path):
        yaml_str = self._bp("type: custom\n          module: myorg.p\n          entry: f")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert any(
                "custom_probe_driver_code" in str(x.message) and "driver" in str(x.message)
                for x in w
            )

    def test_inline_sql_custom_signal_no_warn(self, tmp_path):
        yaml_str = self._bp('type: custom\n          sql: "MAX(amount)"')
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert not any("custom_probe_driver_code" in str(x.message) for x in w)

    def test_conflicting_custom_signal_fails_compile(self, tmp_path):
        from aqueduct.errors import AqueductError

        yaml_str = self._bp('type: custom\n          sql: "MAX(x)"\n          plugin: p')
        with pytest.raises(AqueductError, match="conflicting sources"):
            _compile_yaml(yaml_str, tmp_path)

    def test_empty_custom_signal_fails_compile(self, tmp_path):
        from aqueduct.errors import AqueductError

        yaml_str = self._bp("type: custom")
        with pytest.raises(AqueductError, match="exactly one source"):
            _compile_yaml(yaml_str, tmp_path)


class TestBlueprintWarningSuppress:
    """Per-Blueprint compile-warning suppression (`warnings.suppress` in the
    Blueprint YAML). Covers all compile-time warnings — the modular registry
    (`aqueduct/compiler/warnings/run_all`) and the inline section-7/8 compiler
    checks — unioned with the engine-level suppress set, never touching the
    process-global `set_default_suppress` default (see
    `aqueduct/compiler/compiler.py`).
    """

    _REPARTITION_YAML = """
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

    def test_blueprint_suppress_silences_that_rule(self, tmp_path):
        yaml_str = self._REPARTITION_YAML + "warnings:\n  suppress: [file_format_no_repartition]\n"
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert not any("file_format_no_repartition" in str(x.message) for x in w)

    def test_without_suppress_block_still_warns(self, tmp_path):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(self._REPARTITION_YAML, tmp_path)
            assert any("file_format_no_repartition" in str(x.message) for x in w)

    def test_engine_and_blueprint_suppress_merge(self, tmp_path):
        """Engine suppresses one rule, Blueprint suppresses a different rule —
        both must be silent (union), not just the blueprint's own."""
        from aqueduct.warnings import _DEFAULT_SUPPRESS, set_default_suppress

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
      op: sql
      query: "SELECT COUNT(id) AS n FROM in"
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: out
warnings:
  suppress: [file_format_no_repartition]
"""
        saved = set(_DEFAULT_SUPPRESS)
        try:
            set_default_suppress(["count_col_likely_count_star"])
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always", AqueductWarning)
                _compile_yaml(yaml_str, tmp_path)
                msgs = [str(x.message) for x in w]
                assert not any("file_format_no_repartition" in m for m in msgs)
                assert not any("count_col_likely_count_star" in m for m in msgs)
        finally:
            set_default_suppress(saved)

    def test_blueprint_star_silences_all_compile_warnings(self, tmp_path):
        yaml_str = self._REPARTITION_YAML + 'warnings:\n  suppress: ["*"]\n'
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert not any(isinstance(x.message, AqueductWarning) for x in w)

    def test_blueprint_suppress_covers_inline_rules(self, tmp_path):
        """Blueprint suppress must also silence the inline section-7/8
        compiler warnings (e.g. delivery_append_retry_dupes), not just the
        modular-registry rules — regression for the union being applied only
        to the `run_all` pass."""
        yaml_str = """
aqueduct: "1.0"
id: test
name: Test
retry_policy:
  max_attempts: 3
modules:
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: data
      mode: append
warnings:
  suppress: [delivery_append_retry_dupes, file_format_no_repartition]
"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            _compile_yaml(yaml_str, tmp_path)
            assert not any("delivery_append_retry_dupes" in str(x.message) for x in w)

    def test_arcade_sub_blueprint_own_warnings_block_parses_and_is_ignored(self, tmp_path):
        """Sub-Blueprint's own `warnings:` block must PARSE (valid on its own)
        but is ignored — only the parent's suppress applies to the expanded
        compilation unit."""
        arcades_dir = tmp_path / "arcades"
        arcades_dir.mkdir()
        sub_yaml = """
aqueduct: "1.0"
id: sub.pipeline
name: Sub
modules:
  - id: out
    type: Egress
    label: OUT
    config:
      format: parquet
      path: sub_data
warnings:
  suppress: [kafka_checkpoint_stale]
"""
        (arcades_dir / "sub.yml").write_text(sub_yaml)

        parent_yaml = """
aqueduct: "1.0"
id: parent.pipeline
name: Parent
modules:
  - id: arc
    type: Arcade
    label: ARC
    ref: arcades/sub.yml
    context_override: {}
warnings:
  suppress: [file_format_no_repartition]
"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", AqueductWarning)
            manifest = _compile_yaml(parent_yaml, tmp_path)
            assert any(m.id == "arc__out" for m in manifest.modules)
            assert not any("file_format_no_repartition" in str(x.message) for x in w)
