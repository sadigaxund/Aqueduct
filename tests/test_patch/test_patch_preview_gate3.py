"""Integration tests for Phase 29a Gate 3 sandbox replay."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aqueduct.config import AqueductConfig
from aqueduct.patch.gate_status import sandbox_gate_permits_auto_apply
from aqueduct.patch.preview import run_sandbox_gate

pytestmark = [pytest.mark.spark, pytest.mark.integration]

try:
    from aqueduct.executor.spark.ingress import read_ingress
except ImportError:
    pytest.skip("pyspark required", allow_module_level=True)


def test_gate3_pass_on_valid_blueprint(spark, sample_data, tmp_path):
    # Blueprint: Ingress -> Egress
    # sample_data fixture provides orders.parquet
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Gate 3",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "Input",
                "config": {"format": "parquet", "path": orders_path},
            },
            {
                "id": "out",
                "type": "Egress",
                "label": "Output",
                "config": {"format": "parquet", "path": str(tmp_path / "out"), "mode": "overwrite"},
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p1",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=5,
        spark_session=spark,
    )

    assert result.status == "pass"
    assert "sandbox replay succeeded" in result.detail
    assert result.sample_rows == 5
    assert len(result.egress_targets) == 1
    assert result.egress_targets[0]["id"] == "out"
    # Verify no file was actually written to tmp_path / "out"
    assert not (tmp_path / "out").exists()


def test_gate3_pass_detail_states_scope_for_config_only_patch(spark, sample_data, tmp_path):
    """A patch whose ops touch zero modules (e.g. set_engine_config) still
    gets status=pass on a clean sandbox replay — but the detail must say
    what actually happened (session built, sample replayed under the
    patched config) and what it did NOT prove (it cannot reproduce the
    cluster-scale resource failure the patch usually targets; efficacy is
    only proven by the full re-run). Before this change, `pass` read
    identically to a module-touching patch's replay ("sandbox replay
    succeeded..."), which a user reads as "the fix was validated" — it
    wasn't."""
    from aqueduct.patch.grammar import PatchSpec

    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Gate 3 config-only",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "Input",
                "config": {"format": "parquet", "path": orders_path},
            },
            {
                "id": "out",
                "type": "Egress",
                "label": "Output",
                "config": {"format": "parquet", "path": str(tmp_path / "out"), "mode": "overwrite"},
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    config_only_patch = PatchSpec.model_validate(
        {
            "patch_id": "cfg-1",
            "rationale": "bump shuffle partitions",
            "operations": [
                {
                    "op": "set_engine_config",
                    "engine": "spark",
                    "key": "spark.sql.shuffle.partitions",
                    "value": 200,
                }
            ],
        }
    )

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="cfg-1",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=5,
        spark_session=spark,
        patch_spec=config_only_patch,
    )

    assert result.status == "pass"
    assert "session built and sample replay succeeded" in result.detail
    assert "cannot reproduce the originating resource failure" in result.detail
    assert "validated by the full re-run" in result.detail
    # The old, over-claiming wording must not survive alongside the new one.
    assert "sandbox replay succeeded against" not in result.detail


def test_gate3_pass_detail_unchanged_for_module_patch(spark, sample_data, tmp_path):
    """A patch_spec that DOES touch a module keeps the ordinary
    "sandbox replay succeeded" wording — the honest-scope rewrite is
    scoped to zero-module (config-only) patches only."""
    from aqueduct.patch.grammar import PatchSpec

    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Gate 3 module patch",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "Input",
                "config": {"format": "parquet", "path": orders_path},
            },
            {
                "id": "out",
                "type": "Egress",
                "label": "Output",
                "config": {"format": "parquet", "path": str(tmp_path / "out"), "mode": "overwrite"},
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }
    module_patch = PatchSpec.model_validate(
        {
            "patch_id": "mod-1",
            "rationale": "fix format",
            "operations": [
                {
                    "op": "set_module_config_key",
                    "module_id": "in",
                    "key": "format",
                    "value": "parquet",
                }
            ],
        }
    )

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="mod-1",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=5,
        spark_session=spark,
        patch_spec=module_patch,
    )

    assert result.status == "pass"
    assert "sandbox replay succeeded against" in result.detail
    assert "cannot reproduce the originating resource failure" not in result.detail


def test_gate3_fail_on_compile_error(spark, tmp_path):
    # Blueprint with cycle
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Cycle",
        "modules": [
            {
                "id": "m1",
                "type": "Channel",
                "label": "M1",
                "config": {"op": "sql", "query": "SELECT 1"},
            },
            {
                "id": "m2",
                "type": "Channel",
                "label": "M2",
                "config": {"op": "sql", "query": "SELECT 1"},
            },
        ],
        "edges": [{"from": "m1", "to": "m2"}, {"from": "m2", "to": "m1"}],
    }

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p1",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        spark_session=spark,
    )

    assert result.status == "fail"
    assert "Cycle detected" in result.detail


def test_gate3_fail_on_runtime_error(spark, sample_data, tmp_path):
    # Blueprint with bad SQL
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Runtime",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "In",
                "config": {"format": "parquet", "path": orders_path},
            },
            {
                "id": "m1",
                "type": "Channel",
                "label": "M1",
                "config": {"op": "sql", "query": "SELECT * FROM non_existent_table"},
            },
        ],
        "edges": [{"from": "in", "to": "m1"}],
    }

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p1",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        spark_session=spark,
    )

    assert result.status == "fail"
    assert "non_existent_table" in result.detail


def test_ingress_sandbox_limit_honored(spark, sample_data, tmp_path):
    from aqueduct.parser.models import Module

    orders_path = str(sample_data / "orders.parquet")
    m = Module(
        id="in",
        type="Ingress",
        label="In",
        config={"format": "parquet", "path": orders_path, "sandbox_limit": 3},
    )
    df = read_ingress(m, spark)
    # orders.parquet has 10 rows. sandbox_limit=3 should return 3.
    assert df.count() == 3


# Helper to find FIXTURES
FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_ingress_no_sandbox_limit(spark, sample_data):
    from aqueduct.parser.models import Module

    orders_path = str(sample_data / "orders.parquet")
    m = Module(
        id="in", type="Ingress", label="In", config={"format": "parquet", "path": orders_path}
    )
    df = read_ingress(m, spark)
    # No limit applied
    assert "Limit" not in df._jdf.queryExecution().optimizedPlan().toString()


def test_gate3_sample_rows_zero(spark, sample_data, tmp_path):
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Zero Limit",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "In",
                "config": {"format": "parquet", "path": orders_path},
            }
        ],
        "edges": [],
    }
    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p0",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=0,
        spark_session=spark,
    )
    assert result.status == "pass"
    assert result.sample_rows is None


def test_gate3_temp_file_unlinked(spark, sample_data, tmp_path):
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Temp",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "In",
                "config": {"format": "parquet", "path": orders_path},
            }
        ],
        "edges": [],
    }
    # Mock NamedTemporaryFile to see what happens
    with patch("tempfile.NamedTemporaryFile") as mock_tmp:
        mock_file = MagicMock()
        mock_file.name = str(tmp_path / "mock.yml")
        mock_tmp.return_value.__enter__.return_value = mock_file

        run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="ptmp",
            failed_module=None,
            engine="spark",
            cfg=AqueductConfig(),
            spark_session=spark,
        )

    assert not (tmp_path / "mock.yml").exists()


def test_gate3_reports_unavailable_when_the_engine_will_not_start(tmp_path):
    # Mock make_spark_session (Spark's ExecutorProtocol.make_session) to raise —
    # the sandbox gate resolves the session THROUGH the protocol registry
    # (Phase 79), so patching Spark's own session constructor is still the
    # right seam for a Spark-target replay that cannot happen.
    with patch("aqueduct.executor.spark.session.make_spark_session") as mock_make:
        mock_make.side_effect = Exception("Spark down")
        bp = {
            "aqueduct": "1.0",
            "id": "test.gate3",
            "name": "Test Skip",
            "modules": [
                {
                    "id": "in",
                    "type": "Ingress",
                    "label": "In",
                    "config": {"format": "parquet", "path": "p"},
                }
            ],
            "edges": [],
        }
        result = run_sandbox_gate(
            bp,
            blueprint_path=tmp_path / "bp.yml",
            patch_id="p_skip",
            failed_module=None,
            engine="spark",
            cfg=AqueductConfig(),
            spark_session=None,  # Force it to call the engine's session factory
        )
        # `unavailable`, not `not_applicable`: a replay was owed and the
        # environment prevented it, so this must block auto-apply.
        assert result.status == "unavailable"
        assert not sandbox_gate_permits_auto_apply(result)
        # The detail must name the engine, the underlying cause, and — the
        # part that used to be missing — that nothing was verified.
        assert "spark" in result.detail
        assert "Spark down" in result.detail
        assert "NOT replayed" in result.detail


def test_ingress_limit_after_filter(spark, sample_data):
    from aqueduct.parser.models import Module

    orders_path = str(sample_data / "orders.parquet")
    # orders.parquet has 10 rows. US region has 5 rows.
    # filter US (5 rows) -> limit 2 -> result 2 rows.
    m = Module(
        id="in",
        type="Ingress",
        label="In",
        config={
            "format": "parquet",
            "path": orders_path,
            "partition_filters": "region = 'US'",
            "sandbox_limit": 2,
        },
    )
    df = read_ingress(m, spark)
    assert df.count() == 2
    # Verify both rows are US
    assert df.filter("region != 'US'").count() == 0


class _FakeDepotStore:
    """Minimal DepotStore stand-in for the depot-staleness tests — avoids
    depending on real stores.depots config wiring in AqueductConfig()."""

    def __init__(self, values: dict[str, str]):
        self._values = values

    def get(self, key: str, default: str = "") -> str:
        return self._values.get(key, default)


def _bp_reading_depot_key(orders_path: str, out_path: str) -> dict:
    """An Ingress -> Egress Blueprint whose Egress path is derived from a
    depot read, so run_sandbox_gate's compile resolves @aq.depot.get."""
    return {
        "aqueduct": "1.0",
        "id": "test.gate3.depot",
        "name": "Test Gate 3 depot staleness",
        "context": {"wm": "@aq.depot.get('wm')"},
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "Input",
                "config": {"format": "parquet", "path": orders_path},
            },
            {
                "id": "out",
                "type": "Egress",
                "label": "Output",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        "edges": [{"from": "in", "to": "out"}],
    }


def test_gate3_depot_staleness_notice_on_changed_key(
    spark, sample_data, tmp_path, capsys, monkeypatch
):
    """A depot key whose value moved between the failure and this recompile
    gets exactly one 'depot key ... changed since failure' line — printed to
    stderr and folded into the pass detail — without touching gate status."""
    fake_depot = _FakeDepotStore({"wm": "2026-02-01"})
    monkeypatch.setattr(
        "aqueduct.depot.depot.preview_depots",
        lambda cfg, blueprint_id: (fake_depot, {"default": fake_depot}),
    )

    orders_path = str(sample_data / "orders.parquet")
    bp = _bp_reading_depot_key(orders_path, str(tmp_path / "out"))

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p-depot-1",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=5,
        spark_session=spark,
        depot_reads_at_failure={"wm": "2026-01-01"},
    )

    assert result.status == "pass"
    expected_line = "depot key 'wm' changed since failure: '2026-01-01' → '2026-02-01'"
    assert expected_line in result.detail
    captured = capsys.readouterr()
    assert captured.err.count(expected_line) == 1
    # Exactly one notice line total — no other changed key exists.
    assert captured.err.strip().splitlines() == [expected_line]


def test_gate3_no_depot_staleness_notice_when_value_unchanged(
    spark, sample_data, tmp_path, capsys, monkeypatch
):
    fake_depot = _FakeDepotStore({"wm": "2026-01-01"})
    monkeypatch.setattr(
        "aqueduct.depot.depot.preview_depots",
        lambda cfg, blueprint_id: (fake_depot, {"default": fake_depot}),
    )

    orders_path = str(sample_data / "orders.parquet")
    bp = _bp_reading_depot_key(orders_path, str(tmp_path / "out"))

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p-depot-2",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=5,
        spark_session=spark,
        depot_reads_at_failure={"wm": "2026-01-01"},
    )

    assert result.status == "pass"
    assert "changed since failure" not in result.detail
    captured = capsys.readouterr()
    assert "changed since failure" not in captured.err


def test_gate3_no_depot_staleness_notice_when_depot_reads_at_failure_is_none(
    spark, sample_data, tmp_path, capsys, monkeypatch
):
    fake_depot = _FakeDepotStore({"wm": "2026-02-01"})
    monkeypatch.setattr(
        "aqueduct.depot.depot.preview_depots",
        lambda cfg, blueprint_id: (fake_depot, {"default": fake_depot}),
    )

    orders_path = str(sample_data / "orders.parquet")
    bp = _bp_reading_depot_key(orders_path, str(tmp_path / "out"))

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p-depot-3",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=5,
        spark_session=spark,
        # depot_reads_at_failure omitted — defaults to None
    )

    assert result.status == "pass"
    assert "changed since failure" not in result.detail
    captured = capsys.readouterr()
    assert "changed since failure" not in captured.err


def test_gate3_no_depot_staleness_notice_for_key_present_on_only_one_side(
    spark, sample_data, tmp_path, capsys, monkeypatch
):
    """A key present only in depot_reads_at_failure (patch removed the read)
    or only in the fresh compile (patch added it) is not a staleness signal
    — the current recompile only reads 'wm', so an unrelated failure-side
    key must produce no notice."""
    fake_depot = _FakeDepotStore({"wm": "2026-01-01"})
    monkeypatch.setattr(
        "aqueduct.depot.depot.preview_depots",
        lambda cfg, blueprint_id: (fake_depot, {"default": fake_depot}),
    )

    orders_path = str(sample_data / "orders.parquet")
    bp = _bp_reading_depot_key(orders_path, str(tmp_path / "out"))

    result = run_sandbox_gate(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p-depot-4",
        failed_module=None,
        engine="spark",
        cfg=AqueductConfig(),
        sample_rows=5,
        spark_session=spark,
        depot_reads_at_failure={"only_on_failure_side": "gone-now"},
    )

    assert result.status == "pass"
    assert "changed since failure" not in result.detail
    captured = capsys.readouterr()
    assert "changed since failure" not in captured.err


def test_ingress_limit_before_schema_hint(spark, sample_data):
    from aqueduct.parser.models import Module

    orders_path = str(sample_data / "orders.parquet")
    # schema_hint check should pass even with limit
    m = Module(
        id="in",
        type="Ingress",
        label="In",
        config={
            "format": "parquet",
            "path": orders_path,
            "sandbox_limit": 1,
            "schema_hint": {"order_id": "string", "amount": "double"},
        },
    )
    df = read_ingress(m, spark)  # should not raise
    assert df.count() == 1
