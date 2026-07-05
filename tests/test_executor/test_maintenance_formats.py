"""Phase 59 (Iceberg/Hudi) — format-aware maintenance SQL + doctor catalog check.

Pure unit tests over the SQL-string builder and the doctor check (no Spark, no
jars). Live OPTIMIZE/rewrite/compaction on real tables is a spark-fixture stub
in tests/test_backlog.py.
"""
from __future__ import annotations

from types import SimpleNamespace as NS

import pytest

from aqueduct.executor.spark.egress import EgressError, build_maintenance_ops
from aqueduct.parser.models import ModuleType

pytestmark = [pytest.mark.unit, pytest.mark.spark]


# ── maintenance ops builder ─────────────────────────────────────────────────

def test_delta_ops_optimize_and_vacuum():
    ops = build_maintenance_ops("delta", "/t", None, {"optimize": True, "zorder_by": "id", "vacuum": 168})
    slots = {slot: sql for slot, _label, sql in ops}
    assert slots["optimize"] == "OPTIMIZE delta.`/t` ZORDER BY (id)"
    assert slots["vacuum"] == "VACUUM delta.`/t` RETAIN 168 HOURS"


def test_iceberg_ops_use_catalog_procedures():
    ops = build_maintenance_ops("iceberg", "", "local.db.orders",
                                {"rewrite_data_files": True, "expire_snapshots": True})
    labels = {label: sql for _slot, label, sql in ops}
    assert labels["rewrite_data_files"] == "CALL local.system.rewrite_data_files(table => 'db.orders')"
    assert labels["expire_snapshots"] == "CALL local.system.expire_snapshots(table => 'db.orders')"
    # rewrite is compaction-class (optimize slot), expire is cleanup-class (vacuum slot)
    assert [slot for slot, _l, _s in ops] == ["optimize", "vacuum"]


def test_iceberg_without_catalog_table_raises():
    with pytest.raises(EgressError, match="catalog table"):
        build_maintenance_ops("iceberg", "/path", None, {"rewrite_data_files": True})
    with pytest.raises(EgressError, match="catalog table"):
        build_maintenance_ops("iceberg", "", "db.tbl", {"rewrite_data_files": True})  # not fully-qualified


def test_hudi_ops_path_based():
    ops = build_maintenance_ops("hudi", "/lake/t", None, {"compaction": True, "clean": True})
    labels = {label: sql for _slot, label, sql in ops}
    assert labels["run_compaction"] == "CALL run_compaction(op => 'run', path => '/lake/t')"
    assert labels["run_clean"] == "CALL run_clean(path => '/lake/t')"


def test_no_ops_when_nothing_requested():
    assert build_maintenance_ops("delta", "/t", None, {}) == []
    assert build_maintenance_ops("iceberg", "", "c.d.t", {}) == []


# ── doctor iceberg-catalog check ────────────────────────────────────────────

def _iceberg_manifest(spark_config):
    m = NS(type=ModuleType.Ingress, config={"format": "iceberg", "path": "x"}, id="load")
    return NS(modules=[m], spark_config=spark_config)


def test_doctor_warns_when_iceberg_has_no_catalog():
    from aqueduct.doctor import _check_iceberg_catalog
    res = _check_iceberg_catalog(_iceberg_manifest({}))
    assert len(res) == 1 and res[0].status == "warn" and res[0].name == "iceberg_catalog"


def test_doctor_quiet_when_catalog_present():
    from aqueduct.doctor import _check_iceberg_catalog
    cfg = {"spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog"}
    assert _check_iceberg_catalog(_iceberg_manifest(cfg)) == []


def test_doctor_quiet_when_no_iceberg_module():
    from aqueduct.doctor import _check_iceberg_catalog
    m = NS(type=ModuleType.Ingress, config={"format": "parquet", "path": "x"}, id="load")
    assert _check_iceberg_catalog(NS(modules=[m], spark_config={})) == []
