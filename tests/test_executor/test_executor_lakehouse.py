"""Phase 59 — Iceberg/Hudi round-trip + maintenance integration tests.

Iceberg and Hudi are separate Maven artifacts, not bundled with pyspark. Their
DataSource is resolved by a ServiceLoader on the driver classloader at planning
time, so the jars must be on the classpath *before* the first ``SparkSession``
is created. ``tests/conftest.py`` handles that: when ``AQ_LAKEHOUSE`` is set it
derives the correct ``*-spark<line>_<scala>`` Maven coordinates from the running
pyspark version and injects them via ``PYSPARK_SUBMIT_ARGS`` at import time
(HEAD-checked against Maven Central, so a Spark line with no published build —
e.g. Hudi on Spark 3.3 — is simply dropped).

These tests are therefore **environment-gated**: ``_require_datasource`` skips
with a clear message when the format's jar is not on the classpath (jars not
provisioned, or no build exists for the running Spark version). To run them
locally::

    AQ_LAKEHOUSE=1 pytest tests/test_executor/test_executor_lakehouse.py

In CI the ``executor-tests`` job (test-suite.yml) and the ``compat`` matrix
(version-matrix.yml) set ``AQ_LAKEHOUSE=1``; the coordinates are derived per
matrix Spark version, so no version is hardcoded and no local jar is required.
"""

from __future__ import annotations

import pytest

from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.executor.spark.executor import execute as executor_execute
from aqueduct.parser.parser import parse_dict

pytestmark = [pytest.mark.spark, pytest.mark.integration, pytest.mark.slow]


def _require_datasource(spark, fmt: str) -> None:
    """Skip unless the format's jar is on the driver classpath.

    Reuses the executor's own jar-detection (``listJars`` substring match) so
    the gate is consistent with the runtime ``jar_availability`` warning.
    """
    from aqueduct.executor.spark.warnings.jar_availability import (
        _FORMAT_JAR_FRAGMENTS,
        _loaded_jar_names,
    )

    fragments = _FORMAT_JAR_FRAGMENTS.get(fmt) or []
    jars = _loaded_jar_names(spark)
    if not any(frag in jar for jar in jars for frag in fragments):
        pytest.skip(
            f"{fmt} DataSource jar not on classpath — set AQ_LAKEHOUSE=1 "
            f"(and ensure a published {fmt}-spark build exists for this Spark "
            f"version)"
        )


# ── Hudi ─────────────────────────────────────────────────────────────────────


def test_hudi_roundtrip_with_maintenance(spark, tmp_path):
    _require_datasource(spark, "hudi")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    spark.range(100).write.parquet(str(data_dir / "input.parquet"))
    out = tmp_path / "hudi_out"

    bp = parse_dict(
        {
            "aqueduct": "1.0",
            "id": "hudi-test",
            "name": "Hudi round-trip",
            "modules": [
                {
                    "id": "src",
                    "type": "Ingress",
                    "label": "src",
                    "config": {"format": "parquet", "path": str(data_dir / "input.parquet")},
                },
                {
                    "id": "sink",
                    "type": "Egress",
                    "label": "sink",
                    "config": {
                        "format": "hudi",
                        "mode": "overwrite",
                        "path": str(out),
                        # Hudi mandates a table name + record key on every write;
                        # forwarded verbatim to the writer via cfg["options"].
                        "options": {
                            "hoodie.table.name": "hudi_test_table",
                            "hoodie.datasource.write.recordkey.field": "id",
                        },
                        "maintenance": {"compaction": True, "clean": True},
                    },
                },
            ],
            "edges": [{"from": "src", "to": "sink"}],
        },
        base_dir=tmp_path,
    )

    manifest = compiler_compile(bp, blueprint_path=tmp_path)
    result = executor_execute(manifest, spark)
    assert result.status == "success", str(result.module_results)

    df = spark.read.format("hudi").load(str(out))
    assert df.count() == 100


# ── Iceberg ──────────────────────────────────────────────────────────────────


def test_iceberg_roundtrip_with_maintenance(spark, tmp_path):
    _require_datasource(spark, "iceberg")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    spark.range(100).write.parquet(str(data_dir / "input.parquet"))

    bp = parse_dict(
        {
            "aqueduct": "1.0",
            "id": "iceberg-test",
            "name": "Iceberg round-trip",
            "modules": [
                {
                    "id": "src",
                    "type": "Ingress",
                    "label": "src",
                    "config": {"format": "parquet", "path": str(data_dir / "input.parquet")},
                },
                {
                    "id": "sink",
                    "type": "Egress",
                    "label": "sink",
                    "config": {
                        "format": "iceberg",
                        "mode": "overwrite",
                        "table": "local.db.test_table",
                        "maintenance": {"rewrite_data_files": True, "expire_snapshots": True},
                    },
                },
            ],
            "edges": [{"from": "src", "to": "sink"}],
        },
        base_dir=tmp_path,
    )

    manifest = compiler_compile(bp, blueprint_path=tmp_path)
    result = executor_execute(manifest, spark)
    assert result.status == "success", str(result.module_results)

    df = spark.sql("SELECT count(*) FROM local.db.test_table")
    assert df.collect()[0][0] == 100


def test_ingress_format_iceberg_reads_written_table(spark, tmp_path):
    """`ingress.format.iceberg` — read an Iceberg table back through
    Aqueduct's own Ingress reader (not a raw ``spark.read``), proving the
    curated leaf's `supported` verdict against a real read, not just a
    write. Iceberg tables are catalog-identifier-addressed, not path-based;
    ``read_ingress`` passes a table identifier straight through to
    ``spark.read.format('iceberg').load(<identifier>)`` since ``format:``
    dispatch is a generic pass-through (see ``spark/ingress.py``)."""
    _require_datasource(spark, "iceberg")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    spark.range(50).write.parquet(str(data_dir / "input.parquet"))

    bp = parse_dict(
        {
            "aqueduct": "1.0",
            "id": "iceberg-ingress-test",
            "name": "Iceberg ingress read",
            "modules": [
                {
                    "id": "src",
                    "type": "Ingress",
                    "label": "src",
                    "config": {"format": "parquet", "path": str(data_dir / "input.parquet")},
                },
                {
                    "id": "sink",
                    "type": "Egress",
                    "label": "sink",
                    "config": {
                        "format": "iceberg",
                        "mode": "overwrite",
                        "table": "local.db.ingress_test_table",
                    },
                },
            ],
            "edges": [{"from": "src", "to": "sink"}],
        },
        base_dir=tmp_path,
    )
    manifest = compiler_compile(bp, blueprint_path=tmp_path)
    result = executor_execute(manifest, spark)
    assert result.status == "success", str(result.module_results)

    from aqueduct.executor.spark.ingress import read_ingress
    from aqueduct.models import Module

    module = Module(
        id="ing",
        type="Ingress",
        label="ing",
        config={"format": "iceberg", "path": "local.db.ingress_test_table"},
    )
    df = read_ingress(module, spark)
    assert df.count() == 50
