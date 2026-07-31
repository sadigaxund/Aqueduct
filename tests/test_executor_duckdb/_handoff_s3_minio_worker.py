"""Worker script for `test_handoff_s3_minio.py` — NOT collected by pytest
(no `test_` prefix). Runs cross-engine handoff over a real MinIO `s3a://`
root through `aqueduct.executor.orchestrator.run_polyglot`, the real code
path, and exits 0/prints an OK marker on success.

Why a subprocess, not an in-process pytest test: Spark needs `hadoop-aws`
on its JVM classpath, settable ONLY at the process's FIRST
`SparkSession.builder...getOrCreate()` — `spark.jars.packages` is a static
conf, invisible to an already-running session, and Java's classloader does
not let a jar be added to an already-initialized driver JVM after the fact
(measured: `SparkContext._jsc.addJar()` only ships jars to executors for
task closures, it does not extend the driver's OWN classloader — a
same-JVM `local[*]` write raises `ClassNotFoundException:
org.apache.hadoop.fs.s3a.S3AFileSystem` even after calling it and setting
`hadoopConfiguration()` directly). `tests/conftest.py` unconditionally
builds a PLAIN SparkSession (no hadoop-aws) as a side effect of the
module-level `_spark_is_healthy()` call that defines `requires_healthy_spark`
— this happens on import, before ANY test in this file could run, for
ANY pytest invocation that so much as collects this directory. So there is
no way to be "first" from inside a pytest process; a fresh subprocess is
the only way to get an uncontaminated JVM.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

import duckdb

from aqueduct.compiler.compiler import compile as ccompile
from aqueduct.executor.models import ExecutionStatus
from aqueduct.executor.orchestrator import run_polyglot
from aqueduct.models import ModuleType
from aqueduct.parser.parser import parse_dict
from aqueduct.surveyor.surveyor import Surveyor

_BUCKET = "aqueduct-test"
# hadoop-aws MUST match the installed pyspark's bundled hadoop-client jar
# (measured: pyspark 4.1.1 ships hadoop-client 3.4.2; a mismatched hadoop-aws
# patch version raises NoSuchMethodError on READ, not on write — write alone
# does not prove the versions agree). Bump together with any pyspark upgrade.
_HADOOP_AWS_COORD = "org.apache.hadoop:hadoop-aws:3.4.2"


def _bp(modules, edges):
    d = {
        "aqueduct": "1.0",
        "id": "s3_handoff_worker",
        "name": "t",
        "modules": modules,
        "edges": edges,
    }
    return parse_dict(d, base_dir=Path("/tmp"))


def _handoff_id(manifest):
    ids = [m.id for m in manifest.modules if m.type == ModuleType.Handoff]
    assert len(ids) == 1, "expected exactly one handoff module"
    return ids[0]


def _engine_configs(endpoint_hostport: str, access_key: str, secret_key: str) -> dict:
    return {
        "spark": {
            "spark.jars.packages": _HADOOP_AWS_COORD,
            "spark.hadoop.fs.s3a.endpoint": f"http://{endpoint_hostport}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        # Credentials flow through the `secrets:` env-provider resolver
        # (key NAMES here) — never a literal in engine_config, same wiring a
        # real run uses.
        "duckdb": {
            "s3_key_id_secret": "AQ_S3_KEY",
            "s3_secret_access_key_secret": "AQ_S3_SECRET",
            "s3_endpoint": endpoint_hostport,
            "s3_url_style": "path",
            "s3_use_ssl": False,
        },
    }


def _forward(tmp: Path, endpoint_hostport: str, access_key: str, secret_key: str) -> None:
    """Spark island writes the handoff spill to s3a:// FROM ITS EXECUTORS;
    the DuckDB island reads it back and writes the final Egress locally."""
    in_path = str(tmp / "in.parquet")
    out_path = str(tmp / "out")
    duckdb.sql("SELECT range AS n FROM range(5)").to_parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    assert len(manifest.islands) == 2, f"expected 2 islands, got {len(manifest.islands)}"
    handoff_id = _handoff_id(manifest)

    run_id = f"run-s3-handoff-{uuid.uuid4().hex[:8]}"
    handoff_root = f"s3a://{_BUCKET}/handoff-e2e-{uuid.uuid4().hex[:8]}"
    store_dir = tmp / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[2]",
        engine_configs=_engine_configs(endpoint_hostport, access_key, secret_key),
        secrets_config={"provider": "env"},
    )
    print("RESULT STATUS:", result.status)
    for r in result.module_results:
        print(" module:", r.module_id, r.status, getattr(r, "error", None))

    assert result.status == ExecutionStatus.SUCCESS, result.module_results
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["in"] == ExecutionStatus.SUCCESS
    assert statuses[handoff_id] == ExecutionStatus.SUCCESS
    assert statuses["out"] == ExecutionStatus.SUCCESS

    written = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    assert written == 5, f"expected 5 rows, got {written}"
    print("HANDOFF_S3_FORWARD_OK")


def _reverse(tmp: Path, endpoint_hostport: str, access_key: str, secret_key: str) -> None:
    """Reverse direction — DuckDB island produces, Spark island consumes
    the s3a:// spill."""
    in_path = str(tmp / "in.parquet")
    out_path = str(tmp / "out.parquet")
    duckdb.sql("SELECT range AS n FROM range(7)").to_parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "spark",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    assert len(manifest.islands) == 2, f"expected 2 islands, got {len(manifest.islands)}"
    handoff_id = _handoff_id(manifest)

    run_id = f"run-s3-handoff-rev-{uuid.uuid4().hex[:8]}"
    handoff_root = f"s3a://{_BUCKET}/handoff-e2e-rev-{uuid.uuid4().hex[:8]}"
    store_dir = tmp / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[2]",
        engine_configs=_engine_configs(endpoint_hostport, access_key, secret_key),
        secrets_config={"provider": "env"},
    )
    print("RESULT STATUS:", result.status)
    for r in result.module_results:
        print(" module:", r.module_id, r.status, getattr(r, "error", None))

    assert result.status == ExecutionStatus.SUCCESS, result.module_results
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["in"] == ExecutionStatus.SUCCESS
    assert statuses[handoff_id] == ExecutionStatus.SUCCESS
    assert statuses["out"] == ExecutionStatus.SUCCESS

    written = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out_path}/*.parquet')").fetchone()[0]
    assert written == 7, f"expected 7 rows, got {written}"
    print("HANDOFF_S3_REVERSE_OK")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--direction", choices=["forward", "reverse"], required=True)
    parser.add_argument("--tmp-dir", required=True)
    parser.add_argument("--endpoint", default="localhost:9000")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    args = parser.parse_args()

    os.environ["AQ_S3_KEY"] = args.access_key
    os.environ["AQ_S3_SECRET"] = args.secret_key

    tmp = Path(args.tmp_dir)
    tmp.mkdir(parents=True, exist_ok=True)
    if args.direction == "forward":
        _forward(tmp, args.endpoint, args.access_key, args.secret_key)
    else:
        _reverse(tmp, args.endpoint, args.access_key, args.secret_key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
