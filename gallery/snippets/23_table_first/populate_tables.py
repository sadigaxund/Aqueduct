"""Create demo tables in BOTH engines' catalogs — this snippet's
`aqueduct run` may target either `deployment.engine: spark` (the
aqueduct.yml default) or `deployment.engine: duckdb` (e.g.
`scripts/run_snippets.sh -e duckdb`), and each engine resolves `table:
demo_table` against its OWN catalog, so both must be seeded unconditionally
regardless of which engine the run afterward actually uses:

- Spark: the local Derby-based Hive metastore, shared with the blueprint's
  ``engine.spark.conf.spark.sql.catalogImplementation: hive`` so ``table:
  demo_table`` resolves in ``aqueduct run``.
- DuckDB: a persistent database file (``demo.duckdb``, matching
  ``aqueduct.yml``'s ``engine.duckdb.database_path``) — DuckDB's default
  ``:memory:`` connection is per-process and can't survive into the
  separate ``aqueduct run`` process otherwise.

Purges stale warehouse/metastore/catalog state before creating either
table, so re-running this script is always safe.
"""

import logging
import shutil
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession

logging.getLogger("py4j").setLevel(logging.WARN)
logging.getLogger("pyspark").setLevel(logging.WARN)

# Nuke stale Derby + warehouse + DuckDB catalog state so CREATE TABLE always
# works, on both engines.
for d in ("metastore_db", "spark-warehouse", "derby.log", "demo.duckdb", "demo.duckdb.wal"):
    p = Path(d)
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
    elif p.exists():
        p.unlink()

spark = (
    SparkSession.builder.master("local[1]")
    .appName("snippet-populate")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.log.level", "WARN")
    .getOrCreate()
)

spark.range(10).toDF("id").createOrReplaceTempView("_tmp_demo")
spark.sql("CREATE TABLE demo_table USING parquet AS SELECT id, id % 2 AS even FROM _tmp_demo")
spark.catalog.dropTempView("_tmp_demo")

spark.stop()
print("Created demo_table in Derby-based Hive catalog")

con = duckdb.connect("demo.duckdb")
con.execute(
    "CREATE OR REPLACE TABLE demo_table AS SELECT i AS id, i % 2 AS even FROM range(10) AS t(i)"
)
con.close()
print("Created demo_table in demo.duckdb")
