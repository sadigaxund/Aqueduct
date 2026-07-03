"""Create demo tables in the local Derby-based Hive catalog.

Must share the same Hive metastore (Derby) with the blueprint's
``spark_config`` so ``table: demo_table`` resolves in ``aqueduct run``.
Purges stale warehouse and metastore directories before creating the table.
"""
import logging
import shutil
from pathlib import Path
from pyspark.sql import SparkSession

logging.getLogger("py4j").setLevel(logging.WARN)
logging.getLogger("pyspark").setLevel(logging.WARN)

# Nuke stale Derby + warehouse state so CREATE TABLE always works.
for d in ("metastore_db", "spark-warehouse", "derby.log"):
    p = Path(d)
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
    elif p.exists():
        p.unlink()

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("snippet-populate") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.log.level", "WARN") \
    .getOrCreate()

spark.range(10).toDF("id").createOrReplaceTempView("_tmp_demo")
spark.sql("CREATE TABLE demo_table USING parquet AS SELECT id, id % 2 AS even FROM _tmp_demo")
spark.catalog.dropTempView("_tmp_demo")

spark.stop()
print("Created demo_table in Derby-based Hive catalog")
