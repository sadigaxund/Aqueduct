"""Create demo tables in the local Spark session catalog."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("snippet-populate").getOrCreate()

spark.range(10).toDF("id").createOrReplaceTempView("_tmp_demo")
spark.sql("DROP TABLE IF EXISTS demo_table")
spark.sql("DROP TABLE IF EXISTS demo_output")
spark.sql("CREATE TABLE demo_table USING parquet AS SELECT id, id % 2 AS even FROM _tmp_demo")
spark.catalog.dropTempView("_tmp_demo")

spark.stop()
print("Created demo_table in local session catalog")
