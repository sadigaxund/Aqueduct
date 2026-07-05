"""Create a Delta table with 3 versions for time_travel demo.

Run this once before running the blueprint.
"""
import logging, shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import pyspark

logging.getLogger("py4j").setLevel(logging.WARN)
logging.getLogger("pyspark").setLevel(logging.WARN)

parts = pyspark.__version__.split(".")
major, minor = int(parts[0]), int(parts[1])

if major >= 4:
    if major == 4 and minor == 1:
        DELTA_PACKAGE = "io.delta:delta-spark_4.1_2.13:4.2.0"
    elif major == 4 and minor == 0:
        DELTA_PACKAGE = "io.delta:delta-spark_4.0_2.13:4.2.0"
    else:
        DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.1"
else:
    DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"

spark = SparkSession.builder \
    .appName("time_travel_setup") \
    .master("local[*]") \
    .config("spark.jars.packages", DELTA_PACKAGE) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.log.level", "WARN") \
    .getOrCreate()

# Clean any previous data
shutil.rmtree("data/delta_events", ignore_errors=True)

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("status", StringType()),
])

# Version 1
v1 = spark.createDataFrame([(1, "alpha", "new"), (2, "beta", "new")], schema)
v1.write.format("delta").mode("overwrite").save("data/delta_events")

# Version 2
v2 = spark.createDataFrame(
    [(1, "alpha", "updated"), (2, "beta", "new"), (3, "gamma", "new")], schema
)
v2.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("data/delta_events")

# Version 3
v3 = spark.createDataFrame(
    [(1, "alpha", "completed"), (2, "beta", "updated"), (3, "gamma", "new"), (4, "delta", "new")],
    schema,
)
v3.write.format("delta").mode("overwrite").save("data/delta_events")

spark.stop()
print("Created data/delta_events with 3 versions")
