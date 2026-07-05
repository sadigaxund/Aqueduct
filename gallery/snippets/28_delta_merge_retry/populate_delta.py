import logging, shutil
from pathlib import Path
from pyspark.sql import SparkSession
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

target = Path("data/output/delta_orders")
if target.exists():
    shutil.rmtree(target)

spark = SparkSession.builder \
    .appName("PopulateDeltaOrders") \
    .master("local[*]") \
    .config("spark.jars.packages", DELTA_PACKAGE) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.log.level", "WARN") \
    .getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/input/orders.csv")
df.write.format("delta").mode("overwrite").save(str(target))
print(f"Created initial Delta table at {target} with {df.count()} rows")
spark.stop()
