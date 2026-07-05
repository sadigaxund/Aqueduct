import os
import shutil
import logging
from pyspark.sql import SparkSession
import pandas as pd

import pyspark

# Suppress Spark's own verbose INFO logging
logging.getLogger("py4j").setLevel(logging.WARN)
logging.getLogger("pyspark").setLevel(logging.WARN)

# Delta Lake setup (Spark 3.5 uses Delta 3.x, Spark 4.0+ uses Delta 4.x)
# Note: Starting with Delta 4.1.0, artifact IDs include the Spark version.
parts = pyspark.__version__.split(".")
major = int(parts[0])
minor = int(parts[1])

if major >= 4:
    if major == 4 and minor == 1:
        DELTA_PACKAGE = f"io.delta:delta-spark_4.1_2.13:4.2.0"
    elif major == 4 and minor == 0:
        DELTA_PACKAGE = f"io.delta:delta-spark_4.0_2.13:4.2.0"
    else:
        DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.1"
else:
    DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"

def main():
    target_dir = "data/input"
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    os.makedirs(target_dir, exist_ok=True)

    try:
        import cloudpickle
        from pyspark import cloudpickle as bundled
        bundled.dumps = cloudpickle.dumps
        bundled.loads = cloudpickle.loads
    except (ImportError, AttributeError):
        pass

    spark = SparkSession.builder \
        .appName("PopulateDelta") \
        .master("local[*]") \
        .config("spark.jars.packages", DELTA_PACKAGE) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

    print("Creating Delta table with 3 versions (0-2)...")
    
    # Version 0
    data = [{"user_id": 1, "action": "login", "timestamp": "2024-01-01 10:00:00"}]
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").save(target_dir)

    # Versions 1 to 2
    for i in range(1, 3):
        new_data = [{"user_id": 1, "action": f"click_{i}", "timestamp": f"2024-01-01 10:{i:02d}:00"}]
        new_df = spark.createDataFrame(new_data)
        new_df.write.format("delta").mode("append").save(target_dir)
        print(f"  ✓ Created version {i}")

    print(f"\nDelta table populated at '{target_dir}'")
    print("Run 'aqueduct run blueprint.yml' to test time-travel (version 2).")
    spark.stop()

if __name__ == "__main__":
    main()
