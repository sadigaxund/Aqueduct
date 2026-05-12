import os
import shutil
from pyspark.sql import SparkSession
import pandas as pd

# Delta Lake setup (Spark 3.5 requires Delta 3.x)
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"

def main():
    target_dir = "data/input"
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    os.makedirs(target_dir, exist_ok=True)

    # Fix for PySpark 3.14 serialization issues
    try:
        import cloudpickle
        import pyspark.cloudpickle as bundled
        bundled.dumps = cloudpickle.dumps
        bundled.loads = cloudpickle.loads
    except ImportError:
        pass

    print("Initializing Spark with Delta Lake support...")
    spark = SparkSession.builder \
        .appName("PopulateDelta") \
        .master("local[*]") \
        .config("spark.jars.packages", DELTA_PACKAGE) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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

    print(f"\n[bold green]✓[/bold green] Delta table populated at {target_dir}")
    print("Run 'aqueduct run blueprint.yml' to test time-travel (version 2).")
    spark.stop()

if __name__ == "__main__":
    main()
