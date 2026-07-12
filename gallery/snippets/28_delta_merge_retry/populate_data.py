import csv, logging, shutil
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

logging.getLogger("py4j").setLevel(logging.WARN)
logging.getLogger("pyspark").setLevel(logging.WARN)

DATA_DIR = "data/input"


def write_csv(filename, rows):
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(DATA_DIR) / filename
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"Created {path} ({len(rows)-1} data rows)")


def create_csvs():
    write_csv("orders.csv", [
        ["order_id", "customer", "amount"],
        ["101", "Alice", "100.0"],
        ["102", "Bob", "200.0"],
        ["103", "Charlie", "150.0"],
    ])
    write_csv("updates.csv", [
        ["order_id", "customer", "amount"],
        ["101", "Alice", "110.0"],
        ["104", "Diana", "250.0"],
    ])


def create_delta_table():
    parts = pyspark.__version__.split(".")
    major, minor = int(parts[0]), int(parts[1])

    if major >= 4:
        if major == 4 and minor == 1:
            delta_pkg = "io.delta:delta-spark_4.1_2.13:4.2.0"
        elif major == 4 and minor == 0:
            delta_pkg = "io.delta:delta-spark_4.0_2.13:4.2.0"
        else:
            delta_pkg = "io.delta:delta-spark_2.13:4.0.1"
    else:
        delta_pkg = "io.delta:delta-spark_2.12:3.1.0"

    target = Path("data/output/delta_orders")
    if target.exists():
        shutil.rmtree(target)

    spark = SparkSession.builder \
        .appName("PopulateDeltaOrders") \
        .master("local[*]") \
        .config("spark.jars.packages", delta_pkg) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(Path(DATA_DIR) / "orders.csv"))
    df.write.format("delta").mode("overwrite").save(str(target))
    print(f"Created initial Delta table at {target} with {df.count()} rows")
    spark.stop()


if __name__ == "__main__":
    create_csvs()
    create_delta_table()
