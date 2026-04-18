import pytest
from pyspark.sql import SparkSession


def _spark_is_healthy():
    try:
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        spark.range(1).count()
        spark.stop()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession for fast testing, with health check."""
    if not _spark_is_healthy():
        pytest.skip("Spark Java gateway is unstable in this environment")

    session = (
        SparkSession.builder.master("local[1]")
        .appName("aqueduct-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# Mark all tests that require a working SparkSession
requires_healthy_spark = pytest.mark.skipif(
    not _spark_is_healthy(),
    reason="Spark Java gateway is unstable in this environment"
)


@pytest.fixture(scope="session")
def sample_data(spark: SparkSession, tmp_path_factory):
    """Generate small parquet test data files, reused across all blueprint tests.

    orders.parquet: 10 rows — order_id, region (US/EU), amount (null at id=3), status
    customers.parquet: 5 rows — customer_id, name, email
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    data_dir = tmp_path_factory.mktemp("bp_data")

    # Use Spark column expressions — avoids cloudpickle serialization issues with Row
    orders = (
        spark.range(10)
        .withColumn("order_id", F.concat(F.lit("O-"), F.lpad(F.col("id").cast("string"), 4, "0")))
        .withColumn("region", F.when(F.col("id") < 5, "US").otherwise("EU"))
        .withColumn(
            "amount",
            F.when(F.col("id") == 3, F.lit(None).cast(DoubleType()))
             .otherwise((F.col("id") * 10).cast(DoubleType()))
        )
        .withColumn("status", F.lit("completed"))
        .drop("id")
    )
    orders.write.parquet(str(data_dir / "orders.parquet"))

    customers = (
        spark.range(5)
        .withColumn("customer_id", F.concat(F.lit("C-"), F.lpad(F.col("id").cast("string"), 4, "0")))
        .withColumn("name", F.concat(F.lit("Customer "), F.col("id").cast("string")))
        .withColumn("email", F.concat(F.col("id").cast("string"), F.lit("@test.com")))
        .drop("id")
    )
    customers.write.parquet(str(data_dir / "customers.parquet"))

    return data_dir