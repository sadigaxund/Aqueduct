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