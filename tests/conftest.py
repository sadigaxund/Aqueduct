import os

import pytest
from pyspark.sql import SparkSession


# ── Ollama (local LLM) ────────────────────────────────────────────────────────
# Set AQ_OLLAMA_URL in your local environment (NOT committed to git).
# Example: export AQ_OLLAMA_URL=http://10.0.0.39:11434
# Defaults to http://localhost:11434 when unset.
# Tests skip automatically when the resolved host is not reachable.

def _ollama_url() -> str:
    return os.environ.get("AQ_OLLAMA_URL", "http://localhost:11434")


def _ollama_is_reachable() -> bool:
    url = _ollama_url()
    try:
        import httpx
        resp = httpx.get(url.rstrip("/") + "/api/tags", timeout=3.0)
        return resp.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="session")
def ollama_url() -> str:
    """Session-scoped fixture: returns Ollama base URL or skips the test."""
    url = _ollama_url()
    if not _ollama_is_reachable():
        pytest.skip(f"Ollama at {url} is not reachable — set AQ_OLLAMA_URL or start Ollama locally")
    return url


requires_ollama = pytest.mark.skipif(
    not _ollama_is_reachable(),
    reason="Ollama not reachable (set AQ_OLLAMA_URL and ensure host is up)",
)


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