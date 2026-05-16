import os
import pytest
from pyspark.sql import SparkSession

# Signals short-lived CLI commands (`aqueduct test` / `doctor`) NOT to call
# SparkSession.stop() — under pytest make_spark_session().getOrCreate() returns
# the shared session-scoped fixture; stopping it kills the SparkContext for
# every later test (ISSUE-026). The `spark` fixture owns the lifecycle here.
os.environ.setdefault("AQ_TESTING", "1")


# ── Agent / LLM testing policy ────────────────────────────────────────────────
# No live-LLM fixtures. LLM responses are non-deterministic and a live model
# (e.g. gemma3:12b ≈ 8-12 GB) is slow, RAM-heavy and flaky — it tests model
# precision, not Aqueduct code. Deterministic agent-loop coverage lives in
# tests/test_surveyor/test_agent.py (mocks aqueduct.agent._call_agent /
# httpx.post). End-to-end model behaviour is validated by .aqscenario.yml
# scenarios, not the unit suite.


# ── Anthropic ─────────────────────────────────────────────────────────────────
# Set ANTHROPIC_API_KEY in your environment to run live Claude tests.

def _anthropic_is_available() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


requires_anthropic = pytest.mark.skipif(
    not _anthropic_is_available(),
    reason="ANTHROPIC_API_KEY not set",
)


# ── Postgres ──────────────────────────────────────────────────────────────────
# Set AQ_PG_DSN in your environment. Example: postgresql://user:pass@localhost:5432/db

def _pg_dsn() -> str | None:
    return os.environ.get("AQ_PG_DSN")


def _pg_is_reachable() -> bool:
    dsn = _pg_dsn()
    if not dsn:
        return False
    try:
        import psycopg2
        conn = psycopg2.connect(dsn, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _pg_is_reachable(),
    reason="Postgres not reachable (set AQ_PG_DSN and ensure DB is up)",
)


# ── Redis ─────────────────────────────────────────────────────────────────────
# Set AQ_REDIS_URL in your environment. Defaults to redis://localhost:6379/15

def _redis_url() -> str:
    return os.environ.get("AQ_REDIS_URL", "redis://localhost:6379/15")


def _redis_is_reachable() -> bool:
    url = _redis_url()
    try:
        import redis
        client = redis.Redis.from_url(url, socket_timeout=3)
        client.ping()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(
    not _redis_is_reachable(),
    reason="Redis not reachable (set AQ_REDIS_URL and ensure server is up)",
)




# ── Spark ─────────────────────────────────────────────────────────────────────
# Set AQ_SPARK_MASTER in your environment to run tests against a remote cluster.
# Example: export AQ_SPARK_MASTER=spark://10.0.0.39:7077
# Defaults to local[1] when unset.

def _spark_master() -> str:
    return os.environ.get("AQ_SPARK_MASTER", "local[1]")


def _spark_is_healthy():
    try:
        spark = SparkSession.builder.master(_spark_master()).getOrCreate()
        spark.range(1).count()
        return True
    except Exception:
        return False


@pytest.fixture(autouse=True)
def restore_cwd():
    """Restore CWD after each test — guards against aqueduct run's os.chdir()."""
    original = os.getcwd()
    yield
    os.chdir(original)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession for fast testing, with health check."""
    if not _spark_is_healthy():
        pytest.skip("Spark Java gateway is unstable in this environment")

    from aqueduct.executor.spark.session import make_spark_session
    
    session = make_spark_session(
        blueprint_id="aqueduct-tests",
        spark_config={
            "spark.sql.warehouse.dir": "/tmp/aqueduct_test_spark_warehouse",
            "javax.jdo.option.ConnectionURL": "jdbc:derby:memory:aqueduct_test_metastore;create=true",
            "derby.stream.error.file": "/tmp/aqueduct_test_derby.log"
        },
        master_url=_spark_master(),
        quiet=True
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