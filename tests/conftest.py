import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None  # type: ignore[assignment,misc]

# `spark.sql.warehouse.dir` is a STATIC conf — it binds at the FIRST SparkSession
# created in the process and is a no-op on every getOrCreate after. The health
# probe below builds that first session, so the warehouse must be pinned HERE (and
# reused by the `spark` fixture); otherwise managed tables (saveAsTable) land in
# the repo's cwd `spark-warehouse/` and collide run-over-run.
_SPARK_WAREHOUSE = tempfile.mkdtemp(prefix="aq_test_warehouse_")

# Signals short-lived CLI commands (`aqueduct test` / `doctor`) NOT to call
# SparkSession.stop() — under pytest make_spark_session().getOrCreate() returns
# the shared session-scoped fixture; stopping it kills the SparkContext for
# every later test (ISSUE-026). The `spark` fixture owns the lifecycle here.
os.environ.setdefault("AQ_TESTING", "1")


# ── Lakehouse (Hudi / Iceberg) datasource provisioning ───────────────────────
# Hudi & Iceberg ship as separate Maven artifacts, NOT bundled with pyspark
# (neither is Delta). Their DataSource is resolved by a ServiceLoader on the
# driver classloader at *planning* time, so the jars must be on the classpath
# before the FIRST SparkSession (the health probe below) is created. We set
# PYSPARK_SUBMIT_ARGS here — at import, before any getOrCreate — so Ivy pulls
# the jars when the JVM gateway launches.
#
# Coordinates are DERIVED from the running pyspark version, never hardcoded, so
# the same code works across the compat matrix: pyspark 3.5 → *-spark3.5_2.12,
# pyspark 4.1 → *-spark4.1_2.13. Each coord is HEAD-checked against Maven
# Central first; a build that does not exist for the running Spark line (e.g.
# Hudi has no published build for Spark 3.3, per _HUDI_VER below) is dropped,
# so its test SKIPs cleanly
# instead of the whole shared session dying on an unresolvable --packages
# coordinate. Overrides: AQ_HUDI_COORD / AQ_ICEBERG_COORD (exact Maven coord),
# AQ_SPARK_SCALA (scala suffix).
#
# Opt-in: only runs when AQ_LAKEHOUSE is truthy (CI sets it on the executor /
# compat jobs). Keeps ordinary local spark runs free of the Ivy download.

def _truthy(v: "str | None") -> bool:
    return bool(v) and v.strip().lower() not in ("0", "false", "no", "off")


def _spark_line() -> "str | None":
    try:
        import pyspark
        p = pyspark.__version__.split(".")
        return f"{p[0]}.{p[1]}"
    except Exception:
        return None


def _scala_suffix() -> str:
    ovr = os.environ.get("AQ_SPARK_SCALA")
    if ovr:
        return ovr
    line = _spark_line() or "4.0"
    return "2.13" if int(line.split(".")[0]) >= 4 else "2.12"


# Default library version per Spark minor line (HEAD-checked before use). A
# missing entry / failed check just drops that datasource → clean skip.
_HUDI_VER = {"3.4": "0.15.0", "3.5": "1.0.2", "4.0": "1.2.0", "4.1": "1.2.0"}
_ICEBERG_VER = {"3.3": "1.11.0", "3.4": "1.11.0", "3.5": "1.11.0", "4.0": "1.11.0", "4.1": "1.11.0"}


def _coord_resolvable(coord: str) -> bool:
    """True if `group:artifact:version` has a .pom on Maven Central."""
    try:
        g, a, v = coord.split(":")
    except ValueError:
        return False
    url = f"https://repo1.maven.org/maven2/{g.replace('.', '/')}/{a}/{v}/{a}-{v}.pom"
    import urllib.request
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, method="HEAD"), timeout=6
        ) as r:
            return 200 <= r.status < 300
    except Exception:
        return False


def _lakehouse_coords() -> list[str]:
    """Resolvable Hudi/Iceberg Maven coords for the running Spark line."""
    line, scala = _spark_line(), _scala_suffix()
    coords: list[str] = []
    hudi = os.environ.get("AQ_HUDI_COORD")
    if not hudi and line and line in _HUDI_VER:
        hudi = f"org.apache.hudi:hudi-spark{line}-bundle_{scala}:{_HUDI_VER[line]}"
    if hudi and _coord_resolvable(hudi):
        coords.append(hudi)
    ice = os.environ.get("AQ_ICEBERG_COORD")
    if not ice and line and line in _ICEBERG_VER:
        ice = f"org.apache.iceberg:iceberg-spark-runtime-{line}_{scala}:{_ICEBERG_VER[line]}"
    if ice and _coord_resolvable(ice):
        coords.append(ice)
    return coords


_LAKEHOUSE_ON = _truthy(os.environ.get("AQ_LAKEHOUSE")) and SparkSession is not None
_LAKEHOUSE_COORDS = _lakehouse_coords() if _LAKEHOUSE_ON else []
_ICEBERG_ON = any("iceberg" in c for c in _LAKEHOUSE_COORDS)
# Iceberg's `local` catalog needs a warehouse dir; only allocate when Iceberg
# actually resolved (else the extension class below would be a fatal
# ClassNotFound at session init).
_LAKEHOUSE_WAREHOUSE = tempfile.mkdtemp(prefix="aq_iceberg_wh_") if _ICEBERG_ON else None

if _LAKEHOUSE_COORDS and "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages {','.join(_LAKEHOUSE_COORDS)} pyspark-shell"
    )


def _lakehouse_session_conf() -> dict:
    """STATIC confs the FIRST session needs for the provisioned datasources.

    Empty unless a lakehouse jar actually resolved. These bind only at session
    creation (no-op on later getOrCreate), and the referenced classes exist
    only when the jar is on the classpath — so gating them on real resolution
    is what keeps an un-provisioned run from aborting at startup.

      * Hudi mandates the Kryo serializer (writer hard-fails otherwise).
      * Iceberg needs its SQL extensions + a `local` hadoop catalog so the
        `local.db.tbl` identifier and maintenance procedures resolve.
    """
    conf: dict = {}
    if not _LAKEHOUSE_COORDS:
        return conf
    # Required by Hudi; harmless for Iceberg — safe whenever anything resolved.
    conf["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
    if _ICEBERG_ON and _LAKEHOUSE_WAREHOUSE:
        conf.update({
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": _LAKEHOUSE_WAREHOUSE,
        })
    return conf


# ── Test backlog (pytest-native, replaces TEST_MANIFEST.md) ───────────────────
# `@pytest.mark.todo("why")` marks a planned-but-unwritten test. It is
# auto-skipped here (never a failure), and its reason is surfaced by
# `pytest -rs`. Writing a skipped stub — with whatever asserts you can already
# express — is the unambiguous replacement for a TEST_MANIFEST ⏳ line: the
# spec lives next to the code, and `pytest --collect-only -m todo` is the
# living backlog. Known bugs use `@pytest.mark.xfail(strict=True, reason=...)`
# instead, which fails the moment the bug is fixed (see pyproject xfail_strict).

_LAYER_OR_CAP = {"unit", "integration", "e2e", "spark", "duckdb", "agent", "airflow", "todo"}


def pytest_collection_modifyitems(config, items):
    for item in items:
        # 1. todo → auto-skip with its reason as the spec.
        todo = item.get_closest_marker("todo")
        if todo is not None:
            reason = (todo.args[0] if todo.args else None) or "todo: unwritten test"
            item.add_marker(pytest.mark.skip(reason=f"todo: {reason}"))
        # 2. Default-layer marker: every test gets exactly one layer without
        # editing 1788 files. A test that declares no layer/capability marker
        # and doesn't pull the real Spark session is, by definition, a `unit`.
        # Tests using the `spark` fixture are treated as `spark` (integration);
        # tests using the `duckdb_con` fixture (the DuckDB engine's connection
        # fixture, tests/test_executor_duckdb/conftest.py) are treated as
        # `duckdb` the same way.
        existing = {m.name for m in item.iter_markers()}
        if not (existing & _LAYER_OR_CAP):
            fixturenames = getattr(item, "fixturenames", ())  # type: ignore[arg-type]
            if "spark" in fixturenames:
                item.add_marker(pytest.mark.spark)
            elif "duckdb_con" in fixturenames:
                item.add_marker(pytest.mark.duckdb)
            else:
                item.add_marker(pytest.mark.unit)


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
# Example: export AQ_SPARK_MASTER=spark://spark-master.internal:7077
# Defaults to local[1] when unset.

def _spark_master() -> str:
    return os.environ.get("AQ_SPARK_MASTER", "local[1]")


def _spark_is_healthy():
    if SparkSession is None:
        return False
    try:
        builder = (
            SparkSession.builder.master(_spark_master())
            .config("spark.sql.warehouse.dir", _SPARK_WAREHOUSE)
        )
        # Lakehouse confs (Kryo for Hudi, Iceberg catalog/extensions) are
        # STATIC — bind them on this first session (no-op on later getOrCreate).
        for k, v in _lakehouse_session_conf().items():
            builder = builder.config(k, v)
        spark = builder.getOrCreate()
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


@pytest.fixture
def seed_ts():
    """Seed timestamps for store fixtures. Relative by construction — a literal
    date silently ages out of any now()-windowed query (report --trend's 30-day
    default, count_recent_heal_attempts)."""
    def _ts(**delta):          # seed_ts(hours=-1), seed_ts(days=-2)
        return (datetime.now(timezone.utc) + timedelta(**delta)).isoformat()
    return _ts


@pytest.fixture(scope="session", autouse=True)
def _guard_shared_spark_context():
    """Structural fix for the ISSUE-026 class: no-op BOTH ``SparkContext.stop()``
    and ``SparkSession.stop()`` for the whole pytest session.

    pyspark's ``SparkContext`` is a process-wide singleton (``getOrCreate``),
    and this file shares exactly ONE session-scoped instance (the ``spark``
    fixture below) across the entire suite. ``stop_spark_session``'s
    ``AQ_TESTING`` guard (``aqueduct/executor/spark/session.py``) protects
    exactly one call path — CLI commands going through that function. It does
    NOT protect ``ExecutorProtocol.close_session``
    (``aqueduct/executor/spark/engine.py``), which performs a RAW
    ``session.stop()`` by design (it owns teardown for a real ``aqueduct run``
    where the process exits right after). Phase 79's inline sandbox gate
    (``_run_patch_gates_inline`` -> ``run_sandbox_gate``) builds a session via
    ``session_factory()`` believing it owns a throwaway and closes it with the
    raw ``session_closer()``. Under pytest that "private" session IS the
    shared fixture, so it dies for every test that runs afterward —
    ``'NoneType' object has no attribute 'sc'`` — order-dependently, so it's
    invisible until some CI lane ordering happens to hit it.

    Patching individual call sites keeps missing the next one (two mocks were
    already added for heal-cache tests; the underlying class of bug wasn't
    fixed). Product code (``session.py`` / ``engine.py``) is intentionally
    untouched — this is a test-harness-only guard, restored at session
    teardown so it can never leak into a non-test import of pyspark.

    **Both primitives are patched, not just the JVM-level one.**
    ``SparkContext.stop()`` tears down the JVM ``SparkContext`` — patching
    only that keeps the live JVM context alive, which is enough to stop a
    *fresh* ``getOrCreate()`` from erroring. But ``SparkSession.stop()`` does
    more than call ``self._sc.stop()``: it also calls
    ``jSparkSessionClass.clearDefaultSession()`` /
    ``.clearActiveSession()`` and sets ``SparkSession._instantiatedSession``,
    ``SparkSession._activeSession``, and ``SQLContext._instantiatedContext``
    to ``None``. With only ``SparkContext.stop`` patched, engine.py's raw
    ``session.stop()`` still runs that global-state teardown — the shared
    fixture object keeps working (it holds its own `_jsc` reference, never
    cleared), but any OTHER code path that resolves the session via
    ``SparkSession.getActiveSession()`` (e.g.
    ``aqueduct/executor/spark/metrics.py``) would see ``None`` afterward.
    Neutering both closes that gap; no test in this suite asserts on
    ``getActiveSession()`` / ``_instantiatedSession`` / ``_activeSession``
    post-stop (checked), and the existing ``stop_spark_session`` guard tests
    (``tests/test_executor/test_executor_session.py``) assert against a
    ``MagicMock``, not the real class, so they are unaffected by this patch.

    Autouse + session-scoped means this activates during the FIRST test's
    fixture setup (before any test body runs) and is torn down last (LIFO), so
    it is already active before the ``spark`` fixture — or any inline sandbox
    gate — ever gets a chance to call ``.stop()``, regardless of which test
    happens to trigger it first.
    """
    try:
        from pyspark import SparkContext
        from pyspark.sql import SparkSession
    except ImportError:
        yield  # pyspark not installed (base install / `-m "not spark"` lanes) — nothing to guard
        return

    original_sc_stop = SparkContext.stop
    original_session_stop = SparkSession.stop
    SparkContext.stop = lambda self, *a, **kw: None  # type: ignore[method-assign]
    SparkSession.stop = lambda self, *a, **kw: None  # type: ignore[method-assign]
    try:
        yield
    finally:
        SparkContext.stop = original_sc_stop  # type: ignore[method-assign]
        SparkSession.stop = original_session_stop  # type: ignore[method-assign]


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    """Session-scoped SparkSession for fast testing, with health check.

    Warehouse, derby error log, and shuffle scratch all live under a
    pytest-managed temp dir (``tmp_path_factory``) so they: (a) get cleaned by
    the ``tmp_path_retention_*`` policy in pyproject.toml instead of piling up
    in ``/tmp`` run-over-run, and (b) follow ``TMPDIR`` / ``--basetemp`` when
    the suite is pointed off tmpfs. Previously these were hardcoded
    ``/tmp/aqueduct_test_*`` paths that accumulated and exhausted the tmpfs
    quota on the full suite.
    """
    if not _spark_is_healthy():
        pytest.skip("Spark Java gateway is unstable in this environment")

    from aqueduct.executor.spark.session import make_spark_session

    scratch = tmp_path_factory.mktemp("spark_scratch")

    session = make_spark_session(
        blueprint_id="aqueduct-tests",
        spark_config={
            "spark.sql.warehouse.dir": _SPARK_WAREHOUSE,  # matches the health-probe session (static conf)
            "spark.local.dir": str(scratch / "local"),
            "javax.jdo.option.ConnectionURL": "jdbc:derby:memory:aqueduct_test_metastore;create=true",
            "derby.stream.error.file": str(scratch / "derby.log"),
            **_lakehouse_session_conf(),  # Kryo + Iceberg catalog (empty unless provisioned)
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