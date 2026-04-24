"""Connectivity and health checks for aqueduct.yml configuration.

Each check is independent and returns a CheckResult.  Checks are run
sequentially so that dependent checks (e.g. storage depends on spark) can
inspect prior results.

Spark check note: SparkSession startup involves JVM initialisation — expect
5–15 seconds on first run.  Use skip_spark=True to skip in fast CI contexts.
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

SPARK_PROBE_TIMEOUT = 45  # seconds — JVM cold-start can be slow


# ── Result model ──────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name: str
    status: Literal["ok", "fail", "warn", "skip"]
    detail: str
    elapsed_ms: int = 0


# ── Individual checks ─────────────────────────────────────────────────────────

def check_config(config_path: Path | None) -> CheckResult:
    """Load and schema-validate aqueduct.yml."""
    from aqueduct.config import ConfigError, load_config
    t = time.monotonic()
    try:
        cfg = load_config(config_path)
        source = str(config_path) if config_path else "aqueduct.yml (CWD) or defaults"
        return CheckResult(
            "config", "ok",
            f"{source}  engine={cfg.deployment.engine}  target={cfg.deployment.target}",
            _ms(t),
        )
    except ConfigError as exc:
        return CheckResult("config", "fail", str(exc), _ms(t))


def check_depot(depot_path: Path) -> CheckResult:
    """Open (or create) the Depot DuckDB file and run SELECT 1."""
    import duckdb
    t = time.monotonic()
    try:
        depot_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(depot_path))
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return CheckResult("depot", "ok", str(depot_path), _ms(t))
    except Exception as exc:
        return CheckResult("depot", "fail", f"{depot_path}: {exc}", _ms(t))


def check_observability(obs_path: Path) -> CheckResult:
    """Create the observability directory and open runs.db."""
    import duckdb
    t = time.monotonic()
    try:
        obs_path.mkdir(parents=True, exist_ok=True)
        db = obs_path / "runs.db"
        conn = duckdb.connect(str(db))
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return CheckResult("observability", "ok", str(obs_path), _ms(t))
    except Exception as exc:
        return CheckResult("observability", "fail", f"{obs_path}: {exc}", _ms(t))


def check_spark(master_url: str, spark_config: dict[str, Any]) -> tuple[CheckResult, CheckResult]:
    """Create a SparkSession, probe Spark + object storage, stop.  Runs in a thread with timeout.

    Returns (spark_result, storage_result) — storage probe runs inside the same
    session before stop() so SparkSession.getActiveSession() is valid.
    """
    t = time.monotonic()

    def _probe() -> tuple[str, CheckResult]:
        from aqueduct.executor.spark.session import make_spark_session
        spark = make_spark_session("aqueduct.doctor", spark_config, master_url=master_url)
        n = spark.range(1).count()
        v = spark.version
        spark_detail = f"connected  master={master_url}  spark={v}  range(1).count()={n}"
        storage_result = check_storage(spark_config, spark_ok=True)
        spark.stop()
        return spark_detail, storage_result

    storage_skip = CheckResult("storage", "skip", "spark check did not complete")
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_probe)
        try:
            spark_detail, storage_result = future.result(timeout=SPARK_PROBE_TIMEOUT)
            return CheckResult("spark", "ok", spark_detail, _ms(t)), storage_result
        except FuturesTimeout:
            return (
                CheckResult("spark", "fail", f"timed out after {SPARK_PROBE_TIMEOUT}s — master unreachable? ({master_url})", _ms(t)),
                storage_skip,
            )
        except Exception as exc:
            return (
                CheckResult("spark", "fail", f"{master_url}: {exc}", _ms(t)),
                storage_skip,
            )


def check_webhook(url: str, method: str = "POST", headers: dict[str, str] | None = None, timeout: int = 10) -> CheckResult:
    """Send a probe request to the webhook URL.

    Accepts any HTTP response (even 4xx/5xx) as "reachable" — the endpoint
    exists.  Only network-level errors (DNS, connection refused, TLS) count
    as failures.
    """
    import httpx
    t = time.monotonic()
    probe_payload = {"event": "doctor_probe", "source": "aqueduct doctor"}
    rendered_headers = {"Content-Type": "application/json", **(headers or {})}

    try:
        resp = httpx.request(
            method, url,
            json=probe_payload,
            headers=rendered_headers,
            timeout=timeout,
        )
        status = resp.status_code
        if status < 500:
            return CheckResult(
                "webhook", "ok",
                f"{method} {url} → HTTP {status}",
                _ms(t),
            )
        else:
            return CheckResult(
                "webhook", "warn",
                f"{method} {url} → HTTP {status} (server error — endpoint exists but responded with error)",
                _ms(t),
            )
    except httpx.RequestError as exc:
        return CheckResult("webhook", "fail", f"{url}: {exc}", _ms(t))
    except Exception as exc:
        return CheckResult("webhook", "fail", f"{url}: unexpected error: {exc}", _ms(t))


def check_secrets(provider: str) -> CheckResult:
    """Report secrets provider status.  Env provider is always available."""
    t = time.monotonic()
    if provider == "env":
        return CheckResult(
            "secrets", "ok",
            "provider=env  (@aq.secret() reads os.environ)",
            _ms(t),
        )
    # Non-env providers are stubs — warn, don't fail
    return CheckResult(
        "secrets", "warn",
        f"provider={provider} not yet implemented — @aq.secret() will fall back to env",
        _ms(t),
    )


def check_storage(spark_config: dict[str, Any], spark_ok: bool) -> CheckResult:
    """Detect configured object storage from spark_config keys."""
    t = time.monotonic()
    detected: list[str] = []

    if any(k.startswith("spark.hadoop.fs.s3a") for k in spark_config):
        endpoint = spark_config.get("spark.hadoop.fs.s3a.endpoint", "AWS S3")
        detected.append(f"S3/MinIO ({endpoint})")
    if any(k.startswith("spark.hadoop.google.cloud") for k in spark_config):
        detected.append("GCS")
    if any("abfss" in k or "azure" in k.lower() for k in spark_config):
        detected.append("ADLS")

    if not detected:
        return CheckResult("storage", "skip", "no object storage keys in spark_config", _ms(t))

    if not spark_ok:
        return CheckResult(
            "storage", "warn",
            f"configured ({', '.join(detected)}) but Spark check failed — cannot verify connectivity",
            _ms(t),
        )

    # Spark is up — try a read that should return "path not found", not "auth error"
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return CheckResult("storage", "warn", f"configured ({', '.join(detected)}) but no active session", _ms(t))

        errors: list[str] = []
        for label, path_prefix in _storage_probe_paths(spark_config):
            probe_path = path_prefix + "_aq_doctor_probe_nonexistent/"
            try:
                spark.read.parquet(probe_path)
                # If it actually reads something, storage is accessible
                return CheckResult("storage", "ok", f"{label}: readable (unexpected — probe path exists?)", _ms(t))
            except Exception as exc:
                msg = str(exc).lower()
                if any(word in msg for word in ("no such file", "path does not exist", "filenotfoundexception", "nosuchkey")):
                    errors.append(f"{label}: reachable (path not found — expected)")
                elif any(word in msg for word in ("access denied", "403", "unauthorized", "forbidden", "credentials")):
                    errors.append(f"{label}: auth error — check credentials")
                    return CheckResult("storage", "fail", "; ".join(errors), _ms(t))
                else:
                    errors.append(f"{label}: {exc}")

        # All probes returned "path not found" — storage is reachable
        all_ok = all("reachable" in e for e in errors)
        status = "ok" if all_ok else "warn"
        return CheckResult("storage", status, "; ".join(errors), _ms(t))

    except Exception as exc:
        return CheckResult("storage", "warn", f"storage probe failed: {exc}", _ms(t))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ms(t: float) -> int:
    return int((time.monotonic() - t) * 1000)


def _storage_probe_paths(spark_config: dict[str, Any]) -> list[tuple[str, str]]:
    """Return (label, path_prefix) pairs for configured storage backends."""
    paths = []
    if any(k.startswith("spark.hadoop.fs.s3a") for k in spark_config):
        endpoint = spark_config.get("spark.hadoop.fs.s3a.endpoint", "")
        bucket = "aqueduct-doctor-probe"
        paths.append(("S3/MinIO", f"s3a://{bucket}/"))
    if any(k.startswith("spark.hadoop.google.cloud") for k in spark_config):
        paths.append(("GCS", "gs://aqueduct-doctor-probe/"))
    return paths


# ── Runner ────────────────────────────────────────────────────────────────────

def run_doctor(
    config_path: Path | None = None,
    skip_spark: bool = False,
) -> list[CheckResult]:
    """Run all checks and return results in order."""
    from aqueduct.config import load_config, ConfigError

    results: list[CheckResult] = []

    # Config — must succeed for remaining checks
    cfg_result = check_config(config_path)
    results.append(cfg_result)
    if cfg_result.status == "fail":
        results.append(CheckResult("depot", "skip", "config failed"))
        results.append(CheckResult("observability", "skip", "config failed"))
        results.append(CheckResult("spark", "skip", "config failed"))
        results.append(CheckResult("webhook", "skip", "config failed"))
        results.append(CheckResult("secrets", "skip", "config failed"))
        results.append(CheckResult("storage", "skip", "config failed"))
        return results

    try:
        cfg = load_config(config_path)
    except ConfigError:
        return results  # already recorded above

    # Depot
    results.append(check_depot(Path(cfg.stores.depot.path)))

    # Observability
    results.append(check_observability(Path(cfg.stores.observability.path)))

    # Secrets
    results.append(check_secrets(cfg.secrets.provider))

    # Webhook (if configured)
    wh = cfg.webhooks.on_failure
    if wh:
        # Render header values against env only (no failure context vars available here)
        import re
        rendered_headers = {
            k: re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), v)
            for k, v in wh.headers.items()
        }
        results.append(check_webhook(wh.url, wh.method, rendered_headers, wh.timeout))
    else:
        results.append(CheckResult("webhook", "skip", "not configured"))

    # Spark + storage (optional, slow — storage probe runs inside same session)
    if skip_spark:
        results.append(CheckResult("spark", "skip", "--skip-spark flag set"))
        results.append(check_storage(cfg.spark_config, spark_ok=False))
        return results

    spark_result, storage_result = check_spark(cfg.deployment.master_url, cfg.spark_config)
    results.append(spark_result)
    results.append(storage_result)

    return results
