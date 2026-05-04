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


def check_observability(obs_db: Path) -> CheckResult:
    """Create obs.db (and parent dirs) and verify connectivity."""
    import duckdb
    t = time.monotonic()
    try:
        obs_db.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(obs_db))
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return CheckResult("observability", "ok", str(obs_db), _ms(t))
    except Exception as exc:
        return CheckResult("observability", "fail", f"{obs_db}: {exc}", _ms(t))


def check_spark(master_url: str, spark_config: dict[str, Any]) -> tuple[CheckResult, CheckResult]:
    """Create a SparkSession, probe Spark + object storage, stop.  Runs in a thread with timeout.

    Returns (spark_result, storage_result) — storage probe runs inside the same
    session before stop() so SparkSession.getActiveSession() is valid.
    """
    t = time.monotonic()

    def _probe() -> tuple[str, bool, CheckResult]:
        import pyspark
        from aqueduct.executor.spark.session import make_spark_session
        spark = make_spark_session("aqueduct.doctor", spark_config, master_url=master_url, quiet=True)
        n = spark.range(1).count()
        cluster_ver = spark.version
        client_ver = pyspark.__version__
        ver_mismatch = cluster_ver.split(".")[0] != client_ver.split(".")[0]
        ver_note = f"  ⚠ major version mismatch: pyspark={client_ver} cluster={cluster_ver}" if ver_mismatch else ""
        spark_detail = f"connected  master={master_url}  spark={cluster_ver}  pyspark={client_ver}{ver_note}"
        storage_result = check_storage(spark_config, spark_ok=True)
        spark.stop()
        return spark_detail, ver_mismatch, storage_result

    storage_skip = CheckResult("storage", "skip", "spark check did not complete")
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_probe)
        try:
            spark_detail, ver_mismatch, storage_result = future.result(timeout=SPARK_PROBE_TIMEOUT)
            spark_status = "warn" if ver_mismatch else "ok"
            return CheckResult("spark", spark_status, spark_detail, _ms(t)), storage_result
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


def check_llm(
    llm_provider: str,
    base_url: str | None,
    model: str,
) -> CheckResult:
    """Probe LLM connectivity.

    anthropic:     verifies ANTHROPIC_API_KEY is set; does not make an API call
                   (avoid token cost in a health check).
    openai_compat: GET {base_url}/models — lists loaded models, free, no tokens.
                   Skipped if base_url is not configured.
    """
    import httpx
    t = time.monotonic()

    if llm_provider == "anthropic":
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            return CheckResult(
                "llm", "warn",
                "provider=anthropic  ANTHROPIC_API_KEY not set — LLM self-healing will fail at runtime",
                _ms(t),
            )
        return CheckResult(
            "llm", "ok",
            f"provider=anthropic  model={model}  ANTHROPIC_API_KEY present (API not called)",
            _ms(t),
        )

    # openai_compat (Ollama, vLLM, LM Studio, …)
    if not base_url:
        return CheckResult(
            "llm", "skip",
            "provider=openai_compat  base_url not configured",
            _ms(t),
        )

    models_url = base_url.rstrip("/").rstrip("/v1").rstrip("/") + "/v1/models"
    try:
        resp = httpx.get(models_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        available = [m.get("id", m.get("name", "?")) for m in data.get("data", data.get("models", []))]
        model_note = f"  model={model}"
        if available and model not in available:
            model_note += f"  ⚠ not in loaded models: {', '.join(available[:5])}"
        return CheckResult(
            "llm", "ok",
            f"provider=openai_compat  endpoint={models_url}{model_note}",
            _ms(t),
        )
    except httpx.RequestError as exc:
        return CheckResult("llm", "fail", f"{models_url}: {exc}", _ms(t))
    except Exception as exc:
        return CheckResult("llm", "fail", f"{models_url}: unexpected error: {exc}", _ms(t))


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


# ── Blueprint source checks ───────────────────────────────────────────────────

def check_blueprint_sources(
    blueprint_path: Path,
    _context_override: dict[str, Any] | None = None,
) -> list[CheckResult]:
    """Parse a Blueprint and probe every Ingress/Egress path or JDBC endpoint.

    Local paths: checked for existence (Ingress) or parent-dir writability (Egress).
    Relative paths resolve from the project root (directory containing aqueduct.yml),
    found by walking up from the blueprint — same logic as `aqueduct run`.
    Cloud URIs (s3a://, gs://, abfss://): skipped here — covered by storage check.
    JDBC URLs: TCP socket probe to host:port (3s timeout — checks reachability,
               not credentials or schema).
    _context_override: caller-provided context injected when checking Arcade sub-blueprints.
    """
    import re
    import socket

    # Find project root (same walk-up logic as `run` command)
    project_root = blueprint_path.parent
    _search = blueprint_path.parent
    for _ in range(8):
        if (_search / "aqueduct.yml").exists():
            project_root = _search
            break
        if _search.parent == _search:
            break
        _search = _search.parent

    results: list[CheckResult] = []

    try:
        from aqueduct.parser.parser import parse
        bp = parse(str(blueprint_path), cli_overrides=_context_override or {})
    except Exception as exc:
        return [CheckResult("blueprint", "fail", f"could not parse {blueprint_path}: {exc}")]

    for module in bp.modules:
        cfg = module.config if isinstance(module.config, dict) else {}
        fmt: str = cfg.get("format", "")
        path_val: str | None = cfg.get("path")
        url_val: str | None = cfg.get("url")  # JDBC url key

        if module.type not in ("Ingress", "Egress"):
            continue

        t = time.monotonic()
        name = f"{module.type.lower()}:{module.id}"

        # ── JDBC ──────────────────────────────────────────────────────────────
        jdbc_url = url_val or (path_val if path_val and path_val.startswith("jdbc:") else None)
        if jdbc_url or fmt == "jdbc":
            raw = jdbc_url or path_val or ""
            # jdbc:postgresql://host:5432/db  or  jdbc:mysql://host/db
            m = re.search(r"jdbc:[^:]+://([^/:]+)(?::(\d+))?", raw)
            if not m:
                results.append(CheckResult(name, "warn", f"JDBC URL not parseable: {raw!r}", _ms(t)))
                continue
            host = m.group(1)
            port = int(m.group(2)) if m.group(2) else _jdbc_default_port(raw)
            try:
                with socket.create_connection((host, port), timeout=3):
                    pass
                results.append(CheckResult(name, "ok", f"JDBC {host}:{port} reachable", _ms(t)))
            except OSError as exc:
                results.append(CheckResult(name, "fail", f"JDBC {host}:{port} unreachable: {exc}", _ms(t)))
            continue

        # ── Cloud URIs — skip (covered by storage check) ───────────────────
        if path_val and re.match(r"(s3a?|gs|abfss?)://", path_val):
            results.append(CheckResult(name, "skip", f"cloud URI — covered by storage check: {path_val}", _ms(t)))
            continue

        # ── Local / relative path ──────────────────────────────────────────
        if path_val:
            # Resolve relative to project root (same as runtime chdir behaviour)
            p = (project_root / path_val).resolve() if not Path(path_val).is_absolute() else Path(path_val)
            is_glob = "*" in str(p) or "?" in str(p)

            if module.type == "Ingress":
                if is_glob:
                    import glob as _glob
                    matches = _glob.glob(str(p))
                    if matches:
                        results.append(CheckResult(name, "ok", f"readable: {path_val} ({len(matches)} file(s))", _ms(t)))
                        # Check format/extension mismatch (e.g. format=csv but path=*.parquet)
                        if fmt:
                            _check_format_ext_mismatch(results, name, fmt, matches, path_val, _ms(t))
                    elif p.parent.exists():
                        results.append(CheckResult(name, "warn", f"dir exists but no files match pattern: {path_val}", _ms(t)))
                    else:
                        results.append(CheckResult(name, "fail", f"not found: {p.parent}", _ms(t)))
                else:
                    if p.exists():
                        results.append(CheckResult(name, "ok", f"readable: {path_val}", _ms(t)))
                        if fmt:
                            _check_format_ext_mismatch(results, name, fmt, [str(p)], path_val, _ms(t))
                    else:
                        results.append(CheckResult(name, "fail", f"not found: {p}", _ms(t)))
            else:  # Egress
                parent = p.parent if not is_glob else p.parent.parent
                if parent.exists():
                    results.append(CheckResult(name, "ok", f"parent dir exists: {path_val}", _ms(t)))
                else:
                    results.append(CheckResult(name, "warn", f"output dir does not exist yet: {parent}", _ms(t)))
            continue

        results.append(CheckResult(name, "skip", "no path or url in config", _ms(t)))

    # ── Recurse into Arcade sub-blueprints ────────────────────────────────────
    for module in bp.modules:
        if module.type != "Arcade" or not module.ref:
            continue
        sub_path = (blueprint_path.parent / module.ref).resolve()
        if not sub_path.exists():
            results.append(CheckResult(
                f"arcade:{module.id}", "warn",
                f"sub-blueprint not found: {sub_path}",
            ))
            continue
        sub_results = check_blueprint_sources(
            sub_path,
            _context_override=module.context_override or {},
        )
        # Prefix each result name so the user knows which arcade it came from
        for r in sub_results:
            results.append(CheckResult(
                f"arcade:{module.id}/{r.name}", r.status, r.detail, r.elapsed_ms,
            ))

    return results


_FORMAT_EXTENSIONS: dict[str, set[str]] = {
    "parquet": {".parquet"},
    "orc":     {".orc"},
    "avro":    {".avro"},
    "csv":     {".csv", ".tsv", ".txt"},
    "json":    {".json", ".jsonl", ".ndjson"},
    "text":    {".txt"},
    "delta":   set(),  # Delta dirs have no single extension — skip check
}


def _check_format_ext_mismatch(
    results: list[CheckResult],
    name: str,
    fmt: str,
    file_paths: list[str],
    path_val: str,
    elapsed_ms: int,
) -> None:
    """Append a warn CheckResult if file extension doesn't match declared format."""
    expected_exts = _FORMAT_EXTENSIONS.get(fmt.lower())
    if expected_exts is None or not expected_exts:
        return  # unknown or extension-free format (delta) — skip
    mismatched = [
        f for f in file_paths
        if Path(f).suffix.lower() not in expected_exts
    ]
    if mismatched:
        sample = Path(mismatched[0]).name
        results.append(CheckResult(
            name, "warn",
            f"format={fmt!r} but file extension suggests different format "
            f"(e.g. {sample!r}). Spark may silently misread the data.",
            elapsed_ms,
        ))


def _jdbc_default_port(jdbc_url: str) -> int:
    defaults = {
        "postgresql": 5432, "mysql": 3306, "sqlserver": 1433,
        "oracle": 1521, "db2": 50000, "redshift": 5439,
        "bigquery": 443, "snowflake": 443,
    }
    for key, port in defaults.items():
        if key in jdbc_url.lower():
            return port
    return 5432  # safe fallback


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
    blueprint_path: Path | None = None,
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
        results.append(CheckResult("secrets", "skip", "config failed"))
        results.append(CheckResult("llm", "skip", "config failed"))
        results.append(CheckResult("webhook", "skip", "config failed"))
        results.append(CheckResult("spark", "skip", "config failed"))
        results.append(CheckResult("storage", "skip", "config failed"))
        return results

    try:
        cfg = load_config(config_path)
    except ConfigError:
        return results  # already recorded above

    # Depot
    results.append(check_depot(Path(cfg.stores.depot.path)))

    # Observability
    results.append(check_observability(Path(cfg.stores.obs.path)))

    # Secrets
    results.append(check_secrets(cfg.secrets.provider))

    # LLM connectivity
    results.append(check_llm(cfg.agent.provider, cfg.agent.base_url, cfg.agent.model))

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
        if blueprint_path is not None:
            results.extend(check_blueprint_sources(blueprint_path))
        return results

    spark_result, storage_result = check_spark(cfg.deployment.master_url, cfg.spark_config)
    results.append(spark_result)
    results.append(storage_result)

    if blueprint_path is not None:
        results.extend(check_blueprint_sources(blueprint_path))

    return results
