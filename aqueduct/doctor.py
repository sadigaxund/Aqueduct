"""Connectivity and health checks for aqueduct.yml configuration.

Each check is independent and returns a CheckResult.  Checks are run
sequentially so that dependent checks (e.g. storage depends on spark) can
inspect prior results.

Spark check note: SparkSession startup involves JVM initialisation — expect
5–15 seconds on first run.  Use skip_spark=True to skip in fast CI contexts.

Layer-boundary note: this module is the documented exception to the
"pyspark is imported only inside aqueduct/executor/spark/" rule. All three
pyspark imports here (`check_spark`, `check_storage`, `check_cloudpickle`)
are deferred inside function bodies — top-level `import doctor` does NOT
pull in pyspark, so the doctor module is safe to import in `--skip-spark`
contexts and in test environments without the `[spark]` extra installed.
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


def check_observability(observability_db: Path) -> CheckResult:
    """Create observability.db (and parent dirs) and verify connectivity."""
    import duckdb
    t = time.monotonic()
    try:
        observability_db.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(observability_db))
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return CheckResult("observability", "ok", str(observability_db), _ms(t))
    except Exception as exc:
        return CheckResult("observability", "fail", f"{observability_db}: {exc}", _ms(t))


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


def check_agent(
    agent_provider: str,
    base_url: str | None,
    model: str,
) -> CheckResult:
    """Probe agent (LLM) connectivity.

    anthropic:     verifies ANTHROPIC_API_KEY is set; does not make an API call
                   (avoid token cost in a health check).
    openai_compat: GET {base_url}/models — lists loaded models, free, no tokens.
                   Skipped if base_url is not configured.
    """
    import httpx
    t = time.monotonic()

    if agent_provider == "anthropic":
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            return CheckResult(
                "agent", "warn",
                "provider=anthropic  ANTHROPIC_API_KEY not set — agent self-healing will fail at runtime",
                _ms(t),
            )
        return CheckResult(
            "agent", "ok",
            f"provider=anthropic  model={model}  ANTHROPIC_API_KEY present (API not called)",
            _ms(t),
        )

    # openai_compat (Ollama, vLLM, LM Studio, …)
    if not base_url:
        return CheckResult(
            "agent", "skip",
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
            "agent", "ok",
            f"provider=openai_compat  endpoint={models_url}{model_note}",
            _ms(t),
        )
    except httpx.RequestError as exc:
        return CheckResult("agent", "fail", f"{models_url}: {exc}", _ms(t))
    except Exception as exc:
        return CheckResult("agent", "fail", f"{models_url}: unexpected error: {exc}", _ms(t))


def check_secrets(provider: str, resolver: str | None = None) -> CheckResult:
    """Report secrets provider status and verify required deps are installed."""
    t = time.monotonic()
    if provider == "env":
        return CheckResult("secrets", "ok", "provider=env  (@aq.secret() reads os.environ)", _ms(t))

    _PROVIDER_DEPS = {
        "aws":   ("boto3", "pip install aqueduct-core[aws]"),
        "gcp":   ("google.cloud.secretmanager", "pip install aqueduct-core[gcp]"),
        "azure": ("azure.keyvault.secrets", "pip install aqueduct-core[azure]"),
    }

    if provider in _PROVIDER_DEPS:
        import importlib as _il
        pkg, install_hint = _PROVIDER_DEPS[provider]
        try:
            _il.import_module(pkg)
            return CheckResult("secrets", "ok", f"provider={provider}  ({pkg} installed)", _ms(t))
        except ImportError:
            return CheckResult(
                "secrets", "fail",
                f"provider={provider} requires {pkg} which is not installed. {install_hint}",
                _ms(t),
            )

    if provider == "custom":
        if not resolver:
            return CheckResult(
                "secrets", "fail",
                "provider=custom requires secrets.resolver to be set in aqueduct.yml",
                _ms(t),
            )
        import importlib as _il
        try:
            module_path, fn_name = resolver.rsplit(".", 1)
            mod = _il.import_module(module_path)
            getattr(mod, fn_name)
            return CheckResult("secrets", "ok", f"provider=custom  resolver={resolver!r} loaded", _ms(t))
        except Exception as exc:
            return CheckResult("secrets", "fail", f"provider=custom resolver {resolver!r} failed: {exc}", _ms(t))

    return CheckResult("secrets", "warn", f"provider={provider!r} unknown — will fall back to env", _ms(t))


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

def check_blueprint_sources_from_manifest(manifest: Any, deployment_env: str = "local") -> list[CheckResult]:
    """Check all Ingress/Egress paths using an already-compiled Manifest.

    Advantages over check_blueprint_sources():
    - Arcade-expanded modules are already flat — no recursion needed
    - All ${ctx.*} refs are resolved — path values are concrete strings
    - No re-parsing; no workarounds for sub-blueprint context injection

    Falls back to check_blueprint_sources() if provenance_map is unavailable.
    """
    import re
    import socket

    # Derive project root from provenance_map blueprint_path if available
    pmap = getattr(manifest, "provenance_map", None)
    if pmap and pmap.blueprint_path:
        bp_path = Path(pmap.blueprint_path)
        project_root = bp_path.parent
        _search = bp_path.parent
        for _ in range(8):
            if (_search / "aqueduct.yml").exists():
                project_root = _search
                break
            if _search.parent == _search:
                break
            _search = _search.parent
    else:
        project_root = Path.cwd()

    results: list[CheckResult] = []

    for module in manifest.modules:
        cfg = module.config if isinstance(module.config, dict) else {}
        fmt: str = cfg.get("format", "")
        path_val: str | None = cfg.get("path")
        url_val: str | None = cfg.get("url")

        if module.type not in ("Ingress", "Egress"):
            continue

        t = time.monotonic()
        name = f"{module.type.lower()}:{module.id}"

        # ── JDBC ──────────────────────────────────────────────────────────────
        jdbc_url = url_val or (path_val if path_val and path_val.startswith("jdbc:") else None)
        if jdbc_url or fmt == "jdbc":
            raw = jdbc_url or path_val or ""
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

        # ── Cloud URIs ─────────────────────────────────────────────────────────
        if path_val and re.match(r"(s3a?|gs|abfss?)://", path_val):
            results.append(CheckResult(name, "skip", f"cloud URI — covered by storage check: {path_val}", _ms(t)))
            continue

        # ── Local / relative path (already fully resolved — no ${ctx.*} refs) ─
        if path_val:
            p = (project_root / path_val).resolve() if not Path(path_val).is_absolute() else Path(path_val)
            is_glob = "*" in str(p) or "?" in str(p)

            if module.type == "Ingress":
                if is_glob:
                    import glob as _glob
                    matches = _glob.glob(str(p))
                    if matches:
                        results.append(CheckResult(name, "ok", f"readable: {path_val} ({len(matches)} file(s))", _ms(t)))
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

    if deployment_env in ("cluster", "cloud"):
        for module in manifest.modules:
            if module.type not in ("Ingress", "Egress"):
                continue
            path_val = (module.config or {}).get("path", "")
            if path_val and "://" not in str(path_val) and not str(path_val).startswith("/"):
                results.append(CheckResult(
                    name=f"path_no_uri_scheme:{module.id}",
                    status="warn",
                    detail=(
                        f"Module {module.id!r} path {path_val!r} has no URI scheme "
                        f"(expected s3a://, hdfs://, gs://, abfs://) in {deployment_env} mode."
                    ),
                ))

    results.extend(_check_heal_guardrail_typos(manifest))
    return results


def _check_heal_guardrail_typos(manifest: Any) -> "list[CheckResult]":
    """Warn if heal_on_errors / never_heal_errors entries don't match any known error_type
    in the blueprint's Assert rules and aren't a recognised exception class name pattern.

    Mismatches are almost always typos — the label will never match and the guardrail
    silently has no effect.
    """
    results: list[CheckResult] = []

    agent = getattr(manifest, "agent", None)
    if agent is None:
        return results
    guardrails = getattr(agent, "guardrails", None)
    if guardrails is None:
        return results

    heal_on = tuple(getattr(guardrails, "heal_on_errors", ()))
    never_heal = tuple(getattr(guardrails, "never_heal_errors", ()))
    if not heal_on and not never_heal:
        return results

    # Collect all error_type labels declared in Assert rules
    known_error_types: set[str] = set()
    for module in getattr(manifest, "modules", []):
        if getattr(module, "type", "") != "Assert":
            continue
        for rule in (module.config or {}).get("rules", []):
            et = rule.get("error_type")
            if et:
                known_error_types.add(et)

    all_guardrail_entries = set(heal_on) | set(never_heal)
    for entry in sorted(all_guardrail_entries):
        if entry not in known_error_types:
            results.append(CheckResult(
                name="guardrail_typo",
                status="warn",
                detail=(
                    f"agent.guardrails entry {entry!r} does not match any Assert rule error_type "
                    f"in this blueprint. Known error_types: {sorted(known_error_types) or ['(none)']}. "
                    "If this is an infrastructure exception class (e.g. 'SparkException'), this warning "
                    "can be ignored — it will be matched against the stack trace at runtime."
                ),
            ))

    return results


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


# ── Cloudpickle compatibility check ──────────────────────────────────────────

def check_cloudpickle_compat(master_url: str) -> CheckResult:
    """Detect Python / cloudpickle version mismatch that breaks Python UDFs.

    PySpark 3.5 bundles cloudpickle 2.x. Python 3.13+ changed function
    internals in ways cloudpickle 2.x cannot serialize — any Python UDF
    will crash with infinite recursion. cloudpickle>=3.0 (system-installed)
    fixes this on the driver via monkey-patch; workers need it independently.
    """
    t = time.monotonic()
    py_ver = sys.version_info

    try:
        import pyspark.cloudpickle as bundled_cp
        bundled_ver = tuple(int(x) for x in bundled_cp.__version__.split(".")[:2])
    except Exception as exc:
        return CheckResult("cloudpickle", "skip", f"pyspark not installed: {exc}", _ms(t))

    try:
        import cloudpickle as system_cp
        system_ver = tuple(int(x) for x in system_cp.__version__.split(".")[:2])
    except ImportError:
        system_cp = None  # type: ignore[assignment]
        system_ver = (0, 0)

    # No issue below Python 3.13
    if py_ver < (3, 13):
        detail = (
            f"python={py_ver.major}.{py_ver.minor}  "
            f"bundled={'.'.join(str(x) for x in bundled_ver)}  "
            f"system={'not installed' if system_ver == (0, 0) else '.'.join(str(x) for x in system_ver)}"
            "  (no compatibility issue below Python 3.13)"
        )
        return CheckResult("cloudpickle", "ok", detail, _ms(t))

    # Python 3.13+ — bundled cloudpickle must be >=3.0 or system >=3.0 present for patch
    driver_ok = system_ver >= (3, 0)
    is_local = master_url.startswith("local")

    detail_parts = [
        f"python={py_ver.major}.{py_ver.minor}",
        f"bundled={'.'.join(str(x) for x in bundled_ver)}",
        f"system={'not installed' if system_ver == (0, 0) else '.'.join(str(x) for x in system_ver)}",
    ]

    if not driver_ok:
        detail_parts.append(
            "DRIVER: cloudpickle<3.0 — Python UDFs will crash. "
            "Fix: pip install 'cloudpickle>=3.0'"
        )
        return CheckResult("cloudpickle", "fail", "  ".join(detail_parts), _ms(t))

    if is_local:
        detail_parts.append("driver patched OK  (local mode — no workers)")
        return CheckResult("cloudpickle", "ok", "  ".join(detail_parts), _ms(t))

    # Remote cluster — workers need cloudpickle>=3.0 independently
    detail_parts.append(
        "driver patched OK  "
        "WORKERS: ensure cloudpickle>=3.0 is installed on all worker nodes "
        "(e.g. pip install cloudpickle>=3.0 in cluster init script) "
        "or use lang: java UDFs to avoid Python serialization entirely"
    )
    return CheckResult("cloudpickle", "warn", "  ".join(detail_parts), _ms(t))


# ── Runner ────────────────────────────────────────────────────────────────────

def check_aqtest(aqtest_path: Path) -> list[CheckResult]:
    """Schema pre-flight on a .aqtest.yml file.

    Returns a list of CheckResult: one for the file itself plus one per test case
    when cross-referencing module IDs against the referenced blueprint. Performs
    NO Spark execution — that is `aqueduct test`'s job. Doctor's job is to
    surface bad references and missing modules before the user invokes `test`.
    """
    import yaml
    t = time.monotonic()
    results: list[CheckResult] = []

    if not aqtest_path.exists():
        return [CheckResult("aqtest", "fail", f"file not found: {aqtest_path}", _ms(t))]

    try:
        raw = yaml.safe_load(aqtest_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        return [CheckResult("aqtest", "fail", f"invalid YAML in {aqtest_path}: {exc}", _ms(t))]

    if not isinstance(raw, dict):
        return [CheckResult("aqtest", "fail", f"{aqtest_path}: top-level must be a YAML mapping", _ms(t))]

    version = raw.get("aqueduct_test")
    if version not in ("1.0", 1, "1"):
        return [CheckResult(
            "aqtest", "fail",
            f"{aqtest_path}: missing or unsupported aqueduct_test version: {version!r}",
            _ms(t),
        )]

    bp_ref = raw.get("blueprint")
    if not bp_ref:
        results.append(CheckResult("aqtest", "fail", f"{aqtest_path}: missing 'blueprint' field", _ms(t)))
        return results

    bp_path = (aqtest_path.parent / bp_ref).resolve()
    if not bp_path.exists():
        results.append(CheckResult(
            "aqtest", "fail",
            f"{aqtest_path}: blueprint reference {bp_ref!r} does not resolve to an existing file ({bp_path})",
            _ms(t),
        ))
        return results

    tests = raw.get("tests") or []
    if not tests:
        results.append(CheckResult("aqtest", "warn", f"{aqtest_path}: no test cases declared", _ms(t)))
        return results

    # Cross-reference module IDs against the parsed blueprint
    try:
        from aqueduct.parser.parser import parse as _parse_bp
        bp = _parse_bp(str(bp_path))
        bp_module_ids = {m.id for m in bp.modules}
    except Exception as exc:
        results.append(CheckResult(
            "aqtest", "fail",
            f"{aqtest_path}: referenced blueprint {bp_path} failed to parse: {exc}",
            _ms(t),
        ))
        return results

    bad_cases: list[str] = []
    for tc in tests:
        if not isinstance(tc, dict):
            bad_cases.append(f"<non-dict test case: {tc!r}>")
            continue
        tid = tc.get("id", "<unnamed>")
        mod = tc.get("module")
        if not mod:
            bad_cases.append(f"{tid}: missing 'module'")
            continue
        if mod not in bp_module_ids:
            bad_cases.append(
                f"{tid}: module={mod!r} not in blueprint "
                f"(available: {sorted(bp_module_ids)[:5]}{'…' if len(bp_module_ids) > 5 else ''})"
            )
        if not tc.get("assertions"):
            bad_cases.append(f"{tid}: no assertions declared")

    if bad_cases:
        results.append(CheckResult(
            "aqtest", "fail",
            f"{aqtest_path}: {len(bad_cases)} test case issue(s): " + "; ".join(bad_cases),
            _ms(t),
        ))
    else:
        results.append(CheckResult(
            "aqtest", "ok",
            f"{aqtest_path}: {len(tests)} test case(s), blueprint={bp_ref}",
            _ms(t),
        ))
    return results


def check_aqscenario(aqscenario_path: Path) -> list[CheckResult]:
    """Schema pre-flight on a .aqscenario.yml file.

    Loads via the same parser used by `aqueduct heal --scenario` and
    `aqueduct benchmark`, then verifies the referenced blueprint exists and
    that `inject_failure.module` names an actual module in that blueprint.
    No LLM call is made.
    """
    t = time.monotonic()
    results: list[CheckResult] = []

    if not aqscenario_path.exists():
        return [CheckResult("aqscenario", "fail", f"file not found: {aqscenario_path}", _ms(t))]

    try:
        from aqueduct.surveyor.scenario import load_scenario
        sc = load_scenario(aqscenario_path)
    except ValueError as exc:
        return [CheckResult("aqscenario", "fail", f"{aqscenario_path}: {exc}", _ms(t))]
    except Exception as exc:
        return [CheckResult("aqscenario", "fail", f"{aqscenario_path}: load failed: {exc}", _ms(t))]

    if not sc.blueprint:
        results.append(CheckResult(
            "aqscenario", "fail",
            f"{aqscenario_path}: missing 'blueprint' reference",
            _ms(t),
        ))
        return results

    bp_path = (aqscenario_path.parent / sc.blueprint).resolve()
    if not bp_path.exists():
        results.append(CheckResult(
            "aqscenario", "fail",
            f"{aqscenario_path}: blueprint {sc.blueprint!r} does not resolve to an existing file ({bp_path})",
            _ms(t),
        ))
        return results

    inj_module = sc.inject_failure.get("module") if isinstance(sc.inject_failure, dict) else None
    if not inj_module:
        results.append(CheckResult(
            "aqscenario", "fail",
            f"{aqscenario_path}: inject_failure.module is required",
            _ms(t),
        ))
        return results

    try:
        from aqueduct.parser.parser import parse as _parse_bp
        bp = _parse_bp(str(bp_path))
        bp_module_ids = {m.id for m in bp.modules}
    except Exception as exc:
        results.append(CheckResult(
            "aqscenario", "fail",
            f"{aqscenario_path}: referenced blueprint {bp_path} failed to parse: {exc}",
            _ms(t),
        ))
        return results

    if inj_module not in bp_module_ids:
        results.append(CheckResult(
            "aqscenario", "fail",
            f"{aqscenario_path}: inject_failure.module={inj_module!r} not in blueprint "
            f"(available: {sorted(bp_module_ids)[:5]}{'…' if len(bp_module_ids) > 5 else ''})",
            _ms(t),
        ))
        return results

    expected = sc.expected_patch or {}
    note = ""
    if isinstance(expected, dict):
        ops_count = len(expected.get("ops") or [])
        forbidden = len(expected.get("forbidden_ops") or [])
        note = f"  expected_ops={ops_count}  forbidden_ops={forbidden}"

    results.append(CheckResult(
        "aqscenario", "ok",
        f"{aqscenario_path}: id={sc.id!r}  failed_module={inj_module!r}{note}",
        _ms(t),
    ))
    return results


def check_store_backend(
    label: str,
    store_cfg: Any,
    *,
    is_kv_only: bool = False,
) -> CheckResult:
    """Probe a single configured store backend for reachability.

    Args:
        label:       Display label — `observability`, `lineage`, or `depot`.
        store_cfg:   Pydantic store config (`RelationalStoreConfig` or
                     `KVStoreConfig`). Has `.backend` and `.path` attributes.
        is_kv_only:  When True, treats redis as a valid backend (depot only).

    Returns:
        `CheckResult` with status `ok | fail | warn`. Failures include the
        connection error message.
    """
    t = time.monotonic()
    backend = store_cfg.backend
    path_or_dsn = store_cfg.path

    if backend == "duckdb":
        try:
            import duckdb as _duckdb
            from pathlib import Path as _Path
            p = _Path(path_or_dsn)
            p.parent.mkdir(parents=True, exist_ok=True)
            conn = _duckdb.connect(str(p))
            try:
                conn.execute("SELECT 1").fetchone()
            finally:
                conn.close()
            return CheckResult(label, "ok", f"backend=duckdb  path={path_or_dsn}", _ms(t))
        except Exception as exc:
            return CheckResult(label, "fail", f"backend=duckdb  path={path_or_dsn}  error={exc}", _ms(t))

    if backend == "postgres":
        try:
            import psycopg2  # type: ignore[import-not-found]
        except ImportError:
            return CheckResult(
                label, "fail",
                f"backend=postgres but psycopg2 not installed — "
                "`pip install aqueduct-core[postgres]`",
                _ms(t),
            )
        try:
            conn = psycopg2.connect(path_or_dsn, connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                conn.close()
            # Redact DSN for log line — password lives inside path_or_dsn
            from aqueduct.stores.postgres import _PostgresRelational
            redacted = _PostgresRelational.__dict__["location_label"].fget(
                type("S", (), {"_dsn": path_or_dsn})()
            )
            return CheckResult(label, "ok", f"backend=postgres  dsn={redacted}", _ms(t))
        except Exception as exc:
            return CheckResult(label, "fail", f"backend=postgres  error={exc}", _ms(t))

    if backend == "redis":
        if not is_kv_only:
            return CheckResult(
                label, "fail",
                "backend=redis is depot-only; observability and lineage need duckdb or postgres",
                _ms(t),
            )
        try:
            import redis as _redis  # type: ignore[import-not-found]
        except ImportError:
            return CheckResult(
                label, "fail",
                "backend=redis but redis-py not installed — "
                "`pip install aqueduct-core[redis]`",
                _ms(t),
            )
        try:
            client = _redis.Redis.from_url(path_or_dsn, socket_connect_timeout=5)
            client.ping()
            return CheckResult(label, "ok", f"backend=redis  url={path_or_dsn}", _ms(t))
        except Exception as exc:
            return CheckResult(label, "fail", f"backend=redis  error={exc}", _ms(t))

    return CheckResult(label, "warn", f"backend={backend!r} unknown", _ms(t))


def run_doctor(
    config_path: Path | None = None,
    skip_spark: bool = False,
    blueprint_path: Path | None = None,
    aqtest_path: Path | None = None,
    aqscenario_path: Path | None = None,
) -> list[CheckResult]:
    """Run all checks and return results in order."""
    from aqueduct.config import load_config, ConfigError

    results: list[CheckResult] = []

    # Config — must succeed for remaining checks
    cfg_result = check_config(config_path)
    results.append(cfg_result)
    if cfg_result.status == "fail":
        results.append(CheckResult("cloudpickle", "skip", "config failed"))
        results.append(CheckResult("depot", "skip", "config failed"))
        results.append(CheckResult("observability", "skip", "config failed"))
        results.append(CheckResult("secrets", "skip", "config failed"))
        results.append(CheckResult("agent", "skip", "config failed"))
        results.append(CheckResult("webhook", "skip", "config failed"))
        results.append(CheckResult("spark", "skip", "config failed"))
        results.append(CheckResult("storage", "skip", "config failed"))
        return results

    try:
        cfg = load_config(config_path)
    except ConfigError:
        return results  # already recorded above

    # Cluster-mode store path validation.
    # Only the `duckdb` backend persists to the local FS — Phase 28 added
    # postgres / redis backends whose `path` is a DSN (`postgresql://...` /
    # `redis://...`) and which need no shared FS. Skip those.
    if cfg.deployment.env in ("cluster", "cloud"):
        _store_specs = {
            "observability": (cfg.stores.observability.backend, cfg.stores.observability.path),
            "lineage":        (cfg.stores.lineage.backend,       cfg.stores.lineage.path),
            "depot":          (cfg.stores.depot.backend,         cfg.stores.depot.path),
        }
        _duckdb_paths = {
            name: p for name, (backend, p) in _store_specs.items()
            if backend == "duckdb"
        }
        _bad = [
            name for name, p in _duckdb_paths.items()
            if not p or p.startswith(".") or not Path(p).is_absolute()
        ]
        if _bad:
            results.append(CheckResult(
                "cluster-stores", "fail",
                f"deployment.env={cfg.deployment.env!r} but DuckDB store paths are relative/local: "
                f"{_bad}. On YARN/K8s the driver CWD is ephemeral — stores will be lost on "
                "restart. Either set absolute paths on shared FS (NFS/EFS/PVC) in aqueduct.yml "
                "or switch the affected stores to backend=postgres / redis.",
            ))
        elif _duckdb_paths:
            results.append(CheckResult("cluster-stores", "ok", "DuckDB store paths are absolute"))
        else:
            results.append(CheckResult("cluster-stores", "ok", "no DuckDB stores — backend-managed persistence"))
    else:
        results.append(CheckResult("cluster-stores", "skip", "local mode — no cluster store check"))

    # Cloudpickle compatibility (pure version check — no Spark needed)
    results.append(check_cloudpickle_compat(cfg.deployment.master_url))

    # Per-store backend reachability — replaces the legacy observability.db/depot.db
    # file probes when a non-DuckDB backend is configured. For DuckDB
    # backends both probes still run and report the same OK signal.
    results.append(check_store_backend("observability", cfg.stores.observability))
    results.append(check_store_backend("lineage",       cfg.stores.lineage))
    results.append(check_store_backend("depot",   cfg.stores.depot, is_kv_only=True))

    # Secrets
    results.append(check_secrets(cfg.secrets.provider, resolver=cfg.secrets.resolver))

    # LLM connectivity
    results.append(check_agent(cfg.agent.provider, cfg.agent.base_url, cfg.agent.model))

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
        if aqtest_path is not None:
            results.extend(check_aqtest(aqtest_path))
        if aqscenario_path is not None:
            results.extend(check_aqscenario(aqscenario_path))
        return results

    spark_result, storage_result = check_spark(cfg.deployment.master_url, cfg.spark_config)
    results.append(spark_result)
    results.append(storage_result)

    if blueprint_path is not None:
        results.extend(check_blueprint_sources(blueprint_path))
    if aqtest_path is not None:
        results.extend(check_aqtest(aqtest_path))
    if aqscenario_path is not None:
        results.extend(check_aqscenario(aqscenario_path))

    return results
