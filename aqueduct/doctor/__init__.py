"""Connectivity and health checks for aqueduct.yml configuration.

Each check is independent and returns a CheckResult.  Checks are run
sequentially so that dependent checks (e.g. storage depends on spark) can
inspect prior results.

Spark check note: SparkSession startup involves JVM initialisation — expect
5–15 seconds on first run.  Use skip_spark=True to skip in fast CI contexts.

Layer-boundary note: this module is the documented exception to the
"pyspark is imported only inside aqueduct/executor/spark/" rule. All three
pyspark imports here (`check_spark`, `check_storage`, `check_cloudpickle_compat`)
are deferred inside function bodies — top-level `import aqueduct.doctor` does
NOT pull in pyspark, so the doctor package is safe to import in `--skip-spark`
contexts and in test environments without the `[spark]` extra installed.

Package layout: the spark / network / blueprint-source cluster + ``run_doctor``
live here because the test suite monkeypatches several of them
(``aqueduct.doctor._tcp_ok``, ``check_spark``,
``check_blueprint_sources_from_manifest``, ``run_doctor``) and they call each
other by bare global name — patches only land when caller and callee share this
namespace. The self-contained leaf checks (config / depot / observability /
webhook / agent / secrets / store-backend / aqtest / aqscenario) live in
``checks_io`` and are re-exported below so ``from aqueduct.doctor import
check_config`` keeps working. ``CheckResult`` / ``_ms`` live in ``base`` to
avoid a circular import.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

from aqueduct.doctor.base import CheckResult, _ms
from aqueduct.doctor.checks_io import (
    check_agent,
    check_aqscenario,
    check_aqtest,
    check_config,
    check_depot,
    check_observability,
    check_secrets,
    check_store_backend,
    check_webhook,
)

__all__ = [
    "CheckResult",
    "check_agent",
    "check_aqscenario",
    "check_aqtest",
    "check_blueprint_sources",
    "check_blueprint_sources_from_manifest",
    "check_cloudpickle_compat",
    "check_config",
    "check_depot",
    "check_observability",
    "check_secrets",
    "check_spark",
    "check_storage",
    "check_store_backend",
    "check_webhook",
    "run_doctor",
]


# ── Spark + network cluster ─────────────────────────────────────────────────

def _host_port(url: str, default_port: int) -> tuple[str, int] | None:
    """Extract (host, port) from spark://h:p / http://h:p / h:p. None if unparseable."""
    import re
    from urllib.parse import urlparse
    if "://" in url:
        p = urlparse(url)
        if p.hostname:
            return p.hostname, p.port or default_port
        return None
    m = re.match(r"^([^:/]+):(\d+)$", url.strip())
    if m:
        return m.group(1), int(m.group(2))
    return None


def _tcp_ok(host: str, port: int, timeout: float = 3.0) -> bool:
    import socket
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _reachability_probe(master_url: str, spark_config: dict[str, Any]) -> tuple[CheckResult, CheckResult]:
    """Fast, bounded default: TCP-reach the master + S3 endpoint. No SparkSession.

    Answers the 95% question — "is my master/endpoint wiring sane" — in ~3s,
    with no slow-vs-broken ambiguity. Full session test lives behind --preflight.
    """
    t = time.monotonic()
    hint = "  (reachability only — run `aqueduct doctor --preflight` for a full Spark session test)"

    if master_url.startswith("local"):
        spark_res = CheckResult(
            "spark", "ok",
            f"master={master_url}  local mode — session built in-process at run time{hint}",
            _ms(t),
        )
    else:
        hp = _host_port(master_url, 7077)
        if hp is None:
            spark_res = CheckResult("spark", "warn", f"master={master_url}  cannot parse host:port for TCP probe{hint}", _ms(t))
        elif _tcp_ok(*hp):
            spark_res = CheckResult("spark", "ok", f"master={master_url}  reachable (TCP {hp[0]}:{hp[1]}){hint}", _ms(t))
        else:
            spark_res = CheckResult(
                "spark", "fail",
                f"master={master_url}  TCP connect to {hp[0]}:{hp[1]} failed — master down, "
                f"wrong HOST_IP, or firewall. (Not a timeout: no SparkSession was built.)",
                _ms(t),
            )

    # S3A endpoint reachability (only if object storage is configured).
    s3_ep = spark_config.get("spark.hadoop.fs.s3a.endpoint")
    if s3_ep:
        hp = _host_port(s3_ep, 80)
        if hp and _tcp_ok(*hp):
            storage_res = CheckResult("storage", "ok", f"s3a endpoint {s3_ep} reachable (TCP)", _ms(t))
        elif hp:
            storage_res = CheckResult("storage", "fail", f"s3a endpoint {s3_ep} — TCP connect to {hp[0]}:{hp[1]} failed", _ms(t))
        else:
            storage_res = CheckResult("storage", "warn", f"s3a endpoint {s3_ep} — cannot parse host:port", _ms(t))
    else:
        storage_res = check_storage(spark_config, spark_ok=False)
    return spark_res, storage_res


def check_spark(
    master_url: str, spark_config: dict[str, Any], preflight: bool = False
) -> tuple[CheckResult, CheckResult]:
    """Probe Spark + object storage.

    Default (`preflight=False`): fast bounded TCP reachability of master +
    S3 endpoint. No SparkSession — no slow/broken ambiguity, no false timeout.

    `preflight=True`: build a real SparkSession with the actual spark_config,
    run a task, check version + storage. **Unbounded** — you asked for the
    truth, so it waits as long as cluster cold-start / jar shipping needs
    (Ctrl-C to abort). This is the real "will my pipeline's Spark start" test.
    """
    if not preflight:
        return _reachability_probe(master_url, spark_config)

    t = time.monotonic()
    try:
        import pyspark
        from aqueduct.executor.spark.session import make_spark_session, stop_spark_session
        spark = make_spark_session("aqueduct.doctor", spark_config, master_url=master_url, quiet=True)
        spark.range(1).count()
        cluster_ver = spark.version
        client_ver = pyspark.__version__
        ver_mismatch = cluster_ver.split(".")[0] != client_ver.split(".")[0]
        ver_note = f"  ⚠ major version mismatch: pyspark={client_ver} cluster={cluster_ver}" if ver_mismatch else ""
        spark_detail = f"connected  master={master_url}  spark={cluster_ver}  pyspark={client_ver}  [preflight]{ver_note}"
        storage_result = check_storage(spark_config, spark_ok=True)
        stop_spark_session(spark)
        return CheckResult("spark", "warn" if ver_mismatch else "ok", spark_detail, _ms(t)), storage_result
    except Exception as exc:
        return (
            CheckResult("spark", "fail", f"preflight session failed: {master_url}: {exc}", _ms(t)),
            CheckResult("storage", "skip", "preflight spark session did not complete"),
        )


def check_storage(
    spark_config: dict[str, Any], spark_ok: bool, *, skipped: bool = False
) -> CheckResult:
    """Detect configured object storage from spark_config keys.

    `skipped=True` → the Spark probe was deliberately not run (--skip-spark):
    report as skip, not a failure (nothing failed; it just wasn't verified).
    """
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
        return CheckResult("storage", "skip", "no object storage keys in spark_config", _ms(t), group="spark")

    if skipped:
        return CheckResult(
            "storage", "skip",
            f"configured ({', '.join(detected)}); not probed (--skip-spark)",
            _ms(t), group="spark",
        )

    if not spark_ok:
        return CheckResult(
            "storage", "warn",
            f"configured ({', '.join(detected)}); connectivity not verified (Spark session not built)",
            _ms(t), group="spark",
        )

    # Endpoint reachability + creds-present. NO bucket I/O: doctor must never
    # require the user to pre-create a health-check bucket, and a read against
    # a synthetic bucket is ambiguous (MinIO 403s on missing buckets). Full S3
    # auth is proven by an actual run — stated honestly here.
    s3_ep = spark_config.get("spark.hadoop.fs.s3a.endpoint")
    has_keys = bool(
        spark_config.get("spark.hadoop.fs.s3a.access.key")
        and spark_config.get("spark.hadoop.fs.s3a.secret.key")
    )
    note = "auth not bucket-tested (doctor creates no probe bucket; a real run verifies it fully)"

    if s3_ep:
        hp = _host_port(s3_ep, 80)
        if hp is None:
            return CheckResult("storage", "warn", f"configured ({', '.join(detected)}) — cannot parse endpoint {s3_ep}", _ms(t), group="spark")
        if not _tcp_ok(*hp):
            return CheckResult("storage", "fail", f"S3/MinIO endpoint {s3_ep} — TCP connect to {hp[0]}:{hp[1]} failed", _ms(t), group="spark")
        if not has_keys:
            return CheckResult("storage", "warn", f"endpoint {s3_ep} reachable; no access/secret key in spark_config — {note}", _ms(t), group="spark")
        return CheckResult("storage", "ok", f"endpoint {s3_ep} reachable; creds present; {note}", _ms(t), group="spark")

    # GCS / ADLS: no single fixed endpoint to TCP-probe cheaply.
    return CheckResult("storage", "ok", f"configured ({', '.join(detected)}); {note}", _ms(t), group="spark")


# ── Blueprint source checks ─────────────────────────────────────────────────

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
        return CheckResult("cloudpickle", "skip", f"pyspark not installed: {exc}", _ms(t), group="spark")

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
        return CheckResult("cloudpickle", "ok", detail, _ms(t), group="spark", quiet_when_ok=True)

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
        return CheckResult("cloudpickle", "fail", "  ".join(detail_parts), _ms(t), group="spark")

    if is_local:
        detail_parts.append("driver patched OK  (local mode — no workers)")
        return CheckResult("cloudpickle", "ok", "  ".join(detail_parts), _ms(t), group="spark", quiet_when_ok=True)

    # Remote cluster — workers need cloudpickle>=3.0 independently
    detail_parts.append(
        "driver patched OK  "
        "WORKERS: ensure cloudpickle>=3.0 is installed on all worker nodes "
        "(e.g. pip install cloudpickle>=3.0 in cluster init script) "
        "or use lang: java UDFs to avoid Python serialization entirely"
    )
    return CheckResult("cloudpickle", "warn", "  ".join(detail_parts), _ms(t), group="spark")


# ── Orchestrator ─────────────────────────────────────────────────────────────

def run_doctor(
    config_path: Path | None = None,
    skip_spark: bool = False,
    blueprint_path: Path | None = None,
    aqtest_path: Path | None = None,
    aqscenario_path: Path | None = None,
    preflight: bool = False,
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
            # WARN, not FAIL: the blueprint still runs, and doctor cannot prove
            # data loss — it can't tell if .aqueduct/ sits on a mounted shared
            # FS. ✗ means "this will break"; this is "runs, but fragile".
            results.append(CheckResult(
                "cluster-stores", "warn",
                f"relative DuckDB paths {_bad} on env=cluster — lost on driver restart "
                "unless on a shared FS. Use an absolute shared-FS path or postgres/redis.",
                group="stores",
            ))
        elif _duckdb_paths:
            results.append(CheckResult("cluster-stores", "ok", "DuckDB store paths are absolute", group="stores"))
        else:
            results.append(CheckResult("cluster-stores", "ok", "no DuckDB stores — backend-managed persistence", group="stores"))
    else:
        results.append(CheckResult("cluster-stores", "skip", "local mode — no cluster store check", group="stores"))

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
        results.append(CheckResult("spark", "skip", "--skip-spark flag set", group="spark"))
        results.append(check_storage(cfg.spark_config, spark_ok=False, skipped=True))
        if blueprint_path is not None:
            results.extend(check_blueprint_sources(blueprint_path))
        if aqtest_path is not None:
            results.extend(check_aqtest(aqtest_path))
        if aqscenario_path is not None:
            results.extend(check_aqscenario(aqscenario_path))
        return results

    spark_result, storage_result = check_spark(
        cfg.deployment.master_url, cfg.spark_config, preflight=preflight
    )
    results.append(spark_result)
    results.append(storage_result)

    if blueprint_path is not None:
        results.extend(check_blueprint_sources(blueprint_path))
    if aqtest_path is not None:
        results.extend(check_aqtest(aqtest_path))
    if aqscenario_path is not None:
        results.extend(check_aqscenario(aqscenario_path))

    return results
