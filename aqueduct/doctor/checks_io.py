"""Leaf connectivity checks: config, depot, observability, webhook, agent,
secrets, the per-store-backend probe, and the .aqtest / .aqscenario schema
pre-flights.

These are self-contained — each returns a CheckResult (or list) and is invoked
by ``run_doctor`` (and a few are imported directly by tests). None of them is
monkeypatched by the test suite via ``aqueduct.doctor.X``, so they live outside
the package ``__init__`` cluster. The spark / network / blueprint-source checks
(which ARE patched and call each other by bare global name) stay in
``aqueduct/doctor/__init__.py``.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from aqueduct.doctor.base import CheckResult, _ms


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
        # Self-healing is opt-in and off by default. provider defaults to
        # "anthropic" with no base_url — i.e. nothing was actually configured.
        # No explicit intent + no key → not a misconfig, just unused: skip,
        # don't nag. Intent = an explicit base_url (only set deliberately).
        explicit_intent = bool(base_url)
        if not key:
            if not explicit_intent:
                return CheckResult(
                    "agent", "skip",
                    "self-healing not configured (opt-in; set agent.provider + "
                    "ANTHROPIC_API_KEY, or agent.provider: openai_compat, to enable)",
                    _ms(t), group="agent",
                )
            return CheckResult(
                "agent", "warn",
                "agent configured but ANTHROPIC_API_KEY not set — set it, or switch "
                "agent.provider to openai_compat (Ollama/vLLM/LM Studio). "
                "Self-healing only; pipeline runs fine without it.",
                _ms(t), group="agent",
            )
        return CheckResult(
            "agent", "ok",
            f"provider=anthropic  model={model}  ANTHROPIC_API_KEY present (API not called)",
            _ms(t), group="agent",
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
                "backend=postgres but psycopg2 not installed — "
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
