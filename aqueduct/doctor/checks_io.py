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


def check_capabilities(
    blueprint_path: Path,
    engine: str = "spark",
    *,
    context_override: dict[str, Any] | None = None,
) -> list[CheckResult]:
    """Phase 78 — version-constrained capability check for a compiled blueprint.

    ``aqueduct/compiler/capability_check.py`` already blocks UNSUPPORTED
    leaves at compile time (a hard CompileError). What compile-time CANNOT
    know is which dependency versions are actually installed — that is this
    check's job: parse+compile the blueprint, walk its modules to the
    capability leaves they touch (reusing
    ``aqueduct.compiler.capability_check.leaves_for_module``, the same
    mapping the compile gate uses), and for every leaf whose engine
    ``Capability.requires`` names a dependency, compare the installed
    version (via ``importlib.metadata``) against the declared specifier.

    Skips gracefully when a named dependency isn't installed at all
    (consistent with ``--skip-spark``) — that is reported by other checks
    (spark/cloudpickle), not duplicated here.
    """
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _pkg_version

    from aqueduct.compiler.capability_check import leaves_for_module
    from aqueduct.compiler.compiler import compile as _compile
    from aqueduct.errors import UnknownEngineError
    from aqueduct.executor.capabilities import get_capabilities, version_satisfies
    from aqueduct.parser.parser import parse

    t = time.monotonic()

    def _unregistered_engine_result(exc: UnknownEngineError) -> CheckResult:
        # Two distinct diagnoses, told apart by TYPE + the exception's own
        # `engines` list — never by matching on the message text:
        #   - empty registry  -> aqueduct's entry points are invisible (stale
        #     install); the exception message already carries the reinstall hint.
        #   - non-empty       -> the engine name itself is unknown (typo, or the
        #     engine's package/extra is not installed).
        if exc.no_engines_registered:
            return CheckResult(
                "capabilities", "fail", str(exc), _ms(t), group="validation",
            )
        return CheckResult(
            "capabilities", "fail",
            f"engine {engine!r} is not registered — its package/extra is not "
            f"installed. Registered engines: {exc.engines}",
            _ms(t), group="validation",
        )

    try:
        bp = parse(str(blueprint_path), cli_overrides=context_override or {})
        manifest = _compile(bp, blueprint_path=blueprint_path, engine=engine)
    except UnknownEngineError as exc:
        # Caught by TYPE, before the broader handlers below: an unregistered
        # engine is NOT a blueprint problem, so it must not be reported as
        # "blueprint did not parse/compile". UnknownEngineError subclasses
        # CompileError, so ordering matters — this clause must come first.
        return [_unregistered_engine_result(exc)]
    except Exception as exc:  # noqa: BLE001 — doctor checks never raise
        return [CheckResult(
            "capabilities", "skip",
            f"blueprint did not parse/compile — see other checks: {exc}",
            _ms(t),
            group="validation",
        )]

    try:
        caps = get_capabilities(engine)
    except UnknownEngineError as exc:
        # With registration going through the aqueduct.engines entry-point
        # group, this is unreachable for a shipped engine (the compile()
        # call above would already have raised for an unregistered engine) —
        # kept as a defensive fail rather than a silent skip in case a
        # future caller invokes this check without going through compile().
        return [_unregistered_engine_result(exc)]

    results: list[CheckResult] = []
    version_cache: dict[str, str | None] = {}

    for module in manifest.modules:
        if not getattr(module, "enabled", True):
            continue
        for leaf_id in leaves_for_module(module):
            cap = caps.verdict(leaf_id)
            if not cap.requires:
                continue
            for dep, spec in cap.requires.items():
                if dep not in version_cache:
                    try:
                        version_cache[dep] = _pkg_version(dep)
                    except PackageNotFoundError:
                        version_cache[dep] = None
                installed = version_cache[dep]
                name = f"capabilities:{module.id}:{leaf_id}"
                if installed is None:
                    results.append(CheckResult(
                        name, "skip",
                        f"{dep!r} not installed — cannot verify {leaf_id!r} requires {dep}{spec}",
                        group="validation",
                    ))
                    continue
                if version_satisfies(installed, spec):
                    results.append(CheckResult(
                        name, "ok",
                        f"module {module.id!r} uses {leaf_id!r} — requires {dep}{spec}, installed {installed}",
                        group="validation",
                    ))
                else:
                    hint = f" {cap.hint}" if cap.hint else ""
                    results.append(CheckResult(
                        name, "fail",
                        f"module {module.id!r} uses {leaf_id!r} — requires {dep}{spec}, "
                        f"installed {installed}.{hint}",
                        group="validation",
                    ))

    if not results:
        results.append(CheckResult(
            "capabilities", "ok", "no version-constrained capabilities used", _ms(t), group="validation",
        ))
    return results


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


def _check_webhook_connect(url: str, timeout: int) -> CheckResult:
    """TCP/TLS reachability only — no HTTP request line is ever sent.

    Opens a raw socket to host:port (TLS-wrapped for https) and closes it.
    The lightest-weight probe: proves the endpoint is network-reachable
    without touching the application layer at all.
    """
    import socket
    import ssl
    from urllib.parse import urlsplit

    t = time.monotonic()
    parts = urlsplit(url)
    host = parts.hostname
    if not host:
        return CheckResult("webhook", "fail", f"{url}: could not parse host", _ms(t))
    port = parts.port or (443 if parts.scheme == "https" else 80)
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            if parts.scheme == "https":
                ctx = ssl.create_default_context()
                with ctx.wrap_socket(sock, server_hostname=host):
                    pass
        return CheckResult("webhook", "ok", f"{host}:{port} → TCP/TLS reachable (connect only, no request sent)", _ms(t))
    except OSError as exc:
        return CheckResult("webhook", "fail", f"{url}: {exc}", _ms(t))
    except Exception as exc:
        return CheckResult("webhook", "fail", f"{url}: unexpected error: {exc}", _ms(t))


def check_webhook(
    url: str,
    method: str = "POST",
    headers: dict[str, str] | None = None,
    timeout: int = 10,
    health_probe: str = "options",
) -> CheckResult:
    """Probe the webhook URL. Depth is controlled by ``health_probe``
    (`WebhookEndpointConfig.health_probe`):

      connect  — TCP/TLS reachability only, no HTTP request sent.
      options  — an HTTP OPTIONS request (default). Any HTTP response
                 (including 405) counts as "reachable" — same status
                 semantics as ``full``.
      full     — the original behaviour: a real POST/PUT/PATCH with a
                 synthetic 'doctor_probe' payload. Can trigger consumer-side
                 side effects on endpoints that react to any request body.

    In all HTTP-request modes, any response < 500 is "reachable" (`ok`);
    500+ is a `warn` (endpoint exists, responded with a server error).
    Only network-level errors (DNS, connection refused, TLS) are `fail`.
    """
    if health_probe == "connect":
        return _check_webhook_connect(url, timeout)

    import httpx
    t = time.monotonic()
    rendered_headers = {"Content-Type": "application/json", **(headers or {})}

    if health_probe == "options":
        probe_method, kwargs = "OPTIONS", {}
    else:  # "full" — original real-request behaviour
        probe_method = method
        kwargs = {"json": {"event": "doctor_probe", "source": "aqueduct doctor"}}

    try:
        resp = httpx.request(
            probe_method, url,
            headers=rendered_headers,
            timeout=timeout,
            **kwargs,
        )
        status = resp.status_code
        if status < 500:
            return CheckResult(
                "webhook", "ok",
                f"{probe_method} {url} → HTTP {status}",
                _ms(t),
            )
        else:
            return CheckResult(
                "webhook", "warn",
                f"{probe_method} {url} → HTTP {status} (server error — endpoint exists but responded with error)",
                _ms(t),
            )
    except httpx.RequestError as exc:
        return CheckResult("webhook", "fail", f"{url}: {exc}", _ms(t))
    except Exception as exc:
        return CheckResult("webhook", "fail", f"{url}: unexpected error: {exc}", _ms(t))


def _redact_dsn(dsn: str) -> str:
    from urllib.parse import urlsplit, urlunsplit
    try:
        parts = urlsplit(dsn)
        if parts.password:
            netloc = parts.netloc.replace(f":{parts.password}@", ":***@")
            return urlunsplit(parts._replace(netloc=netloc))
    except Exception:
        return dsn
    return dsn


def _openai_models_url(base_url: str) -> str:
    return base_url.rstrip("/").rstrip("/v1").rstrip("/") + "/v1/models"


def _probe_openai_models(base_url: str, timeout: float = 10) -> tuple[list[str], str | None]:
    """``GET {base_url}/v1/models`` → ``(model_ids, error)`` (free, no tokens).

    Shared by the main agent check and the cascade-tier preflight ping so both
    report the same "N models available / selected model present" signal."""
    import httpx
    try:
        resp = httpx.get(_openai_models_url(base_url), timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        ids = [m.get("id", m.get("name", "?")) for m in data.get("data", data.get("models", []))]
        return ids, None
    except Exception as exc:
        return [], str(exc)


def check_agent(
    agent_provider: str,
    base_url: str | None,
    model: str,
    *,
    preflight: bool = False,
) -> CheckResult:
    """Probe agent (LLM) connectivity.

    anthropic:     default verifies ANTHROPIC_API_KEY is set (no API call, no
                   token cost). ``--preflight`` additionally proves the key +
                   endpoint actually work via ``GET /v1/models`` (free, no tokens).
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
        if preflight:
            # Prove the key + endpoint actually work — GET /v1/models lists the
            # account's models (free, no tokens), unlike the default key-set check.
            api = (base_url or "https://api.anthropic.com").rstrip("/")
            if api.endswith("/v1"):
                api = api[:-3]
            url = api + "/v1/models"
            try:
                resp = httpx.get(
                    url, headers={"x-api-key": key, "anthropic-version": "2023-06-01"}, timeout=10,
                )
                resp.raise_for_status()
                return CheckResult(
                    "agent", "ok",
                    f"provider=anthropic  model={model}  key verified ({url})  [preflight]",
                    _ms(t), group="agent",
                )
            except httpx.HTTPStatusError as exc:
                return CheckResult(
                    "agent", "warn",
                    f"provider=anthropic  ANTHROPIC_API_KEY set but {url} returned "
                    f"{exc.response.status_code} — key may be invalid/expired",
                    _ms(t), group="agent",
                )
            except Exception as exc:
                return CheckResult(
                    "agent", "warn",
                    f"provider=anthropic  key set but {url} unreachable: {exc}",
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

    models_url = _openai_models_url(base_url)
    available, err = _probe_openai_models(base_url)
    if err is not None:
        return CheckResult("agent", "fail", f"{models_url}: {err}", _ms(t))
    if available and model not in available:
        return CheckResult(
            "agent", "warn",
            f"provider=openai_compat  endpoint={models_url}  ⚠ model={model} not in "
            f"{len(available)} loaded models: {', '.join(available[:5])}",
            _ms(t),
        )
    return CheckResult(
        "agent", "ok",
        f"provider=openai_compat  endpoint={models_url}  model={model}  "
        f"({len(available)} models available)",
        _ms(t),
    )


def check_secrets(
    provider: str, resolver: str | None = None, base_dir: str | None = None
) -> CheckResult:
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
        from aqueduct.secrets import load_resolver_fn
        try:
            load_resolver_fn(resolver, base_dir)
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
    preflight: bool = False,
) -> CheckResult:
    """Probe a single configured store backend for reachability.

    Args:
        label:       Display label — `observability`, `lineage`, or `depot`.
        store_cfg:   Pydantic store config (`RelationalStoreConfig` or
                     `KVStoreConfig`). Has `.backend` and `.path` attributes.
        is_kv_only:  When True, treats redis as a valid backend (depot only).
        preflight:   When True, do a throwaway **write+read** round-trip (temp
                     table / SET-GET-DEL) — proves write perms, not just connect.

    Returns:
        `CheckResult` with status `ok | fail | warn`. Failures include the
        connection error message.
    """
    t = time.monotonic()
    backend = store_cfg.backend
    path_or_dsn = store_cfg.path

    if backend == "duckdb":
        try:
            from pathlib import Path as _Path

            import duckdb as _duckdb
            if path_or_dsn is None:
                return CheckResult(label, "ok", "backend=duckdb  (default per-blueprint routing)", _ms(t))
            p = _Path(path_or_dsn)
            # Per-blueprint routing: path is a directory, not a db file.
            # Verify write access instead of trying duckdb.connect().
            if p.is_dir():
                from tempfile import NamedTemporaryFile as _Ntf
                _Ntf(dir=str(p), delete=True).close()
                return CheckResult(label, "ok", f"backend=duckdb  path={path_or_dsn}  (routing base)", _ms(t))
            p.parent.mkdir(parents=True, exist_ok=True)
            conn = _duckdb.connect(str(p))
            try:
                conn.execute("SELECT 1").fetchone()
                if preflight:
                    conn.execute("CREATE TEMPORARY TABLE _aq_doctor_rt (x INTEGER)")
                    conn.execute("INSERT INTO _aq_doctor_rt VALUES (1)")
                    conn.execute("SELECT x FROM _aq_doctor_rt").fetchone()
                    conn.execute("DROP TABLE _aq_doctor_rt")
            finally:
                conn.close()
            _rt = "  [preflight: write+read ok]" if preflight else ""
            return CheckResult(label, "ok", f"backend=duckdb  path={path_or_dsn}{_rt}", _ms(t))
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
                    if preflight:
                        # Temp table auto-drops on disconnect; proves write perms.
                        cur.execute("CREATE TEMP TABLE _aq_doctor_rt (x INT)")
                        cur.execute("INSERT INTO _aq_doctor_rt VALUES (1)")
                        cur.execute("SELECT x FROM _aq_doctor_rt")
                        cur.fetchone()
            finally:
                conn.close()
            # Redact DSN for log line — password lives inside path_or_dsn
            from aqueduct.stores.postgres import _PostgresRelational
            redacted = _PostgresRelational.__dict__["location_label"].fget(
                type("S", (), {"_dsn": path_or_dsn})()
            )
            _rt = "  [preflight: write+read ok]" if preflight else ""
            return CheckResult(label, "ok", f"backend=postgres  dsn={redacted}{_rt}", _ms(t))
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
            if preflight:
                _k = "_aq_doctor_rt"
                client.set(_k, "1", ex=60)
                client.get(_k)
                client.delete(_k)
            _rt = "  [preflight: write+read ok]" if preflight else ""
            return CheckResult(label, "ok", f"backend=redis  url={_redact_dsn(path_or_dsn)}{_rt}", _ms(t))
        except Exception as exc:
            return CheckResult(label, "fail", f"backend=redis  error={_redact_dsn(str(exc))}", _ms(t))

    return CheckResult(label, "warn", f"backend={backend!r} unknown", _ms(t))


def check_remote_target(cfg: Any) -> CheckResult:
    """Verify credentials and API reachability for remote-submit targets.

    Only runs when ``deployment.target`` is ``databricks`` / ``emr`` /
    ``dataproc`` — no-op (return ``warn``) otherwise.  Non-fatal:
    failures warn, never block.
    """
    t = time.monotonic()
    target = getattr(cfg.deployment, "target", "local")
    if target not in ("databricks", "emr", "dataproc"):
        if target in ("standalone", "yarn", "kubernetes"):
            msg = (
                f"target={target} — no remote-submit credentials to verify "
                "(cluster reachability is covered by the Spark check)"
            )
        else:
            msg = "target is local — no remote cluster to check"
        return CheckResult("remote-target", "skip", msg, _ms(t))

    if target == "databricks":
        db_cfg = getattr(cfg.deployment, "databricks", None)
        if db_cfg is None:
            return CheckResult(
                "remote-target", "fail",
                "deployment.databricks block is required for target=databricks",
                _ms(t),
            )
        workspace = getattr(db_cfg, "workspace_url", "")
        if not workspace:
            return CheckResult(
                "remote-target", "fail",
                "deployment.databricks.workspace_url is required",
                _ms(t),
            )

        token = os.environ.get("DATABRICKS_TOKEN")
        if not token:
            return CheckResult(
                "remote-target", "fail",
                "DATABRICKS_TOKEN environment variable is not set — "
                "set it or reference it via @aq.secret('DATABRICKS_TOKEN')",
                _ms(t),
            )

        # Liveness check
        try:
            import httpx as _httpx
            ws = workspace.rstrip("/")
            if not ws.startswith("https://"):
                ws = f"https://{ws}"
            r = _httpx.head(
                f"{ws}/api/2.1/clusters/list",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            r.raise_for_status()
        except Exception as exc:
            return CheckResult(
                "remote-target", "fail",
                f"Databricks API unreachable at {workspace!r}: {exc}",
                _ms(t),
            )

        return CheckResult(
            "remote-target", "ok",
            f"Databricks workspace={workspace}  cluster_id={getattr(db_cfg, 'cluster_id', None) or 'new_cluster'}",
            _ms(t),
        )

    # emr / dataproc — not yet wired
    return CheckResult(
        "remote-target", "warn",
        f"remote target {target!r} is not yet implemented for doctor checks",
        _ms(t),
    )
