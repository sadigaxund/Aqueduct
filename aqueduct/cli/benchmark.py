"""`aqueduct benchmark`, `benchmark-diff`, `benchmark-stats` commands.

Extracted verbatim from aqueduct/cli/__init__.py — no behaviour change. The
click group + shared helpers are imported from the package; the commands
register onto `cli` when this module is imported at the bottom of __init__.
"""
from __future__ import annotations

import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
    cli,
)
from aqueduct.cli.output import emit
from aqueduct.cli.style import error as _error

# ── aqueduct benchmark ────────────────────────────────────────────────────────

@cli.command()
@click.argument(
    "scenarios_pos",
    required=False,
    default=None,
    type=click.Path(exists=True),
)
@click.option(
    "--scenarios",
    "scenarios_dir",
    default=None,
    type=click.Path(exists=True),
    help="A .aqscenario.yml file or a directory of them (searched recursively). "
    "May also be given as a positional argument.",
)
@click.option(
    "--model",
    "models",
    multiple=True,
    default=None,
    help="Model to benchmark (repeatable: --model A --model B). Defaults to agent.model in aqueduct.yml",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@click.option(
    "--patches-dir",
    default="patches",
    show_default=True,
    help="Root directory for patch lifecycle (for previous-patch history)",
)
@click.option(
    "--format",
    "fmt",
    default="table",
    type=click.Choice(["table", "json"]),
    show_default=True,
    help="Output data shape (table | json). (-o/--output is reserved for "
    "file destinations on other commands; this is the data format.)",
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    help="Max concurrent LLM calls. Default 1 (serial); set >1 to parallelize scenario×model pairs.",
)
@click.option(
    "-s", "--set", "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Override an aqueduct.yml value for this run only (repeatable, "
         "in-memory). Dotted path — e.g. --set agent.provider=openai_compat "
         "--set agent.base_url=http://h:11434/v1 --set agent.timeout=600, "
         "--set stores.benchmark.persist=false / .gate_on_regression=true / .path=…",
)
@_env_options
def benchmark(
    scenarios_pos: str | None,
    scenarios_dir: str | None,
    models: tuple[str, ...],
    config_path: str | None,
    patches_dir: str,
    fmt: str,
    workers: int,
    set_items: tuple[str, ...],
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Run one or many scenarios against one or more LLM models and compare.

    \b
    Example:
      aqueduct benchmark aqscenarios/ --model claude-opus-4-7 --model llama3
      aqueduct benchmark one.aqscenario.yml          # single scenario

    Takes a .aqscenario.yml file or a directory (recursively globbed),
    runs each against every specified model, prints a comparison table.
    No Spark required — scenarios inject failures synthetically.
    """
    target = scenarios_pos or scenarios_dir
    if not target:
        click.echo(
            "✗ provide a scenario file or directory (positional, or --scenarios)",
            err=True,
        )
        sys.exit(exit_codes.USAGE_ERROR)
    scenarios_dir = target
    from aqueduct.cli.style import error as _error
    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.scenario import format_benchmark_table, run_benchmark

    try:
        _resolve_and_load_env(
            env_file,
            Path(config_path) if config_path else Path(scenarios_dir),
            cli_env=cli_env,
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    # ── -s/--set overrides (config-only; no blueprint in benchmark) ────────────
    if set_items:
        from aqueduct.overrides import OverrideError, apply_to_model, route_overrides
        try:
            _cfg_set_nested, _ = route_overrides(set_items, allow_blueprint=False)
            cfg = apply_to_model(cfg, _cfg_set_nested)
        except OverrideError as exc:
            _error(f"{exc}")
            sys.exit(exit_codes.CONFIG_ERROR)

    eng = cfg.agent
    # Connection identity from cfg.agent (override with `--set agent.*`).
    resolved_provider = eng.provider
    resolved_base_url = eng.base_url
    resolved_model = eng.model
    resolved_provider_options = eng.provider_options
    resolved_timeout = eng.timeout  # None = unbounded read (connect still bounded)
    resolved_max_reprompts = eng.max_reprompts
    resolved_engine_prompt_context = eng.prompt_context

    model_list = list(models) if models else ([resolved_model] if resolved_model else None)
    if not model_list:
        click.echo(
            "✗ no models specified — use --model <model> or set agent.model in aqueduct.yml",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    click.echo(
        f"↻ benchmark  scenarios={scenarios_dir}  "
        f"models={model_list}  provider={resolved_provider}",
        err=True,
    )

    # Count scenarios up-front for the banner. Cheap glob — load_scenario
    # runs again inside run_benchmark, but we only need the count here.
    _scn_count = (
        1 if Path(scenarios_dir).is_file()
        else len(list(Path(scenarios_dir).glob("**/*.aqscenario.yml")))
    )
    _pair_count = _scn_count * len(model_list)

    # Benchmark MUST use the same BudgetConfig as production. Reading from
    # the same engine config block enforces parity — divergence would
    # silently invalidate the leaderboard.
    from aqueduct.agent import resolve_budget as _resolve_budget
    _budget = _resolve_budget(
        getattr(cfg.agent, "budget", None),
        max_reprompts=resolved_max_reprompts,
    )

    # Banner shows pair count + LLM-call envelope (call counts, not time).
    # Floor = 1 call per pair (every pair succeeds first try); ceiling =
    # pair_count × budget.max_reprompts (every pair burns the full reprompt
    # budget). Use "floor/ceiling" instead of "min/max" so it can't be
    # misread as minutes next to a duration.
    _floor_calls = _pair_count
    _ceiling_calls = _pair_count * _budget.max_reprompts
    click.echo(
        f"[benchmark] {_scn_count} scenarios × {len(model_list)} models = "
        f"{_pair_count} pairs · LLM calls: {_floor_calls} floor / "
        f"{_ceiling_calls} ceiling (max_reprompts={_budget.max_reprompts})",
        err=True,
    )
    try:
        results = run_benchmark(
            scenarios_dir=Path(scenarios_dir),
            models=model_list,
            patches_dir=Path(patches_dir),
            provider=resolved_provider or "anthropic",
            base_url=resolved_base_url,
            provider_options=resolved_provider_options,
            timeout=resolved_timeout,
            max_reprompts=resolved_max_reprompts,
            engine_prompt_context=resolved_engine_prompt_context,
            workers=workers,
            budget=_budget,
        )
    except KeyboardInterrupt:
        # Per-pair results persist to benchmark.duckdb inside run_scenario
        # via Surveyor.record_benchmark_result, so completed pairs survive
        # the interrupt. Queued pairs are dropped when the executor's
        # __exit__ propagates the KeyboardInterrupt; in-flight HTTP calls
        # close their socket via the `with httpx.Client():` context
        # manager, signalling the LLM server to abort generation.
        click.echo(
            "\n↑ interrupted — completed pairs persisted to benchmark store",
            err=True,
        )
        sys.exit(130)  # SIGINT convention

    if fmt == "json":
        output: dict = {}
        for sid, model_results in results.items():
            output[sid] = {}
            for model, r in model_results.items():
                output[sid][model] = {
                    "passed": r.passed,
                    "patch_valid": r.patch_valid,
                    "patch_applies": r.patch_applies,
                    "confidence": r.confidence,
                    "duration_seconds": r.duration_seconds,
                    "attempts_to_parse": r.attempts_to_parse,
                    "reprompt_errors": r.reprompt_errors,
                    "root_cause_match": r.root_cause_match,
                    "category_match": r.category_match,
                    "diag_score": r.diag_score,
                    "violated_guardrails": r.violated_guardrails,
                    "failures": r.failures,
                    "soft_failures": r.soft_failures,
                    "patch": (
                        r.patch.model_dump(mode="json")
                        if r.patch is not None else None
                    ),
                }
        emit(output, fmt="json")
    else:
        _table = format_benchmark_table(results, model_list)
        click.echo(_table)
        # Mirror to stderr when stdout is redirected/piped so the user sees
        # the table in the terminal AND captures it in `> file`. Skip when
        # stdout is a TTY (avoid duplicate output in interactive runs).
        if not sys.stdout.isatty():
            click.echo(_table, err=True)

    total = sum(
        1 for model_results in results.values()
        for r in model_results.values()
    )
    passed = sum(
        1 for model_results in results.values()
        for r in model_results.values()
        if r.passed
    )
    failed = total - passed
    if failed and fmt != "json":
        click.echo(
            f"({failed} failed — rerun with --format json for failure "
            f"detail + the generated patch)",
            err=True,
        )

    # ── Persist + optional regression gate ────────────────────────────────────
    # Effective settings come from stores.benchmark (override with `--set`).
    from aqueduct.surveyor.benchmark_store import (
        BenchmarkStore,
        diff_latest,
        format_diff_table,
        has_regressions,
        persist_results,
    )
    bench_cfg = cfg.stores.benchmark
    _persist = bench_cfg.persist
    _gate = bench_cfg.gate_on_regression

    try:
        bench_store = BenchmarkStore.from_config(bench_cfg, Path(scenarios_dir))
    except ValueError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    regression_exit = False
    if _persist:
        written = persist_results(results, bench_store)
        if written and fmt != "json":
            click.echo(f"↳ persisted {written} benchmark row(s) → {bench_store.label}")
        if _gate:
            diff_entries = diff_latest(results, bench_store)
            if fmt == "json":
                emit(
                    {
                        "diff": [
                            {
                                "scenario_id": e.scenario_id,
                                "model": e.model,
                                "baseline_prompt_mismatch": e.baseline_prompt_mismatch,
                                "baseline_recorded_at": e.baseline.recorded_at if e.baseline else None,
                                "regressions": list(e.regressions),
                                "improvements": list(e.improvements),
                            }
                            for e in diff_entries
                        ],
                    },
                    fmt="json",
                )
            else:
                click.echo("")
                click.echo("Regression diff vs baseline:")
                click.echo(format_diff_table(diff_entries))
            if has_regressions(diff_entries):
                regression_exit = True
                if fmt != "json":
                    click.echo(
                        "✗ regression(s) detected vs baseline — failing the gate",
                        err=True,
                    )
    elif _gate and fmt != "json":
        click.echo(
            "(regression gate ignored: persistence is off)",
            err=True,
        )

    if failed or regression_exit:
        sys.exit(exit_codes.DATA_OR_RUNTIME)


# ── aqueduct benchmark-diff ──────────────────────────────────────────────────


@cli.command("benchmark-diff")
@click.option(
    "--store-path",
    "store_path_override",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the benchmark store. Default: resolved from stores.benchmark.",
)
@click.option("--config", "config_path", default=None, help="Path to aqueduct.yml")
@click.option(
    "--scenario",
    "scenario_filter",
    default=None,
    help="Restrict the diff to a single scenario_id.",
)
@click.option(
    "--model",
    "model_filter",
    default=None,
    help="Restrict the diff to a single model.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
@_env_options
def benchmark_diff_cmd(
    store_path_override: str | None,
    config_path: str | None,
    scenario_filter: str | None,
    model_filter: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Diff the two most recent benchmark runs per (scenario, model) pair.

    Reads from the benchmark store written by ``aqueduct benchmark`` (resolved
    from ``stores.benchmark`` in aqueduct.yml, like ``benchmark``/``benchmark-stats``
    — was a hardcoded ``./.aqueduct/benchmark.duckdb``). Does not re-run any
    scenarios. Exits non-zero on any regression (passed True→False, patch_applies
    True→False, diag_score or confidence drop > 5pp).
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.benchmark_store import (
        _SELECT_COLS,
        BenchmarkStore,
        DiffEntry,
        _connect,
        _fetch_baseline,
        _row_from_record,
        format_diff_table,
        has_regressions,
    )

    _resolve_and_load_env(env_file, Path(config_path) if config_path else None, cli_env=cli_env)
    # Resolve the store from config (honours stores.benchmark.path) unless an
    # explicit --store-path is given. Diff reads via a raw duckdb connection, so
    # a postgres benchmark backend isn't supported here yet.
    if store_path_override:
        store_path = Path(store_path_override)
    else:
        try:
            cfg = load_config(Path(config_path) if config_path else None)
            _bs = BenchmarkStore.from_config(cfg.stores.benchmark, Path("."))
        except (ConfigError, ValueError) as exc:
            _error(f"config error: {exc}")
            sys.exit(exit_codes.CONFIG_ERROR)
        if _bs.backend != "duckdb":
            _error(f"benchmark-diff supports duckdb benchmark stores only "
                   f"(stores.benchmark.backend={_bs.backend!r}); use --store-path for a duckdb file")
            sys.exit(exit_codes.USAGE_ERROR)
        store_path = Path(_bs.location)
    if not store_path.exists():
        _error(f"benchmark store not found: {store_path}")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    try:
        con = _connect(store_path)
    except Exception as exc:  # noqa: BLE001
        _error(f"cannot open benchmark store {store_path}: {exc}")
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    where_parts: list[str] = []
    params: list = []
    if scenario_filter:
        where_parts.append("scenario_id = ?")
        params.append(scenario_filter)
    if model_filter:
        where_parts.append("model = ?")
        params.append(model_filter)
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    try:
        pairs = con.execute(
            f"SELECT DISTINCT scenario_id, model FROM benchmark_results {where}",
            params,
        ).fetchall()
        entries: list[DiffEntry] = []
        for sid, model in pairs:
            rec = con.execute(
                f"SELECT {_SELECT_COLS} FROM benchmark_results "
                "WHERE scenario_id = ? AND model = ? "
                "ORDER BY recorded_at DESC LIMIT 1",
                [sid, model],
            ).fetchone()
            if rec is None:
                continue
            current = _row_from_record(rec)
            baseline, prompt_mismatch = _fetch_baseline(
                con, sid, model, current.prompt_version, current.recorded_at,
            )
            if baseline is None:
                entries.append(DiffEntry(sid, model, None, current, False, (), ()))
                continue
            from aqueduct.surveyor.benchmark_store import _compare
            regs, imps = _compare(baseline, current)
            entries.append(DiffEntry(sid, model, baseline, current, prompt_mismatch, tuple(regs), tuple(imps)))
    finally:
        con.close()

    if fmt == "json":
        emit(
            {
                "diff": [
                    {
                        "scenario_id": e.scenario_id,
                        "model": e.model,
                        "baseline_prompt_mismatch": e.baseline_prompt_mismatch,
                        "baseline_recorded_at": e.baseline.recorded_at if e.baseline else None,
                        "regressions": list(e.regressions),
                    "improvements": list(e.improvements),
                }
                for e in entries
            ],
        }, fmt="json")
    else:
        click.echo(format_diff_table(entries))

    if has_regressions(entries):
        if fmt != "json":
            _error("regression(s) detected")
        sys.exit(exit_codes.DATA_OR_RUNTIME)


# ── aqueduct benchmark-stats ─────────────────────────────────────────────────


@cli.command("benchmark-stats")
@click.argument("scenarios", required=False, default=None, type=click.Path(exists=True))
@click.option("--config", "config_path", default=None, help="Path to aqueduct.yml")
@click.option(
    "--store-path",
    "store_path_override",
    default=None,
    type=click.Path(dir_okay=False),
    help="Benchmark store path (overrides stores.benchmark). Default: scenario-anchored "
    "`.aqueduct/benchmark.duckdb`.",
)
@click.option(
    "-s", "--set", "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Override an aqueduct.yml value (e.g. --set stores.benchmark.backend=postgres "
         "--set stores.benchmark.path=postgresql://h/db).",
)
@click.option(
    "--format", "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
@_env_options
def benchmark_stats_cmd(
    scenarios: str | None,
    config_path: str | None,
    store_path_override: str | None,
    set_items: tuple[str, ...],
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Aggregate the benchmark store: leaderboard, hardest scenarios, pass-rate trend.

    Read-only over the store written by ``aqueduct benchmark`` (DuckDB or
    Postgres). Uses the LATEST row per (scenario, model) for the leaderboard
    and difficulty views.
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.benchmark_store import (
        BenchmarkStore,
        compute_stats,
        format_stats,
    )

    _resolve_and_load_env(
        env_file, Path(config_path) if config_path else None, cli_env=cli_env
    )
    try:
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    if set_items:
        from aqueduct.overrides import OverrideError, apply_to_model, route_overrides
        try:
            _cfg_set_nested, _ = route_overrides(set_items, allow_blueprint=False)
            cfg = apply_to_model(cfg, _cfg_set_nested)
        except OverrideError as exc:
            _error(f"{exc}")
            sys.exit(exit_codes.CONFIG_ERROR)

    anchor = Path(scenarios) if scenarios else Path(".")
    try:
        if store_path_override:
            store = BenchmarkStore(backend="duckdb", location=store_path_override)
        else:
            store = BenchmarkStore.from_config(cfg.stores.benchmark, anchor)
    except ValueError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    stats = compute_stats(store)
    if fmt == "json":
        emit(stats, fmt="json")
    else:
        click.echo(format_stats(stats))
