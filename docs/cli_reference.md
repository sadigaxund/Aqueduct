# Aqueduct CLI Reference

All commands accept `--config <path>` to point to a non-default `aqueduct.yml`. Aqueduct also automatically walks up from the current working directory to find it.

Bare `aqueduct` (no subcommand) prints a branded version banner including the engine version, Python version, and Spark version (if available).

## Global flags

| Flag | Default | Description |
|------|---------|-------------|
| `--version` | n/a | Print the installed `aqueduct-core` version and exit |
| `-v`, `--verbose` | off | Enable DEBUG logging (LLM prompts, SQL plans, etc.) |
| `--log-format text\|json` | `text` | Output format for logs |
| `--suppress-warning <id>` | n/a | Silence one `AQ-WARN [<id>]` rule. Repeatable. Use `'*'` to silence all. Applied BEFORE the subcommand. |

**Output control flags:**

| Flag | Default | Purpose | Used by |
|------|---------|---------|---------|
| `--format table\|json\|csv` | `table` | Result data shape | `runs`, `report`, `benchmark`, etc. |
| `--format html` | n/a | `report` only: self-contained single-file HTML run report (to stdout; redirect to a file) | `report` |
| `-o`, `--output <path>` | `-` (stdout) | Write output to file | `compile`, `schema` |
| `--show manifest\|provenance\|inputs\|all` | `manifest` | Choose what to display | `compile` |

---

## Validate vs doctor

- **`aqueduct validate`**: Fast, static validation (schema + parsing). Ideal for CI/pre-commit. For a Blueprint with a `hooks:` block, it also runs the same static hook-graph walk `doctor` uses (cycles / chain-depth overflow / missing `blueprint:` targets) and reports problems as a suppressible **warning** (`[aqueduct:hook_cycle]`, added to `warnings.suppress`), never a validation failure.
- **`aqueduct doctor`**: Live connectivity checks (Spark, stores, agent, sources). Use before deploying.

Both commands auto-detect file type based on the version header (`aqueduct:` vs `aqueduct_config:`).

### `.env` resolution

Aqueduct automatically loads `.env` from the directory of the config or blueprint file (not CWD). You can override with:
- `-e KEY=VAL` (highest precedence, repeatable)
- `--env-file <path>`
- `AQ_NO_ENV_FILE=1` to disable entirely

---

## 1. Project setup

| Command | Description |
|---------|-------------|
| `aqueduct init` | Create a new project skeleton with templates, directories, and `.gitignore` |
| `aqueduct doctor` | Check connectivity and configuration health |
| `aqueduct doctor <file>` | Validate a specific blueprint or config file |
| *(webhook check depth)* | Each configured `webhooks.*` endpoint's `health_probe:` field (`connect`/`options`/`full`, default `options`) controls how `doctor` probes it, see [Production Guide](production_guide.md) |
| `aqueduct doctor --skip-spark` | Fast check without starting Spark |
| `aqueduct doctor --preflight` | Full Spark session + storage validation. Also: verifies cloud Ingress/Egress objects (`s3a://`/`gs://`/`abfss://`) exist via Spark's Hadoop FileSystem; warns on a **Spark major.minor** vs client-pyspark mismatch; for `agent.provider: anthropic` proves the API key works (`GET /v1/models`, no tokens); **imports** each Python `udf_registry` entry (catches typos/missing deps); does a store **write+read** round-trip (write perms, not just connect); for `jdbc:` sources attempts a real connect+auth (postgres via psycopg2); and proves Spark can round-trip `handoff.root` via a real SparkSession (see the handoff rows below). Default `doctor` only checks endpoint reachability. A standalone **Java** runtime check (detected JVM version + a pyspark-4-needs-Java-17 nudge) runs even without `--preflight`. |
| *(handoff checks, always on)* | `handoff-space`: free disk space at `handoff.root` (skips on a remote URI — not a local-disk question; warns, never fails, below a 5 GiB heuristic). `handoff-access:<engine>`: a write+read+cleanup round-trip at `handoff.root` for every registered engine (`aqueduct.executor.capabilities.CAPABILITY_REGISTRY`, never a hardcoded engine list). DuckDB's probe runs unconditionally (a `:memory:` connection is cheap) and attempts a real round trip on a remote root too — `httpfs` autoloads on first touch, using the configured `engine.duckdb.s3_*`/`extension_repository` if present (see [specs.md §10.9](specs.md#109-engines-and-the-capability-framework)) — reporting a genuine `ok`/`fail` rather than an unconditional skip. Spark's probe needs a real SparkSession to prove the configured `engine.spark.conf` credentials actually work, so it only runs under `--preflight`; the default is a `skip` naming that. |
| `aqueduct doctor --aqtest <file>` | Schema pre-flight on a `.aqtest.yml` (verifies blueprint ref + module IDs) |
| `aqueduct doctor --aqscenario <file>` | Schema pre-flight on a `.aqscenario.yml`: hard-validates the whole file (unknown keys at any level, the `expected_patch.effect` shape, the assertion vocabulary, `domains:`), then verifies the blueprint ref and that `inject_failure.module` names a module in it. Reports the simulated engine, the effect keys the scenario actually grades, and its declared domains. No LLM call. |
| `aqueduct doctor -v, --verbose` | Also show skipped checks (not-applicable / not-configured), not just the collapsed summary |
| `aqueduct doctor --format json` | Machine-readable result of every check (`{schema_version, summary, checks[]}`); implies `--verbose` (nothing collapsed). Text mode groups checks into sections (Config, Stores, Spark, …). |
| `aqueduct completion {bash\|zsh\|fish}` | Emit a shell-completion script for installation |

### Shell completion

`aqueduct completion` generates a completion script for `bash`, `zsh`, or `fish` from the live click command tree, new flags pick up automatically; rerun after upgrading Aqueduct.

```bash
aqueduct completion bash > /etc/bash_completion.d/aqueduct.sh
aqueduct completion zsh  > /usr/local/share/zsh/site-functions/_aqueduct
aqueduct completion fish > ~/.config/fish/completions/aqueduct.fish
```

---

## 2. Development loop

| Command | Description |
|---------|-------------|
| `aqueduct validate <file>...` | Static validation of blueprints/configs |
| `aqueduct validate <file>... --format json` | Same checks, machine-readable (`{schema_version, summary, files[]}`) for CI |
| `aqueduct lint <blueprint>` | Static style + correctness checks beyond schema validation (AQ-LINT rules) |
| `aqueduct lint <blueprint> --strict` | Promote every finding to error: exit non-zero on any finding (CI gate) |
| `aqueduct lint <blueprint> --format json` | Machine-readable findings (`{schema_version, summary, findings[]}`) |
| `aqueduct compile <blueprint>` | Output the fully resolved Manifest |
| `aqueduct run <blueprint>` | Compile and execute the pipeline |
| `aqueduct test <file.aqtest.yml>` | Run isolated module unit tests |
| `aqueduct schema [--target blueprint\|config\|patch] [-o <file>]` | Emit the Pydantic-derived JSON Schema for a Blueprint, `aqueduct.yml`, or PatchSpec, enables IDE autocomplete and CI schema gates. Writes to stdout by default. |

### `aqueduct lint` rules

`lint` runs after a successful parse and reports static smells the schema permits. Each rule has a stable `AQ-LINT<NNN>` id and a severity. All initial rules are advisory (`warn`), a warn-only result exits `0`; `--strict` promotes findings to errors so a non-empty result exits `1` (`CONFIG_ERROR`), for CI gating. SQL rules parse Channel `op: sql` queries with sqlglot (`dialect="spark"`); unparseable SQL is skipped, never errored. Findings are suppressible by rule_id via the Blueprint's `warnings.suppress` block (or `--suppress-warning`/`aqueduct.yml`), same as every other AQ-* diagnostic.

| Rule | Severity | Flags |
|------|----------|-------|
| `AQ-LINT001` | warn | Orphan module: not referenced by any edge, `depends_on`, `spillway`, or `attach_to` |
| `AQ-LINT002` | warn | Module label is empty or just repeats its `id` |
| `AQ-LINT003` | warn | Duplicate edge: same `(from, to, port)` declared more than once |
| `AQ-LINT004` | warn | Un-aliased self-join: a relation referenced 2+ times without distinct aliases |
| `AQ-LINT010` | warn | Cartesian join: `JOIN` with no `ON`/`USING` (explicit `CROSS JOIN` is allowed) |
| `AQ-LINT011` | warn | `SELECT *` in a Channel that feeds directly into an Egress (silent schema drift) |
| `AQ-LINT012` | warn | Aggregate function mixed with a non-aggregated column and no `GROUP BY` |

### Important `aqueduct run` flags

| Flag | Default | Description |
|------|---------|-------------|
| `--run-id <uuid>` | auto-generated | User-supplied run id (otherwise UUID4) |
| `--from <module_id>` | n/a | Start execution at this module. Refused (`CONFIG_ERROR`) for a polyglot Blueprint (2.37) — cross-island module-range selection isn't implemented. |
| `--to <module_id>` | n/a | Stop execution after this module. Same polyglot refusal as `--from` above. |
| `--execution-date YYYY-MM-DD` | today (UTC) | Logical date for `@aq.date.*`: enables idempotent backfills |
| `--resume <run_id>` | n/a | Resume from checkpoints of a previous run |
| `--parallel` | off | Execute independent DAG branches concurrently (one thread per connected component) |
| `--sandbox` | off | Dev dry-run: execute against sampled inputs with every Egress skipped (no writes, no self-healing, no observability persistence). Fast feedback loop for iterating on transforms. Requires `engine: spark`; also refused (`CONFIG_ERROR`) for a polyglot Blueprint (2.37) — a single-session dry-run can't replay a multi-engine Manifest. |
| `--sample <N>` | `1000` | Row cap per Ingress in `--sandbox` mode (`0` = no limit). Ignored without `--sandbox`. |
| `-s` / `--set PATH=VALUE` | n/a | Override any config or blueprint value for this run only (repeatable, in-memory, never persisted). See [Config overrides](#config-overrides--s--set) below. |
| `--ctx KEY=VALUE` | n/a | Override a Tier 0 context variable. Repeatable. Environments that can't pass CLI args (CI, Airflow) can set `AQUEDUCT_CTX_<KEY>` env vars instead, top-level keys only, one priority step below `--ctx` (see specs.md §5.2). |
| `--profile <name>` | n/a | Activate a `context_profiles:` block |
| `--store-dir <path>` | from `aqueduct.yml` (else `.aqueduct/`) | Override store directory for this run |
| `--webhook <url>` | from `aqueduct.yml` | Override failure webhook |
| `--allow-multi-patch` | off | Permit `max_patches > 1` for this run (overrides `danger.allow_multi_patch=false`). |
| `--config <path>` | `./aqueduct.yml` walked upward | Path to `aqueduct.yml` |
| `-e KEY=VAL` / `--env KEY=VAL` | n/a | Inline env override (highest precedence). Repeatable. |
| `--env-file <path>` | anchored `<dir>/.env` | Explicit fallback `.env` (used only when no anchored project `.env` exists) |

### Run output: runtime warning summary

Runtime warnings raised *during* execution (Probe/Assert findings, retry notices, and other per-module diagnostics) are shown twice: inline under the module that raised them (`↳ [rule_id] …`, no `⚠` icon; the roll-up header carries it) and again as a collapsed roll-up just before the run footer:

```
⚠ runtime: 3 warnings   ·  -v for full text
  · [runtime_assert]         clean_users: Assert [not_null]: 12 null emails
  · [runtime_probe_signal_error]  ingest_orders: signal evaluation failed
  · [runtime_retry_waiting]  write_warehouse: retrying egress (attempt 2/3)
```

Each line keeps its stable `rule_id` (e.g. `runtime_assert`, `runtime_probe_*`, `runtime_retry_*`) so it can be copied straight into `warnings.suppress` in `aqueduct.yml` (same mechanism as compile-time `AQ-WARN` ids). The roll-up is additive, inline per-module warnings still print. Pass `-v` / `--verbose` for the full (untruncated) warning text.

### Run output: Arcade tree view

Arcade-expanded modules nest under their Arcade in the summary block, the parent row shows the worst child status (any ✗ → ✗, else any ✓ → ✓, else ⏭):

```
  ✓ raw_tickets              1.8 s
  ✗ arcade_conditional
    ├─ ✓ save_active         4 rows  ·  626 ms
    ├─ ✗ save_other          — source not found at 'data/other'
    └─ ⏭ notify_export
```

The nesting is display-only. Logs, the observability store, and the `failed_module=` footer keep the full flattened id (`arcade_conditional__save_other`), so copy-pasting ids into `--from`/`--to`, `report`, or SQL against `run_records` works unchanged.

### Run output: lifecycle hooks

When the Blueprint declares `hooks:` (see specs.md §4.2), the matching event's entries run, each with a `✓/⚠` line. `on_success`/`on_failure` run after the terminal footer and close with a final `✓ run complete`; `on_patch_pending`/`on_healed` fire mid-run at heal milestones (staging a patch for review, and a heal's re-run succeeding) and print inline, without their own footer. A chained `blueprint:` hook streams its own full run output inline when it's a subprocess (the default); `in_process: true` instead reuses the caller's live SparkSession, no separate subprocess, no separate `aqueduct` invocation in the output:

```
✓ blueprint complete
· hooks  ·  on_success (2)
  ✓ scripts/commit_outputs.sh r-1234    1.2 s
  ✓ aqueduct run downstream.yml    4m 02s
✓ run complete
```

Hook outcomes never change the run's exit code. `command:` entries require `danger.allow_command_hooks: true` in `aqueduct.yml` (skipped with `[hook_command_disabled]` otherwise); cyclic `blueprint:` chains are refused with `[hook_cycle]` (`aqueduct doctor` checks the chain statically across all four events). `when_error: [ErrorType, ...]` on `on_failure`/`on_patch_pending`/`on_healed` entries filters which entries fire for a given run, a non-matching entry is silently skipped (no `⚠` line, doesn't count against the "first failure stops the rest" rule).

### Config overrides (`-s` / `--set`)

`--set PATH=VALUE` overrides any value in `aqueduct.yml` or the Blueprint for a single invocation, repeatable, applied in memory, **never written back to disk**. It is the highest-precedence layer:

```
--set  >  blueprint agent:  >  aqueduct.yml  >  built-in defaults
```

One flat dotted namespace addresses whichever schema owns the field. For `aqueduct run`, an `agent.*` path that the Blueprint schema declares (e.g. `agent.approval`, `agent.max_patches` — POLICY fields) lands on the Blueprint (which already wins the merge); CONNECTION fields (`agent.provider`, `agent.base_url`, `agent.model`, `agent.api_key`, `agent.provider_options`, `agent.timeout`, `agent.cascade` — 2.59: no longer legal on the Blueprint schema at all) and engine-only agent fields (`agent.budget.*`, `agent.retry.*`) and everything else (`deployment.*`, `danger.*`, `stores.*`) land on `aqueduct.yml`. A path no schema declares is an error with a nearest-sibling suggestion.

> **Precedence is per-key, and a cascade tier's own fields are separate keys.** `--set` wins among the *sources* for the key it targets. `agent.cascade:` is engine-level only (2.59) — each tier's `timeout` / `max_reprompts` / `provider` / … are *their own keys* that only inherit the flat `agent.*` (`aqueduct.yml`) value **when the tier leaves them unset**. So `--set agent.timeout=600` raises the flat `aqueduct.yml` default and any tier that inherits it, but it does **not** override a tier that declares its own `timeout:` (that is a different key, and the tier's explicit value is intentional). To change one tier, edit that tier's field in `aqueduct.yml`'s `agent.cascade:` block. (A per-tier `--set agent.cascade[N].timeout` addressing form is on the roadmap; see `TODOs.md`.)

> **Engine/session config is a three-layer merge, and `--set` is the top layer.** `engine.<name>.*` is not resolved by the plain overlay above: `aqueduct.executor.session_config.resolve_session_engine_config` layers the Blueprint's own `engine.<name>:` block over the `aqueduct.yml` one, and then `--set` over both. That third layer exists because the overlay alone left `--set` UNDER the Blueprint, so a value a self-heal had written into `engine.spark.conf` months earlier beat the flag typed at the prompt. It is safe for the flag to win because it is per-invocation and never written back: it overrides a heal for one run, it does not undo one. Two visible consequences: a heal that tries to write a key the invocation pins is refused (Gate 1 names the exact `--set` path rather than telling the author to write a different value), and `aqueduct patch preview` takes no `--set`, so its engine-config verdict is measured with no pins and can differ from what `aqueduct run -s ...` reports.

Value grammar:
- `PATH=value`: coerced: `true`/`false` → bool, `null`/`none` → None, then int, then float, else the literal string.
- `PATH:=value`: `value` parsed as JSON, for structured values (objects/arrays/typed scalars).
- A path that continues past a free-form dict field (`engine.spark.conf`, whose keys are themselves dotted) rejoins the remaining segments into one key: `--set engine.spark.conf.spark.sql.shuffle.partitions=800` sets the single key `spark.sql.shuffle.partitions`. A dict of structured entries (`stores.depots.<name>.backend`) keeps nesting normally.

```bash
aqueduct run bp.yml \
  --set agent.approval=auto \
  --set agent.budget.max_seconds=5 \
  --set agent.budget.max_tokens_total=80000 \
  --set engine.spark.master_url=spark://10.0.0.39:7077 \
  --set engine.spark.conf.spark.sql.shuffle.partitions=800 \
  --set agent.provider_options:='{"temperature":0.1}'
```

`--set danger.*` overrides print a loud stderr warning (single-run, not persisted). Available on `run`, `benchmark`, and `heal`. `--set` replaced the one-off override flags (`--provider`/`--base-url`/`--timeout`/`--no-persist`/`--store-path`/`--gate-on-regression`), which were removed in 2.0.

**Remote-submit targets** (`deployment.target: emr/dataproc`) are rejected at config-load with a "not yet supported" error; there is no built-in remote-submit target today.

---

## 3. Observability

| Command | Description |
|---------|-------------|
| `aqueduct runs` | List recent runs |
| `aqueduct runs --failed` | Show only failed runs |
| `aqueduct runs --heal-coverage` | Zero-token heal coverage (heals resolved by the signature memory cache vs the LLM) |
| `aqueduct runs --format text\|json` | `text\|json` only: the global `table\|json\|csv` does not apply to `runs` |
| `aqueduct report <run_id>` | Detailed flow report for a run |
| `aqueduct report <run_id> --format json` | Since 2.37: also carries a top-level `engines` list (every engine this run's modules actually used — one entry for a single-engine run, more for a polyglot one) and a per-module `engine` field on each `module_results` entry |
| `aqueduct report --trend <column> --blueprint <id>` | Cross-run quality trend for one column (null-rate + type history) from probe signals; `--since <ISO_DATE>` windows it (default 30 days) |
| `aqueduct report <run_id> --profile` | Per-module resource profile for one run (duration + I/O over `module_metrics`), heaviest module first, with each module's share of run time/bytes |
| `aqueduct report --profile --blueprint <id> [--last N]` | Cross-run resource trend per module over the last N runs (default 10): runs count, avg/max/last duration, flags a module whose latest run is >1.5× its window average as a slowdown |
| `aqueduct report <run_id> --format html > run.html` | Self-contained single-file HTML run report (status, module results, resource profile); no server, renders offline |
| `aqueduct lineage <blueprint>` | Column-level lineage graph |
| `aqueduct lineage <blueprint.yml> --chain <column> [--types]` | Vertical source→output trace for one column; `--types` annotates each hop with the sqlglot-inferred SQL type and marks type changes (computed on demand from the blueprint; needs a file path, not an id) |
| `aqueduct signal <signal_id> --blueprint <id>` | View or override Probe gates. `--blueprint` is required with the duckdb backend (unless `--store-dir` is given), the override lives in that blueprint's routed store, `<base>/<blueprint_id>/observability.db`; ignored for postgres (one shared schema) |
| `aqueduct blueprint history <id\|blueprint.yml>` | Chronological remediation timeline for one blueprint: heal run starts, `PATCH_APPLY` (with confidence), outcome ✓/✗, `PATCH_REJECT`, read from `patch_index` + `healing_outcomes`, merged with the blueprint file's git commit history when it is git-tracked and a file path was given (a commit with no `---aqueduct---` trailer shows as `manual_edit`). `--store-dir`, `--config`, `--format table\|json` (default `table`). Read-only; also registered as the `blueprint_history` diagnostics tool (`aqueduct/tools/`, specs.md §8.10). |
| `aqueduct dashboard [--config <f>] [--store-dir <d>] [--port 8501] [--no-browser]` | Launch the local, read-only **Streamlit** observability dashboard: fleet view (cross-blueprint runs / success-rate / heal-rate, trends), per-run module metrics, column-lineage Sankey, doctor, config. On-demand local viewer (like the Spark UI), never a production server. Requires the optional `dashboard` extra: `pip install aqueduct-core[dashboard]`. A 🔄 Refresh button re-reads the store (manual; no background polling). |
| `aqueduct mcp serve [--config <f>]` | Serve the read-only diagnostics tools (runs, run detail, lineage, patches, probe signals, doctor, blueprint history) over **MCP on stdin/stdout**, the MCP client (Claude Desktop, an IDE) spawns this command as a subprocess; do not run it interactively. stdio/local-only, no ports; every tool is read-only and every result (and error message) is secret-redacted. `--config` (default: none) is injected into each tool call that accepts `config_path` unless the client sets it explicitly. Requires the optional `mcp` extra: `pip install aqueduct-core[mcp]`. See specs.md §8.10. |

**`--chain --types` example**: tracing one column's per-hop transform, source to output:

```
$ aqueduct lineage pipelines/orders.yml --chain total_amount --types
Column chain — blueprint: orders_pipeline  column: total_amount

  ▸ apply_discount.total_amount  :: DECIMAL(10,2)
      ← read_orders.total_amount  [passthrough]
  │
  ▸ cast_to_float.total_amount  :: DOUBLE  ⚠ type change
  │    ← apply_discount.total_amount  [CAST]
```

Each `▸` line is one hop: the Channel module and output column, plus (with `--types`) the sqlglot-inferred SQL type; a `⚠ type change` marks a hop where the inferred type differs from the previous one, the fastest way to spot an unintended implicit cast before it reaches a downstream consumer. The `←` line underneath names the immediate source (table or upstream module) and the SQL op that produced this hop. Computed on demand at compile time (no Spark action, no store read); needs a Blueprint **file path**, not a blueprint id, since it re-parses and recompiles the YAML. `--format json` emits the same hops as structured records (`channel_id`, `output_column`, `source_table`, `source_column`, `output_type`, `transform_op`).

---

## Path resolution rule (1.1.0+)

Every relative path inside a YAML file resolves to **that YAML file's directory**, not the CWD of the `aqueduct` command. Matches Compose, k8s, and Terraform conventions. Anchored in:
- Blueprint module configs: `path`, `data_dir`, `input_dir`, `output_dir`, `jar`
- Engine config `stores.*.path`

URI-style values (`s3://`, `gs://`, `postgresql://`, `file://`, etc.) and already-absolute paths pass through unchanged. The on-disk YAML is never rewritten, only the in-memory compiled `Manifest`/config carry absolute paths. LLM context (the raw blueprint dict) is untouched.

Practical effect: running `aqueduct run subdir/bp.yml` from anywhere in the project finds the same CSV the blueprint declared.

---

## Sandbox modes (1.1.0+)

The sandbox gate replays a generated patch BEFORE applying it, to catch broken patches without writing to production. `agent.sandbox_mode` controls how the replay runs:

| Mode | Sample size | Egress writes | Danger gate | When to use |
|------|-------------|---------------|-------------|-------------|
| `sample` (default) | 1000 rows per Ingress | dropped | n/a | Fast confidence check; default for most projects |
| `preflight` | full dataset | dropped | `danger.allow_full_preflight: true` | Slow but conclusive: use when sample misses representative rows |
| `off` | (no replay) | next `execute()` writes for real | `danger.allow_skip_sandbox: true` | Skip pre-validation entirely. Patch hits real data immediately. **Use only on tiny, fully-trusted blueprints.** |

`approval` (who applies) and `sandbox_mode` (how to validate before apply) are orthogonal axes that compose:

| `approval` | Behaviour | `sandbox_mode` impact |
|---|---|---|
| `disabled` | No patching | N/A |
| `human` | Patch staged for manual review | Replay still runs (gives reviewer signal) |
| `ci` | Patch staged for CI | Replay still runs |
| `auto` | Auto-apply. `max_patches: 1` = single shot. `max_patches > 1` = multi-patch reprompt loop (requires `danger.allow_multi_patch: true`). | Replay gates apply every iteration |

`agent.approval` is the config key. Values: `disabled`, `human`, `auto`, `ci`.

**Double-danger combo**: `sandbox_mode: off` + `max_patches > 1` means every LLM patch hits production data without pre-validation, in a loop. Engine prints a `⚠ DANGER COMBO` line at startup when both are set; use only on tiny scopes you fully trust.

`agent.sandbox_mode` is a Blueprint-only policy field (`agent:` block in the Blueprint YAML) — there is no engine-wide `aqueduct.yml` default for it. Per-run `--set` override is a planned addition.

---

## 4. LLM self-healing & benchmarking

| Command | Description |
|---------|-------------|
| `aqueduct heal <run_id>` | Trigger self-healing on a failed run (the **reactive arm**, fix after a failure) |
| `aqueduct drift <blueprint>` | Detect upstream schema drift and pre-emptively heal it (the **proactive arm**, fix before a failure) |
| `aqueduct benchmark <path>` | Evaluate scenarios against models |
| `aqueduct benchmark-diff` | Compare benchmark results for regressions |
| `aqueduct benchmark-stats [path]` | Aggregate the store: model leaderboard, hardest scenarios, pass-rate trend |

**Key flags for `heal`:**

| Flag | Default | Description |
|------|---------|-------------|
| `--module <module_id>` | failed module from the run record | Scope healing to a specific module |
| `--print-prompt [text\|json]` | (bare flag = `text`) | Print the LLM prompt that would be sent and exit without calling the model |
| `--patches-dir <path>` | `patches` | Root directory for the patch lifecycle subdirs |
| `--store-dir <path>` | from `aqueduct.yml` (else `.aqueduct/`) | Override store directory |
| `--config <path>` | `./aqueduct.yml` walked upward | Path to `aqueduct.yml` |
| `-s` / `--set PATH=VALUE` | n/a | Override any config value for this heal invocation (repeatable, in-memory). See [Config overrides](#config-overrides--s--set) below. |

**Key flags for `drift`:**

`drift` is standalone and schedulable: run it on a cron *ahead* of the batch so
an upstream schema change is caught and a patch staged before the pipeline runs.
It reads each Ingress's live schema metadata-only (zero Spark actions), diffs
against a self-owned baseline (the `drift_checks` table, no Probe required), and
heals only **breaking** changes (dropped / type-changed columns); added columns
are benign and never trigger a heal.

| Flag | Default | Description |
|------|---------|-------------|
| `--module <module_id>` | all Ingress | Limit the check to one Ingress module |
| `--patches-dir <path>` | `patches` | Root directory for the patch lifecycle subdirs |
| `--store-dir <path>` | from config | Observability store directory |
| `--config <path>` | `./aqueduct.yml` walked upward | Path to `aqueduct.yml` |
| `--format text\|json` | `text` | Output shape |

Exit codes: `0` (no drift, or a baseline was established), `3` `HEAL_PENDING`
(a patch was staged), `2` `DATA_OR_RUNTIME` (a source could not be read/diffed).

**Key flags for `benchmark`:**

| Flag | Default | Description |
|------|---------|-------------|
| `--model <name>` | `agent.model` | Repeatable. Each value runs the suite against that model. (Stays, multi-model runs aren't expressible as `--set`.) |
| `-s` / `--set PATH=VALUE` | n/a | Override an `aqueduct.yml` value for this run (repeatable, in-memory). E.g. `--set agent.provider=openai_compat --set agent.base_url=http://h:11434/v1 --set agent.timeout=600`. |
| `--domain pipeline\|engine_config` | all | Repeatable. Run only scenarios whose `domains:` list includes one of these. A scenario matches when any of its declared domains is selected. An unknown value is a usage error naming the legal set. |
| `--workers <N>` | 1 | Parallel scenario×model pairs. Per-pair progress prints one line per completed pair (serial mode keeps the grouped multi-line view). |
| `--format table\|json` | `table` | |

`--domain` filters on what the scenario's expected FIX touches, not on what
failed: `pipeline` is a Blueprint edit (modules, config, edges),
`engine_config` is a `set_engine_config` write. A scenario may declare both,
because some failures are legitimately fixable either way. A scenario that
declares no `domains:` at all is excluded by any `--domain` filter and
reported by id, so a suite never shrinks without saying why. A `--domain` that
selects nothing exits `2` `DATA_OR_RUNTIME` rather than reporting success
after running zero pairs.

`--format json` carries two per-pair fields specific to config heals:
`refusal` (`policy` / `inert` / `guardrail` / `invalid`, or `null` when the
patch applied) and `engine_config_gate` (Gate 1's status, or `null` when that
gate never ran). Without them a failed pair reads only as `patch_applies:
false`, which covers four causes with four different fixes.

Every pair also carries `stop_reason` — which axis ended the heal, from the
`agent.budget` vocabulary in `aqueduct/agent/budget.py` (`solved`,
`exhausted_attempts`, `budget_seconds_exceeded`, `budget_tokens_exceeded`,
`stuck_signature`, `progress_stalled`, `api_error`, `deferred`; `null` only
when no reason was recorded). It is the difference between "the model could
not do it in the attempts it was given" and "our own budget stopped it": a
pair that shows `attempts_to_parse: 1` with
`stop_reason: budget_seconds_exceeded` was cut off by
`agent.budget.max_seconds`, not out-argued — the `--format table` view cannot
express that, and the LLM-call ceiling in the run banner is a ceiling, not a
promise.

The benchmark store backend is configured under `stores.benchmark` in `aqueduct.yml` (`backend: duckdb\|postgres`, `path`, `persist`, `gate_on_regression`), Postgres rows live in the `benchmark` schema. Override any of these per-run with `--set stores.benchmark.*`.

**Key flags for `benchmark-stats`:**

| Flag | Default | Description |
|------|---------|-------------|
| `[scenarios]` (positional) | `.` | Scenarios path: anchors the default DuckDB store location |
| `--store-path <path>` | from `stores.benchmark` | Read a specific store file directly |
| `-s` / `--set PATH=VALUE` | n/a | e.g. `--set stores.benchmark.backend=postgres --set stores.benchmark.path=postgresql://h/db` |
| `--format table\|json` | `table` | Leaderboard / hardest-scenarios / trend as text, or structured JSON |

Production heal and `aqueduct benchmark` share the same `agent.budget:`
block: divergence would let the leaderboard cheat by running under softer
caps than production.

> [!NOTE]
> When self-healing finishes, `stop_reason: "solved"` indicates only that the LLM returned a parseable PatchSpec and the loop terminated cleanly. It does **not** guarantee that the patch successfully fixed the pipeline at runtime. Downstream validation gates (like apply, lineage, sandbox, or explain) may still reject the patch. Cross-reference with `healing_outcomes.run_success_after_patch` to determine if the pipeline actually healed successfully.

---

## 5. Patch management

| Command | Description |
|---------|-------------|
| `aqueduct patch list` | Show pending/applied/rejected patches |
| `aqueduct patch policy [--engine <name>] [--format text\|json]` | Print the effective `set_engine_config` healing policy — allowed `engine.<name>` config keys (type + any enum/range) and denied key families (with `reason`) — read from each registered engine's core `engine_config_allowlist.yml` (the same table Gate 1 enforces). Default: every registered engine; `--engine` narrows to one (fails with a `USAGE_ERROR` if unregistered). Operator extension/narrowing of this policy is not yet implemented — this command prints the whole policy. |
| `aqueduct patch preview <file>` | Review changes and run gates. Renders the Blueprint diff, the lineage gate, the engine-config gate (the effective session-config delta the patch produces, or `not_applicable` when it writes no engine config), and with `--sandbox` the sandbox + explain gates. `aqueduct.yml` is loaded on every invocation, not only under `--sandbox`, because the engine-config gate compares against its `engine.<name>` layer; a config error exits `CONFIG_ERROR`. `--format json` adds an `engine_config` object (`status`, `detail`, `delta`, `write_targets`). |
| `aqueduct patch apply <file>` | Apply a patch |
| `aqueduct patch revert <patch_id> --blueprint <file> [--patches-dir <dir>] [--config <file>] [--dry-run] [--format text\|json]` | Undo an applied heal patch's engine-config writes, in place, without git. Restores every `engine.<name>` key the patch wrote to the value its `healed_by:` record captured before it was applied, then stamps that record `reverted_at:` (the record is kept, so the heal stays in the history). Backs the Blueprint up under `<patches-dir>/backups/` first. Only engine-config writes can be reverted: they are the only change for which a prior value is recorded. The command REFUSES, with the reason named and nothing written, when the patch also carries a non-config operation, when a later patch overwrote the same key (revert in reverse order, or use `rollback`), when the value has been edited since, or when the plan cannot be shown to reproduce the recorded pre-patch config. Exit `DATA_OR_RUNTIME` on refusal. `--dry-run` plans and verifies without writing. |
| `aqueduct patch import <file> --blueprint <bp> [--no-commit]` | Apply a received patch and `git commit` it in one step, the `approval: ci` entry point a CI runner calls after the `on_patch_pending` webhook. Equivalent to `apply` + `commit`; `--no-commit` stages only. See `docs/templates/ci-heal-workflow.yml`. |
| `aqueduct patch reject <file>` | Reject a patch |
| `aqueduct patch pull <id> --blueprint <file> [--out <dir>]` | Fetch a patch body from the object store (`stores.blob`) into a local checkout for review, for the cluster-heals/laptop-reviews flow when patches live on s3/gcs/adls. Writes `<out>/<id>.json` (default `<blueprint-dir>/patches/pending/`). |
| `aqueduct patch commit` | Git commit all applied patches |
| `aqueduct patch discard --blueprint <file>` | Restore Blueprint to last git commit (`git checkout HEAD`) and move uncommitted applied patches back to `patches/pending/` |
| `aqueduct patch log <blueprint> [--format table\|json]` | Show the Blueprint's git history with parsed Aqueduct patch metadata (patch ids, ops, run_id); manual edits show as `(manual change)` |
| `aqueduct patch rollback <blueprint> --to <patch_id>` | Restore the whole Blueprint file to its state before this patch_id's git commit, then forward-commit that restore. The git-based counterpart to `patch revert`: it undoes everything in that commit (including later-unrelated content of the same file) and needs the patch to have been committed, where `revert` undoes one patch's engine-config keys in place and needs no git at all. |

---

## 6. Store management

| Command | Description |
|---------|-------------|
| `aqueduct stores info` | Print each store's (observability / depots) resolved backend and location label |
| `aqueduct stores migrate --from-duckdb <file> [--store depot]` | Copy depot KV rows from a source DuckDB file into the configured target backend (Postgres/Redis). Idempotent. v1 migrates `depot` only. |

The target backend is read from `aqueduct.yml` (`stores.*`), set it to `postgres`/`redis` **before** running `migrate`. See [Production Guide](production_guide.md) for promoting a DuckDB project to a server backend.

---

## 7. Engine authoring (`aqueduct dev`)

Tools for writing an execution engine. An engine registers through the `aqueduct.engines` entry-point group and cannot register until its `capabilities.yml` gives an explicit verdict for every leaf on ITS OWN checklist (Spark's is 206 today: 189 Blueprint-grammar leaves + 17 engine-scoped `config.*` leaves — a `config.*` leaf that runs only in core code paths, e.g. webhooks/secrets/stores/lineage, is never on any engine's checklist at all; see [specs.md §10.9](specs.md) "Config-leaf scoping"). These commands generate and maintain that file.

| Command | Description |
|---------|-------------|
| `aqueduct dev capabilities scaffold --engine <name> [--out PATH] [--force]` | Write a complete `capabilities.yml` for a new engine — every leaf on THAT engine's own checklist present with verdict `undeclared`. Default output: the engine's package directory under `aqueduct/executor/`. Refuses to overwrite without `--force`. |
| `aqueduct dev capabilities sync [--no-prune]` | Append every newly-derived leaf to each engine's `capabilities.yml` as `undeclared`. Never invents a verdict. Prunes an orphaned row (no longer a real leaf on that engine's checklist) by default; `--no-prune` falls back to report-only. |
| `aqueduct dev capabilities check` | Report drift (missing / `undeclared` / orphaned rows) without writing. Exit `1` if any engine is incomplete. This is the CI gate. |
| `aqueduct dev capabilities docs [--out docs/compatibility.md]` | Regenerate the engine capability matrix between the `ENGINE_MATRIX_START` / `ENGINE_MATRIX_END` markers from the declarations. |
| `aqueduct dev scaffold <kind> [--name N] [--module M] [--out DIR] [--force]` | Generate an extension stub for one seam: `probe` (custom Probe signal), `assert` (custom Assert rule), `udf` (python UDF), `datasource` (Python DataSource, Spark 4.0+), `secrets` (custom resolver). Writes the `.py` stub and prints the config snippet that points at it. Stubs are generated from the live contracts (schema models, Assert enums, the installed pyspark DataSource, the resolver's annotated type), so they cannot drift from what the loader expects. |

The build stays red while any row is `undeclared`: it is a sentinel ("nobody has decided yet"), not a verdict, and registration raises `CapabilityDeclarationError` naming the leaves. Read Spark's `capabilities.yml` as a reference; copying it hands a new engine ~206 `supported` rows, a claim to implement the whole grammar.

---

## Exit codes

| Code | Name | Meaning |
|------|------|---------|
| `0` | SUCCESS | Command completed successfully |
| `1` | CONFIG_ERROR | Configuration or schema error |
| `2` | DATA_OR_RUNTIME | Runtime / Spark / data error |
| `3` | HEAL_PENDING | Patch staged for human review |
| `4` | VALIDATION_GATE | Patch rejected by validation |
| `5` | USAGE_ERROR | Invalid command usage |

Note: Click's own usage errors (unknown flag, missing required argument) exit `2`, not `5` — Click does not know this taxonomy. Code `5` is reachable only from an Aqueduct-detected usage mistake that a command raises explicitly.

---

**Tip:** Most common operations have rich built-in help. Try `aqueduct <command> --help`.