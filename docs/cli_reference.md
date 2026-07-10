# Aqueduct CLI Reference

All commands accept `--config <path>` to point to a non-default `aqueduct.yml`. Aqueduct also automatically walks up from the current working directory to find it.

Bare `aqueduct` (no subcommand) prints a branded version banner including the engine version, Python version, and Spark version (if available).

## Global Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--version` | — | Print the installed `aqueduct-core` version and exit |
| `-v`, `--verbose` | off | Enable DEBUG logging (LLM prompts, SQL plans, etc.) |
| `--log-format text\|json` | `text` | Output format for logs |
| `--suppress-warning <id>` | — | Silence one `AQ-WARN [<id>]` rule. Repeatable. Use `'*'` to silence all. Applied BEFORE the subcommand. |

**Output control flags:**

| Flag | Default | Purpose | Used by |
|------|---------|---------|---------|
| `--format table\|json\|csv` | `table` | Result data shape | `runs`, `report`, `benchmark`, etc. |
| `--format html` | — | `report` only — self-contained single-file HTML run report (to stdout; redirect to a file) | `report` |
| `-o`, `--output <path>` | `-` (stdout) | Write output to file | `compile`, `schema` |
| `--show manifest\|provenance\|inputs\|all` | `manifest` | Choose what to display | `compile` |

---

## Validate vs Doctor

- **`aqueduct validate`** — Fast, static validation (schema + parsing). Ideal for CI/pre-commit. For a Blueprint with a `hooks:` block, it also runs the same static hook-graph walk `doctor` uses (cycles / chain-depth overflow / missing `blueprint:` targets) and reports problems as a suppressible **warning** (`[aqueduct:hook_cycle]`, added to `warnings.suppress`) — never a validation failure.
- **`aqueduct doctor`** — Live connectivity checks (Spark, stores, agent, sources). Use before deploying.

Both commands auto-detect file type based on the version header (`aqueduct:` vs `aqueduct_config:`).

### `.env` Resolution

Aqueduct automatically loads `.env` from the directory of the config or blueprint file (not CWD). You can override with:
- `-e KEY=VAL` (highest precedence, repeatable)
- `--env-file <path>`
- `AQ_NO_ENV_FILE=1` to disable entirely

---

## 1. Project Setup

| Command | Description |
|---------|-------------|
| `aqueduct init` | Create a new project skeleton with templates, directories, and `.gitignore` |
| `aqueduct doctor` | Check connectivity and configuration health |
| `aqueduct doctor <file>` | Validate a specific blueprint or config file |
| *(webhook check depth)* | Each configured `webhooks.*` endpoint's `health_probe:` field (`connect`/`options`/`full`, default `options`) controls how `doctor` probes it — see [Production Guide](production_guide.md) |
| `aqueduct doctor --skip-spark` | Fast check without starting Spark |
| `aqueduct doctor --preflight` | Full Spark session + storage validation. Also: verifies cloud Ingress/Egress objects (`s3a://`/`gs://`/`abfss://`) exist via Spark's Hadoop FileSystem; warns on a **Spark major.minor** vs client-pyspark mismatch; for `agent.provider: anthropic` proves the API key works (`GET /v1/models`, no tokens); **imports** each Python `udf_registry` entry (catches typos/missing deps); does a store **write+read** round-trip (write perms, not just connect); and for `jdbc:` sources attempts a real connect+auth (postgres via psycopg2). Default `doctor` only checks endpoint reachability. A standalone **Java** runtime check (detected JVM version + a pyspark-4-needs-Java-17 nudge) runs even without `--preflight`. |
| `aqueduct doctor --aqtest <file>` | Schema pre-flight on a `.aqtest.yml` (verifies blueprint ref + module IDs) |
| `aqueduct doctor --aqscenario <file>` | Schema pre-flight on a `.aqscenario.yml` (verifies blueprint ref + `inject_failure.module`) |
| `aqueduct doctor -v, --verbose` | Also show skipped checks (not-applicable / not-configured), not just the collapsed summary |
| `aqueduct doctor --format json` | Machine-readable result of every check (`{schema_version, summary, checks[]}`); implies `--verbose` (nothing collapsed). Text mode groups checks into sections (Config, Stores, Spark, …). |
| `aqueduct completion {bash\|zsh\|fish}` | Emit a shell-completion script for installation |

### Shell completion

`aqueduct completion` generates a completion script for `bash`, `zsh`, or `fish` from the live click command tree — new flags pick up automatically; rerun after upgrading Aqueduct.

```bash
aqueduct completion bash > /etc/bash_completion.d/aqueduct.sh
aqueduct completion zsh  > /usr/local/share/zsh/site-functions/_aqueduct
aqueduct completion fish > ~/.config/fish/completions/aqueduct.fish
```

---

## 2. Development Loop

| Command | Description |
|---------|-------------|
| `aqueduct validate <file>...` | Static validation of blueprints/configs |
| `aqueduct validate <file>... --format json` | Same checks, machine-readable (`{schema_version, summary, files[]}`) for CI |
| `aqueduct lint <blueprint>` | Static style + correctness checks beyond schema validation (AQ-LINT rules) |
| `aqueduct lint <blueprint> --strict` | Promote every finding to error — exit non-zero on any finding (CI gate) |
| `aqueduct lint <blueprint> --format json` | Machine-readable findings (`{schema_version, summary, findings[]}`) |
| `aqueduct compile <blueprint>` | Output the fully resolved Manifest |
| `aqueduct run <blueprint>` | Compile and execute the pipeline |
| `aqueduct test <file.aqtest.yml>` | Run isolated module unit tests |
| `aqueduct schema [--target blueprint\|config\|patch] [-o <file>]` | Emit the Pydantic-derived JSON Schema for a Blueprint, `aqueduct.yml`, or PatchSpec — enables IDE autocomplete and CI schema gates. Writes to stdout by default. |

### `aqueduct lint` rules

`lint` runs after a successful parse and reports static smells the schema permits. Each rule has a stable `AQ-LINT<NNN>` id and a severity. All initial rules are advisory (`warn`) — a warn-only result exits `0`; `--strict` promotes findings to errors so a non-empty result exits `1` (`CONFIG_ERROR`), for CI gating. SQL rules parse Channel `op: sql` queries with sqlglot (`dialect="spark"`); unparseable SQL is skipped, never errored.

| Rule | Severity | Flags |
|------|----------|-------|
| `AQ-LINT001` | warn | Orphan module — not referenced by any edge, `depends_on`, `spillway`, or `attach_to` |
| `AQ-LINT002` | warn | Module label is empty or just repeats its `id` |
| `AQ-LINT003` | warn | Duplicate edge — same `(from, to, port)` declared more than once |
| `AQ-LINT004` | warn | Un-aliased self-join — a relation referenced 2+ times without distinct aliases |
| `AQ-LINT010` | warn | Cartesian join — `JOIN` with no `ON`/`USING` (explicit `CROSS JOIN` is allowed) |
| `AQ-LINT011` | warn | `SELECT *` in a Channel that feeds directly into an Egress (silent schema drift) |
| `AQ-LINT012` | warn | Aggregate function mixed with a non-aggregated column and no `GROUP BY` |

### Important `aqueduct run` Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--run-id <uuid>` | auto-generated | User-supplied run id (otherwise UUID4) |
| `--from <module_id>` | — | Start execution at this module |
| `--to <module_id>` | — | Stop execution after this module |
| `--execution-date YYYY-MM-DD` | today (UTC) | Logical date for `@aq.date.*` — enables idempotent backfills |
| `--resume <run_id>` | — | Resume from checkpoints of a previous run |
| `--parallel` | off | Execute independent DAG branches concurrently (one thread per connected component) |
| `--sandbox` | off | Dev dry-run: execute against sampled inputs with every Egress skipped (no writes, no self-healing, no observability persistence). Fast feedback loop for iterating on transforms. Requires `engine: spark`. |
| `--sample <N>` | `1000` | Row cap per Ingress in `--sandbox` mode (`0` = no limit). Ignored without `--sandbox`. |
| `-s` / `--set PATH=VALUE` | — | Override any config or blueprint value for this run only (repeatable, in-memory, never persisted). See [Config overrides](#config-overrides--s--set) below. |
| `--ctx KEY=VALUE` | — | Override a Tier 0 context variable. Repeatable. Environments that can't pass CLI args (CI, Airflow) can set `AQUEDUCT_CTX_<KEY>` env vars instead — top-level keys only, one priority step below `--ctx` (see specs.md §5.2). |
| `--profile <name>` | — | Activate a `context_profiles:` block |
| `--store-dir <path>` | from `aqueduct.yml` (else `.aqueduct/`) | Override store directory for this run |
| `--webhook <url>` | from `aqueduct.yml` | Override failure webhook |
| `--allow-multi-patch` | off | Permit `max_patches > 1` for this run (overrides `danger.allow_multi_patch=false`). |
| `--config <path>` | `./aqueduct.yml` walked upward | Path to `aqueduct.yml` |
| `-e KEY=VAL` / `--env KEY=VAL` | — | Inline env override (highest precedence). Repeatable. |
| `--env-file <path>` | anchored `<dir>/.env` | Explicit fallback `.env` (used only when no anchored project `.env` exists) |

### Run output: runtime warning summary

Runtime warnings raised *during* execution (Probe/Assert findings, retry notices, and other per-module diagnostics) are shown twice: inline under the module that raised them (`↳ [rule_id] …` — no `⚠` icon; the roll-up header carries it) and again as a collapsed roll-up just before the run footer:

```
⚠ runtime: 3 warnings   ·  -v for full text
  · [runtime_assert]         clean_users: Assert [not_null]: 12 null emails
  · [runtime_probe_signal_error]  ingest_orders: signal evaluation failed
  · [runtime_retry_waiting]  write_warehouse: retrying egress (attempt 2/3)
```

Each line keeps its stable `rule_id` (e.g. `runtime_assert`, `runtime_probe_*`, `runtime_retry_*`) so it can be copied straight into `warnings.suppress` in `aqueduct.yml` (same mechanism as compile-time `AQ-WARN` ids). The roll-up is additive — inline per-module warnings still print. Pass `-v` / `--verbose` for the full (untruncated) warning text.

### Run output: Arcade tree view

Arcade-expanded modules nest under their Arcade in the summary block — the parent row shows the worst child status (any ✗ → ✗, else any ✓ → ✓, else ⏭):

```
  ✓ raw_tickets              1.8 s
  ✗ arcade_conditional
    ├─ ✓ save_active         4 rows  ·  626 ms
    ├─ ✗ save_other          — source not found at 'data/other'
    └─ ⏭ notify_export
```

The nesting is display-only. Logs, the observability store, and the `failed_module=` footer keep the full flattened id (`arcade_conditional__save_other`), so copy-pasting ids into `--from`/`--to`, `report`, or SQL against `run_records` works unchanged.

### Run output: lifecycle hooks

When the Blueprint declares `hooks:` (see specs.md §4.2), the matching event's entries run after the terminal footer, each with a `✓/⚠` line; a chained `blueprint:` hook streams its own full run output inline (it is a fresh `aqueduct run` subprocess). The section closes with a final `✓ run complete`:

```
✓ blueprint complete
· hooks  ·  on_success (2)
  ✓ scripts/commit_outputs.sh r-1234    1.2 s
  ✓ aqueduct run downstream.yml    4m 02s
✓ run complete
```

Hook outcomes never change the run's exit code. `command:` entries require `danger.allow_command_hooks: true` in `aqueduct.yml` (skipped with `[hook_command_disabled]` otherwise); cyclic `blueprint:` chains are refused with `[hook_cycle]` (`aqueduct doctor` checks the chain statically).

### Config overrides (`-s` / `--set`)

`--set PATH=VALUE` overrides any value in `aqueduct.yml` or the Blueprint for a single invocation — repeatable, applied in memory, **never written back to disk**. It is the highest-precedence layer:

```
--set  >  blueprint agent:  >  aqueduct.yml  >  built-in defaults
```

One flat dotted namespace addresses whichever schema owns the field. For `aqueduct run`, an `agent.*` path that the Blueprint schema declares (e.g. `agent.approval`, `agent.timeout`) lands on the Blueprint (which already wins the merge); engine-only agent fields (`agent.budget.*`, `agent.retry.*`) and everything else (`deployment.*`, `danger.*`, `stores.*`) land on `aqueduct.yml`. A path no schema declares is an error with a nearest-sibling suggestion.

> **Precedence is per-key, and a cascade tier's own fields are separate keys.** `--set` wins among the *sources* for the key it targets (`--set agent.timeout` beats the blueprint's and `aqueduct.yml`'s `agent.timeout`). But in a **cascade** (`agent.cascade:`), each tier's `timeout` / `max_reprompts` / `provider` / … are *their own keys* that only inherit the flat `agent.*` value **when the tier leaves them unset**. So `--set agent.timeout=600` raises the solo/flat default and any tier that inherits it — but it does **not** override a tier that declares its own `timeout:` (that is a different key, and the tier's explicit value is intentional). To change one tier, set that tier's field in the blueprint's `agent.cascade:` block. (A per-tier `--set agent.cascade[N].timeout` addressing form is on the roadmap; see `TODOs.md`.)

Value grammar:
- `PATH=value` — coerced: `true`/`false` → bool, `null`/`none` → None, then int, then float, else the literal string.
- `PATH:=value` — `value` parsed as JSON, for structured values (objects/arrays/typed scalars).

```bash
aqueduct run bp.yml \
  --set agent.approval=auto \
  --set agent.budget.max_seconds=5 \
  --set agent.budget.max_tokens_total=80000 \
  --set deployment.master_url=spark://10.0.0.39:7077 \
  --set agent.provider_options:='{"temperature":0.1}'
```

`--set danger.*` overrides print a loud stderr warning (single-run, not persisted). Available on `run`, `benchmark`, and `heal`. `--set` replaced the one-off override flags (`--provider`/`--base-url`/`--timeout`/`--no-persist`/`--store-path`/`--gate-on-regression`), which were removed in 2.0.

---

## 3. Observability

| Command | Description |
|---------|-------------|
| `aqueduct runs` | List recent runs |
| `aqueduct runs --failed` | Show only failed runs |
| `aqueduct runs --heal-coverage` | Zero-token heal coverage (heals resolved by the signature memory cache vs the LLM) |
| `aqueduct runs --format text\|json` | `text\|json` only — the global `table\|json\|csv` does not apply to `runs` |
| `aqueduct report <run_id>` | Detailed flow report for a run |
| `aqueduct report --trend <column> --blueprint <id>` | Cross-run quality trend for one column (null-rate + type history) from probe signals; `--since <ISO_DATE>` windows it (default 30 days) |
| `aqueduct report <run_id> --profile` | Per-module resource profile for one run (duration + I/O over `module_metrics`), heaviest module first, with each module's share of run time/bytes |
| `aqueduct report --profile --blueprint <id> [--last N]` | Cross-run resource trend per module over the last N runs (default 10): runs count, avg/max/last duration, flags a module whose latest run is >1.5× its window average as a slowdown |
| `aqueduct report <run_id> --format html > run.html` | Self-contained single-file HTML run report (status, module results, resource profile); no server, renders offline |
| `aqueduct lineage <blueprint>` | Column-level lineage graph |
| `aqueduct lineage <blueprint.yml> --chain <column> [--types]` | Vertical source→output trace for one column; `--types` annotates each hop with the sqlglot-inferred SQL type and marks type changes (computed on demand from the blueprint; needs a file path, not an id) |
| `aqueduct signal <signal_id> --blueprint <id>` | View or override Probe gates. `--blueprint` is required with the duckdb backend (unless `--store-dir` is given) — the override lives in that blueprint's routed store, `<base>/<blueprint_id>/observability.db`; ignored for postgres (one shared schema) |
| `aqueduct studio [--config <f>] [--store-dir <d>]` | Launch the interactive read-only TUI (runs, ad-hoc SQL over the observability store, doctor, config, lineage). Requires the optional `tui` extra: `pip install aqueduct-core[tui]` |
| `aqueduct dashboard [--config <f>] [--store-dir <d>] [--port 8501] [--no-browser]` | Launch the local, read-only **Streamlit** observability dashboard: fleet view (cross-blueprint runs / success-rate / heal-rate, trends), per-run module metrics, column-lineage Sankey, doctor, config. On-demand local viewer (like the Spark UI) — never a production server. Requires the optional `dashboard` extra: `pip install aqueduct-core[dashboard]`. A 🔄 Refresh button re-reads the store (manual; no background polling). |

**`--chain --types` example** — tracing one column's per-hop transform, source to output:

```
$ aqueduct lineage pipelines/orders.yml --chain total_amount --types
Column chain — blueprint: orders_pipeline  column: total_amount

  ▸ apply_discount.total_amount  :: DECIMAL(10,2)
      ← read_orders.total_amount  [passthrough]
  │
  ▸ cast_to_float.total_amount  :: DOUBLE  ⚠ type change
  │    ← apply_discount.total_amount  [CAST]
```

Each `▸` line is one hop — the Channel module and output column, plus (with `--types`) the sqlglot-inferred SQL type; a `⚠ type change` marks a hop where the inferred type differs from the previous one, the fastest way to spot an unintended implicit cast before it reaches a downstream consumer. The `←` line underneath names the immediate source (table or upstream module) and the SQL op that produced this hop. Computed on demand at compile time (no Spark action, no store read); needs a Blueprint **file path**, not a blueprint id, since it re-parses and recompiles the YAML. `--format json` emits the same hops as structured records (`channel_id`, `output_column`, `source_table`, `source_column`, `output_type`, `transform_op`).

---

## Path Resolution Rule (1.1.0+)

Every relative path inside a YAML file resolves to **that YAML file's directory**, not the CWD of the `aqueduct` command. Matches Compose, k8s, and Terraform conventions. Anchored in:
- Blueprint module configs: `path`, `data_dir`, `input_dir`, `output_dir`, `jar`
- Engine config `stores.*.path`

URI-style values (`s3://`, `gs://`, `postgresql://`, `file://`, etc.) and already-absolute paths pass through unchanged. The on-disk YAML is never rewritten — only the in-memory compiled `Manifest`/config carry absolute paths. LLM context (the raw blueprint dict) is untouched.

Practical effect: running `aqueduct run subdir/bp.yml` from anywhere in the project finds the same CSV the blueprint declared.

---

## Sandbox Modes (1.1.0+)

The sandbox gate replays a generated patch BEFORE applying it, to catch broken patches without writing to production. `agent.sandbox_mode` controls how the replay runs:

| Mode | Sample size | Egress writes | Danger gate | When to use |
|------|-------------|---------------|-------------|-------------|
| `sample` (default) | 1000 rows per Ingress | dropped | — | Fast confidence check; default for most projects |
| `preflight` | full dataset | dropped | `danger.allow_full_preflight: true` | Slow but conclusive — use when sample misses representative rows |
| `off` | — (no replay) | next `execute()` writes for real | `danger.allow_skip_sandbox: true` | Skip pre-validation entirely. Patch hits real data immediately. **Use only on tiny, fully-trusted blueprints.** |

`approval` (who applies) and `sandbox_mode` (how to validate before apply) are orthogonal axes that compose:

| `approval` | Behaviour | `sandbox_mode` impact |
|---|---|---|
| `disabled` | No patching | N/A |
| `human` | Patch staged for manual review | Replay still runs (gives reviewer signal) |
| `ci` | Patch staged for CI | Replay still runs |
| `auto` | Auto-apply. `max_patches: 1` = single shot. `max_patches > 1` = multi-patch reprompt loop (requires `danger.allow_multi_patch: true`). | Replay gates apply every iteration |

`agent.approval` is the config key. Values: `disabled`, `human`, `auto`, `ci`.

**Double-danger combo** — `sandbox_mode: off` + `max_patches > 1` means every LLM patch hits production data without pre-validation, in a loop. Engine prints a `⚠ DANGER COMBO` line at startup when both are set; use only on tiny scopes you fully trust.

Configure per engine (`agent.sandbox_mode:` in `aqueduct.yml`) or per blueprint (`agent.sandbox_mode:` in the blueprint's `agent:` block — Blueprint value wins). Per-run `--set` override is a planned addition.

---

## 4. LLM Self-Healing & Benchmarking

| Command | Description |
|---------|-------------|
| `aqueduct heal <run_id>` | Trigger self-healing on a failed run (the **reactive arm** — fix after a failure) |
| `aqueduct drift <blueprint>` | Detect upstream schema drift and pre-emptively heal it (the **proactive arm** — fix before a failure) |
| `aqueduct benchmark <path>` | Evaluate scenarios against models |
| `aqueduct benchmark-diff` | Compare benchmark results for regressions |
| `aqueduct benchmark-stats [path]` | Aggregate the store: model leaderboard, hardest scenarios, pass-rate trend |

**Key flags for `heal`:**

| Flag | Default | Description |
|------|---------|-------------|
| `--module <module_id>` | failed module from the run record | Scope healing to a specific module |
| `--print-prompt [text\|json]` | — (bare flag = `text`) | Print the LLM prompt that would be sent and exit without calling the model |
| `--patches-dir <path>` | `patches` | Root directory for the patch lifecycle subdirs |
| `--store-dir <path>` | from `aqueduct.yml` (else `.aqueduct/`) | Override store directory |
| `--config <path>` | `./aqueduct.yml` walked upward | Path to `aqueduct.yml` |
| `-s` / `--set PATH=VALUE` | — | Override any config value for this heal invocation (repeatable, in-memory). See [Config overrides](#config-overrides--s--set) below. |

**Key flags for `drift`:**

`drift` is standalone and schedulable — run it on a cron *ahead* of the batch so
an upstream schema change is caught and a patch staged before the pipeline runs.
It reads each Ingress's live schema metadata-only (zero Spark actions), diffs
against a self-owned baseline (the `drift_checks` table — no Probe required), and
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
| `--model <name>` | `agent.model` | Repeatable. Each value runs the suite against that model. (Stays — multi-model runs aren't expressible as `--set`.) |
| `-s` / `--set PATH=VALUE` | — | Override an `aqueduct.yml` value for this run (repeatable, in-memory). E.g. `--set agent.provider=openai_compat --set agent.base_url=http://h:11434/v1 --set agent.timeout=600`. |
| `--workers <N>` | 1 | Parallel scenario×model pairs. Per-pair progress prints one line per completed pair (serial mode keeps the grouped multi-line view). |
| `--format table\|json` | `table` | |

The benchmark store backend is configured under `stores.benchmark` in `aqueduct.yml` (`backend: duckdb\|postgres`, `path`, `persist`, `gate_on_regression`) — Postgres rows live in the `benchmark` schema. Override any of these per-run with `--set stores.benchmark.*`.

**Key flags for `benchmark-stats`:**

| Flag | Default | Description |
|------|---------|-------------|
| `[scenarios]` (positional) | `.` | Scenarios path — anchors the default DuckDB store location |
| `--store-path <path>` | from `stores.benchmark` | Read a specific store file directly |
| `-s` / `--set PATH=VALUE` | — | e.g. `--set stores.benchmark.backend=postgres --set stores.benchmark.path=postgresql://h/db` |
| `--format table\|json` | `table` | Leaderboard / hardest-scenarios / trend as text, or structured JSON |

Production heal and `aqueduct benchmark` share the same `agent.budget:`
block — divergence would let the leaderboard cheat by running under softer
caps than production.

> [!NOTE]
> When self-healing finishes, `stop_reason: "solved"` indicates only that the LLM returned a parseable PatchSpec and the loop terminated cleanly. It does **not** guarantee that the patch successfully fixed the pipeline at runtime. Downstream validation gates (like apply, lineage, sandbox, or explain) may still reject the patch. Cross-reference with `healing_outcomes.run_success_after_patch` to determine if the pipeline actually healed successfully.

---

## 5. Patch Management

| Command | Description |
|---------|-------------|
| `aqueduct patch list` | Show pending/applied/rejected patches |
| `aqueduct patch preview <file>` | Review changes and run gates |
| `aqueduct patch apply <file>` | Apply a patch |
| `aqueduct patch import <file> --blueprint <bp> [--no-commit]` | Apply a received patch and `git commit` it in one step — the `approval: ci` entry point a CI runner calls after the `on_patch_pending` webhook. Equivalent to `apply` + `commit`; `--no-commit` stages only. See `docs/templates/ci-heal-workflow.yml`. |
| `aqueduct patch reject <file>` | Reject a patch |
| `aqueduct patch pull <id> --blueprint <file> [--out <dir>]` | Fetch a patch body from the object store (`stores.blob`) into a local checkout for review — for the cluster-heals/laptop-reviews flow when patches live on s3/gcs/adls. Writes `<out>/<id>.json` (default `<blueprint-dir>/patches/pending/`). |
| `aqueduct patch commit` | Git commit all applied patches |
| `aqueduct patch discard --blueprint <file>` | Restore Blueprint to last git commit (`git checkout HEAD`) and move uncommitted applied patches back to `patches/pending/` |
| `aqueduct patch log <blueprint> [--format table\|json]` | Show the Blueprint's git history with parsed Aqueduct patch metadata (patch ids, ops, run_id); manual edits show as `(manual change)` |
| `aqueduct patch rollback <blueprint> --to <patch_id>` | Revert the git commit containing this patch_id |

---

## 6. Store Management

| Command | Description |
|---------|-------------|
| `aqueduct stores info` | Print each store's (observability / depots) resolved backend and location label |
| `aqueduct stores migrate --from-duckdb <file> [--store depot]` | Copy depot KV rows from a source DuckDB file into the configured target backend (Postgres/Redis). Idempotent. v1 migrates `depot` only. |

The target backend is read from `aqueduct.yml` (`stores.*`) — set it to `postgres`/`redis` **before** running `migrate`. See [Production Guide](production_guide.md) for promoting a DuckDB project to a server backend.

---

## Exit Codes

| Code | Name | Meaning |
|------|------|---------|
| `0` | SUCCESS | Command completed successfully |
| `1` | CONFIG_ERROR | Configuration or schema error |
| `2` | DATA_OR_RUNTIME | Runtime / Spark / data error |
| `3` | HEAL_PENDING | Patch staged for human review |
| `4` | VALIDATION_GATE | Patch rejected by validation |
| `5` | USAGE_ERROR | Invalid command usage |

---

**Tip:** Most common operations have rich built-in help. Try `aqueduct <command> --help`.