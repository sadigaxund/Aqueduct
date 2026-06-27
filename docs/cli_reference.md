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

- **`aqueduct validate`** — Fast, static validation (schema + parsing). Ideal for CI/pre-commit.
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
| `aqueduct doctor --skip-spark` | Fast check without starting Spark |
| `aqueduct doctor --preflight` | Full Spark session + storage validation; also verifies cloud Ingress/Egress objects (`s3a://`/`gs://`/`abfss://`) exist via Spark's Hadoop FileSystem (default `doctor` only checks endpoint reachability) |
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
| `--ctx KEY=VALUE` | — | Override a Tier 0 context variable. Repeatable. |
| `--profile <name>` | — | Activate a `context_profiles:` block |
| `--store-dir <path>` | from `aqueduct.yml` (else `.aqueduct/`) | Override store directory for this run |
| `--webhook <url>` | from `aqueduct.yml` | Override failure webhook |
| `--allow-multi-patch` | off | Permit `max_patches > 1` for this run (overrides `danger.allow_multi_patch=false`). |
| `--config <path>` | `./aqueduct.yml` walked upward | Path to `aqueduct.yml` |
| `-e KEY=VAL` / `--env KEY=VAL` | — | Inline env override (highest precedence). Repeatable. |
| `--env-file <path>` | anchored `<dir>/.env` | Explicit fallback `.env` (used only when no anchored project `.env` exists) |

### Config overrides (`-s` / `--set`)

`--set PATH=VALUE` overrides any value in `aqueduct.yml` or the Blueprint for a single invocation — repeatable, applied in memory, **never written back to disk**. It is the highest-precedence layer:

```
--set  >  blueprint agent:  >  aqueduct.yml  >  built-in defaults
```

One flat dotted namespace addresses whichever schema owns the field. For `aqueduct run`, an `agent.*` path that the Blueprint schema declares (e.g. `agent.approval`, `agent.timeout`) lands on the Blueprint (which already wins the merge); engine-only agent fields (`agent.budget.*`, `agent.retry.*`) and everything else (`deployment.*`, `danger.*`, `stores.*`) land on `aqueduct.yml`. A path no schema declares is an error with a nearest-sibling suggestion.

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

`--set danger.*` overrides print a loud stderr warning (single-run, not persisted). Available on `run`, `benchmark`, and `heal`. `--set` replaces the deprecated one-off override flags (`--provider`, `--base-url`, `--timeout`).

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
| `aqueduct signal <signal_id>` | View or override Probe gates |
| `aqueduct studio [--config <f>] [--store-dir <d>]` | Launch the interactive read-only TUI (runs, ad-hoc SQL over the observability store, doctor, config, lineage). Requires the optional `tui` extra: `pip install aqueduct-core[tui]` |
| `aqueduct dashboard [--config <f>] [--store-dir <d>] [--port 8501] [--no-browser]` | Launch the local, read-only **Streamlit** observability dashboard: fleet view (cross-blueprint runs / success-rate / heal-rate, trends), per-run module metrics, column-lineage Sankey, doctor, config. On-demand local viewer (like the Spark UI) — never a production server. Requires the optional `dashboard` extra: `pip install aqueduct-core[dashboard]`. A 🔄 Refresh button re-reads the store (manual; no background polling). |

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

`agent.approval` is the config key (values: `disabled` | `human` | `auto` | `aggressive` | `ci`). The former `agent.approval_mode` key, the `aggressive_max_patches` alias, the `danger.allow_aggressive_patching` alias, and the `--allow-aggressive` flag were removed in 2.0 — use `agent.approval`, `agent.max_patches`, `danger.allow_multi_patch`, and `--allow-multi-patch`.

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
| `--provider anthropic\|openai_compat` | `agent.provider` | **Deprecated** → `--set agent.provider=…` (removed in 2.0) |
| `--base-url <url>` | `agent.base_url` | **Deprecated** → `--set agent.base_url=…` (removed in 2.0) |
| `--timeout <seconds>` | `agent.timeout` (300) | **Deprecated** → `--set agent.timeout=…` (removed in 2.0) |
| `--workers <N>` | 1 | Parallel scenario×model pairs. Per-pair progress prints one line per completed pair (serial mode keeps the grouped multi-line view). |
| `--format table\|json` | `table` | |
| `--no-persist` | from `stores.benchmark.persist` (true) | **Deprecated** → `--set stores.benchmark.persist=false` (removed in 2.0) |
| `--store-path <path>` | from `stores.benchmark.path` (else `<scenarios_dir>/.aqueduct/benchmark.duckdb`) | **Deprecated** → `--set stores.benchmark.path=…` (removed in 2.0) |
| `--gate-on-regression` | from `stores.benchmark.gate_on_regression` (false) | **Deprecated** → `--set stores.benchmark.gate_on_regression=true` (removed in 2.0) |

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
| `aqueduct stores info` | Print each store's (observability / lineage / depot) resolved backend and location label |
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