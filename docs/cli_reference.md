# Aqueduct CLI Reference

All commands accept `--config <path>` to point to a non-default `aqueduct.yml`. Aqueduct also automatically walks up from the current working directory to find it.

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
| `aqueduct doctor --preflight` | Full Spark session + storage validation |

---

## 2. Development Loop

| Command | Description |
|---------|-------------|
| `aqueduct validate <file>...` | Static validation of blueprints/configs |
| `aqueduct compile <blueprint>` | Output the fully resolved Manifest |
| `aqueduct run <blueprint>` | Compile and execute the pipeline |
| `aqueduct test <file.aqtest.yml>` | Run isolated module unit tests |

### Important `aqueduct run` Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--run-id <uuid>` | auto-generated | User-supplied run id (otherwise UUID4) |
| `--from <module_id>` | — | Start execution at this module |
| `--to <module_id>` | — | Stop execution after this module |
| `--execution-date YYYY-MM-DD` | today (UTC) | Logical date for `@aq.date.*` — enables idempotent backfills |
| `--resume <run_id>` | — | Resume from checkpoints of a previous run |
| `--parallel` | off | Execute independent DAG branches concurrently (one thread per connected component) |
| `--ctx KEY=VALUE` | — | Override a Tier 0 context variable. Repeatable. |
| `--profile <name>` | — | Activate a `context_profiles:` block |
| `--store-dir <path>` | from `aqueduct.yml` (else `.aqueduct/`) | Override store directory for this run |
| `--webhook <url>` | from `aqueduct.yml` | Override failure webhook |
| `--allow-multi-patch` / `--allow-aggressive` | off | Permit `max_patches > 1` for this run (overrides `danger.allow_multi_patch=false`). `--allow-aggressive` is a deprecated alias. |
| `--config <path>` | `./aqueduct.yml` walked upward | Path to `aqueduct.yml` |
| `-e KEY=VAL` / `--env KEY=VAL` | — | Inline env override (highest precedence). Repeatable. |
| `--env-file <path>` | anchored `<dir>/.env` | Explicit fallback `.env` (used only when no anchored project `.env` exists) |

---

## 3. Observability

| Command | Description |
|---------|-------------|
| `aqueduct runs` | List recent runs |
| `aqueduct runs --failed` | Show only failed runs |
| `aqueduct report <run_id>` | Detailed flow report for a run |
| `aqueduct lineage <blueprint>` | Column-level lineage graph |
| `aqueduct signal <signal_id>` | View or override Probe gates |

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

`approval_mode` (who applies) and `sandbox_mode` (how to validate before apply) are orthogonal axes that compose:

| `approval_mode` | Behaviour | `sandbox_mode` impact |
|---|---|---|
| `disabled` | No patching | N/A |
| `human` | Patch staged for manual review | Replay still runs (gives reviewer signal) |
| `ci` | Patch staged for CI | Replay still runs |
| `auto` | Auto-apply. `max_patches: 1` = single shot. `max_patches > 1` = multi-patch reprompt loop (requires `danger.allow_multi_patch: true`). | Replay gates apply every iteration |

`approval_mode: aggressive` is a deprecated alias for `auto` + `max_patches > 1`; it still parses and emits a `[deprecated]` warning. `aggressive_max_patches` is an alias for `max_patches`. Both are slated for removal in `aqueduct: "2.0"` schema.

**Double-danger combo** — `sandbox_mode: off` + `max_patches > 1` means every LLM patch hits production data without pre-validation, in a loop. Engine prints a `⚠ DANGER COMBO` line at startup when both are set; use only on tiny scopes you fully trust.

Configure per engine (`agent.sandbox_mode:` in `aqueduct.yml`) or per blueprint (`agent.sandbox_mode:` in the blueprint's `agent:` block — Blueprint value wins). Per-run `--set` override is a planned addition.

---

## 4. LLM Self-Healing & Benchmarking

| Command | Description |
|---------|-------------|
| `aqueduct heal <run_id>` | Trigger self-healing on a failed run |
| `aqueduct benchmark <path>` | Evaluate scenarios against models |
| `aqueduct benchmark-diff` | Compare benchmark results for regressions |

**Key flags for `benchmark`:**

| Flag | Default | Description |
|------|---------|-------------|
| `--model <name>` | `agent.model` | Repeatable. Each value runs the suite against that model. |
| `--provider anthropic\|openai_compat` | `agent.provider` | One-shot override |
| `--base-url <url>` | `agent.base_url` | One-shot override |
| `--timeout <seconds>` | 300 | Per-call HTTP timeout; `0` = unbounded read |
| `--workers <N>` | 1 | Parallel scenario×model pairs |
| `--format table\|json` | `table` | |
| `--no-persist` | off | Skip writing to `<scenarios_dir>/.aqueduct/benchmark.duckdb` |
| `--store-path <path>` | `<scenarios_dir>/.aqueduct/benchmark.duckdb` | Override store path |
| `--gate-on-regression` | off | Exit non-zero if any regression vs. prior row (passed flip, `patch_applies` flip, `diag_score` drop > 5pp). Implies persistence. |

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
| `aqueduct patch reject <file>` | Reject a patch |
| `aqueduct patch commit` | Git commit all applied patches |
| `aqueduct patch rollback` | Revert to previous state |

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